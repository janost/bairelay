//! Session send loop.
//!
//! Receives `Frame`s via `broadcast::Receiver`, packetizes them through
//! the RTP dispatcher, and writes packets to each session track's own
//! `Transport`. RTCP Sender Reports are intentionally never emitted
//! (each SR re-anchored mpv/ffmpeg's decode clock and surfaced as an A-V
//! hitch). Terminates on `cancel`, broadcast closed, or unrecoverable
//! transport error.
//!
//! The session task is a small **coordinator** that, after the PLAY
//! gate, spawns two independent per-kind dispatch loops —
//! `video_dispatch_loop` and `audio_dispatch_loop`. Each owns its
//! own `broadcast::Receiver`, polls `session_tracks` for its kind's
//! track lazily on track-miss, and writes packets via the track's
//! transport. The shared `TcpInterleavedTransport` mutex is held only
//! for one `$-framed` packet at a time, so audio FU fragments
//! interleave between video FU fragments at packet granularity. This
//! prevents a 4 K HEVC IDR from monopolising the wire while audio
//! frames pile up — the unified-loop design that preceded this task
//! produced audible 0.1–1.4 s audio gaps at the camera's GOP cadence.

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::buffer::LastFrameBuffer;
use crate::provider::Frame;
use crate::rtp::RtpCounters;
use crate::server::registry::{FirstVideoRtpSlot, SessionRegistry, TrackEntry, TrackKind};
use crate::server::transport::Transport;

/// Per-track state owned by one per-kind dispatch loop: the track's
/// transport plus its running RTP sequence counter.
struct RuntimeTrack {
	kind: TrackKind,
	transport: Arc<dyn Transport>,
	counters: RtpCounters,
}

/// Dispatch one [`Frame`] to a single [`RuntimeTrack`].
///
/// Returns `Err(())` on the first unrecoverable transport error so the
/// caller (`video_dispatch_loop` / `audio_dispatch_loop`) can tear the
/// session down. The kind-vs-track match is the caller's responsibility:
/// each per-kind dispatch loop only invokes this with frames of its
/// own kind.
///
/// `first_video_rtp` is `Some(...)` only for the video loop — audio
/// frames don't contribute to the `RTP-Info:` PLAY response header.
async fn dispatch_one(
	frame: &Frame,
	track: &mut RuntimeTrack,
	first_video_rtp: Option<&FirstVideoRtpSlot>,
) -> Result<(), ()> {
	let pts_for_first = if let Frame::Video { pts_90khz, .. } = frame {
		Some(*pts_90khz)
	} else {
		None
	};
	let seq_for_first = track.counters.seq;

	let packets = crate::server::packetizer::dispatch(frame, &mut track.counters);

	for pkt in &packets {
		if track.transport.send_rtp(pkt).await.is_err() {
			return Err(());
		}
	}

	// First video packet's (seq, rtp_ts) is recorded once for the
	// PLAY handler's `RTP-Info:` header.
	if let (Some(slot), TrackKind::Video) = (first_video_rtp, track.kind)
		&& !packets.is_empty()
		&& let Ok(mut guard) = slot.lock()
		&& guard.is_none()
		&& let Some(pts) = pts_for_first
	{
		*guard = Some((seq_for_first, pts));
	}

	Ok(())
}

/// Build a [`RuntimeTrack`] for the requested `kind` from the registry's
/// shared track list. Returns `None` when no track of that kind has
/// been appended yet (typical for audio between PLAY and the second
/// SETUP).
fn build_runtime_for_kind(
	shared: &Arc<Mutex<Vec<TrackEntry>>>,
	kind: TrackKind,
) -> Option<RuntimeTrack> {
	let guard = shared.lock().expect("session tracks lock poisoned");
	let entry = guard.iter().find(|t| t.kind == kind)?;
	Some(RuntimeTrack {
		kind,
		transport: Arc::clone(&entry.transport),
		counters: RtpCounters {
			ssrc: entry.ssrc,
			// Random starting seq per RFC 3550.
			seq: rand::random::<u16>(),
		},
	})
}

/// Run the per-session send loop until cancelled or the upstream broadcast
/// closes.
///
/// **Architecture (2026-05-01):** the session task is a small
/// **coordinator** that, after the PLAY gate, spawns two independent
/// per-kind dispatch loops — `video_dispatch_loop` and
/// `audio_dispatch_loop`. Each loop:
/// - holds its own `broadcast::Receiver` (via `resubscribe()`), so a
///   slow video write never queues audio behind it on the consumer side;
/// - writes packets through its track's `Arc<dyn Transport>`, which
///   internally locks the shared TCP writer for ONE `$-framed` packet
///   at a time (≤ MTU, typically 1400 bytes), so audio FU fragments
///   interleave between video FU fragments at packet granularity;
/// - polls `session_tracks` lazily on track-miss, so a late audio
///   SETUP that appends a track is picked up on the next matching
///   frame.
///
/// The video loop runs `replay_burst` once before its live-dispatch
/// phase so the cached parameter-sets + I-frame still arrive before the
/// first live frame on the wire. Audio doesn't wait for replay.
///
/// **Why split:** running audio + video through a single `tokio::select!`
/// arm meant a 4 K HEVC IDR (~200–500 KB / ~150–370 RTP packets)
/// monopolised the dispatch task while it was being TCP-written. Audio
/// frames built up in the broadcast queue and drained in a burst once
/// the video write completed — audible 0.1–1.4 s gaps every ~2 s
/// (camera GOP cadence). Per-RTP-packet probe before/after this
/// refactor is in `tests/scripts/rtsp_probe.py`-style tooling.
///
/// **Failure model:** any child loop returns on broadcast `Lagged`,
/// `Closed`, or unrecoverable transport I/O. The coordinator awaits
/// `cancel` and the JoinSet's first completion — whichever wins, the
/// other child is then cancelled and joined, transports closed, and
/// the session removed from the registry (which drops the
/// `SubscriptionHandle` + its `WakeLockGuard`).
#[allow(clippy::too_many_arguments)]
pub async fn run(
	frames: broadcast::Receiver<Frame>,
	last_frame: Arc<LastFrameBuffer>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	sessions: Arc<SessionRegistry>,
	session_id: String,
	first_video_rtp: FirstVideoRtpSlot,
	cancel: CancellationToken,
	play_signal: Arc<Notify>,
	play_fired: Arc<std::sync::atomic::AtomicBool>,
) {
	// RFC 2326 §10.4/§10.5: PLAY starts media delivery. Between SETUP
	// and PLAY the server MUST NOT send media data. Violating this breaks
	// downstream RTSP re-publishers — HA's go2rtc `ffmpeg:` wrap breaks
	// its RECORD pipe when RTP arrives before its second SETUP completes.
	// Park until PLAY fires or the session is cancelled.
	//
	// Notify ordering safety: PLAY handler sets `play_fired` THEN calls
	// `notify_waiters`. We register the notified() future BEFORE
	// re-checking the flag, so any PLAY that happens between these two
	// points latches a permit the await will consume — no lost edge.
	{
		let notified = play_signal.notified();
		tokio::pin!(notified);
		if !play_fired.load(std::sync::atomic::Ordering::SeqCst) {
			tokio::select! {
				_ = cancel.cancelled() => {
					let _ = sessions.remove(&session_id);
					return;
				}
				_ = notified.as_mut() => {}
			}
		}
	}

	// Spawn per-kind dispatch loops. Each child holds its own
	// `broadcast::Receiver` — independent positions, no head-of-line
	// blocking between video and audio.
	//
	// Asymmetry by design: the video child takes ownership of the
	// original `frames` receiver (preserving its read position so
	// pre-PLAY buffered frames + lag detection both flow through it),
	// while the audio child gets a fresh `resubscribe()` (starts at
	// the channel tail). Audio losing a few buffered frames at session
	// start is benign — receivers expect to start at "now" anyway.
	let audio_frames = frames.resubscribe();
	let mut joinset = tokio::task::JoinSet::new();

	{
		let last_frame = Arc::clone(&last_frame);
		let session_tracks = Arc::clone(&session_tracks);
		let cancel = cancel.clone();
		let first_video_rtp = first_video_rtp.clone();
		joinset.spawn(async move {
			video_dispatch_loop(frames, last_frame, session_tracks, cancel, first_video_rtp).await;
		});
	}
	{
		let session_tracks = Arc::clone(&session_tracks);
		let cancel = cancel.clone();
		joinset.spawn(async move {
			audio_dispatch_loop(audio_frames, session_tracks, cancel).await;
		});
	}

	// Wait for either an external cancel or the first child to exit
	// (broadcast lag/closed, transport error). Any child exit cascades
	// to the other via `cancel`.
	tokio::select! {
		biased;
		_ = cancel.cancelled() => {}
		_ = joinset.join_next() => {}
	}
	cancel.cancel();
	while joinset.join_next().await.is_some() {}

	let _ = sessions.remove(&session_id);
}

/// Per-session video dispatch loop. Owns its own broadcast receiver,
/// runs `replay_burst` once at startup, then forwards `Frame::Video`
/// packets via the video track's transport.
async fn video_dispatch_loop(
	mut frames: broadcast::Receiver<Frame>,
	last_frame: Arc<LastFrameBuffer>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	cancel: CancellationToken,
	first_video_rtp: FirstVideoRtpSlot,
) {
	let mut track: Option<RuntimeTrack> = build_runtime_for_kind(&session_tracks, TrackKind::Video);

	// Replay cached burst on the video track if we have one. Audio
	// dispatch has already started in parallel — replay does not block
	// audio.
	let mut awaiting_live_keyframe = false;
	if let Some(t) = track.as_mut()
		&& let Some(burst) = last_frame.video_snapshot()
	{
		// Only arm the keyframe gate when replay actually emitted
		// something. An empty `iframe_nals` burst would otherwise
		// suppress live P-frames until the camera's next IDR for
		// nothing in return.
		awaiting_live_keyframe = replay_burst(&burst, &t.transport, &mut t.counters).await;
	}

	loop {
		let frame = tokio::select! {
			biased;
			_ = cancel.cancelled() => return,
			recv = frames.recv() => match recv {
				Ok(f) => f,
				Err(broadcast::error::RecvError::Lagged(n)) => {
					tracing::warn!(missed = n, kind = "video", "subscriber lagged; dropping session");
					return;
				}
				Err(broadcast::error::RecvError::Closed) => return,
			},
		};

		// Skip non-video frames; the audio loop handles those.
		if !matches!(frame, Frame::Video { .. }) {
			continue;
		}

		// Lazy track resolution. Refreshes after a track-miss in case
		// the video track was appended late (uncommon — video is
		// usually the first SETUP).
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Video);
			if track.is_none() {
				continue;
			}
		}
		let track = track.as_mut().expect("just resolved Some above");

		// Suppress post-burst live video until a fresh IDR arrives.
		// Cached IDR was from the previous GOP; live P-frames before
		// the next live IDR would log `Could not find ref with POC N`
		// in the receiver's decoder.
		if awaiting_live_keyframe {
			match &frame {
				Frame::Video { keyframe: true, .. } => awaiting_live_keyframe = false,
				_ => continue,
			}
		}

		if dispatch_one(&frame, track, Some(&first_video_rtp))
			.await
			.is_err()
		{
			return;
		}
	}
}

/// Per-session audio dispatch loop. Owns its own broadcast receiver,
/// forwards `Frame::Audio` via the audio track's transport.
async fn audio_dispatch_loop(
	mut frames: broadcast::Receiver<Frame>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	cancel: CancellationToken,
) {
	let mut track: Option<RuntimeTrack> = build_runtime_for_kind(&session_tracks, TrackKind::Audio);

	loop {
		let frame = tokio::select! {
			biased;
			_ = cancel.cancelled() => return,
			recv = frames.recv() => match recv {
				Ok(f) => f,
				Err(broadcast::error::RecvError::Lagged(n)) => {
					tracing::warn!(missed = n, kind = "audio", "subscriber lagged; dropping session");
					return;
				}
				Err(broadcast::error::RecvError::Closed) => return,
			},
		};

		if !matches!(frame, Frame::Audio { .. }) {
			continue;
		}

		// Lazy track resolution. Audio is frequently SETUP'd late
		// (after the client has issued its first DESCRIBE → SETUP
		// pair on video), so this branch fires on real traffic.
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Audio);
			if track.is_none() {
				continue;
			}
		}
		let track = track.as_mut().expect("just resolved Some above");

		if dispatch_one(&frame, track, None).await.is_err() {
			return;
		}
	}
}
/// Replay the cached `VideoBurst` on `transport`. Returns `true` when at
/// least one RTP packet was successfully sent, `false` for an empty
/// `iframe_nals` burst or when the first send returned an error.
///
/// Packets carry the camera-side pts captured with the burst so they are
/// continuous with the live packets that follow; replaying at ts=0 before
/// a live pts of 60M+ ticks made HA's go2rtc `ffmpeg:` wrap drop the feed.
///
/// Only the I-frame NALs are sent. Parameter sets reach the decoder via
/// SDP `sprop-*`; sending them in-band as single-NAL packets made go2rtc's
/// `ffmpeg:` re-publish aggregate them into an HEVC AP (type 48) its own
/// depayloader can't split, crashing its frame.jpeg transcoder.
async fn replay_burst(
	burst: &crate::buffer::VideoBurst,
	transport: &Arc<dyn Transport>,
	counters: &mut RtpCounters,
) -> bool {
	let pkts = crate::server::packetizer::packetize_video(
		burst.codec,
		&burst.iframe_nals,
		burst.captured_pts_90khz,
		true,
		counters,
	);
	let mut sent = false;
	for pkt in &pkts {
		if transport.send_rtp(pkt).await.is_err() {
			return sent;
		}
		sent = true;
	}
	sent
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::AtomicBool;
	use std::time::Duration;

	use crate::codec::VideoCodec;
	use crate::provider::{AudioPayload, Frame};
	use crate::server::transport::noop_transport_for_tests;
	use bytes::Bytes;

	fn track(kind: TrackKind, ssrc: u32) -> TrackEntry {
		TrackEntry {
			kind,
			transport: noop_transport_for_tests(),
			ssrc,
		}
	}

	fn video_frame(keyframe: bool, pts: u32) -> Frame {
		Frame::Video {
			codec: VideoCodec::H264,
			nals: vec![Bytes::from_static(&[0x65, 0xaa, 0xbb])],
			pts_90khz: pts,
			keyframe,
			access_unit_end: true,
		}
	}

	fn audio_frame(pts: u32) -> Frame {
		Frame::Audio {
			payload: AudioPayload::G711Ulaw {
				samples: Bytes::from_static(&[0x7f; 160]),
			},
			pts,
		}
	}

	/// Everything `run()` needs, with PLAY already fired.
	struct Fixture {
		tx: broadcast::Sender<Frame>,
		rx: broadcast::Receiver<Frame>,
		last_frame: Arc<LastFrameBuffer>,
		tracks: Arc<Mutex<Vec<TrackEntry>>>,
		cancel: CancellationToken,
		play_signal: Arc<Notify>,
		play_fired: Arc<AtomicBool>,
	}

	impl Fixture {
		fn new(tracks: Vec<TrackEntry>, capacity: usize) -> Self {
			let (tx, rx) = broadcast::channel::<Frame>(capacity);
			Self {
				tx,
				rx,
				last_frame: Arc::new(LastFrameBuffer::new()),
				tracks: Arc::new(Mutex::new(tracks)),
				cancel: CancellationToken::new(),
				play_signal: Arc::new(Notify::new()),
				play_fired: Arc::new(AtomicBool::new(true)),
			}
		}

		/// Spawn `run()`; returns the sender (keep it alive to hold the
		/// broadcast open) and the task handle.
		fn spawn(self) -> (broadcast::Sender<Frame>, tokio::task::JoinHandle<()>) {
			let handle = tokio::spawn(run(
				self.rx,
				self.last_frame,
				self.tracks,
				Arc::new(SessionRegistry::new()),
				"sess".to_string(),
				Arc::new(Mutex::new(None)),
				self.cancel,
				self.play_signal,
				self.play_fired,
			));
			(self.tx, handle)
		}
	}

	async fn yield_many(n: usize) {
		for _ in 0..n {
			tokio::task::yield_now().await;
		}
	}

	async fn join(handle: tokio::task::JoinHandle<()>, what: &str) {
		tokio::time::timeout(Duration::from_millis(500), handle)
			.await
			.unwrap_or_else(|_| panic!("run did not exit: {what}"))
			.unwrap();
	}

	#[tokio::test(start_paused = true)]
	async fn run_dispatches_video_frame_and_cancels_cleanly() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 0xdead_beef)], 32);
		let cancel = fx.cancel.clone();
		let (tx, handle) = fx.spawn();
		tx.send(video_frame(true, 90_000)).unwrap();
		yield_many(8).await;
		cancel.cancel();
		join(handle, "cancel").await;
	}

	#[tokio::test]
	async fn run_cancels_before_play_fires() {
		let mut fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		fx.play_fired = Arc::new(AtomicBool::new(false));
		fx.cancel.cancel();
		let (_tx, handle) = fx.spawn();
		join(handle, "cancel before PLAY").await;
	}

	#[tokio::test(start_paused = true)]
	async fn run_replays_cached_burst_on_startup() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 0x0101_0101)], 32);
		fx.last_frame.replace_video(crate::buffer::VideoBurst {
			codec: VideoCodec::H264,
			iframe_nals: vec![vec![0x65, 0xaa, 0xbb, 0xcc]],
			captured_pts_90khz: 12_345,
		});
		let cancel = fx.cancel.clone();
		let (_tx, handle) = fx.spawn();
		yield_many(8).await;
		cancel.cancel();
		join(handle, "cancel after replay").await;
	}

	#[tokio::test(start_paused = true)]
	async fn run_picks_up_audio_track_appended_after_play() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		let tracks = Arc::clone(&fx.tracks);
		let cancel = fx.cancel.clone();
		let (tx, handle) = fx.spawn();
		// Audio before the track exists is dropped without ending the task.
		tx.send(audio_frame(0)).unwrap();
		yield_many(8).await;
		tracks.lock().unwrap().push(track(TrackKind::Audio, 2));
		tx.send(audio_frame(160)).unwrap();
		yield_many(8).await;
		assert!(!handle.is_finished());
		cancel.cancel();
		join(handle, "cancel").await;
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_closed() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		let (tx, handle) = fx.spawn();
		drop(tx);
		join(handle, "broadcast closed").await;
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_lagged() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 2);
		for i in 0..10 {
			let _ = fx.tx.send(video_frame(false, i));
		}
		let (_tx, handle) = fx.spawn();
		join(handle, "lagged").await;
	}

	#[test]
	fn build_runtime_for_kind_returns_none_when_kind_absent() {
		let shared: Arc<Mutex<Vec<TrackEntry>>> = Arc::new(Mutex::new(vec![]));
		assert!(build_runtime_for_kind(&shared, TrackKind::Video).is_none());
		assert!(build_runtime_for_kind(&shared, TrackKind::Audio).is_none());
	}

	/// Transport that fails every send — exercises the Err arms in
	/// `dispatch_one` / `replay_burst`.
	struct FailingTransport;
	#[async_trait::async_trait]
	impl Transport for FailingTransport {
		async fn send_rtp(&self, _: &[u8]) -> std::io::Result<()> {
			Err(std::io::Error::other("boom"))
		}
	}

	#[tokio::test]
	async fn dispatch_one_returns_err_when_transport_send_fails() {
		let first_video_rtp: FirstVideoRtpSlot = Arc::new(Mutex::new(None));
		let mut track = RuntimeTrack {
			kind: TrackKind::Video,
			transport: Arc::new(FailingTransport),
			counters: RtpCounters { ssrc: 1, seq: 0 },
		};
		let frame = video_frame(true, 0);
		let res = dispatch_one(&frame, &mut track, Some(&first_video_rtp)).await;
		assert!(res.is_err());
	}

	#[tokio::test]
	async fn replay_burst_reports_nothing_sent_on_failing_transport() {
		for (codec, header) in [
			(VideoCodec::H264, vec![0x65u8]),
			(VideoCodec::H265, vec![0x26u8, 0x01]),
		] {
			// Large IDR forces the FU path.
			let mut big = header;
			big.extend(vec![0xaa; 5000]);
			let burst = crate::buffer::VideoBurst {
				codec,
				iframe_nals: vec![big],
				captured_pts_90khz: 0,
			};
			let transport: Arc<dyn Transport> = Arc::new(FailingTransport);
			let mut counters = RtpCounters { ssrc: 7, seq: 0 };
			assert!(!replay_burst(&burst, &transport, &mut counters).await);
		}
	}
}
