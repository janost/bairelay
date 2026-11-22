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
//!
//! Cancellation is observed between packets, never mid-packet: dropping
//! a TCP write halfway would leave a torn `$` frame in front of the next
//! RTSP response (e.g. the TEARDOWN reply). Each packet write is bounded
//! by the transport's write timeout, so a stalled client still frees the
//! task within that bound.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::buffer::VideoBurst;
use crate::provider::Frame;
use crate::rtp::RtpCounters;
use crate::server::registry::{PlayGate, SessionRegistry, TrackEntry, TrackKind};
use crate::server::transport::Transport;

/// Per-track state owned by one per-kind dispatch loop: the track's
/// transport plus its running RTP sequence counter.
struct RuntimeTrack {
	transport: Arc<dyn Transport>,
	counters: RtpCounters,
}

/// Send `packets` in order. `Err(())` on cancellation (checked before
/// each packet) or the first transport error; either ends the session.
async fn send_all(
	packets: &[Vec<u8>],
	transport: &Arc<dyn Transport>,
	cancel: &CancellationToken,
) -> Result<(), ()> {
	for pkt in packets {
		if cancel.is_cancelled() || transport.send_rtp(pkt).await.is_err() {
			return Err(());
		}
	}
	Ok(())
}

/// Dispatch one [`Frame`] to a single [`RuntimeTrack`]. The kind-vs-track
/// match is the caller's responsibility: each per-kind dispatch loop only
/// invokes this with frames of its own kind.
async fn dispatch_one(
	frame: &Frame,
	track: &mut RuntimeTrack,
	cancel: &CancellationToken,
) -> Result<(), ()> {
	let packets = crate::server::packetizer::dispatch(frame, &mut track.counters);
	send_all(&packets, &track.transport, cancel).await
}

/// Build a [`RuntimeTrack`] for the requested `kind` from the registry's
/// shared track list. Returns `None` when no track of that kind has
/// been appended yet (typical for audio between PLAY and the second
/// SETUP). The counter starts at the track's SETUP-time `initial_seq`,
/// which PLAY advertised in `RTP-Info`.
fn build_runtime_for_kind(
	shared: &Arc<Mutex<Vec<TrackEntry>>>,
	kind: TrackKind,
) -> Option<RuntimeTrack> {
	let guard = shared.lock().expect("session tracks lock poisoned");
	let entry = guard.iter().find(|t| t.kind == kind)?;
	Some(RuntimeTrack {
		transport: Arc::clone(&entry.transport),
		counters: RtpCounters {
			ssrc: entry.ssrc,
			seq: entry.initial_seq,
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
/// The video loop first replays the keyframe burst handed over by the
/// PLAY gate, so a new client can decode before the camera's next IDR.
/// Audio doesn't wait for replay.
///
/// **Why split:** running audio + video through a single `tokio::select!`
/// arm meant a 4 K HEVC IDR (~200–500 KB / ~150–370 RTP packets)
/// monopolised the dispatch task while it was being TCP-written. Audio
/// frames built up in the broadcast queue and drained in a burst once
/// the video write completed — audible 0.1–1.4 s gaps every ~2 s
/// (camera GOP cadence).
///
/// **Failure model:** any child loop returns on cancel, broadcast
/// `Lagged`, `Closed`, or unrecoverable transport I/O (including a write
/// timeout). The coordinator awaits `cancel` and the JoinSet's first
/// completion — whichever wins, the other child is then cancelled and
/// joined, and the session removed from the registry (which drops the
/// `SubscriptionHandle` + its `WakeLockGuard`).
pub async fn run(
	frames: broadcast::Receiver<Frame>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	sessions: Arc<SessionRegistry>,
	session_id: String,
	play: Arc<PlayGate>,
	cancel: CancellationToken,
) {
	// RFC 2326 §10.4/§10.5: no media before PLAY. The PLAY handler opens
	// the gate only after writing its response, so the replayed IDR can't
	// overtake the reply (ffmpeg discards `$` data while it waits for it).
	let replay = tokio::select! {
		_ = cancel.cancelled() => {
			let _ = sessions.remove(&session_id);
			return;
		}
		replay = play.wait() => replay,
	};

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
		let session_tracks = Arc::clone(&session_tracks);
		let cancel = cancel.clone();
		joinset.spawn(async move {
			video_dispatch_loop(frames, replay, session_tracks, cancel).await;
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

/// Receive the next frame, or `None` when the loop should end (cancel,
/// broadcast closed, or lagged — a lagged subscriber has lost frames the
/// decoder needs, so the session is dropped and the client reconnects).
async fn next_frame(
	frames: &mut broadcast::Receiver<Frame>,
	cancel: &CancellationToken,
	kind: &'static str,
) -> Option<Frame> {
	tokio::select! {
		biased;
		_ = cancel.cancelled() => None,
		recv = frames.recv() => match recv {
			Ok(f) => Some(f),
			Err(broadcast::error::RecvError::Lagged(n)) => {
				tracing::warn!(missed = n, kind, "subscriber lagged; dropping session");
				None
			}
			Err(broadcast::error::RecvError::Closed) => None,
		},
	}
}

/// Per-session video dispatch loop. Replays `replay` once at startup,
/// then forwards `Frame::Video` packets via the video track's transport.
async fn video_dispatch_loop(
	mut frames: broadcast::Receiver<Frame>,
	replay: Option<VideoBurst>,
	session_tracks: Arc<Mutex<Vec<TrackEntry>>>,
	cancel: CancellationToken,
) {
	let mut track: Option<RuntimeTrack> = build_runtime_for_kind(&session_tracks, TrackKind::Video);

	// Audio dispatch has already started in parallel — replay does not
	// block audio.
	let mut awaiting_live_keyframe = false;
	if let Some(t) = track.as_mut()
		&& let Some(burst) = replay
	{
		match replay_burst(&burst, t, &cancel).await {
			Ok(sent) => {
				// Only arm the keyframe gate when replay actually emitted
				// something. An empty `iframe_nals` burst would otherwise
				// suppress live P-frames until the camera's next IDR for
				// nothing in return.
				awaiting_live_keyframe = sent;
			}
			Err(()) => return,
		}
	}

	while let Some(frame) = next_frame(&mut frames, &cancel, "video").await {
		// Skip non-video frames; the audio loop handles those.
		if !matches!(frame, Frame::Video { .. }) {
			continue;
		}

		// Lazy track resolution. Refreshes after a track-miss in case
		// the video track was appended late (uncommon — video is
		// usually the first SETUP).
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Video);
		}
		let Some(track) = track.as_mut() else {
			continue;
		};

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

		if dispatch_one(&frame, track, &cancel).await.is_err() {
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

	while let Some(frame) = next_frame(&mut frames, &cancel, "audio").await {
		if !matches!(frame, Frame::Audio { .. }) {
			continue;
		}

		// Lazy track resolution. Audio is frequently SETUP'd late
		// (after the client has issued its first DESCRIBE → SETUP
		// pair on video), so this branch fires on real traffic.
		if track.is_none() {
			track = build_runtime_for_kind(&session_tracks, TrackKind::Audio);
		}
		let Some(track) = track.as_mut() else {
			continue;
		};

		if dispatch_one(&frame, track, &cancel).await.is_err() {
			return;
		}
	}
}

/// Replay the cached `VideoBurst` on `track`. `Ok(true)` when packets
/// were sent, `Ok(false)` for an empty `iframe_nals` burst, `Err(())` on
/// cancellation or a transport error.
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
	burst: &VideoBurst,
	track: &mut RuntimeTrack,
	cancel: &CancellationToken,
) -> Result<bool, ()> {
	let pkts = crate::server::packetizer::packetize_video(
		burst.codec,
		&burst.iframe_nals,
		burst.captured_pts_90khz,
		true,
		&mut track.counters,
	);
	send_all(&pkts, &track.transport, cancel).await?;
	Ok(!pkts.is_empty())
}

#[cfg(test)]
mod tests {
	use super::*;
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
			initial_seq: 0,
			control_uri: String::new(),
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

	/// Everything `run()` needs, with a closed PLAY gate.
	struct Fixture {
		tx: broadcast::Sender<Frame>,
		rx: broadcast::Receiver<Frame>,
		tracks: Arc<Mutex<Vec<TrackEntry>>>,
		cancel: CancellationToken,
		play: Arc<PlayGate>,
	}

	impl Fixture {
		fn new(tracks: Vec<TrackEntry>, capacity: usize) -> Self {
			let (tx, rx) = broadcast::channel::<Frame>(capacity);
			Self {
				tx,
				rx,
				tracks: Arc::new(Mutex::new(tracks)),
				cancel: CancellationToken::new(),
				play: Arc::new(PlayGate::new()),
			}
		}

		/// Spawn `run()`; returns the sender (keep it alive to hold the
		/// broadcast open) and the task handle.
		fn spawn(self) -> (broadcast::Sender<Frame>, tokio::task::JoinHandle<()>) {
			let handle = tokio::spawn(run(
				self.rx,
				self.tracks,
				Arc::new(SessionRegistry::new()),
				"sess".to_string(),
				self.play,
				self.cancel,
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

	/// Transport that records every packet it is asked to send.
	#[derive(Default)]
	struct Recorder(Mutex<Vec<Vec<u8>>>);
	#[async_trait::async_trait]
	impl Transport for Recorder {
		async fn send_rtp(&self, p: &[u8]) -> std::io::Result<()> {
			self.0.lock().unwrap().push(p.to_vec());
			Ok(())
		}
	}

	#[tokio::test(start_paused = true)]
	async fn run_dispatches_video_frame_and_cancels_cleanly() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 0xdead_beef)], 32);
		fx.play.open(None);
		let cancel = fx.cancel.clone();
		let (tx, handle) = fx.spawn();
		tx.send(video_frame(true, 90_000)).unwrap();
		yield_many(8).await;
		cancel.cancel();
		join(handle, "cancel").await;
	}

	#[tokio::test]
	async fn run_cancels_before_play_fires() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		fx.cancel.cancel();
		let (_tx, handle) = fx.spawn();
		join(handle, "cancel before PLAY").await;
	}

	#[tokio::test(start_paused = true)]
	async fn nothing_is_sent_before_play_then_replay_starts_at_initial_seq() {
		let rec = Arc::new(Recorder::default());
		let mut video = track(TrackKind::Video, 0x0101_0101);
		video.transport = Arc::clone(&rec) as Arc<dyn Transport>;
		video.initial_seq = 4242;
		let fx = Fixture::new(vec![video], 32);
		let play = Arc::clone(&fx.play);
		let cancel = fx.cancel.clone();
		let (tx, handle) = fx.spawn();

		tx.send(video_frame(true, 1)).unwrap();
		yield_many(8).await;
		assert!(rec.0.lock().unwrap().is_empty(), "media before PLAY");

		play.open(Some(VideoBurst {
			codec: VideoCodec::H264,
			iframe_nals: vec![vec![0x65, 0xaa, 0xbb, 0xcc]],
			captured_pts_90khz: 12_345,
		}));
		yield_many(8).await;
		{
			let sent = rec.0.lock().unwrap();
			let first = sent.first().expect("replay sent");
			assert_eq!(u16::from_be_bytes([first[2], first[3]]), 4242);
			assert_eq!(
				u32::from_be_bytes([first[4], first[5], first[6], first[7]]),
				12_345
			);
		}
		cancel.cancel();
		join(handle, "cancel after replay").await;
	}

	#[tokio::test(start_paused = true)]
	async fn run_picks_up_audio_track_appended_after_play() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		fx.play.open(None);
		let tracks = Arc::clone(&fx.tracks);
		let cancel = fx.cancel.clone();
		let (tx, handle) = fx.spawn();
		// Audio before the track exists is dropped without ending the task.
		tx.send(audio_frame(0)).unwrap();
		yield_many(8).await;
		let rec = Arc::new(Recorder::default());
		let mut audio = track(TrackKind::Audio, 2);
		audio.transport = Arc::clone(&rec) as Arc<dyn Transport>;
		tracks.lock().unwrap().push(audio);
		tx.send(audio_frame(160)).unwrap();
		yield_many(8).await;
		assert_eq!(rec.0.lock().unwrap().len(), 1);
		cancel.cancel();
		join(handle, "cancel").await;
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_closed() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 32);
		fx.play.open(None);
		let (tx, handle) = fx.spawn();
		drop(tx);
		join(handle, "broadcast closed").await;
	}

	#[tokio::test]
	async fn run_exits_on_broadcast_lagged() {
		let fx = Fixture::new(vec![track(TrackKind::Video, 1)], 2);
		fx.play.open(None);
		for i in 0..10 {
			let _ = fx.tx.send(video_frame(false, i));
		}
		let (_tx, handle) = fx.spawn();
		join(handle, "lagged").await;
	}

	#[tokio::test]
	async fn send_all_stops_at_packet_boundary_once_cancelled() {
		let rec = Arc::new(Recorder::default());
		let transport: Arc<dyn Transport> = Arc::clone(&rec) as Arc<dyn Transport>;
		let cancel = CancellationToken::new();
		cancel.cancel();
		assert!(
			send_all(&[vec![0u8; 12]], &transport, &cancel)
				.await
				.is_err()
		);
		assert!(rec.0.lock().unwrap().is_empty());
	}

	#[test]
	fn build_runtime_for_kind_returns_none_when_kind_absent() {
		let shared: Arc<Mutex<Vec<TrackEntry>>> = Arc::new(Mutex::new(vec![]));
		assert!(build_runtime_for_kind(&shared, TrackKind::Video).is_none());
		assert!(build_runtime_for_kind(&shared, TrackKind::Audio).is_none());
	}

	/// Transport that fails every send.
	struct FailingTransport;
	#[async_trait::async_trait]
	impl Transport for FailingTransport {
		async fn send_rtp(&self, _: &[u8]) -> std::io::Result<()> {
			Err(std::io::Error::other("boom"))
		}
	}

	fn failing_track() -> RuntimeTrack {
		RuntimeTrack {
			transport: Arc::new(FailingTransport),
			counters: RtpCounters { ssrc: 1, seq: 0 },
		}
	}

	#[tokio::test]
	async fn dispatch_one_returns_err_when_transport_send_fails() {
		let res = dispatch_one(
			&video_frame(true, 0),
			&mut failing_track(),
			&CancellationToken::new(),
		)
		.await;
		assert!(res.is_err());
	}

	#[tokio::test]
	async fn replay_burst_errs_on_failing_transport() {
		for (codec, header) in [
			(VideoCodec::H264, vec![0x65u8]),
			(VideoCodec::H265, vec![0x26u8, 0x01]),
		] {
			// Large IDR forces the FU path.
			let mut big = header;
			big.extend(vec![0xaa; 5000]);
			let burst = VideoBurst {
				codec,
				iframe_nals: vec![big],
				captured_pts_90khz: 0,
			};
			let res = replay_burst(&burst, &mut failing_track(), &CancellationToken::new()).await;
			assert!(res.is_err());
		}
	}
}
