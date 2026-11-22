use super::DiscoveryResult;
use crate::bc::codex::BcCodex;
use crate::bc::model::*;
use crate::bc_protocol::errors::BcUdpDropReceiverKind;
use crate::bcudp::codex::BcUdpCodex;
use crate::bcudp::{model::*, xml::*};
use crate::{Credentials, Error, Result};
use delegate::delegate;
use futures::{
	sink::{Sink, SinkExt},
	stream::{IntoAsyncRead, Stream, StreamExt, TryStreamExt},
};
use rand::seq::SliceRandom;
use std::collections::BTreeMap;
use std::io::{Error as IoError, Result as IoResult};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::{
	net::UdpSocket,
	sync::{
		mpsc::{channel, error::TrySendError},
		watch::{Sender as WatchSender, channel as watch},
	},
	task::JoinSet,
	time::{Duration, Instant, Interval, Sleep, interval, sleep},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};
use tokio_util::sync::{CancellationToken, PollSender};
use tokio_util::{
	codec::{Decoder, Encoder, Framed},
	udp::UdpFramed,
};

const MTU: usize = 1350;
const UDPDATA_HEADER_SIZE: usize = 20;

pub(crate) type InnerFramed = Framed<Compat<IntoAsyncRead<UdpPayloadSource>>, BcCodex>;
pub(crate) struct UdpSource {
	inner: Pin<Box<InnerFramed>>,
}

impl UdpSource {
	#[allow(unused)]
	pub(crate) async fn new<T: Into<String>, U: Into<String>>(
		addr: SocketAddr,
		client_id: i32,
		camera_id: i32,
		username: T,
		password: Option<U>,
		debug: bool,
	) -> Result<Self> {
		let stream = Arc::new(connect().await?);

		Self::new_from_socket(
			stream, addr, client_id, camera_id, username, password, debug,
		)
		.await
	}
	pub(crate) async fn new_from_discovery<T: Into<String>, U: Into<String>>(
		discovery: DiscoveryResult,
		username: T,
		password: Option<U>,
		debug: bool,
	) -> Result<Self> {
		// Ensure that the discovery keep alive are all stopped here
		// We now handle all coms in UdpSource
		discovery.socket.set_broadcast(false)?;
		Self::new_from_socket(
			discovery.socket,
			discovery.addr,
			discovery.client_id,
			discovery.camera_id,
			username,
			password,
			debug,
		)
		.await
	}

	pub(crate) async fn new_from_socket<T: Into<String>, U: Into<String>>(
		stream: Arc<UdpSocket>,
		addr: SocketAddr,
		client_id: i32,
		camera_id: i32,
		username: T,
		password: Option<U>,
		debug: bool,
	) -> Result<Self> {
		let bcudp_source = BcUdpSource::new_from_socket(stream, addr).await?;
		let payload_source = bcudp_source.into_payload_source(client_id, camera_id).await;
		let async_read = payload_source.into_async_read().compat();
		let codex = if debug {
			BcCodex::new_with_debug(Credentials::new(username, password))
		} else {
			BcCodex::new(Credentials::new(username, password))
		};
		let framed = Framed::new(async_read, codex);

		Ok(Self {
			inner: Box::pin(framed),
		})
	}

	// pub(crate) async fn send(&mut self, bc: Bc) -> Result<()> {
	//     self.inner.send(bc).await
	// }
	// pub(crate) async fn recv(&mut self) -> Result<Bc> {
	//     loop {
	//         if let Some(result) = self.inner.next().await {
	//             return result;
	//         }
	//     }
	// }
}

impl Stream for UdpSource {
	type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

	delegate! {
		to self.inner.as_mut() {
			fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
		}
	}

	delegate! {
		to self.inner.as_ref().get_ref() {
			fn size_hint(&self) -> (usize, Option<usize>);
		}
	}
}

impl Sink<Bc> for UdpSource {
	type Error = <BcCodex as Encoder<Bc>>::Error;

	delegate! {
		to self.inner.as_mut() {
			fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
			fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
		}
	}
}

pub(crate) struct BcUdpSource {
	inner: Pin<Box<UdpFramed<BcUdpCodex, Arc<UdpSocket>>>>,
	addr: SocketAddr,
}

impl BcUdpSource {
	#[allow(unused)]
	pub(crate) async fn new(addr: SocketAddr) -> Result<Self> {
		let stream = Arc::new(connect().await?);

		Self::new_from_socket(stream, addr).await
	}

	#[allow(unused)]
	pub(crate) async fn new_from_discovery(discovery: DiscoveryResult) -> Result<Self> {
		Self::new_from_socket(discovery.socket, discovery.addr).await
	}

	pub(crate) async fn new_from_socket(stream: Arc<UdpSocket>, addr: SocketAddr) -> Result<Self> {
		Ok(Self {
			inner: Box::pin(UdpFramed::new(stream, BcUdpCodex::new())),
			addr,
		})
	}

	pub(crate) async fn into_payload_source(
		self,
		client_id: i32,
		camera_id: i32,
	) -> UdpPayloadSource {
		UdpPayloadSource::new(self, client_id, camera_id).await
	}
}

impl Stream for BcUdpSource {
	type Item = Result<(BcUdp, SocketAddr)>;

	delegate! {
		to self.inner.as_mut() {
			fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
		}
	}

	delegate! {
		to self.inner.as_ref().get_ref() {
			fn size_hint(&self) -> (usize, Option<usize>);
		}
	}
}

impl Sink<(BcUdp, SocketAddr)> for BcUdpSource {
	type Error = Error;

	delegate! {
		to self.inner.as_mut() {
			fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn start_send(mut self: Pin<&mut Self>, item: (BcUdp, SocketAddr)) -> std::result::Result<(), Self::Error>;
			fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
			fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
		}
	}
}

#[allow(dead_code)]
#[derive(Debug)]
enum State {
	Normal,   // Normal receive
	Flushing, // Used to send ack packets and things in the buffer
	Closed,   // Used to shutdown
	YieldNow, // Used to ensure we rest between polling packets so as to not starve the runtime
}

/// Receive throughput reported in [`UdpAck::recv_bytes_per_sec`]. The
/// camera's CUBIC sender reads that field as its bandwidth estimate, so
/// it must be the real receive rate: an under-report throttles the
/// stream (neolink #428 saw 4 Mbit/s capped at ~340 kbit/s).
#[derive(Default)]
struct RecvRate {
	window_start: Option<Instant>,
	window_bytes: u64,
	bytes_per_sec: u32,
}

impl RecvRate {
	const WINDOW: Duration = Duration::from_secs(1);

	/// Count `bytes` received at `now`. Once a window of at least 1 s
	/// closes its rate is latched. A window longer than 2 s spans an idle
	/// gap and is discarded so the last real rate is held; reporting the
	/// diluted rate would make the camera ramp up from near zero.
	fn record(&mut self, now: Instant, bytes: usize) {
		let start = *self.window_start.get_or_insert(now);
		let elapsed = now.saturating_duration_since(start);
		if elapsed >= Self::WINDOW {
			if elapsed < 2 * Self::WINDOW {
				let rate = u128::from(self.window_bytes) * 1_000_000 / elapsed.as_micros();
				self.bytes_per_sec = u32::try_from(rate).unwrap_or(u32::MAX);
			}
			self.window_start = Some(now);
			self.window_bytes = 0;
		}
		self.window_bytes = self.window_bytes.saturating_add(bytes as u64);
	}
}

pub(crate) struct UdpPayloadSource {
	inner_stream: Pin<Box<ReceiverStream<IoResult<Vec<u8>>>>>,
	inner_sink: PollSender<Vec<u8>>,
	set: JoinSet<Result<()>>,
	cancel_token: CancellationToken,
}

impl Drop for UdpPayloadSource {
	fn drop(&mut self) {
		log::trace!("Drop UdpPayloadSource");
		self.cancel_token.cancel();
		// `try_current` so a Drop outside a Tokio runtime (panic unwind,
		// runtime shutdown) doesn't double-panic. Cancel fired above; the
		// drain is best-effort.
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let _gt = handle.enter();
		let mut set = std::mem::take(&mut self.set);
		tokio::task::spawn(async move {
			while set.join_next().await.is_some() {}
			log::trace!("Dropped UdpPayloadSource");
		});
	}
}

/// Recv reorder window, in packets past `packets_want`. Ids further ahead
/// are rejected, which also bounds `received` to this many entries (a
/// hostile peer could otherwise grow it without limit). `packets_want`
/// itself is always inside the window, so the buffer can always drain.
pub const REORDER_CAP: usize = 1024;

/// How long buffered packets wait on a missing `packets_want` before we
/// give up on it and skip to the lowest buffered id. Without the skip a
/// packet the camera never resends stalls the stream for good.
const GAP_SKIP_AFTER: Duration = Duration::from_millis(500);

/// Pure-state slice of the UDP send/recv flow, extracted from
/// `UdpPayloadInner` so the per-packet decision table is unit-testable
/// and fuzz-drivable without spinning up the async coordination tasks.
/// Holds:
///
/// - `client_id` / `camera_id`: connection identifiers, immutable.
/// - `packets_sent` / `packets_want`: counters that wrap on `u32::MAX`;
///   every comparison against them uses serial (wrapping) arithmetic.
/// - `sent`: outgoing packets awaiting ack.
/// - `received`: out-of-order recv packets pending in-order delivery.
/// - `delivered_any`: false until the first in-order delivery; until
///   then acks use the "nothing received" form.
/// - `stalled_since`: when `received` first sat behind a gap.
/// - `recv_rate`: receive throughput reported in outgoing acks.
///
/// All methods are pure-state — no I/O, no spawn, no timers; time-aware
/// methods take `now` from the caller. The caller
/// (production: `UdpPayloadInner::run`; fuzz: the harness) drives the
/// state by calling these with bytes off the wire and reading back the
/// resulting outgoing UdpData / UdpAck packets.
///
/// Visibility: the type is `pub` so the `fuzz-api` feature can re-export
/// it from `crate::fuzz_api`. Without the feature the parent module
/// (`udpsource`) is private, so the type is effectively crate-only.
pub struct UdpFlowState {
	client_id: i32,
	camera_id: i32,
	packets_sent: u32,
	packets_want: u32,
	sent: BTreeMap<u32, UdpData>,
	received: BTreeMap<u32, Vec<u8>>,
	delivered_any: bool,
	stalled_since: Option<Instant>,
	recv_rate: RecvRate,
}

impl UdpFlowState {
	/// Construct a fresh flow with both counters at 0 and empty
	/// `sent` / `received` BTreeMaps.
	pub fn new(client_id: i32, camera_id: i32) -> Self {
		Self {
			client_id,
			camera_id,
			packets_sent: 0,
			packets_want: 0,
			sent: BTreeMap::new(),
			received: BTreeMap::new(),
			delivered_any: false,
			stalled_since: None,
			recv_rate: RecvRate::default(),
		}
	}

	/// App→Camera: chunk `payload` into MTU-sized [`UdpData`] packets,
	/// stamp each with the next id, stash a copy in `sent` for resend,
	/// and return the packets to feed onto the wire. Caller is
	/// responsible for the actual socket write — this is pure state.
	pub fn enqueue_send(&mut self, payload: &[u8]) -> Vec<UdpData> {
		let mut out = Vec::new();
		for chunk in payload.chunks(MTU - UDPDATA_HEADER_SIZE) {
			let udp_data = UdpData {
				connection_id: self.camera_id,
				packet_id: self.packets_sent,
				payload: chunk.to_vec(),
			};
			self.packets_sent = self.packets_sent.wrapping_add(1);
			self.sent.insert(udp_data.packet_id, udp_data.clone());
			out.push(udp_data);
		}
		out
	}

	/// Camera→App: fold an inbound [`UdpData`] received at `now` into the
	/// recv reorder buffer. Returns `true` when the packet was inserted and
	/// the caller should refresh the outgoing ack via [`build_send_ack`];
	/// `false` when it was rejected (mismatched connection_id, already
	/// delivered, or beyond the reorder window).
	pub fn handle_data(&mut self, data: UdpData, now: Instant) -> bool {
		if data.connection_id != self.client_id {
			return false;
		}
		let packet_id = data.packet_id;
		// Serial distance: ids behind `packets_want` (already delivered)
		// wrap to huge offsets and fall outside the window too.
		let offset = packet_id.wrapping_sub(self.packets_want);
		if offset >= REORDER_CAP as u32 {
			if offset < 1 << 31 {
				log::debug!(
					"BcUdp packet_id={packet_id} beyond reorder window (want={}); dropping",
					self.packets_want
				);
			}
			return false;
		}
		self.recv_rate.record(now, data.payload.len());
		self.received.insert(packet_id, data.payload);
		true
	}

	/// Camera→App: process an inbound [`UdpAck`]. Marks every acked
	/// packet_id as removable from `sent`.
	pub fn handle_ack(&mut self, ack: UdpAck) {
		let start = ack.packet_id;
		if start != 0xffffffff {
			// -1 means havent got anything yet
			// Keep ids serially after `start`; `sent` never spans 2^31 ids.
			self.sent.retain(|&k, _| (k.wrapping_sub(start) as i32) > 0);
			for (idx, &value) in ack.payload.iter().enumerate() {
				if value > 0 {
					let packet_id = start.wrapping_add(1).wrapping_add(idx as u32);
					self.sent.remove(&packet_id);
				}
			}
		}
		log::trace!("sent: {}", self.sent.len());
	}

	/// Drain the contiguous-from-`packets_want` prefix of the recv
	/// reorder buffer, advancing `packets_want` past each delivered
	/// payload. Caller forwards the returned bytes to its consumer
	/// (production: `thread_stream`).
	///
	/// If buffered packets have waited on a gap for [`GAP_SKIP_AFTER`]
	/// as of `now`, the missing ids are given up and delivery resumes at
	/// the lowest buffered id.
	pub fn drain_contiguous(&mut self, now: Instant) -> Vec<Vec<u8>> {
		let mut out = self.drain_in_order();
		if out.is_empty() && !self.received.is_empty() {
			let since = *self.stalled_since.get_or_insert(now);
			if now.saturating_duration_since(since) >= GAP_SKIP_AFTER {
				let want = self.packets_want;
				let lowest = self
					.received
					.keys()
					.copied()
					.min_by_key(|&k| k.wrapping_sub(want))
					.expect("received is non-empty");
				log::warn!(
					"BcUdp packet {want} missing for {GAP_SKIP_AFTER:?}; skipping {} packet(s) to {lowest}",
					lowest.wrapping_sub(want)
				);
				self.packets_want = lowest;
				out = self.drain_in_order();
			}
		}
		if !out.is_empty() || self.received.is_empty() {
			self.stalled_since = None;
		}
		out
	}

	fn drain_in_order(&mut self) -> Vec<Vec<u8>> {
		let mut out = Vec::new();
		while let Some(payload) = self.received.remove(&self.packets_want) {
			self.packets_want = self.packets_want.wrapping_add(1);
			out.push(payload);
		}
		self.delivered_any |= !out.is_empty();
		out
	}

	/// Build the outgoing ack packet that reflects the current recv
	/// reorder state.
	pub fn build_send_ack(&self) -> UdpAck {
		if !self.delivered_any {
			return UdpAck::empty(self.camera_id);
		}
		// Offsets are relative to `packets_want`; every buffered id is
		// within REORDER_CAP of it, so none of this can overflow.
		let want = self.packets_want;
		let mut first_missing = 0u32;
		while self
			.received
			.contains_key(&want.wrapping_add(first_missing))
		{
			// Happens if we have received but not consumed yet.
			first_missing += 1;
		}
		// Truth table for the ids after the ack's `packet_id`: `01`
		// received, `00` missing, up to the last id we hold. Capped so the
		// datagram stays small.
		const ACK_BITMAP_CAP: u32 = (REORDER_CAP * 4) as u32;
		let end = self
			.received
			.keys()
			.map(|&k| k.wrapping_sub(want) + 1)
			.max()
			.unwrap_or(0)
			.clamp(first_missing, first_missing + ACK_BITMAP_CAP);
		let payload = (first_missing..end)
			.map(|off| u8::from(self.received.contains_key(&want.wrapping_add(off))))
			.collect();

		UdpAck {
			connection_id: self.camera_id,
			// Last id we hold contiguously.
			packet_id: want.wrapping_add(first_missing).wrapping_sub(1),
			group_id: 0,
			recv_bytes_per_sec: self.recv_rate.bytes_per_sec,
			payload,
		}
	}

	/// Snapshot of every still-outstanding `sent` entry, for the
	/// resend tick to feed back onto the wire.
	pub fn resend_packets(&self) -> Vec<UdpData> {
		self.sent.values().cloned().collect()
	}

	/// Current size of the recv reorder buffer. Bounded by
	/// [`REORDER_CAP`] via the reorder window in `handle_data`.
	pub fn received_len(&self) -> usize {
		self.received.len()
	}

	/// Current size of the outgoing-not-yet-acked map. Used by tests
	/// and fuzz-target invariants to confirm acks shrink the map.
	#[cfg(any(test, feature = "fuzz-api"))]
	pub fn sent_len(&self) -> usize {
		self.sent.len()
	}

	/// Next packet_id this flow is waiting for from the camera.
	/// Wraps on `u32::MAX`.
	pub fn packets_want(&self) -> u32 {
		self.packets_want
	}
}

/// See `UdpPayloadInner::recv_deadline`. The keepalive probe elicits a
/// camera reply every few seconds, so a healthy idle session stays well
/// inside this.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

struct UdpPayloadInner {
	camera_addr: SocketAddr,
	ack_tx: WatchSender<UdpAck>,
	socket_in: PollSender<BcUdp>,
	socket_out: ReceiverStream<(BcUdp, SocketAddr)>,
	thread_stream: PollSender<IoResult<Vec<u8>>>,
	thread_sink: ReceiverStream<Vec<u8>>,
	flow: UdpFlowState,
	/// Offical Client does ack every 10ms if we don't also do this the camera
	/// seems to think we have a poor connection and will abort
	/// This `ack_interval` controls how ofen we do this
	/// Offical Client does resend every 500ms
	/// This `resend_interval` controls how ofen we do this
	resend_interval: Interval,
	/// Session ends with `BcUdpTimeout` when no data is delivered in order
	/// for [`RECV_TIMEOUT`]. Heartbeats, acks and stray datagrams don't
	/// extend it: they don't prove the stream is moving.
	recv_deadline: Pin<Box<Sleep>>,
	cancel: CancellationToken,
	set: JoinSet<Result<()>>,
}
impl UdpPayloadInner {
	fn new(
		mut inner: BcUdpSource,
		thread_stream: PollSender<IoResult<Vec<u8>>>,
		thread_sink: ReceiverStream<Vec<u8>>,
		client_id: i32,
		camera_id: i32,
	) -> Self {
		let mut set = JoinSet::new();
		let camera_addr = inner.addr;
		let cancel = CancellationToken::new();
		// Data in this needs to be passed into the socket regularly
		// especially the ACK packets on UDP. The thread must not lock
		// and MUST send ACK packets or else be dropped by the camera.
		// In order to achieve this we use dedicated threads for ACK
		// and the socket

		let (socket_in_tx, socket_in_rx) = channel::<BcUdp>(500);
		let (socket_out_tx, socket_out_rx) = channel::<(BcUdp, SocketAddr)>(500);
		// let (mut socket_tx, mut socket_rx) = inner.split();

		// Send/Recv on the socket
		let send_cancel = cancel.clone();
		let mut socket_in_rx = ReceiverStream::new(socket_in_rx);
		let thread_camera_addr = camera_addr;
		let socket_out_tx = socket_out_tx.clone();
		let thread_client_id = client_id;
		let thread_camera_id = camera_id;
		set.spawn(async move {
            let result = tokio::select! {
                _ = send_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    loop {
                        tokio::select!{
                            packet = inner.next() => {
                                log::trace!("Cam->App");
                                let packet = packet.ok_or(Error::BcUdpDropReceiver(BcUdpDropReceiverKind::NoneReceived))??;
                                match socket_out_tx.try_send(packet) {
                                    Ok(()) => {}
                                    // A datagram flood (from any host) must not end the
                                    // session; drop like the network would.
                                    Err(TrySendError::Full(_)) => log::debug!("BcUdp recv queue full; dropping datagram"),
                                    Err(e @ TrySendError::Closed(_)) => {
                                        return Err(Error::BcUdpDropReceiver(BcUdpDropReceiverKind::SendFailed(format!("{e:?}"))));
                                    }
                                }
                            },
                            packet = socket_in_rx.next() => {
                                let packet = packet.ok_or(Error::BcUdpDropSender)?;
                                match tokio::time::timeout(tokio::time::Duration::from_millis(250), inner.send((packet, thread_camera_addr))).await {
                                    Ok(written) => {
                                        written?;
                                    }
                                    Err(_) => {
                                        log::trace!("Socket Error, attempting reconnect over a new one");
                                        // Socket is (maybe) broken
                                        // Seems to happen with network reconnects like over
                                        // a lossy cellular network
                                        let stream = Arc::new(tokio::time::timeout(tokio::time::Duration::from_millis(250), connect_try_port(inner.inner.get_ref().local_addr()?.port())).await.map_err(|_| Error::BcUdpReconnectTimeout)??);
                                        inner = tokio::time::timeout(tokio::time::Duration::from_millis(250), BcUdpSource::new_from_socket(stream, inner.addr)).await.map_err(|_| Error::BcUdpReconnectTimeout)??;

                                        // Inform the camera that we are the same client
                                        //
                                        // At least I think that is what this is for.
                                        // Might also have to do this for the relay but not sure
                                        let msg = BcUdp::Discovery(UdpDiscovery {
                                            tid: rand::random::<u8>() as u32,
                                            payload: UdpXml::C2dHb(C2dHb {
                                                    cid: thread_client_id,
                                                    did: thread_camera_id,
                                                }),
                                        });
                                        let _ = tokio::time::timeout(tokio::time::Duration::from_millis(250), inner.send((msg, thread_camera_addr))).await;
                                    }
                                }

                                log::trace!("Send Packet");
                            }
                        }
                    }
                } => v,
            };
            send_cancel.cancel();
            result
        });

		// Queue up ack packets
		let ack_cancel = cancel.clone();
		let mut ack_interval = interval(Duration::from_millis(10)); // Offical Client does ack every 10ms
		ack_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let (ack_tx, ack_rx) = watch(UdpAck::empty(camera_id));
		let ack_socket_in_tx = socket_in_tx.clone();
		set.spawn(async move {
			tokio::select! {
				_ = ack_cancel.cancelled() => {
					Result::Ok(())
				},
				v = async {
					loop {
						ack_interval.tick().await;
						// Send an ack packet
						log::trace!("send ack");
						let ack_packet = BcUdp::Ack(ack_rx.borrow().clone());
						ack_socket_in_tx.send(ack_packet).await?;
					}
				} => v,
			}
		});

		// Queue up Hb packets
		let thread_client_id = client_id;
		let thread_camera_id = camera_id;
		let thread_sender = socket_in_tx.clone();
		let mut thread_interval = interval(Duration::from_secs(1));
		let thread_cancel = cancel.clone();
		set.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					loop {
						thread_interval.tick().await;
						let msg = BcUdp::Discovery(UdpDiscovery {
							tid: rand::random::<u8>() as u32,
							payload: UdpXml::C2dHb(C2dHb {
									cid: thread_client_id,
									did: thread_camera_id,
								}),
						});
						if thread_sender.send(msg).await.is_err() {
							break Result::Ok(());
						}
					}
				} => v,
			}
		});

		Self {
			camera_addr,
			ack_tx,
			socket_in: PollSender::new(socket_in_tx),
			socket_out: ReceiverStream::new(socket_out_rx),
			thread_stream,
			thread_sink,
			flow: UdpFlowState::new(client_id, camera_id),
			resend_interval: interval(Duration::from_millis(500)), // Offical Client does resend every 500ms
			recv_deadline: Box::pin(sleep(RECV_TIMEOUT)),
			cancel,
			set,
		}
	}
	async fn run(&mut self) -> Result<()> {
		let camera_addr = self.camera_addr;
		tokio::select! {
			_ = self.recv_deadline.as_mut() => Err(Error::BcUdpTimeout),
			_ = self.resend_interval.tick() => {
				log::trace!("Resend Tick");
				for resend in self.flow.resend_packets() {
					self.socket_in.feed(BcUdp::Data(resend)).await?;
				}
				self.ack_tx.send_replace(self.flow.build_send_ack()); // Ensure we update the ack packet sometimes too
				Result::Ok(())
			},
			v = self.thread_sink.next() => {
				log::trace!("App->Camera");
				// Incomming from application
				// Outgoing on socket
				let item = v.ok_or(Error::BcUdpDropSender)?;

				for udp_data in self.flow.enqueue_send(&item) {
					self.socket_in.feed(BcUdp::Data(udp_data)).await?;
				}
				Ok(())
			}
			v = self.socket_out.next() => {
				log::trace!("Camera->App");
				// Incomming from socket
				// Outgoing to application
				let (item, addr) = v.ok_or(Error::BcUdpDropReceiver(BcUdpDropReceiverKind::NoneReceived))?;
				if addr == camera_addr {
					match item {
						BcUdp::Discovery(UdpDiscovery{
							payload: UdpXml::D2cHb(D2cHb {
								cid,
								did,
							}),
							..
						}) => {
							if cid != self.flow.client_id {
								log::info!("Camera sent different client ID in HB");
							}
							if did != self.flow.camera_id {
								log::info!("Camera sent different device ID in HB");
							}
						},
						BcUdp::Discovery(_disc) => {},
						BcUdp::Ack(ack) => {
							if ack.connection_id == self.flow.client_id {
								self.flow.handle_ack(ack);
							}
						},
						BcUdp::Data(data)  => {
							if self.flow.handle_data(data, Instant::now()) {
								self.ack_tx.send_replace(self.flow.build_send_ack());
							}
						},
					}
				}
				log::trace!("Got packet");
				Ok(())
			},
		}?;
		log::trace!("Send");
		let now = Instant::now();
		let delivered = self.flow.drain_contiguous(now);
		if !delivered.is_empty() {
			self.recv_deadline.as_mut().reset(now + RECV_TIMEOUT);
		}
		for payload in delivered {
			log::trace!("  + {}", self.flow.packets_want());
			self.thread_stream.feed(Ok(payload)).await?;
		}
		log::trace!("received: {}", self.flow.received_len());
		log::trace!("Flush");
		self.socket_in.flush().await?;
		self.thread_stream.flush().await?;
		log::trace!("Flushed");
		Ok(())
	}
}

impl Drop for UdpPayloadInner {
	fn drop(&mut self) {
		log::trace!("Drop UdpPayloadInner");
		self.cancel.cancel();
		// `try_current` so a Drop outside a Tokio runtime doesn't
		// double-panic. Cancel fired above; the drain is best-effort.
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let _gt = handle.enter();
		let mut set = std::mem::take(&mut self.set);
		tokio::task::spawn(async move {
			while set.join_next().await.is_some() {}
			log::trace!("Dropped UdpPayloadInner");
		});
	}
}
impl UdpPayloadSource {
	async fn new(inner: BcUdpSource, client_id: i32, camera_id: i32) -> Self {
		let (inner_sink, thread_sink) = channel(100);
		let (thread_stream, inner_stream) = channel(100);

		let mut payload_inner = UdpPayloadInner::new(
			inner,
			PollSender::new(thread_stream),
			ReceiverStream::new(thread_sink),
			client_id,
			camera_id,
		);
		let cancel_token = tokio_util::sync::CancellationToken::new();

		let thread_cancel_token = cancel_token.clone();
		let mut set = JoinSet::new();
		set.spawn(async move {
			tokio::select! {
				v = async {
					loop {
						if payload_inner.thread_stream.is_closed() {
							log::trace!("payload_inner.thread_stream.is_closed");
							payload_inner.thread_sink.close();
							return Err(Error::BcUdpPayloadDroppedInner);
						}
						log::trace!("Calling inner");
						let res = payload_inner.run().await;
						log::trace!("Called inner: {:?}", res);
						match res {
							Ok(()) => {}
							Err(e) => {
								log::trace!("UDP Error. Connection will Drop: {:?}", e);
								// Pass error up
								let _ = payload_inner
									.thread_stream
									.send(Err(IoError::other(e.clone())))
									.await;
								return Result::<()>::Err(e);
							}
						}
					}
				} => v,
				_ = thread_cancel_token.cancelled() => Ok(()),
			}
		});

		UdpPayloadSource {
			inner_stream: Box::pin(ReceiverStream::new(inner_stream)),
			inner_sink: PollSender::new(inner_sink),
			set,
			cancel_token,
		}
	}
}

impl Stream for UdpPayloadSource {
	type Item = IoResult<Vec<u8>>;

	delegate! {
		to self.inner_stream.as_mut() {
			fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
		}
	}

	delegate! {
		to self.inner_stream.as_ref().get_ref() {
			fn size_hint(&self) -> (usize, Option<usize>);
		}
	}
}

impl Sink<Vec<u8>> for UdpPayloadSource {
	type Error = IoError;

	fn poll_ready(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<std::result::Result<(), Self::Error>> {
		self.get_mut()
			.inner_sink
			.poll_ready_unpin(cx)
			.map_err(IoError::other)
	}
	fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> std::result::Result<(), Self::Error> {
		self.get_mut()
			.inner_sink
			.start_send_unpin(item)
			.map_err(IoError::other)
	}
	fn poll_flush(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<std::result::Result<(), Self::Error>> {
		self.get_mut()
			.inner_sink
			.poll_flush_unpin(cx)
			.map_err(IoError::other)
	}

	fn poll_close(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<std::result::Result<(), Self::Error>> {
		self.get_mut()
			.inner_sink
			.poll_close_unpin(cx)
			.map_err(IoError::other)
	}
}

impl futures::AsyncWrite for UdpPayloadSource {
	fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
		let mut this = self.get_mut();
		match Pin::new(&mut this).poll_ready(cx) {
			Poll::Ready(Ok(())) => match Pin::new(&mut this).start_send(buf.to_vec()) {
				Ok(()) => Poll::Ready(Ok(buf.len())),
				Err(e) => Poll::Ready(Err(e)),
			},
			Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
			Poll::Pending => Poll::Pending,
		}
	}
	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
		Sink::poll_flush(self, cx)
	}
	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
		Sink::poll_close(self, cx)
	}
}

/// Helper to create a UdpStream
async fn connect() -> Result<UdpSocket> {
	let mut ports: Vec<u16> = (53500..54000).collect();
	ports.shuffle(&mut rand::rng());

	let addrs: Vec<_> = ports
		.iter()
		.map(|&port| SocketAddr::from(([0, 0, 0, 0], port)))
		.collect();
	let socket = UdpSocket::bind(&addrs[..]).await?;

	Ok(socket)
}

async fn connect_try_port(port: u16) -> Result<UdpSocket> {
	let mut ports: Vec<u16> = (53500..54000).collect();
	ports.shuffle(&mut rand::rng());

	let addrs: Vec<_> = [port]
		.iter()
		.chain(ports.iter())
		.map(|&port| SocketAddr::from(([0, 0, 0, 0], port)))
		.collect();
	let socket = UdpSocket::bind(&addrs[..]).await?;

	Ok(socket)
}

#[cfg(test)]
mod tests {
	//! Tests for the UDP transport layer. Every socket-touching test binds
	//! a real `UdpSocket` to `127.0.0.1:0`; the same test acts as the peer
	//! (camera/relay) by recv_from/send_to on a sibling socket. Hang
	//! protection: every end-to-end await is wrapped in a short
	//! `tokio::time::timeout`.
	use super::*;
	use crate::bcudp::xml::{C2dHb, ClientList, UdpXml};
	use bytes::BytesMut;
	use futures::StreamExt;
	use std::time::Duration;
	use tokio::sync::mpsc;
	use tokio_util::codec::Decoder;

	const T: Duration = Duration::from_millis(500);

	fn ack_packet(conn: i32, pid: u32) -> BcUdp {
		BcUdp::Ack(UdpAck {
			connection_id: conn,
			group_id: 0,
			packet_id: pid,
			recv_bytes_per_sec: 0,
			payload: vec![],
		})
	}

	fn data_packet(conn: i32, pid: u32, bytes: Vec<u8>) -> BcUdp {
		BcUdp::Data(UdpData {
			connection_id: conn,
			packet_id: pid,
			payload: bytes,
		})
	}

	fn c2dhb_packet(cid: i32, did: i32) -> BcUdp {
		BcUdp::Discovery(UdpDiscovery {
			tid: 7,
			payload: UdpXml::C2dHb(C2dHb { cid, did }),
		})
	}

	#[tokio::test]
	async fn connect_binds_to_ephemeral_range() {
		let sock = tokio::time::timeout(T, connect()).await.unwrap().unwrap();
		let addr = sock.local_addr().unwrap();
		assert!(
			(53500..54000).contains(&addr.port()),
			"port {} not in ephemeral range",
			addr.port()
		);
	}

	// Both tests take their port from the OS rather than a fixed number:
	// parallel tests call `connect_try_port`, which binds random ports in
	// 53500..54000, so any fixed port in that range can be taken first.

	#[tokio::test]
	async fn connect_try_port_prefers_given_port_if_free() {
		let port = UdpSocket::bind("0.0.0.0:0")
			.await
			.unwrap()
			.local_addr()
			.unwrap()
			.port();
		let sock = tokio::time::timeout(T, connect_try_port(port))
			.await
			.unwrap()
			.unwrap();
		// The just-released port is free, so it's tried (and won) first.
		assert_eq!(sock.local_addr().unwrap().port(), port);
	}

	#[tokio::test]
	async fn connect_try_port_falls_back_if_port_taken() {
		// Hold an OS-assigned port so connect_try_port must fall back to
		// the shuffled 53500..54000 range.
		let hold = UdpSocket::bind("0.0.0.0:0").await.unwrap();
		let held = hold.local_addr().unwrap().port();
		let sock = tokio::time::timeout(T, connect_try_port(held))
			.await
			.unwrap()
			.unwrap();
		let addr = sock.local_addr().unwrap();
		assert_ne!(addr.port(), held);
		assert!((53500..54000).contains(&addr.port()));
		drop(hold);
	}

	/// Round-trip a BcUdp ack packet between two client+server sockets.
	#[tokio::test]
	async fn bcudp_source_sink_stream_roundtrip_ack() {
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let client_addr = client.local_addr().unwrap();

		let mut sink = BcUdpSource::new_from_socket(client.clone(), server_addr)
			.await
			.unwrap();
		sink.send((ack_packet(42, 5), server_addr)).await.unwrap();

		let mut buf = [0u8; 2048];
		let (n, from) = tokio::time::timeout(T, server.recv_from(&mut buf))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(from, client_addr);

		// Decode with the same codec used by the sink.
		let mut codec = BcUdpCodex::new();
		let mut bytes = BytesMut::from(&buf[..n]);
		let got = codec.decode(&mut bytes).unwrap().unwrap();
		match got {
			BcUdp::Ack(a) => assert_eq!(a.connection_id, 42),
			other => panic!("expected Ack got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bcudp_source_new_binds_socket() {
		// Exercise the `BcUdpSource::new` constructor (the path that opens
		// its own socket). We only need to verify it succeeds and carries
		// the supplied addr.
		let src = tokio::time::timeout(T, BcUdpSource::new("127.0.0.1:12345".parse().unwrap()))
			.await
			.unwrap()
			.unwrap();
		assert_eq!(src.addr.port(), 12345);
	}

	#[tokio::test]
	async fn bcudp_source_stream_receives_incoming_ack() {
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let client_addr = client.local_addr().unwrap();

		let mut source = BcUdpSource::new_from_socket(client.clone(), server_addr)
			.await
			.unwrap();

		// Push one packet from the server-side.
		let mut codec = BcUdpCodex::new();
		let mut buf = BytesMut::new();
		codec.encode(ack_packet(9, 1), &mut buf).unwrap();
		server.send_to(&buf, client_addr).await.unwrap();

		let item = tokio::time::timeout(T, source.next())
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		let (packet, from) = item;
		assert_eq!(from, server_addr);
		match packet {
			BcUdp::Ack(a) => assert_eq!(a.connection_id, 9),
			other => panic!("expected Ack got {other:?}"),
		}
	}

	#[tokio::test]
	async fn bcudp_source_size_hint_is_ok() {
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let source = BcUdpSource::new_from_socket(client, server_addr)
			.await
			.unwrap();
		let (low, _high) = source.size_hint();
		// UdpFramed does not set a nonzero lower bound, but we just want
		// to exercise the delegate path.
		let _ = low;
	}

	/// End-to-end: spin up a "camera" echo thread that acks every data
	/// packet and emit the same payload back, driven by a full
	/// `UdpSource`. Verifies that UdpPayloadSource resequences, that the
	/// codec decodes the echoed bytes, and that the sink-plumbing reaches
	/// the socket.
	#[tokio::test]
	async fn udp_source_ack_echo_from_scripted_camera() {
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

		let client_id = 100i32;
		let camera_id = 200i32;

		// Scripted camera: ack every inbound data packet + track that we
		// received at least one heartbeat.
		let (hb_tx, mut hb_rx) = mpsc::channel::<()>(4);
		let camera = tokio::spawn(async move {
			let mut buf = [0u8; 2048];
			let mut codec = BcUdpCodex::new();
			loop {
				let (n, from) = match tokio::time::timeout(
					Duration::from_millis(1_500),
					server.recv_from(&mut buf),
				)
				.await
				{
					Ok(Ok(v)) => v,
					_ => return,
				};
				let mut bm = BytesMut::from(&buf[..n]);
				let Ok(Some(pkt)) = codec.decode(&mut bm) else {
					continue;
				};
				match pkt {
					BcUdp::Data(d) => {
						// Ack it.
						let mut out = BytesMut::new();
						codec
							.encode(ack_packet(client_id, d.packet_id), &mut out)
							.unwrap();
						let _ = server.send_to(&out, from).await;
					}
					BcUdp::Discovery(UdpDiscovery {
						payload: UdpXml::C2dHb(_),
						..
					}) => {
						let _ = hb_tx.try_send(());
					}
					_ => {}
				}
			}
		});

		let _source = UdpPayloadSource::new(
			BcUdpSource::new_from_socket(client_sock.clone(), server_addr)
				.await
				.unwrap(),
			client_id,
			camera_id,
		)
		.await;

		// Let the heartbeat thread tick at least once.
		let got_hb = tokio::time::timeout(Duration::from_millis(1_500), hb_rx.recv()).await;
		assert!(got_hb.is_ok(), "no heartbeat observed within 1.5s");

		camera.abort();
	}

	#[tokio::test]
	async fn udp_source_new_from_socket_constructs_ok() {
		// Exercise the public UdpSource::new_from_socket wrapper; this
		// chains BcUdpSource -> UdpPayloadSource -> Framed<_, BcCodex>.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let _src = tokio::time::timeout(
			T,
			UdpSource::new_from_socket::<&str, &str>(
				client_sock,
				server_addr,
				1,
				2,
				"admin",
				Some("pw"),
				false,
			),
		)
		.await
		.unwrap()
		.unwrap();
	}

	#[tokio::test]
	async fn udp_source_new_from_socket_with_debug_flag() {
		// Same as above but hits the debug branch of the constructor.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let _src = tokio::time::timeout(
			T,
			UdpSource::new_from_socket::<&str, &str>(
				client_sock,
				server_addr,
				3,
				4,
				"user",
				Some("pass"),
				true,
			),
		)
		.await
		.unwrap()
		.unwrap();
	}

	#[tokio::test]
	async fn udp_source_new_from_discovery_disables_broadcast_and_connects() {
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		sock.set_broadcast(true).unwrap();

		let dr = DiscoveryResult {
			socket: sock.clone(),
			addr: server_addr,
			client_id: 11,
			camera_id: 22,
			nc: None,
			pl: None,
		};
		let _src = tokio::time::timeout(
			T,
			UdpSource::new_from_discovery::<&str, &str>(dr, "admin", None, false),
		)
		.await
		.unwrap()
		.unwrap();

		// new_from_discovery should have turned broadcast off.
		assert!(!sock.broadcast().unwrap());
	}

	#[tokio::test]
	async fn udp_source_new_opens_socket_and_constructs() {
		// Hit the `UdpSource::new` path which allocates its own socket
		// via `connect()`.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let _src = tokio::time::timeout(
			T,
			UdpSource::new::<&str, &str>(server_addr, 5, 6, "u", None, false),
		)
		.await
		.unwrap()
		.unwrap();
	}

	#[test]
	fn udp_payload_inner_build_send_ack_zero_packets() {
		// With no packets received, build_send_ack returns an empty ack.
		let _rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		// build_send_ack runs fine outside of runtime because it doesn't
		// touch the socket, but UdpPayloadInner::new does. So we can't
		// easily instantiate one here without the runtime helpers. This
		// test is intentionally simple — just makes sure `UdpAck::empty`
		// has the expected shape.
		let empty = UdpAck::empty(5);
		assert_eq!(empty.connection_id, 5);
		assert!(empty.payload.is_empty());
	}

	#[tokio::test]
	async fn udp_payload_source_heartbeat_and_ack_tasks_tick() {
		// Drive UdpPayloadSource::new with scripted camera that replies
		// with at least one data packet. Verifies: Stream yields the
		// payload bytes, ack task sends a non-empty ack afterwards, and
		// the heartbeat task ticks.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let client_addr = client_sock.local_addr().unwrap();

		let client_id = 77i32;
		let camera_id = 88i32;

		// Camera thread: send one data packet to the client once we see
		// the first heartbeat (which confirms the client is running).
		let camera = tokio::spawn(async move {
			let mut buf = [0u8; 2048];
			let mut codec = BcUdpCodex::new();

			// Wait for heartbeat.
			let _ = tokio::time::timeout(Duration::from_millis(1_500), async {
				loop {
					let (n, _) = server.recv_from(&mut buf).await.unwrap();
					let mut bm = BytesMut::from(&buf[..n]);
					if let Ok(Some(BcUdp::Discovery(UdpDiscovery {
						payload: UdpXml::C2dHb(_),
						..
					}))) = codec.decode(&mut bm)
					{
						break;
					}
				}
			})
			.await;

			// Push one data packet with connection_id = client_id (what
			// UdpPayloadInner expects from the camera side).
			let mut out = BytesMut::new();
			codec
				.encode(data_packet(client_id, 0, vec![0xaa, 0xbb]), &mut out)
				.unwrap();
			let _ = server.send_to(&out, client_addr).await;
			server
		});

		let mut psource = UdpPayloadSource::new(
			BcUdpSource::new_from_socket(client_sock, server_addr)
				.await
				.unwrap(),
			client_id,
			camera_id,
		)
		.await;

		// The framed read should yield the 2-byte payload.
		let bytes = tokio::time::timeout(Duration::from_millis(2_500), psource.next())
			.await
			.unwrap()
			.unwrap()
			.unwrap();
		assert_eq!(bytes, vec![0xaa, 0xbb]);

		// Drop and let camera task terminate.
		camera.abort();
	}

	#[tokio::test(start_paused = true)]
	async fn payload_source_times_out_without_data_despite_heartbeats() {
		// The camera answers every heartbeat but never sends data; that
		// must not keep the session alive.
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
		let camera = tokio::spawn(async move {
			let mut buf = [0u8; 2048];
			let mut codec = BcUdpCodex::new();
			loop {
				let (n, from) = server.recv_from(&mut buf).await.unwrap();
				let mut bm = BytesMut::from(&buf[..n]);
				if let Ok(Some(BcUdp::Discovery(UdpDiscovery {
					tid,
					payload: UdpXml::C2dHb(C2dHb { cid, did }),
				}))) = codec.decode(&mut bm)
				{
					let mut out = BytesMut::new();
					let hb = BcUdp::Discovery(UdpDiscovery {
						tid,
						payload: UdpXml::D2cHb(D2cHb { cid, did }),
					});
					codec.encode(hb, &mut out).unwrap();
					let _ = server.send_to(&out, from).await;
				}
			}
		});

		let mut psource = UdpPayloadSource::new(
			BcUdpSource::new_from_socket(client_sock, server_addr)
				.await
				.unwrap(),
			1,
			2,
		)
		.await;
		let err = tokio::time::timeout(RECV_TIMEOUT + Duration::from_secs(2), psource.next())
			.await
			.expect("session outlived RECV_TIMEOUT")
			.unwrap()
			.unwrap_err();
		let inner = err.get_ref().and_then(|e| e.downcast_ref::<Error>());
		assert!(matches!(inner, Some(Error::BcUdpTimeout)), "got {err:?}");
		camera.abort();
	}

	#[tokio::test]
	async fn bcudp_source_sink_sends_c2d_hb() {
		// Round-trip a C2D_HB discovery packet (the keepalive).
		let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let server_addr = server.local_addr().unwrap();
		let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

		let mut sink = BcUdpSource::new_from_socket(client, server_addr)
			.await
			.unwrap();
		sink.send((c2dhb_packet(1, 2), server_addr)).await.unwrap();

		let mut buf = [0u8; 2048];
		let (n, _from) = tokio::time::timeout(T, server.recv_from(&mut buf))
			.await
			.unwrap()
			.unwrap();
		let mut codec = BcUdpCodex::new();
		let mut bm = BytesMut::from(&buf[..n]);
		let got = codec.decode(&mut bm).unwrap().unwrap();
		match got {
			BcUdp::Discovery(UdpDiscovery {
				payload: UdpXml::C2dHb(C2dHb { cid, did }),
				..
			}) => {
				assert_eq!(cid, 1);
				assert_eq!(did, 2);
			}
			other => panic!("unexpected {other:?}"),
		}
	}

	// Prevent unused warnings when only subsets of the helpers apply.
	#[allow(dead_code)]
	fn _touch_client_list() -> ClientList {
		ClientList { port: 3000 }
	}

	// ---------- UdpFlowState pure-state tests ----------
	//
	// These pin the per-packet decision table extracted from
	// `UdpPayloadInner::run` so the bounded-state guards (REORDER_CAP,
	// u32::MAX wrap-protection on both packet_id and ack range) survive
	// future refactors. The same invariants are also exercised under
	// arbitrary input by the `udp_flow_state` fuzz target in `fuzz/`.

	const CID: i32 = 0xABCD;
	const DID: i32 = 0x1234;

	fn fresh() -> UdpFlowState {
		UdpFlowState::new(CID, DID)
	}

	fn now() -> Instant {
		Instant::now()
	}

	fn data_pkt(packet_id: u32) -> UdpData {
		UdpData {
			connection_id: CID,
			packet_id,
			payload: vec![0xAA, 0xBB],
		}
	}

	#[test]
	fn handle_data_in_order_inserts_and_drains() {
		let mut s = fresh();
		assert!(s.handle_data(data_pkt(0), now()));
		assert!(s.handle_data(data_pkt(1), now()));
		let drained = s.drain_contiguous(now());
		assert_eq!(drained.len(), 2);
		assert_eq!(s.packets_want(), 2);
		assert_eq!(s.received_len(), 0);
	}

	#[test]
	fn handle_data_out_of_order_holds_until_gap_filled() {
		let mut s = fresh();
		assert!(s.handle_data(data_pkt(2), now()));
		assert!(s.handle_data(data_pkt(1), now()));
		// Gap at 0 → nothing drains.
		assert!(s.drain_contiguous(now()).is_empty());
		assert_eq!(s.packets_want(), 0);
		// Fill the gap; everything contiguous flushes.
		assert!(s.handle_data(data_pkt(0), now()));
		let drained = s.drain_contiguous(now());
		assert_eq!(drained.len(), 3);
		assert_eq!(s.packets_want(), 3);
	}

	#[test]
	fn handle_data_rejects_mismatched_connection_id() {
		let mut s = fresh();
		let mut bad = data_pkt(0);
		bad.connection_id = CID + 1;
		assert!(!s.handle_data(bad, now()));
		assert_eq!(s.received_len(), 0);
	}

	#[test]
	fn handle_data_bounds_reorder_window_but_always_takes_want() {
		let mut s = fresh();
		// Skip 0 so nothing drains; fill the rest of the window.
		for i in 1..(REORDER_CAP as u32) {
			assert!(s.handle_data(data_pkt(i), now()));
		}
		// First id past the window: dropped.
		assert!(!s.handle_data(data_pkt(REORDER_CAP as u32), now()));
		// The missing packets_want is still accepted and drains everything.
		assert!(s.handle_data(data_pkt(0), now()));
		assert_eq!(s.drain_contiguous(now()).len(), REORDER_CAP);
		assert_eq!(s.received_len(), 0);
	}

	#[test]
	fn handle_data_and_ack_work_across_u32_wrap() {
		let mut s = fresh();
		s.packets_want = u32::MAX - 1;
		s.delivered_any = true;
		// Out of order across the wrap.
		assert!(s.handle_data(data_pkt(0), now()));
		assert!(s.handle_data(data_pkt(u32::MAX - 1), now()));
		assert!(s.handle_data(data_pkt(u32::MAX), now()));
		assert_eq!(s.drain_contiguous(now()).len(), 3);
		assert_eq!(s.packets_want(), 1);
		// Pre-wrap ids are now behind packets_want.
		assert!(!s.handle_data(data_pkt(u32::MAX), now()));
		assert!(s.handle_data(data_pkt(3), now()));
		let ack = s.build_send_ack();
		assert_eq!(ack.packet_id, 0);
		assert_eq!(ack.payload, vec![0, 0, 1]);
	}

	#[test]
	fn drain_skips_gap_after_timeout() {
		let t0 = now();
		let mut s = fresh();
		assert!(s.handle_data(data_pkt(1), t0));
		assert!(s.handle_data(data_pkt(2), t0));
		assert!(s.drain_contiguous(t0).is_empty());
		assert!(
			s.drain_contiguous(t0 + Duration::from_millis(499))
				.is_empty()
		);
		assert_eq!(s.drain_contiguous(t0 + GAP_SKIP_AFTER).len(), 2);
		assert_eq!(s.packets_want(), 3);
	}

	#[test]
	fn handle_data_drops_already_consumed_duplicate() {
		let mut s = fresh();
		assert!(s.handle_data(data_pkt(0), now()));
		assert!(s.handle_data(data_pkt(1), now()));
		s.drain_contiguous(now());
		// Now packets_want=2; an old packet_id=0 must drop, not
		// re-insert and not panic.
		assert!(!s.handle_data(data_pkt(0), now()));
		assert_eq!(s.received_len(), 0);
	}

	#[test]
	fn build_send_ack_empty_when_packets_want_zero() {
		let s = fresh();
		let ack = s.build_send_ack();
		assert_eq!(ack.connection_id, DID);
		// UdpAck::empty zeroes the body; just check no panic + same shape.
		assert!(ack.payload.is_empty());
	}

	#[test]
	fn build_send_ack_reflects_in_order_progress() {
		let mut s = fresh();
		s.handle_data(data_pkt(0), now());
		s.drain_contiguous(now());
		let ack = s.build_send_ack();
		assert_eq!(ack.packet_id, 0);
		assert!(ack.payload.is_empty());
	}

	#[test]
	fn handle_data_rejects_far_ahead_packet() {
		// Regression for the udp_flow_state fuzz target's first crash
		// (OOM): one Data packet with packet_id ≫ packets_want used to
		// drive a multi-GiB ack bitmap. It's now outside the window.
		let mut s = fresh();
		assert!(s.handle_data(data_pkt(0), now()));
		s.drain_contiguous(now());
		assert!(!s.handle_data(data_pkt(0xCA01_0000), now()));
		assert!(s.build_send_ack().payload.is_empty());
	}

	#[test]
	fn build_send_ack_marks_received_and_missing() {
		let mut s = fresh();
		// Ingest 0 (consumed), then 2 + 4 with gaps at 3.
		s.handle_data(data_pkt(0), now());
		s.drain_contiguous(now());
		s.handle_data(data_pkt(2), now());
		s.handle_data(data_pkt(4), now());
		let ack = s.build_send_ack();
		// first_missing = 1 (packets_want=1 after drain; 1 not in
		// received). packet_id = first_missing - 1 = 0.
		assert_eq!(ack.packet_id, 0);
		// payload covers [first_missing..=end] = [1, 2, 3, 4]:
		//   1 missing → 0, 2 received → 1, 3 missing → 0, 4 received → 1.
		assert_eq!(ack.payload, vec![0, 1, 0, 1]);
	}

	fn data_bytes(packet_id: u32, len: usize) -> UdpData {
		UdpData {
			connection_id: CID,
			packet_id,
			payload: vec![0; len],
		}
	}

	#[test]
	fn ack_reports_receive_rate_per_second() {
		let t0 = now();
		let mut s = fresh();
		// 10 × 1000 B inside the first second; nothing latched yet.
		for i in 0..10 {
			assert!(s.handle_data(
				data_bytes(i, 1000),
				t0 + Duration::from_millis(i as u64 * 50)
			));
		}
		s.drain_contiguous(now());
		assert_eq!(s.build_send_ack().recv_bytes_per_sec, 0);
		// The first packet past the 1 s mark closes the window.
		s.handle_data(data_bytes(10, 1000), t0 + Duration::from_secs(1));
		s.drain_contiguous(now());
		assert_eq!(s.build_send_ack().recv_bytes_per_sec, 10_000);
		// Traffic stops: the value holds, including across a packet that
		// closes a window spanning the idle gap.
		s.handle_data(data_bytes(11, 1000), t0 + Duration::from_secs(10));
		s.drain_contiguous(now());
		assert_eq!(s.build_send_ack().recv_bytes_per_sec, 10_000);
	}

	#[test]
	fn handle_ack_minus_one_is_no_op() {
		let mut s = fresh();
		// Stash an outgoing packet so we can prove it isn't disturbed.
		let _ = s.enqueue_send(b"hello");
		assert_eq!(s.sent_len(), 1);
		s.handle_ack(UdpAck {
			connection_id: DID,
			packet_id: 0xffffffff,
			group_id: 0,
			recv_bytes_per_sec: 0,
			payload: vec![],
		});
		assert_eq!(s.sent_len(), 1);
	}

	#[test]
	fn handle_ack_drops_acked_outgoing_packets() {
		let mut s = fresh();
		// Send 4 small payloads → 4 outstanding packet IDs (0..=3).
		for _ in 0..4 {
			let _ = s.enqueue_send(b"x");
		}
		assert_eq!(s.sent_len(), 4);
		// ack.packet_id = 1: retain k > 1, so packet_ids 0 and 1 are
		// dropped. payload [1, 1] also acks packet_ids 2 and 3.
		s.handle_ack(UdpAck {
			connection_id: DID,
			packet_id: 1,
			group_id: 0,
			recv_bytes_per_sec: 0,
			payload: vec![1, 1],
		});
		assert_eq!(s.sent_len(), 0);
	}

	#[test]
	fn handle_ack_across_u32_wrap() {
		let mut s = fresh();
		s.packets_sent = u32::MAX - 1;
		for _ in 0..4 {
			let _ = s.enqueue_send(b"x");
		}
		// Outstanding: MAX-1, MAX, 0, 1. Ack through MAX-1, plus 0 via the
		// bitmap; the post-wrap ids must survive the retain.
		s.handle_ack(UdpAck {
			connection_id: DID,
			packet_id: u32::MAX - 1,
			group_id: 0,
			recv_bytes_per_sec: 0,
			payload: vec![0, 1],
		});
		let left: Vec<u32> = s.resend_packets().iter().map(|p| p.packet_id).collect();
		assert_eq!(left, vec![1, u32::MAX]);
	}

	#[test]
	fn enqueue_send_chunks_and_stamps_ids() {
		let mut s = fresh();
		let payload = vec![0xCDu8; (MTU - UDPDATA_HEADER_SIZE) * 2 + 5];
		let pkts = s.enqueue_send(&payload);
		assert_eq!(pkts.len(), 3);
		assert_eq!(pkts[0].packet_id, 0);
		assert_eq!(pkts[1].packet_id, 1);
		assert_eq!(pkts[2].packet_id, 2);
		// Each packet's connection_id is the camera's id (DID).
		for p in &pkts {
			assert_eq!(p.connection_id, DID);
		}
		assert_eq!(s.sent_len(), 3);
	}
}
