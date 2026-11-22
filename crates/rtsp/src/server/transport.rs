//! Transport abstraction: TCP-interleaved vs UDP-unicast.
//!
//! `Transport` is the RTSP server's write-side interface for sending RTP
//! packets to a subscribed client. The server never emits RTCP. Two concrete implementations:
//! - `TcpInterleavedTransport` — writes framed packets on the RTSP TCP
//!   connection (RFC 7826 §14).
//! - `UdpUnicastTransport` — sends to a client-supplied address.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::server::udp_pool::{UdpPortLease, UdpPortPool};

/// Writes RTP/RTCP packets to a subscribed RTSP client.
///
/// Implementations are `Send + Sync` so the server can hand them into
/// tokio tasks via `Arc`.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
	/// Send an RTP packet.
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()>;
}

/// TCP-interleaved transport. Packets are prefixed with `$ ch len` and
/// written on a shared TCP stream (the same connection that carries RTSP).
pub struct TcpInterleavedTransport {
	writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
	channel_rtp: u8,
}

impl TcpInterleavedTransport {
	/// Create a new transport. `writer` is the shared half of the RTSP TCP
	/// connection's split write half.
	pub fn new(
		writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
		channel_rtp: u8,
	) -> Self {
		Self {
			writer,
			channel_rtp,
		}
	}

	async fn write_framed(&self, channel: u8, packet: &[u8]) -> io::Result<()> {
		if packet.len() > u16::MAX as usize {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"packet too large",
			));
		}
		// One buffer, one locked write: the `$ ch len` header and payload
		// must stay contiguous on the wire. Concurrent senders (parallel
		// video + audio dispatch tasks) would otherwise interleave bytes
		// mid-frame, and RFC 7826 §14 framing has no recovery once the
		// boundary is lost. The lock is held for at most one ≤ MTU packet.
		let mut frame = Vec::with_capacity(4 + packet.len());
		frame.extend_from_slice(&[b'$', channel]);
		frame.extend_from_slice(&(packet.len() as u16).to_be_bytes());
		frame.extend_from_slice(packet);
		write_all_bounded(&self.writer, &frame).await
	}
}

/// Upper bound on one write to the RTSP TCP stream. A client that stops
/// reading (zero TCP window) would otherwise park the writer, and with it
/// the session task and its camera wake lock, forever.
pub const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Write and flush `bytes` under the shared writer's lock, failing with
/// `TimedOut` after [`WRITE_TIMEOUT`]. The timeout starts once the lock is
/// held: waiting behind another writer is already bounded by that
/// writer's own timeout, and a slow-but-live client shouldn't fail a
/// queued writer.
pub(crate) async fn write_all_bounded(
	writer: &Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>,
	bytes: &[u8],
) -> io::Result<()> {
	let mut w = writer.lock().await;
	tokio::time::timeout(WRITE_TIMEOUT, async {
		w.write_all(bytes).await?;
		w.flush().await
	})
	.await
	.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "RTSP client write timed out"))?
}

#[async_trait::async_trait]
impl Transport for TcpInterleavedTransport {
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()> {
		self.write_framed(self.channel_rtp, packet).await
	}
}

/// UDP-unicast transport. Sends RTP to `client_rtp_addr` from a
/// server-side socket on a port held in `UdpPortLease`.
///
/// The [`UdpPortLease`] is held as a private field so the port pair is
/// guaranteed to stay reserved for the transport's lifetime; dropping the
/// transport drops the lease and returns the ports to the pool.
pub struct UdpUnicastTransport {
	rtp_sock: UdpSocket,
	/// Bound but never read or written: holding the advertised RTCP port
	/// keeps client receiver reports from drawing ICMP port-unreachable.
	_rtcp_sock: UdpSocket,
	client_rtp_addr: SocketAddr,
	/// Keeps the reserved UDP port pair held for this transport's lifetime.
	/// Also the source of truth for `server_rtp_port` / `server_rtcp_port`
	/// (both accessors read directly from the lease rather than calling
	/// `local_addr()` on the socket, so they're infallible and never
	/// surface the `0` "any port" sentinel).
	lease: UdpPortLease,
}

#[async_trait::async_trait]
impl Transport for UdpUnicastTransport {
	async fn send_rtp(&self, packet: &[u8]) -> io::Result<()> {
		self.rtp_sock.send_to(packet, self.client_rtp_addr).await?;
		Ok(())
	}
}

impl UdpUnicastTransport {
	/// Bind UDP sockets on the server's pooled ports and send RTP to
	/// `client_rtp_addr`.
	///
	/// Returns the transport which holds the port lease until dropped; when
	/// the transport is dropped the lease is released and the port pair is
	/// returned to `pool`.
	pub async fn bind(
		server_bind_ip: IpAddr,
		pool: Arc<UdpPortPool>,
		client_rtp_addr: SocketAddr,
	) -> io::Result<Self> {
		// A leased port can still be occupied at the OS level — a
		// TIME_WAIT socket from a prior session, or an unrelated process
		// on the same port. Rather than fail the SETUP on the first such
		// pair, step to the next one. Failed leases are parked (not
		// dropped) so `acquire` skips them until a pair binds; the parked
		// leases release when `_parked` drops at end of scope.
		let mut _parked = Vec::new();
		loop {
			let lease = pool.acquire().map_err(io::Error::other)?;

			match Self::try_bind_pair(server_bind_ip, &lease).await {
				Ok((rtp_sock, rtcp_sock)) => {
					return Ok(Self {
						rtp_sock,
						_rtcp_sock: rtcp_sock,
						client_rtp_addr,
						lease,
					});
				}
				Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
					// Pair unusable — park the lease so the next acquire
					// hands back a different port, and retry.
					_parked.push(lease);
				}
				// A non-conflict error (bad bind IP, host out of sockets)
				// won't be fixed by trying another port.
				Err(e) => return Err(e),
			}
		}
	}

	/// Bind the RTP + RTCP sockets for a single leased pair. Binding the
	/// RTCP socket only after the RTP socket succeeds means an
	/// `AddrInUse` on either surfaces as `AddrInUse` to the caller.
	async fn try_bind_pair(
		server_bind_ip: IpAddr,
		lease: &UdpPortLease,
	) -> io::Result<(UdpSocket, UdpSocket)> {
		let rtp_sock = UdpSocket::bind(SocketAddr::new(server_bind_ip, lease.rtp_port)).await?;
		let rtcp_sock = UdpSocket::bind(SocketAddr::new(server_bind_ip, lease.rtcp_port)).await?;
		Ok((rtp_sock, rtcp_sock))
	}

	/// Server-assigned RTP port (even). Read directly from the held lease,
	/// so this is infallible and never returns the `0` "any port" sentinel.
	pub fn server_rtp_port(&self) -> u16 {
		self.lease.rtp_port
	}

	/// Server-assigned RTCP port (odd). Read directly from the held lease,
	/// so this is infallible and never returns the `0` "any port" sentinel.
	pub fn server_rtcp_port(&self) -> u16 {
		self.lease.rtcp_port
	}
}

/// No-op [`Transport`] for unit tests that need a concrete `Arc<dyn
/// Transport>` but don't care about the wire side. `send_rtp` returns
/// `Ok(())` without touching any socket.
#[cfg(test)]
pub(crate) fn noop_transport_for_tests() -> std::sync::Arc<dyn Transport> {
	struct Noop;
	#[async_trait::async_trait]
	impl Transport for Noop {
		async fn send_rtp(&self, _packet: &[u8]) -> io::Result<()> {
			Ok(())
		}
	}
	std::sync::Arc::new(Noop)
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::io::AsyncReadExt;

	#[tokio::test]
	async fn tcp_interleaved_frames_packet_correctly() {
		let (client, server) = tokio::io::duplex(4096);
		let (mut client_read, _client_write) = tokio::io::split(client);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));

		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 0);
		let payload = b"\x80\x60\x00\x01rtp-payload";
		transport.send_rtp(payload).await.unwrap();

		// Read framed packet: $ (1 byte) + channel (1 byte) + len (2 bytes) + payload.
		let mut header = [0u8; 4];
		client_read.read_exact(&mut header).await.unwrap();
		assert_eq!(header[0], 0x24);
		assert_eq!(header[1], 0); // channel_rtp
		let len = u16::from_be_bytes([header[2], header[3]]) as usize;
		assert_eq!(len, payload.len());
		let mut body = vec![0u8; len];
		client_read.read_exact(&mut body).await.unwrap();
		assert_eq!(&body, payload);
	}

	#[tokio::test(start_paused = true)]
	async fn tcp_interleaved_write_times_out_when_client_stops_reading() {
		// 16-byte pipe the client never drains: a zero-window TCP peer.
		let (_client, server) = tokio::io::duplex(16);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));
		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 0);
		let started = tokio::time::Instant::now();
		let err = transport.send_rtp(&[0u8; 100]).await.unwrap_err();
		assert_eq!(err.kind(), io::ErrorKind::TimedOut);
		assert!(started.elapsed() >= WRITE_TIMEOUT);
	}

	#[tokio::test]
	async fn tcp_interleaved_rejects_oversize_packet() {
		let (_client, server) = tokio::io::duplex(4096);
		let writer: Arc<Mutex<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>> =
			Arc::new(Mutex::new(Box::new(server)));
		let transport = TcpInterleavedTransport::new(Arc::clone(&writer), 0);
		// One byte over u16::MAX - framing header has 2-byte length field.
		let too_big = vec![0u8; u16::MAX as usize + 1];
		let err = transport.send_rtp(&too_big).await.unwrap_err();
		assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
	}

	#[tokio::test]
	async fn noop_transport_returns_ok() {
		let t = noop_transport_for_tests();
		t.send_rtp(b"ignored").await.unwrap();
	}

	#[tokio::test]
	async fn udp_transport_sends_to_client_addr() {
		use std::net::Ipv4Addr;
		let client_rtp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let client_rtp_addr = client_rtp_sock.local_addr().unwrap();

		let pool = Arc::new(UdpPortPool::new());
		let transport = UdpUnicastTransport::bind(
			IpAddr::V4(Ipv4Addr::LOCALHOST),
			Arc::clone(&pool),
			client_rtp_addr,
		)
		.await
		.unwrap();

		transport.send_rtp(b"rtp-data").await.unwrap();

		let mut buf = [0u8; 64];
		let (n, _) = client_rtp_sock.recv_from(&mut buf).await.unwrap();
		assert_eq!(&buf[..n], b"rtp-data");
	}

	/// `bind` must step past a leased pair whose ports are already
	/// occupied at the OS level, rather than fail the SETUP. Occupy the
	/// pool's first RTP port, then bind and assert the transport landed
	/// on a later pair.
	#[tokio::test]
	async fn udp_bind_skips_occupied_pair() {
		use crate::server::udp_pool::POOL_START;
		use std::net::Ipv4Addr;

		let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
		// Squat on the even (RTP) port of the pool's first pair.
		let squatter = UdpSocket::bind(SocketAddr::new(ip, POOL_START)).await;
		// If something else already holds POOL_START this test can't set
		// up its precondition; skip rather than false-fail.
		let Ok(_squatter) = squatter else {
			eprintln!("POOL_START busy; skipping");
			return;
		};

		let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
		let caddr = client.local_addr().unwrap();

		let pool = Arc::new(UdpPortPool::new());
		let transport = UdpUnicastTransport::bind(ip, Arc::clone(&pool), caddr)
			.await
			.expect("bind must retry past the occupied first pair");

		assert_ne!(
			transport.server_rtp_port(),
			POOL_START,
			"transport must not claim the occupied port"
		);
		assert!(transport.server_rtp_port() > POOL_START);
	}
}
