use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rumqttc::{
	AsyncClient, Event, EventLoop, LastWill, MqttOptions, Packet, Publish, QoS, Request,
};
use serde::{Deserialize, Serialize};
use tracing;

use crate::error::MqttError;
use crate::topics;

/// TLS client certificate + key pair for mutual authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAuth {
	pub cert_pem: String,
	pub key_pem: String,
}

/// Configuration for connecting to an MQTT broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
	/// Broker hostname or IP address.
	pub broker_addr: String,
	/// Broker port (typically 1883 for plain, 8883 for TLS).
	pub port: u16,
	/// Optional (username, password) credentials.
	pub credentials: Option<(String, String)>,
	/// Optional CA certificate in PEM format for TLS.
	pub ca: Option<String>,
	/// Optional client certificate + key for mutual TLS.
	pub client_auth: Option<ClientAuth>,
}

impl MqttConfig {
	/// Build `rumqttc::MqttOptions` from this configuration.
	pub fn to_mqtt_options(&self, client_id: &str) -> MqttOptions {
		let mut opts = MqttOptions::new(client_id, &self.broker_addr, self.port);
		opts.set_keep_alive(Duration::from_secs(30));
		// rumqttc 0.25 defaults both incoming and outgoing max packet size
		// to 10 KiB. `status/preview` carries a base64-encoded camera JPEG
		// that is routinely 400 KiB–1.5 MiB (larger for 4K cameras), so the
		// default refuses every publish with:
		//   "Cannot send packet of size 'N'. It's greater than the broker's
		//    maximum packet size of: '10240'"
		// Neolink's Rust client raises this to fit real cameras; the broker
		// typically accepts the full payload. 16 MiB is conservative
		// headroom without being absurd.
		opts.set_max_packet_size(16 * 1024 * 1024, 16 * 1024 * 1024);

		if let Some((ref user, ref pass)) = self.credentials {
			opts.set_credentials(user, pass);
		}

		// TLS configuration if CA is provided
		if let Some(ref ca_pem) = self.ca {
			let mut tls_config = rumqttc::TlsConfiguration::Simple {
				ca: ca_pem.as_bytes().to_vec(),
				alpn: None,
				client_auth: None,
			};

			if let Some(ref auth) = self.client_auth {
				tls_config = rumqttc::TlsConfiguration::Simple {
					ca: ca_pem.as_bytes().to_vec(),
					alpn: None,
					client_auth: Some((
						auth.cert_pem.as_bytes().to_vec(),
						auth.key_pem.as_bytes().to_vec(),
					)),
				};
			}

			opts.set_transport(rumqttc::Transport::Tls(tls_config));
		}

		opts
	}
}

/// Type of each captured publish: `(topic, payload, retained)`. Only
/// used behind the `test-util` feature; kept as a type alias so the
/// `Arc<Mutex<Vec<..>>>` wrapper stays readable at its three occurrences.
pub(crate) type CaptureRow = (String, Vec<u8>, bool);

/// Shared capture sink handed off between [`SharedMqttClient`] and
/// `test_support::MockHandle`.
pub(crate) type CaptureSink = std::sync::Arc<std::sync::Mutex<Vec<CaptureRow>>>;

/// How long a publish may wait for a request-channel slot while the
/// broker session is up. The event loop drains the channel at network
/// speed, so hitting this means the loop has stalled.
const PUBLISH_SLOT_TIMEOUT: Duration = Duration::from_secs(10);

/// Payload published retained on the bridge status topic on every
/// ConnAck. The LWT sets the same topic to [`BRIDGE_OFFLINE`].
pub const BRIDGE_ONLINE: &str = "online";
/// LWT payload on the bridge status topic.
pub const BRIDGE_OFFLINE: &str = "offline";

/// Broker-session state shared by every [`SharedMqttClient`] clone and
/// the [`MqttEventLoop`] from one [`connect`].
#[derive(Default)]
struct Link {
	/// True between a ConnAck and the next connection error, as seen by
	/// [`MqttEventLoop::poll`]. rumqttc only drains its request channel
	/// while connected.
	connected: bool,
	/// Latest retained payload per topic published while disconnected,
	/// sent right after the next ConnAck. Keeping only the latest value
	/// bounds this by the topic set and keeps state current.
	deferred: HashMap<String, Vec<u8>>,
}

/// Cloneable MQTT client for sharing across camera tasks.
#[derive(Clone)]
pub struct SharedMqttClient {
	client: AsyncClient,
	link: Arc<Mutex<Link>>,
	/// When populated (test-util feature), every publish is mirrored
	/// into this sink so the binary's behaviour tests can assert on
	/// topic + payload without a broker round-trip. `None` in all
	/// production code paths so prod binary stays free of any capture
	/// overhead.
	capture: Option<CaptureSink>,
}

impl SharedMqttClient {
	/// Get a reference to the underlying `AsyncClient`.
	pub fn client(&self) -> &AsyncClient {
		&self.client
	}

	/// Test helper: wrap an already-constructed `AsyncClient`
	/// without going through [`connect`]. Used by unit tests that
	/// never drive the associated event loop — e.g. the discovery
	/// publisher's `compute_payloads` seam tests, and the binary's
	/// integration tests that assert on the publish/unpublish
	/// plumbing without touching a broker.
	///
	/// `doc(hidden)` because this is a test seam, not a supported
	/// public API — runtime code must go through [`connect`].
	#[doc(hidden)]
	pub fn for_test(client: AsyncClient) -> Self {
		Self::new(client, None)
	}

	fn new(client: AsyncClient, capture: Option<CaptureSink>) -> Self {
		Self {
			client,
			link: Arc::default(),
			capture,
		}
	}

	/// Treat the broker session as up, so publishes wait for a channel
	/// slot. For tests that need a publish to stall.
	#[cfg(test)]
	pub(crate) fn assume_connected(self) -> Self {
		lock_link(&self.link).connected = true;
		self
	}

	/// Internal constructor used by [`crate::test_support::mock_client`]
	/// to attach an observable capture sink. Not part of the public
	/// API — the double-underscore prefix flags it as internal.
	#[doc(hidden)]
	pub fn __test_new_with_capture(client: AsyncClient, capture: CaptureSink) -> Self {
		Self::new(client, Some(capture))
	}

	/// Test helper that constructs a fully-stubbed `SharedMqttClient`
	/// backed by an `AsyncClient` whose event loop is never polled.
	/// Callers do not need to depend on `rumqttc` directly; the event
	/// loop is dropped.
	///
	/// Intended only for tests that exercise broker-less code paths
	/// (e.g. `compute_payloads`) — any code that awaits broker I/O
	/// will hang.
	#[doc(hidden)]
	pub fn for_test_stub(client_id: &str) -> Self {
		// The rumqttc `AsyncClient::publish` path fails with
		// "Failed to send mqtt requests to eventloop" the instant
		// the matching `EventLoop` is dropped — it owns the
		// receiver half of the request channel. Keep the event
		// loop alive for the duration of the test process by
		// leaking it into `Box::leak`. Tests never rely on broker
		// I/O so the leak is bounded by `#[cfg(test)]` wiring that
		// holds these stubs only through the test run.
		let (client, event_loop) =
			AsyncClient::new(MqttOptions::new(client_id, "127.0.0.1", 1883), 1024);
		Box::leak(Box::new(event_loop));
		Self::new(client, None)
	}

	/// Publish a retained message to the given topic.
	pub async fn publish_retained(&self, topic: &str, payload: &[u8]) -> Result<(), MqttError> {
		self.send_publish(topic, payload, true).await
	}

	/// Publish a non-retained message to the given topic.
	pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), MqttError> {
		self.send_publish(topic, payload, false).await
	}

	/// Queue a QoS 1 publish without blocking indefinitely. rumqttc
	/// stops draining its request channel while it reconnects, so an
	/// unbounded `publish().await` during a broker outage wedges the
	/// caller until the broker returns.
	///
	/// While disconnected, a retained publish is deferred to the next
	/// ConnAck (latest value per topic) and a non-retained one is queued
	/// if a channel slot is free, else dropped with an error. While
	/// connected it waits at most [`PUBLISH_SLOT_TIMEOUT`] for a slot.
	async fn send_publish(
		&self,
		topic: &str,
		payload: &[u8],
		retain: bool,
	) -> Result<(), MqttError> {
		self.record_publish(topic, payload, retain);
		{
			// Checked and deferred under one lock, so a ConnAck can't
			// flush `deferred` between the two and strand this entry.
			let mut link = lock_link(&self.link);
			if !link.connected {
				if !retain {
					return self
						.client
						.try_publish(topic, QoS::AtLeastOnce, false, payload)
						.map_err(|e| {
							MqttError::PublishError(format!(
								"broker disconnected, publish dropped: {e}"
							))
						});
				}
				// The channel would reject an invalid topic; a deferred
				// one goes straight to the wire, where the broker would
				// drop the connection instead.
				if !rumqttc::valid_topic(topic) {
					return Err(MqttError::PublishError(format!("invalid topic {topic:?}")));
				}
				link.deferred.insert(topic.to_string(), payload.to_vec());
				return Ok(());
			}
		}
		match tokio::time::timeout(
			PUBLISH_SLOT_TIMEOUT,
			self.client
				.publish(topic, QoS::AtLeastOnce, retain, payload),
		)
		.await
		{
			Ok(res) => res.map_err(|e| MqttError::PublishError(e.to_string())),
			Err(_) => Err(MqttError::PublishError(format!(
				"request queue full for {PUBLISH_SLOT_TIMEOUT:?}, publish dropped"
			))),
		}
	}

	fn record_publish(&self, topic: &str, payload: &[u8], retained: bool) {
		if let Some(ref sink) = self.capture {
			// Recover from poison rather than re-panicking — a panic in
			// one test holding this lock would otherwise cascade to
			// every subsequent test in the same process. Mirrors the
			// `lock_recover` discipline adopted in stream_source.rs
			// (commit b394b95).
			sink.lock().unwrap_or_else(|p| p.into_inner()).push((
				topic.to_string(),
				payload.to_vec(),
				retained,
			));
		}
	}

	/// Subscribe to a topic with QoS 1.
	pub async fn subscribe(&self, topic: &str) -> Result<(), MqttError> {
		self.client
			.subscribe(topic, QoS::AtLeastOnce)
			.await
			.map_err(|e| MqttError::SubscribeError(e.to_string()))?;
		Ok(())
	}

	/// Subscribe to all control and query topics for a camera under the
	/// given topic prefix.
	pub async fn subscribe_all(
		&self,
		topic_prefix: &str,
		camera_name: &str,
	) -> Result<(), MqttError> {
		for topic in topics::subscribe_topics(topic_prefix, camera_name) {
			tracing::debug!(topic = %topic, "subscribing to topic");
			self.subscribe(&topic).await?;
		}
		Ok(())
	}
}

/// Owns the MQTT event loop. Must be polled to process messages.
pub struct MqttEventLoop {
	event_loop: EventLoop,
	/// `{topic_prefix}/status`, the LWT topic.
	bridge_status_topic: String,
	/// Shared with the [`SharedMqttClient`] from the same [`connect`].
	link: Arc<Mutex<Link>>,
}

impl MqttEventLoop {
	/// Poll the event loop for the next MQTT event. The error is boxed,
	/// as in [`MqttError::ConnectionFailure`]: at 144 bytes,
	/// `ConnectionError` trips clippy's `result_large_err`.
	///
	/// On each ConnAck this queues `{topic_prefix}/status = online`
	/// (retained) ahead of everything else, replacing the retained LWT
	/// `offline` left by a crash or network loss, followed by the
	/// retained publishes deferred while disconnected.
	///
	/// [`MqttError::ConnectionFailure`]: crate::MqttError::ConnectionFailure
	pub async fn poll(&mut self) -> Result<Event, Box<rumqttc::ConnectionError>> {
		let event = self.event_loop.poll().await.map_err(Box::new);
		match &event {
			Ok(Event::Incoming(Packet::ConnAck(_))) => {
				let mut link = lock_link(&self.link);
				link.connected = true;
				// rumqttc sends `pending` before the request channel, so
				// these go out first even when the channel is full.
				let pending = &mut self.event_loop.pending;
				for (topic, payload) in link.deferred.drain() {
					pending.push_front(retained(&topic, payload));
				}
				pending.push_front(retained(&self.bridge_status_topic, BRIDGE_ONLINE));
			}
			Err(_) => lock_link(&self.link).connected = false,
			Ok(_) => {}
		}
		event
	}
}

/// Create an MQTT connection, returning the shared client and event loop.
///
/// Sets a retained Last Will of `"offline"` on `{topic_prefix}/status`.
/// The broker publishes it when the bridge drops off without a
/// DISCONNECT: crash, SIGKILL, network loss, or a normal exit (bairelay
/// never sends DISCONNECT). [`MqttEventLoop::poll`] sets it back to
/// `"online"` on every ConnAck. `topic_prefix` is the
/// `mqtt.topic_prefix` config value (default `"bairelay"`; `"neolink"`
/// for legacy migration).
pub fn connect(
	config: &MqttConfig,
	client_id: &str,
	topic_prefix: &str,
) -> Result<(SharedMqttClient, MqttEventLoop), MqttError> {
	let mut opts = config.to_mqtt_options(client_id);
	let bridge_status_topic = topics::bridge_status(topic_prefix);
	opts.set_last_will(LastWill::new(
		&bridge_status_topic,
		BRIDGE_OFFLINE,
		QoS::AtLeastOnce,
		true,
	));
	let (client, event_loop) = AsyncClient::new(opts, 256);
	let client = SharedMqttClient::new(client, None);
	let link = Arc::clone(&client.link);
	Ok((
		client,
		MqttEventLoop {
			event_loop,
			bridge_status_topic,
			link,
		},
	))
}

fn retained(topic: &str, payload: impl Into<Vec<u8>>) -> Request {
	let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
	publish.retain = true;
	Request::Publish(publish)
}

/// Recover from poison: each critical section on `Link` is a single
/// field write or map operation, so it is never left half-updated.
fn lock_link(link: &Mutex<Link>) -> MutexGuard<'_, Link> {
	link.lock().unwrap_or_else(|p| p.into_inner())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn loopback_config() -> MqttConfig {
		MqttConfig {
			broker_addr: "127.0.0.1".into(),
			port: 1883,
			credentials: None,
			ca: None,
			client_auth: None,
		}
	}

	/// Read one MQTT packet: `(first header byte, body after the length)`.
	async fn read_packet(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
		use tokio::io::AsyncReadExt;
		let header = sock.read_u8().await.unwrap();
		let (mut len, mut shift) = (0usize, 0);
		loop {
			let b = sock.read_u8().await.unwrap();
			len |= usize::from(b & 0x7f) << shift;
			shift += 7;
			if b & 0x80 == 0 {
				break;
			}
		}
		let mut body = vec![0; len];
		sock.read_exact(&mut body).await.unwrap();
		(header, body)
	}

	/// Against a minimal fake broker: CONNECT carries the retained
	/// `offline` LWT on the prefix's status topic; after CONNACK the
	/// retained `online` goes first, then the latest retained value
	/// published while disconnected (the superseded one never goes out).
	#[tokio::test]
	async fn connack_publishes_online_then_deferred_retained() {
		use tokio::io::AsyncWriteExt;
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let cfg = MqttConfig {
			port: listener.local_addr().unwrap().port(),
			..loopback_config()
		};
		let (client, mut event_loop) = connect(&cfg, "lwt-test", "neolink").unwrap();
		client
			.publish_retained("neolink/cam/status", b"connected")
			.await
			.unwrap();
		client
			.publish_retained("neolink/cam/status", b"disconnected")
			.await
			.unwrap();
		let poller = tokio::spawn(async move {
			loop {
				let _ = event_loop.poll().await;
			}
		});

		let (mut sock, _) = listener.accept().await.unwrap();
		let (header, connect) = read_packet(&mut sock).await;
		assert_eq!(header, 0x10, "CONNECT");
		// Variable header: "MQTT" (2 + 4 bytes), level, connect flags.
		let flags = connect[7];
		assert_ne!(flags & 0x04, 0, "will flag");
		assert_ne!(flags & 0x20, 0, "will retain");
		let will = b"\x00\x0eneolink/status\x00\x07offline";
		assert!(connect.windows(will.len()).any(|w| w == will));

		sock.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();
		// PUBLISH, QoS 1, retain: topic, 2-byte packet id, payload.
		let (header, publish) = read_packet(&mut sock).await;
		assert_eq!(header, 0x33);
		assert_eq!(&publish[..16], b"\x00\x0eneolink/status");
		assert_eq!(&publish[18..], BRIDGE_ONLINE.as_bytes());
		let (header, publish) = read_packet(&mut sock).await;
		assert_eq!(header, 0x33);
		assert_eq!(&publish[..20], b"\x00\x12neolink/cam/status");
		assert_eq!(&publish[22..], b"disconnected");
		assert!(lock_link(&client.link).deferred.is_empty());
		poller.abort();
	}

	/// While disconnected rumqttc never drains its channel: retained
	/// publishes are deferred, and a non-retained one past the 256 slots
	/// fails instead of waiting for the broker.
	#[tokio::test]
	async fn publish_while_disconnected_never_waits() {
		let (client, _event_loop) = connect(&loopback_config(), "drop-test", "bairelay").unwrap();
		for _ in 0..256 {
			client.publish("bairelay/cam/reply", b"x").await.unwrap();
		}
		assert!(client.publish("bairelay/cam/reply", b"x").await.is_err());
		client
			.publish_retained("bairelay/cam/status", b"x")
			.await
			.unwrap();
		assert!(
			client
				.publish_retained("bairelay/+/status", b"x")
				.await
				.is_err()
		);
		assert_eq!(lock_link(&client.link).deferred.len(), 1);
	}

	/// While connected a full queue is waited on, but only for
	/// `PUBLISH_SLOT_TIMEOUT`.
	#[tokio::test(start_paused = true)]
	async fn publish_while_connected_times_out_on_full_queue() {
		let (client, _event_loop) = connect(&loopback_config(), "stall-test", "bairelay").unwrap();
		for _ in 0..256 {
			client.publish("bairelay/cam/reply", b"x").await.unwrap();
		}
		let client = client.assume_connected();
		let start = tokio::time::Instant::now();
		assert!(client.publish("bairelay/cam/reply", b"x").await.is_err());
		assert_eq!(start.elapsed(), PUBLISH_SLOT_TIMEOUT);
	}

	#[test]
	fn to_mqtt_options_with_credentials() {
		let cfg = MqttConfig {
			broker_addr: "broker.example".into(),
			port: 8883,
			credentials: Some(("user".into(), "secret".into())),
			ca: None,
			client_auth: None,
		};
		let opts = cfg.to_mqtt_options("cid");
		// Credentials are set on the internal options; `MqttOptions` does
		// not expose a credentials getter, so we settle for
		// not-panicking on the full build path. The broker_addr /
		// keep_alive / max_packet_size observations below guard the
		// rest of the builder surface.
		assert_eq!(opts.broker_address(), ("broker.example".into(), 8883));
		// rumqttc 0.25 returns a single usize here (incoming size).
		assert_eq!(opts.max_packet_size(), 16 * 1024 * 1024);
	}

	#[test]
	fn to_mqtt_options_with_tls_ca_only() {
		let cfg = MqttConfig {
			broker_addr: "broker".into(),
			port: 8883,
			credentials: None,
			// A minimal PEM block — the options builder does NOT parse it;
			// it just stores the bytes for rumqttc to use at connect time.
			ca: Some("-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----".into()),
			client_auth: None,
		};
		let opts = cfg.to_mqtt_options("cid");
		assert_eq!(opts.broker_address(), ("broker".into(), 8883));
	}

	#[test]
	fn to_mqtt_options_with_client_auth() {
		let cfg = MqttConfig {
			broker_addr: "broker".into(),
			port: 8883,
			credentials: None,
			ca: Some("-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----".into()),
			client_auth: Some(crate::client::ClientAuth {
				cert_pem: "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----".into(),
				key_pem: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----".into(),
			}),
		};
		let opts = cfg.to_mqtt_options("cid");
		assert_eq!(opts.broker_address(), ("broker".into(), 8883));
	}

	#[tokio::test]
	async fn shared_client_publish_and_subscribe_round_trip() {
		let client = SharedMqttClient::for_test_stub("client-test");
		// Exercise the happy paths — the stub never polls, so these
		// return Ok(()) after queueing on the rumqttc request channel.
		client
			.publish_retained("bairelay/cam/status", b"connected")
			.await
			.unwrap();
		client
			.publish("bairelay/cam/status/battery_level", b"75")
			.await
			.unwrap();
		client
			.subscribe("bairelay/cam/control/floodlight")
			.await
			.unwrap();
		client.subscribe_all("bairelay", "cam").await.unwrap();
	}

	#[test]
	fn shared_client_exposes_inner() {
		let client = SharedMqttClient::for_test_stub("inner-test");
		// `client()` returns a reference; the mere fact of calling is the test.
		let _inner: &rumqttc::AsyncClient = client.client();
	}
}
