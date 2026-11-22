pub mod ai;
pub mod client;
pub mod control;
pub mod discovery;
pub mod error;
pub mod status;
pub mod system_ca;
pub mod test_support;
pub mod topics;

pub use ai::AiObject;
pub use client::{ClientAuth, MqttConfig, MqttEventLoop, SharedMqttClient, connect};
pub use control::{ControlCommand, parse_control_message};
pub use discovery::{CameraEnableFlags, DiscoveryPublisher};
pub use error::MqttError;
pub use status::StatusPublisher;
pub use system_ca::system_ca_bundle;

// Re-export rumqttc types needed by the event loop consumer + tests.
pub use rumqttc::{
	ConnAck, ConnectReturnCode, ConnectionError, Event, Outgoing, Packet, Publish, QoS,
};
