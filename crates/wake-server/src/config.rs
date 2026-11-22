//! Operator-facing configuration for the wake server.

use bairelay_neolink_core::redact::UidTail;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

use crate::registry::{MIN_UID_LEN, uid_matches};

/// Operator-facing config block. Bind IP is inherited from the top-level
/// `bind_addr`, not duplicated here.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakeServerConfig {
	#[serde(default)]
	pub enable: bool,

	#[serde(default = "default_middleman_port")]
	pub middleman_port: u16,

	#[serde(default = "default_register_port")]
	pub register_port: u16,

	#[serde(default = "default_heartbeat_ms")]
	pub heartbeat_ms: u32,

	#[serde(default = "default_stale_after_ms")]
	pub stale_after_ms: u64,

	/// Bind IP, populated from the top-level `bind_addr` when the binary
	/// constructs a `RuntimeConfig` (see [`RuntimeConfig::from_block`]).
	/// Skipped at TOML parse time so operators never set it directly.
	#[serde(skip)]
	pub bind: Option<IpAddr>,
}

impl Default for WakeServerConfig {
	fn default() -> Self {
		Self {
			enable: false,
			middleman_port: default_middleman_port(),
			register_port: default_register_port(),
			heartbeat_ms: default_heartbeat_ms(),
			stale_after_ms: default_stale_after_ms(),
			bind: None,
		}
	}
}

fn default_middleman_port() -> u16 {
	9999
}
fn default_register_port() -> u16 {
	58200
}
fn default_heartbeat_ms() -> u32 {
	20000
}
fn default_stale_after_ms() -> u64 {
	80000
}

/// Validated runtime view consumed by `run()`. Constructed by the binary
/// from a `WakeServerConfig`, the top-level `bind_addr` and the
/// configured camera UIDs.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
	pub bind: IpAddr,
	pub middleman_port: u16,
	pub register_port: u16,
	pub heartbeat_ms: u32,
	pub stale_after_ms: u64,
	/// Configured (short, sticker) camera UIDs. `D2M_Q` and `D2R_HB` for
	/// any other UID are dropped, so an unauthenticated LAN peer can't
	/// fill the registry or anchor maps. See [`Self::allows_uid`].
	pub allowed_uids: Vec<String>,
}

impl RuntimeConfig {
	/// Build a runtime config from a parsed `[wake_server]` block, a
	/// resolved bind IP and the configured camera UIDs. Returns `Err(_)`
	/// with a human-readable message on validation failure.
	pub fn from_block(
		block: &WakeServerConfig,
		bind: IpAddr,
		allowed_uids: impl IntoIterator<Item = String>,
	) -> Result<Self, String> {
		if block.middleman_port == 0 {
			return Err("middleman_port must be > 0".into());
		}
		if block.register_port == 0 {
			return Err("register_port must be > 0".into());
		}
		if block.middleman_port == block.register_port {
			return Err(format!(
				"middleman_port and register_port must differ (both {})",
				block.middleman_port
			));
		}
		if block.heartbeat_ms < 1000 {
			return Err(format!(
				"heartbeat_ms must be >= 1000 (got {})",
				block.heartbeat_ms
			));
		}
		if block.stale_after_ms < block.heartbeat_ms as u64 {
			return Err(format!(
				"stale_after_ms ({}) must be >= heartbeat_ms ({})",
				block.stale_after_ms, block.heartbeat_ms
			));
		}
		let allowed_uids: Vec<String> = allowed_uids.into_iter().collect();
		if allowed_uids.is_empty() {
			tracing::warn!("wake server: no camera has a uid configured; every camera is ignored");
		}
		for uid in allowed_uids.iter().filter(|u| u.len() < MIN_UID_LEN) {
			tracing::warn!(
				uid = %UidTail(uid),
				"wake server: uid shorter than {MIN_UID_LEN} characters; wake requests for it are refused",
			);
		}
		Ok(Self {
			bind,
			middleman_port: block.middleman_port,
			register_port: block.register_port,
			heartbeat_ms: block.heartbeat_ms,
			stale_after_ms: block.stale_after_ms,
			allowed_uids,
		})
	}

	/// True if `reported`, a UID from a camera's `D2M_Q` / `D2R_HB`,
	/// belongs to a configured camera (see [`uid_matches`]).
	pub fn allows_uid(&self, reported: &str) -> bool {
		self.allowed_uids.iter().any(|c| uid_matches(reported, c))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::Ipv4Addr;

	fn loopback() -> IpAddr {
		Ipv4Addr::LOCALHOST.into()
	}

	fn uids() -> Vec<String> {
		vec!["9527000TESTCAM01".into()]
	}

	#[test]
	fn defaults_match_spec() {
		let cfg: WakeServerConfig = toml::from_str("").unwrap();
		assert!(!cfg.enable);
		assert_eq!(cfg.middleman_port, 9999);
		assert_eq!(cfg.register_port, 58200);
		assert_eq!(cfg.heartbeat_ms, 20000);
		assert_eq!(cfg.stale_after_ms, 80000);
	}

	#[test]
	fn deny_unknown_fields() {
		let result: Result<WakeServerConfig, _> = toml::from_str("totally_made_up_field = 1");
		assert!(result.is_err());
	}

	#[test]
	fn runtime_rejects_zero_ports() {
		let block = WakeServerConfig {
			middleman_port: 0,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback(), uids()).is_err());
	}

	#[test]
	fn from_block_zero_register_port_message_mentions_register_port() {
		let block = WakeServerConfig {
			register_port: 0,
			..WakeServerConfig::default()
		};
		let err = RuntimeConfig::from_block(&block, loopback(), uids())
			.expect_err("zero register_port must error");
		assert!(
			err.contains("register_port"),
			"expected error to mention register_port, got: {err}"
		);
	}

	#[test]
	fn runtime_rejects_equal_ports() {
		let block = WakeServerConfig {
			middleman_port: 5000,
			register_port: 5000,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback(), uids()).is_err());
	}

	#[test]
	fn runtime_rejects_low_heartbeat() {
		let block = WakeServerConfig {
			heartbeat_ms: 500,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback(), uids()).is_err());
	}

	#[test]
	fn runtime_rejects_stale_below_heartbeat() {
		let block = WakeServerConfig {
			heartbeat_ms: 5000,
			stale_after_ms: 1000,
			..WakeServerConfig::default()
		};
		assert!(RuntimeConfig::from_block(&block, loopback(), uids()).is_err());
	}

	#[test]
	fn runtime_accepts_defaults_with_loopback_bind() {
		let block = WakeServerConfig::default();
		let rt = RuntimeConfig::from_block(&block, loopback(), uids()).unwrap();
		assert_eq!(rt.middleman_port, 9999);
		assert_eq!(rt.register_port, 58200);
	}

	#[test]
	fn allows_configured_uid_short_or_with_firmware_suffix_only() {
		let rt =
			RuntimeConfig::from_block(&WakeServerConfig::default(), loopback(), uids()).unwrap();
		assert!(rt.allows_uid("9527000TESTCAM01"));
		assert!(rt.allows_uid("9527000TESTCAM010123"));
		assert!(!rt.allows_uid("9527000TESTCAM01012"));
		assert!(!rt.allows_uid("9527000TESTCAM02"));
		assert!(!rt.allows_uid("9527000TESTCAM0"));
		assert!(!rt.allows_uid(""));
	}
}
