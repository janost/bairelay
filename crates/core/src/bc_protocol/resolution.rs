//! Discovery method selection for UID cameras

use serde::{Deserialize, Serialize};

/// Select permitted discovery methods
///
/// This is used for UID lookup, it is unused with
/// TPC/known ip address cameras
#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiscoveryMethods {
	/// Forbid all discovery methods. Only TCP connections with known addresses will work
	#[serde(alias = "none")]
	None,
	/// Allow local discovery on the local network using broadcasts
	/// This method does NOT contact reolink servers
	#[serde(alias = "local")]
	Local,
	/// Allow contact with the reolink servers to learn the ip address but DO NOT
	/// allow the camera/clinet to communicate through the reolink servers.
	///
	/// **This also enabled `Local` discovery**
	#[serde(alias = "remote")]
	Remote,
	/// Allow contact with the reolink servers to learn the ip address and map the connection
	/// from dev to client through those servers.
	///
	/// **This also enabled `Local` and `Remote` discovery**
	#[serde(alias = "map")]
	Map,
	/// Allow contact with the reolink servers to learn the ip address and relay the connection
	/// client to dev through those servers.
	///
	/// **This also enabled `Local`, `Map` and `Remote` discovery**
	#[serde(alias = "relay")]
	Relay,
	/// Cellular camera only support relay and map, by choosing this option
	/// only those are tried
	#[serde(alias = "cellular")]
	Cellular,
	/// Account ("cloud") camera. The camera is bound to a Reolink account and
	/// only accepts the sigV3 login with a cloud-minted token. Enables the same
	/// discovery reach as [`Relay`](Self::Relay) (local + remote + map + relay)
	/// and advertises `lver=3` so the camera issues the sigV3 handshake.
	/// Requires top-level `cloud_account` / `cloud_password`.
	#[serde(alias = "cloud")]
	Cloud,
	#[doc(hidden)]
	#[serde(alias = "debug")]
	/// Used for debugging it is set to whatever the dev is currently testing
	Debug,
}

/// Flags telling `find_camera` which UID-discovery steps the configured
/// [`DiscoveryMethods`] value permits. Each field gates one branch of
/// `find_camera`'s `tokio::select!` fallback chain
/// (local UDP broadcast → remote P2P → map → relay).
///
/// Extracted as a pure function so Stage 6 can cover the table in unit
/// tests without standing up the real UDP sockets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiscoveryFlags {
	pub local: bool,
	pub remote: bool,
	pub map: bool,
	pub relay: bool,
}

/// Translate [`DiscoveryMethods`] into the (local, remote, map, relay)
/// enable flags used by `BcCamera::find_camera`. The mapping follows
/// neolink's precedent: each level includes every cheaper level, except
/// `Cellular` which skips local/remote (cellular cameras are never on
/// the local subnet and P2P-remote rarely works through carrier NAT).
pub(crate) fn discovery_flags_for(method: DiscoveryMethods) -> DiscoveryFlags {
	match method {
		DiscoveryMethods::None => DiscoveryFlags {
			local: false,
			remote: false,
			map: false,
			relay: false,
		},
		DiscoveryMethods::Local => DiscoveryFlags {
			local: true,
			remote: false,
			map: false,
			relay: false,
		},
		DiscoveryMethods::Remote => DiscoveryFlags {
			local: true,
			remote: true,
			map: false,
			relay: false,
		},
		DiscoveryMethods::Map => DiscoveryFlags {
			local: true,
			remote: true,
			map: true,
			relay: false,
		},
		DiscoveryMethods::Relay | DiscoveryMethods::Cloud => DiscoveryFlags {
			local: true,
			remote: true,
			map: true,
			relay: true,
		},
		DiscoveryMethods::Cellular => DiscoveryFlags {
			local: false,
			remote: false,
			map: true,
			relay: true,
		},
		DiscoveryMethods::Debug => DiscoveryFlags {
			local: false,
			remote: false,
			map: true,
			relay: false,
		},
	}
}

#[cfg(test)]
mod discovery_flags_tests {
	use super::*;

	#[test]
	fn none_disables_everything() {
		let f = discovery_flags_for(DiscoveryMethods::None);
		assert!(!f.local && !f.remote && !f.map && !f.relay);
	}

	#[test]
	fn local_enables_only_local() {
		let f = discovery_flags_for(DiscoveryMethods::Local);
		assert!(f.local);
		assert!(!f.remote && !f.map && !f.relay);
	}

	#[test]
	fn remote_enables_local_and_remote() {
		let f = discovery_flags_for(DiscoveryMethods::Remote);
		assert!(f.local && f.remote);
		assert!(!f.map && !f.relay);
	}

	#[test]
	fn map_enables_local_remote_map() {
		let f = discovery_flags_for(DiscoveryMethods::Map);
		assert!(f.local && f.remote && f.map);
		assert!(!f.relay);
	}

	#[test]
	fn relay_enables_all_four() {
		let f = discovery_flags_for(DiscoveryMethods::Relay);
		assert!(f.local && f.remote && f.map && f.relay);
	}

	#[test]
	fn cellular_skips_local_and_remote() {
		// Cellular cameras aren't reachable via subnet broadcast and
		// rarely traverse carrier NAT for direct P2P — only map/relay
		// are useful.
		let f = discovery_flags_for(DiscoveryMethods::Cellular);
		assert!(!f.local && !f.remote);
		assert!(f.map && f.relay);
	}

	#[test]
	fn debug_enables_only_map() {
		// Debug is a dev-only knob that sidesteps other paths while
		// exercising the map code path in isolation.
		let f = discovery_flags_for(DiscoveryMethods::Debug);
		assert!(!f.local && !f.remote && !f.relay);
		assert!(f.map);
	}

	#[test]
	fn level_inclusivity_monotonic_through_relay() {
		// Each level in the None→Local→Remote→Map→Relay ladder must be
		// a strict superset of the previous. This invariant is what
		// lets operators pick the least-privileged method that still
		// reaches their camera.
		let ladder = [
			DiscoveryMethods::None,
			DiscoveryMethods::Local,
			DiscoveryMethods::Remote,
			DiscoveryMethods::Map,
			DiscoveryMethods::Relay,
		];
		for pair in ladder.windows(2) {
			let lower = discovery_flags_for(pair[0]);
			let upper = discovery_flags_for(pair[1]);
			assert!(
				(!lower.local || upper.local)
					&& (!lower.remote || upper.remote)
					&& (!lower.map || upper.map)
					&& (!lower.relay || upper.relay),
				"monotonicity broken between {:?} and {:?}",
				pair[0],
				pair[1],
			);
		}
	}
}
