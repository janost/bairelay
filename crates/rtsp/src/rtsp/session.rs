//! RTSP session identifiers. Per-session state lives in
//! [`crate::server::registry`].

/// Generate a fresh cryptographically-random session ID (32 hex chars).
///
/// 128 bits of entropy — chosen so a long-lived server with many
/// concurrent clients has a vanishing birthday-collision probability over
/// its lifetime. RFC 7826 §18.49 merely requires "random … at least eight
/// octets"; we exceed that comfortably.
pub fn new_session_id() -> String {
	let bytes: [u8; 16] = rand::random();
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn session_id_is_32_hex_chars() {
		let id = new_session_id();
		assert_eq!(id.len(), 32);
		assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
	}

	#[test]
	fn session_ids_are_distinct() {
		let ids: std::collections::HashSet<_> = (0..100).map(|_| new_session_id()).collect();
		assert_eq!(ids.len(), 100);
	}
}
