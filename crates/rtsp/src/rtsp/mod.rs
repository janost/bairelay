//! RTSP protocol layer: message handling, auth, transport, and session state.

pub mod auth;
pub mod message;
pub mod session;
pub mod transport;

/// ASCII case-insensitive `str::strip_prefix`. Header values are
/// attacker-controlled, so a multibyte char straddling `prefix.len()`
/// must yield `None`, never a slice panic.
pub(crate) fn strip_prefix_ignore_case<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
	s.get(..prefix.len())
		.is_some_and(|p| p.eq_ignore_ascii_case(prefix))
		.then(|| &s[prefix.len()..])
}

#[cfg(test)]
mod tests {
	use super::strip_prefix_ignore_case;

	#[test]
	fn strip_prefix_ignore_case_matches_any_case() {
		assert_eq!(strip_prefix_ignore_case("bAsIc abc", "Basic "), Some("abc"));
		assert_eq!(strip_prefix_ignore_case("Bas", "Basic "), None);
	}

	#[test]
	fn strip_prefix_ignore_case_multibyte_boundary_is_none() {
		// 'é' is 2 bytes; byte 6 / byte 12 fall inside it.
		assert_eq!(strip_prefix_ignore_case("Basicé", "Basic "), None);
		assert_eq!(
			strip_prefix_ignore_case("interleavedé0-1", "interleaved="),
			None
		);
	}

	#[test]
	fn auth_and_transport_parsers_survive_multibyte_input() {
		let users = [crate::rtsp::auth::UserCred {
			name: "a".into(),
			password: "b".into(),
		}];
		assert!(crate::rtsp::auth::verify_basic("Basicé", &users).is_err());
		assert!(crate::rtsp::auth::parse_digest_params("Digesté").is_err());
		assert!(crate::rtsp::transport::parse("RTP/AVP/TCP;interleavedé0-1").is_err());
	}
}
