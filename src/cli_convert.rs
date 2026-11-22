//! CLI → oneshot-action converters and log-directive helpers.
//!
//! Pure glue lifted out of `main.rs` so unit tests can cover every
//! branch without spawning the full binary. The functions here bridge
//! the clap-derived enums in [`crate::cli`] with the action / type
//! enums the `oneshot` modules consume. Living in the library (rather
//! than as `From` impls on either side) keeps `crate::oneshot` free of
//! CLI concerns and `crate::cli` free of `bairelay_neolink_core` concerns —
//! each enum stays focused on its own boundary.

use tracing_subscriber::EnvFilter;

use crate::cli;
use crate::oneshot;

/// Decide whether to emit ANSI colour codes on the log stream. Off
/// when stderr is not a TTY (redirected to a file, piped to another
/// program, captured by a process supervisor) or when `NO_COLOR` is
/// set per <https://no-color.org>. Keeps the four-combination
/// decision table testable; the live TTY probe happens in `main.rs`.
pub fn should_emit_ansi(stderr_is_tty: bool, no_color_set: bool) -> bool {
	stderr_is_tty && !no_color_set
}

/// Translate a `-v` count into a `RUST_LOG` directive. `RUST_LOG`
/// always wins if set in the environment (explicit beats implicit —
/// operator-visible).
pub fn verbosity_env_filter(verbose: u8) -> EnvFilter {
	if let Ok(explicit) = EnvFilter::try_from_default_env() {
		return explicit;
	}
	let directive = match verbose {
		0 => "warn,bairelay=info,rumqttc=warn",
		1 => "info,bairelay=debug",
		2 => "debug,bairelay=trace,bairelay_neolink_core=debug",
		_ => "trace",
	};
	EnvFilter::new(directive)
}

/// The `ptz` sub-command, defaulting an absent one to listing presets
/// (`Preset { preset_id: None }`), the same as `ptz <cam> preset`.
pub fn ptz_cmd_or_list(cmd: &Option<cli::PtzCommand>) -> cli::PtzCommand {
	cmd.clone()
		.unwrap_or(cli::PtzCommand::Preset { preset_id: None })
}

/// Map a CLI-level PTZ direction onto the bairelay_neolink_core value.
pub fn ptz_direction_to_core(
	d: cli::PtzDirection,
) -> bairelay_neolink_core::bc_protocol::Direction {
	use bairelay_neolink_core::bc_protocol::Direction;
	use cli::PtzDirection;
	match d {
		PtzDirection::Up => Direction::Up,
		PtzDirection::Down => Direction::Down,
		PtzDirection::Left => Direction::Left,
		PtzDirection::Right => Direction::Right,
		PtzDirection::Stop => Direction::Stop,
	}
}

/// Map a CLI-level service-name onto the oneshot `Service` enum.
pub fn service_name_to_core(s: cli::ServiceName) -> oneshot::services::Service {
	use cli::ServiceName;
	use oneshot::services::Service;
	match s {
		ServiceName::Baichuan => Service::Baichuan,
		ServiceName::Http => Service::Http,
		ServiceName::Https => Service::Https,
		ServiceName::Rtmp => Service::Rtmp,
		ServiceName::Rtsp => Service::Rtsp,
		ServiceName::Onvif => Service::Onvif,
	}
}

/// Clone / normalise a `cli::ServiceAction`. An absent action
/// defaults to `get` (read-mode).
pub fn clone_service_action(a: &Option<cli::ServiceAction>) -> oneshot::services::Action {
	use cli::{OnOff, ServiceAction};
	use oneshot::services::Action;
	match a {
		None | Some(ServiceAction::Get) => Action::Get,
		Some(ServiceAction::On) => Action::On,
		Some(ServiceAction::Off) => Action::Off,
		Some(ServiceAction::Port { port }) => Action::Port(*port),
		Some(ServiceAction::Set { port, enabled }) => Action::Set {
			port: *port,
			enabled: matches!(enabled, OnOff::On),
		},
	}
}

/// Translate a CLI [`cli::UserAction`] into the oneshot
/// [`oneshot::users::Action`]. An absent action defaults to `list`.
///
/// `Add` / `Password` carry the password as `Option<String>` so the
/// operator can omit it; [`oneshot::password_source::resolve`] then
/// prompts (blocking TTY / stdin read). The one-shot path fills it in
/// before connecting via
/// [`oneshot::password_source::resolve_command`], so here it is only
/// validated.
pub fn clone_user_action(a: &Option<cli::UserAction>) -> anyhow::Result<oneshot::users::Action> {
	use cli::{UserAction, UserTypeArg};
	use oneshot::password_source::{self, PROMPT_ADD, PROMPT_PASSWORD};
	use oneshot::users::{Action, UserType};
	let action = match a {
		None | Some(UserAction::List) => Action::List,
		Some(UserAction::Add {
			name,
			password,
			user_type,
		}) => {
			let password = password_source::resolve(password.clone(), PROMPT_ADD)?;
			Action::Add {
				name: name.clone(),
				password,
				user_type: match user_type {
					UserTypeArg::User => UserType::User,
					UserTypeArg::Administrator => UserType::Administrator,
				},
			}
		}
		Some(UserAction::Password { name, password }) => Action::Password {
			name: name.clone(),
			password: password_source::resolve(password.clone(), PROMPT_PASSWORD)?,
		},
		Some(UserAction::Delete { name }) => Action::Delete { name: name.clone() },
	};
	Ok(action)
}

/// Coarse-grained label for a one-shot exit code, for error reporting
/// to operators.
pub fn exit_code_to_kind(code: i32) -> &'static str {
	use crate::oneshot::classify::{
		EXIT_CONFIG, EXIT_CONNECTION, EXIT_INTERRUPTED, EXIT_PROTOCOL, EXIT_UNSUPPORTED, EXIT_USAGE,
	};
	match code {
		EXIT_USAGE => "usage",
		EXIT_CONFIG => "config",
		EXIT_CONNECTION => "connection",
		EXIT_PROTOCOL => "protocol",
		EXIT_UNSUPPORTED => "unsupported",
		EXIT_INTERRUPTED => "interrupted",
		_ => "internal",
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn should_emit_ansi_only_when_tty_and_no_color_unset() {
		// TTY + no NO_COLOR → colour on (interactive terminal).
		assert!(should_emit_ansi(true, false));
		// Not a TTY (file redirect, pipe) → colour off regardless.
		assert!(!should_emit_ansi(false, false));
		// NO_COLOR set → colour off even on a TTY.
		assert!(!should_emit_ansi(true, true));
		// Both off-conditions → colour off.
		assert!(!should_emit_ansi(false, true));
	}

	#[test]
	fn verbosity_env_filter_maps_each_level() {
		// Make sure RUST_LOG isn't set in the test env so the
		// implicit mapping path runs (don't touch global env here —
		// we just verify the fn doesn't panic and builds an EnvFilter
		// for each level).
		// SAFETY (single-threaded test, scoped flip): ensure the
		// explicit env-var branch is not taken.
		let previous = std::env::var("RUST_LOG").ok();
		unsafe {
			std::env::remove_var("RUST_LOG");
		}
		for v in [0u8, 1, 2, 3, 255] {
			let _f = verbosity_env_filter(v);
		}
		if let Some(v) = previous {
			unsafe {
				std::env::set_var("RUST_LOG", v);
			}
		}
	}

	#[test]
	fn ptz_cmd_or_list_defaults_to_preset_list() {
		match ptz_cmd_or_list(&None) {
			cli::PtzCommand::Preset { preset_id: None } => {}
			other => panic!("expected Preset{{None}}, got {other:?}"),
		}
	}

	#[test]
	fn ptz_direction_to_core_maps_every_variant() {
		use bairelay_neolink_core::bc_protocol::Direction;
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Up),
			Direction::Up
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Down),
			Direction::Down
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Left),
			Direction::Left
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Right),
			Direction::Right
		));
		assert!(matches!(
			ptz_direction_to_core(cli::PtzDirection::Stop),
			Direction::Stop
		));
	}

	#[test]
	fn service_name_to_core_maps_every_variant() {
		use oneshot::services::Service;
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Baichuan),
			Service::Baichuan
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Http),
			Service::Http
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Https),
			Service::Https
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Rtmp),
			Service::Rtmp
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Rtsp),
			Service::Rtsp
		));
		assert!(matches!(
			service_name_to_core(cli::ServiceName::Onvif),
			Service::Onvif
		));
	}

	#[test]
	fn clone_service_action_defaults_to_get_when_absent() {
		use oneshot::services::Action;
		assert!(matches!(clone_service_action(&None), Action::Get));
	}

	#[test]
	fn clone_service_action_maps_each_variant() {
		use cli::{OnOff, ServiceAction};
		use oneshot::services::Action;
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Get)),
			Action::Get
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::On)),
			Action::On
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Off)),
			Action::Off
		));
		assert!(matches!(
			clone_service_action(&Some(ServiceAction::Port { port: 8080 })),
			Action::Port(8080)
		));
		let set_on = clone_service_action(&Some(ServiceAction::Set {
			port: 443,
			enabled: OnOff::On,
		}));
		match set_on {
			Action::Set { port, enabled } => {
				assert_eq!(port, 443);
				assert!(enabled);
			}
			other => panic!("expected Set, got {other:?}"),
		}
		let set_off = clone_service_action(&Some(ServiceAction::Set {
			port: 80,
			enabled: OnOff::Off,
		}));
		match set_off {
			Action::Set { port, enabled } => {
				assert_eq!(port, 80);
				assert!(!enabled);
			}
			other => panic!("expected Set(off), got {other:?}"),
		}
	}

	#[test]
	fn clone_user_action_defaults_to_list_when_absent() {
		use oneshot::users::Action;
		assert!(matches!(
			clone_user_action(&None).expect("ok"),
			Action::List
		));
	}

	#[test]
	fn clone_user_action_maps_each_variant() {
		use cli::{UserAction, UserTypeArg};
		use oneshot::users::{Action, UserType};
		assert!(matches!(
			clone_user_action(&Some(UserAction::List)).expect("ok"),
			Action::List
		));
		let add = clone_user_action(&Some(UserAction::Add {
			name: "bob".into(),
			password: Some("pw".into()),
			user_type: UserTypeArg::Administrator,
		}))
		.expect("ok");
		match add {
			Action::Add {
				name,
				password,
				user_type: UserType::Administrator,
			} => {
				assert_eq!(name, "bob");
				assert_eq!(password, "pw");
			}
			other => panic!("expected Add Administrator, got {other:?}"),
		}
		let add_user = clone_user_action(&Some(UserAction::Add {
			name: "a".into(),
			password: Some("b".into()),
			user_type: UserTypeArg::User,
		}))
		.expect("ok");
		assert!(matches!(
			add_user,
			Action::Add {
				user_type: UserType::User,
				..
			}
		));

		let pw = clone_user_action(&Some(UserAction::Password {
			name: "x".into(),
			password: Some("y".into()),
		}))
		.expect("ok");
		match pw {
			Action::Password { name, password } => {
				assert_eq!(name, "x");
				assert_eq!(password, "y");
			}
			other => panic!("expected Password, got {other:?}"),
		}
		let del = clone_user_action(&Some(UserAction::Delete { name: "z".into() })).expect("ok");
		match del {
			Action::Delete { name } => assert_eq!(name, "z"),
			other => panic!("expected Delete, got {other:?}"),
		}
	}

	#[test]
	fn clone_user_action_empty_provided_password_rejected() {
		use cli::{UserAction, UserTypeArg};
		// Empty positional should classify as a usage error (operator
		// pilot error: `bairelay users add bob user ""`). The
		// password resolver enforces non-empty regardless of source.
		let err = clone_user_action(&Some(UserAction::Add {
			name: "bob".into(),
			password: Some(String::new()),
			user_type: UserTypeArg::User,
		}))
		.expect_err("must reject empty password");
		assert!(
			err.chain()
				.any(|c| c.is::<crate::oneshot::errors::UsageError>()),
			"empty password must classify as UsageError; chain: {err:#}"
		);
	}

	#[test]
	fn verbosity_honours_explicit_rust_log() {
		// Force the explicit branch by setting RUST_LOG.
		let previous = std::env::var("RUST_LOG").ok();
		unsafe {
			std::env::set_var("RUST_LOG", "warn");
		}
		let _ = verbosity_env_filter(0);
		if let Some(v) = previous {
			unsafe {
				std::env::set_var("RUST_LOG", v);
			}
		} else {
			unsafe {
				std::env::remove_var("RUST_LOG");
			}
		}
	}

	#[test]
	fn exit_code_to_kind_covers_every_label() {
		use crate::oneshot::classify::{
			EXIT_CONFIG, EXIT_CONNECTION, EXIT_INTERRUPTED, EXIT_PROTOCOL, EXIT_UNSUPPORTED,
			EXIT_USAGE,
		};
		assert_eq!(exit_code_to_kind(EXIT_USAGE), "usage");
		assert_eq!(exit_code_to_kind(EXIT_CONFIG), "config");
		assert_eq!(exit_code_to_kind(EXIT_CONNECTION), "connection");
		assert_eq!(exit_code_to_kind(EXIT_PROTOCOL), "protocol");
		assert_eq!(exit_code_to_kind(EXIT_UNSUPPORTED), "unsupported");
		assert_eq!(exit_code_to_kind(EXIT_INTERRUPTED), "interrupted");
		assert_eq!(exit_code_to_kind(0), "internal");
		assert_eq!(exit_code_to_kind(999), "internal");
	}
}
