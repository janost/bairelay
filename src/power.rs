//! Power-source detection from the camera's `<Support>` reply.
//!
//! Battery models report `battery > 0` on their channel's `<Support>`
//! item and mains models `battery = 0`, WiFi-mains included (nodelink-js
//! model fixtures). Transport, login device type and model name are not
//! reliable signals, so they are not consulted.

use std::fmt;

use bairelay_neolink_core::bc::xml::Support;
use bairelay_neolink_core::bc_protocol::CameraDriver;

use crate::capabilities::channel_item;
use crate::config::Power;

/// The signal one session's detection was based on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Evidence {
	/// The channel's `<Support>` item carries `battery`.
	SupportBattery(u32),
	/// No `battery` field, but a login ability name contains "battery".
	BatteryAbility,
	/// The `<Support>` reply has no item for the channel, and no
	/// battery ability is advertised.
	NoSupportItem(u8),
	/// The channel's item has no `battery` field, and no battery
	/// ability is advertised.
	NoBatteryField(u8),
	/// `get_support` failed or timed out.
	SupportFailed,
}

impl Evidence {
	/// Fold this session's evidence into the `earlier` result: an
	/// unknown result (a failed request, say) never erases a known one.
	pub fn fold(self, earlier: Power) -> Power {
		match self.power() {
			Power::Unknown => earlier,
			power => power,
		}
	}

	/// The power source this evidence points to.
	pub fn power(self) -> Power {
		match self {
			Evidence::SupportBattery(0) => Power::Mains,
			Evidence::SupportBattery(_) | Evidence::BatteryAbility => Power::Battery,
			Evidence::NoSupportItem(_) | Evidence::NoBatteryField(_) | Evidence::SupportFailed => {
				Power::Unknown
			}
		}
	}
}

impl fmt::Display for Evidence {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Evidence::SupportBattery(n) => write!(f, "Support battery={n}"),
			Evidence::BatteryAbility => f.write_str("battery ability advertised"),
			Evidence::NoSupportItem(ch) => write!(f, "no Support item for channel {ch}"),
			Evidence::NoBatteryField(ch) => {
				write!(f, "Support item for channel {ch} has no battery field")
			}
			Evidence::SupportFailed => f.write_str("Support request failed"),
		}
	}
}

/// Detect the power source of `channel` from `support` (`None` when the
/// request failed). A missing `battery` field falls back to the login
/// abilities, matching nodelink-js's `/battery/i` test.
pub async fn detect(driver: &dyn CameraDriver, support: Option<&Support>, channel: u8) -> Evidence {
	let Some(support) = support else {
		return Evidence::SupportFailed;
	};
	let item = channel_item(support, channel);
	if let Some(n) = item.and_then(|item| item.battery) {
		return Evidence::SupportBattery(n);
	}
	if driver.ability_name_contains("battery").await == Some(true) {
		return Evidence::BatteryAbility;
	}
	if item.is_some() {
		Evidence::NoBatteryField(channel)
	} else {
		Evidence::NoSupportItem(channel)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc::xml::SupportItem;
	use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

	fn support(items: Vec<SupportItem>) -> Support {
		Support {
			items,
			..Default::default()
		}
	}

	fn item(chn_id: u32, battery: Option<u32>) -> SupportItem {
		SupportItem {
			chn_id,
			battery,
			..Default::default()
		}
	}

	#[tokio::test]
	async fn detect_maps_support_battery_and_ability_fallback() {
		let plain = FakeCameraBuilder::new().build();
		let battery_ability = FakeCameraBuilder::new()
			.with_abilities(&["batteryInfo", "ledState"])
			.build();
		let no_battery_ability = FakeCameraBuilder::new()
			.with_abilities(&["ledState"])
			.build();

		let s = support(vec![item(0, Some(0)), item(1, Some(2))]);
		assert_eq!(
			detect(&*plain, Some(&s), 0).await,
			Evidence::SupportBattery(0)
		);
		assert_eq!(detect(&*plain, Some(&s), 0).await.power(), Power::Mains);
		assert_eq!(detect(&*plain, Some(&s), 1).await.power(), Power::Battery);
		// The Support field wins over the abilities.
		assert_eq!(
			detect(&*battery_ability, Some(&s), 0).await.power(),
			Power::Mains
		);

		let no_field = support(vec![item(0, None)]);
		assert_eq!(
			detect(&*battery_ability, Some(&no_field), 0).await,
			Evidence::BatteryAbility
		);
		assert_eq!(
			detect(&*no_battery_ability, Some(&no_field), 0).await,
			Evidence::NoBatteryField(0)
		);
		assert_eq!(
			detect(&*plain, Some(&no_field), 3).await,
			Evidence::NoSupportItem(3)
		);
		assert_eq!(detect(&*plain, None, 0).await, Evidence::SupportFailed);
		assert_eq!(Evidence::NoSupportItem(3).power(), Power::Unknown);
	}

	#[test]
	fn fold_keeps_a_known_result_over_unknown() {
		let mains = Evidence::SupportBattery(0);
		assert_eq!(mains.fold(Power::Unknown), Power::Mains);
		assert_eq!(Evidence::SupportFailed.fold(Power::Mains), Power::Mains);
		assert_eq!(Evidence::SupportFailed.fold(Power::Unknown), Power::Unknown);
		assert_eq!(
			Evidence::SupportBattery(2).fold(Power::Mains),
			Power::Battery
		);
	}
}
