use std::time::Duration;

use tokio::time::Instant;

use crate::wake_lock::WakeLockCounter;

// ── GracePeriod ──────────────────────────────────────────────────────

pub struct GracePeriod {
	wake_lock: WakeLockCounter,
	duration: Duration,
}

impl GracePeriod {
	pub fn new(wake_lock: WakeLockCounter, duration: Duration) -> Self {
		Self {
			wake_lock,
			duration,
		}
	}

	/// Returns once the wake lock has been idle for `duration` in one
	/// stretch. Any acquire restarts the countdown from its release,
	/// even a brief acquire+release that falls entirely inside a sleep:
	/// the deadline is re-derived from `idle_since()` after each wake.
	/// A lock never acquired counts as idle from the call.
	pub async fn run(self) {
		let started = Instant::now();
		loop {
			// Register for notification BEFORE checking state
			// to avoid TOCTOU race with Notify
			let released = self.wake_lock.notify_future();
			if !self.wake_lock.is_idle() {
				released.await;
				continue;
			}
			let idle_from = match self.wake_lock.idle_since() {
				Some(at) => at,
				None if self.wake_lock.is_idle() => started,
				None => continue, // acquired just now
			};
			let deadline = idle_from + self.duration;
			if Instant::now() >= deadline {
				return; // Grace period expired — caller should disconnect
			}
			tokio::time::sleep_until(deadline).await;
		}
	}
}
