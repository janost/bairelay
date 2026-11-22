use bairelay::grace_period::GracePeriod;
use bairelay::wake_lock::WakeLockCounter;
use std::time::Duration;

#[tokio::test]
async fn fires_after_timeout() {
	let wl = WakeLockCounter::new();
	let guard = wl.acquire();
	let gp = GracePeriod::new(wl.clone(), Duration::from_millis(50));
	let handle = tokio::spawn(gp.run());
	drop(guard); // Release → grace period starts
	let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
	assert!(result.is_ok()); // Grace period completed
}

#[tokio::test]
async fn does_not_fire_while_locked() {
	let wl = WakeLockCounter::new();
	let _guard = wl.acquire(); // Hold lock for entire test
	let gp = GracePeriod::new(wl.clone(), Duration::from_millis(50));
	let handle = tokio::spawn(gp.run());
	let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
	assert!(result.is_err()); // Timed out — grace period never completed
}

#[tokio::test]
async fn resets_on_reacquire() {
	let wl = WakeLockCounter::new();
	let guard1 = wl.acquire();
	let gp = GracePeriod::new(wl.clone(), Duration::from_millis(100));
	let handle = tokio::spawn(gp.run());

	drop(guard1); // Start grace period

	// Reacquire before grace period expires (at 30ms of 100ms)
	tokio::time::sleep(Duration::from_millis(30)).await;
	let guard2 = wl.acquire();

	// Hold for a bit, then release
	tokio::time::sleep(Duration::from_millis(30)).await;
	drop(guard2); // Grace period restarts

	// Should complete ~100ms after second release (not before)
	let result = tokio::time::timeout(Duration::from_millis(200), handle).await;
	assert!(result.is_ok()); // Grace period fired after second release
}

/// A brief acquire+release inside the countdown restarts it: the
/// camera must be idle for the full duration after the last release.
#[tokio::test(start_paused = true)]
async fn brief_reacquire_inside_the_window_restarts_the_countdown() {
	let wl = WakeLockCounter::new();
	drop(wl.acquire());
	let start = tokio::time::Instant::now();
	let handle = tokio::spawn(GracePeriod::new(wl.clone(), Duration::from_secs(10)).run());

	tokio::time::sleep(Duration::from_secs(4)).await;
	drop(wl.acquire());

	handle.await.unwrap();
	assert_eq!(start.elapsed(), Duration::from_secs(14));
}
