//! PTZ motion shared by the CLI (`ptz <cam> control` / `zoom`) and the
//! MQTT `control/ptz` / `control/zoom` handlers, so both move the
//! camera the same way.

use std::time::Duration;

use bairelay_neolink_core::bc_protocol::{CameraDriver, Direction, Error};

/// Move speed when the caller doesn't pick one (neolink's).
pub const DEFAULT_SPEED: f32 = 32.0;
/// Longest move; a larger `amount / speed` is clamped to it.
pub const MAX_MOVE: Duration = Duration::from_secs(10);
/// Bound on each camera call (move, stop, zoom).
pub const STEP_TIMEOUT: Duration = Duration::from_secs(30);

/// Move `direction` at `speed` for `amount / speed` seconds (clamped
/// to [`MAX_MOVE`]), then send `Stop`, also after a failed move:
/// without it the camera keeps moving. `Direction::Stop` sends one
/// stop. Returns the move's error, else the stop's.
pub async fn ptz_move(
	cam: &dyn CameraDriver,
	direction: Direction,
	amount: f32,
	speed: f32,
) -> Result<(), Error> {
	let send = |dir| bounded(cam.send_ptz(dir, speed));
	if direction == Direction::Stop {
		return send(Direction::Stop).await;
	}
	let moved = send(direction).await;
	if moved.is_ok() {
		tokio::time::sleep(move_duration(amount, speed)).await;
	}
	let stopped = send(Direction::Stop).await;
	moved.and(stopped)
}

/// Zoom to `level` (the operator's float scale; `1.0` is position
/// 1000).
pub async fn zoom(cam: &dyn CameraDriver, level: f32) -> Result<(), Error> {
	bounded(cam.zoom_to(zoom_position(level))).await
}

/// `zoom_to` position for a zoom `level`: `level * 1000`, rounded;
/// negative and NaN levels are 0.
pub fn zoom_position(level: f32) -> u32 {
	// `as` saturates and maps NaN to 0.
	(level * 1000.0).round() as u32
}

/// How long a move of `amount` at `speed` runs; 0 for a non-positive
/// or NaN ratio.
fn move_duration(amount: f32, speed: f32) -> Duration {
	Duration::try_from_secs_f32(amount / speed)
		.unwrap_or(Duration::ZERO)
		.min(MAX_MOVE)
}

async fn bounded(call: impl Future<Output = Result<(), Error>>) -> Result<(), Error> {
	tokio::time::timeout(STEP_TIMEOUT, call)
		.await
		.unwrap_or(Err(Error::Other("PTZ command timed out")))
}

#[cfg(test)]
mod tests {
	use super::*;
	use bairelay_neolink_core::bc_protocol::FakeCameraBuilder;

	#[tokio::test(start_paused = true)]
	async fn move_runs_amount_over_speed_then_stops() {
		let fake = FakeCameraBuilder::new().build();
		let start = tokio::time::Instant::now();
		ptz_move(&*fake, Direction::Left, 64.0, 16.0).await.unwrap();
		assert_eq!(start.elapsed(), Duration::from_secs(4));
		assert_eq!(
			*fake.calls().send_ptz.lock().unwrap(),
			[(Direction::Left, 16.0), (Direction::Stop, 16.0)]
		);
	}

	#[tokio::test(start_paused = true)]
	async fn move_is_clamped_and_stop_is_one_call() {
		let fake = FakeCameraBuilder::new().build();
		let start = tokio::time::Instant::now();
		ptz_move(&*fake, Direction::Up, 1e9, DEFAULT_SPEED)
			.await
			.unwrap();
		assert_eq!(start.elapsed(), MAX_MOVE);
		ptz_move(&*fake, Direction::Stop, 5.0, DEFAULT_SPEED)
			.await
			.unwrap();
		assert_eq!(fake.calls().send_ptz.lock().unwrap().len(), 3);
	}

	#[tokio::test(start_paused = true)]
	async fn a_hung_move_still_sends_stop() {
		let fake = FakeCameraBuilder::new().with_send_ptz_pending().build();
		assert!(
			ptz_move(&*fake, Direction::Down, 32.0, DEFAULT_SPEED)
				.await
				.is_err()
		);
		assert_eq!(
			*fake.calls().send_ptz.lock().unwrap(),
			[(Direction::Down, 32.0), (Direction::Stop, 32.0)]
		);
	}

	#[test]
	fn move_duration_is_zero_for_degenerate_input() {
		assert_eq!(move_duration(f32::NAN, 32.0), Duration::ZERO);
		assert_eq!(move_duration(-5.0, 32.0), Duration::ZERO);
		assert_eq!(move_duration(5.0, 0.0), Duration::ZERO);
	}

	#[test]
	fn zoom_position_rounds_and_clamps() {
		assert_eq!(zoom_position(1.5), 1500);
		assert_eq!(zoom_position(0.0015), 2);
		assert_eq!(zoom_position(-4.0), 0);
		assert_eq!(zoom_position(f32::NAN), 0);
	}
}
