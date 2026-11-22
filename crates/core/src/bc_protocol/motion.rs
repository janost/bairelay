use super::{AlarmUpdate, BcCamera, Error, Result, classify_alarm_events};
use crate::bc::{model::*, xml::*};
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, channel, error::TryRecvError};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use crate::bc_protocol::connection::mock::{MockConnection, reply_200_empty, reply_err_code};

/// Motion Status that the callback can send
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MotionStatus {
	/// Sent when motion is first detected
	Start(Instant),
	/// Sent when motion stops
	Stop(Instant),
	/// Sent when an Alarm about something other than motion was received
	NoChange(Instant),
}

/// Source feeding a [`MotionData`]: full alarm updates from the camera
/// task, or a motion-only feed from a test harness.
enum UpdateRx {
	Alarm(Receiver<Result<AlarmUpdate>>),
	#[cfg(any(test, feature = "test-util"))]
	Motion(Receiver<Result<MotionStatus>>),
}

/// The motion transition to report for a batch of buffered updates: the
/// last Start/Stop, or the last update when every one is `NoChange`.
fn latest_motion(results: &[MotionStatus]) -> Option<MotionStatus> {
	results
		.iter()
		.rev()
		.find(|m| !matches!(m, MotionStatus::NoChange(_)))
		.or(results.last())
		.copied()
}

impl UpdateRx {
	fn try_recv(&mut self) -> std::result::Result<Result<AlarmUpdate>, TryRecvError> {
		match self {
			UpdateRx::Alarm(rx) => rx.try_recv(),
			#[cfg(any(test, feature = "test-util"))]
			UpdateRx::Motion(rx) => rx.try_recv().map(|m| m.map(AlarmUpdate::from)),
		}
	}

	async fn recv(&mut self) -> Option<Result<AlarmUpdate>> {
		match self {
			UpdateRx::Alarm(rx) => rx.recv().await,
			#[cfg(any(test, feature = "test-util"))]
			UpdateRx::Motion(rx) => rx.recv().await.map(|m| m.map(AlarmUpdate::from)),
		}
	}
}

/// A handle on current motion related events comming from the camera
///
/// When this object is dropped the motion events are stopped
pub struct MotionData {
	handle: JoinSet<Result<()>>,
	cancel: CancellationToken,
	rx: UpdateRx,
	/// Cached at construction so the `Drop` impl can spawn its
	/// detached cancel-and-join task without calling
	/// `tokio::runtime::Handle::current()` — that call panics if Drop
	/// runs from a thread with no current runtime, which can happen
	/// if a `MotionData` ever crosses a non-tokio drop site.
	rt_handle: tokio::runtime::Handle,
}

impl MotionData {
	fn from_parts(handle: JoinSet<Result<()>>, cancel: CancellationToken, rx: UpdateRx) -> Self {
		Self {
			handle,
			cancel,
			rx,
			rt_handle: tokio::runtime::Handle::current(),
		}
	}

	/// Consume the motion events diretly
	///
	/// Every buffered update is returned.
	///
	/// Draining keeps only each update's `motion` field; listeners that
	/// need AI objects or doorbell presses read [`Self::next_alarm_update`]
	/// instead. An error is raised if the motion connection to the camera
	/// is dropped
	pub fn consume_motion_events(&mut self) -> Result<Vec<MotionStatus>> {
		let mut results: Vec<MotionStatus> = vec![];
		loop {
			match self.rx.try_recv() {
				Ok(update) => results.push(update?.motion),
				Err(TryRecvError::Empty) => break,
				Err(e) => return Err(Error::from(e)),
			}
		}
		Ok(results)
	}

	/// Await a new motion event
	///
	/// Buffered events are coalesced: the most recent Start/Stop is
	/// returned, or `NoChange` when every buffered update is one.
	pub async fn next_motion(&mut self) -> Result<MotionStatus> {
		let motions = self.consume_motion_events()?;
		if let Some(change) = latest_motion(&motions) {
			Ok(change)
		} else {
			Ok(self.next_alarm_update().await?.motion)
		}
	}

	/// Await the next full alarm update: motion transition, AI objects
	/// and doorbell press.
	///
	/// Updates are delivered one at a time, in arrival order, so no
	/// doorbell press or object change is skipped. An error is raised if
	/// the motion connection to the camera is dropped.
	pub async fn next_alarm_update(&mut self) -> Result<AlarmUpdate> {
		self.rx
			.recv()
			.await
			.unwrap_or(Err(Error::Other("Motion dropped")))
	}
}

impl BcCamera {
	/// This message tells the camera to send the motion events to us
	/// Which are the received on msgid 33
	async fn start_motion_query(&self) -> Result<u16> {
		self.has_ability_rw("motion").await?;
		let connection = self.get_connection();

		let msg_num = self.new_message_num();
		let mut sub = connection.subscribe(MSG_ID_MOTION_REQUEST, msg_num).await?;
		let msg = Bc {
			meta: BcMeta {
				msg_id: MSG_ID_MOTION_REQUEST,
				channel_id: self.channel_id,
				msg_num,
				stream_type: 0,
				response_code: 0,
				class: 0x6414,
			},
			body: BcBody::ModernMsg(ModernMsg {
				..Default::default()
			}),
		};

		sub.send(msg).await?;

		let msg = sub.recv().await?;

		if let BcMeta {
			response_code: 200, ..
		} = msg.meta
		{
			Ok(msg_num)
		} else {
			Err(Error::UnintelligibleReply {
				reply: std::sync::Arc::new(msg),
				why: "The camera did not accept the request to start motion",
			})
		}
	}

	/// This returns a data structure which can be used to
	/// query motion events
	///
	/// Each update carries the push's raw `visitor` state and leaves
	/// `doorbell_press` `false`: a caller that publishes presses tracks
	/// them across pushes with a [`VisitorEdge`](super::VisitorEdge).
	pub async fn listen_on_motion(&self) -> Result<MotionData> {
		self.start_motion_query().await?;

		let connection = self.get_connection();

		// After start_motion_query (MSG_ID 31) the camera sends motion messages
		// when whenever motion is detected.
		let (tx, rx) = channel(20);

		let mut set = JoinSet::new();
		let channel_id = self.channel_id;
		let cancel = CancellationToken::new();
		let thread_cancel = cancel.clone();
		set.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					let mut sub = connection.subscribe_to_id(MSG_ID_MOTION).await?;

					loop {
						tokio::task::yield_now().await;
						let msg = sub.recv().await;
						// On connection drop the error is forwarded and the
						// receiver surfaces it.
						let update = msg.map(|motion_msg| match motion_msg.body {
							BcBody::ModernMsg(ModernMsg {
								payload:
									Some(BcPayloads::BcXml(BcXml {
										alarm_event_list: Some(alarm_event_list),
										..
									})),
								..
							}) => classify_alarm_events(
								&alarm_event_list.alarm_events,
								channel_id,
								Instant::now(),
							),
							_ => AlarmUpdate::no_change(Instant::now()),
						});

						if tx.send(update).await.is_err() {
							// Motion receiver has been dropped
							break;
						}
					}
					Ok(())
				} => v,
			}
		});

		Ok(MotionData::from_parts(set, cancel, UpdateRx::Alarm(rx)))
	}
}

impl MotionData {
	/// Construct a `MotionData` wired to a caller-supplied channel.
	///
	/// Gated on the `test-util` feature. Tests drive the returned handle
	/// by sending `MotionStatus` values on the matching `Sender` — no
	/// camera, no `JoinSet` task. The internal cancel token is unused
	/// (no background task to cancel) but is kept so `Drop` stays a
	/// no-op cancel.
	#[cfg(any(test, feature = "test-util"))]
	pub fn test_new(rx: tokio::sync::mpsc::Receiver<Result<MotionStatus>>) -> Self {
		Self::from_parts(
			JoinSet::new(),
			CancellationToken::new(),
			UpdateRx::Motion(rx),
		)
	}

	/// Construct a `MotionData` fed full [`AlarmUpdate`]s from a
	/// caller-supplied channel.
	///
	/// Gated on the `test-util` feature. Same shape as [`Self::test_new`];
	/// each `MotionStatus` seen by the motion methods is the update's
	/// `motion` field.
	#[cfg(any(test, feature = "test-util"))]
	pub fn test_new_alarm(rx: tokio::sync::mpsc::Receiver<Result<AlarmUpdate>>) -> Self {
		Self::from_parts(
			JoinSet::new(),
			CancellationToken::new(),
			UpdateRx::Alarm(rx),
		)
	}
}

impl Drop for MotionData {
	fn drop(&mut self) {
		log::trace!("Drop MotionData");
		self.cancel.cancel();
		let mut handle = std::mem::take(&mut self.handle);
		// Use the cached runtime handle rather than `Handle::current()`
		// so Drop is safe on any thread.
		self.rt_handle.spawn(async move {
			while handle.join_next().await.is_some() {}
			log::trace!("Dropped MotionData");
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bc_protocol::AiObjects;
	use std::time::Duration;

	#[tokio::test]
	async fn listen_on_motion_happy_path_returns_handle() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let md = cam.listen_on_motion().await.ok();
		assert!(md.is_some(), "listen_on_motion should succeed");
		// Drop MotionData; its Drop cancels the background task.
		drop(md);
	}

	#[tokio::test]
	async fn listen_on_motion_non_200_returns_err() {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(|req| reply_err_code(req, 500))
			.build()
			.await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let err = cam.listen_on_motion().await.err().expect("should fail");
		assert!(matches!(err, Error::UnintelligibleReply { .. }));
	}

	#[tokio::test]
	async fn listen_on_motion_missing_ability_returns_err() {
		let mock = MockConnection::new().build().await;
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_abilities_reported().await;
		let err = cam.listen_on_motion().await.err().expect("should fail");
		assert!(matches!(err, Error::MissingAbility { .. }));
	}

	// ---- coalescing regressions ----

	#[tokio::test]
	async fn next_motion_coalesces_to_last_change_over_trailing_no_change() {
		let (tx, rx) = channel::<Result<MotionStatus>>(8);
		let mut md = MotionData::test_new(rx);
		let start = Instant::now();
		tx.send(Ok(MotionStatus::Start(start))).await.unwrap();
		tx.send(Ok(MotionStatus::NoChange(Instant::now())))
			.await
			.unwrap();
		let got = tokio::time::timeout(Duration::from_secs(2), md.next_motion())
			.await
			.expect("no timeout")
			.expect("ok");
		assert_eq!(got, MotionStatus::Start(start));
	}

	#[tokio::test]
	async fn next_motion_all_no_change_reports_no_change() {
		let (tx, rx) = channel::<Result<MotionStatus>>(8);
		let mut md = MotionData::test_new(rx);
		tx.send(Ok(MotionStatus::NoChange(Instant::now())))
			.await
			.unwrap();
		tx.send(Ok(MotionStatus::NoChange(Instant::now())))
			.await
			.unwrap();
		let got = tokio::time::timeout(Duration::from_secs(2), md.next_motion())
			.await
			.expect("no timeout")
			.expect("ok");
		assert!(matches!(got, MotionStatus::NoChange(_)));
	}

	// ---- MotionData state-machine tests driven by `test_new` ----

	#[tokio::test]
	async fn consume_motion_events_propagates_err() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		tx.send(Err(Error::Other("boom"))).await.unwrap();
		tokio::task::yield_now().await;
		assert!(md.consume_motion_events().is_err());
	}

	#[tokio::test]
	async fn next_motion_consumes_buffered_event() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		let t = Instant::now();
		tx.send(Ok(MotionStatus::Start(t))).await.unwrap();
		tokio::task::yield_now().await;
		let status = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::Start(_)));
	}

	#[tokio::test]
	async fn next_motion_awaits_fresh_event() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Nothing buffered; spawn producer.
		let h = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(10)).await;
			tx.send(Ok(MotionStatus::Stop(Instant::now())))
				.await
				.unwrap();
		});
		let status = tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok");
		assert!(matches!(status, MotionStatus::Stop(_)));
		h.await.unwrap();
	}

	#[tokio::test]
	async fn next_motion_returns_err_when_channel_closed() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		drop(tx);
		// With no sender alive, try_recv yields Disconnected inside
		// consume_motion_events → DroppedConnectionTry. If the branch
		// ever changes to hit `rx.recv().await` → None, the Other arm
		// also counts.
		let err = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(
			err,
			Error::DroppedConnectionTry(_) | Error::Other(_)
		));
	}

	// ---- Inner listen_on_motion task body tests ----
	// These drive the MSG_ID_MOTION subscription loop by pushing frames
	// via the mock injector. The loop lives inside a spawned task inside
	// `listen_on_motion`; waiting on `md.next_motion()` drains it.

	/// Inject one cmd 33 alarm push carrying a single `<AlarmEvent>`.
	fn push_alarm(
		injector: &crate::bc_protocol::connection::mock::MockInjector,
		channel_id: u8,
		status: &str,
		ai_type: Option<&str>,
	) -> tokio::task::JoinHandle<()> {
		push_alarm_xml(
			injector,
			BcXml {
				alarm_event_list: Some(AlarmEventList {
					version: "1".into(),
					alarm_events: vec![AlarmEvent {
						version: "1".into(),
						channel_id,
						status: status.into(),
						recording: 0,
						timeStamp: 0,
						ai_type: ai_type.map(Into::into),
					}],
				}),
				..Default::default()
			},
		)
	}

	/// Inject one cmd 33 push on channel 0 with an arbitrary XML body.
	fn push_alarm_xml(
		injector: &crate::bc_protocol::connection::mock::MockInjector,
		xml: BcXml,
	) -> tokio::task::JoinHandle<()> {
		let injector = injector.clone();
		tokio::spawn(async move {
			let push = Bc::new_from_xml(
				BcMeta {
					msg_id: MSG_ID_MOTION,
					channel_id: 0,
					msg_num: 0,
					stream_type: 0,
					response_code: 0,
					class: 0x6414,
				},
				xml,
			);
			injector.push(push).await;
		})
	}

	/// `listen_on_motion` on a mock camera with motion ability, plus the
	/// injector for unsolicited pushes. Sleeps briefly so the cmd 33
	/// subscription is registered before the caller pushes.
	async fn listening_camera() -> (
		MotionData,
		crate::bc_protocol::connection::mock::MockInjector,
		BcCamera,
	) {
		let mock = MockConnection::new()
			.expect_msg(MSG_ID_MOTION_REQUEST)
			.reply_with(reply_200_empty)
			.build()
			.await;
		let injector = mock.injector();
		let cam = BcCamera::from_mock_connection(mock).await;
		cam.test_set_ability("motion", true).await;
		let md = cam.listen_on_motion().await.expect("ok");
		tokio::time::sleep(Duration::from_millis(30)).await;
		(md, injector, cam)
	}

	async fn next_motion_within(md: &mut MotionData) -> MotionStatus {
		tokio::time::timeout(Duration::from_millis(500), md.next_motion())
			.await
			.expect("did not hang")
			.expect("ok")
	}

	async fn next_alarm_update_within(md: &mut MotionData) -> AlarmUpdate {
		tokio::time::timeout(Duration::from_millis(500), md.next_alarm_update())
			.await
			.expect("did not hang")
			.expect("ok")
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_start_on_motion_frame() {
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "MD", None).await.unwrap();
		assert!(matches!(
			next_motion_within(&mut md).await,
			MotionStatus::Start(_)
		));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_stop_on_none_status_frame() {
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "none", Some("none"))
			.await
			.unwrap();
		assert!(matches!(
			next_motion_within(&mut md).await,
			MotionStatus::Stop(_)
		));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_emits_start_on_ai_type_not_none() {
		// status=="none" but ai_type != "none" → motion detected.
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "none", Some("person"))
			.await
			.unwrap();
		assert!(matches!(
			next_motion_within(&mut md).await,
			MotionStatus::Start(_)
		));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_returns_nochange_on_empty_xml() {
		// Frame without alarm_event_list → NoChange path.
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm_xml(&injector, BcXml::default()).await.unwrap();
		assert!(matches!(
			next_motion_within(&mut md).await,
			MotionStatus::NoChange(_)
		));
	}

	#[tokio::test]
	async fn listen_on_motion_inner_task_skips_other_channel_ids() {
		// Alarm event for a different channel_id (ours is 0) → NoChange.
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 7, "MD", None).await.unwrap();
		assert!(matches!(
			next_motion_within(&mut md).await,
			MotionStatus::NoChange(_)
		));
	}

	#[tokio::test]
	async fn listen_on_motion_delivers_ai_objects() {
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "MD", Some("people,vehicle"))
			.await
			.unwrap();
		let update = next_alarm_update_within(&mut md).await;
		assert!(matches!(update.motion, MotionStatus::Start(_)));
		assert_eq!(
			update.objects,
			Some(AiObjects {
				person: true,
				vehicle: true,
				..Default::default()
			})
		);
		assert!(!update.doorbell_press);
	}

	#[tokio::test]
	async fn listen_on_motion_visitor_push_reports_visitor_without_motion_change() {
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "visitor", None).await.unwrap();
		let update = next_alarm_update_within(&mut md).await;
		assert_eq!(update.visitor, Some(true));
		assert!(!update.doorbell_press, "the caller owns the press edge");
		assert!(matches!(update.motion, MotionStatus::NoChange(_)));
		assert_eq!(update.objects, None);
	}

	/// `visitor` is a state the doorbell repeats in every push for
	/// seconds after one press. The listener reports it raw, push by
	/// push, and leaves `doorbell_press` to the caller's `VisitorEdge`.
	#[tokio::test]
	async fn listen_on_motion_reports_the_raw_visitor_state_per_push() {
		let (mut md, injector, _cam) = listening_camera().await;
		let pushes: [(&str, &str); 12] = [
			("MD", "none"),
			("MD", "people"),
			("MD,visitor", "people"),
			("visitor", "people"),
			("none", "people"),
			("none", "none"),
			("MD", "people"),
			("MD,visitor", "people"),
			("MD,visitor", "none"),
			("MD,visitor", "people"),
			("visitor", "people"),
			("none", "none"),
		];
		for (status, ai_type) in pushes {
			push_alarm(&injector, 0, status, Some(ai_type))
				.await
				.unwrap();
		}
		let mut visitor = vec![];
		for _ in 0..pushes.len() {
			let update = next_alarm_update_within(&mut md).await;
			assert!(!update.doorbell_press);
			visitor.push(update.visitor == Some(true));
		}
		let held: Vec<bool> = pushes.iter().map(|(s, _)| s.contains("visitor")).collect();
		assert_eq!(visitor, held);
	}

	#[tokio::test]
	async fn listen_on_motion_daynight_push_yields_nothing() {
		let (mut md, injector, _cam) = listening_camera().await;
		push_alarm(&injector, 0, "dn", None).await.unwrap();
		let update = next_alarm_update_within(&mut md).await;
		// No motion, object or press change; the push reports no visitor.
		let expected = AlarmUpdate {
			visitor: Some(false),
			..AlarmUpdate::no_change(update_instant(&update))
		};
		assert_eq!(update, expected);
	}

	fn update_instant(update: &AlarmUpdate) -> Instant {
		match update.motion {
			MotionStatus::Start(t) | MotionStatus::Stop(t) | MotionStatus::NoChange(t) => t,
		}
	}

	// ---- full-update API driven by the test constructors ----

	#[tokio::test]
	async fn next_alarm_update_delivers_each_update_in_order() {
		// Unlike `next_motion`, buffered updates are not coalesced: a
		// press followed by a motion update yields both.
		let (tx, rx) = channel::<Result<AlarmUpdate>>(4);
		let mut md = MotionData::test_new_alarm(rx);
		let at = Instant::now();
		let press = AlarmUpdate {
			visitor: Some(true),
			doorbell_press: true,
			..AlarmUpdate::no_change(at)
		};
		let start = AlarmUpdate {
			motion: MotionStatus::Start(at),
			objects: Some(AiObjects {
				face: true,
				..Default::default()
			}),
			visitor: Some(false),
			doorbell_press: false,
		};
		tx.send(Ok(press)).await.unwrap();
		tx.send(Ok(start)).await.unwrap();
		assert_eq!(next_alarm_update_within(&mut md).await, press);
		assert_eq!(next_alarm_update_within(&mut md).await, start);
	}

	#[tokio::test]
	async fn next_motion_reads_motion_from_full_updates() {
		let (tx, rx) = channel::<Result<AlarmUpdate>>(4);
		let mut md = MotionData::test_new_alarm(rx);
		let at = Instant::now();
		tx.send(Ok(AlarmUpdate::from(MotionStatus::Stop(at))))
			.await
			.unwrap();
		assert_eq!(next_motion_within(&mut md).await, MotionStatus::Stop(at));
	}

	#[tokio::test]
	async fn next_alarm_update_wraps_motion_only_test_feed() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		let at = Instant::now();
		tx.send(Ok(MotionStatus::Start(at))).await.unwrap();
		assert_eq!(
			next_alarm_update_within(&mut md).await,
			AlarmUpdate::from(MotionStatus::Start(at))
		);
	}

	#[tokio::test]
	async fn next_alarm_update_propagates_err_and_close() {
		let (tx, rx) = channel::<Result<AlarmUpdate>>(4);
		let mut md = MotionData::test_new_alarm(rx);
		tx.send(Err(Error::Other("boom"))).await.unwrap();
		drop(tx);
		for expected in ["boom", "Motion dropped"] {
			let got = tokio::time::timeout(Duration::from_millis(200), md.next_alarm_update())
				.await
				.expect("did not hang");
			assert!(
				matches!(got, Err(Error::Other(msg)) if msg == expected),
				"expected Other({expected:?})"
			);
		}
	}

	/// `next_motion` falls through to `rx.recv().await` returning `None`
	/// (sender dropped while we were already past the buffered drain),
	/// surfacing `Other("Motion dropped")`.
	#[tokio::test]
	async fn next_motion_returns_other_when_sender_dropped_after_drain() {
		let (tx, rx) = channel::<Result<MotionStatus>>(4);
		let mut md = MotionData::test_new(rx);
		// Spawn a task that closes the sender after a short delay so
		// `next_motion` enters its inner `rx.recv().await`, which then
		// returns `None` and surfaces `Other("Motion dropped")`.
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(20)).await;
			drop(tx);
		});
		let err = tokio::time::timeout(Duration::from_millis(200), md.next_motion())
			.await
			.expect("did not hang")
			.expect_err("should fail");
		assert!(matches!(err, Error::Other("Motion dropped")));
	}
}
