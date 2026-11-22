use super::BcSubscription;
use crate::{Error, Result, bc::model::*};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, channel, error::TrySendError};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

#[derive(Default)]
struct Subscriber {
	/// Subscribers based on their ID and their num
	/// First filtered by ID then number
	/// If num is None it will be upgraded to a Some based on the number the
	/// camera assigns
	num: BTreeMap<u32, BTreeMap<Option<u16>, Sender<Result<Bc>>>>,
	/// Subscribers based on their ID
	id: BTreeMap<u32, Arc<MsgHandler>>,
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<Bc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
	sink: Sender<Result<Bc>>,
	poll_commander: Sender<PollCommand>,
	rx_thread: RwLock<JoinSet<Result<()>>>,
	cancel: CancellationToken,
}

impl BcConnection {
	pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
		let (sinker, sinker_rx) = channel::<Result<Bc>>(100);
		let cancel = CancellationToken::new();

		let (poll_commander, poll_commanded) = channel(200);
		let mut poller = Poller {
			subscribers: Default::default(),
			sink: sinker.clone(),
			receiver: ReceiverStream::new(poll_commanded),
		};

		let mut rx_thread = JoinSet::<Result<()>>::new();
		let thread_poll_commander = poll_commander.clone();
		let thread_cancel = cancel.clone();
		rx_thread.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => {
					Result::Ok(())
				},
				v = async {
					let sender = thread_poll_commander;
					while let Some(bc) = source.next().await {
						sender.send(PollCommand::Bc(Box::new(bc))).await?;
					}
					// A clean EOF still has to reach the poller: it fans the
					// error out so subscribers fail fast instead of waiting
					// on a socket that will never deliver again.
					sender
						.send(PollCommand::Bc(Box::new(Err(Error::DroppedConnection))))
						.await?;
					Result::Ok(())
				} => v
			}
		});

		let thread_cancel = cancel.clone();
		rx_thread.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = async {
					let mut stream = ReceiverStream::new(sinker_rx);
					while let Some(packet) = stream.next().await {
						sink.send(packet?).await?;
					}
					Ok(())
				} => v
			}
		});

		let thread_cancel = cancel.clone();
		rx_thread.spawn(async move {
			tokio::select! {
				_ = thread_cancel.cancelled() => Result::Ok(()),
				v = poller.run() => {
					trace!("Polling has ended");
					v
				}
			}
		});

		Ok(BcConnection {
			sink: sinker,
			poll_commander,
			rx_thread: RwLock::new(rx_thread),
			cancel,
		})
	}

	pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
		self.sink.send(Ok(bc)).await?;
		Ok(())
	}

	pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription<'_>> {
		let (tx, rx) = channel(100);
		self.poll_commander
			.send(PollCommand::AddSubscriber(msg_id, Some(msg_num), tx))
			.await?;
		Ok(BcSubscription::new(rx, Some(msg_num as u32), self))
	}

	/// Some messages are initiated by the camera. This creates a handler for them
	/// It requires a closure that will be used to handle the message
	/// and return either None or Some(Bc) reply
	///
	/// Fails with [`Error::SimultaneousSubscriptionId`] if `msg_id` already
	/// has a handler; the existing handler and the connection are kept.
	pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<()>
	where
		T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
	{
		let (reply, added) = oneshot::channel();
		self.poll_commander
			.send(PollCommand::AddHandler(msg_id, Arc::new(handler), reply))
			.await?;
		added.await.map_err(|_| Error::DroppedConnection)?
	}

	/// Some times we want to wait for a reply on a new message ID
	/// to do this we wait for the next packet with a certain ID
	/// grab it's message ID and then subscribe to that ID
	///
	/// The command Snap that grabs a jpeg payload is an example of this
	///
	/// This function creates a temporary handle to grab this single message
	pub async fn subscribe_to_id(&self, msg_id: u32) -> Result<BcSubscription<'_>> {
		let (tx, rx) = channel(100);
		self.poll_commander
			.send(PollCommand::AddSubscriber(msg_id, None, tx))
			.await?;
		Ok(BcSubscription::new(rx, None, self))
	}

	pub async fn shutdown(&self) -> Result<()> {
		// Cancel first: awaiting a send into a full command channel would
		// stall shutdown behind the very backlog it is meant to clear.
		self.cancel.cancel();
		let _ = self.poll_commander.try_send(PollCommand::Disconnect);
		let mut locked_threads = self.rx_thread.write().await;
		while locked_threads.join_next().await.is_some() {}
		Ok(())
	}

	/// Test-only: number of tasks still alive in the rx JoinSet. Drives
	/// the regression test that proves `shutdown` clears every spawned
	/// task — without an external counter, a future task added without
	/// wiring it to the cancel token would silently leak past shutdown
	/// and the lock-based loop in `shutdown` would hang on it.
	#[cfg(test)]
	pub(crate) async fn task_count(&self) -> usize {
		self.rx_thread.read().await.len()
	}
}

impl Drop for BcConnection {
	fn drop(&mut self) {
		log::trace!("Drop BcConnection");
		self.cancel.cancel();

		let poll_commander = self.poll_commander.clone();
		// `try_current` instead of `current` so a Drop that fires outside
		// a Tokio runtime (panic-during-unwind, runtime shutdown, sync
		// move out of `#[tokio::test]`) doesn't double-panic and abort.
		// Cancel-token already fired synchronously above; the spawned
		// drain is best-effort.
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			log::trace!("Drop BcConnection: no runtime; skipping cleanup spawn");
			return;
		};
		let _gt = handle.enter();
		let mut threads = std::mem::take(&mut self.rx_thread);
		tokio::task::spawn(async move {
			let _ = poll_commander.try_send(PollCommand::Disconnect);
			let locked_threads = threads.get_mut();
			while locked_threads.join_next().await.is_some() {}
			log::trace!("Dropped BcConnection");
		});
	}
}

enum PollCommand {
	Bc(Box<Result<Bc>>),
	AddHandler(u32, Arc<MsgHandler>, oneshot::Sender<Result<()>>),
	AddSubscriber(u32, Option<u16>, Sender<Result<Bc>>),
	Disconnect,
}

impl std::fmt::Debug for PollCommand {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
			PollCommand::AddHandler(..) => f.write_str("PollCommand::AddHandler"),
			PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
			PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
		}
	}
}

struct Poller {
	subscribers: Subscriber,
	sink: Sender<Result<Bc>>,
	receiver: ReceiverStream<PollCommand>,
}

impl Poller {
	async fn run(&mut self) -> Result<()> {
		let cancel = CancellationToken::new();
		let _dropguard = cancel.clone().drop_guard();
		while let Some(command) = self.receiver.next().await {
			// Clean Up subscribers
			self.subscribers
				.num
				.iter_mut()
				.for_each(|(_, channels)| channels.retain(|_, channel| !channel.is_closed()));
			self.subscribers
				.num
				.retain(|_, channels| !channels.is_empty());
			// Handle the command
			match command {
				PollCommand::Bc(boxed_response) => {
					match *boxed_response {
						Ok(response) => {
							let msg_id = response.meta.msg_id;
							let msg_num = response.meta.msg_num;
							log::trace!(
								"Looking for ID: {} with num: {}, in {:?} and {:?}",
								msg_id,
								msg_num,
								self.subscribers.id.keys().to_owned(),
								self.subscribers
									.num
									.iter()
									.map(|(k, v)| (k, v.keys()))
									.collect::<Vec<_>>(),
							);
							match (
								self.subscribers.id.get(&msg_id),
								self.subscribers.num.get_mut(&msg_id), // Both filter first on ID
							) {
								(Some(occ), _) => {
									log::trace!("Calling ID callback");
									let occ = occ.clone();
									let sink = self.sink.clone();
									// Move this on another thread coz I have NO idea
									// how long the callback will run for
									// and we must NOT hang
									let cancel = cancel.clone();
									tokio::task::spawn(async move {
										tokio::select! {
											_ = cancel.cancelled() => Result::Ok(()),
											v = occ(&response) => {
												if let Some(reply) = v {
													// `debug_assert!` rather than `assert!`:
													// `response.meta.msg_num` is wire-derived
													// and the closure is application-supplied.
													// A mismatch on this invariant should log
													// and drop the reply, not abort the entire
													// connection task on a hard panic.
													if reply.meta.msg_num != response.meta.msg_num {
														log::warn!(
															"reply.msg_num ({}) != response.msg_num ({}); dropping",
															reply.meta.msg_num,
															response.meta.msg_num,
														);
														return Ok(());
													}
													sink.send(Ok(reply)).await?;
												}
												Result::Ok(())
											}
										}
									});
									log::trace!("Called ID callback");
								}
								(None, Some(occ)) => {
									let sender = if let Some(sender) =
										occ.get(&Some(msg_num)).filter(|a| !a.is_closed()).cloned()
									{
										// Connection with id exists and is not closed
										Some(sender)
									} else if let Some(sender) = occ.get(&None).cloned() {
										// Upgrade a None to a known MsgID
										occ.remove(&None);
										occ.insert(Some(msg_num), sender.clone());
										Some(sender)
									} else if occ
										.get(&Some(msg_num))
										.map(|a| a.is_closed())
										.unwrap_or(false)
									{
										// Connection is closed and there is no None to replace it
										// Remove it for cleanup and report no sender
										occ.remove(&Some(msg_num));
										None
									} else {
										None
									};
									if let Some(sender) = sender {
										// Never await a subscriber: one stalled
										// consumer would block routing for every
										// other one, keepalive replies included.
										match sender.try_send(Ok(response)) {
											Ok(()) => {}
											Err(TrySendError::Full(_)) => {
												error!(
													"Subscriber for msg_id {msg_id} (num {msg_num}) is not keeping up; dropping it"
												);
												occ.remove(&Some(msg_num));
											}
											Err(TrySendError::Closed(_)) => {
												occ.remove(&Some(msg_num));
											}
										}
									} else {
										trace!(
											"Ignoring uninteresting message id {} (number: {})",
											msg_id, msg_num
										);
										trace!("Contents: {:?}", response);
									}
								}
								(None, None) => {
									trace!(
										"Ignoring uninteresting message id {} (number: {})",
										msg_id, msg_num
									);
									trace!("Contents: {:?}", response);
								}
							}
						}
						Err(e) => {
							for (msg_id, sub) in &self.subscribers.num {
								for (msg_num, sender) in sub {
									if let Err(TrySendError::Full(_)) =
										sender.try_send(Err(e.clone()))
									{
										// Dropped below either way; it sees end of stream.
										error!(
											"Subscriber for msg_id {msg_id} (num {msg_num:?}) is full; dropping it without the error"
										);
									}
								}
							}
							self.subscribers.num.clear();
							self.subscribers.id.clear();
							return Err(e);
						}
					}
				}
				PollCommand::AddHandler(msg_id, handler, reply) => {
					// A duplicate is the caller's error, not the connection's:
					// report it back and keep polling.
					let added = match self.subscribers.id.entry(msg_id) {
						Entry::Vacant(vac_entry) => {
							vac_entry.insert(handler);
							Ok(())
						}
						Entry::Occupied(_) => {
							warn!("A handler for msg_id {msg_id} already exists; keeping it");
							Err(Error::SimultaneousSubscriptionId { msg_id })
						}
					};
					let _ = reply.send(added);
				}
				PollCommand::AddSubscriber(msg_id, msg_num, tx) => {
					match self
						.subscribers
						.num
						.entry(msg_id)
						.or_default()
						.entry(msg_num)
					{
						Entry::Vacant(vac_entry) => {
							vac_entry.insert(tx);
						}
						Entry::Occupied(mut occ_entry) => {
							if occ_entry.get().is_closed() {
								occ_entry.insert(tx);
							} else {
								// log::error!("Failed to subscribe in bcconn to {:?} for {:?}", msg_num, msg_id);
								let _ =
									tx.try_send(Err(Error::SimultaneousSubscription { msg_num }));
							}
						}
					};
				}
				PollCommand::Disconnect => {
					return Err(Error::ConnectionShutdown);
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::sink::drain;
	use std::time::Duration;

	/// A connection whose inbound side is fed by the returned sender and
	/// whose outbound side discards everything.
	async fn conn() -> (BcConnection, Sender<Result<Bc>>) {
		let (tx, rx) = channel::<Result<Bc>>(1000);
		let sink: BcConnSink = Box::new(drain().sink_map_err(|never| match never {}));
		let source: BcConnSource = Box::new(ReceiverStream::new(rx));
		(BcConnection::new(sink, source).await.unwrap(), tx)
	}

	fn msg(msg_id: u32, msg_num: u16) -> Bc {
		Bc::new_from_meta(BcMeta {
			msg_id,
			channel_id: 0,
			stream_type: 0,
			response_code: 200,
			msg_num,
			class: 0x6414,
		})
	}

	#[tokio::test]
	async fn clean_eof_fails_subscribers() {
		let (conn, tx) = conn().await;
		let mut sub = conn.subscribe(5, 1).await.unwrap();
		drop(tx);
		let got = tokio::time::timeout(Duration::from_secs(2), sub.recv())
			.await
			.expect("subscriber must not hang on EOF");
		assert!(matches!(got, Err(Error::DroppedConnection)), "{got:?}");
	}

	#[tokio::test]
	async fn stalled_subscriber_does_not_block_others() {
		let (conn, tx) = conn().await;
		let mut stalled = conn.subscribe(5, 1).await.unwrap();
		let mut live = conn.subscribe(6, 1).await.unwrap();
		for _ in 0..150 {
			tx.send(Ok(msg(5, 1))).await.unwrap();
		}
		tx.send(Ok(msg(6, 1))).await.unwrap();
		let got = tokio::time::timeout(Duration::from_secs(2), live.recv())
			.await
			.expect("routing blocked behind a full subscriber");
		assert_eq!(got.unwrap().meta.msg_id, 6);

		// The stalled one keeps what was queued, then sees its end.
		for _ in 0..100 {
			stalled.recv().await.unwrap();
		}
		assert!(matches!(
			stalled.recv().await,
			Err(Error::DroppedSubscriber)
		));
	}
}
