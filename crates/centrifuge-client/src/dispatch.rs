//! Event delivery (docs/spec.md §9.4–§9.5).
//!
//! A step's events are delivered as one batch; the driver waits for the ack
//! before the next step. Handlers run one at a time, on a dedicated thread
//! ([`EventDelivery::Thread`]) or on the driver task ([`EventDelivery::Inline`]).

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use centrifuge_client_core::machine::Event;
use centrifuge_client_core::types::Publication;
use tokio::sync::{mpsc, oneshot, watch};

use crate::client::{Client, ClientInner};
use crate::config::EventDelivery;
use crate::error::{ClientError, SubscriptionError};
use crate::events::{
    ClientEvent, ConnectedEvent, ConnectingEvent, DisconnectedEvent, ErrorEvent, JoinEvent,
    LeaveEvent, MessageEvent, PublicationEvent, ServerJoinEvent, ServerLeaveEvent,
    ServerPublicationEvent, ServerSubscribedEvent, ServerSubscribingEvent, ServerUnsubscribedEvent,
    SubscribedEvent, SubscribingEvent, SubscriptionErrorEvent, SubscriptionEvent,
    UnsubscribedEvent, connected_event, subscribed_event,
};
use crate::shared::{Shared, SubInner, lock};
use crate::subscription::Subscription;

thread_local! {
    /// The client whose handler is running on this thread (§9.5).
    static IN_HANDLER: Cell<u64> = const { Cell::new(0) };
}

tokio::task_local! {
    /// The client whose token or state callback this task runs (§9.5).
    pub(crate) static IN_CALLBACK: u64;
}

/// Whether the current thread runs a handler of client `id`.
pub(crate) fn in_handler(id: u64) -> bool {
    IN_HANDLER.with(Cell::get) == id
}

/// Whether the current task runs a token or state callback of client `id`.
pub(crate) fn in_callback(id: u64) -> bool {
    IN_CALLBACK.try_with(|c| *c == id).unwrap_or(false)
}

struct HandlerScope(u64);

impl HandlerScope {
    fn enter(id: u64) -> Self {
        Self(IN_HANDLER.with(|c| c.replace(id)))
    }
}

impl Drop for HandlerScope {
    fn drop(&mut self) {
        IN_HANDLER.with(|c| c.set(self.0));
    }
}

type H<T, E> = Option<Box<dyn FnMut(&T, E) + Send>>;

/// Client handler slots. A handler is taken out while it runs, so registering
/// one from inside a handler is safe (the Rust form of pattern 32).
#[derive(Default)]
pub(crate) struct ClientHandlers {
    pub(crate) connecting: H<Client, ConnectingEvent>,
    pub(crate) connected: H<Client, ConnectedEvent>,
    pub(crate) disconnected: H<Client, DisconnectedEvent>,
    pub(crate) error: H<Client, ErrorEvent>,
    pub(crate) message: H<Client, MessageEvent>,
    pub(crate) server_subscribed: H<Client, ServerSubscribedEvent>,
    pub(crate) server_subscribing: H<Client, ServerSubscribingEvent>,
    pub(crate) server_unsubscribed: H<Client, ServerUnsubscribedEvent>,
    pub(crate) server_publication: H<Client, ServerPublicationEvent>,
    pub(crate) server_join: H<Client, ServerJoinEvent>,
    pub(crate) server_leave: H<Client, ServerLeaveEvent>,
    /// Bumped by every registration, so a handler taken out isn't put back over
    /// one registered while it ran.
    pub(crate) generation: u64,
}

/// Subscription handler slots.
#[derive(Default)]
pub(crate) struct SubHandlers {
    pub(crate) subscribing: H<Subscription, SubscribingEvent>,
    pub(crate) subscribed: H<Subscription, SubscribedEvent>,
    pub(crate) unsubscribed: H<Subscription, UnsubscribedEvent>,
    pub(crate) error: H<Subscription, SubscriptionErrorEvent>,
    pub(crate) publication: H<Subscription, PublicationEvent>,
    pub(crate) join: H<Subscription, JoinEvent>,
    pub(crate) leave: H<Subscription, LeaveEvent>,
    pub(crate) generation: u64,
}

/// A hand-off stream: the dispatcher offers one item and waits until the
/// consumer's `next()` takes it (§9.4). At most one per kind and object; a new
/// one takes over and the old one ends.
pub(crate) struct StreamSlot<T> {
    tx: Option<Offerer<T>>,
}

/// Where a stream's items are offered: its one-slot channel and its tail.
type Offerer<T> = (mpsc::Sender<(T, oneshot::Sender<()>)>, Tail<T>);

/// Items offered while the stream's slot was still full, which only happens
/// once the client is stopping (nobody waits then): the stream yields them
/// after the channel, before it ends, so the final events aren't lost.
pub(crate) type Tail<T> = Arc<std::sync::Mutex<std::collections::VecDeque<T>>>;

/// Bound on [`Tail`]: a close emits one event per stream.
const TAIL_MAX: usize = 16;

impl<T> Default for StreamSlot<T> {
    fn default() -> Self {
        Self { tx: None }
    }
}

impl<T> StreamSlot<T> {
    pub(crate) fn open(&mut self) -> (mpsc::Receiver<(T, oneshot::Sender<()>)>, Tail<T>) {
        let (tx, rx) = mpsc::channel(1);
        let tail = Tail::default();
        self.tx = Some((tx, tail.clone()));
        (rx, tail)
    }

    pub(crate) fn close(&mut self) {
        self.tx = None;
    }

    fn sender(&self) -> Option<Offerer<T>> {
        self.tx.as_ref().filter(|(t, _)| !t.is_closed()).cloned()
    }
}

/// The outcome of one batch.
#[derive(Debug, Default)]
pub(crate) struct BatchResult {
    pub(crate) panic: Option<String>,
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "event handler panicked".into())
}

/// Runs a handler slot: take it out, call it with panics caught, put it back
/// unless a newer one was registered meanwhile.
macro_rules! run_slot {
    ($lock:expr, $field:ident, $handle:expr, $ev:expr, $res:expr, $client_id:expr) => {{
        let (taken, generation) = {
            let mut h = lock($lock);
            (h.$field.take(), h.generation)
        };
        if let Some(mut f) = taken {
            let scope = HandlerScope::enter($client_id);
            let outcome = catch_unwind(AssertUnwindSafe(|| f($handle, $ev)));
            drop(scope);
            {
                let mut h = lock($lock);
                if h.generation == generation && h.$field.is_none() {
                    h.$field = Some(f);
                }
            }
            if let Err(p) = outcome {
                let msg = panic_message(&*p);
                tracing::error!(target: "centrifuge", "event handler panicked: {msg}");
                $res.panic.get_or_insert(msg);
            }
        }
    }};
}

/// Hands an item to a stream: offer and wait until it's taken.
enum Offer<T> {
    Sent(oneshot::Receiver<()>),
    Full(T),
    Gone,
}

fn offer<T>(tx: &mpsc::Sender<(T, oneshot::Sender<()>)>, item: T) -> Offer<T> {
    let (ack_tx, ack_rx) = oneshot::channel();
    match tx.try_send((item, ack_tx)) {
        Ok(()) => Offer::Sent(ack_rx),
        Err(mpsc::error::TrySendError::Full((item, _))) => Offer::Full(item),
        Err(mpsc::error::TrySendError::Closed(_)) => Offer::Gone,
    }
}

/// Delivers one batch of core events. Synchronous: called on the dispatcher
/// thread, or on the driver task in inline mode (where streams use
/// [`deliver_async`]).
#[derive(Clone)]
pub(crate) struct Delivery {
    pub(crate) client: Weak<ClientInner>,
    pub(crate) shared: Arc<Shared>,
}

/// What a stream delivery needs to wait for.
pub(crate) type Pending = Vec<oneshot::Receiver<()>>;

impl Delivery {
    /// Runs the handlers of `events` and offers stream items. Returns the batch
    /// result and acks still to wait for (streams).
    #[allow(clippy::too_many_lines)] // one arm per event kind
    pub(crate) fn run(
        &self,
        events: Vec<Event>,
        waits: &mut Pending,
        blocking: bool,
    ) -> BatchResult {
        let mut res = BatchResult::default();
        let Some(inner) = self.client.upgrade() else {
            return res;
        };
        let client = Client::from_inner(inner);
        let id = self.shared.id;
        for ev in events {
            match ev {
                Event::Connecting { code, reason } => {
                    let e = ConnectingEvent { code, reason };
                    run_slot!(
                        &self.shared.handlers,
                        connecting,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::Connecting(e), waits, blocking);
                }
                Event::Connected {
                    client_id,
                    version,
                    data,
                    transport,
                } => {
                    let e = connected_event(client_id, version, data, transport);
                    run_slot!(
                        &self.shared.handlers,
                        connected,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::Connected(e), waits, blocking);
                }
                Event::Disconnected { code, reason } => {
                    let e = DisconnectedEvent { code, reason };
                    run_slot!(
                        &self.shared.handlers,
                        disconnected,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::Disconnected(e), waits, blocking);
                }
                Event::Error {
                    kind,
                    message,
                    source,
                } => {
                    let e = ErrorEvent {
                        error: ClientError {
                            kind,
                            message,
                            source,
                        },
                    };
                    tracing::debug!(target: "centrifuge", client = id, error = %e.error, "client error");
                    // A panic in the error handler stops the client like any other
                    // handler panic, but is never re-emitted as another error.
                    run_slot!(&self.shared.handlers, error, &client, e.clone(), res, id);
                    self.client_stream(ClientEvent::Error(e), waits, blocking);
                }
                Event::Message { data } => {
                    let e = MessageEvent { data };
                    run_slot!(&self.shared.handlers, message, &client, e.clone(), res, id);
                    self.client_stream(ClientEvent::Message(e), waits, blocking);
                }
                Event::ServerSubscribed { channel, info } => {
                    let e = ServerSubscribedEvent {
                        channel,
                        info: subscribed_event(info),
                    };
                    run_slot!(
                        &self.shared.handlers,
                        server_subscribed,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerSubscribed(e), waits, blocking);
                }
                Event::ServerSubscribing { channel } => {
                    let e = ServerSubscribingEvent { channel };
                    run_slot!(
                        &self.shared.handlers,
                        server_subscribing,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerSubscribing(e), waits, blocking);
                }
                Event::ServerUnsubscribed { channel } => {
                    let e = ServerUnsubscribedEvent { channel };
                    run_slot!(
                        &self.shared.handlers,
                        server_unsubscribed,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerUnsubscribed(e), waits, blocking);
                }
                Event::ServerPublication {
                    channel,
                    publication,
                } => {
                    let e = ServerPublicationEvent {
                        channel,
                        publication,
                    };
                    run_slot!(
                        &self.shared.handlers,
                        server_publication,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerPublication(e), waits, blocking);
                }
                Event::ServerJoin { channel, info } => {
                    let e = ServerJoinEvent { channel, info };
                    run_slot!(
                        &self.shared.handlers,
                        server_join,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerJoin(e), waits, blocking);
                }
                Event::ServerLeave { channel, info } => {
                    let e = ServerLeaveEvent { channel, info };
                    run_slot!(
                        &self.shared.handlers,
                        server_leave,
                        &client,
                        e.clone(),
                        res,
                        id
                    );
                    self.client_stream(ClientEvent::ServerLeave(e), waits, blocking);
                }
                Event::Subscribing { sub, code, reason } => {
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Subscribing(SubscribingEvent { code, reason }),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                Event::Subscribed { sub, info } => {
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Subscribed(subscribed_event(info)),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                Event::Unsubscribed { sub, code, reason } => {
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Unsubscribed(UnsubscribedEvent { code, reason }),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                Event::SubscriptionError {
                    sub,
                    kind,
                    message,
                    source,
                } => {
                    let e = SubscriptionErrorEvent {
                        error: SubscriptionError {
                            kind,
                            message,
                            source,
                        },
                    };
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Error(e),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                Event::Publication { sub, publication } => {
                    self.sub_publication(&client, sub, publication, &mut res, waits, blocking);
                }
                Event::Join { sub, info } => {
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Join(JoinEvent { info }),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                Event::Leave { sub, info } => {
                    self.sub_event(
                        &client,
                        sub,
                        SubscriptionEvent::Leave(LeaveEvent { info }),
                        &mut res,
                        waits,
                        blocking,
                    );
                }
                _ => {}
            }
        }
        res
    }

    fn client_stream(&self, ev: ClientEvent, waits: &mut Pending, blocking: bool) {
        let tx = lock(&self.shared.events_stream).sender();
        if let Some(tx) = tx {
            hand_off(&tx, ev, waits, blocking);
        }
    }

    fn sub_inner(&self, sub: u64) -> Option<Arc<SubInner>> {
        lock(&self.shared.by_key).get(&sub).cloned()
    }

    fn sub_event(
        &self,
        client: &Client,
        sub: u64,
        ev: SubscriptionEvent,
        res: &mut BatchResult,
        waits: &mut Pending,
        blocking: bool,
    ) {
        let Some(inner) = self.sub_inner(sub) else {
            return;
        };
        let handle = Subscription::from_parts(inner.clone(), client.clone());
        let id = self.shared.id;
        match ev.clone() {
            SubscriptionEvent::Subscribing(e) => {
                run_slot!(&inner.handlers, subscribing, &handle, e, *res, id);
            }
            SubscriptionEvent::Subscribed(e) => {
                run_slot!(&inner.handlers, subscribed, &handle, e, *res, id);
            }
            SubscriptionEvent::Unsubscribed(e) => {
                run_slot!(&inner.handlers, unsubscribed, &handle, e, *res, id);
            }
            SubscriptionEvent::Error(e) => run_slot!(&inner.handlers, error, &handle, e, *res, id),
            SubscriptionEvent::Publication(e) => {
                run_slot!(&inner.handlers, publication, &handle, e, *res, id);
            }
            SubscriptionEvent::Join(e) => run_slot!(&inner.handlers, join, &handle, e, *res, id),
            SubscriptionEvent::Leave(e) => run_slot!(&inner.handlers, leave, &handle, e, *res, id),
        }
        let tx = lock(&inner.events_stream).sender();
        if let Some(tx) = tx {
            hand_off(&tx, ev, waits, blocking);
        }
    }

    fn sub_publication(
        &self,
        client: &Client,
        sub: u64,
        publication: Publication,
        res: &mut BatchResult,
        waits: &mut Pending,
        blocking: bool,
    ) {
        let e = PublicationEvent { publication };
        self.sub_event(
            client,
            sub,
            SubscriptionEvent::Publication(e.clone()),
            res,
            waits,
            blocking,
        );
        let Some(inner) = self.sub_inner(sub) else {
            return;
        };
        let tx = lock(&inner.publications_stream).sender();
        if let Some(tx) = tx {
            hand_off(&tx, e, waits, blocking);
        }
    }
}

fn hand_off<T>((tx, tail): &Offerer<T>, item: T, waits: &mut Pending, _blocking: bool) {
    // The channel has room for one item; it's empty here, because every
    // previous offer was waited for before, unless the client is stopping
    // (nobody waits then): the item goes to the tail.
    match offer(tx, item) {
        // The driver waits until the consumer's next() takes it. A dropped
        // stream drops the ack: it counts as taken (no listener).
        Offer::Sent(ack) => waits.push(ack),
        Offer::Full(item) => {
            let mut t = lock(tail);
            if t.len() < TAIL_MAX {
                t.push_back(item);
            }
        }
        Offer::Gone => {}
    }
}

/// One event for the dispatcher thread; it answers with the result and the
/// stream acks still to wait for.
pub(crate) struct Batch {
    pub(crate) events: Vec<Event>,
    pub(crate) done: oneshot::Sender<(BatchResult, Pending)>,
}

/// Where batches go.
pub(crate) enum Dispatcher {
    Thread {
        tx: std::sync::mpsc::Sender<Batch>,
        /// The thread is gone: that was reported once, and events are now
        /// delivered inline with `fallback` (docs/spec.md §9.4).
        gone: AtomicBool,
        fallback: Delivery,
    },
    Inline(Delivery),
}

/// Starts a thread; a parameter so tests can make it fail.
type Spawn = fn(std::thread::Builder, Box<dyn FnOnce() + Send>) -> std::io::Result<()>;

fn os_spawn(b: std::thread::Builder, f: Box<dyn FnOnce() + Send>) -> std::io::Result<()> {
    b.spawn(f).map(drop)
}

/// Starts the `centrifuge-events` thread. Without it no event would ever be
/// delivered, so a failure is an error for `build()`, not a log line. On
/// targets without threads (wasm32-unknown-unknown) spawning always fails.
fn start_event_thread(spawn: Spawn, f: Box<dyn FnOnce() + Send>) -> Result<(), crate::Error> {
    let builder = std::thread::Builder::new().name("centrifuge-events".into());
    spawn(builder, f).map_err(|e| {
        crate::Error::Configuration(format!(
            "can't start the event thread for EventDelivery::Thread: {e}; \
             use EventDelivery::Inline where threads aren't available"
        ))
    })
}

/// Why the batch in which the event thread stopped doesn't count as delivered.
const THREAD_GONE: &str =
    "the event thread is gone (an SDK bug); later events are delivered on the client's task";

/// The event thread stopped taking events. Handler panics are caught, so only
/// an SDK bug gets here. Marks it gone and logs; true only the first time.
fn thread_gone(gone: &AtomicBool) -> bool {
    let first = !gone.swap(true, Ordering::Relaxed);
    if first {
        tracing::error!(target: "centrifuge", "{THREAD_GONE}");
    }
    first
}

/// One event on the calling task (inline mode, or after the thread is gone).
fn run_inline(d: &Delivery, ev: Event) -> (BatchResult, Pending) {
    let mut waits = Vec::new();
    let r = d.run(vec![ev], &mut waits, false);
    (r, waits)
}

impl Dispatcher {
    /// # Errors
    ///
    /// [`Error::Configuration`](crate::Error::Configuration) if the event
    /// thread can't be started.
    pub(crate) fn new(
        mode: EventDelivery,
        delivery: Delivery,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, crate::Error> {
        Self::with_spawn(mode, delivery, runtime, os_spawn)
    }

    fn with_spawn(
        mode: EventDelivery,
        delivery: Delivery,
        runtime: tokio::runtime::Handle,
        spawn: Spawn,
    ) -> Result<Self, crate::Error> {
        Ok(match mode {
            EventDelivery::Inline => Self::Inline(delivery),
            EventDelivery::Thread => {
                let (tx, rx) = std::sync::mpsc::channel::<Batch>();
                let fallback = delivery.clone();
                start_event_thread(
                    spawn,
                    Box::new(move || {
                        let _rt = runtime.enter();
                        while let Ok(batch) = rx.recv() {
                            let mut waits = Vec::new();
                            let res = delivery.run(batch.events, &mut waits, false);
                            let _ = batch.done.send((res, waits));
                        }
                    }),
                )?;
                Self::Thread {
                    tx,
                    gone: AtomicBool::new(false),
                    fallback,
                }
            }
        })
    }

    /// Delivers a step's events and waits for them, without blocking the
    /// runtime. One event at a time: each stream item must be taken before the
    /// next is offered (one-slot hand-off). Once `stop` is set (`close()`, the
    /// last handle dropped), streams are no longer waited for: an offered item
    /// stays buffered for the stream, which ends after it.
    pub(crate) async fn deliver(
        &self,
        events: Vec<Event>,
        stop: &mut watch::Receiver<bool>,
    ) -> BatchResult {
        let mut res = BatchResult::default();
        for ev in events {
            let (r, waits) = match self {
                Self::Thread { gone, fallback, .. } if gone.load(Ordering::Relaxed) => {
                    run_inline(fallback, ev)
                }
                Self::Thread { tx, gone, fallback } => {
                    let (done, rx) = oneshot::channel();
                    match tx.send(Batch {
                        events: vec![ev],
                        done,
                    }) {
                        Ok(()) => {
                            if let Ok(r) = rx.await {
                                r
                            } else {
                                // The thread stopped while handling this event,
                                // so it may have been half delivered: like a
                                // handler panic, the batch doesn't count as
                                // delivered and the client stops loudly. No
                                // second delivery, which could duplicate it.
                                if thread_gone(gone) {
                                    res.panic.get_or_insert_with(|| THREAD_GONE.into());
                                }
                                (BatchResult::default(), Vec::new())
                            }
                        }
                        // The event never reached the thread: deliver it here.
                        Err(std::sync::mpsc::SendError(batch)) => {
                            if thread_gone(gone) {
                                res.panic.get_or_insert_with(|| THREAD_GONE.into());
                            }
                            let mut waits = Vec::new();
                            let r = fallback.run(batch.events, &mut waits, false);
                            (r, waits)
                        }
                    }
                }
                Self::Inline(d) => run_inline(d, ev),
            };
            if res.panic.is_none() {
                res.panic = r.panic;
            }
            for w in waits {
                if *stop.borrow() {
                    break;
                }
                tokio::select! {
                    _ = w => {}
                    _ = stop.wait_for(|s| *s) => {}
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};

    use super::{
        Delivery, Dispatcher, Event, EventDelivery, THREAD_GONE, os_spawn, start_event_thread,
        thread_gone, watch,
    };

    #[test]
    fn a_failed_spawn_is_a_configuration_error() {
        fn failing(_: std::thread::Builder, _: Box<dyn FnOnce() + Send>) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "no threads",
            ))
        }
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        let err = start_event_thread(failing, Box::new(move || r.store(true, Ordering::SeqCst)))
            .unwrap_err();
        assert!(matches!(err, crate::Error::Configuration(_)), "{err:?}");
        let msg = err.to_string();
        assert!(msg.contains("no threads"), "{msg}");
        assert!(msg.contains("EventDelivery::Inline"), "{msg}");
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[test]
    fn the_event_thread_starts_and_is_named() {
        let (tx, rx) = mpsc::channel();
        start_event_thread(
            os_spawn,
            Box::new(move || {
                let _ = tx.send(std::thread::current().name().map(str::to_owned));
            }),
        )
        .unwrap();
        assert_eq!(rx.recv().unwrap().as_deref(), Some("centrifuge-events"));
    }

    #[test]
    fn a_gone_thread_is_reported_once() {
        let gone = AtomicBool::new(false);
        assert!(thread_gone(&gone), "the first call reports");
        assert!(!thread_gone(&gone), "later calls don't");
        assert!(gone.load(Ordering::Relaxed));
    }

    fn delivery() -> Delivery {
        let net = crate::transport::net::Net::new(
            crate::ProxyConfig::None,
            &crate::TlsConfig::new(),
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let shared = crate::shared::Shared::new(
            tokio::sync::mpsc::unbounded_channel().0,
            crate::shared::CallConfig {
                protocol: centrifuge_client_core::codec::Protocol::Json,
                timeout: std::time::Duration::from_secs(1),
            },
            http::HeaderMap::new(),
            Arc::new(net),
        );
        Delivery {
            client: std::sync::Weak::new(),
            shared: Arc::new(shared),
        }
    }

    fn error_event() -> Event {
        Event::Error {
            kind: centrifuge_client_core::machine::ClientErrorKind::Connect,
            message: "test".into(),
            source: None,
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    #[test]
    fn build_fails_when_the_thread_cant_start() {
        fn failing(_: std::thread::Builder, _: Box<dyn FnOnce() + Send>) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "no threads",
            ))
        }
        let rt = runtime();
        let r = Dispatcher::with_spawn(
            EventDelivery::Thread,
            delivery(),
            rt.handle().clone(),
            failing,
        );
        assert!(matches!(r, Err(crate::Error::Configuration(_))));
        // Inline never spawns.
        let r = Dispatcher::with_spawn(
            EventDelivery::Inline,
            delivery(),
            rt.handle().clone(),
            failing,
        );
        assert!(r.is_ok());
    }

    #[test]
    fn a_dead_thread_fails_the_batch_once_then_delivers_inline() {
        // "Starts" the thread but drops it: as if it had died at once.
        #[allow(clippy::unnecessary_wraps)] // must match `Spawn`
        fn dead(_: std::thread::Builder, _: Box<dyn FnOnce() + Send>) -> std::io::Result<()> {
            Ok(())
        }
        let rt = runtime();
        let d =
            Dispatcher::with_spawn(EventDelivery::Thread, delivery(), rt.handle().clone(), dead)
                .unwrap();
        let (_tx, mut stop) = watch::channel(false);
        // The first batch doesn't count as delivered, so the core stops the
        // client loudly and commits nothing.
        let first = rt.block_on(d.deliver(vec![error_event(), error_event()], &mut stop));
        assert_eq!(first.panic.as_deref(), Some(THREAD_GONE));
        // Later batches (the error and disconnect the core emits for it) are
        // delivered inline, so they reach the app and nothing loops.
        let later = rt.block_on(d.deliver(vec![error_event()], &mut stop));
        assert_eq!(later.panic, None);
    }
}
