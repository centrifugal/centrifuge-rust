//! The driver task (docs/spec.md §9.2): owns the [`Core`], does all IO, and
//! hands each step's events to the dispatcher, waiting for the ack.

use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use centrifuge_client_core::time::Instant as CoreInstant;

use centrifuge_client_core::machine::{
    Callback, CallbackError, Core, Event, Input, Op, OpenRequest, Outputs, State, SubKey, SubState,
    TimerKey, TransportId, TransportKind,
};
use centrifuge_client_core::proto;
use futures_util::FutureExt as _;
use tokio::sync::{Semaphore, mpsc, watch};
use url::Url;

use crate::dispatch::Dispatcher;
use crate::error::TokenError;
use crate::maybe_send::{dyn_send, dyn_send_sync};
use crate::rt;
use crate::shared::{Command, SessionHandle, Shared, Snapshot, SubSnapshot, lock};
use crate::transport::{
    self, Inbound, LastReceived, READ_AHEAD_BYTES, READ_AHEAD_ITEMS, WriteReq, WriterTx,
};

pub(crate) type TokenFuture =
    Pin<Box<dyn_send!(Future<Output = Result<Option<String>, TokenError>>)>>;
pub(crate) type ConnTokenGetter =
    Arc<dyn_send_sync!(Fn(crate::client::ConnectionTokenContext) -> TokenFuture)>;
pub(crate) type SubTokenFuture = Pin<Box<dyn_send!(Future<Output = Result<String, TokenError>>)>>;
pub(crate) type SubTokenGetter =
    Arc<dyn_send_sync!(Fn(crate::subscription::SubscriptionTokenContext) -> SubTokenFuture)>;
pub(crate) type StateFuture = Pin<
    Box<
        dyn_send!(
            Future<
                Output = Result<
                    centrifuge_client_core::types::StreamPosition,
                    crate::error::BoxError,
                >,
            >
        ),
    >,
>;
pub(crate) type StateGetter =
    Arc<dyn_send_sync!(Fn(crate::subscription::SubscriptionGetStateContext) -> StateFuture)>;

/// Per-subscription callbacks, looked up by key.
#[derive(Default, Clone)]
pub(crate) struct SubCallbacks {
    pub(crate) get_token: Option<SubTokenGetter>,
    pub(crate) get_state: Option<StateGetter>,
}

pub(crate) struct DriverConfig {
    pub(crate) endpoints: Vec<(TransportKind, Url)>,
    pub(crate) emulation_endpoint: Option<Url>,
    pub(crate) max_message_size: usize,
    pub(crate) get_token: Option<ConnTokenGetter>,
    /// The browser `fetch` credentials mode.
    #[cfg(target_arch = "wasm32")]
    pub(crate) credentials: crate::config::Credentials,
}

struct TransportHandle {
    task: rt::AbortHandle,
    closing: watch::Sender<bool>,
    writer: Option<WriterTx>,
}

pub(crate) struct Driver {
    core: Core,
    shared: Arc<Shared>,
    cfg: DriverConfig,
    dispatcher: Dispatcher,
    commands: mpsc::UnboundedReceiver<Command>,
    inbound_tx: mpsc::Sender<Inbound>,
    inbound: mpsc::Receiver<Inbound>,
    callbacks_tx: mpsc::UnboundedSender<Input>,
    callbacks: mpsc::UnboundedReceiver<Input>,
    transports: HashMap<TransportId, TransportHandle>,
    pending_writes: Vec<(TransportId, proto::Command)>,
    budget: Arc<Semaphore>,
    last_received: Arc<LastReceived>,
    applied_seq: u64,
    /// The watchdog's last reading: wall clock (`None` where it's invalid,
    /// wasm only) and the real monotonic clock.
    wall: (Option<SystemTime>, CoreInstant),
    stopping: bool,
    /// `close()` or the last handle dropped: don't wait for stream consumers.
    stop: watch::Receiver<bool>,
    /// Token and state callbacks in flight, aborted on shutdown.
    tasks: Vec<rt::AbortHandle>,
    finished: bool,
    /// Browser lifecycle listeners, feeding `callbacks`; removed with the
    /// driver.
    #[cfg(target_arch = "wasm32")]
    _lifecycle: Option<crate::lifecycle::Listeners>,
}

impl Drop for Driver {
    /// Also when the driver task is dropped mid-step (a panic, the runtime
    /// shutting down): waiters fail, streams end, `close()` resolves.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A timer that runs this much late means the process was suspended (§9.7).
const LATE: Duration = Duration::from_secs(1);
/// The wall clock moving this much more than the monotonic one is a wake.
const WAKE_GAP: Duration = Duration::from_secs(3);
/// Inputs taken per loop iteration before writes are flushed (§9.10).
const BURST: usize = 64;

enum Next {
    Command(Command),
    Deliver,
    Inbound(Inbound),
    Callback(Input),
    Timer(TimerKey, bool),
    Watchdog,
}

impl Driver {
    pub(crate) fn new(
        core: Core,
        shared: Arc<Shared>,
        cfg: DriverConfig,
        dispatcher: Dispatcher,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (inbound_tx, inbound) = mpsc::channel(READ_AHEAD_ITEMS);
        let stop = shared.stop.subscribe();
        let (callbacks_tx, callbacks) = mpsc::unbounded_channel();
        #[cfg(target_arch = "wasm32")]
        let lifecycle = crate::lifecycle::Listeners::attach(&callbacks_tx);
        Self {
            core,
            shared,
            cfg,
            dispatcher,
            commands,
            inbound_tx,
            inbound,
            callbacks_tx,
            callbacks,
            transports: HashMap::new(),
            pending_writes: Vec::new(),
            budget: Arc::new(Semaphore::new(READ_AHEAD_BYTES as usize)),
            last_received: Arc::new(LastReceived::new()),
            applied_seq: 0,
            wall: (rt::system_now(), CoreInstant::now()),
            stopping: false,
            stop,
            tasks: Vec::new(),
            finished: false,
            #[cfg(target_arch = "wasm32")]
            _lifecycle: lifecycle,
        }
    }

    fn now() -> CoreInstant {
        rt::to_core(rt::Instant::now())
    }

    pub(crate) async fn run(mut self) {
        let mut watchdog = rt::interval(Duration::from_secs(2));
        loop {
            let Some(next) = self.wait_next(&mut watchdog).await else {
                break;
            };
            self.handle(next).await;
            for _ in 1..BURST {
                if self.stopping {
                    break;
                }
                match self.try_next() {
                    Some(n) => self.handle(n).await,
                    None => break,
                }
            }
            self.flush_writes();
            if self.stopping {
                break;
            }
        }
        self.shutdown();
    }

    async fn wait_next(&mut self, watchdog: &mut rt::Interval) -> Option<Next> {
        let deadline = self.core.next_deadline();
        let has_delivery = self.core.has_pending_delivery();
        let sleep = async {
            match deadline {
                Some((at, _)) => rt::sleep_until(rt::from_core(at)).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            c = self.commands.recv() => Some(c.map_or(Next::Command(Command::Dropped), Next::Command)),
            () = std::future::ready(()), if has_delivery => Some(Next::Deliver),
            Some(i) = self.inbound.recv() => Some(Next::Inbound(i)),
            Some(c) = self.callbacks.recv() => Some(Next::Callback(c)),
            () = sleep => {
                let (at, key) = deadline?;
                Some(Next::Timer(key, Self::now().saturating_duration_since(at) >= LATE))
            }
            _ = watchdog.tick() => Some(Next::Watchdog),
        }
    }

    fn try_next(&mut self) -> Option<Next> {
        if let Ok(c) = self.commands.try_recv() {
            return Some(Next::Command(c));
        }
        if self.core.has_pending_delivery() {
            return Some(Next::Deliver);
        }
        if let Ok(i) = self.inbound.try_recv() {
            return Some(Next::Inbound(i));
        }
        if let Ok(c) = self.callbacks.try_recv() {
            return Some(Next::Callback(c));
        }
        if let Some((at, key)) = self.core.next_deadline() {
            let now = Self::now();
            if at <= now {
                return Some(Next::Timer(key, now.saturating_duration_since(at) >= LATE));
            }
        }
        None
    }

    async fn handle(&mut self, next: Next) {
        match next {
            Next::Command(Command::Op(seq_op)) => {
                let seq = seq_op.seq;
                let is_close = matches!(seq_op.op, Op::Close);
                let removed = match seq_op.op {
                    Op::RemoveSubscription(key) => Some(key),
                    _ => None,
                };
                self.step(Input::App(seq_op.op), Some(seq)).await;
                if let Some(key) = removed {
                    // After the cleanup unsubscribe was delivered: its streams end
                    // (pattern 60).
                    if let Some(inner) = lock(&self.shared.by_key).remove(&key) {
                        lock(&inner.events_stream).close();
                        lock(&inner.publications_stream).close();
                    }
                }
                if is_close {
                    self.stopping = true;
                }
            }
            Next::Command(Command::Dropped) => {
                self.step(Input::App(Op::Close), None).await;
                self.stopping = true;
            }
            Next::Deliver => self.step(Input::DeliverNext, None).await,
            Next::Inbound(i) => self.on_inbound(i).await,
            Next::Callback(i) => self.step(i, None).await,
            Next::Timer(key, late) => {
                let last_received = self.last_received.get();
                self.step(
                    Input::TimerFired {
                        key,
                        late,
                        last_received,
                    },
                    None,
                )
                .await;
            }
            Next::Watchdog => self.watchdog().await,
        }
    }

    async fn watchdog(&mut self) {
        let (wall0, mono0) = self.wall;
        let (wall1, mono1) = (rt::system_now(), CoreInstant::now());
        self.wall = (wall1, mono1);
        // An invalid reading skips this check; the next valid one is the base.
        let (Some(wall0), Some(wall1)) = (wall0, wall1) else {
            return;
        };
        let wall = wall1.duration_since(wall0).unwrap_or_default();
        let mono = mono1.duration_since(mono0);
        if wall > mono + WAKE_GAP {
            // The system slept: the monotonic clock didn't move meanwhile.
            self.step(
                Input::Resumed {
                    slept: wall.saturating_sub(mono),
                },
                None,
            )
            .await;
        }
    }

    async fn on_inbound(&mut self, i: Inbound) {
        match i {
            Inbound::Opened { transport, writer } => {
                if let Some(h) = self.transports.get_mut(&transport) {
                    h.writer = Some(writer);
                }
                self.step(Input::Opened { transport }, None).await;
            }
            Inbound::Item {
                transport,
                reply,
                _permit: permit,
            } => {
                self.step(Input::Item { transport, reply }, None).await;
                // The read-ahead budget is released once the item is handled.
                drop(permit);
            }
            Inbound::Closed { transport, reason } => {
                self.step(Input::Closed { transport, reason }, None).await;
            }
            Inbound::WriteFailed {
                transport,
                command_ids,
                had_pong,
            } => {
                self.step(
                    Input::WriteFailed {
                        transport,
                        command_ids,
                        had_pong,
                    },
                    None,
                )
                .await;
            }
        }
    }

    /// One core step and everything it leads to: outputs applied, events
    /// delivered (ack awaited), commits applied, snapshots published.
    async fn step(&mut self, input: Input, seq: Option<u64>) {
        let mut out = self.core.step(input, Self::now());
        if let Some(seq) = seq {
            self.applied_seq = seq;
        }
        loop {
            let events = std::mem::take(&mut out.events);
            let fail = std::mem::take(&mut out.fail_calls);
            self.apply(out);
            let touched = touched_subs(&events);
            if events.is_empty() {
                self.publish(&touched, true);
                self.sweep(&fail);
                return;
            }
            // Commands of this step go out before waiting for the handlers, so a
            // slow handler never holds back a pong or an unsubscribe (§9.2).
            self.flush_writes();
            self.publish(&touched, false);
            let res = self.dispatcher.deliver(events, &mut self.stop).await;
            // Close first, then fail the calls (pattern 47).
            self.sweep(&fail);
            out = self.core.step(
                Input::Delivered {
                    ok: res.panic.is_none(),
                    panic: res.panic,
                },
                Self::now(),
            );
            self.publish(&touched, true);
        }
    }

    fn sweep(&self, transports: &[TransportId]) {
        if transports.is_empty() {
            return;
        }
        let mut calls = lock(&self.shared.calls);
        for t in transports {
            calls.sweep(*t);
        }
    }

    fn apply(&mut self, out: Outputs) {
        for t in out.close {
            self.close_transport(t);
        }
        if let Some(req) = out.open {
            self.open(req);
        }
        self.pending_writes.extend(out.commands);
        for cb in out.callbacks {
            self.spawn_callback(cb);
        }
    }

    fn flush_writes(&mut self) {
        if self.pending_writes.is_empty() {
            return;
        }
        let session = self.core.session();
        let mut by_transport: HashMap<TransportId, Vec<proto::Command>> = HashMap::new();
        for (t, c) in self.pending_writes.drain(..) {
            by_transport.entry(t).or_default().push(c);
        }
        for (t, cmds) in by_transport {
            let Some(writer) = self.transports.get(&t).and_then(|h| h.writer.clone()) else {
                continue;
            };
            let emu = session
                .as_ref()
                .filter(|s| s.transport == t && s.kind.uses_emulation())
                .map(|s| (s.session.clone(), s.node.clone()));
            let _ = writer.send(WriteReq::core(cmds, emu));
        }
    }

    /// Publishes the client snapshot and those of `subs`. `ready` is set only
    /// after the step's handlers ran (§9.4).
    fn publish(&self, subs: &[SubKey], after_delivery: bool) {
        let state = self.core.state();
        let session = self.core.session().and_then(|s| {
            let writer = self.transports.get(&s.transport)?.writer.clone()?;
            Some(SessionHandle {
                transport: s.transport,
                kind: s.kind,
                writer,
                session: s.session,
                node: s.node,
            })
        });
        let snap = Snapshot {
            state,
            disconnects: self.core.disconnects(),
            applied_seq: self.applied_seq,
            ready: after_delivery && state == State::Connected,
            session,
            core_stats: self.core.stats(),
        };
        if state == State::Connected {
            let mut since = lock(&self.shared.connected_since);
            if since.is_none() {
                *since = rt::system_now();
            }
        } else {
            *lock(&self.shared.connected_since) = None;
        }
        // Subscriptions first: whoever sees the new `applied_seq` must also see
        // the subscription states it led to.
        {
            let by_key = lock(&self.shared.by_key);
            for key in subs {
                let (Some(inner), Some((st, _session, unsubscribes))) =
                    (by_key.get(key), self.core.subscription(*key))
                else {
                    continue;
                };
                inner.snapshot.send_replace(SubSnapshot {
                    state: st,
                    unsubscribes,
                    ready: after_delivery && st == SubState::Subscribed,
                });
            }
        }
        self.shared.snapshot.send_replace(snap);
    }

    fn open(&mut self, req: OpenRequest) {
        let Some((kind, url)) = self.cfg.endpoints.get(req.index).cloned() else {
            return;
        };
        let (closing_tx, closing_rx) = watch::channel(false);
        self.last_received.reset();
        let ctx = transport::Ctx {
            id: req.transport,
            kind,
            url,
            protocol: self.core.protocol(),
            max_message_size: self.cfg.max_message_size,
            http_headers: lock(&self.shared.http_headers).clone(),
            emulation_endpoint: self.cfg.emulation_endpoint.clone(),
            timeout: self.shared.cfg.timeout,
            net: self.shared.net.clone(),
            shared: self.shared.clone(),
            inbound: self.inbound_tx.clone(),
            budget: self.budget.clone(),
            last_received: self.last_received.clone(),
            closing: closing_rx,
            #[cfg(target_arch = "wasm32")]
            credentials: self.cfg.credentials,
        };
        let task = rt::spawn(transport::run(ctx, req.initial));
        self.transports.insert(
            req.transport,
            TransportHandle {
                task,
                closing: closing_tx,
                writer: None,
            },
        );
    }

    fn close_transport(&mut self, t: TransportId) {
        if let Some(h) = self.transports.remove(&t) {
            // The writer sends a close frame in the background; the reader (and
            // an open in progress) are aborted at once (§5.1).
            let _ = h.closing.send(true);
            h.task.abort();
        }
    }

    fn spawn_callback(&mut self, cb: Callback) {
        self.tasks.retain(|t| !t.is_finished());
        let tx = self.callbacks_tx.clone();
        let id = self.shared.id;
        match cb {
            Callback::ConnectionToken { flow } => {
                let Some(getter) = self.cfg.get_token.clone() else {
                    let _ = tx.send(Input::TokenSettled {
                        flow,
                        result: Ok(None),
                    });
                    return;
                };
                let task = rt::spawn(rt::callback_scope(id, async move {
                    let fut = AssertUnwindSafe(async move {
                        getter(crate::client::ConnectionTokenContext::new()).await
                    })
                    .catch_unwind();
                    let result = settle(fut.await.unwrap_or_else(|p| Err(panicked(&*p))));
                    let _ = tx.send(Input::TokenSettled { flow, result });
                }));
                self.tasks.push(task);
            }
            Callback::SubscriptionToken { flow, sub, channel } => {
                let getter = lock(&self.shared.by_key)
                    .get(&sub)
                    .and_then(|s| s.callbacks.get_token.clone());
                let Some(getter) = getter else {
                    let _ = tx.send(Input::TokenSettled {
                        flow,
                        result: Ok(None),
                    });
                    return;
                };
                let task = rt::spawn(rt::callback_scope(id, async move {
                    let ctx = crate::subscription::SubscriptionTokenContext::new(channel);
                    let fut = AssertUnwindSafe(async move { getter(ctx).await }).catch_unwind();
                    let result = settle(fut.await.unwrap_or_else(|p| Err(panicked(&*p))).map(Some));
                    let _ = tx.send(Input::TokenSettled { flow, result });
                }));
                self.tasks.push(task);
            }
            Callback::GetState { flow, sub, channel } => {
                let getter = lock(&self.shared.by_key)
                    .get(&sub)
                    .and_then(|s| s.callbacks.get_state.clone());
                let Some(getter) = getter else { return };
                let task = rt::spawn(rt::callback_scope(id, async move {
                    let ctx = crate::subscription::SubscriptionGetStateContext::new(channel);
                    let fut = AssertUnwindSafe(async move { getter(ctx).await }).catch_unwind();
                    let result = match fut.await {
                        Ok(Ok(pos)) => Ok(pos),
                        Ok(Err(e)) => Err(CallbackError::Other(Arc::from(e))),
                        Err(p) => Err(CallbackError::Other(Arc::new(PanicError(panic_text(&*p))))),
                    };
                    let _ = tx.send(Input::StateSettled { flow, result });
                }));
                self.tasks.push(task);
            }
            _ => {}
        }
    }

    /// Idempotent: from the end of `run()`, or from `Drop`.
    fn shutdown(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        let ids: Vec<_> = self.transports.keys().copied().collect();
        for t in ids {
            self.close_transport(t);
        }
        for t in self.tasks.drain(..) {
            t.abort();
        }
        // Whatever stopped the driver, the client is closed from now on.
        self.shared.stop.send_replace(true);
        self.shared.snapshot.send_modify(|s| {
            // Every op is settled: nothing queued will ever be applied, so the
            // projected state of an unapplied connect() must not linger.
            s.applied_seq = u64::MAX;
            s.state = State::Closed;
            s.ready = false;
            s.session = None;
        });
        lock(&self.shared.calls).fail_all(&crate::Error::ClientClosed);
        // Mark closed before ending the streams: a stream asked for meanwhile
        // sees it and ends at once (§9.4).
        let waiters = lock(&self.shared.close_waiters).take().unwrap_or_default();
        lock(&self.shared.events_stream).close();
        let subs: Vec<_> = lock(&self.shared.registry).values().cloned().collect();
        let removed: Vec<_> = lock(&self.shared.by_key).values().cloned().collect();
        for inner in subs.iter().chain(&removed) {
            lock(&inner.events_stream).close();
            lock(&inner.publications_stream).close();
        }
        for w in waiters {
            let _ = w.send(());
        }
    }
}

fn touched_subs(events: &[Event]) -> Vec<SubKey> {
    let mut v: Vec<SubKey> = events
        .iter()
        .filter_map(|e| match e {
            Event::Subscribing { sub, .. }
            | Event::Subscribed { sub, .. }
            | Event::Unsubscribed { sub, .. }
            | Event::SubscriptionError { sub, .. }
            | Event::Publication { sub, .. }
            | Event::Join { sub, .. }
            | Event::Leave { sub, .. } => Some(*sub),
            _ => None,
        })
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

#[derive(Debug)]
struct PanicError(String);

impl std::fmt::Display for PanicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "callback panicked: {}", self.0)
    }
}

impl std::error::Error for PanicError {}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

fn panicked(p: &(dyn std::any::Any + Send)) -> TokenError {
    TokenError::Other(Box::new(PanicError(panic_text(p))))
}

fn settle(r: Result<Option<String>, TokenError>) -> Result<Option<String>, CallbackError> {
    match r {
        Ok(t) => Ok(t),
        Err(TokenError::Unauthorized) => Err(CallbackError::Unauthorized),
        Err(TokenError::Other(e)) => Err(CallbackError::Other(Arc::from(e))),
    }
}
