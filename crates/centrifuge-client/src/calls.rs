//! The call fast path (docs/spec.md §9.3).
//!
//! `publish`, `rpc`, `history`, `presence`, `presence_stats` and `send` don't
//! change SDK state, so they skip the driver: the reader resolves their replies
//! from the call table directly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use centrifuge_client_core::machine::{State, SubState, TransportId};
use centrifuge_client_core::proto;
use tokio::sync::oneshot;

use crate::dispatch::in_handler;
use crate::error::Error;
use crate::rt::{self, in_callback};
use crate::shared::{Shared, SubInner, lock};
use crate::transport::WriteReq;

type ReplyTx = oneshot::Sender<Result<proto::Reply, Error>>;

/// Calls waiting for a reply, by command id.
#[derive(Default)]
pub(crate) struct CallTable {
    calls: HashMap<u32, (TransportId, ReplyTx)>,
    /// Transports torn down: inserting for them is refused, so no call is left
    /// behind after a sweep.
    closed: HashSet<TransportId>,
}

impl CallTable {
    fn insert(&mut self, id: u32, transport: TransportId, tx: ReplyTx) -> Result<(), ReplyTx> {
        if self.closed.contains(&transport) {
            return Err(tx);
        }
        self.calls.insert(id, (transport, tx));
        Ok(())
    }

    /// Resolves a reply; `false` if nobody waits for it (TLA+ `Sdk_StaleReply`).
    pub(crate) fn resolve(&mut self, reply: proto::Reply) -> bool {
        match self.calls.remove(&reply.id) {
            Some((_, tx)) => {
                let _ = tx.send(Ok(reply));
                true
            }
            None => false,
        }
    }

    pub(crate) fn fail(&mut self, id: u32, err: Error) {
        if let Some((_, tx)) = self.calls.remove(&id) {
            let _ = tx.send(Err(err));
        }
    }

    /// Fails every call of a transport (after its close was reported).
    pub(crate) fn sweep(&mut self, transport: TransportId) {
        self.closed.insert(transport);
        // Transport ids only grow: keep the most recent ones, so the set
        // doesn't grow with every reconnect. An insert for a transport this old
        // comes from a long-stale snapshot and just times out.
        while self.closed.len() > 64 {
            let Some(oldest) = self.closed.iter().min().copied() else {
                break;
            };
            self.closed.remove(&oldest);
        }
        let ids: Vec<u32> = self
            .calls
            .iter()
            .filter(|(_, (t, _))| *t == transport)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.fail(id, Error::ClientDisconnected);
        }
    }

    pub(crate) fn fail_all(&mut self, err: &Error) {
        for (_, (_, tx)) in self.calls.drain() {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

/// Removes the call when its future is dropped (cancel safety, §4.3).
struct CallGuard<'a> {
    shared: &'a Shared,
    id: u32,
}

impl Drop for CallGuard<'_> {
    fn drop(&mut self) {
        lock(&self.shared.calls).calls.remove(&self.id);
    }
}

/// Grace for a timer that ran far too late (a suspended process, §9.7).
const LATE: Duration = Duration::from_secs(1);

/// Waits until the client's ops before this call are applied, then until it's
/// connected (and, for subscription calls, until the subscription is
/// subscribed). Calls from a handler or a callback can't wait for the loop.
async fn wait_ready(
    shared: &Shared,
    sub: Option<&SubInner>,
    deadline: rt::Instant,
) -> Result<(), Error> {
    let seq = shared
        .requested_seq
        .load(std::sync::atomic::Ordering::SeqCst);
    let id = shared.id;
    let nested = in_handler(id) || in_callback(id);
    let mut rx = shared.snapshot.subscribe();
    let start = {
        let s = rx.borrow();
        (s.disconnects, sub.map(|s| s.snapshot.borrow().unsubscribes))
    };
    let mut sub_rx = sub.map(|s| s.snapshot.subscribe());
    loop {
        {
            // Copies, not held borrows: `state()` reads the same watches, and a
            // second read lock deadlocks behind a waiting writer.
            let (snap_state, applied, disconnects, has_session) = {
                let s = rx.borrow_and_update();
                (s.state, s.applied_seq, s.disconnects, s.session.is_some())
            };
            // Closed is final: nothing after it is ever applied.
            if snap_state == State::Closed {
                return Err(Error::ClientClosed);
            }
            let caught_up = applied >= seq || nested;
            if caught_up {
                // The state as of this call (projected inside handlers).
                let state = if nested { shared.state() } else { snap_state };
                if state == State::Closed {
                    return Err(Error::ClientClosed);
                }
                if disconnects != start.0 && state != State::Connected {
                    return Err(Error::ClientDisconnected);
                }
                match sub {
                    None => {
                        if state == State::Connected && has_session {
                            return Ok(());
                        }
                    }
                    Some(s) => {
                        let ss = s.snapshot.borrow().clone();
                        let sub_state = if nested { s.state(shared) } else { ss.state };
                        if sub_state == SubState::Unsubscribed || Some(ss.unsubscribes) != start.1 {
                            return Err(Error::SubscriptionUnsubscribed);
                        }
                        if sub_state == SubState::Subscribed
                            && state == State::Connected
                            && has_session
                        {
                            return Ok(());
                        }
                    }
                }
            }
            if nested {
                // Waiting would wait for the handler or callback itself.
                return Err(Error::WouldDeadlock);
            }
        }
        let changed = async {
            match sub_rx.as_mut() {
                Some(srx) => tokio::select! {
                    r = rx.changed() => r.is_ok(),
                    r = srx.changed() => r.is_ok(),
                },
                None => rx.changed().await.is_ok(),
            }
        };
        match rt::timeout_at(deadline, changed).await {
            Ok(true) => {}
            Ok(false) => return Err(Error::ClientClosed),
            Err(_) => return Err(Error::Timeout),
        }
    }
}

/// Sends a call command and waits for its reply.
pub(crate) async fn call(
    shared: &Arc<Shared>,
    sub: Option<&SubInner>,
    mut cmd: proto::Command,
) -> Result<proto::Reply, Error> {
    let timeout = shared.cfg.timeout;
    let deadline = rt::Instant::now() + timeout.min(crate::config::MAX_DURATION);
    wait_ready(shared, sub, deadline).await?;
    let session = shared
        .snapshot
        .borrow()
        .session
        .clone()
        .ok_or(Error::ClientDisconnected)?;
    let id = shared.next_call_id();
    cmd.id = id;
    let (tx, rx) = oneshot::channel();
    if lock(&shared.calls)
        .insert(id, session.transport, tx)
        .is_err()
    {
        return Err(Error::ClientDisconnected);
    }
    let guard = CallGuard { shared, id };
    if session
        .writer
        .send(WriteReq::call(cmd, id, &session))
        .is_err()
    {
        return Err(Error::ClientDisconnected);
    }
    let res = wait_reply(rx, deadline).await;
    drop(guard);
    let reply = res?;
    if let Some(e) = reply.error {
        return Err(Error::Server(e.into()));
    }
    Ok(reply)
}

async fn wait_reply(
    mut rx: oneshot::Receiver<Result<proto::Reply, Error>>,
    deadline: rt::Instant,
) -> Result<proto::Reply, Error> {
    tokio::select! {
        biased;
        r = &mut rx => return r.unwrap_or(Err(Error::ClientDisconnected)),
        () = rt::sleep_until(deadline) => {}
    }
    // A reply already received wins over the timeout (pattern 28); a timer that
    // ran far too late gets one grace period.
    if rt::Instant::now().duration_since(deadline) >= LATE {
        if let Ok(r) = rt::timeout(LATE, &mut rx).await {
            return r.unwrap_or(Err(Error::ClientDisconnected));
        }
    } else if let Ok(r) = rx.try_recv() {
        return r;
    }
    Err(Error::Timeout)
}

/// `send()`: fire and forget (no reply), resolves once written to a connected
/// transport.
pub(crate) async fn send(shared: &Arc<Shared>, cmd: proto::Command) -> Result<(), Error> {
    let deadline = rt::Instant::now() + shared.cfg.timeout.min(crate::config::MAX_DURATION);
    wait_ready(shared, None, deadline).await?;
    let session = shared
        .snapshot
        .borrow()
        .session
        .clone()
        .ok_or(Error::ClientDisconnected)?;
    // Resolves once written (backpressure on a slow socket), not once queued.
    let (tx, rx) = oneshot::channel();
    session
        .writer
        .send(WriteReq::send(cmd, &session, tx))
        .map_err(|_| Error::ClientDisconnected)?;
    match rt::timeout_at(deadline, rx).await {
        Ok(Ok(written)) => written,
        // The transport went away with it queued.
        Ok(Err(_)) => Err(Error::ClientDisconnected),
        Err(_) => Err(Error::Timeout),
    }
}

/// `ready()` (docs/spec.md §4.3): connected, after the `connected` handler.
pub(crate) async fn client_ready(shared: &Shared, timeout: Option<Duration>) -> Result<(), Error> {
    let seq = shared
        .requested_seq
        .load(std::sync::atomic::Ordering::SeqCst);
    let id = shared.id;
    let nested = in_handler(id) || in_callback(id);
    let mut rx = shared.snapshot.subscribe();
    let fut = async {
        loop {
            {
                // Copies, not a held borrow: `shared.state()` reads the same
                // watch, and a second read lock deadlocks behind a waiting writer.
                let (snap_state, applied, ready) = {
                    let s = rx.borrow_and_update();
                    (s.state, s.applied_seq, s.ready)
                };
                // Closed is final: nothing after it is ever applied.
                if snap_state == State::Closed {
                    return Err(Error::ClientClosed);
                }
                let state = if nested || applied < seq {
                    shared.state()
                } else {
                    snap_state
                };
                match state {
                    State::Closed => return Err(Error::ClientClosed),
                    State::Disconnected => return Err(Error::ClientDisconnected),
                    State::Connected if ready && applied >= seq => return Ok(()),
                    _ if nested => return Err(Error::WouldDeadlock),
                    _ => {}
                }
            }
            if rx.changed().await.is_err() {
                return Err(Error::ClientClosed);
            }
        }
    };
    match timeout {
        Some(t) => rt::timeout(t, fut).await.unwrap_or(Err(Error::Timeout)),
        None => fut.await,
    }
}

/// `sub.ready()`: subscribed, after the `subscribed` handler.
pub(crate) async fn sub_ready(
    shared: &Shared,
    sub: &SubInner,
    timeout: Option<Duration>,
) -> Result<(), Error> {
    let seq = shared
        .requested_seq
        .load(std::sync::atomic::Ordering::SeqCst);
    let id = shared.id;
    let nested = in_handler(id) || in_callback(id);
    let mut rx = shared.snapshot.subscribe();
    let mut srx = sub.snapshot.subscribe();
    let start = sub.snapshot.borrow().unsubscribes;
    let fut = async {
        loop {
            {
                let applied = rx.borrow_and_update().applied_seq;
                let client_state = rx.borrow().state;
                // A copy, not a held borrow: `sub.state()` reads the same watch,
                // and a second read lock deadlocks behind a waiting writer.
                let ss = srx.borrow_and_update().clone();
                let state = if nested || applied < seq {
                    sub.state(shared)
                } else {
                    ss.state
                };
                if client_state == State::Closed {
                    return Err(Error::ClientClosed);
                }
                if state == SubState::Unsubscribed || (applied >= seq && ss.unsubscribes != start) {
                    return Err(Error::SubscriptionUnsubscribed);
                }
                if state == SubState::Subscribed && ss.ready && applied >= seq {
                    return Ok(());
                }
                if nested {
                    return Err(Error::WouldDeadlock);
                }
            }
            tokio::select! {
                r = rx.changed() => if r.is_err() { return Err(Error::ClientClosed) },
                r = srx.changed() => if r.is_err() { return Err(Error::ClientClosed) },
            }
        }
    };
    match timeout {
        Some(t) => rt::timeout(t, fut).await.unwrap_or(Err(Error::Timeout)),
        None => fut.await,
    }
}
