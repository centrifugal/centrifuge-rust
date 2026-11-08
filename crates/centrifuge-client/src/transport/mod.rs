//! Transports (docs/spec.md §5, §9.6): WebSocket and HTTP-streaming with
//! bidirectional emulation. Each transport has a reader (in its open task) and
//! a writer task; the driver only sees [`Inbound`] messages.

mod browser_error;
#[cfg(feature = "http-stream")]
mod http_common;
#[cfg(all(feature = "http-stream", not(target_arch = "wasm32")))]
mod http_stream;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod net;
// The browser connects by itself: only the settings the shared code reads.
#[cfg(target_arch = "wasm32")]
#[path = "net_wasm.rs"]
pub(crate) mod net;
#[cfg(all(feature = "http-stream", target_arch = "wasm32"))]
mod web_http;
#[cfg(target_arch = "wasm32")]
mod web_ws;
#[cfg(not(target_arch = "wasm32"))]
mod ws;
mod ws_close;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use centrifuge_client_core::codec::{self, DecodeError, Protocol};
use centrifuge_client_core::machine::{CloseReason, TransportId, TransportKind};
use centrifuge_client_core::proto;
// The real monotonic clock, as the driver's watchdog reads.
use centrifuge_client_core::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use url::Url;

use crate::shared::{CALL_ID_BASE, SessionHandle, Shared, lock};

/// Frames are capped at this size; larger batches are split (docs/spec.md §9.10).
pub(crate) const MAX_FRAME: usize = 15 * 1024;
/// The reader may run ahead of the driver by this many items...
pub(crate) const READ_AHEAD_ITEMS: usize = 256;
/// ...or this many bytes of undelivered data (docs/spec.md §9.2).
pub(crate) const READ_AHEAD_BYTES: u32 = 4 << 20;

/// Commands to write, with what the writer must report about them.
#[derive(Debug)]
pub(crate) struct WriteReq {
    pub(crate) cmds: Vec<proto::Command>,
    /// Fast-path call ids among `cmds` (failed by the writer itself).
    pub(crate) call_ids: Vec<u32>,
    /// Emulation session and node.
    #[cfg_attr(not(feature = "http-stream"), allow(dead_code))]
    pub(crate) emu: Option<(String, String)>,
    /// A `send()` waiting to know it was written.
    pub(crate) sent: Option<tokio::sync::oneshot::Sender<Result<(), crate::Error>>>,
}

impl WriteReq {
    fn emu_of(s: &SessionHandle) -> Option<(String, String)> {
        s.kind
            .uses_emulation()
            .then(|| (s.session.clone(), s.node.clone()))
    }

    pub(crate) fn call(cmd: proto::Command, id: u32, s: &SessionHandle) -> Self {
        Self {
            cmds: vec![cmd],
            call_ids: vec![id],
            emu: Self::emu_of(s),
            sent: None,
        }
    }

    /// A `send()`: `sent` reports whether the writer wrote it.
    pub(crate) fn send(
        cmd: proto::Command,
        s: &SessionHandle,
        sent: tokio::sync::oneshot::Sender<Result<(), crate::Error>>,
    ) -> Self {
        Self {
            cmds: vec![cmd],
            call_ids: vec![],
            emu: Self::emu_of(s),
            sent: Some(sent),
        }
    }

    pub(crate) fn core(cmds: Vec<proto::Command>, emu: Option<(String, String)>) -> Self {
        Self {
            cmds,
            call_ids: vec![],
            emu,
            sent: None,
        }
    }
}

pub(crate) type WriterTx = mpsc::UnboundedSender<WriteReq>;

/// What transports report to the driver.
#[derive(Debug)]
pub(crate) enum Inbound {
    Opened {
        transport: TransportId,
        writer: WriterTx,
    },
    Item {
        transport: TransportId,
        reply: Box<proto::Reply>,
        _permit: Option<OwnedSemaphorePermit>,
    },
    Closed {
        transport: TransportId,
        reason: CloseReason,
    },
    #[cfg_attr(not(feature = "http-stream"), allow(dead_code))]
    WriteFailed {
        transport: TransportId,
        command_ids: Vec<u32>,
        had_pong: bool,
    },
}

/// When the current transport last delivered a decoded message, as nanoseconds
/// since `base` (docs/spec.md §6.4).
#[derive(Debug)]
pub(crate) struct LastReceived {
    base: Instant,
    nanos: AtomicU64,
}

impl LastReceived {
    pub(crate) fn new() -> Self {
        Self {
            base: Instant::now(),
            nanos: AtomicU64::new(0),
        }
    }

    pub(crate) fn touch(&self) {
        let n = u64::try_from(self.base.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        self.nanos.store(n, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> Option<Instant> {
        match self.nanos.load(Ordering::Relaxed) {
            0 => None,
            n => Some(self.base + std::time::Duration::from_nanos(n)),
        }
    }

    pub(crate) fn reset(&self) {
        self.nanos.store(0, Ordering::Relaxed);
    }
}

/// Whether `url` is connected with TLS.
pub(crate) fn uses_tls(url: &Url) -> bool {
    matches!(url.scheme(), "wss" | "https")
}

/// Everything a transport needs; cheap to clone per attempt.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub(crate) id: TransportId,
    pub(crate) kind: TransportKind,
    pub(crate) url: Url,
    pub(crate) protocol: Protocol,
    pub(crate) max_message_size: usize,
    pub(crate) http_headers: http::HeaderMap,
    #[cfg_attr(not(feature = "http-stream"), allow(dead_code))]
    pub(crate) emulation_endpoint: Option<Url>,
    #[cfg_attr(not(feature = "http-stream"), allow(dead_code))]
    pub(crate) timeout: std::time::Duration,
    pub(crate) net: Arc<net::Net>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) inbound: mpsc::Sender<Inbound>,
    pub(crate) budget: Arc<Semaphore>,
    pub(crate) last_received: Arc<LastReceived>,
    /// Set by the driver to close this transport's writer gracefully.
    pub(crate) closing: watch::Receiver<bool>,
}

impl Ctx {
    async fn report(&self, msg: Inbound) {
        let _ = self.inbound.send(msg).await;
    }

    async fn closed(&self, reason: CloseReason) {
        self.report(Inbound::Closed {
            transport: self.id,
            reason,
        })
        .await;
    }

    /// Hands decoded replies on: call replies to the call table, the rest to
    /// the driver within the read-ahead budget. Returns `false` once the driver
    /// is gone.
    async fn route(&self, replies: Vec<proto::Reply>, frame_len: usize) -> bool {
        if replies.is_empty() {
            return true;
        }
        self.last_received.touch();
        let share = u32::try_from(frame_len / replies.len())
            .unwrap_or(u32::MAX)
            .clamp(1, READ_AHEAD_BYTES);
        for r in replies {
            if r.id >= CALL_ID_BASE {
                lock(&self.shared.calls).resolve(r);
                continue;
            }
            let permit = self.budget.clone().acquire_many_owned(share).await.ok();
            if self
                .inbound
                .send(Inbound::Item {
                    transport: self.id,
                    reply: Box::new(r),
                    _permit: permit,
                })
                .await
                .is_err()
            {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Opens the transport and runs its reader until it ends. Aborting the task
/// aborts the open or the read (patterns 34, 51).
pub(crate) async fn run(ctx: Ctx, initial: Option<proto::Command>) {
    match ctx.kind {
        #[cfg(not(target_arch = "wasm32"))]
        TransportKind::WebSocket => ws::run(ctx).await,
        #[cfg(target_arch = "wasm32")]
        TransportKind::WebSocket => web_ws::run(ctx).await,
        #[cfg(all(feature = "http-stream", not(target_arch = "wasm32")))]
        TransportKind::HttpStream => http_stream::run(ctx, initial).await,
        #[cfg(all(feature = "http-stream", target_arch = "wasm32"))]
        TransportKind::HttpStream => web_http::run(ctx, initial).await,
        #[allow(unreachable_patterns)]
        _ => {
            let _ = initial;
            ctx.closed(CloseReason::Failed {
                status: None,
                message: "transport not compiled in".into(),
                source: None,
            })
            .await;
        }
    }
}

/// Encodes write requests into frames of at most [`MAX_FRAME`] bytes. A single
/// larger command goes alone.
pub(crate) fn encode_frames(
    protocol: Protocol,
    cmds: &[proto::Command],
) -> Result<Vec<Vec<u8>>, codec::EncodeError> {
    let mut frames = Vec::new();
    let mut frame = Vec::new();
    let mut one = Vec::new();
    for cmd in cmds {
        one.clear();
        codec::encode_commands(protocol, std::slice::from_ref(cmd), &mut one)?;
        let sep = usize::from(!frame.is_empty() && is_json(protocol));
        if !frame.is_empty() && frame.len() + sep + one.len() > MAX_FRAME {
            frames.push(std::mem::take(&mut frame));
        }
        if !frame.is_empty() && is_json(protocol) {
            frame.push(b'\n');
        }
        frame.extend_from_slice(&one);
    }
    if !frame.is_empty() {
        frames.push(frame);
    }
    Ok(frames)
}

fn is_json(protocol: Protocol) -> bool {
    #[cfg(feature = "json")]
    {
        protocol == Protocol::Json
    }
    #[cfg(not(feature = "json"))]
    {
        let _ = protocol;
        false
    }
}

/// Collects every queued request (the writer drains its queue per write).
fn drain(first: WriteReq, rx: &mut mpsc::UnboundedReceiver<WriteReq>) -> Vec<WriteReq> {
    let mut reqs = vec![first];
    while let Ok(r) = rx.try_recv() {
        reqs.push(r);
    }
    reqs
}

fn decode_close(e: &DecodeError) -> CloseReason {
    match e {
        DecodeError::TooLarge { .. } => CloseReason::TooLarge,
        other => CloseReason::Undecodable(other.clone()),
    }
}

/// Fails the fast-path calls of failed requests, and their `send()`s, with
/// `err` (e.g. `Transport { status: 413 }` for a rejected emulation request).
fn fail_calls(shared: &Shared, reqs: &mut [WriteReq], err: &crate::Error) {
    let mut calls = lock(&shared.calls);
    for r in reqs {
        for id in &r.call_ids {
            calls.fail(*id, err.clone());
        }
        if let Some(tx) = r.sent.take() {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

/// The requests were written: their `send()`s resolve.
fn mark_sent(reqs: &mut [WriteReq]) {
    for r in reqs {
        if let Some(tx) = r.sent.take() {
            let _ = tx.send(Ok(()));
        }
    }
}
