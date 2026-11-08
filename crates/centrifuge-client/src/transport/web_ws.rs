//! The browser's `WebSocket` (docs/wasm-plan.md, step 6). Bytes only:
//! protocol, auth and pings stay shared, as natively (`ws.rs`).
//!
//! The socket's handlers only queue events for the session task, through a
//! channel they share with it; they never touch the session itself. The
//! session owns the socket and the handlers' `Closure`s and cleans up in its
//! `Drop`, which runs on a normal end and when the driver aborts the task.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use centrifuge_client_core::codec::{self, Protocol};
use centrifuge_client_core::machine::CloseReason;
use tokio::sync::mpsc;
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::Closure;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use super::browser_error::{BrowserError, Failure, failed};
use super::ws_close::close_reason;
use super::{Ctx, Inbound, WriteReq, drain, encode_frames, fail_calls, mark_sent};

const SUBPROTOCOL: &str = "centrifuge-protobuf";

/// What the socket's handlers queue, in the order the browser fires them.
enum Ev {
    Open,
    Data(Bytes),
    /// Not terminal: the `close` event always follows (WHATWG WebSocket).
    Error,
    Close {
        code: u16,
        reason: String,
    },
}

/// The socket and its handlers; cleans up when dropped.
struct Session {
    ws: WebSocket,
    /// Set first in `Drop`: an event already queued reports nothing.
    closed: Rc<Cell<bool>>,
    _on_open: Closure<dyn FnMut(Event)>,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
}

impl Drop for Session {
    fn drop(&mut self) {
        // In order: the flag, the handlers, `close()`, and only then the
        // `Closure`s (the fields, after this): the browser must never call a
        // freed one.
        self.closed.set(true);
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        if matches!(
            self.ws.ready_state(),
            WebSocket::CONNECTING | WebSocket::OPEN
        ) {
            // Cancels a handshake in progress too.
            let _ = self.ws.close_with_code(1000);
        }
    }
}

impl Session {
    fn open(ctx: &Ctx) -> Result<(Self, mpsc::UnboundedReceiver<Ev>), CloseReason> {
        let has_api = js_sys::Reflect::has(&js_sys::global(), &"WebSocket".into()).unwrap_or(false);
        if !has_api {
            return Err(failed(Failure::Unsupported("WebSocket"), None));
        }
        // The constructor throws on an invalid URL or subprotocol.
        let ws = if ctx.protocol == Protocol::Protobuf {
            WebSocket::new_with_str(ctx.url.as_str(), SUBPROTOCOL)
        } else {
            WebSocket::new(ctx.url.as_str())
        }
        .map_err(|e| failed(Failure::WsOpen, Some(BrowserError::from_js(&e))))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let (tx, rx) = mpsc::unbounded_channel();
        let closed = Rc::new(Cell::new(false));
        // Each handler queues its event unless the session is closing.
        let queue = |closed: &Rc<Cell<bool>>, tx: &mpsc::UnboundedSender<Ev>| {
            let (closed, tx) = (closed.clone(), tx.clone());
            move |ev: Ev| {
                if !closed.get() {
                    let _ = tx.send(ev);
                }
            }
        };
        let q = queue(&closed, &tx);
        let on_open = Closure::<dyn FnMut(Event)>::new(move |_: Event| q(Ev::Open));
        let q = queue(&closed, &tx);
        let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
            let data = e.data();
            let bytes = if let Some(buf) = data.dyn_ref::<js_sys::ArrayBuffer>() {
                Bytes::from(js_sys::Uint8Array::new(buf).to_vec())
            } else if let Some(s) = data.as_string() {
                Bytes::from(s)
            } else {
                // `binaryType` is "arraybuffer": nothing else arrives.
                return;
            };
            q(Ev::Data(bytes));
        });
        let q = queue(&closed, &tx);
        let on_error = Closure::<dyn FnMut(Event)>::new(move |_: Event| q(Ev::Error));
        let q = queue(&closed, &tx);
        let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |e: CloseEvent| {
            q(Ev::Close {
                code: e.code(),
                reason: e.reason(),
            });
        });
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        Ok((
            Self {
                ws,
                closed,
                _on_open: on_open,
                _on_message: on_message,
                _on_error: on_error,
                _on_close: on_close,
            },
            rx,
        ))
    }
}

pub(super) async fn run(ctx: Ctx) {
    let (session, mut events) = match Session::open(&ctx) {
        Ok(s) => s,
        Err(reason) => {
            ctx.closed(reason).await;
            return;
        }
    };
    // Reported at most once; the writer reports a failed write through it.
    let reported = Rc::new(Cell::new(false));
    let mut had_error = false;
    let mut opened = false;
    while let Some(ev) = events.recv().await {
        match ev {
            Ev::Open => {
                if ctx.protocol == Protocol::Protobuf && session.ws.protocol() != SUBPROTOCOL {
                    report(
                        &ctx,
                        &reported,
                        CloseReason::Failed {
                            status: None,
                            message: format!(
                                "the server didn't accept the {SUBPROTOCOL} subprotocol"
                            ),
                            source: None,
                        },
                    )
                    .await;
                    return;
                }
                opened = true;
                let (tx, rx) = mpsc::unbounded_channel();
                crate::rt::spawn(writer(
                    ctx.clone(),
                    session.ws.clone(),
                    rx,
                    reported.clone(),
                ));
                ctx.report(Inbound::Opened {
                    transport: ctx.id,
                    writer: tx,
                })
                .await;
            }
            Ev::Data(data) if opened => {
                if data.len() > ctx.max_message_size {
                    report(&ctx, &reported, CloseReason::TooLarge).await;
                    return;
                }
                ctx.shared
                    .bytes_in
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                let len = data.len();
                match codec::decode_frame(ctx.protocol, data, ctx.max_message_size) {
                    Ok(replies) => {
                        if !ctx.route(replies, len).await {
                            return;
                        }
                    }
                    Err(e) => {
                        // Nothing after it in the frame is dispatched.
                        report(&ctx, &reported, super::decode_close(&e)).await;
                        return;
                    }
                }
            }
            Ev::Data(_) => {}
            Ev::Error => had_error = true,
            Ev::Close { code, reason } => {
                report(&ctx, &reported, close_reason(code, &reason, had_error)).await;
                return;
            }
        }
    }
}

/// The session's one terminal report.
async fn report(ctx: &Ctx, reported: &Cell<bool>, reason: CloseReason) {
    if !reported.replace(true) {
        ctx.closed(reason).await;
    }
}

/// Writes in order. `send()` buffers in the browser (no backpressure), so a
/// write only fails if the socket isn't open.
async fn writer(
    ctx: Ctx,
    ws: WebSocket,
    mut rx: mpsc::UnboundedReceiver<WriteReq>,
    reported: Rc<Cell<bool>>,
) {
    let mut closing = ctx.closing.clone();
    loop {
        let first = tokio::select! {
            biased;
            r = rx.recv() => match r {
                Some(r) => r,
                None => break,
            },
            _ = closing.wait_for(|c| *c) => break,
        };
        let mut reqs = drain(first, &mut rx);
        let cmds: Vec<_> = reqs
            .iter_mut()
            .flat_map(|r| std::mem::take(&mut r.cmds))
            .collect();
        let frames = match encode_frames(ctx.protocol, &cmds) {
            Ok(f) => f,
            Err(e) => {
                fail_calls(
                    &ctx.shared,
                    &mut reqs,
                    &crate::Error::InvalidData(e.to_string()),
                );
                continue;
            }
        };
        for frame in frames {
            ctx.shared
                .bytes_out
                .fetch_add(frame.len() as u64, Ordering::Relaxed);
            let sent = if ctx.protocol == Protocol::Protobuf {
                ws.send_with_u8_array(&frame)
            } else {
                match String::from_utf8(frame) {
                    Ok(s) => ws.send_with_str(&s),
                    Err(e) => ws.send_with_u8_array(e.as_bytes()),
                }
            };
            if let Err(e) = sent {
                fail_calls(&ctx.shared, &mut reqs, &crate::Error::ClientDisconnected);
                report(
                    &ctx,
                    &reported,
                    failed(Failure::WsError, Some(BrowserError::from_js(&e))),
                )
                .await;
                return;
            }
        }
        mark_sent(&mut reqs);
    }
    // Graceful close; the session's `Drop` closes too, whichever comes first.
    if ws.ready_state() == WebSocket::OPEN {
        let _ = ws.close_with_code(1000);
    }
}
