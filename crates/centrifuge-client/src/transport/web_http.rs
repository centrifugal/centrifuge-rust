//! HTTP-streaming over the browser's `fetch` (docs/wasm-plan.md, step 7),
//! wire-compatible with the native transport (`http_stream.rs`), whose
//! writer and HTTP rules it shares (`http_common.rs`).
//!
//! Cancellation is cleanup on drop: dropping a future that awaits a `fetch`
//! doesn't cancel the request, so every request holds an [`AbortOnDrop`].
//! `abort()` covers the request and a body already streaming; a rejection it
//! causes finds no task left to report it.

use std::sync::atomic::Ordering;

use centrifuge_client_core::codec::{self, StreamDecoder};
use centrifuge_client_core::machine::CloseReason;
use centrifuge_client_core::proto;
use tokio::sync::mpsc;
use url::Url;
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    AbortController, ReadableStreamDefaultReader, Request, RequestCredentials, RequestInit,
    Response,
};

use super::browser_error::{BrowserError, Failure, failed};
use super::http_common::{
    PostError, Poster, content_type, emulation_writer, post_status, stream_response,
};
use super::{Ctx, Inbound};
use crate::config::Credentials;

#[wasm_bindgen]
extern "C" {
    // The global `fetch`: a window's or a worker's, never `window` itself.
    #[wasm_bindgen(js_name = fetch)]
    fn global_fetch(request: &Request) -> js_sys::Promise;
}

/// Aborts its request (and a body still streaming) when dropped: on a
/// normal end, an error, or the task being aborted. Idempotent.
struct AbortOnDrop(AbortController);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn has_api() -> Result<(), CloseReason> {
    for api in ["fetch", "AbortController"] {
        if !js_sys::Reflect::has(&js_sys::global(), &api.into()).unwrap_or(false) {
            return Err(failed(Failure::Unsupported(api), None));
        }
    }
    Ok(())
}

fn js_failure(what: Failure) -> impl Fn(JsValue) -> CloseReason {
    move |e| failed(what, Some(BrowserError::from_js(&e)))
}

/// Sends a POST with `body`; the returned guard aborts it when dropped.
async fn post(
    ctx: &Ctx,
    url: &Url,
    body: &[u8],
    accept: bool,
) -> Result<(Response, AbortOnDrop), CloseReason> {
    let controller = AbortController::new().map_err(js_failure(Failure::Fetch))?;
    let guard = AbortOnDrop(controller);
    let init = RequestInit::new();
    init.set_method("POST");
    let headers = web_sys::Headers::new().map_err(js_failure(Failure::Fetch))?;
    let ct = content_type(ctx.protocol);
    headers
        .set("Content-Type", ct)
        .map_err(js_failure(Failure::Fetch))?;
    if accept {
        headers
            .set("Accept", ct)
            .map_err(js_failure(Failure::Fetch))?;
    }
    init.set_headers(&headers);
    init.set_body(&js_sys::Uint8Array::from(body).into());
    init.set_signal(Some(&guard.0.signal()));
    init.set_credentials(match ctx.credentials {
        Credentials::Include => RequestCredentials::Include,
        Credentials::Omit => RequestCredentials::Omit,
        _ => RequestCredentials::SameOrigin,
    });
    let request =
        Request::new_with_str_and_init(url.as_str(), &init).map_err(js_failure(Failure::Fetch))?;
    let resp = JsFuture::from(global_fetch(&request))
        .await
        .map_err(js_failure(Failure::Fetch))?;
    let resp: Response = resp.dyn_into().map_err(|_| {
        failed(
            Failure::Fetch,
            Some(BrowserError::new(None, "not a Response")),
        )
    })?;
    Ok((resp, guard))
}

pub(super) async fn run(ctx: Ctx, initial: Option<proto::Command>) {
    if let Err(reason) = stream(&ctx, initial).await {
        ctx.closed(reason).await;
    }
}

/// The stream until it ends; `Ok` once the end was reported (or the driver
/// is gone), `Err` for a failure to report.
async fn stream(ctx: &Ctx, initial: Option<proto::Command>) -> Result<(), CloseReason> {
    has_api()?;
    let connect = initial.ok_or_else(|| CloseReason::Failed {
        status: None,
        message: "HTTP-streaming needs the connect command".into(),
        source: None,
    })?;
    let mut body = Vec::new();
    codec::encode_commands(ctx.protocol, std::slice::from_ref(&connect), &mut body).map_err(
        |e| CloseReason::Failed {
            status: None,
            message: e.to_string(),
            source: None,
        },
    )?;
    // Held until the stream ends: dropping it aborts the request.
    let (resp, _abort) = post(ctx, &ctx.url, &body, true).await?;
    let ct = resp.headers().get("Content-Type").ok().flatten();
    stream_response(resp.status(), ct.as_deref())?;
    let reader: ReadableStreamDefaultReader = resp
        .body()
        .ok_or_else(|| failed(Failure::NoBody, None))?
        .get_reader()
        .unchecked_into();

    let (tx, rx) = mpsc::unbounded_channel();
    crate::rt::spawn(emulation_writer(ctx.clone(), rx, FetchPoster));
    ctx.report(Inbound::Opened {
        transport: ctx.id,
        writer: tx,
    })
    .await;

    let mut decoder = StreamDecoder::new(ctx.protocol, ctx.max_message_size);
    let mut replies = Vec::new();
    loop {
        // The next chunk only after the last one's replies are routed: the
        // read-ahead budget holds the stream back, as natively.
        let chunk = JsFuture::from(reader.read())
            .await
            .map_err(js_failure(Failure::Read))?;
        let done = js_sys::Reflect::get(&chunk, &"done".into())
            .ok()
            .and_then(|d| d.as_bool())
            .unwrap_or(true);
        if done {
            let reason = match decoder.finish() {
                Ok(()) => CloseReason::Eof,
                Err(e) => CloseReason::Failed {
                    status: None,
                    message: e.to_string(),
                    source: None,
                },
            };
            ctx.closed(reason).await;
            return Ok(());
        }
        let value =
            js_sys::Reflect::get(&chunk, &"value".into()).map_err(js_failure(Failure::Read))?;
        let data = js_sys::Uint8Array::new(&value).to_vec();
        ctx.shared
            .bytes_in
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        let res = decoder.push(&data, &mut replies);
        // Replies decoded before an error were received before it:
        // dispatched first (§5.7).
        let batch = std::mem::take(&mut replies);
        if !ctx.route(batch, data.len()).await {
            return Ok(());
        }
        if let Err(e) = res {
            ctx.closed(super::decode_close(&e)).await;
            return Ok(());
        }
    }
}

/// One `fetch` per emulation request, each with its own `AbortController`,
/// alive until the response status is in. The browser pools connections.
struct FetchPoster;

impl Poster for FetchPoster {
    async fn post(&mut self, ctx: &Ctx, url: &Url, body: Vec<u8>) -> Result<(), PostError> {
        // The guard aborts the request if the writer drops this future (a
        // closed session); once the status is in, aborting only drops the
        // (unread) reply body.
        let (resp, _abort) = post(ctx, url, &body, false)
            .await
            .map_err(PostError::Close)?;
        post_status(resp.status())
    }
}
