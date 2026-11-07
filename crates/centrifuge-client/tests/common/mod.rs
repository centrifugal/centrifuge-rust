//! Shared helpers for the fake-server tests.
#![allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic
)]

use std::time::Duration;

use centrifuge::{Client, ClientBuilder, Protocol, ProxyConfig, TransportEndpoint, TransportKind};
pub use centrifuge_fake_server::{Action, FakeConfig, FakeServer, Received, proto};
use tokio::sync::mpsc;

/// Every transport × protocol pair.
pub const MATRIX: [(TransportKind, Protocol); 4] = [
    (TransportKind::WebSocket, Protocol::Protobuf),
    (TransportKind::WebSocket, Protocol::Json),
    (TransportKind::HttpStream, Protocol::Protobuf),
    (TransportKind::HttpStream, Protocol::Json),
];

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

pub async fn server() -> FakeServer {
    init_tracing();
    FakeServer::start(FakeConfig::default()).await
}

pub fn endpoint(srv: &FakeServer, kind: TransportKind) -> TransportEndpoint {
    match kind {
        TransportKind::HttpStream => TransportEndpoint::http_stream(srv.http_stream_url()),
        _ => TransportEndpoint::websocket(srv.ws_url()),
    }
}

/// A builder with test-friendly delays and no proxy lookup.
pub fn builder(srv: &FakeServer, kind: TransportKind, protocol: Protocol) -> ClientBuilder {
    Client::builder_with_transports([endpoint(srv, kind)])
        .protocol(protocol)
        .proxy(ProxyConfig::None)
        .min_reconnect_delay(Duration::from_millis(20))
        .max_reconnect_delay(Duration::from_millis(200))
        .timeout(Duration::from_secs(2))
}

/// Collects named events from handlers, in order.
#[derive(Clone)]
pub struct Recorder {
    tx: mpsc::UnboundedSender<String>,
}

pub struct Events {
    rx: mpsc::UnboundedReceiver<String>,
}

pub fn recorder() -> (Recorder, Events) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Recorder { tx }, Events { rx })
}

impl Recorder {
    pub fn push(&self, e: impl Into<String>) {
        let _ = self.tx.send(e.into());
    }
}

impl Events {
    /// The next event, within 5 s.
    pub async fn next(&mut self) -> String {
        tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
            .await
            .expect("timed out waiting for an event")
            .expect("recorder dropped")
    }

    /// Waits for an event starting with `prefix`, returning it; earlier ones
    /// are skipped.
    pub async fn wait(&mut self, prefix: &str) -> String {
        // Bounded as a whole, not only per event: a client in a reconnect loop
        // keeps sending events and would otherwise hold the test forever.
        let mut skipped = Vec::new();
        let found = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let e = self.next().await;
                if e.starts_with(prefix) {
                    return e;
                }
                skipped.push(e);
            }
        })
        .await;
        found.unwrap_or_else(|_| panic!("no {prefix:?} event within 15 s; skipped: {skipped:?}"))
    }

    /// Everything received so far, without waiting.
    pub fn drain(&mut self) -> Vec<String> {
        let mut v = Vec::new();
        while let Ok(e) = self.rx.try_recv() {
            v.push(e);
        }
        v
    }
}

/// Records every client and the given subscription's events.
pub fn record_client(b: ClientBuilder, r: &Recorder) -> ClientBuilder {
    let (r1, r2, r3, r4, r5) = (r.clone(), r.clone(), r.clone(), r.clone(), r.clone());
    let (r6, r7, r8) = (r.clone(), r.clone(), r.clone());
    b.on_connecting(move |_, e| r1.push(format!("connecting {} {}", e.code, e.reason)))
        .on_connected(move |_, e| r2.push(format!("connected {:?}", e.transport)))
        .on_disconnected(move |_, e| r3.push(format!("disconnected {} {}", e.code, e.reason)))
        .on_error(move |_, e| r4.push(format!("error {}", e.error)))
        .on_message(move |_, e| r5.push(format!("message {}", String::from_utf8_lossy(&e.data))))
        .on_subscribed(move |_, e| r6.push(format!("server_subscribed {}", e.channel)))
        .on_publication(move |_, e| {
            r7.push(format!(
                "server_publication {} {}",
                e.channel,
                String::from_utf8_lossy(&e.data)
            ));
        })
        .on_unsubscribed(move |_, e| r8.push(format!("server_unsubscribed {}", e.channel)))
}

pub fn record_sub(
    b: centrifuge::SubscriptionBuilder,
    r: &Recorder,
) -> centrifuge::SubscriptionBuilder {
    let (r1, r2, r3, r4, r5, r6, r7) = (
        r.clone(),
        r.clone(),
        r.clone(),
        r.clone(),
        r.clone(),
        r.clone(),
        r.clone(),
    );
    b.on_subscribing(move |_, e| r1.push(format!("subscribing {} {}", e.code, e.reason)))
        .on_subscribed(move |_, e| {
            r2.push(format!(
                "subscribed recovered={} pos={:?}",
                e.recovered,
                e.stream_position.map(|p| p.offset)
            ));
        })
        .on_unsubscribed(move |_, e| r3.push(format!("unsubscribed {} {}", e.code, e.reason)))
        .on_error(move |_, e| r4.push(format!("sub_error {}", e.error)))
        .on_publication(move |_, e| {
            r5.push(format!(
                "pub {} {}",
                e.offset,
                String::from_utf8_lossy(&e.data)
            ));
        })
        .on_join(move |_, e| r6.push(format!("join {}", e.client)))
        .on_leave(move |_, e| r7.push(format!("leave {}", e.client)))
}
