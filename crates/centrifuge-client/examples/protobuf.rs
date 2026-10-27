//! Binary payloads over the Protobuf protocol (the default). Payloads are
//! opaque bytes: Protobuf messages, `MessagePack`, or a custom layout like the
//! 20-byte reading here. They arrive exactly as published.
//!
//! The JSON protocol carries only JSON payloads: a JSON client refuses to send
//! these bytes (`Error::InvalidData`) instead of a frame the server would
//! answer with a disconnect. JSON and Protobuf clients can share a channel
//! only while its payloads are JSON.
//!
//! ```sh
//! docker compose up -d                 # insecure Centrifugo on :8010
//! cargo run --example protobuf
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::{Bytes, Client, Error, Protocol};
use futures_util::StreamExt as _;

/// A sensor reading in a fixed big-endian layout: id, unix millis, value.
#[derive(Debug, PartialEq)]
struct Reading {
    sensor: u32,
    at_ms: i64,
    value: f64,
}

impl Reading {
    fn encode(&self) -> Bytes {
        let mut b = Vec::with_capacity(20);
        b.extend_from_slice(&self.sensor.to_be_bytes());
        b.extend_from_slice(&self.at_ms.to_be_bytes());
        b.extend_from_slice(&self.value.to_be_bytes());
        Bytes::from(b)
    }

    fn decode(b: &[u8]) -> Option<Self> {
        Some(Self {
            sensor: u32::from_be_bytes(b.get(0..4)?.try_into().ok()?),
            at_ms: i64::from_be_bytes(b.get(4..12)?.try_into().ok()?),
            value: f64::from_be_bytes(b.get(12..20)?.try_into().ok()?),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let channel = format!("sensors-{}", std::process::id());

    // `Protocol::Protobuf` is the default; set here to be explicit.
    let client = Client::builder(url.as_str())
        .protocol(Protocol::Protobuf)
        .build()?;
    let sub = client.new_subscription(channel.as_str()).build()?;
    let mut readings = sub.publications();
    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;

    let sent = Reading {
        sensor: 7,
        at_ms: 1_767_225_600_000,
        value: 21.5,
    };
    client.publish(&channel, sent.encode()).await?;
    let p = tokio::time::timeout(Duration::from_secs(5), readings.next())
        .await?
        .ok_or("publication stream ended")?;
    let got = Reading::decode(&p.data).ok_or("short payload")?;
    println!("received {} bytes: {got:?}", p.data.len());
    assert_eq!(got, sent);

    // The same bytes from a JSON client: refused before anything is sent.
    let json = Client::builder(url.as_str())
        .protocol(Protocol::Json)
        .build()?;
    json.connect()?;
    json.ready(Some(Duration::from_secs(5))).await?;
    match json.publish(&channel, sent.encode()).await {
        Err(Error::InvalidData(e)) => println!("JSON client refuses binary data: {e}"),
        other => println!("unexpected: {other:?}"),
    }

    json.close().await;
    client.close().await;
    Ok(())
}
