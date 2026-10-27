//! Delta compression: the server sends a publication as a Fossil delta against
//! the previous one, and the SDK applies it, so handlers always get the full
//! payload. Worth it for large payloads that change a little each time.
//!
//! Two subscribers on one channel, one with `.delta(DeltaType::Fossil)` and
//! one without, receive the same payloads; the bytes they received differ.
//! The channel's namespace must allow delta (`allowed_delta_types`), and the
//! publisher asks for it (`delta_publish` here, or `"delta": true` in the
//! server API's publish).
//!
//! ```sh
//! docker compose up -d                 # insecure Centrifugo on :8010
//! cargo run --example delta
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::{Client, DeltaType};
use futures_util::StreamExt as _;

/// A ~2 KB document where one field changes per update.
fn document(version: u32) -> String {
    let items: Vec<String> = (0..60)
        .map(|i| format!(r#"{{"id":{i},"name":"item {i}","price":{}}}"#, 100 + i))
        .collect();
    format!(r#"{{"version":{version},"items":[{}]}}"#, items.join(","))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    let channel = format!("delta:document-{}", std::process::id());

    let mut readers = Vec::new();
    for delta in [true, false] {
        let client = Client::builder(url.as_str()).build()?;
        let mut builder = client.new_subscription(channel.as_str());
        if delta {
            builder = builder.delta(DeltaType::Fossil);
        }
        let sub = builder.build()?;
        let publications = sub.publications();
        sub.subscribe();
        client.connect()?;
        sub.ready(Some(Duration::from_secs(5))).await?;
        readers.push((delta, client, publications));
    }

    let publisher = Client::builder(url.as_str()).build()?;
    publisher.connect()?;
    publisher.ready(Some(Duration::from_secs(5))).await?;

    let before: Vec<u64> = readers
        .iter()
        .map(|(_, c, _)| c.statistics().bytes_in)
        .collect();
    let updates = 20;
    for version in 1..=updates {
        publisher.publish(&channel, document(version)).await?;
    }
    for (delta, _, publications) in &mut readers {
        for version in 1..=updates {
            let p = tokio::time::timeout(Duration::from_secs(5), publications.next())
                .await?
                .ok_or("publication stream ended")?;
            // Both get the full document, whatever went over the wire.
            assert_eq!(p.data, document(version).as_bytes(), "delta={delta}");
        }
    }

    println!(
        "{updates} updates of a {} byte document:",
        document(1).len()
    );
    for ((delta, client, _), before) in readers.iter().zip(before) {
        let received = client.statistics().bytes_in - before;
        let label = if *delta {
            "with delta"
        } else {
            "without delta"
        };
        println!("  {label:<14} {received:>7} bytes received");
    }
    for (_, client, _) in readers {
        client.close().await;
    }
    publisher.close().await;
    Ok(())
}
