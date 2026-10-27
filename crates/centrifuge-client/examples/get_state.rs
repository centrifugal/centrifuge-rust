//! App-owned state with `get_state`: the app keeps a value together with the
//! stream position it reflects, and the subscription continues from that
//! position. When the server can no longer recover the gap (history too
//! short), `get_state` is asked again and the app reloads its state.
//!
//! Here the "backend" is a counter in this process, and a second client
//! publishes increments. A real app loads both value and position from its
//! own backend, which stores them together when it publishes.
//!
//! ```sh
//! docker compose up -d                 # insecure Centrifugo on :8010
//! cargo run --example get_state
//! ```

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use centrifuge::{Client, HistoryOptions, StreamPosition};

/// The value and position the app shows.
#[derive(Default)]
struct View {
    value: i64,
    position: Option<StreamPosition>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8010/connection/websocket".into());
    // `smallhistory` keeps 2 publications, so a longer gap can't be recovered.
    let channel = format!("smallhistory:counter-{}", std::process::id());

    // The backend: the true value, and a client to read the stream position
    // that value corresponds to.
    let total = Arc::new(AtomicI64::new(0));
    let publisher = Client::builder(url.as_str()).build()?;
    publisher.connect()?;
    publisher.ready(Some(Duration::from_secs(5))).await?;

    let view = Arc::new(Mutex::new(View::default()));
    let loads = Arc::new(AtomicUsize::new(0));
    let client = Client::builder(url.as_str()).build()?;
    let sub = {
        let (view_load, view_pub) = (view.clone(), view.clone());
        let (total, publisher, loads) = (total.clone(), publisher.clone(), loads.clone());
        client
            .new_subscription(channel.as_str())
            .recoverable(true)
            // Called before the first subscribe, and again when the gap can't
            // be recovered. An error is retried after the resubscribe delay.
            .get_state(move |ctx| {
                let (view, total, publisher, loads) = (
                    view_load.clone(),
                    total.clone(),
                    publisher.clone(),
                    loads.clone(),
                );
                async move {
                    let top = publisher
                        .history(&ctx.channel, HistoryOptions::new())
                        .await?;
                    let value = total.load(Ordering::SeqCst);
                    let position = StreamPosition::new(top.offset, top.epoch);
                    let n = loads.fetch_add(1, Ordering::SeqCst) + 1;
                    println!(
                        "get_state #{n}: loaded value {value} at offset {}",
                        position.offset
                    );
                    let mut v = view
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    v.value = value;
                    v.position = Some(position.clone());
                    Ok(position)
                }
            })
            .on_subscribed(|_, e| println!("subscribed (recovered: {})", e.recovered))
            .on_publication(move |_, e| {
                let delta: i64 = String::from_utf8_lossy(&e.data).parse().unwrap_or(0);
                let mut v = view_pub
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                v.value += delta;
                if let Some(p) = v.position.as_mut() {
                    p.offset = e.offset;
                }
                println!("publication {}: +{delta} -> {}", e.offset, v.value);
            })
            .build()?
    };

    let publish = |n: i64| {
        let (publisher, total, channel) = (publisher.clone(), total.clone(), channel.clone());
        async move {
            // The backend applies the change, then publishes it.
            total.fetch_add(n, Ordering::SeqCst);
            publisher.publish(&channel, n.to_string()).await
        }
    };

    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;

    for n in [1, 2, 3] {
        publish(n).await?;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Offline while 5 changes happen: more than the 2 kept in history.
    println!("-- offline, 5 changes meanwhile --");
    client.disconnect();
    for _ in 0..5 {
        publish(10).await?;
    }
    client.connect()?;
    // The server rejects the recovery (error 112), get_state reloads.
    let end = tokio::time::Instant::now() + Duration::from_secs(10);
    while loads.load(Ordering::SeqCst) < 2 && tokio::time::Instant::now() < end {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sub.ready(Some(Duration::from_secs(5))).await?;
    publish(100).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let shown = view
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .value;
    println!("shown {shown}, backend {}", total.load(Ordering::SeqCst));
    client.close().await;
    publisher.close().await;
    Ok(())
}
