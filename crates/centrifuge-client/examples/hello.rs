//! The smallest useful client: connect, subscribe, publish, receive.
//!
//! ```sh
//! docker compose -f examples/native/compose.yml up   # Centrifugo and this example
//! ```
//!
//! Or against your own Centrifugo: `CENTRIFUGO_URL=ws://... cargo run --example hello`
//! (the channel needs publishing allowed for clients).

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8000/connection/websocket".into());

    let client = Client::builder(url)
        .on_connected(|_, e| println!("connected as {}", e.client_id))
        .on_disconnected(|_, e| println!("disconnected: {}", e.reason))
        .build()?;

    let sub = client
        .new_subscription("chat")
        .on_subscribed(|_, _| println!("subscribed to chat"))
        .on_publication(|_, e| println!("received: {}", String::from_utf8_lossy(&e.data)))
        .build()?;

    sub.subscribe();
    client.connect()?;

    // Publish a message every few seconds; every subscriber, this one
    // included, receives it.
    let mut n = 0;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            () = tokio::time::sleep(Duration::from_secs(3)) => {
                n += 1;
                let msg = format!(r#"{{"text":"hello #{n} from Rust"}}"#);
                if let Err(e) = sub.publish(msg).await {
                    eprintln!("publish failed: {e}");
                }
            }
        }
    }
    client.close().await;
    Ok(())
}
