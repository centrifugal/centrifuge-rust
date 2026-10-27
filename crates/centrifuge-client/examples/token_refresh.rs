//! Connection and subscription tokens fetched from the app backend, refreshed
//! before they expire. Run against the secure instance of the repo's docker
//! compose (`localhost:8012`, HMAC secret "secret").
//!
//! The "backend" here signs short-lived HS256 tokens itself; in an app it's an
//! HTTP call to your server.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use centrifuge::{Client, TokenError};
use hmac::Mac as _;

fn b64url(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..=c.len() {
            out.push(char::from(T[((n >> (18 - 6 * i)) & 63) as usize]));
        }
    }
    out
}

/// What your backend would do: sign a token. Async, as an HTTP call to it is.
#[allow(clippy::unused_async)]
async fn backend_token(claims: serde_json::Value) -> Result<String, TokenError> {
    let head = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
    let body = b64url(claims.to_string().as_bytes());
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret")
        .map_err(|e| TokenError::Other(e.to_string().into()))?;
    mac.update(format!("{head}.{body}").as_bytes());
    Ok(format!(
        "{head}.{body}.{}",
        b64url(&mac.finalize().into_bytes())
    ))
}

fn exp(ttl: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        + ttl
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8012/connection/websocket".into());
    let client = Client::builder(url)
        // Called before the first connect, and before the token expires.
        .get_token(|_ctx| async move {
            println!("fetching a connection token");
            let token =
                backend_token(serde_json::json!({ "sub": "user-42", "exp": exp(10) })).await?;
            Ok(Some(token))
        })
        .on_connecting(|_, e| println!("connecting: {} ({})", e.reason, e.code))
        .on_connected(|_, _| println!("connected"))
        .on_disconnected(|_, e| println!("disconnected: {} ({})", e.reason, e.code))
        .on_error(|_, e| eprintln!("error: {}", e.error))
        .build()?;

    let sub = client
        .new_subscription("private:token-demo")
        .get_token(|ctx| async move {
            println!("fetching a subscription token for {}", ctx.channel);
            backend_token(
                serde_json::json!({ "sub": "user-42", "channel": ctx.channel, "exp": exp(10) }),
            )
            .await
        })
        .on_subscribing(|_, e| println!("subscribing: {} ({})", e.reason, e.code))
        .on_subscribed(|s, _| println!("subscribed to {}", s.channel()))
        .on_unsubscribed(|_, e| println!("unsubscribed: {} ({})", e.reason, e.code))
        .build()?;
    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;

    println!("staying connected for 25 s: watch the tokens refresh");
    tokio::time::sleep(Duration::from_secs(25)).await;
    println!(
        "still {:?}, {} connect(s)",
        client.state(),
        client.statistics().connects
    );
    client.close().await;
    Ok(())
}
