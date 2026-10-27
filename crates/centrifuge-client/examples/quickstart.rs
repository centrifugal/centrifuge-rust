//! Connect and subscribe, both with tokens from the app backend.
//!
//! ```sh
//! docker compose up -d      # in the repo root: the secure Centrifugo on localhost:8012
//! cargo run --example quickstart
//! ```
//!
//! `backend` stands in for your app server: here it signs short-lived HS256
//! tokens with the test secret of the repo's docker compose.

#![allow(clippy::print_stdout, clippy::print_stderr)] // an example

use std::time::Duration;

use centrifuge::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CENTRIFUGO_URL")
        .unwrap_or_else(|_| "ws://localhost:8012/connection/websocket".into());

    let client = Client::builder(url)
        // Called before the first connect and before the token expires.
        .get_token(|_ctx| async move {
            let token = backend::connection_token("user-42").await?;
            Ok(Some(token))
        })
        .on_connecting(|_, e| println!("connecting: {} ({})", e.reason, e.code))
        .on_connected(|_, e| {
            println!(
                "connected over {:?}, client id {}",
                e.transport, e.client_id
            );
        })
        .on_disconnected(|_, e| println!("disconnected: {} ({})", e.reason, e.code))
        .on_error(|_, e| eprintln!("error: {}", e.error))
        .build()?;

    let sub = client
        .new_subscription("private:quickstart")
        // A token for this channel, also refreshed before it expires.
        .get_token(
            |ctx| async move { Ok(backend::subscription_token("user-42", &ctx.channel).await?) },
        )
        .recoverable(true)
        .on_subscribing(|_, e| println!("subscribing: {} ({})", e.reason, e.code))
        .on_subscribed(|s, e| {
            println!("subscribed to {} (recovered: {})", s.channel(), e.recovered);
        })
        .on_unsubscribed(|_, e| println!("unsubscribed: {} ({})", e.reason, e.code))
        .on_publication(|_, e| {
            println!(
                "publication {}: {}",
                e.offset,
                String::from_utf8_lossy(&e.data)
            );
        })
        .build()?;

    // Neither call waits: the subscribe goes out right after the connect reply.
    sub.subscribe();
    client.connect()?;
    sub.ready(Some(Duration::from_secs(5))).await?;

    sub.publish(r#"{"text":"hello from Rust"}"#).await?;

    tokio::time::sleep(Duration::from_millis(300)).await;
    client.close().await;
    Ok(())
}

/// What your app server does: issue tokens for the authenticated user.
mod backend {
    use std::time::{SystemTime, UNIX_EPOCH};

    use hmac::Mac as _;

    const SECRET: &[u8] = b"secret";

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

    fn sign(claims: &serde_json::Value) -> Result<String, std::io::Error> {
        let head = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
        let body = b64url(claims.to_string().as_bytes());
        let mut mac =
            hmac::Hmac::<sha2::Sha256>::new_from_slice(SECRET).map_err(std::io::Error::other)?;
        mac.update(format!("{head}.{body}").as_bytes());
        Ok(format!(
            "{head}.{body}.{}",
            b64url(&mac.finalize().into_bytes())
        ))
    }

    fn exp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
            + 3600
    }

    #[allow(clippy::unused_async)] // an HTTP call in a real app
    pub(crate) async fn connection_token(user: &str) -> Result<String, std::io::Error> {
        sign(&serde_json::json!({ "sub": user, "exp": exp() }))
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn subscription_token(
        user: &str,
        channel: &str,
    ) -> Result<String, std::io::Error> {
        sign(&serde_json::json!({ "sub": user, "channel": channel, "exp": exp() }))
    }
}
