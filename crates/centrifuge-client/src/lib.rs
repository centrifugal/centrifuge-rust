//! Real-time client SDK for [Centrifugo](https://centrifugal.dev) and the
//! [Centrifuge](https://github.com/centrifugal/centrifuge) library.
//!
//! WebSocket and HTTP-streaming transports with fallback, client-side and
//! server-side subscriptions, automatic reconnect and resubscribe, recovery of
//! missed publications, token refresh, delta compression, proxies and TLS
//! with the OS trust store.
//!
//! ```no_run
//! use centrifuge::Client;
//! # mod my_backend {
//! #     pub async fn connection_token() -> Result<String, std::io::Error> { Ok(String::new()) }
//! #     pub async fn subscription_token(_: &str) -> Result<String, std::io::Error> { Ok(String::new()) }
//! # }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder("wss://example.com/connection/websocket")
//!     // Called before the first connect and before the token expires.
//!     .get_token(|_ctx| async move {
//!         let token = my_backend::connection_token().await?;
//!         Ok(Some(token))
//!     })
//!     // Connecting happens on every reconnect: a good place to show "offline".
//!     .on_connecting(|_client, e| println!("connecting: {} ({})", e.reason, e.code))
//!     .on_connected(|_client, e| println!("connected, client id {}", e.client_id))
//!     .on_disconnected(|_client, e| println!("disconnected: {} ({})", e.reason, e.code))
//!     .build()?;
//!
//! let sub = client
//!     .new_subscription("chat:42")
//!     // A token for this channel, also refreshed before it expires.
//!     .get_token(|ctx| async move { Ok(my_backend::subscription_token(&ctx.channel).await?) })
//!     .recoverable(true)
//!     // Subscribing happens on every resubscribe, after a reconnect too.
//!     .on_subscribing(|_sub, e| println!("subscribing: {} ({})", e.reason, e.code))
//!     .on_subscribed(|sub, _| println!("subscribed to {}", sub.channel()))
//!     .on_unsubscribed(|_sub, e| println!("unsubscribed: {} ({})", e.reason, e.code))
//!     .on_publication(|_sub, e| println!("{}", String::from_utf8_lossy(&e.data)))
//!     .build()?;
//!
//! sub.subscribe(); // never waits: sent right after the connect reply
//! client.connect()?;
//! sub.ready(None).await?;
//! sub.publish(r#"{"text":"hello"}"#).await?;
//! # Ok(()) }
//! ```
//!
//! The client runs on Tokio: `build()` takes the current runtime (or the one
//! given with [`ClientBuilder::runtime`]), and its async methods must be
//! awaited inside a Tokio runtime.
//!
//! See the [README](https://github.com/centrifugal/centrifuge-rust) for a
//! guide and the [design](https://github.com/centrifugal/centrifuge-rust/blob/main/docs/spec.md)
//! for the exact behaviour.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod calls;
mod client;
mod config;
mod dispatch;
mod driver;
mod error;
mod events;
mod maybe_send;
mod shared;
mod stats;
mod stream;
mod subscription;
mod transport;

pub use bytes::Bytes;
pub use centrifuge_client_core::codes;
pub use centrifuge_client_core::filter::{Filter, FilterNode};
pub use centrifuge_client_core::machine::{State, SubState};
pub use centrifuge_client_core::types::{
    ClientInfo, HistoryOptions, HistoryResult, PresenceResult, PresenceStatsResult, Publication,
    RpcResult, StreamPosition,
};
pub use client::{Client, ClientBuilder, ConnectionTokenContext, WeakClient};
pub use config::{
    CustomProxy, EventDelivery, Protocol, ProxyAuth, ProxyConfig, ProxyTarget, TlsConfig,
    TransportEndpoint, TransportKind,
};
pub use error::{
    ArcError, BoxError, ClientError, ClientErrorKind, Error, ServerError, SubscriptionError,
    SubscriptionErrorKind, TokenError,
};
pub use events::{
    ClientEvent, ConnectedEvent, ConnectingEvent, DisconnectedEvent, ErrorEvent, JoinEvent,
    LeaveEvent, MessageEvent, PublicationEvent, ServerJoinEvent, ServerLeaveEvent,
    ServerPublicationEvent, ServerSubscribedEvent, ServerSubscribingEvent, ServerUnsubscribedEvent,
    SubscribedEvent, SubscribingEvent, SubscriptionErrorEvent, SubscriptionEvent,
    UnsubscribedEvent,
};
pub use maybe_send::{MaybeSend, MaybeSync};
pub use stats::Statistics;
pub use stream::EventStream;
pub use subscription::{
    DeltaType, Subscription, SubscriptionBuilder, SubscriptionGetStateContext,
    SubscriptionTokenContext,
};

/// Version of this crate, as published on crates.io.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn released_in_lockstep_with_core() {
        assert_eq!(VERSION, centrifuge_client_core::VERSION);
    }
}
