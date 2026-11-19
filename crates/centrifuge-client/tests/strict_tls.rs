//! `ClientBuilder::require_tls`: clear-text endpoints and disabled
//! certificate checks fail `build()`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use centrifuge::{Client, ClientBuilder, Error, TlsConfig, TransportEndpoint};

fn refused(b: ClientBuilder) -> String {
    match b.build() {
        Err(Error::Configuration(m)) => m,
        Err(e) => panic!("expected a configuration error, got {e:?}"),
        Ok(_) => panic!("expected a configuration error, got a client"),
    }
}

#[tokio::test]
async fn default_allows_plain_endpoints() {
    assert!(Client::builder("ws://example.test/x").build().is_ok());
}

#[tokio::test]
async fn refuses_plain_websocket() {
    let m = refused(Client::builder("ws://example.test/x").require_tls(true));
    assert!(m.contains("ws://example.test/x"), "{m}");
}

#[tokio::test]
async fn refuses_plain_http_stream() {
    refused(Client::builder("http://example.test/x").require_tls(true));
}

#[tokio::test]
async fn accepts_tls_endpoints() {
    for url in ["wss://example.test/x", "https://example.test/x"] {
        assert!(
            Client::builder(url).require_tls(true).build().is_ok(),
            "{url}"
        );
    }
}

#[tokio::test]
async fn refuses_when_any_fallback_endpoint_is_plain() {
    let b = Client::builder_with_transports([
        TransportEndpoint::websocket("wss://example.test/ws"),
        TransportEndpoint::http_stream("http://example.test/s"),
    ])
    .require_tls(true);
    refused(b);
}

#[tokio::test]
async fn refuses_plain_emulation_endpoint() {
    refused(
        Client::builder("wss://example.test/x")
            .emulation_endpoint("http://example.test/e")
            .require_tls(true),
    );
}

#[tokio::test]
async fn refuses_disabled_certificate_checks() {
    refused(
        Client::builder("wss://example.test/x")
            .tls(TlsConfig::new().danger_accept_invalid_certs())
            .require_tls(true),
    );
}

#[tokio::test]
async fn require_tls_false_allows_plain() {
    let b = Client::builder("ws://example.test/x")
        .require_tls(true)
        .require_tls(false);
    assert!(b.build().is_ok());
}
