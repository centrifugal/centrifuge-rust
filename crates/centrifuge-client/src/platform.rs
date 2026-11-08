//! Platform preconditions, in one place (docs/wasm-plan.md, decision 7).
//!
//! - [`check_build`], from `build()`: what's known then and can never change
//!   for the client, so it fails `build()` instead of starting a reconnect
//!   loop.
//! - [`check_attempt`], at the start of every connection attempt, before any
//!   I/O: what's only known then, or can become true later; it fails that
//!   attempt, which is retried with backoff.
//!
//! The rules are plain functions built on every target, so host tests cover
//! them; each platform calls only its own.

use centrifuge_client_core::machine::TransportKind;
use url::Url;

use crate::config::{ProxyConfig, TlsConfig};
use crate::error::Error;
use crate::transport::uses_tls;

/// What `build()` knows about the connections.
pub(crate) struct BuildCheck<'a> {
    pub(crate) endpoints: &'a [(TransportKind, Url)],
    pub(crate) emulation: Option<&'a Url>,
    pub(crate) proxy: &'a ProxyConfig,
    pub(crate) tls: &'a TlsConfig,
}

/// # Errors
///
/// [`Error::Configuration`] for a configuration the platform can never serve.
// Infallible on most platforms, not on all.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn check_build(c: &BuildCheck<'_>) -> Result<(), Error> {
    #[cfg(target_os = "android")]
    {
        android::check_build(c, crate::android::initialized())
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = c;
        Ok(())
    }
}

/// `tls`: the attempt uses TLS, to the endpoint or to an HTTPS proxy;
/// `platform_verifier`: certificates are checked by rustls-platform-verifier.
///
/// # Errors
///
/// Why the attempt can't be made now.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn check_attempt(tls: bool, platform_verifier: bool) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        android::check_attempt(tls, platform_verifier, crate::android::initialized())
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = (tls, platform_verifier);
        Ok(())
    }
}

/// Whether a connection known at `build()` uses TLS: a `wss`/`https`
/// endpoint, an `https` emulation endpoint (only HTTP-streaming uses it), or an
/// explicit `https` proxy. Proxies from the environment, the OS, a custom
/// function or `set_proxy` are only known per connection attempt, and are
/// checked there.
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
fn configured_tls(c: &BuildCheck<'_>) -> bool {
    let streaming = c
        .endpoints
        .iter()
        .any(|(k, _)| *k == TransportKind::HttpStream);
    c.endpoints.iter().any(|(_, u)| uses_tls(u))
        || (streaming && c.emulation.is_some_and(uses_tls))
        || matches!(c.proxy, ProxyConfig::Http { url, .. } if uses_tls(url))
}

/// Android's TLS rules (docs/spec.md §5.2).
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
mod android {
    use super::{BuildCheck, configured_tls};
    use crate::error::Error;

    /// The verifier's setup is checked at `build()` for the TLS connections
    /// known then, and per attempt for proxies known only then; its extra
    /// roots are refused at `build()`.
    pub(super) fn check_build(c: &BuildCheck<'_>, initialized: bool) -> Result<(), Error> {
        let verifier = c.tls.uses_platform_verifier();
        if verifier {
            extra_roots(c.tls.extra_roots.len())?;
        }
        // A TLS endpoint or proxy can't work without the verifier's setup, so
        // say so here instead of retrying forever.
        if configured_tls(c) {
            verifier_ready(verifier, initialized).map_err(Error::Configuration)?;
        }
        Ok(())
    }

    /// `build()` refuses TLS endpoints before the setup; this also covers an
    /// HTTPS proxy from the environment, the OS or `set_proxy`.
    pub(super) fn check_attempt(
        tls: bool,
        platform_verifier: bool,
        initialized: bool,
    ) -> Result<(), String> {
        if tls {
            verifier_ready(platform_verifier, initialized)?;
        }
        Ok(())
    }

    /// Android's verifier takes no extra roots: refuse them at `build()`.
    pub(super) fn extra_roots(count: usize) -> Result<(), Error> {
        if count == 0 {
            return Ok(());
        }
        Err(Error::Configuration(
            "TLS: extra roots aren't supported on Android; install the CA in the OS store, \
             or pass your own rustls::ClientConfig with TlsConfig::rustls_client_config \
             (feature `rustls-config`)"
                .into(),
        ))
    }

    pub(super) const NOT_INITIALIZED: &str = "TLS on Android needs setup first: call \
        centrifuge::android::init_with_env (or init_with_refs, init_with_runtime) before \
        build(), also if the app already set up rustls-platform-verifier another way \
        (the call is then a no-op)";

    /// Without `centrifuge::android::init_*`, the verifier panics on its first
    /// certificate check. Fail with a clear error instead.
    pub(super) fn verifier_ready(
        uses_platform_verifier: bool,
        initialized: bool,
    ) -> Result<(), String> {
        if !uses_platform_verifier || initialized {
            return Ok(());
        }
        Err(NOT_INITIALIZED.into())
    }
}

#[cfg(test)]
mod tests {
    use super::{BuildCheck, ProxyConfig, TlsConfig, TransportKind, Url, android, configured_tls};

    fn check<'a>(
        endpoints: &'a [(TransportKind, Url)],
        emulation: Option<&'a Url>,
        proxy: &'a ProxyConfig,
        tls: &'a TlsConfig,
    ) -> BuildCheck<'a> {
        BuildCheck {
            endpoints,
            emulation,
            proxy,
            tls,
        }
    }

    fn ep(kind: TransportKind, url: &str) -> (TransportKind, Url) {
        (kind, Url::parse(url).unwrap())
    }

    fn configured(
        endpoints: &[(TransportKind, Url)],
        emu: Option<&Url>,
        proxy: &ProxyConfig,
    ) -> bool {
        configured_tls(&check(endpoints, emu, proxy, &TlsConfig::new()))
    }

    #[test]
    fn configured_tls_covers_endpoints_emulation_and_proxy() {
        let ws = ep(TransportKind::WebSocket, "ws://h/connection/websocket");
        let wss = ep(TransportKind::WebSocket, "wss://h/connection/websocket");
        let http = ep(TransportKind::HttpStream, "http://h/connection/http-stream");
        let https = ep(
            TransportKind::HttpStream,
            "https://h/connection/http-stream",
        );
        let tls_emu = Url::parse("https://h/emulation").unwrap();
        let plain_emu = Url::parse("http://h/emulation").unwrap();
        let none = ProxyConfig::None;
        let tls_proxy = ProxyConfig::http(Url::parse("https://proxy:3128").unwrap(), None);
        let plain_proxy = ProxyConfig::http(Url::parse("http://proxy:3128").unwrap(), None);

        assert!(!configured(std::slice::from_ref(&ws), None, &none));
        assert!(configured(&[wss], None, &none));
        assert!(configured(&[https], None, &none));
        assert!(!configured(
            std::slice::from_ref(&http),
            Some(&plain_emu),
            &none
        ));
        // A plain stream endpoint with an HTTPS emulation endpoint still uses TLS.
        assert!(configured(
            std::slice::from_ref(&http),
            Some(&tls_emu),
            &none
        ));
        assert!(configured(&[ws.clone(), http], Some(&tls_emu), &none));
        // Without HTTP-streaming, the emulation endpoint is never used.
        assert!(!configured(
            std::slice::from_ref(&ws),
            Some(&tls_emu),
            &none
        ));
        // An explicit HTTPS proxy is TLS even for ws:// endpoints.
        assert!(configured(std::slice::from_ref(&ws), None, &tls_proxy));
        assert!(!configured(std::slice::from_ref(&ws), None, &plain_proxy));
        // Proxies only known per attempt are checked at connect time instead.
        assert!(!configured(&[ws], None, &ProxyConfig::System));
    }

    #[test]
    fn android_refuses_extra_roots() {
        assert!(android::extra_roots(0).is_ok());
        let err = android::extra_roots(1).unwrap_err().to_string();
        assert!(
            err.contains("extra roots aren't supported on Android"),
            "{err}"
        );
        assert!(err.contains("feature `rustls-config`"), "{err}");
    }

    #[test]
    fn android_needs_init_only_for_the_platform_verifier() {
        assert!(android::verifier_ready(true, true).is_ok());
        assert!(android::verifier_ready(false, false).is_ok());
        let err = android::verifier_ready(true, false).unwrap_err();
        assert!(err.contains("centrifuge::android::init_with_env"), "{err}");
    }

    #[test]
    fn android_build_and_attempt_checks() {
        let url = |s: &str| Url::parse(s).unwrap();
        let wss = [(TransportKind::WebSocket, url("wss://h/c"))];
        let ws = [(TransportKind::WebSocket, url("ws://h/c"))];
        let none = ProxyConfig::None;
        let t = TlsConfig::new();
        // TLS before the setup: refused; without TLS, or after it: fine.
        assert!(android::check_build(&check(&wss, None, &none, &t), false).is_err());
        assert!(android::check_build(&check(&ws, None, &none, &t), false).is_ok());
        assert!(android::check_build(&check(&wss, None, &none, &t), true).is_ok());
        // Not checking certificates needs no setup.
        let any = TlsConfig::new().danger_accept_invalid_certs();
        assert!(android::check_build(&check(&wss, None, &none, &any), false).is_ok());
        // Extra roots are refused even after the setup and without TLS.
        let roots = TlsConfig {
            extra_roots: vec![rustls_pki_types_der()],
            ..TlsConfig::new()
        };
        assert!(android::check_build(&check(&ws, None, &none, &roots), true).is_err());
        assert!(android::check_attempt(true, true, false).is_err());
        assert!(android::check_attempt(false, true, false).is_ok());
        assert!(android::check_attempt(true, false, false).is_ok());
        assert!(android::check_attempt(true, true, true).is_ok());
    }

    fn rustls_pki_types_der() -> rustls::pki_types::CertificateDer<'static> {
        rustls::pki_types::CertificateDer::from(vec![0u8])
    }

    #[test]
    fn platform_verifier_flag() {
        assert!(TlsConfig::new().uses_platform_verifier());
        assert!(
            !TlsConfig::new()
                .danger_accept_invalid_certs()
                .uses_platform_verifier()
        );
    }
}
