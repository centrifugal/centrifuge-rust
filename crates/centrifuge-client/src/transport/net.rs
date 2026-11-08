//! The connector both transports use: DNS, TCP, proxy tunnel, TLS
//! (docs/spec.md §5.1–§5.2). One stack, so proxy and TLS behaviour can't differ
//! between WebSocket and HTTP-streaming.

// Wire code: short names mirror the protocol fields (host/phost, u/p).
#![allow(clippy::similar_names, clippy::many_single_char_names)]
use std::fmt::Write as _;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use base64_lite::encode as b64;
use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;
use url::Url;

use super::uses_tls;
use crate::config::{ProxyAuth, ProxyConfig, ProxyTarget, TlsConfig};
use crate::shared::lock;

/// A connected byte stream.
pub(crate) trait Io: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Io for T {}

pub(crate) type BoxIo = Pin<Box<dyn Io>>;

/// A connect failure, with the HTTP status of a proxy refusal when there was one.
#[derive(Debug)]
pub(crate) struct NetError {
    pub(crate) status: Option<u16>,
    pub(crate) message: String,
}

impl NetError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            status: None,
            message: message.into(),
        }
    }
}

impl From<io::Error> for NetError {
    fn from(e: io::Error) -> Self {
        Self::new(e.to_string())
    }
}

type Lookup = Shared<BoxFuture<'static, Result<Vec<SocketAddr>, Arc<io::Error>>>>;

/// Connection settings shared by every attempt of one client.
pub(crate) struct Net {
    pub(crate) proxy: Mutex<ProxyConfig>,
    tls: Arc<rustls::ClientConfig>,
    /// `tls` checks certificates with rustls-platform-verifier, which needs
    /// `centrifuge::android::init_*` first (`platform::check_attempt`).
    platform_verifier: bool,
    accept_invalid: bool,
    /// At most one DNS lookup in flight per client: a dropped future doesn't
    /// stop a blocking `getaddrinfo` (docs/spec.md §6.2).
    lookup: Mutex<Option<(String, Lookup)>>,
    /// The whole handshake's timeout; with several addresses, each TCP
    /// attempt gets a share of it.
    handshake_timeout: std::time::Duration,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net").finish_non_exhaustive()
    }
}

mod base64_lite {
    /// Standard base64, for `Proxy-Authorization: Basic`.
    pub(crate) fn encode(input: &[u8]) -> String {
        const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
        for chunk in input.chunks(3) {
            let b = [
                chunk[0],
                *chunk.get(1).unwrap_or(&0),
                *chunk.get(2).unwrap_or(&0),
            ];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            out.push(T[(n >> 18) as usize & 63] as char);
            out.push(T[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                T[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                T[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    #[cfg(feature = "aws-lc-rs")]
    {
        Arc::new(rustls::crypto::aws_lc_rs::default_provider())
    }
    #[cfg(all(feature = "ring", not(feature = "aws-lc-rs")))]
    {
        Arc::new(rustls::crypto::ring::default_provider())
    }
}

#[cfg(not(any(feature = "aws-lc-rs", feature = "ring")))]
compile_error!(
    "centrifuge-client needs a rustls crypto provider: enable the `aws-lc-rs` (default) or `ring` feature"
);

/// Accepts any certificate (`danger_accept_invalid_certs`, development only).
#[derive(Debug)]
struct AcceptAll(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAll {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// The OS verifier (docs/spec.md §5.2). Android's trusts the OS store only:
/// rustls-platform-verifier has no extra roots there, so they are refused
/// rather than silently dropped.
fn platform_verifier(
    tls: &TlsConfig,
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls_platform_verifier::Verifier, crate::Error> {
    // No extra roots there: `platform::check_build` refuses them first.
    #[cfg(target_os = "android")]
    {
        let _ = tls;
        rustls_platform_verifier::Verifier::new(provider).map_err(|e| tls_error(&e))
    }
    #[cfg(not(target_os = "android"))]
    {
        rustls_platform_verifier::Verifier::new_with_extra_roots(tls.extra_roots.clone(), provider)
            .map_err(|e| tls_error(&e))
    }
}

/// Whether connecting to `url` (an endpoint or an HTTP proxy) uses TLS.
fn tls_error(e: &rustls::Error) -> crate::Error {
    crate::Error::Configuration(format!("TLS: {e}"))
}

impl Net {
    pub(crate) fn new(
        proxy: ProxyConfig,
        tls: &TlsConfig,
        handshake_timeout: std::time::Duration,
    ) -> Result<Self, crate::Error> {
        let cfg = Self::tls_config(tls)?;
        Ok(Self {
            proxy: Mutex::new(proxy),
            tls: cfg,
            platform_verifier: tls.uses_platform_verifier(),
            accept_invalid: tls.accept_invalid,
            lookup: Mutex::new(None),
            handshake_timeout,
        })
    }

    fn tls_config(tls: &TlsConfig) -> Result<Arc<rustls::ClientConfig>, crate::Error> {
        #[cfg(feature = "rustls-config")]
        if let Some(c) = &tls.custom {
            return Ok(c.clone());
        }
        let provider = provider();
        let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| tls_error(&e))?;
        let cfg = if tls.accept_invalid {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAll(provider)))
                .with_no_client_auth()
        } else {
            let verifier = platform_verifier(tls, provider)?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth()
        };
        Ok(Arc::new(cfg))
    }

    /// The proxy decision for `url` (re-read on every attempt: the OS settings
    /// change when a laptop moves between networks).
    fn route_for(&self, url: &Url) -> Route {
        let cfg = lock(&self.proxy).clone();
        let target = |t: ProxyTarget| match t {
            ProxyTarget::Direct => Route::Direct,
            ProxyTarget::Http { url, auth } => Route::Http {
                url,
                auth: auth.map(|a| basic(&a)),
            },
            ProxyTarget::Socks5 {
                url,
                auth,
                remote_dns,
            } => Route::Socks5 {
                url,
                auth,
                remote_dns,
            },
        };
        let matcher = match cfg {
            ProxyConfig::None => return Route::Direct,
            ProxyConfig::Http { url, auth } => {
                return Route::Http {
                    url,
                    auth: auth.map(|a| basic(&a)),
                };
            }
            ProxyConfig::Socks5 {
                url,
                auth,
                remote_dns,
            } => {
                return Route::Socks5 {
                    url,
                    auth,
                    remote_dns,
                };
            }
            ProxyConfig::Custom(f) => return target((f.0)(url)),
            ProxyConfig::FromEnv => hyper_util::client::proxy::matcher::Matcher::from_env(),
            ProxyConfig::System => hyper_util::client::proxy::matcher::Matcher::from_system(),
        };
        // The matcher keys on http/https; a ws(s) URL maps to its HTTP twin.
        let mut probe = url.clone();
        let scheme = if matches!(url.scheme(), "wss" | "https") {
            "https"
        } else {
            "http"
        };
        let _ = probe.set_scheme(scheme);
        let Ok(uri) = probe.as_str().parse::<http::Uri>() else {
            return Route::Direct;
        };
        let Some(intercept) = matcher.intercept(&uri) else {
            return Route::Direct;
        };
        let Ok(purl) = Url::parse(&intercept.uri().to_string()) else {
            return Route::Direct;
        };
        let raw = intercept.raw_auth().map(|(u, p)| ProxyAuth::new(u, p));
        match purl.scheme() {
            "socks5" | "socks5h" => Route::Socks5 {
                remote_dns: purl.scheme() == "socks5h",
                url: purl,
                auth: raw,
            },
            _ => {
                let header = intercept
                    .basic_auth()
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
                    .or_else(|| raw.as_ref().map(basic));
                Route::Http {
                    url: purl,
                    auth: header,
                }
            }
        }
    }

    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, NetError> {
        if let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
        {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let key = format!("{host}:{port}");
        let fut = {
            let mut slot = lock(&self.lookup);
            match slot.as_ref() {
                Some((k, f)) if *k == key && f.peek().is_none() => f.clone(),
                _ => {
                    let k = key.clone();
                    let f: Lookup = async move {
                        tokio::net::lookup_host(k)
                            .await
                            .map(Iterator::collect)
                            .map_err(Arc::new)
                    }
                    .boxed()
                    .shared();
                    *slot = Some((key, f.clone()));
                    f
                }
            }
        };
        fut.await
            .map_err(|e| NetError::new(format!("DNS lookup of {host} failed: {e}")))
    }

    async fn tcp(&self, host: &str, port: u16) -> Result<TcpStream, NetError> {
        let mut last = NetError::new(format!("no address for {host}"));
        let addrs = self.resolve(host, port).await?;
        // A dead first address (often IPv6) mustn't use up the whole handshake
        // timeout: each attempt gets a share of it, at least 500 ms.
        let n = u32::try_from(addrs.len()).unwrap_or(u32::MAX).max(1);
        let per = if n > 1 {
            (self.handshake_timeout / n).max(std::time::Duration::from_millis(500))
        } else {
            self.handshake_timeout
        };
        for addr in addrs {
            match tokio::time::timeout(per, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => {
                    let _ = s.set_nodelay(true);
                    return Ok(s);
                }
                Ok(Err(e)) => last = NetError::new(format!("connect to {addr}: {e}")),
                Err(_) => last = NetError::new(format!("connect to {addr}: timed out")),
            }
        }
        Err(last)
    }

    async fn tls_wrap(&self, io: BoxIo, host: &str, alpn: &[&[u8]]) -> Result<BoxIo, NetError> {
        if self.accept_invalid {
            tracing::warn!(target: "centrifuge", "TLS certificate verification is disabled (danger_accept_invalid_certs)");
        }
        let mut cfg = (*self.tls).clone();
        cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        let name = rustls::pki_types::ServerName::try_from(host.to_owned())
            .map_err(|e| NetError::new(format!("invalid TLS server name {host}: {e}")))?;
        let tls = tokio_rustls::TlsConnector::from(Arc::new(cfg))
            .connect(name, io)
            .await
            .map_err(|e| NetError::new(format!("TLS handshake with {host}: {e}")))?;
        Ok(Box::pin(tls))
    }

    /// Opens a byte stream to `url`'s host: through the proxy when one applies,
    /// with TLS for `wss`/`https`.
    pub(crate) async fn connect(&self, url: &Url, alpn: &[&[u8]]) -> Result<BoxIo, NetError> {
        let host = host_of(url)?;
        let secure = uses_tls(url);
        let port = url
            .port_or_known_default()
            .unwrap_or(if secure { 443 } else { 80 });
        let route = self.route_for(url);
        // Before any DNS or TCP, so a retry costs no network.
        let tls = secure || matches!(&route, Route::Http { url: p, .. } if uses_tls(p));
        crate::platform::check_attempt(tls, self.platform_verifier).map_err(NetError::new)?;
        let io: BoxIo = match route {
            Route::Direct => Box::pin(self.tcp(&host, port).await?),
            Route::Http { url: purl, auth } => {
                let phost = host_of(&purl).map_err(|_| NetError::new("proxy URL has no host"))?;
                let pport = purl.port_or_known_default().unwrap_or(80);
                let mut io: BoxIo = Box::pin(self.tcp(&phost, pport).await?);
                if uses_tls(&purl) {
                    io = self.tls_wrap(io, &phost, &[]).await?;
                }
                http_connect(io, &host, port, auth.as_deref()).await?
            }
            Route::Socks5 {
                url: purl,
                auth,
                remote_dns,
            } => {
                let phost = host_of(&purl).map_err(|_| NetError::new("proxy URL has no host"))?;
                let pport = purl.port_or_known_default().unwrap_or(1080);
                if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                    // An address literal: sent as an address, never as a name.
                    let io: BoxIo = Box::pin(self.tcp(&phost, pport).await?);
                    socks5_connect(
                        io,
                        Target::Ip(SocketAddr::new(ip, port)),
                        port,
                        auth.as_ref(),
                    )
                    .await?
                } else if remote_dns {
                    let io: BoxIo = Box::pin(self.tcp(&phost, pport).await?);
                    socks5_connect(io, Target::Domain(host.clone()), port, auth.as_ref()).await?
                } else {
                    // Every resolved address in turn, one SOCKS session each, as
                    // a direct connection tries them (e.g. `localhost` → ::1,
                    // then 127.0.0.1).
                    let mut last = NetError::new(format!("no address for {host}"));
                    let mut tunnel = None;
                    for addr in self.resolve(&host, port).await? {
                        let io: BoxIo = Box::pin(self.tcp(&phost, pport).await?);
                        match socks5_connect(io, Target::Ip(addr), port, auth.as_ref()).await {
                            Ok(io) => {
                                tunnel = Some(io);
                                break;
                            }
                            Err(e) => last = e,
                        }
                    }
                    tunnel.ok_or(last)?
                }
            }
        };
        if secure {
            self.tls_wrap(io, &host, alpn).await
        } else {
            Ok(io)
        }
    }
}

/// Where a connection goes.
enum Route {
    Direct,
    /// `auth`: a ready `Proxy-Authorization` value.
    Http {
        url: Url,
        auth: Option<String>,
    },
    Socks5 {
        url: Url,
        auth: Option<ProxyAuth>,
        remote_dns: bool,
    },
}

fn basic(a: &ProxyAuth) -> String {
    format!(
        "Basic {}",
        b64(format!("{}:{}", a.username, a.password).as_bytes())
    )
}

/// The host of a URL as a name or a bare address: an IPv6 literal without its
/// URL brackets (`::1`, not `[::1]`), as TLS server names, socket addresses
/// and SOCKS targets want it.
fn host_of(url: &Url) -> Result<String, NetError> {
    match url.host() {
        Some(url::Host::Domain(d)) => Ok(d.to_owned()),
        Some(url::Host::Ipv4(a)) => Ok(a.to_string()),
        Some(url::Host::Ipv6(a)) => Ok(a.to_string()),
        None => Err(NetError::new("URL has no host")),
    }
}

/// An HTTP CONNECT tunnel (docs/spec.md §5.2).
async fn http_connect(
    mut io: BoxIo,
    host: &str,
    port: u16,
    auth: Option<&str>,
) -> Result<BoxIo, NetError> {
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut req = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(a) = auth {
        let _ = write!(req, "Proxy-Authorization: {a}\r\n");
    }
    req.push_str("\r\n");
    io.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    // Read exactly the response head: the tunnel's first bytes belong to TLS.
    while !buf.ends_with(b"\r\n\r\n") {
        if buf.len() > 16 * 1024 {
            return Err(NetError::new("proxy CONNECT response too large"));
        }
        if io.read(&mut byte).await? == 0 {
            return Err(NetError::new("proxy closed the connection during CONNECT"));
        }
        buf.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&buf);
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        let message = if status == 407 {
            "proxy authentication required (407)".to_string()
        } else {
            format!("proxy refused CONNECT with status {status}")
        };
        return Err(NetError {
            status: Some(status),
            message,
        });
    }
    Ok(io)
}

enum Target {
    Domain(String),
    Ip(SocketAddr),
}

/// A SOCKS5 CONNECT (RFC 1928, username/password per RFC 1929).
async fn socks5_connect(
    mut io: BoxIo,
    target: Target,
    port: u16,
    auth: Option<&ProxyAuth>,
) -> Result<BoxIo, NetError> {
    let methods: &[u8] = if auth.is_some() {
        &[0x00, 0x02]
    } else {
        &[0x00]
    };
    let mut hello = vec![0x05, u8::try_from(methods.len()).unwrap_or(1)];
    hello.extend_from_slice(methods);
    io.write_all(&hello).await?;
    let mut resp = [0u8; 2];
    io.read_exact(&mut resp).await?;
    if resp[0] != 0x05 {
        return Err(NetError::new("not a SOCKS5 proxy"));
    }
    match resp[1] {
        0x00 => {}
        0x02 => {
            let a = auth.ok_or_else(|| NetError::new("SOCKS5 proxy requires credentials"))?;
            let (u, p) = (a.username.as_bytes(), a.password.as_bytes());
            if u.len() > 255 || p.len() > 255 {
                return Err(NetError::new("SOCKS5 credentials too long"));
            }
            let mut m = vec![0x01, u8::try_from(u.len()).unwrap_or(0)];
            m.extend_from_slice(u);
            m.push(u8::try_from(p.len()).unwrap_or(0));
            m.extend_from_slice(p);
            io.write_all(&m).await?;
            let mut r = [0u8; 2];
            io.read_exact(&mut r).await?;
            if r[1] != 0x00 {
                return Err(NetError::new("SOCKS5 authentication failed"));
            }
        }
        _ => {
            return Err(NetError::new(
                "SOCKS5 proxy accepts no offered authentication method",
            ));
        }
    }
    let mut req = vec![0x05, 0x01, 0x00];
    match target {
        Target::Domain(d) => {
            let d = d.as_bytes();
            req.push(0x03);
            req.push(
                u8::try_from(d.len())
                    .map_err(|_| NetError::new("host name too long for SOCKS5"))?,
            );
            req.extend_from_slice(d);
        }
        Target::Ip(SocketAddr::V4(a)) => {
            req.push(0x01);
            req.extend_from_slice(&a.ip().octets());
        }
        Target::Ip(SocketAddr::V6(a)) => {
            req.push(0x04);
            req.extend_from_slice(&a.ip().octets());
        }
    }
    req.extend_from_slice(&port.to_be_bytes());
    io.write_all(&req).await?;
    let mut head = [0u8; 4];
    io.read_exact(&mut head).await?;
    if head[1] != 0x00 {
        return Err(NetError::new(format!(
            "SOCKS5 proxy refused the connection (code {})",
            head[1]
        )));
    }
    let skip = match head[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut l = [0u8; 1];
            io.read_exact(&mut l).await?;
            usize::from(l[0])
        }
        _ => return Err(NetError::new("SOCKS5 reply with an unknown address type")),
    };
    let mut rest = vec![0u8; skip + 2];
    io.read_exact(&mut rest).await?;
    Ok(io)
}

#[cfg(test)]
mod tests {
    #[test]
    fn base64_matches_rfc4648() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("Aladdin:opensesame", "QWxhZGRpbjpvcGVuc2VzYW1l"),
        ] {
            assert_eq!(super::b64(i.as_bytes()), o);
        }
    }
}
