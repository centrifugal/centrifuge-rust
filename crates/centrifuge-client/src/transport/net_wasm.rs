//! The browser's stand-in for the native connector (`net.rs`): the browser
//! does DNS, TCP, proxies and TLS itself, so only the settings the shared
//! code reads are kept (docs/wasm-plan.md, step 1).

use std::sync::Mutex;

use crate::config::{ProxyConfig, TlsConfig};

/// Connection settings shared by every attempt of one client.
pub(crate) struct Net {
    pub(crate) proxy: Mutex<ProxyConfig>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net").finish_non_exhaustive()
    }
}

impl Net {
    // The native signature: there, building the TLS config can fail.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn new(
        proxy: ProxyConfig,
        _tls: &TlsConfig,
        _handshake_timeout: std::time::Duration,
    ) -> Result<Self, crate::Error> {
        Ok(Self {
            proxy: Mutex::new(proxy),
        })
    }
}
