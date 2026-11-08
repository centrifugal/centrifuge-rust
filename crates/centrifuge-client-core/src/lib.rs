//! Sans-IO protocol state machine behind
//! [`centrifuge-client`](https://docs.rs/centrifuge-client).
//!
//! **Use `centrifuge-client`, not this crate.** It does no IO and has no
//! runtime or clock, so it builds for every target, including
//! `wasm32-unknown-unknown`. Its API outside what `centrifuge-client`
//! re-exports has no stability guarantees.
//!
//! The design is in
//! [`docs/spec.md` §9](https://github.com/centrifugal/centrifuge-rust/blob/main/docs/spec.md).

pub mod codec;
pub mod codes;
pub mod delta;
pub mod filter;
pub mod machine;
pub mod proto;
pub mod types;

/// The clock type the state machine takes (`Core::step`, timers): the caller
/// reads the clock. `std::time::Instant` on native; on `wasm32`, where
/// `std::time::Instant::now()` panics, `web_time::Instant`, which is
/// `std::time::Instant` itself on every other target.
pub mod time {
    #[cfg(not(target_arch = "wasm32"))]
    pub use std::time::Instant;
    #[cfg(target_arch = "wasm32")]
    pub use web_time::Instant;
}

pub use bytes::Bytes;

/// Version of this crate, as published on crates.io.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
