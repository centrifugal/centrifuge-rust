//! Client statistics (docs/spec.md §4.3).

use std::time::SystemTime;

use centrifuge_client_core::machine::{State, TransportKind};

/// Counters since the client was built, for status displays, logs and
/// support tickets. Reading them is cheap: no round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Statistics {
    /// The client state.
    pub state: State,
    /// The transport in use, while connected.
    pub transport: Option<TransportKind>,
    /// When the current connection was established. In a browser, to the
    /// millisecond (`Date.now()`), and `None` if the browser's clock gives
    /// an invalid time.
    pub connected_since: Option<SystemTime>,
    /// Successful connects, including reconnects.
    pub connects: u64,
    /// Connect attempts that didn't end in a connect.
    pub reconnect_attempts: u64,
    /// Bytes received.
    pub bytes_in: u64,
    /// Bytes sent.
    pub bytes_out: u64,
    /// Decoded replies and pushes.
    pub messages_in: u64,
    /// Commands written.
    pub messages_out: u64,
    /// Publications delivered to the app (also with no handler; never one
    /// whose handler panicked).
    pub publications_delivered: u64,
    /// Registered subscriptions.
    pub subscriptions: usize,
}
