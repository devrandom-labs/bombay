// --- #61 quarantine (vendored kameo, pre-god-level-bar) -------------------
// This file predates the workspace god-level clippy bar (root Cargo.toml).
// It is held at the prior standard and is cleaned or deleted file-by-file
// under M1/M7. NEW code is NOT exempt — remove this block when the file is
// brought up to the bar or dropped. De-quarantine checklist: issue #61.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::disallowed_methods,
    clippy::clone_on_ref_ptr,
    clippy::as_conversions,
    clippy::str_to_string,
    clippy::implicit_clone,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::allow_attributes_without_reason,
    reason = "Vendored kameo predating the #61 god-level clippy bar; held at the prior standard, cleaned or deleted file-by-file under M1/M7. New code is not exempt. See #61."
)]
//! Types for sending requests including messages and queries to actors.

use std::time::Duration;

mod ask;
mod tell;

#[cfg(feature = "remote")]
pub use ask::RemoteAskRequest;

#[cfg(feature = "remote")]
pub use tell::RemoteTellRequest;

pub use ask::{AskRequest, BlockingPendingReply, PendingReply, ReplyRecipientAskRequest};
pub use tell::{RecipientTellRequest, ReplyRecipientTellRequest, TellRequest};

/// A type for requests without any timeout set.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithoutRequestTimeout;

/// A type for timeouts in actor requests.
#[derive(Clone, Copy, Debug, Default)]
pub struct WithRequestTimeout(Option<Duration>);

/// A type which might contain a request timeout.
///
/// This type is used internally for remote messaging and will panic if used incorrectly with any MessageSend trait.
#[derive(Clone, Copy, Debug)]
pub enum MaybeRequestTimeout {
    /// No timeout set.
    NoTimeout,
    /// A timeout with a duration.
    Timeout(Duration),
}

impl From<Option<Duration>> for MaybeRequestTimeout {
    fn from(timeout: Option<Duration>) -> Self {
        match timeout {
            Some(timeout) => MaybeRequestTimeout::Timeout(timeout),
            None => MaybeRequestTimeout::NoTimeout,
        }
    }
}

impl From<WithoutRequestTimeout> for MaybeRequestTimeout {
    fn from(_: WithoutRequestTimeout) -> Self {
        MaybeRequestTimeout::NoTimeout
    }
}

impl From<WithRequestTimeout> for MaybeRequestTimeout {
    fn from(WithRequestTimeout(timeout): WithRequestTimeout) -> Self {
        match timeout {
            Some(timeout) => MaybeRequestTimeout::Timeout(timeout),
            None => MaybeRequestTimeout::NoTimeout,
        }
    }
}

impl From<WithoutRequestTimeout> for Option<Duration> {
    fn from(_: WithoutRequestTimeout) -> Self {
        None
    }
}

impl From<WithRequestTimeout> for Option<Duration> {
    fn from(WithRequestTimeout(duration): WithRequestTimeout) -> Self {
        duration
    }
}

impl From<MaybeRequestTimeout> for Option<Duration> {
    fn from(timeout: MaybeRequestTimeout) -> Self {
        match timeout {
            MaybeRequestTimeout::NoTimeout => None,
            MaybeRequestTimeout::Timeout(duration) => Some(duration),
        }
    }
}
