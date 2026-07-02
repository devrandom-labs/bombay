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
//! Live monitoring of a running actor system for the bombay console.
//!
//! Enabling the `console` feature instruments every actor with a lightweight per-instance
//! monitor (counters, status, mailbox depth) kept in a global registry. Call [`serve`] (or
//! build one with [`Console`]) to expose those snapshots over TCP to a console client.

// A ready-made demo actor system for showcasing the console (used by the `console` example and
// `bombay_console --demo`). Hidden from the docs as it isn't part of the public API; its docs live
// in the module's own `//!` comment so intra-doc links resolve in the module's scope.
#[doc(hidden)]
#[allow(missing_docs, missing_debug_implementations)]
pub mod demo;
pub(crate) mod registry;
mod server;
/// The console wire protocol (the serialization contract with console clients).
///
/// **Unstable:** hidden from the docs because the format may change in any release. The types
/// are public only so a console client crate can deserialize snapshots.
#[doc(hidden)]
#[allow(missing_docs)] // the protocol is intentionally undocumented; see the note above
pub mod wire;

pub use server::{Console, ConsoleHandle, serve};

/// Test-only surface for driving the console source side from cucumber scenarios.
///
/// Gated behind the `testing` feature: exposes the snapshot producer and a hook to reset the
/// process-global registry/counters between scenarios (cucumber shares one process per feature).
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use super::registry::{reset_for_test, snapshot};
    pub use super::server::testing::fail_next_encode;
}
