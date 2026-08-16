//! Bombay runtime core, built one ownership layer at a time.
//!
//! [`bombay_engine::Driver`] owns universal Behavior execution. [`core`] keeps
//! lifecycle and local-launch plumbing private while exposing typed actor
//! references.
//!
//! # Panic strategy
//!
//! Driver panics are normalized internally, and terminal retirement completes
//! during unwinding. The runtime therefore requires `panic = "unwind"`;
//! abort-mode programs cannot preserve these lifecycle guarantees.

extern crate self as bombay;

#[cfg(panic = "abort")]
compile_error!(
    "bombay requires panic=unwind to classify actor panics and complete terminal retirement"
);

pub use behavior;
pub use bombay_macros::actor;

pub mod core;

pub use core::{ActorRef, Effect, LocalActors, SendError, SpawnError};
