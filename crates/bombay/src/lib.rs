//! Bombay runtime core, built one ownership layer at a time.
//!
//! [`bombay_engine::Driver`] owns universal Behavior execution. Bombay keeps
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

pub use application_runtime::{App, RunError};
pub use behavior;
mod application_runtime;
mod generation;
mod incarnation;
mod interpret;
mod launch;
mod local;
mod observation;
mod outcome;
mod reports;
mod retirement;
mod time;
mod topology;

pub(crate) use incarnation::Incarnation;
pub use launch::ActorSpace;
pub use local::{ActorRef, SendError};
pub(crate) use outcome::IncarnationOutcome;
pub(crate) use retirement::Retirement;
pub use topology::Hosts;

/// Conventional imports for Bombay applications.
pub mod prelude {
    pub use crate::behavior::*;
    pub use crate::{ActorSpace, App, Hosts, RunError};
}
