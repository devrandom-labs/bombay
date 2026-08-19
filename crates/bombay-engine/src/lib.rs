//! Actor-independent universal Driver over one closed Behavior.
//!
//! # Architecture
//!
//! This crate provides the orchestration layer:
//!
//! - [`Driver`] — the single direct-Behavior causal turn loop
//! - [`Environment`] — actor-independent I/O abstraction
//! - [`Completion`] and [`DriverError`] — exact success and failure vocabulary

mod driver;
mod environment;

#[doc(hidden)]
pub use driver::{ActionsOf, Completion, Driver, DriverError};
#[doc(hidden)]
pub use environment::{ActiveEnvironment, Environment};
