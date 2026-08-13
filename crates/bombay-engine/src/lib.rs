//! Actor-independent runtime engine composing Bombay Behavior, Transition,
//! and Machine Executor.
//!
//! # Architecture
//!
//! This crate provides the orchestration layer:
//!
//! - `BehaviorMachine` (internal) — adapts a `Behavior` to [`bombay_transition::Machine`]
//! - [`Driver`] — production async executor using
//!   [`bombay_machine_executor::ExclusiveExecutor`] for machine stepping
//! - [`Environment`] — actor-independent I/O abstraction
//! - [`RuntimeEffects`], [`RunError`], [`RunExit`] — effect and terminal types

mod behavior_machine;
mod behavior_machine_tests;
mod driver;
mod driver_tests;
mod environment;
mod inversion_tests;
mod property_tests;
mod run;

pub(crate) use behavior_machine::BehaviorMachine;
pub use driver::{Driver, RuntimeEffects};
pub use environment::Environment;
pub use run::{RunError, RunExit};
