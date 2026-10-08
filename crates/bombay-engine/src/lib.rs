//! Actor-independent universal Driver over one closed Behavior.
//!
//! # Architecture
//!
//! This crate provides the orchestration layer:
//!
//! - [`Driver`] — the single direct-Behavior causal turn loop
//! - [`Environment`] — actor-independent I/O abstraction
//! - [`DriverRetirement`] — final Behavior and Environment custody paired with
//!   exact [`Completion`] or [`DriverError`] disposition
//!
//! The Engine accepts the complete closed Behavior phase expected by the
//! Engine phase contract. An open-phase Behavior cannot form an Engine Driver:
//!
//! ```compile_fail
//! use behavior::{
//!     Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never,
//!     NoBirths, User,
//! };
//!
//! use bombay_engine::{Driver, Environment};
//!
//! struct OpenPhase;
//!
//! impl Behavior for OpenPhase {
//!     type Protocol = behavior::MessageProtocol<MailAddr, ()>;
//!     type Event = User<MailAddr, ()>;
//!     type Sends = Vec<Never>;
//!     type Ph = u8;
//!     type Error = core::convert::Infallible;
//!     type Birth = NoBirths;
//!
//!     fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
//!         Ok(Actions::cont())
//!     }
//!
//!     fn transition(
//!         &mut self,
//!         _: behavior::ActiveTurn,
//!         _: Self::Event,
//!     ) -> BehaviorActed<Self> {
//!         Ok(Actions::cont())
//!     }
//! }
//!
//! fn open_phase_driver<E: Environment<OpenPhase>>(environment: E) {
//!     let _ = Driver::new(OpenPhase, environment);
//! }
//! ```

mod driver;
mod environment;

#[doc(hidden)]
pub use driver::{ActionsOf, Completion, Driver, DriverError, DriverRetirement, SettlementFailure};
#[doc(hidden)]
pub use environment::{ActiveEnvironment, Environment};
