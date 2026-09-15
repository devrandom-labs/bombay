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
//! owning templates. An open-phase Behavior cannot be wrapped in `Stash`:
//!
//! ```compile_fail
//! use behavior::{
//!     Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never,
//!     NoBirths, Stash, User,
//! };
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
//! let _ = Stash::new(OpenPhase, |_| behavior::StashRoute::Deliver);
//! ```

mod driver;
mod environment;

#[doc(hidden)]
pub use driver::{ActionsOf, Completion, Driver, DriverError, DriverRetirement};
#[doc(hidden)]
pub use environment::{ActiveEnvironment, Environment};
