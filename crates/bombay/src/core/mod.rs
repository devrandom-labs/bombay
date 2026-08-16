//! Minimal runtime layers built directly above Bombay Engine.
//!
//! This module grows one ownership layer at a time. A crate-private local
//! environment crosses directly from prepared mailbox/address ownership to
//! active ingress, and [`Incarnation`] owns one consuming Driver execution plus
//! terminal retirement. Neither introduces a System.

mod authoring;
#[cfg(test)]
mod behavior_actors_support;
mod generation;
#[cfg(test)]
mod behavior_actors_tests {
    use super::behavior_actors_support::direct;

    include!("../../../../tests/behavior_actors_scenarios.rs");
}
mod incarnation;
mod launch;
mod local;
mod outcome;
mod retirement;

pub use authoring::Effect;
pub(crate) use incarnation::Incarnation;
pub use launch::{LocalActors, SpawnError};
pub use local::{ActorRef, SendError};
pub(crate) use outcome::IncarnationOutcome;
pub(crate) use retirement::Retirement;
