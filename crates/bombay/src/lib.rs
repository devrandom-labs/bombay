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

pub use ::behavior;
pub use ::behavior_actors::{
    atomic, composition, discovery, lifecycle, operations, persistence, routing, time as timing,
    workflow,
};
pub use address::MailAddr;
mod actor_interface;
pub use actor_interface::{ActorInterface, ExternalActor, ExternalActorError, ExternalTarget};
pub mod actors;
pub use application::Application;
#[cfg(feature = "axum")]
pub use application_runtime::AxumRunError;
pub use application_runtime::{
    App, ApplicationBehavior, ApplicationHandle, ApplicationLifecycle, RunError,
};
pub use bombay_engine::{Completion, SettlementFailure};
pub use bombay_macros::{HostedAddresses, TerminalProjection, actor};
mod address;
mod application;
mod application_runtime;
mod child_bindings;
pub mod entity;
mod incarnation;
mod interpret;
mod launch;
mod local;
mod observation;
mod observe;
mod outcome;
mod prepare_workers;
mod reports;
mod retirement;
mod terminal;
mod termination;
pub mod testing;
mod time;

pub(crate) use incarnation::Incarnation;
/// The exact Address-owned endpoint table for one concrete Behavior protocol.
///
/// Deliberately `#[doc(hidden)]`: applications claim hosting through the
/// [`HostedAddresses`] trait and its derive instead of naming this alias.
#[doc(hidden)]
pub use launch::LocalAddresses;
pub use local::{ActorRef, SendError};
pub(crate) use outcome::IncarnationOutcome;
pub use prepare_workers::PreparesWorkers;
pub(crate) use retirement::Retirement;
pub use terminal::{ActorOrigin, ActorRetirement, ProjectTerminal};

/// Conventional imports for Bombay applications.
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use crate::AxumRunError;
    pub use crate::actors::ActorExt;
    pub use crate::behavior::{
        Actions, BehaviorActed, ChildDelivery, ChildRole, Children, CreationRejection, Delivery,
        EstablishedCreation, EstablishedDelivery, EstablishedRecipient, Never, Protocol, Recipient,
        Step,
    };
    pub use crate::entity::{
        Entities, EntityActivationError, EntityAdmission, EntityCapacity, EntityDefinition,
        EntityMetrics, EntityRef,
    };
    pub use crate::{
        ActorInterface, ActorOrigin, ActorRef, ActorRetirement, Application, ApplicationBehavior,
        ApplicationHandle, ApplicationLifecycle, Completion, ExternalActor, ExternalActorError,
        ExternalTarget, MailAddr, RunError, SettlementFailure, TerminalProjection,
    };
    pub use behavior_actors::{
        Activate, Crash, Exit, Machine, MachineError, Move, ShutdownRejection, StopOnShutdown,
        TimerId,
    };
}
