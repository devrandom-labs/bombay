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
pub use ::behavior::{
    composition, discovery, lifecycle, operations, persistence, routing, time as timing, workflow,
};
pub use address::MailAddr;
mod actor_interface;
pub use actor_interface::{ActorInterface, ExternalActor, ExternalActorError, ExternalTarget};
pub mod actors;
pub use application::Application;
#[cfg(feature = "axum")]
pub use application_runtime::AxumRunError;
pub use application_runtime::{App, ApplicationHandle, ApplicationLifecycle, RunError};
pub use bombay_engine::Completion;
pub use bombay_macros::{ActorSpaces, TerminalProjection, actor};
pub use interpret::EffectInterpretationError;
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
#[allow(
    dead_code,
    reason = "the complete imported Observe algebra remains verified while Bombay narrows its private production consumers"
)]
mod observe;
mod outcome;
mod reports;
mod retirement;
mod terminal;
mod termination;
pub mod testing;
mod time;
mod topology;

pub(crate) use incarnation::Incarnation;
pub use launch::ActorSpace;
pub use local::{ActorRef, SendError};
pub(crate) use outcome::IncarnationOutcome;
pub(crate) use retirement::Retirement;
pub use terminal::{ActorOrigin, ActorRetirement, ProjectTerminal};
pub use topology::Hosts;

/// Conventional imports for Bombay applications.
pub mod prelude {
    #[cfg(feature = "axum")]
    pub use crate::AxumRunError;
    pub use crate::actors::ActorExt;
    pub use crate::behavior::{
        Actions, Activate, BehaviorActed, ChildDelivery, ChildRole, Children, Crash,
        CreationRejection, Delivery, EstablishedCreation, EstablishedDelivery,
        EstablishedRecipient, Exit, Machine, Never, Protocol, Recipient, ShutdownRejection, Step,
        StopOnShutdown, TimerId,
    };
    pub use crate::entity::{
        Entities, EntityActivationError, EntityAdmission, EntityCapacity, EntityDefinition,
        EntityMetrics, EntityRef,
    };
    pub use crate::{
        ActorInterface, ActorOrigin, ActorRef, ActorRetirement, Application, ApplicationHandle,
        ApplicationLifecycle, Completion, EffectInterpretationError, ExternalActor,
        ExternalActorError, ExternalTarget, MailAddr, RunError, TerminalProjection,
    };
}
