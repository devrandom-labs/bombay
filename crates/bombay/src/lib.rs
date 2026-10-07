//! Bombay runtime core, built one ownership layer at a time.
//!
//! [`bombay_engine::Driver`] owns universal Behavior execution. Bombay keeps
//! lifecycle and local-launch plumbing private while exposing typed actor
//! references.
//!
//! # Panic strategy
//!
//! Caught Behavior fold panics preserve their original cause and surviving state
//! for asynchronous retirement. Drop does not join; cleanup needs a live,
//! cooperative host. Other unwinding cannot promise completed cleanup.
//! The runtime requires `panic = "unwind"`; abort destroys recoverable custody.

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
pub use application_runtime::{
    App, ApplicationBehavior, ApplicationDefinitionError, ApplicationHandle, ApplicationLifecycle,
    ApplicationOutcome, ApplicationStagingError, RunError,
};
pub use bombay_engine::{Completion, SettlementFailure};
pub use bombay_macros::{ActorSpaces, TerminalProjection, actor};
mod actor_execution;
mod actor_outcome;
mod address;
mod application;
mod application_runtime;
mod child_bindings;
pub mod entity;
mod interpret;
mod launch;
mod local;
mod observation;
#[allow(
    dead_code,
    reason = "Bombay uses pair publication while this same private source retains the separately verified keyed Observe API"
)]
mod observe;
mod reports;
mod retirement;
mod terminal;
mod termination;
pub mod testing;
mod time;
mod topology;
mod worker_preparation;

pub(crate) use actor_execution::ActorExecution;
pub(crate) use actor_outcome::ActorExecutionOutcome;
pub use launch::ActorSpace;
pub use local::{ActorRef, InstalledActor, LocalActivationRejection, SendError};
pub(crate) use retirement::Retirement;
pub use terminal::{ActorRetirement, ChildFailure, ChildOrigin, ProjectTerminal, RootOrigin};
pub use topology::Hosts;
pub use worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};

/// Conventional imports for Bombay applications.
pub mod prelude {
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
        ActorInterface, ActorRef, ActorRetirement, Application, ApplicationBehavior,
        ApplicationHandle, ApplicationLifecycle, ApplicationStagingError, ChildFailure,
        ChildOrigin, Completion, ExternalActor, ExternalActorError, ExternalTarget, MailAddr,
        RootOrigin, RunError, SettlementFailure, TerminalProjection,
    };
    pub use behavior_actors::{
        Activate, Crash, Exit, Machine, MachineError, Move, ShutdownRejection, StopOnShutdown,
        TimerId,
    };
}
