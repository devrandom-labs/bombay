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

pub use crate::address::MailAddr;
pub use crate::application::{ActorInterface, ExternalActor, ExternalActorError, ExternalTarget};
pub use ::behavior;
pub use ::behavior_actors::{
    atomic, composition, discovery, lifecycle, operations, persistence, routing, time as timing,
    workflow,
};
pub mod actors;
pub use crate::application::Application;
pub use crate::application::{
    App, ApplicationBehavior, ApplicationCleanupError, ApplicationDefinitionError,
    ApplicationHandle, ApplicationLifecycle, ApplicationOutcome, ApplicationStagingError, RunError,
};
pub use bombay_engine::{Completion, SettlementFailure};
pub use bombay_macros::{ActorSpaces, TerminalProjection, actor};
pub use communication::TrySendError;
mod actor_execution;
mod actor_outcome;
mod address;
mod application;
pub mod entity;
mod launch;
mod local;
#[allow(
    dead_code,
    reason = "Bombay uses pair publication while this same private source retains the separately verified keyed Observe API"
)]
mod observe;
mod retirement;
mod terminal;
mod termination;
pub mod testing;
mod topology;
mod worker_preparation;

pub(crate) use crate::actor_execution::ActorExecution;
pub(crate) use crate::actor_outcome::ActorExecutionOutcome;
pub use crate::launch::ActorSpace;
pub use crate::local::endpoint::{ActorRef, InstalledActor, SendError};
pub use crate::local::environment::LocalActivationRejection;
pub(crate) use crate::retirement::Retirement;
pub use crate::terminal::{
    ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, ActorRetirementReport,
    ChildFailure, ChildOrigin, ProjectTerminal, RetirementAssessment, RetirementNotificationError,
    RootOrigin,
};
pub use crate::topology::Hosts;
pub use crate::worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};

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

#[cfg(test)]
use behavior::{Actions, BehaviorActed, User};

#[cfg(test)]
struct ShutdownLedger {
    entries: Vec<u64>,
    received: Vec<User<MailAddr, Vec<u64>>>,
}
#[cfg(test)]
#[actor]
impl ShutdownLedger {
    fn receive(&mut self, from: MailAddr, entries: Vec<u64>) -> BehaviorActed<Self> {
        self.received.push(User::new(from, entries));
        Ok(Actions::cont())
    }
}
#[cfg(test)]
type LedgerProtocol = ShutdownLedger;
