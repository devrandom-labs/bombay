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

#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", env!("CARGO_PKG_README")))]

extern crate self as bombay;

#[cfg(panic = "abort")]
compile_error!(
    "bombay requires panic=unwind to classify actor panics and complete terminal retirement"
);

pub use crate::address::MailAddr;
pub use crate::application::{ActorInterface, ExternalActor, ExternalActorError, ExternalTarget};
pub use ::behavior;
pub use ::behavior_actors::{
    composition, diagnostic, discovery, lifecycle, operations, persistence, pool, proxy, routing,
    supervisor, time as timing, worker, workflow,
};
pub mod actors;
pub use crate::application::{
    Application, ApplicationScope, MailboxCapacity, MailboxCapacityError,
};
pub use crate::application::{
    ApplicationBehavior, ApplicationCleanupError, ApplicationClient, ApplicationDefinitionError,
    ApplicationHandle, ApplicationLifecycle, ApplicationOutcome, ApplicationRetirement,
    ApplicationSender, ApplicationStagingError, RunError, WorkOutcome,
};
pub use bombay_engine::{Completion, SettlementFailure};
pub use bombay_macros::{ActorSpaces, TerminalProjection, actor, topology};
pub use communication::TrySendError;
mod actor_execution;
mod actor_outcome;
mod address;
mod application;
pub mod entity;
mod launch;
mod local;
pub use local::effects::services::{ExternalRequest, InterpreterFactory, NativeHost, Runtime};
#[allow(
    dead_code,
    reason = "Bombay uses pair publication while this same private source retains the separately verified keyed Observe API"
)]
mod observe;
#[cfg(feature = "remote")]
pub mod remote;
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
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement,
    ActorRetirementReport, ChildFailure, ChildOrigin, ProjectTerminal, RetirementAssessment,
    RetirementNotificationError, RetirementSchema, RootOrigin,
};
pub use crate::topology::{ApplicationTopology, Hosts, RootRegistration};
pub use crate::worker_preparation::{
    WorkerFactory, WorkerPreparationSource, WorkerPreparationStart,
};

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
        ApplicationClient, ApplicationHandle, ApplicationLifecycle, ApplicationSender,
        ApplicationStagingError, ChildFailure, ChildOrigin, Completion, ExternalActor,
        ExternalActorError, ExternalTarget, MailAddr, RootOrigin, RunError, SettlementFailure,
        TerminalProjection,
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

pub use crate::local::effects::{EstablishNativeChild, NativeChild, PendingChild};
