//! bombay — a typed behavior runtime.
//!
//! The behavior half is [`behavior`]'s pure algebra (depended on, never
//! copied). Bombay Communication supplies the sole mailbox implementation. A
//! behavior is driven sequentially while its environment supplies events and
//! interprets effects. An inert [`Actor`] pairs one behavior with its address,
//! and runtime construction remains centralized in [`System::spawn`].
//! Consumers that may publish an endpoint only after
//! initialization and its effects succeed use [`System::activate`].
//!
//! # Panic strategy
//!
//! Actor task panics are normalized as [`TaskOutcome::Panicked`], and terminal
//! retirement completes during unwinding. The runtime therefore requires
//! `panic = "unwind"`; abort-mode programs cannot preserve these public
//! lifecycle outcomes.

#[cfg(panic = "abort")]
compile_error!(
    "bombay requires panic=unwind to classify actor panics and complete terminal retirement"
);

// The behavior algebra, re-exported so actor users depend on one crate.
pub use behavior;

mod mailbox;
mod routing;
mod runtime;

// Terminal results are part of System's observable API; their implementation
// and all execution authority remain owned by Bombay Behavior Engine.
pub use bombay_engine::{RunError, RunExit};
pub(crate) use mailbox::{EventSender, EventSource, MailboxReceiver};
pub use mailbox::{MailboxAnchor, MailboxConfig, MailboxSender};
pub use routing::{
    ActorRef, AddressInUse, AddressRouter, DeliveryEndpoint, DeliveryRouter, EndpointRegistry,
    IncarnationEndpoint, MailboxDeliveryClosed, ObservesCreations, PeerObservationError,
    PeerObserver, RejectedDelivery, RouteSends, RoutingError, ShutdownRequestError,
};
pub use runtime::{
    Actor, BehaviorActivation, BehaviorRetirement, CreationFailure, Handle, LifecycleEvent,
    LifecycleSink, LifecycleTransition, NoLifecycle, RegistrationIdentity, RootActivation,
    RootEndpoint, RootOutcome, RootRetirement, RuntimeEffectError, ScheduleAfterError, System,
    SystemBirthError, TaskOutcome,
};
pub(crate) use runtime::{
    ActorEnvironment, ChildLease, ChildRuntime, RuntimeBirthMode, SystemChildren,
};
