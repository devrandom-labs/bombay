//! Generation-local runtime ownership.

mod child_lease;
mod child_publication;
mod child_scope;
mod children;
mod completion;
mod environment;
mod handle;
mod incarnation;
mod incarnation_effects;
pub(crate) mod lifecycle;
mod observation_interpretation;
mod outcome;
mod system;
mod timer_interpretation;

pub use child_lease::ChildLease;
#[cfg(test)]
pub(crate) use child_lease::ChildShutdownEdge;
pub(crate) use child_lease::CoordinatedChild;
#[cfg(test)]
pub(crate) use child_scope::ChildObservationError;
pub(crate) use child_scope::ChildScope;
pub use child_scope::CreationObservationError;
#[cfg(test)]
pub(crate) use children::NoChildren;
#[cfg(test)]
pub(crate) use children::SealedChildRuntime;
pub use children::{
    ChildRuntime, CreationFailure, RuntimeBirthMode, RuntimeEffectError, SystemChildren,
};
pub(crate) use completion::Completion;
pub use environment::ActorEnvironment;
pub use handle::Handle;
pub(crate) use incarnation::{LaunchMode, PreparedIncarnation, ProvisionalIncarnation};
pub use incarnation_effects::IncarnationEffects;
pub(crate) use incarnation_effects::{NoParent, ParentReporter};
pub use lifecycle::{
    Lifecycle, LifecycleEvent, LifecycleSink, LifecycleTransition, NoLifecycle,
    RegistrationIdentity,
};
pub use outcome::TaskOutcome;
pub(crate) use outcome::{IntoPeerOutcome, classify_task};
pub use system::{
    Actor, BehaviorActivation, BehaviorRetirement, RootActivation, RootEndpoint, RootOutcome,
    RootRetirement, System, SystemBirthError,
};
pub use timer_interpretation::ScheduleAfterError;
