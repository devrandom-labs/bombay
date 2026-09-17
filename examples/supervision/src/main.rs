//! For learning Behavior Actors' fixed-supervisor construction and initial
//! topology. The template owns ordered roles, activation capacity, recovery,
//! failure, drain, and diagnostic policy; initialization emits one stable
//! proxy creation and its matching observation for every declared role.
//!
//! Bombay does not yet expose an application `PrepareWorkers` capability, so
//! this example stops at the pure Behavior boundary instead of inventing a
//! runtime callback or silently discarding recovery custody.

use core::time::Duration;

use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, DiagnosticDisposition, FailureReaction,
    ImmediateActivation, OrderedRoles, ProxyPhase, Recovery, RestartLimit, RestartRelease,
    Strategy, WorkerSource, WorkerSubmission, fixed,
};
use bombay::behavior::{CreationKind, Never, Step};
use bombay::prelude::{Activate as _, ActorExt, StopOnShutdown};

#[derive(Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

type ManagedWorker = StopOnShutdown<Worker>;

#[derive(Clone, Copy)]
struct Workshop;

impl WorkerSource<WorkerRole, ManagedWorker, ImmediateActivation> for Workshop {
    type WorkerRejection = Never;
    type SourceRejection = Never;
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the fixed-supervisor factory owns a fallible preparation contract"
)]
fn prepare_worker(
    role: &WorkerRole,
) -> Result<WorkerSubmission<ManagedWorker, ImmediateActivation>, Never> {
    match role {
        WorkerRole::Primary => Ok(WorkerSubmission::immediate(Worker.stop_on_shutdown())),
    }
}

fn assert_initial_supervision_plan() {
    let roles = OrderedRoles::new(WorkerRole::Primary, [])
        .expect("the one-role supervisor roster is distinct");
    let activation = ActivationPolicy::new(1).expect("one activation is positive capacity");
    let release = RestartRelease::constant(Duration::from_millis(1))
        .expect("the restart release delay is positive");
    let recovery = Recovery::permanent(
        Workshop,
        Strategy::OneForOne,
        RestartLimit::new(1, Duration::from_secs(30)),
        release,
    );
    let supervisor = fixed(
        prepare_worker,
        roles,
        activation,
        recovery,
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("the declared worker is prepared"));
    let initialized = supervisor
        .initialize()
        .unwrap_or_else(|_| panic!("the fixed supervisor initializes its stable proxy"));

    assert_eq!(initialized.actions.creates.len(), 1);
    let proxy = initialized
        .actions
        .creates
        .iter()
        .next()
        .expect("the primary role owns one stable proxy");
    let creation = proxy.id();
    assert_eq!(proxy.kind(), CreationKind::Birth);
    assert_eq!(proxy.child().phase(), ProxyPhase::Dormant);
    assert_eq!(initialized.actions.sends.proxy_observations.len(), 1);
    let observation = &initialized.actions.sends.proxy_observations[0];
    assert_eq!(observation.child, creation);
    assert_eq!(initialized.actions.become_, Step::Continue);
}

fn main() {
    assert_initial_supervision_plan();
}

#[cfg(test)]
mod tests {
    use super::assert_initial_supervision_plan;

    #[test]
    fn fixed_supervisor_initialization_preserves_role_order_and_correlation() {
        assert_initial_supervision_plan();
    }
}
