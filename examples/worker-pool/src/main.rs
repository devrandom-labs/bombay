//! For learning Behavior Actors' bounded FIFO pool construction and initial
//! worker topology. The pool owns role order, activation capacity, backlog,
//! interruption, retirement, and recovery policy; its worker owns assignment
//! completion through the `pool_worker` contract.
//!
//! Bombay does not yet expose an application `PrepareWorkers` capability, so
//! this example stops at the pure Behavior boundary instead of recreating the
//! removed 0.13 pool runtime or losing replacement custody.

mod worker;

use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, BacklogCapacity, DiagnosticDisposition, Interruption,
    OrderedRoles, PoolFailureReaction, PoolRecovery, WorkerSubmission, fifo,
};
use bombay::behavior::{CreationKind, Never, Step};
use bombay::prelude::Activate as _;
use worker::SearchWorker;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

fn assert_initial_pool_plan() {
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the one-role worker roster is distinct");
    let pool = fifo(
        |_: &WorkerRole| Ok::<_, Never>(WorkerSubmission::immediate(SearchWorker)),
        roles,
        ActivationPolicy::new(1).expect("one activation is positive capacity"),
        PoolRecovery::temporary(PoolFailureReaction::RetireRole),
        BacklogCapacity::new(8),
        Interruption::Retry,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .unwrap_or_else(|_| panic!("the declared search worker is prepared"));
    let initialized = pool
        .initialize()
        .unwrap_or_else(|_| panic!("the FIFO pool initializes its direct worker"));

    assert_eq!(initialized.actions.creates.len(), 1);
    let worker = initialized
        .actions
        .creates
        .iter()
        .next()
        .expect("the primary role owns one worker creation");
    let creation = worker.id();
    assert_eq!(worker.kind(), CreationKind::Birth);
    assert_eq!(initialized.actions.sends.worker_observations.len(), 1);
    let observation = &initialized.actions.sends.worker_observations[0];
    assert_eq!(observation.child, creation);
    assert_eq!(initialized.actions.become_, Step::Continue);
}

fn main() {
    assert_initial_pool_plan();
}

#[cfg(test)]
mod tests {
    use super::assert_initial_pool_plan;

    #[test]
    fn fifo_initialization_preserves_role_order_and_correlation() {
        assert_initial_pool_plan();
    }
}
