//! Typed worker-source preparation for fixed supervision and FIFO pools.
//!
//! Behavior Actors' [`WorkerSource`] deliberately declares the type bindings
//! of a source capability without any behavior, because the attempt protocol
//! — one role at a time, in declared order, until the whole request is
//! prepared or one role is rejected — belongs to the runtime. This module
//! completes that contract: Bombay owns the one preparation method an
//! application implements, and a pure driver walks the affine attempt
//! protocol to its complete [`WorkerPreparation`] settlement.
//!
//! The driver never inspects the settled preparation: its contents stay in
//! Behavior Actors' custody, matched later by the owner's preparation ticket.
//! It also never fabricates an application `SourceRejection` value, so every
//! settlement it returns is [`ItemSettlement::Accepted`], carrying either the
//! all-roles-prepared outcome or the exhaustive typed rejection (prepared
//! prefix, failed role, exact reason, untouched suffix).

use core::ops::ControlFlow;

use behavior::Behavior;
use behavior_actors::atomic::{
    ActivationPlan, PendingWorkerPreparation, PrepareWorkers, WorkerPreparation, WorkerSource,
    WorkerSubmission,
};

/// The preparation port of one exact worker source.
///
/// Implemented by the application that owns worker creation; invoked only by
/// the Bombay interpreter outside every behavior fold, exactly like
/// [`behavior_actors::atomic::ActivationPlan::activate`] and the diagnostic
/// route family. The static [`WorkerSource`] supertrait binds the source, its
/// ordered roles, the concrete worker behavior, and its activation plan.
pub trait PreparesWorkers<Role, Worker, Plan>: WorkerSource<Role, Worker, Plan>
where
    Worker: Behavior,
    Plan: ActivationPlan,
{
    /// Prepare one submission for the exact ordered role, or return the
    /// typed per-role rejection.
    fn prepare_worker(
        &mut self,
        role: &Role,
    ) -> Result<WorkerSubmission<Worker, Plan>, Self::WorkerRejection>;
}

/// One walk position across the affine attempt protocol.
///
/// `Opening` holds the original request while its first role is attempted;
/// `Pending` holds the partially prepared request over its remaining roles.
/// Both stages expose the same attempt surface, so the walk is a loop over
/// this closed sum rather than recursion.
enum PreparationStage<Source, Role, Worker, Plan>
where
    Worker: Behavior,
    Plan: ActivationPlan,
{
    Opening(PrepareWorkers<Source, Role, Worker, Plan>),
    Pending(PendingWorkerPreparation<Source, Role, Worker, Plan>),
}

/// Drive one complete ordered preparation walk to its settled outcome.
///
/// Roles are attempted strictly in the source's declared order. The first
/// typed role rejection ends the walk with the complete rejection custody —
/// every prepared worker before the failed role, the failed role, its exact
/// reason, and every role after it untouched — exactly as the owning
/// supervisor or pool ticket expects.
pub(crate) fn drive_preparation<Source, Role, Worker, Plan>(
    request: PrepareWorkers<Source, Role, Worker, Plan>,
) -> WorkerPreparation<Source, Role, Worker, Plan>
where
    Source: PreparesWorkers<Role, Worker, Plan>,
    Worker: Behavior,
    Plan: ActivationPlan,
{
    let mut stage = PreparationStage::Opening(request);
    loop {
        match stage {
            PreparationStage::Opening(request) => {
                let (source, role) = request.source_and_role();
                match source.prepare_worker(role) {
                    Ok(submission) => match request.accept(submission) {
                        ControlFlow::Continue(pending) => {
                            stage = PreparationStage::Pending(pending);
                        }
                        ControlFlow::Break(prepared) => return prepared,
                    },
                    Err(reason) => return request.reject(reason),
                }
            }
            PreparationStage::Pending(pending) => {
                let (source, role) = pending.source_and_role();
                match source.prepare_worker(role) {
                    Ok(submission) => match pending.accept(submission) {
                        ControlFlow::Continue(next) => {
                            stage = PreparationStage::Pending(next);
                        }
                        ControlFlow::Break(prepared) => return prepared,
                    },
                    Err(reason) => return pending.reject(reason),
                }
            }
        }
    }
}
