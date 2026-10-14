use bombay::ActorNotificationReceipts;
use bombay::WorkOutcome;
use core::convert::Infallible;
use core::time::Duration;
use std::sync::Arc;

use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, Assignment, BacklogCapacity, DiagnosticDisposition,
    FifoCommand, FifoOutcome, FifoPool, ImmediateActivation, Interruption, OrderedRoles,
    PoolFailureReaction, PoolRecovery, RestartLimit, RestartRelease, SubmissionId, WorkerSource,
    WorkerSubmission, fifo, pool_worker,
};
use bombay::behavior::{Actions, BehaviorBase, CreationKind, MessageProtocol};
use bombay::prelude::Exit;
use bombay::prelude::{ActorRetirement, Completion, MailAddr};
use bombay::{Application, WorkerPreparationSource, WorkerPreparationStart};
use std::collections::VecDeque;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerAvailability {
    RetireAfterAssignment,
    RemainAvailable,
}

struct RecoverableWorker {
    payload: Box<[u8]>,
    availability: WorkerAvailability,
}

#[pool_worker(addr = MailAddr, result = u16)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the worker's pure transition uses the owning Behavior result contract"
)]
impl RecoverableWorker {
    fn transition(&mut self, assignment: Assignment<u8>) -> WorkerActed<Self> {
        match self.availability {
            WorkerAvailability::RetireAfterAssignment => {
                let result = u16::from(*assignment.payload());
                Ok(Actions::stop().with_send(assignment.complete(result)))
            }
            WorkerAvailability::RemainAvailable => {
                let result = u16::from(*assignment.payload());
                Ok(Actions::cont().with_send(assignment.complete(result)))
            }
        }
    }
}

struct RecoveryWorkshop {
    admission: Arc<Semaphore>,
    workers: VecDeque<RecoverableWorker>,
}

#[derive(Debug)]
enum WorkerRejection {
    SourceClosed,
    Exhausted,
}

struct WorkshopClosed {
    worker: RecoverableWorker,
}

impl WorkerSource<WorkerRole, RecoverableWorker, ImmediateActivation> for RecoveryWorkshop {
    type WorkerRejection = WorkerRejection;
    type SourceRejection = WorkshopClosed;
}

impl WorkerPreparationSource<WorkerRole, RecoverableWorker, ImmediateActivation>
    for RecoveryWorkshop
{
    async fn prepare_first(
        &mut self,
        role: &WorkerRole,
    ) -> WorkerPreparationStart<
        RecoverableWorker,
        ImmediateActivation,
        WorkerRejection,
        WorkshopClosed,
    > {
        assert_eq!(*role, WorkerRole::Primary);
        let Some(worker) = self.workers.pop_front() else {
            return WorkerPreparationStart::WorkerRejected(WorkerRejection::Exhausted);
        };
        match self.admission.acquire().await {
            Ok(permit) => {
                drop(permit);
                WorkerPreparationStart::Submitted(WorkerSubmission::immediate(worker))
            }
            Err(_) => WorkerPreparationStart::SourceRejected(WorkshopClosed { worker }),
        }
    }

    async fn prepare_next(
        &mut self,
        role: &WorkerRole,
    ) -> Result<WorkerSubmission<RecoverableWorker, ImmediateActivation>, WorkerRejection> {
        assert_eq!(*role, WorkerRole::Primary);
        let permit = self
            .admission
            .acquire()
            .await
            .map_err(|_| WorkerRejection::SourceClosed)?;
        let worker = self.workers.pop_front().ok_or(WorkerRejection::Exhausted)?;
        drop(permit);
        Ok(WorkerSubmission::immediate(worker))
    }
}

type Pool = FifoPool<
    WorkerRole,
    RecoverableWorker,
    ImmediateActivation,
    RecoveryWorkshop,
    Infallible,
    u8,
    u16,
>;
type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, u8, u16>>;

bombay::topology! {
    pool_application {
        root { Worker => worker; }
        worker {}
        hosts { customers: CustomerProtocol; }
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native source refusal trace retains original worker allocations, source diagnostic, independent caller work and complete root/child retirement"
)]
async fn closed_replacement_source_preserves_original_request_actor_work_and_cleanup() {
    let first: Box<[u8]> = Box::from([2, 5]);
    let first_allocation = first.as_ptr();
    let replacement: Box<[u8]> = Box::from([11, 19]);
    let replacement_allocation = replacement.as_ptr();
    let work = Box::new(String::from("source refusal preserves caller work"));
    let work_allocation = work.as_ptr();
    let admission = Arc::new(Semaphore::new(1));
    admission.close();
    let mut initial = Some(RecoverableWorker {
        payload: first,
        availability: WorkerAvailability::RetireAfterAssignment,
    });
    let pool: Pool = fifo(
        move |_: &WorkerRole| {
            WorkerSubmission::immediate(initial.take().expect("one original worker"))
        },
        OrderedRoles::new(WorkerRole::Primary, []).expect("one real role"),
        ActivationPolicy::new(1).expect("one active worker"),
        PoolRecovery::permanent(
            RecoveryWorkshop {
                admission: Arc::clone(&admission),
                workers: VecDeque::from([RecoverableWorker {
                    payload: replacement,
                    availability: WorkerAvailability::RemainAvailable,
                }]),
            },
            RestartLimit::new(1, Duration::from_secs(30)),
            RestartRelease::immediate(),
            PoolFailureReaction::StopPool,
        ),
        BacklogCapacity::new(8),
        Interruption::Retry,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .unwrap_or_else(|_| panic!("initial worker definition prepared"));
    let outcome = Application::with_topology(pool, pool_application::Topology::new())
        .run_with(move |application| async move {
            let mut caller = application.client().expect("real customer");
            let customer = caller.recipient();
            caller
                .send(FifoCommand::submit(SubmissionId::new(41), 8, customer))
                .await
                .expect("job admitted");
            let admitted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                .await
                .expect("pool admits original job")
                .expect("exact admission outcome")
                .message;
            let (submission, job) = admitted.into_accepted().unwrap_or_else(|original| {
                drop(original);
                panic!("first outcome is original job admission");
            });
            assert_eq!(submission, SubmissionId::new(41));
            let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                .await
                .expect("first worker completes original job")
                .expect("exact completion outcome")
                .message;
            assert_eq!(completed.role(), Some(&WorkerRole::Primary));
            let (completed_job, result) = completed.into_completed().unwrap_or_else(|original| {
                drop(original);
                panic!("second outcome completes the original job");
            });
            assert_eq!(completed_job, job);
            assert_eq!(result, 8);
            let termination =
                tokio::time::timeout(Duration::from_secs(5), application.termination())
                    .await
                    .expect("real source refusal stops pool");
            (work, termination)
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("live entered application");
        })
        .into_parts();
    let (
        WorkOutcome::Completed {
            output: (work, termination),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = outcome
    else {
        panic!("caller work and full independent cleanup return");
    };
    assert_eq!(work.as_ptr(), work_allocation);
    assert_eq!(&*work, "source refusal preserves caller work");
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody,
        mut settlements,
        interpretation,
        source,
        completion,
    } = retirement
    else {
        panic!("source refusal follows normal configured pool shutdown");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none());
    assert!(custody.child_failures.is_empty());
    let mut diagnostics = Vec::new();
    for action in &mut settlements {
        for diagnostic in core::mem::take(&mut action.sends.diagnostics) {
            if let bombay::behavior::SettledItem::Attempted(
                bombay::behavior::ItemSettlement::Accepted(
                    bombay::atomic::DiagnosticAccepted::Terminal(diagnostic),
                ),
            ) = diagnostic
            {
                diagnostics.push(diagnostic);
            } else {
                panic!("complete original accepted terminal diagnostic");
            }
        }
    }
    assert_eq!(diagnostics.len(), 1);
    let (role, previous, stopped, returned_source, rejection) = diagnostics
        .pop()
        .expect("original source failure")
        .into_source_rejection()
        .unwrap_or_else(|original| {
            drop(original);
            panic!("exact source rejection");
        });
    assert_eq!(*role, WorkerRole::Primary);
    assert_eq!(previous.creation(), stopped.child);
    assert_eq!(stopped.outcome, Ok(Exit::Normal));
    assert!(
        returned_source.is_none(),
        "source remains restored in returned original pool"
    );
    assert_eq!(rejection.worker.payload.as_ptr(), replacement_allocation);
    assert_eq!(rejection.worker.payload.as_ref(), [11, 19]);
    assert_eq!(custody.descendants.len(), 1, "no replacement birth");
    for descendant in custody.descendants {
        let pool_application::RootRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(origin.creation_id(), previous.creation());
        assert_eq!(origin.creation_kind(), CreationKind::Birth);
        let ActorRetirement::Completed { custody, .. } = *terminal else {
            panic!("original worker joined");
        };
        assert_eq!(custody.behavior.base().payload.as_ptr(), first_allocation);
        assert_eq!(custody.behavior.base().payload.as_ref(), [2, 5]);
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
        assert!(custody.control.is_empty());
        assert!(custody.user.is_empty());
        assert!(custody.additional_failures.is_empty());
        assert!(custody.received_interpretation.is_none());
        assert!(custody.received_source.is_none());
        assert!(custody.source_index.is_none());
        assert!(custody.acquired_ingress.is_none());
        assert!(custody.retirement_failures.is_empty());
        assert!(custody.terminal_report.is_none());
        assert!(custody.capability_failures.is_empty());
        assert!(custody.unread_owner_cancellation.is_none());
    }
    drop((
        custody.behavior,
        previous,
        stopped,
        returned_source,
        role,
        rejection,
        work,
        settlements,
    ));
}
