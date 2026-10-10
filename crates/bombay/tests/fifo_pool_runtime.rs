use bombay::ActorNotificationReceipts;
use bombay::ApplicationOutcome;
use core::convert::Infallible;
use core::time::Duration;
use tokio::runtime::Builder;

use behavior_actors::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, AdmissionRejection, Assignment,
    BacklogCapacity, DiagnosticDisposition, FifoCommand, FifoOutcome, FifoPool, Interruption,
    OrderedRoles, PoolFailureReaction, PoolRecovery, SubmissionId, WorkerSubmission, fifo,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::ProjectTerminal;
use bombay::behavior::{ChildHead, MessageProtocol, Never};
use bombay::prelude::{
    ActorRetirement, ChildFailure, ChildOrigin, Completion, MailAddr, RootOrigin,
    TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App};
use tokio::sync::oneshot;

#[path = "../../../examples/worker-pool/src/worker.rs"]
mod worker;

use worker::{SearchJob, SearchResult, SearchWorker};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

struct ActivationNotice {
    started: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
}

impl ActivationPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        self.started
            .send(())
            .unwrap_or_else(|()| panic!("the application awaits worker activation"));
        self.released
            .await
            .expect("the customer releases the worker after queued admission");
        Ok(())
    }
}

type Pool = FifoPool<
    WorkerRole,
    SearchWorker,
    ActivationNotice,
    Never,
    Infallible,
    SearchJob,
    SearchResult,
>;
type WorkerProtocol = MessageProtocol<MailAddr, Assignment<SearchJob>>;
type RootProtocol =
    MessageProtocol<MailAddr, FifoCommand<MailAddr, WorkerRole, SearchJob, SearchResult>>;
type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, SearchJob, SearchResult>>;

#[derive(ActorSpaces)]
struct PoolSpaces {
    #[actor_space(RootProtocol)]
    root: ActorSpace<RootProtocol>,
    #[actor_space(WorkerProtocol)]
    workers: ActorSpace<WorkerProtocol>,
    #[actor_space(CustomerProtocol)]
    customers: ActorSpace<CustomerProtocol>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "this test retains the exact unboxed actor terminal required by TerminalProjection"
)]
#[derive(TerminalProjection)]
enum PoolTerminal {
    Root {
        origin: RootOrigin<Pool>,
        #[expect(
            clippy::type_complexity,
            reason = "the pool retirement retains exact worker origins and complete child failures"
        )]
        terminal: ActorRetirement<
            Pool,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<Pool, ChildHead>, StopOnShutdown<SearchWorker>>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Worker {
        origin: ChildOrigin<Pool, ChildHead>,
        terminal: ActorRetirement<StopOnShutdown<SearchWorker>, Self, ()>,
    },
}

fn prepared_pool(
    activation_started: oneshot::Sender<()>,
    activation_release: oneshot::Receiver<()>,
    backlog: BacklogCapacity,
) -> Pool {
    let mut initial_activation = Some(ActivationNotice {
        started: activation_started,
        released: activation_release,
    });
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the single declared role is unique");
    fifo(
        move |_: &WorkerRole| {
            let activation = initial_activation
                .take()
                .expect("the initial role owns one activation notice");
            Ok::<_, Never>(WorkerSubmission::activated(SearchWorker, activation))
        },
        roles,
        ActivationPolicy::new(1).expect("one activation is positive capacity"),
        PoolRecovery::temporary(PoolFailureReaction::RetireRole),
        backlog,
        Interruption::Retry,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .unwrap_or_else(|_| panic!("the declared worker is prepared"))
}

#[test]
fn fifo_pool_admits_queued_job_before_activation_and_drains_worker() {
    let (activation_started, activation_received) = oneshot::channel();
    let (activation_release, release_received) = oneshot::channel();
    let pool = prepared_pool(
        activation_started,
        release_received,
        BacklogCapacity::new(8),
    );
    let spaces = PoolSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(App::new(pool, spaces).run_with::<PoolTerminal, _, _, _>(
            move |application| async move {
                tokio::time::timeout(Duration::from_secs(5), activation_received)
                    .await
                    .expect("the pool starts its worker activation")
                    .expect("the pool activates its worker");
                let interface = application.interface(application.root().established_recipient());
                let mut caller = interface
                    .external::<CustomerProtocol>()
                    .expect("the pool caller is established");
                let customer = caller.recipient();
                caller
                    .send(
                        interface.api(),
                        FifoCommand::submit(
                            SubmissionId::new(7),
                            SearchJob {
                                document: String::from("bombay behavior"),
                                needle: 'b',
                            },
                            customer,
                        ),
                    )
                    .await
                    .expect("the pool accepts its search submission");
                let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                    .await
                    .expect("the pool admits a job while activation is pending")
                    .expect("the pool reports accepted admission")
                    .message;
                let (submission, job) = accepted
                    .into_accepted()
                    .unwrap_or_else(|_| panic!("the first customer outcome accepts the job"));
                assert_eq!(submission, SubmissionId::new(7));
                activation_release
                    .send(())
                    .unwrap_or_else(|()| panic!("the activation task still owns its release"));
                let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                    .await
                    .expect("the activated worker completes queued work")
                    .expect("the worker returns its search result")
                    .message;
                assert_eq!(completed.role(), Some(&WorkerRole::Primary));
                let (completed_job, result) = completed
                    .into_completed()
                    .unwrap_or_else(|_| panic!("the second customer outcome completes the job"));
                assert_eq!(completed_job, job);
                assert_eq!(result.matches, 3);
                caller
                    .send(interface.api(), FifoCommand::shutdown())
                    .await
                    .expect("the pool accepts its shutdown command");
                let lifecycle = application.lifecycle();
                tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                    .await
                    .expect("the pool terminates after shutdown")
            },
        ))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs its worker and shuts down");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: PoolTerminal = ProjectTerminal::project(root_origin, joined_actor);
    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_pool_terminal(terminal);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
)]
fn fifo_pool_returns_the_exact_payload_when_backlog_is_full() {
    let (activation_started, activation_received) = oneshot::channel();
    let (activation_release, release_received) = oneshot::channel();
    let pool = prepared_pool(
        activation_started,
        release_received,
        BacklogCapacity::new(1),
    );
    let spaces = PoolSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(App::new(pool, spaces).run_with::<PoolTerminal, _, _, _>(
            move |application| async move {
                tokio::time::timeout(Duration::from_secs(5), activation_received)
                    .await
                    .expect("the pool starts worker activation")
                    .expect("the pool activates its worker");
                let interface = application.interface(application.root().established_recipient());
                let mut caller = interface
                    .external::<CustomerProtocol>()
                    .expect("the pool caller is established");
                let customer = caller.recipient();
                caller
                    .send(
                        interface.api(),
                        FifoCommand::submit(
                            SubmissionId::new(7),
                            SearchJob {
                                document: String::from("bombay behavior"),
                                needle: 'b',
                            },
                            customer.clone(),
                        ),
                    )
                    .await
                    .expect("the first search reaches the pool");
                let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                    .await
                    .expect("the first search enters the one-slot backlog")
                    .expect("the pool reports first admission")
                    .message;
                let (submission, job) = accepted
                    .into_accepted()
                    .unwrap_or_else(|_| panic!("the first outcome accepts the queued search"));
                assert_eq!(submission, SubmissionId::new(7));

                let rejected_job = SearchJob {
                    document: String::from("worker recovery"),
                    needle: 'r',
                };
                caller
                    .send(
                        interface.api(),
                        FifoCommand::submit(SubmissionId::new(8), rejected_job.clone(), customer),
                    )
                    .await
                    .expect("the second search reaches the pool");
                let rejected = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                    .await
                    .expect("the full backlog rejects the second search")
                    .expect("the pool returns the rejected search")
                    .message;
                let (rejected_submission, returned_job, reason) = rejected
                    .into_rejected()
                    .unwrap_or_else(|_| panic!("the second outcome rejects the saturated search"));
                assert_eq!(rejected_submission, SubmissionId::new(8));
                assert_eq!(returned_job, rejected_job);
                assert_eq!(reason, AdmissionRejection::BacklogFull);

                activation_release
                    .send(())
                    .unwrap_or_else(|()| panic!("worker activation awaits release"));
                let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                    .await
                    .expect("the worker completes the admitted search")
                    .expect("the worker reports its result")
                    .message;
                assert_eq!(completed.role(), Some(&WorkerRole::Primary));
                let (completed_job, result) = completed
                    .into_completed()
                    .unwrap_or_else(|_| panic!("the admitted search completes"));
                assert_eq!(completed_job, job);
                assert_eq!(result.matches, 3);
                caller
                    .send(interface.api(), FifoCommand::shutdown())
                    .await
                    .expect("the pool receives orderly shutdown");
                let lifecycle = application.lifecycle();
                tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                    .await
                    .expect("the saturated pool drains its worker")
            },
        ))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool returns the full-backlog job and shuts down");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: PoolTerminal = ProjectTerminal::project(root_origin, joined_actor);
    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_pool_terminal(terminal);
}

fn assert_orderly_pool_terminal(terminal: PoolTerminal) {
    let PoolTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns the pool root");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        descendants,
        completion,
        ..
    } = terminal
    else {
        panic!("the pool completes after its worker graph");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let PoolTerminal::Worker { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the declared role retains one worker")
    else {
        panic!("the declared child is the worker");
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (),

        capability_failures,
        unread_owner_cancellation,
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the pool shutdown completes the exact worker");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert!(descendants.is_empty());
}
