use bombay::WorkOutcome;
use bombay::composition::replies::ExactReplies;
use bombay::{ActorCustody, ActorNotificationReceipts};
use core::convert::Infallible;
use core::time::Duration;
use tokio::runtime::Builder;

use behavior_actors::Exit;
use behavior_actors::diagnostic::DiagnosticDisposition;
use behavior_actors::lifecycle::ActorDrainPolicy;
use behavior_actors::pool::queue::{self, AdmissionRejection, FifoCommand, FifoPool};
use behavior_actors::pool::{
    BacklogCapacity, Interruption, PoolFailureReaction, PoolRecovery, SubmissionId,
};
use behavior_actors::worker::{OrderedRoles, StartupLimit, StartupPlan, WorkerSubmission};
use bombay::Application;
use bombay::behavior::Never;
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin};
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

impl StartupPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    async fn start(self) -> Result<Self::Ready, Self::Rejection> {
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
    ExactReplies,
>;

bombay::topology! {
    pool_application {
        root { Worker => worker; }
        worker {}
    }
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
    queue::new(
        move |_: &WorkerRole| {
            let activation = initial_activation
                .take()
                .expect("the initial role owns one activation notice");
            WorkerSubmission::with_startup(SearchWorker, activation)
        },
        roles,
        queue::Config {
            startup: StartupLimit::new(1).expect("one activation is positive capacity"),
            recovery: PoolRecovery::temporary(PoolFailureReaction::RetireRole),
            backlog,
            interruption: Interruption::Retry,
            shutdown: ActorDrainPolicy::WaitForActorGraph,
            diagnostics: DiagnosticDisposition::terminate(),
        },
    )
    .build()
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
    let spaces = pool_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application = Application::with_topology(pool, spaces);
    let application_outcome = application_host
        .block_on(application.run_with(move |application| async move {
            tokio::time::timeout(Duration::from_secs(5), activation_received)
                .await
                .expect("the pool starts its worker activation")
                .expect("the pool activates its worker");
            let mut caller = application
                .client()
                .expect("the pool caller is established");
            let customer = caller.recipient();
            caller
                .send(FifoCommand::submit(
                    SubmissionId::new(7),
                    SearchJob {
                        document: String::from("bombay behavior"),
                        needle: 'b',
                    },
                    customer,
                ))
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
                .send(FifoCommand::shutdown())
                .await
                .expect("the pool accepts its shutdown command");
            tokio::time::timeout(Duration::from_secs(5), application.termination())
                .await
                .expect("the pool terminates after shutdown")
        }))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs its worker and shuts down");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        _root_report,
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_pool_terminal(origin, terminal);
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
    let spaces = pool_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application = Application::with_topology(pool, spaces);
    let application_outcome = application_host
        .block_on(application.run_with(move |application| async move {
            tokio::time::timeout(Duration::from_secs(5), activation_received)
                .await
                .expect("the pool starts worker activation")
                .expect("the pool activates its worker");
            let mut caller = application
                .client()
                .expect("the pool caller is established");
            let customer = caller.recipient();
            caller
                .send(FifoCommand::submit(
                    SubmissionId::new(7),
                    SearchJob {
                        document: String::from("bombay behavior"),
                        needle: 'b',
                    },
                    customer.clone(),
                ))
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
                .send(FifoCommand::submit(
                    SubmissionId::new(8),
                    rejected_job.clone(),
                    customer,
                ))
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
                .send(FifoCommand::shutdown())
                .await
                .expect("the pool receives orderly shutdown");
            tokio::time::timeout(Duration::from_secs(5), application.termination())
                .await
                .expect("the saturated pool drains its worker")
        }))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool returns the full-backlog job and shuts down");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        _root_report,
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_pool_terminal(origin, terminal);
}

fn assert_orderly_pool_terminal(
    origin: RootOrigin<Pool>,
    terminal: ActorRetirement<
        Pool,
        pool_application::RootRetirements<Pool>,
        Vec<<pool_application::RootRetirements<Pool> as bombay::RetirementSchema>::Failure>,
    >,
) {
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
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
    let pool_application::RootRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the declared role retains one worker");
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = *terminal
    else {
        panic!("the pool shutdown completes the exact worker");
    };
    assert_eq!(child_failures, []);
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants, []);
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete FIFO business, shutdown and retirement trace preserves every custody oracle"
)]
#[test]
fn fifo_search_completes_and_drains_its_worker() {
    let pool = {
        let roles =
            OrderedRoles::new(WorkerRole::Primary, []).expect("the single search role is unique");
        queue::new(
            |_: &WorkerRole| WorkerSubmission::immediate(SearchWorker),
            roles,
            queue::Config {
                startup: StartupLimit::new(1).expect("one activation is positive capacity"),
                recovery: PoolRecovery::temporary(PoolFailureReaction::RetireRole),
                backlog: BacklogCapacity::new(8),
                interruption: Interruption::Retry,
                shutdown: ActorDrainPolicy::WaitForActorGraph,
                diagnostics: DiagnosticDisposition::terminate(),
            },
        )
        .build()
        .unwrap_or_else(|_| panic!("the declared search worker is prepared"))
    };
    let spaces = pool_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(pool, spaces).run_with(|application| async move {
                let mut customer = application
                    .client()
                    .expect("the search customer is established");
                let reply = customer.recipient();
                customer
                    .send(FifoCommand::submit(
                        SubmissionId::new(7),
                        SearchJob {
                            document: String::from("bombay behavior"),
                            needle: 'b',
                        },
                        reply,
                    ))
                    .await
                    .expect("the pool accepts the search submission");
                let accepted = customer
                    .receive()
                    .await
                    .expect("the pool reports accepted admission")
                    .message;
                let (submission, job) = accepted
                    .into_accepted()
                    .unwrap_or_else(|_| panic!("the first customer outcome accepts the job"));
                assert_eq!(submission, SubmissionId::new(7));
                let completed = customer
                    .receive()
                    .await
                    .expect("the worker returns its search result")
                    .message;
                assert_eq!(completed.role(), Some(&WorkerRole::Primary));
                let (completed_job, result) = completed
                    .into_completed()
                    .unwrap_or_else(|_| panic!("the second customer outcome completes the job"));
                assert_eq!(completed_job, job);
                assert_eq!(result.matches, 3);
                customer
                    .send(FifoCommand::shutdown())
                    .await
                    .expect("the pool accepts orderly shutdown");
                application.termination().await
            }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs its worker and shuts down");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        _root_report,
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    assert_eq!(termination, Ok(Exit::Normal));
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the pool completes after its worker graph");
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let pool_application::RootRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the primary role retains one worker");
    let terminal = *terminal;
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("orderly shutdown completes the exact worker");
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 0);
}
