//! Run one bounded FIFO pool through Bombay's concrete local application.
//! The pool owns admission, assignment, completion, and orderly worker drain.

use bombay::ApplicationOutcome;
use tokio::runtime::Builder;
mod worker;

use core::convert::Infallible;

use bombay::ProjectTerminal;
use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, Assignment, BacklogCapacity, DiagnosticDisposition,
    FifoCommand, FifoOutcome, FifoPool, ImmediateActivation, Interruption, OrderedRoles,
    PoolFailureReaction, PoolRecovery, SubmissionId, WorkerSubmission, fifo,
};
use bombay::behavior::{ChildHead, MessageProtocol, Never};
use bombay::prelude::{
    ActorRetirement, ChildFailure, ChildOrigin, Completion, Exit, MailAddr, RootOrigin,
    StopOnShutdown, TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App};
use worker::{SearchJob, SearchResult, SearchWorker};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

type SearchPool = FifoPool<
    WorkerRole,
    SearchWorker,
    ImmediateActivation,
    Never,
    Infallible,
    SearchJob,
    SearchResult,
>;
type WorkerProtocol = MessageProtocol<MailAddr, Assignment<SearchJob>>;
type PoolProtocol =
    MessageProtocol<MailAddr, FifoCommand<MailAddr, WorkerRole, SearchJob, SearchResult>>;
type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, SearchJob, SearchResult>>;

#[derive(ActorSpaces)]
struct SearchSpaces {
    #[actor_space(PoolProtocol)]
    pool: ActorSpace<PoolProtocol>,
    #[actor_space(WorkerProtocol)]
    workers: ActorSpace<WorkerProtocol>,
    #[actor_space(CustomerProtocol)]
    customers: ActorSpace<CustomerProtocol>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "the projection retains the exact unboxed pool and worker terminals"
)]
#[derive(TerminalProjection)]
enum SearchTerminal {
    Pool {
        origin: RootOrigin<SearchPool>,
        #[expect(
            clippy::type_complexity,
            reason = "the pool retirement retains exact worker origins and complete child failures"
        )]
        terminal: ActorRetirement<
            SearchPool,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<SearchPool, ChildHead>, StopOnShutdown<SearchWorker>>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Worker {
        origin: ChildOrigin<SearchPool, ChildHead>,
        terminal: ActorRetirement<StopOnShutdown<SearchWorker>, Self, ()>,
    },
}

fn search_pool() -> SearchPool {
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the single search role is unique");
    fifo(
        |_: &WorkerRole| Ok::<_, Never>(WorkerSubmission::immediate(SearchWorker)),
        roles,
        ActivationPolicy::new(1).expect("one activation is positive capacity"),
        PoolRecovery::temporary(PoolFailureReaction::RetireRole),
        BacklogCapacity::new(8),
        Interruption::Retry,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .unwrap_or_else(|_| panic!("the declared search worker is prepared"))
}

fn run_search_pool() {
    let spaces = SearchSpaces {
        pool: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(search_pool(), spaces).run_with::<SearchTerminal, _, _, _, _>(
                |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut customer = interface
                        .external::<CustomerProtocol>()
                        .expect("the search customer is established");
                    let reply = customer.recipient();
                    customer
                        .send(
                            interface.api(),
                            FifoCommand::submit(
                                SubmissionId::new(7),
                                SearchJob {
                                    document: String::from("bombay behavior"),
                                    needle: 'b',
                                },
                                reply,
                            ),
                        )
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
                    let (completed_job, result) = completed.into_completed().unwrap_or_else(|_| {
                        panic!("the second customer outcome completes the job")
                    });
                    assert_eq!(completed_job, job);
                    assert_eq!(result.matches, 3);
                    customer
                        .send(interface.api(), FifoCommand::shutdown())
                        .await
                        .expect("the pool accepts orderly shutdown");
                    application.lifecycle().termination().await
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs its worker and shuts down");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: Some(termination),
        cleanup: Ok(Ok((root_origin, joined_actor))),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: SearchTerminal = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );
    assert_eq!(termination, Ok(Exit::Normal));
    assert_search_terminal(terminal);
}

fn assert_search_terminal(terminal: SearchTerminal) {
    let SearchTerminal::Pool { origin, terminal } = terminal else {
        panic!("the application returns the pool root");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
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
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SearchTerminal::Worker { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the primary role retains one worker")
    else {
        panic!("the declared child is the search worker");
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        capability_failures,
        unread_owner_cancellation,
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("orderly shutdown completes the exact worker");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert!(descendants.is_empty());
}

fn main() {
    run_search_pool();
}

#[cfg(test)]
mod tests {
    use super::run_search_pool;

    #[test]
    fn fifo_search_completes_and_drains_its_worker() {
        run_search_pool();
    }
}
