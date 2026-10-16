//! Run one bounded FIFO pool through Bombay's concrete local application.
//! The pool owns admission, assignment, completion, and orderly worker drain.

use bombay::ActorCustody;
use bombay::WorkOutcome;
use tokio::runtime::Builder;

use super::search_worker::{SearchJob, SearchResult, SearchWorker};
use bombay::Application;
use bombay::behavior::MessageProtocol;
use bombay::diagnostic::DiagnosticDisposition;
use bombay::lifecycle::ActorDrainPolicy;
use bombay::pool::queue::{self, FifoCommand, FifoOutcome};
use bombay::pool::{
    BacklogCapacity, Interruption, PoolFailureReaction, PoolRecovery, SubmissionId,
};
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr};
use bombay::worker::{OrderedRoles, StartupLimit, WorkerSubmission};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, SearchJob, SearchResult>>;

bombay::topology! {
    search_application {
        root { Worker => worker; }
        worker {}
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete FIFO business, shutdown and retirement trace preserves every custody oracle"
)]
fn run_search_pool() {
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
    let spaces = search_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let (application_outcome, native, notifications, ()) = application_host
        .block_on(
            Application::with_topology(pool, spaces).run_with(|application| async move {
                let mut customer = application
                    .client::<CustomerProtocol>()
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
                application.lifecycle().termination().await
            }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs its worker and shuts down");
        })
        .into_parts();
    drop(application_host);
    let WorkOutcome::Completed {
        output: termination,
        cleanup: Ok(()),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (root_origin, joined_actor) =
        native.expect("the exact root retirement remains independent");
    assert_eq!(termination, Ok(Exit::Normal));
    let origin = root_origin;
    let terminal = joined_actor;
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
    let search_application::RootRetirements {
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

#[cfg(test)]
mod tests {
    use super::run_search_pool;

    #[test]
    fn fifo_search_completes_and_drains_its_worker() {
        run_search_pool();
    }
}
