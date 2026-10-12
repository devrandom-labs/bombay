//! Run one bounded FIFO pool through Bombay's concrete local application.
//! The pool owns admission, assignment, completion, and orderly worker drain.

use bombay::ActorCustody;
use bombay::ApplicationOutcome;
use tokio::runtime::Builder;
mod worker;

use bombay::Application;
use bombay::atomic::{
    ActivationPolicy, ActorDrainPolicy, BacklogCapacity, DiagnosticDisposition, FifoCommand,
    FifoOutcome, Interruption, OrderedRoles, PoolFailureReaction, PoolRecovery, SubmissionId,
    WorkerSubmission, fifo,
};
use bombay::behavior::{MessageProtocol, Never};
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr};
use worker::{SearchJob, SearchResult, SearchWorker};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, SearchJob, SearchResult>>;

bombay::topology! {
    search_application {
        root { Worker => worker; }
        worker {}
        hosts { customers: CustomerProtocol; }
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
    };
    let spaces = search_application::Topology::new();
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
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
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
