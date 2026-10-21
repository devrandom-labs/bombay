//! Run one bounded FIFO pool through Bombay's concrete local application.
//! The pool owns admission, assignment, completion, and orderly worker drain.

mod worker;

use bombay::Application;
use bombay::diagnostic::DiagnosticDisposition;
use bombay::lifecycle::ActorDrainPolicy;
use bombay::pool::queue::{self, FifoCommand};
use bombay::pool::{
    BacklogCapacity, Interruption, PoolFailureReaction, PoolRecovery, SubmissionId,
};
use bombay::worker::{OrderedRoles, StartupLimit, WorkerSubmission};
use worker::{SearchJob, SearchWorker};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

bombay::topology! {
    search_application {
        root { Worker => worker; }
        worker {}
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the single search role is unique");
    let pool = queue::new(
        |_| WorkerSubmission::immediate(SearchWorker),
        roles,
        queue::Config {
            startup: StartupLimit::new(1).expect("one worker has positive capacity"),
            recovery: PoolRecovery::temporary(PoolFailureReaction::RetireRole),
            backlog: BacklogCapacity::new(8),
            interruption: Interruption::Retry,
            shutdown: ActorDrainPolicy::WaitForActorGraph,
            diagnostics: DiagnosticDisposition::terminate(),
        },
    )
    .build()
    .unwrap_or_else(|_| panic!("the search worker is prepared"));

    let outcome = Application::with_topology(pool, search_application::Topology::new())
        .run_with(|application| async move {
            let mut customer = application
                .client()
                .expect("the search customer is established");
            customer
                .send(FifoCommand::submit(
                    SubmissionId::new(7),
                    SearchJob {
                        document: String::from("bombay behavior"),
                        needle: 'b',
                    },
                    customer.recipient(),
                ))
                .await
                .expect("the pool admits the search submission");
            let accepted = customer.receive().await.expect("search admission").message;
            let (_submission, _job) = accepted
                .into_accepted()
                .unwrap_or_else(|_| panic!("the pool accepts the job"));
            let completed = customer.receive().await.expect("search result").message;
            let (_job, result) = completed
                .into_completed()
                .unwrap_or_else(|_| panic!("the worker completes the job"));
            customer
                .send(FifoCommand::shutdown())
                .await
                .expect("orderly pool shutdown");
            result
        })
        .await
        .unwrap_or_else(|_| panic!("the search application starts"));
    let (result, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the search work completes"));
    let report = retirement
        .root_report()
        .expect("the joined pool published its report");
    println!("actor retirement: {report:?}");
    println!("search matches: {}", result.matches);
    // Execution already performed cleanup; these reports are optional inspection.
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
}
