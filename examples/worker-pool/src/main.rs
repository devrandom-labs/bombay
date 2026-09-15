//! For learning a bounded worker pool with explicit topology, capacity,
//! interruption, restart, assignment ownership, typed replies, and graceful
//! shutdown. Behavior Actors owns every pool policy; Bombay only executes the
//! concrete composition and supplies a truthful external customer.

mod worker;

use core::time::Duration;

use bombay::behavior::composition::RelayChildReports;
use bombay::behavior::{
    ChildHead, ChildTopology, EstablishedRecipient, InterruptionPolicy, JobId, Never,
    PoolCompletion, PoolConfiguration, PoolFailure, PoolMessage, PoolResponse, Proxy,
    RestartPolicy, RestartTiming, WorkerPool,
};
use bombay::prelude::*;
use tokio::time::timeout;
use worker::{ManagedSearchWorker, SearchJob, SearchResult, managed_search_worker};

const PRIMARY_WORKER: u64 = 7;
const EXAMPLE_DEADLINE: Duration = Duration::from_secs(1);

struct SearchReplies;

impl Protocol for SearchReplies {
    type Addr = MailAddr;
    type Msg = PoolResponse<SearchJob, SearchResult, MailAddr>;
}

type StableLayer = fn(ManagedSearchWorker) -> Proxy<ManagedSearchWorker>;
type SearchPool = WorkerPool<
    MailAddr,
    SearchJob,
    SearchResult,
    ManagedSearchWorker,
    EstablishedRecipient<SearchReplies>,
    StableLayer,
>;
type StableSearchWorker = RelayChildReports<
    Proxy<ManagedSearchWorker>,
    ManagedSearchWorker,
    PoolCompletion<SearchResult>,
>;
type SearchPoolRunError = RunError<PoolFailure<MailAddr, SearchJob, SearchResult, Never>>;

#[derive(TerminalProjection)]
#[allow(
    clippy::large_enum_variant,
    reason = "exact unboxed terminal custody is the observable example result"
)]
enum ApplicationTerminal {
    Root {
        origin: ActorOrigin<SearchPool>,
        terminal: ActorRetirement<SearchPool, Self>,
    },
    StableWorker {
        origin: ActorOrigin<SearchPool, ChildHead>,
        terminal: ActorRetirement<StableSearchWorker, Self>,
    },
    WorkerIncarnation {
        origin: ActorOrigin<Proxy<ManagedSearchWorker>, ChildHead>,
        terminal: ActorRetirement<ManagedSearchWorker, Self>,
    },
}

fn main() -> Result<(), SearchPoolRunError> {
    let configuration = PoolConfiguration::new(
        2,
        InterruptionPolicy::Retry,
        RestartPolicy::Permanent,
        2,
        Duration::from_secs(30),
        RestartTiming::Immediate,
    );
    let pool = WorkerPool::new(
        ChildTopology::new([PRIMARY_WORKER], managed_search_worker),
        configuration,
        Proxy::new as StableLayer,
    )
    .expect("the pool has one uniquely named worker slot");

    let (termination, terminal): (_, ApplicationTerminal) =
        Application::new(pool).run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let interface = application.interface(application.root().established_recipient());
            let mut customer = interface
                .external::<SearchReplies>()
                .expect("the search customer is established");
            let job = SearchJob {
                document: "Bombay keeps assignment ownership explicit".to_owned(),
                needle: 'e',
            };
            customer
                .send(
                    interface.api(),
                    PoolMessage::Submit {
                        job: JobId(41),
                        payload: job,
                        reply_to: customer.recipient(),
                    },
                )
                .await
                .expect("the pool admits the search job");

            let accepted = timeout(EXAMPLE_DEADLINE, customer.receive())
                .await
                .expect("the pool accepts before the example deadline")
                .expect("the customer reply lane remains live");
            assert!(matches!(
                accepted.message,
                PoolResponse::Accepted { job: JobId(41) }
            ));
            let completed = timeout(EXAMPLE_DEADLINE, customer.receive())
                .await
                .expect("the worker completes before the example deadline")
                .expect("the customer receives the completion");
            assert!(matches!(
                completed.message,
                PoolResponse::Completed {
                    job: JobId(41),
                    result: SearchResult { matches: 5 },
                }
            ));

            assert_eq!(lifecycle.request_shutdown(), Ok(()));
            lifecycle.termination().await
        })?;

    assert_eq!(termination, Ok(Exit::Normal));
    assert_terminal_tree(terminal);
    Ok(())
}

fn assert_terminal_tree(terminal: ApplicationTerminal) {
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("pool shutdown must preserve the completed coordinator state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert_eq!(behavior.backlog_len(), 0);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(completion, Completion::Stopped);

    let [
        ApplicationTerminal::StableWorker {
            origin,
            terminal:
                ActorRetirement::Completed {
                    control,
                    user,
                    descendants,
                    completion,
                    ..
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("pool shutdown must retain the stable worker terminal")
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), Some(PRIMARY_WORKER));
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(*completion, Completion::Stopped);

    let [
        ApplicationTerminal::WorkerIncarnation {
            origin,
            terminal:
                ActorRetirement::Completed {
                    control,
                    user,
                    descendants,
                    completion,
                    ..
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("pool shutdown must retain the exact worker incarnation")
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), Some(0));
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(*completion, Completion::Stopped);
}
