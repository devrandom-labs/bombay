//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

use tokio::runtime::Builder;
#[path = "../../../../examples/counter/src/counter.rs"]
mod counter_actor;

use bombay::behavior::{ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

use counter_actor::{Counter, CounterMessage, CounterValue};

bombay::topology! { counter_application { root: Counter {} } }

#[test]
fn inferred_counter_returns_reply_and_complete_retirement() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(
                Counter::new().stop_on_shutdown(),
                counter_application::Topology::new(),
            )
            .run_with(|application| async move {
                let mut caller = application
                    .client::<CounterValue>()
                    .expect("the counter customer is established");
                caller
                    .send(CounterMessage::Increment)
                    .await
                    .expect("the counter accepts increment");
                caller
                    .send(CounterMessage::Read(caller.recipient()))
                    .await
                    .expect("the counter accepts the exact reply capability");
                let value = caller
                    .receive()
                    .await
                    .expect("the counter replies to the exact customer");
                assert_eq!(value.message, 1);
                let shutdown = application.request_shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
            }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the counter application retains exact cold or joined failure inputs");
        });
    drop(application_host);
    let ((), cleanup) = application_outcome
        .try_into_completed()
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the counter work must complete with its original outcome retained");
        });
    let (origin, retirement) = cleanup.expect("the counter's independent cleanup must be received");
    let retirement = match retirement {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the counter root returns its actual retirement: {failure}")
        }
        retirement => retirement,
    };
    assert!(
        retirement.behavior().is_some(),
        "the counter retains its actor for inspection before owned consumption"
    );
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let terminal = retirement;
    let ActorRetirement::Completed {
        custody,
        settlements,
        completion,
        interpretation,
        source,
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(custody.child_failures.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.additional_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.terminal_report.is_none());
    assert_eq!(custody.capability_failures.len(), 0);
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(completion, Completion::Stopped);
}
