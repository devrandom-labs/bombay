//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

use tokio::runtime::Builder;
mod counter;

use bombay::ProjectTerminal;
use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

use crate::counter::{Counter, CounterMessage, CounterValue};

struct Api {
    counter: EstablishedRecipient<Counter>,
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: RootOrigin<R>,
        terminal: ActorRetirement<R, Self, ()>,
    },
}

fn main() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(Counter::new().stop_on_shutdown()).run_with::<ApplicationTerminal<
                StopOnShutdown<Counter>,
            >, _, _, _, _, _>(
                |application| async move {
                    let lifecycle = application.lifecycle();
                    let interface = application.interface(Api {
                        counter: application.root().established_recipient(),
                    });
                    let mut caller = interface
                        .external::<CounterValue>()
                        .expect("the counter customer is established");
                    caller
                        .send(&interface.api().counter, CounterMessage::Increment)
                        .await
                        .expect("the counter accepts increment");
                    caller
                        .send(
                            &interface.api().counter,
                            CounterMessage::Read(caller.recipient()),
                        )
                        .await
                        .expect("the counter accepts the exact reply capability");
                    let value = caller
                        .receive()
                        .await
                        .expect("the counter replies to the exact customer");
                    assert_eq!(value.message, 1);
                    let shutdown = lifecycle.request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let termination = lifecycle.termination().await;
                    assert_eq!(termination, Ok(Exit::Normal));
                },
            ),
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
    let terminal = <ApplicationTerminal<_> as ProjectTerminal<_, _>>::project(origin, retirement);
    assert_application_stopped(terminal);
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                custody,
                settlements,
                completion,
                interpretation,
                source,
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    let () = custody.child_failures;
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(custody.behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}
