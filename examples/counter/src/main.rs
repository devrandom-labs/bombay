//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

use bombay::ApplicationOutcome;
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
    let ApplicationOutcome::Completed {
        output: (),
        cleanup: Ok((origin, retirement)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let retirement = match retirement {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the counter root returns its actual retirement: {failure}")
        }
        retirement => retirement,
    };
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
                child_failures: (),
                capability_failures,
                unread_owner_cancellation,
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
                interpretation,
                source,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}
