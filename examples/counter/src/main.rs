//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

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
    let ((), origin, retirement) = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
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
        })
        .expect("the counter application retains exact cold or joined failure inputs");
    let retirement = retirement.expect("the counter root returns its actual retirement");
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
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
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
