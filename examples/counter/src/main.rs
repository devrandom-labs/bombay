//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

mod counter;

use bombay::behavior::Behavior;
use bombay::prelude::*;

use crate::counter::{Counter, CounterError, CounterMessage, CounterValue};

type CounterRunError = RunError<CounterError>;

struct Api {
    counter: EstablishedRecipient<Counter>,
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
}

fn main() -> Result<(), CounterRunError> {
    let ((), terminal) =
        Application::new(Counter::new().stop_on_shutdown()).run_with(|application| async move {
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
            assert_eq!(lifecycle.request_shutdown(), Ok(()));
            assert_eq!(lifecycle.termination().await, Ok(Exit::Normal));
        })?;
    assert_application_stopped(terminal);
    Ok(())
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
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
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    drop(behavior);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}
