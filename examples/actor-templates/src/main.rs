//! For learning how to select a standalone actor template and compose wrapper
//! policy through `ActorExt` without implementing a custom Behavior.

mod processor;

use core::time::Duration;

use bombay::behavior::{Behavior, MachineError};
use bombay::prelude::*;
use processor::{ProcessorError, ProcessorMessage, ProcessorPhase, ProcessorState, transition};

type ProcessorRunError = RunError<MachineError<MailAddr, ProcessorMessage, ProcessorError>>;

struct BoundaryReplies;

impl Protocol for BoundaryReplies {
    type Addr = MailAddr;
    type Msg = Never;
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

fn main() -> Result<(), ProcessorRunError> {
    run_receive_timeout()?;
    run_shutdown()
}

fn run_receive_timeout() -> Result<(), ProcessorRunError> {
    let actor = Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
        .with_receive_timeout(TimerId(1), Duration::from_millis(50), |processor| {
            if processor.state().completed(7) {
                Actions::stop()
            } else {
                Actions::cont()
            }
        })
        .stop_on_shutdown();

    let ((), terminal) = Application::new(actor).run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let interface = application.interface(application.root().established_recipient());
        let boundary = interface
            .external::<BoundaryReplies>()
            .expect("the example boundary is established");
        boundary
            .send(interface.api(), ProcessorMessage::Work(7))
            .await
            .expect("the actor accepts work while closed");
        boundary
            .send(interface.api(), ProcessorMessage::Open)
            .await
            .expect("the actor opens and replays deferred work");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), lifecycle.termination())
                .await
                .expect("the receive-timeout policy terminates before the example deadline"),
            Ok(Exit::Normal)
        );
    })?;
    assert_application_stopped(terminal);
    Ok(())
}

fn run_shutdown() -> Result<(), ProcessorRunError> {
    let ((), terminal) = Application::new(
        Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition).stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
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
