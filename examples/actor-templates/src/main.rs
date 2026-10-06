//! For learning how to select a standalone actor template and compose wrapper
//! policy through `ActorExt` without implementing a custom Behavior.

mod processor;

use core::time::Duration;

use bombay::ProjectTerminal;
use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;
use processor::{ProcessorMessage, ProcessorPhase, ProcessorState, transition};

struct BoundaryReplies;

impl Protocol for BoundaryReplies {
    type Addr = MailAddr;
    type Msg = Never;
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
    run_receive_timeout();
    run_shutdown();
}

fn run_receive_timeout() {
    let actor = Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
        .with_receive_timeout(TimerId(1), Duration::from_millis(50), |processor| {
            if processor.state().completed(7) {
                Actions::stop()
            } else {
                Actions::cont()
            }
        })
        .stop_on_shutdown();

    let ((), origin, retirement) = Application::new(actor)
        .run_with(|application| async move {
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
            let termination = tokio::time::timeout(Duration::from_secs(1), lifecycle.termination())
                .await
                .expect("the receive-timeout policy terminates before the example deadline");
            assert_eq!(termination, Ok(Exit::Normal));
        })
        .expect("the receive-timeout actor starts and retires");
    let terminal = ApplicationTerminal::project(
        origin,
        retirement.expect("the actual actor returned its joined retirement"),
    );
    assert_application_stopped(terminal);
}

fn run_shutdown() {
    let ((), origin, retirement) = Application::new(
        Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition).stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let shutdown = lifecycle.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        let termination = lifecycle.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    })
    .expect("the shutdown actor starts and retires");
    let terminal = ApplicationTerminal::project(
        origin,
        retirement.expect("the actual actor returned its joined retirement"),
    );
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
