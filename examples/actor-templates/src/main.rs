//! For learning how to select a standalone actor template and compose wrapper
//! policy through `ActorExt` without implementing a custom Behavior.

use bombay::ActorNotificationReceipts;
use bombay::ApplicationOutcome;
use tokio::runtime::Builder;
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

    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(actor).run_with::<ApplicationTerminal<_>, _, _, _, _, _>(
                |application| async move {
                    let lifecycle = application.lifecycle();
                    let interface =
                        application.interface(application.root().established_recipient());
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
                    let termination =
                        tokio::time::timeout(Duration::from_secs(1), lifecycle.termination())
                            .await
                            .expect(
                                "the receive-timeout policy terminates before the example deadline",
                            );
                    assert_eq!(termination, Ok(Exit::Normal));
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the receive-timeout actor starts and retires");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal = ApplicationTerminal::project(
        origin,
        match retirement {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual actor returned its joined retirement: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_application_stopped(terminal);
}

fn run_shutdown() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(
                Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
                    .stop_on_shutdown(),
            )
            .run_with::<ApplicationTerminal<_>, _, _, _, _, _>(
                |application| async move {
                    let lifecycle = application.lifecycle();
                    let shutdown = lifecycle.request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let termination = lifecycle.termination().await;
                    assert_eq!(termination, Ok(Exit::Normal));
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the shutdown actor starts and retires");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal = ApplicationTerminal::project(
        origin,
        match retirement {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual actor returned its joined retirement: {failure}")
            }
            retirement => retirement,
        },
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
