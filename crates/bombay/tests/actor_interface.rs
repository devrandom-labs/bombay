use bombay::ActorNotificationReceipts;
use bombay::ApplicationOutcome;
use bombay::TrySendError;
use core::any::type_name;
use core::ptr;
use tokio::runtime::Builder;

use bombay::ProjectTerminal;
use bombay::behavior::{EstablishedDelivery, MessageProtocol};
use bombay::prelude::*;

mod application_support;

use application_support::{RootTerminal, assert_completed};

struct Replies;

impl Protocol for Replies {
    type Addr = MailAddr;
    type Msg = (MailAddr, u64);
}

struct ScalarReplies;

impl Protocol for ScalarReplies {
    type Addr = MailAddr;
    type Msg = u64;
}

fn close_reply_admission(receiver: &ExternalActor<ScalarReplies>) {
    receiver.close_admission();
}

#[derive(Clone)]
struct Api {
    service: EstablishedRecipient<Service>,
}

enum Command {
    Get {
        value: u64,
        reply_to: EstablishedRecipient<Replies>,
    },
}

struct Service;

#[bombay::actor(
    sends = pub(crate) {
        replies: Vec<EstablishedDelivery<Replies>>,
    },
)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "the owning Behavior fold consumes a typed customer capability"
)]
impl Service {
    fn receive(&mut self, from: MailAddr, command: Command) -> BehaviorActed<Self> {
        match command {
            Command::Get { value, reply_to } => {
                Ok(Actions::stop()
                    .send_replies(EstablishedDelivery::new(reply_to, (from, value + 1))))
            }
        }
    }
}

struct WaitsForShutdown;

#[bombay::actor]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unused_self,
    reason = "the owning Behavior fold requires the impossible typed receiver"
)]
impl WaitsForShutdown {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[test]
fn external_actor_sends_with_its_allocated_origin_and_receives_exact_reply() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(Service.stop_on_shutdown())
                .run_with::<RootTerminal<StopOnShutdown<Service>>, _, _, _, _, _>(
                    |application| async move {
                        let interface = application.interface(Api {
                            service: application.root().established_recipient(),
                        });
                        let interface_description = format!("{interface:?}");
                        assert!(interface_description.contains("ActorInterface"));
                        assert!(interface_description.contains("api_type"));
                        assert!(interface_description.contains(type_name::<Api>()));
                        assert!(!interface_description.contains("EstablishedRecipient"));
                        let lifecycle = application.lifecycle();
                        let service_address = application.root().address();
                        let mut caller = interface
                            .external::<Replies>()
                            .expect("the external actor is established");
                        let caller_address = caller.address();

                        let admitted = caller.try_send(
                            &interface.api().service,
                            Command::Get {
                                value: 41,
                                reply_to: caller.recipient(),
                            },
                        );
                        match admitted {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => panic!("the receptionist has capacity"),
                            Err(TrySendError::Closed(_)) => panic!("the receptionist is open"),
                        }

                        let reply = caller
                            .receive()
                            .await
                            .expect("the external actor receives the exact reply");
                        assert_eq!(reply.from, service_address);
                        assert_eq!(reply.message, (caller_address, 42));
                        let termination = lifecycle.termination().await;
                        assert_eq!(termination, Ok(Exit::Normal));
                    },
                ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the application terminates normally");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_completed(terminal, None);
}

#[test]
fn external_actor_close_drains_the_prefix_and_stale_exact_recipient_never_retargets() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(WaitsForShutdown.stop_on_shutdown()).run_with::<RootTerminal<
                StopOnShutdown<WaitsForShutdown>,
            >, _, _, _, _, _>(
                |application| async move {
                    let interface = application.interface(());
                    let lifecycle = application.lifecycle();
                    let sender = interface
                        .external::<ScalarReplies>()
                        .expect("the sender is established");
                    let mut receiver = interface
                        .external::<ScalarReplies>()
                        .expect("the receiver is established");
                    assert_ne!(sender.address(), receiver.address());

                    let recipient = receiver.recipient();
                    sender
                        .send(&recipient, 10)
                        .await
                        .expect("the first reply is accepted");
                    sender
                        .send(&recipient, 20)
                        .await
                        .expect("the second reply is accepted");
                    close_reply_admission(&receiver);
                    let rejected_after_close = sender
                        .send(&recipient, 30)
                        .await
                        .expect_err("closed admission rejects the exact recipient immediately");
                    let rejected_message = rejected_after_close.into_message();
                    assert_eq!(rejected_message, 30);
                    let first_reply = receiver.receive().await.map(|user| user.message);
                    assert_eq!(first_reply, Some(10));
                    let second_reply = receiver.receive().await.map(|user| user.message);
                    assert_eq!(second_reply, Some(20));
                    let exhausted = receiver.receive().await;
                    assert_eq!(exhausted, None);
                    drop(receiver);

                    let rejected = sender
                        .send(&recipient, 31)
                        .await
                        .expect_err("the stale exact endpoint is closed");
                    let rejected_message = rejected.into_message();
                    assert_eq!(rejected_message, 31);
                    let shutdown = lifecycle.request_shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let repeated = lifecycle.request_shutdown();
                    assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the application terminates normally");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_completed(terminal, None);
}

struct DeliveryCommand {
    sequence: usize,
    payload: Box<[u8]>,
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one public Application trace conserves each original command across pressure, retry, closure and complete ordered receiving"
)]
fn external_actor_try_send_keeps_full_and_closed_originals_and_truthful_origin() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(WaitsForShutdown.stop_on_shutdown()).run_with::<RootTerminal<
                StopOnShutdown<WaitsForShutdown>,
            >, _, _, _, _, _>(
                |application| async move {
                    let interface = application.interface(());
                    let sender = interface
                        .external::<MessageProtocol<MailAddr, DeliveryCommand>>()
                        .expect("the sender owns its actual allocated identity");
                    let mut receiver = interface
                        .external::<MessageProtocol<MailAddr, DeliveryCommand>>()
                        .expect("the target owns its exact mailbox consumer");
                    let recipient = receiver.recipient();
                    assert_ne!(sender.address(), receiver.address());
                    let mut accepted_allocations = Vec::new();
                    // Exercise the existing external-mailbox bound, without
                    // selecting a protected-admission resource policy.
                    for sequence in 0..1_024 {
                        let payload = vec![17, 19].into_boxed_slice();
                        accepted_allocations.push(payload.as_ptr());
                        let admitted =
                            sender.try_send(&recipient, DeliveryCommand { sequence, payload });
                        match admitted {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                panic!("the original bounded prefix must fit")
                            }
                            Err(TrySendError::Closed(_)) => panic!("prefix admission remains open"),
                        }
                    }
                    let payload = vec![23, 29].into_boxed_slice();
                    let retry_allocation = payload.as_ptr();
                    let full = sender.try_send(
                        &recipient,
                        DeliveryCommand {
                            sequence: 1_024,
                            payload,
                        },
                    );
                    let original = match full {
                        Err(TrySendError::Full(original)) => original,
                        Err(TrySendError::Closed(_)) => panic!("live pressure is not closure"),
                        Ok(()) => panic!("a full mailbox cannot accept another command"),
                    };
                    assert_eq!(original.sequence, 1_024);
                    assert_eq!(original.payload.as_ref(), [23, 29]);
                    assert_eq!(original.payload.as_ptr(), retry_allocation);
                    let first = receiver
                        .receive()
                        .await
                        .expect("the accepted prefix starts");
                    assert_eq!(first.from, sender.address());
                    assert_eq!(first.message.sequence, 0);
                    assert_eq!(first.message.payload.as_ref(), [17, 19]);
                    assert_eq!(first.message.payload.as_ptr(), accepted_allocations[0]);
                    let retried = sender.try_send(&recipient, original);
                    match retried {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => {
                            panic!("the same command fits the released capacity")
                        }
                        Err(TrySendError::Closed(_)) => panic!("retry admission remains open"),
                    }
                    receiver.close_admission();
                    let payload = vec![31, 37].into_boxed_slice();
                    let closed_allocation = payload.as_ptr();
                    let closed = sender.try_send(
                        &recipient,
                        DeliveryCommand {
                            sequence: 1_025,
                            payload,
                        },
                    );
                    let original = match closed {
                        Err(TrySendError::Closed(original)) => original,
                        Err(TrySendError::Full(_)) => {
                            panic!("closure wins over remaining pressure")
                        }
                        Ok(()) => panic!("closed admission cannot accept a command"),
                    };
                    assert_eq!(original.sequence, 1_025);
                    assert_eq!(original.payload.as_ref(), [31, 37]);
                    assert_eq!(original.payload.as_ptr(), closed_allocation);
                    let replay = sender.try_send(&recipient, original);
                    let original = match replay {
                        Err(TrySendError::Closed(original)) => original,
                        Err(TrySendError::Full(_)) => {
                            panic!("replay cannot reopen closed admission")
                        }
                        Ok(()) => panic!("replay cannot admit the returned command"),
                    };
                    assert_eq!(original.sequence, 1_025);
                    assert_eq!(original.payload.as_ref(), [31, 37]);
                    assert_eq!(original.payload.as_ptr(), closed_allocation);
                    drop(original);
                    for (sequence, allocation) in
                        accepted_allocations.into_iter().enumerate().skip(1)
                    {
                        let accepted = receiver.receive().await.expect("the whole prefix drains");
                        assert_eq!(accepted.from, sender.address());
                        assert_eq!(accepted.message.sequence, sequence);
                        assert_eq!(accepted.message.payload.as_ref(), [17, 19]);
                        assert_eq!(accepted.message.payload.as_ptr(), allocation);
                    }
                    let retried = receiver
                        .receive()
                        .await
                        .expect("one retried command follows");
                    assert_eq!(retried.from, sender.address());
                    assert_eq!(retried.message.sequence, 1_024);
                    assert_eq!(retried.message.payload.as_ref(), [23, 29]);
                    assert_eq!(retried.message.payload.as_ptr(), retry_allocation);
                    let exhausted = receiver.receive().await;
                    assert!(
                        exhausted.is_none(),
                        "neither refusal introduced an extra command"
                    );
                    let mut borrowed_value = 41_u64;
                    let borrowed_allocation = ptr::from_mut(&mut borrowed_value);
                    {
                        let borrowed_sender = interface
                            .external::<MessageProtocol<MailAddr, &mut u64>>()
                            .expect("the sender supports a non-static reply protocol");
                        let mut borrowed_receiver = interface
                            .external::<MessageProtocol<MailAddr, &mut u64>>()
                            .expect("the target owns its borrowed message consumer");
                        let borrowed_recipient = borrowed_receiver.recipient();
                        let admitted =
                            borrowed_sender.try_send(&borrowed_recipient, &mut borrowed_value);
                        match admitted {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                panic!("the borrowed-message mailbox has capacity")
                            }
                            Err(TrySendError::Closed(_)) => {
                                panic!("borrowed-message admission is open")
                            }
                        }
                        let borrowed = borrowed_receiver
                            .receive()
                            .await
                            .expect("the same mutable loan is received");
                        assert_eq!(borrowed.from, borrowed_sender.address());
                        let received_allocation = ptr::from_mut(&mut *borrowed.message);
                        assert_eq!(received_allocation, borrowed_allocation);
                        *borrowed.message += 1;
                    }
                    assert_eq!(borrowed_value, 42);
                    let stopped = application.lifecycle().request_shutdown();
                    assert_eq!(stopped, Ok(()));
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the application completes its actual native retirement");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the Work, native actor and two actual notifications remain independent");
    };
    let terminal: RootTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_completed(terminal, None);
}

#[test]
fn interface_denies_lifecycle_and_receive_authority_duplication() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/external_actor_error.rs");
    cases.compile_fail("tests/compile/fail/actor_interface_has_no_lifecycle.rs");
    cases.compile_fail("tests/compile/fail/external_actor_receiver_is_affine.rs");
    cases.compile_fail("tests/compile/fail/retirement_report_is_runtime_issued.rs");
    cases.compile_fail("tests/compile/fail/retirement_report_constructor_is_private.rs");
}
