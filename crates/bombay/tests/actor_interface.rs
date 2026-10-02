use core::any::type_name;

use bombay::behavior::EstablishedDelivery;
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
    let ((), terminal): (_, RootTerminal<_>) = Application::new(Service.stop_on_shutdown())
        .run_with(|application| async move {
            let interface = application.interface(Api {
                service: application.root().established_recipient(),
            });
            let interface_description = format!("{interface:?}");
            assert!(interface_description.contains("ActorInterface"));
            assert!(interface_description.contains("api_type"));
            assert!(interface_description.contains(type_name::<Api>()));
            assert!(!interface_description.contains("EstablishedRecipient"));
            let lifecycle = application.lifecycle();
            let mut caller = interface
                .external::<Replies>()
                .expect("the external actor is established");
            let caller_address = caller.address();

            caller
                .send(
                    &interface.api().service,
                    Command::Get {
                        value: 41,
                        reply_to: caller.recipient(),
                    },
                )
                .await
                .expect("the exact receptionist admits the command");

            let reply = caller
                .receive()
                .await
                .expect("the external actor receives the exact reply");
            assert_eq!(reply.from, MailAddr(0));
            assert_eq!(reply.message, (caller_address, 42));
            let termination = lifecycle.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
        })
        .expect("the application terminates normally");
    assert_completed(terminal);
}

#[test]
fn external_actor_close_drains_the_prefix_and_stale_exact_recipient_never_retargets() {
    let ((), terminal): (_, RootTerminal<_>) =
        Application::new(WaitsForShutdown.stop_on_shutdown())
            .run_with(|application| async move {
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
                assert_eq!(rejected_after_close.into_message(), 30);
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
            })
            .expect("the application terminates normally");
    assert_completed(terminal);
}

#[test]
fn interface_denies_lifecycle_and_receive_authority_duplication() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/external_actor_error.rs");
    cases.compile_fail("tests/compile/fail/actor_interface_has_no_lifecycle.rs");
    cases.compile_fail("tests/compile/fail/external_actor_receiver_is_affine.rs");
}
