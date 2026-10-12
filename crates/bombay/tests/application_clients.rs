//! Root-bound communication preserves affine receiving and truthful sender identity.

use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EstablishedDelivery, EstablishedRecipient, Protocol};
use bombay::prelude::{
    Actions, ActorRetirement, Application, BehaviorActed, Completion, Exit, MailAddr,
};

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use bombay::prelude::TimerId;
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use counter::{Counter, CounterMessage};

struct Receipts;

impl Protocol for Receipts {
    type Addr = MailAddr;
    type Msg = Vec<MailAddr>;
}

enum LedgerCommand {
    Append(Box<[u8]>),
    Read(EstablishedRecipient<Receipts>),
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_targets_and_one_shared_caller_preserve_protocols_origins_and_rejections() {
    let original = Box::<[u8]>::from([59, 61]);
    let original_allocation = original.as_ptr();
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let mut peer = application
                .external::<MailLedger>()
                .expect("a real external ledger command endpoint");
            let mut client = application
                .client_to(peer.recipient())
                .unwrap_or_else(|_| panic!("the selected ledger customer is allocated"));
            let sender = application
                .sender_to(peer.recipient())
                .unwrap_or_else(|_| panic!("the selected ledger sender is allocated"));
            assert_ne!(peer.address(), client.address());
            assert_ne!(sender.address(), client.address());
            let mut root_client = application.client().expect("counter customer");
            peer.send(
                &application.root().established_recipient(),
                CounterMessage::Increment,
            )
            .await
            .expect("the same external actor can address another concrete protocol");
            root_client
                .send(CounterMessage::Read(root_client.recipient()))
                .await
                .expect("the actual counter admits the read");
            let root_reply = root_client.receive().await.expect("actual counter reply");
            assert_eq!(root_reply.message, 1);
            assert_eq!(root_reply.from, application.root().address());

            client
                .send(LedgerCommand::Read(client.recipient()))
                .await
                .expect("the selected ledger endpoint admits its own protocol");
            let request = peer.receive().await.expect("original customer command");
            assert_eq!(request.from, client.address());
            let LedgerCommand::Read(reply_to) = request.message else {
                panic!("the complete original read capability is received");
            };
            peer.send(&reply_to, vec![request.from])
                .await
                .expect("the same external actor replies to a different target protocol");
            let reply = client.receive().await.expect("the selected target's reply");
            assert_eq!(reply.from, peer.address());
            assert_eq!(reply.message, [client.address()]);

            sender
                .send(LedgerCommand::Append(original))
                .await
                .expect("one-way admission retains the original allocation");
            let accepted = peer.receive().await.expect("exact accepted command");
            assert_eq!(accepted.from, sender.address());
            let LedgerCommand::Append(original) = accepted.message else {
                panic!("the complete accepted append survives");
            };
            peer.close_admission();
            for rejected_by in [client.address(), sender.address()] {
                let payload = Box::<[u8]>::from([67, 71]);
                let allocation = payload.as_ptr();
                let rejected = if rejected_by == client.address() {
                    client.send(LedgerCommand::Append(payload)).await
                } else {
                    sender.send(LedgerCommand::Append(payload)).await
                }
                .expect_err("closed exact target returns its original command");
                let LedgerCommand::Append(payload) = rejected.into_message() else {
                    panic!("rejection retains the entire original append");
                };
                assert_eq!(payload.as_ptr(), allocation);
                assert_eq!(&*payload, &[67, 71]);
            }
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            original
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller owns the entered host"));
    let (original, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("exact accepted work remains independently owned"));
    assert_eq!(original.as_ptr(), original_allocation);
    assert_eq!(&*original, &[59, 61]);
    let (_, retirement) = cleanup.expect("complete counter cleanup is acquired");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let custody = retirement.custody().expect("the actual counter survives");
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
}

struct MailLedger {
    entries: Vec<(MailAddr, Box<[u8]>)>,
}

#[bombay::actor(sends = { receipts: Vec<EstablishedDelivery<Receipts>> })]
impl MailLedger {
    fn receive(&mut self, from: MailAddr, command: LedgerCommand) -> BehaviorActed<Self> {
        match command {
            LedgerCommand::Append(original) => {
                self.entries.push((from, original));
                Ok(Actions::cont())
            }
            LedgerCommand::Read(reply_to) => {
                let origins = self.entries.iter().map(|(origin, _)| *origin).collect();
                Ok(Actions::stop().send_receipts(EstablishedDelivery::new(reply_to, origins)))
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exported_targets_use_the_same_client_and_sender_authority() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let interface = application.interface(application.root().established_recipient());
            let sender = interface
                .sender_to(interface.api().clone())
                .unwrap_or_else(|_| panic!("the exported counter sender is allocated"));
            let mut client = interface
                .client_to(interface.api().clone())
                .unwrap_or_else(|_| panic!("the exported counter customer is allocated"));
            let root_sender = application.sender().expect("same application allocator");
            assert_ne!(sender.address(), client.address());
            assert_ne!(sender.address(), root_sender.address());
            assert_ne!(client.address(), root_sender.address());
            sender
                .send(CounterMessage::Increment)
                .await
                .expect("the explicitly exported counter admits the command");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .expect("the selected exported counter admits its exact reply capability");
            let reply = client
                .receive()
                .await
                .expect("the exported counter replies");
            assert_eq!(reply.from, application.root().address());
            assert_eq!(reply.message, 1);
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller host is entered"));
    let ((), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the independent caller completes"));
    let (_, retirement) = cleanup.expect("the original root is received");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let custody = retirement.custody().expect("all root custody remains");
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
}

#[tokio::test(flavor = "current_thread")]
async fn inferred_root_client_reads_counter_without_an_interface_product() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let mut client = application
                .client()
                .expect("the actual reply endpoint is allocated");
            client
                .send(CounterMessage::Increment)
                .await
                .expect("increment admitted");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .expect("read admitted");
            let reply = client.receive().await.expect("the exact business reply");
            assert_eq!(reply.from, application.root().address());
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            reply.message
        })
        .await
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let (value, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("counter work completes independently of cleanup"));
    assert_eq!(value, 1);
    let (_, retirement) = cleanup.expect("the complete actual root retirement is acquired");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let custody = retirement
        .custody()
        .expect("all original actor custody remains");
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
}

#[tokio::test(flavor = "current_thread")]
async fn sender_and_client_keep_distinct_origins_and_exact_closed_root_rejections() {
    let first = Box::<[u8]>::from([13, 17]);
    let second = Box::<[u8]>::from([19, 23]);
    let allocations = (first.as_ptr(), second.as_ptr());
    let outcome = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let sender = application
            .sender()
            .expect("the real one-way sender is allocated");
        let mut client = application
            .client()
            .expect("the real reply mailbox is allocated");
        let origins = (sender.address(), client.address());
        assert_ne!(origins.0, origins.1);
        assert_ne!(origins.0, application.root().address());
        assert_ne!(origins.1, application.root().address());
        sender
            .send(LedgerCommand::Append(first))
            .await
            .expect("one-way command admitted");
        client
            .send(LedgerCommand::Append(second))
            .await
            .expect("client command admitted");
        client
            .send(LedgerCommand::Read(client.recipient()))
            .await
            .expect("read admitted");
        let receipt = client
            .receive()
            .await
            .expect("the real reply preserves both origins");
        assert_eq!(receipt.from, application.root().address());
        assert_eq!(receipt.message, [origins.0, origins.1]);
        let termination = application.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
        let late = Box::<[u8]>::from([29, 31]);
        let original = late.as_ptr();
        let rejection = client
            .send(LedgerCommand::Append(late))
            .await
            .expect_err("the old exact root is closed");
        let LedgerCommand::Append(returned) = rejection.into_message() else {
            panic!("the entire original rejected command returns");
        };
        assert_eq!(returned.as_ptr(), original);
        assert_eq!(&*returned, &[29, 31]);
        let late = Box::<[u8]>::from([37, 41]);
        let original = late.as_ptr();
        let rejection = sender
            .send(LedgerCommand::Append(late))
            .await
            .expect_err("the one-way sender retains the same rejection law");
        let LedgerCommand::Append(returned) = rejection.into_message() else {
            panic!("the entire original one-way command returns");
        };
        assert_eq!(returned.as_ptr(), original);
        assert_eq!(&*returned, &[37, 41]);
        origins
    })
    .await
    .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let (origins, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("business work and original cleanup remain independent"));
    let (_, ActorRetirement::Completed { custody, .. }) = cleanup.expect("the actual root result")
    else {
        panic!("the exact ledger completes after its accepted reply");
    };
    assert_eq!(custody.child_failures, []);
    assert_eq!(custody.descendants, []);
    let ledger = custody.behavior.base();
    assert_eq!(ledger.entries.len(), 2);
    assert_eq!(ledger.entries[0].0, origins.0);
    assert_eq!(ledger.entries[1].0, origins.1);
    assert_eq!(ledger.entries[0].1.as_ptr(), allocations.0);
    assert_eq!(ledger.entries[1].1.as_ptr(), allocations.1);
}

#[tokio::test(flavor = "current_thread")]
async fn root_client_protocol_survives_both_timer_shutdown_orders() {
    let actor = Counter::new()
        .with_one_shot(TimerId(13), Duration::from_secs(3600), |_| Actions::stop())
        .stop_on_shutdown();
    let outcome = Application::new(actor)
        .run_with(|application| async move {
            let mut client = application.client().expect("real reply endpoint");
            client
                .send(CounterMessage::Increment)
                .await
                .expect("increment admitted");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .expect("read admitted");
            let reply = client.receive().await.expect("the exact counter reply");
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            reply.message
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller owns the entered host"));
    let (value, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work completed"));
    assert_eq!(value, 1);
    let (_, retirement) = cleanup.expect("timer and shutdown retirement retained");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let actor = Counter::new().stop_on_shutdown().with_one_shot(
        TimerId(17),
        Duration::from_secs(3600),
        |_| Actions::stop(),
    );
    let outcome = Application::new(actor)
        .run_with(|application| async move {
            let mut client = application.client().expect("real reply endpoint");
            client
                .send(CounterMessage::Increment)
                .await
                .expect("increment admitted");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .expect("read admitted");
            let reply = client.receive().await.expect("the exact counter reply");
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            reply.message
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller owns the entered host"));
    let (value, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work completed"));
    assert_eq!(value, 1);
    let (_, retirement) = cleanup.expect("shutdown and timer retirement retained");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
}

#[test]
fn root_clients_preserve_authority_and_protocol_denials() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/application_client_receiver_is_affine.rs");
    cases.compile_fail("tests/compile/fail/application_sender_has_no_receive.rs");
    cases.compile_fail("tests/compile/fail/application_client_has_no_lifecycle.rs");
    cases.compile_fail("tests/compile/fail/application_client_wrong_root_message.rs");
    cases.compile_fail("tests/compile/fail/application_client_forged_root.rs");
    cases.compile_fail("tests/compile/fail/application_sender_requires_target_capability.rs");
    cases.compile_fail("tests/compile/fail/application_client_wrong_selected_message.rs");
}

#[test]
fn application_shutdown_requires_its_original_event_capability() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/application_shutdown_has_no_lane.rs");
}

#[tokio::test(flavor = "current_thread")]
async fn closing_root_client_preserves_accepted_reply_and_then_exhausts() {
    let outcome = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let mut client = application.client().expect("real reply mailbox");
        let identity = client.address();
        client
            .send(LedgerCommand::Append(Box::from([47, 53])))
            .await
            .expect("append admitted");
        client
            .send(LedgerCommand::Read(client.recipient()))
            .await
            .expect("read admitted");
        let termination = application.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
        client.close_admission();
        let reply = client
            .receive()
            .await
            .expect("closing retains the accepted reply prefix");
        assert_eq!(reply.from, application.root().address());
        assert_eq!(reply.message, [identity]);
        let mut exhausted = pin!(client.receive());
        let mut context = Context::from_waker(Waker::noop());
        let received = exhausted.as_mut().poll(&mut context);
        assert_eq!(received, Poll::Ready(None));
    })
    .await
    .unwrap_or_else(|_| panic!("the actual host is entered"));
    let ((), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work completed"));
    let (_, retirement) = cleanup.expect("complete actor retirement remains independently owned");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
}
