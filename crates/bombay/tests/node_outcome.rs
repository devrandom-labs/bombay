#![cfg(feature = "remote")]

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use bombay::actors::ActorExt;
use bombay::behavior::{ClassifySettlement, Never, SettlementStatus};
use bombay::prelude::*;
use bombay::remote::NodeExecutionRefusal;
use counter::{Counter, CounterMessage, CounterValue};
use std::cell::Cell;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use zenoh::Session;
use zenoh_config::Config;

fn listening_configuration() -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let declaration = serde_json::json!({
        "mode": "peer", "queries_max_retained": 2,
        "listen": {"endpoints": ["tls/localhost:0"]},
        "scouting": {"multicast": {"enabled": false, "listen": false}, "gossip": {"enabled": false}},
        "transport": {"link": {"protocols": ["tls"], "tls": {
            "root_ca_certificate": certificates.join("root-ca.pem"),
            "listen_certificate": certificates.join("recipient-certificate.pem"),
            "listen_private_key": certificates.join("recipient-private-key.pem"),
            "enable_mtls": true, "verify_name_on_connect": true, "use_public_pki": false
        }}}
    }).to_string();
    Config::from_deserializer(&mut serde_json::Deserializer::from_str(&declaration))
        .expect("the upstream native listening profile")
}

#[test]
fn native_node_returns_original_work_and_complete_retirement_through_one_outcome() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the native SDK host");
    let mut node = host
        .block_on(
            Application::new(Counter::new().stop_on_shutdown())
                .open_node(listening_configuration()),
        )
        .unwrap_or_else(|_| panic!("the actual native node opens"));
    let original = vec![41, 43].into_boxed_slice();
    let allocation = original.as_ptr();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let work = {
        let operation = node.run_with(async move |application, session: &Session| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            assert!(!session.is_closed(), "the actual borrowed Session is live");
            let mut caller = application
                .client::<CounterValue>()
                .expect("the actual counter client");
            caller
                .send(CounterMessage::Increment)
                .await
                .unwrap_or_else(|_| panic!("the increment is admitted"));
            caller
                .send(CounterMessage::Read(caller.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the read is admitted"));
            let reply = caller.receive().await.expect("the actual typed reply");
            let shutdown = application.request_shutdown();
            assert!(
                !session.is_closed(),
                "the original Session remains live after the reply"
            );
            (original, reply, application.root().address(), shutdown)
        });
        let mut operation = pin!(operation);
        let mut context = Context::from_waker(Waker::noop());
        let refused = operation.as_mut().poll(&mut context);
        let Poll::Ready(Err(NodeExecutionRefusal::Host { work, cause })) = refused else {
            panic!("an absent host returns the exact original callable");
        };
        assert!(cause.is_missing_context());
        work
    };
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    host.block_on(async {
        let outcome = node
            .run_with(work)
            .await
            .unwrap_or_else(|_| panic!("the restored declaration retries its original work"));
        let ((original, reply, root_address, shutdown), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the same original work completes"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(ptr::eq(original.as_ptr(), allocation));
        assert_eq!(&*original, &[41, 43]);
        assert_eq!((reply.from, reply.message), (root_address, 1));
        assert_eq!(shutdown, Ok(()));
        assert!(retirement.cleanup().is_ok());
        let notifications = retirement
            .notifications()
            .as_ref()
            .expect("both original notifications return");
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        let (origin, root) = retirement
            .retirement()
            .as_ref()
            .expect("the independent native root returns");
        assert_eq!(origin.address(), root_address);
        assert_counter_retirement(root);
        let closed = node.close().await;
        assert!(closed.is_ok());
    });
}

fn assert_counter_retirement(root: &ActorRetirement<StopOnShutdown<Counter>, Never, Vec<Never>>) {
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        interpretation,
        source,
        settlements,
        ..
    } = root
    else {
        panic!("the original counter stops normally");
    };
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[test]
fn native_join_resumes_after_a_borrowed_wait_and_retains_caller_local_work() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the native SDK host");
    let calls = Cell::new(0);
    let original = Rc::new(vec![47, 53]);
    let allocation = Rc::as_ptr(&original);
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("the actual native node opens"));
        let (entered, entry) = oneshot::channel();
        let (release, permission) = oneshot::channel();
        let outcome = {
            let operation = node.run_with(async |application, session: &Session| {
                calls.set(calls.get() + 1);
                let published = entered.send(session.zid());
                let permitted = permission.await;
                let current = session.zid();
                let shutdown = application.request_shutdown();
                (original, published, permitted, current, shutdown)
            });
            let mut operation = pin!(operation);
            let observed = {
                let borrowing = operation.as_mut();
                let mut borrowing = pin!(borrowing);
                tokio::select! {
                    observed = entry => observed.expect("work entered its real awaited gate"),
                    _ = borrowing.as_mut() => panic!("work cannot finish before permission"),
                }
            };
            let pending = operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            assert!(pending.is_pending());
            let released = release.send(());
            assert!(released.is_ok());
            (
                observed,
                operation
                    .await
                    .unwrap_or_else(|_| panic!("same owned join resumes")),
            )
        };
        let (observed, outcome) = outcome;
        let ((original, published, permitted, current, shutdown), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("caller-local work completes"));
        assert_eq!(calls.get(), 1);
        assert!(ptr::eq(Rc::as_ptr(&original), allocation));
        assert_eq!(&**original, &[47, 53]);
        assert_eq!(published, Ok(()));
        assert_eq!(permitted, Ok(()));
        assert_eq!(observed, current);
        assert_eq!(shutdown, Ok(()));
        assert!(retirement.cleanup().is_ok());
        let notifications = retirement
            .notifications()
            .as_ref()
            .expect("both notifications return");
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        let (origin, root) = retirement
            .retirement()
            .as_ref()
            .expect("native root returns");
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_counter_retirement(root);
        let closed = node.close().await;
        assert!(closed.is_ok());
    });
}

#[test]
fn raw_native_execution_retains_a_result_borrowing_its_original_session() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the native SDK host");
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("the actual native node opens"));
        {
            let (execution, receiving) = node
                .execute_with(|application, session| async move {
                    let mut caller = application
                        .client::<CounterValue>()
                        .expect("the actual client");
                    caller
                        .send(CounterMessage::Read(caller.recipient()))
                        .await
                        .unwrap_or_else(|_| panic!("the real read is admitted"));
                    let reply = caller.receive().await.expect("the actual reply");
                    let current = session.zid();
                    let shutdown = application.request_shutdown();
                    (session, current, reply.message, shutdown)
                })
                .unwrap_or_else(|_| panic!("the raw lending operation issues"));
            execution.await;
            let outcome = receiving.await;
            let ((session, current, reply, shutdown), retirement) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("the original borrowed result returns"));
            assert_eq!(session.zid(), current);
            assert!(!session.is_closed());
            assert_eq!(reply, 0);
            assert_eq!(shutdown, Ok(()));
            assert!(retirement.cleanup().is_ok());
            let notifications = retirement
                .notifications()
                .as_ref()
                .expect("independent notifications return");
            assert!(notifications.termination.is_ok());
            assert!(notifications.retirement.is_ok());
            let (origin, root) = retirement
                .retirement()
                .as_ref()
                .expect("native root returns");
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_counter_retirement(root);
        }
        let closed = node.close().await;
        assert!(closed.is_ok());
    });
}

#[test]
fn native_join_returns_original_work_for_closed_and_already_issued_nodes() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the native SDK host");
    host.block_on(async {
        let mut closed_node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("the original node opens"));
        let closed = closed_node.close().await;
        assert!(closed.is_ok());
        let original = vec![59, 61].into_boxed_slice();
        let allocation = original.as_ptr();
        let calls = Cell::new(0);
        let Err(NodeExecutionRefusal::SessionClosed { work }) = closed_node
            .run_with(async |application, session: &Session| {
                calls.set(calls.get() + 1);
                assert!(!session.is_closed());
                let shutdown = application.request_shutdown();
                (original, shutdown)
            })
            .await
        else {
            panic!("closed Session returns the original callable");
        };
        assert_eq!(calls.get(), 0);
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("a live node opens"));
        let outcome = node
            .run_with(work)
            .await
            .unwrap_or_else(|_| panic!("the uncalled work issues on a live node"));
        let ((original, shutdown), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original work completes"));
        assert!(ptr::eq(original.as_ptr(), allocation));
        assert_eq!(&*original, &[59, 61]);
        assert_eq!(calls.get(), 1);
        assert_eq!(shutdown, Ok(()));
        assert!(retirement.cleanup().is_ok());
        let notifications = retirement
            .notifications()
            .as_ref()
            .expect("independent notifications return");
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        assert_counter_retirement(&retirement.retirement().as_ref().expect("root returns").1);
        let closed = node.close().await;
        assert!(closed.is_ok());

        let original = vec![67, 71].into_boxed_slice();
        let allocation = original.as_ptr();
        let calls = Cell::new(0);
        let Err(NodeExecutionRefusal::ExecutionIssued { work }) = node
            .run_with(async |application, session: &Session| {
                calls.set(calls.get() + 1);
                assert!(!session.is_closed());
                let shutdown = application.request_shutdown();
                (original, shutdown)
            })
            .await
        else {
            panic!("issued declaration takes precedence over closed Session");
        };
        assert_eq!(calls.get(), 0);
        let mut replacement = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("another live node opens"));
        let outcome = replacement
            .run_with(work)
            .await
            .unwrap_or_else(|_| panic!("the refused work issues on another declaration"));
        let ((original, shutdown), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original second work completes"));
        assert!(ptr::eq(original.as_ptr(), allocation));
        assert_eq!(&*original, &[67, 71]);
        assert_eq!(calls.get(), 1);
        assert_eq!(shutdown, Ok(()));
        assert!(retirement.cleanup().is_ok());
        let notifications = retirement
            .notifications()
            .as_ref()
            .expect("independent notifications return");
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        assert_counter_retirement(&retirement.retirement().as_ref().expect("root returns").1);
        let closed = replacement.close().await;
        assert!(closed.is_ok());
    });
}
