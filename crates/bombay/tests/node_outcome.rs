#![cfg(feature = "remote")]

mod counter {
    include!("../../../examples/counter/src/counter.rs");

    pub(super) fn retained_value(counter: &Counter) -> u64 {
        counter.value
    }
}

use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, ClassifySettlement, Never, NoSends, SettlementStatus, Stopped,
};
use bombay::prelude::*;
use bombay::remote::{
    ConnectionEndpoints, CredentialSource, Network, NetworkLayout, NodeExecutionRefusal,
};
use counter::{Counter, CounterMessage, CounterValue};
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::pin;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

fn listening_configuration() -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    Network {
        layout: NetworkLayout::Peer {
            connect: None,
            listen: Some(ConnectionEndpoints {
                addresses: vec!["localhost:0".to_owned()],
                certificate: CredentialSource::File(certificates.join("recipient-certificate.pem")),
                private_key: CredentialSource::File(certificates.join("recipient-private-key.pem")),
            }),
        },
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}

#[test]
fn node_work_uses_only_actor_capabilities_and_returns_original_work_for_retry() {
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
    let original_file = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("real non-Clone work custody");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let work = {
        let operation = node.run_with(|application| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
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
            let shutdown = application.shutdown();
            (
                original,
                original_file,
                reply,
                application.root().address(),
                shutdown,
            )
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
        let completed = timeout(Duration::from_secs(10), async {
            let outcome = node
                .run_with(work)
                .await
                .unwrap_or_else(|_| panic!("the restored declaration retries its original work"));
            let ((original, mut original_file, reply, root_address, shutdown), retirement) =
                outcome
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("the same original work completes"));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(ptr::eq(original.as_ptr(), allocation));
            assert_eq!(&*original, &[41, 43]);
            let mut contents = String::new();
            original_file
                .read_to_string(&mut contents)
                .expect("the original non-Clone file remains usable after retry and retirement");
            assert_eq!(contents, include_str!("../Cargo.toml"));
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
            assert_counter_retirement(root, 1);
            let closed = node.close().await;
            assert!(closed.is_ok());
        })
        .await;
        completed.expect("actor work and joined retirement finish within the watchdog");
    });
}

fn assert_counter_retirement(
    root: &ActorRetirement<StopOnShutdown<Counter>, Never, Vec<Never>>,
    expected_value: u64,
) {
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
    assert_eq!(
        counter::retained_value(custody.behavior.base()),
        expected_value
    );
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(settlements.len(), 1);
    let stopped = &settlements[0];
    assert_eq!(stopped.creations.len(), 0);
    assert!(matches!(stopped.sends.owned, NoSends));
    assert_eq!(stopped.sends.inner.values.len(), 0);
    assert_eq!(stopped.sends.inner.processed_increments.len(), 0);
    assert_eq!(stopped.become_, Step::Stop(Stopped));
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
    let calls = &Cell::new(0);
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
            let operation = node.run_with(|application| async move {
                calls.set(calls.get() + 1);
                let published = entered.send(application.root().address());
                let permitted = permission.await;
                let current = application.root().address();
                let shutdown = application.shutdown();
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
        assert_counter_retirement(root, 0);
        let closed = node.close().await;
        assert!(closed.is_ok());
    });
}

#[test]
fn node_execution_uses_actor_capabilities_and_returns_original_work_for_retry() {
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
    let original_file = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("real non-Clone work custody");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let work = {
        let refused = node.execute_with(|application| async move {
            observed_calls.fetch_add(1, Ordering::SeqCst);
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
            let shutdown = application.shutdown();
            (
                original,
                original_file,
                reply,
                application.root().address(),
                shutdown,
            )
        });
        let Err(NodeExecutionRefusal::Host { work, cause }) = refused else {
            panic!("an absent host returns the exact original callable");
        };
        assert!(cause.is_missing_context());
        work
    };
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    host.block_on(async {
        let completed = timeout(Duration::from_secs(10), async {
            let (execution, receiving) = node
                .execute_with(work)
                .unwrap_or_else(|_| panic!("the restored declaration retries its original work"));
            let ((), outcome) = tokio::join!(execution, receiving);
            let ((original, mut original_file, reply, root_address, shutdown), retirement) =
                outcome
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("the same original work completes"));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(ptr::eq(original.as_ptr(), allocation));
            assert_eq!(&*original, &[41, 43]);
            let mut contents = String::new();
            original_file
                .read_to_string(&mut contents)
                .expect("the original non-Clone file remains usable after retry and retirement");
            assert_eq!(contents, include_str!("../Cargo.toml"));
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
            assert_counter_retirement(root, 1);
            let closed = node.close().await;
            assert!(closed.is_ok());
        })
        .await;
        completed.expect("actor work and joined retirement finish within the watchdog");
    });
}

#[test]
fn node_execution_resumes_original_futures_with_borrowed_caller_local_work() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the native SDK host");
    let calls = &Cell::new(0);
    let original = Rc::new(vec![47, 53]);
    let allocation = Rc::as_ptr(&original);
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("the actual native node opens"));
        let (entered, entry) = oneshot::channel();
        let (release, permission) = oneshot::channel();
        let (observed, outcome) = {
            let (execution, receiving) = node
                .execute_with(|application| async move {
                    calls.set(calls.get() + 1);
                    let published = entered.send(application.root().address());
                    let permitted = permission.await;
                    let current = application.root().address();
                    let shutdown = application.shutdown();
                    (original, calls, published, permitted, current, shutdown)
                })
                .unwrap_or_else(|_| panic!("caller-local paired work is issued"));
            let mut execution = pin!(execution);
            let mut receiving = pin!(receiving);
            let observed = timeout(Duration::from_secs(10), async {
                let borrowing = execution.as_mut();
                let mut borrowing = pin!(borrowing);
                tokio::select! {
                    observed = entry => observed.expect("work entered its actual permission gate"),
                    () = borrowing.as_mut() => panic!("execution cannot finish without permission"),
                }
            })
            .await
            .expect("the execution starts within the watchdog");
            let mut context = Context::from_waker(Waker::noop());
            let receiver_wait = receiving.as_mut().poll(&mut context);
            assert!(matches!(receiver_wait, Poll::Pending));
            let execution_wait = execution.as_mut().poll(&mut context);
            assert!(matches!(execution_wait, Poll::Pending));
            assert_eq!(calls.get(), 1);
            let released = release.send(());
            assert!(released.is_ok());
            let ((), outcome) = timeout(Duration::from_secs(10), async {
                tokio::join!(execution.as_mut(), receiving.as_mut())
            })
            .await
            .expect("both original paired owners finish after permission");
            (observed, outcome)
        };
        let ((original, borrowed, published, permitted, current, shutdown), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original caller-local work completes"));
        assert!(ptr::eq(Rc::as_ptr(&original), allocation));
        assert_eq!(&*original, &[47, 53]);
        assert!(ptr::eq(borrowed, calls));
        assert_eq!(borrowed.get(), 1);
        assert_eq!(published, Ok(()));
        assert_eq!(permitted, Ok(()));
        assert_eq!(current, observed);
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
        assert_eq!(origin.address(), observed);
        assert_counter_retirement(root, 0);
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
        let calls = &Cell::new(0);
        let Err(NodeExecutionRefusal::SessionClosed { work }) = closed_node
            .run_with(|application| async move {
                calls.set(calls.get() + 1);
                let shutdown = application.shutdown();
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
        assert_counter_retirement(
            &retirement.retirement().as_ref().expect("root returns").1,
            0,
        );
        let closed = node.close().await;
        assert!(closed.is_ok());

        let original = vec![67, 71].into_boxed_slice();
        let allocation = original.as_ptr();
        let calls = &Cell::new(0);
        let Err(NodeExecutionRefusal::ExecutionIssued { work }) = node
            .run_with(|application| async move {
                calls.set(calls.get() + 1);
                let shutdown = application.shutdown();
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
        assert_counter_retirement(
            &retirement.retirement().as_ref().expect("root returns").1,
            0,
        );
        let closed = replacement.close().await;
        assert!(closed.is_ok());
    });
}
