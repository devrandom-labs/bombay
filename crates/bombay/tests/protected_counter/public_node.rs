//! Public Node composition; listening on TLS is not a handshake witness.
use super::stop_admission::{
    CounterChildren, CounterExports, CounterParent, CounterStopExport, counter_children_application,
};
use super::{Counter, CounterMessage, CounterValue, completed_actor};
use bombay::actors::ActorExt;
use bombay::behavior::{Behavior, ClassifySettlement, Here, Ingress, SettlementStatus};
use bombay::prelude::{Application, Completion, StopOnShutdown};
use bombay::remote::{NodeExecutionRefusal, NodeStartupRefusal};
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, ApplicationHandle,
    RetirementAssessment, WorkOutcome,
};
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::cell::Cell;
use std::error::Error;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::{Builder, Handle, TryCurrentError};
use tokio::sync::oneshot;
use zenoh_config::Config;

fn listening_configuration() -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certificates");
    let root = certificates.join("root-ca.pem");
    let certificate = certificates.join("recipient-certificate.pem");
    let key = certificates.join("recipient-private-key.pem");
    let declaration = serde_json::json!({
        "mode": "peer", "queries_max_retained": 2,
        "listen": {"endpoints": ["tls/localhost:0"]},
        "scouting": {"multicast": {"enabled": false, "listen": false}, "gossip": {"enabled": false}},
        "transport": {"link": {"protocols": ["tls"], "tls": {
            "root_ca_certificate": root, "listen_certificate": certificate,
            "listen_private_key": key, "enable_mtls": true,
            "verify_name_on_connect": true, "use_public_pki": false
        }}}
    }).to_string();
    Config::from_deserializer(&mut serde_json::Deserializer::from_str(&declaration))
        .expect("actual fixture PEM configuration")
}

#[test]
fn public_node_root_preserves_borrowed_execution_and_original_second_work() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("actual Node opens"));
        let visits = Cell::new(0_u8);
        let (
            pending,
            observed_entry,
            released,
            published,
            observed_visits,
            permitted,
            admitted,
            reply,
            address,
            stopped,
            cleanup,
            native,
            notifications,
        ) = {
            let (release, permission) = oneshot::channel();
            let (entered, entry) = oneshot::channel();
            let visits = &visits;
            let (execution, receiving) = node
                .execute_with(|application| async move {
                    let published = entered.send(visits.get());
                    let permitted = permission.await;
                    visits.set(visits.get() + 1);
                    let observed_visits = visits.get();
                    let mut client = application.client::<CounterValue>().unwrap();
                    let admitted = client.send(CounterMessage::Read(client.recipient())).await;
                    let reply = if admitted.is_ok() {
                        client.receive().await
                    } else {
                        None
                    };
                    let address = application.root().address();
                    let stopped = application.shutdown();
                    (
                        published,
                        observed_visits,
                        permitted,
                        admitted,
                        reply,
                        address,
                        stopped,
                        client,
                    )
                })
                .unwrap_or_else(|_| panic!("actual borrowed execution is issued"));
            let (pending, observed_entry, released) = {
                let mut execution = pin!(execution);
                let (pending, observed_entry) = {
                    let mut borrowing = pin!(async { execution.as_mut().await });
                    let observed_entry = tokio::select! {
                        entered = entry => Some(entered),
                        () = borrowing.as_mut() => None,
                    };
                    let pending = match observed_entry {
                        Some(_) => borrowing
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop())),
                        None => Poll::Ready(()),
                    };
                    (pending, observed_entry)
                };
                let released = release.send(());
                match pending {
                    Poll::Ready(()) => (),
                    Poll::Pending => execution.await,
                }
                (pending, observed_entry, released)
            };
            let (outcome, native, notifications, ()) = receiving.await.into_parts();
            let WorkOutcome::Completed { output, cleanup } = outcome else {
                panic!("actual Work output");
            };
            let (published, observed_visits, permitted, admitted, reply, address, stopped, client) =
                output;
            drop(client);
            (
                pending,
                observed_entry,
                released,
                published,
                observed_visits,
                permitted,
                admitted,
                reply,
                address,
                stopped,
                cleanup,
                native,
                notifications,
            )
        };
        let original: Arc<[u8]> = Arc::from([13_u8, 17]);
        let original_allocation = Arc::as_ptr(&original);
        let weak = Arc::downgrade(&original);
        let calls = Arc::new(AtomicUsize::new(0));
        let work_calls = Arc::clone(&calls);
        let (release, permission) = oneshot::channel::<()>();
        let (refused, retained, permission_before_disposal) = {
            let attempted = node.execute_with(move |_| {
                work_calls.fetch_add(1, Ordering::SeqCst);
                async move { (original, permission) }
            });
            let retained = weak.upgrade();
            let permission_before_disposal = release.is_closed();
            let refused = match attempted {
                Err(NodeExecutionRefusal::ExecutionIssued { work }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::ExecutionIssued { work: () })
                }
                Err(NodeExecutionRefusal::SessionClosed { work }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::SessionClosed { work: () })
                }
                Err(NodeExecutionRefusal::Host { work, cause }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::Host { work: (), cause })
                }
                Ok(products) => {
                    drop(products);
                    None
                }
            };
            (refused, retained, permission_before_disposal)
        };
        let calls_after_refusal = calls.load(Ordering::SeqCst);
        let retained_allocation = retained.as_ref().map(Arc::as_ptr);
        let retained_bytes = retained.as_deref().map(<[u8]>::to_vec);
        drop(retained);
        let after_disposal = weak.upgrade();
        let permission_after_disposal = release.is_closed();
        let closed = node.close().await;
        assert!(closed.is_ok());
        assert_eq!(
            observed_entry
                .as_ref()
                .and_then(|entry| entry.as_ref().ok())
                .copied(),
            Some(0)
        );
        assert_eq!(observed_visits, 1);
        assert_eq!(visits.get(), 1);
        assert!(matches!(pending, Poll::Pending));
        assert!(released.is_ok() && published.is_ok() && permitted.is_ok() && cleanup.is_ok());
        assert!(admitted.is_ok() && stopped.is_ok());
        assert_eq!(
            reply.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address, 0))
        );
        assert!(matches!(
            refused,
            Some(NodeExecutionRefusal::ExecutionIssued { .. })
        ));
        assert_eq!(calls_after_refusal, 0);
        assert_eq!(retained_allocation, Some(original_allocation));
        assert_eq!(retained_bytes.as_deref(), Some([13_u8, 17].as_slice()));
        assert!(!permission_before_disposal && permission_after_disposal);
        assert!(after_disposal.is_none());
        let (origin, retirement) = native.expect("original native root");
        let notifications = notifications.expect("original independent notifications");
        assert!(matches!(notifications.termination, Ok(())));
        assert!(matches!(notifications.retirement, Ok(())));
        let _retired_actor = completed_actor(origin, retirement);
    });
}

#[test]
fn public_node_child_keeps_installed_target_and_complete_descendant_receipts() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let application = Application::with_topology(
            CounterParent::default().stop_on_shutdown(),
            counter_children_application::Topology::new(),
        );
        let mut node = application
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("child Node opens"));
        let (execution, receiving) = node
            .execute_with(|application| async move {
                let mut exports = application.external::<CounterExports>().unwrap();
                let exported = exports
                    .send(
                        &application.root().established_recipient(),
                        exports.recipient(),
                    )
                    .await;
                let received = exports
                    .receive()
                    .await
                    .expect("actual Counter creator export");
                let CounterChildren { target, sibling } = received.message;
                let target_recipient = target.recipient();
                let mut service = CounterStopExport::default();
                let target_address = target_recipient.clone().interpret(&mut service);
                target.interpret_actor(&mut service);
                let installed = service.installed.take().expect("actual installed target");
                let mut values = application.external::<CounterValue>().unwrap();
                let incremented = values
                    .send(&target_recipient, CounterMessage::Increment)
                    .await;
                let admitted = values
                    .send(&target_recipient, CounterMessage::Read(values.recipient()))
                    .await;
                let reply = if admitted.is_ok() {
                    values.receive().await
                } else {
                    None
                };
                let target_stopped = installed.request_shutdown(Ingress::<_, Here>::new());
                let target_report = installed.retirement().await;
                let sibling_recipient = sibling.recipient();
                let mut sibling_service = CounterStopExport::default();
                let sibling_address = sibling_recipient.clone().interpret(&mut sibling_service);
                sibling.interpret_actor(&mut sibling_service);
                let sibling_installed = sibling_service.installed.take().expect("actual sibling");
                let sibling_incremented = values
                    .send(&sibling_recipient, CounterMessage::Increment)
                    .await;
                let sibling_admitted = values
                    .send(&sibling_recipient, CounterMessage::Read(values.recipient()))
                    .await;
                let sibling_reply = if sibling_admitted.is_ok() {
                    values.receive().await
                } else {
                    None
                };
                let sibling_stopped = sibling_installed.request_shutdown(Ingress::<_, Here>::new());
                let sibling_report = sibling_installed.retirement().await;
                let parent_stopped = application.shutdown();
                (
                    exported,
                    received.from,
                    incremented,
                    admitted,
                    reply,
                    target_address,
                    target_recipient,
                    target_stopped,
                    target_report,
                    sibling_address,
                    sibling_incremented,
                    sibling_admitted,
                    sibling_reply,
                    sibling_stopped,
                    sibling_report,
                    parent_stopped,
                    exports,
                    values,
                    installed,
                    sibling_installed,
                )
            })
            .unwrap_or_else(|_| panic!("existing child topology is issued"));
        execution.await;
        let (outcome, native, notifications, ()) = receiving.await.into_parts();
        let WorkOutcome::Completed { output, cleanup } = outcome else {
            panic!("child Work retained");
        };
        let (
            exported,
            parent_address,
            incremented,
            admitted,
            reply,
            target_address,
            target_recipient,
            target_stopped,
            target_report,
            sibling_address,
            sibling_incremented,
            sibling_admitted,
            sibling_reply,
            sibling_stopped,
            sibling_report,
            parent_stopped,
            exports,
            values,
            installed,
            sibling_installed,
        ) = output;
        drop((
            target_recipient,
            exports,
            values,
            installed,
            sibling_installed,
        ));
        let closed = node.close().await;
        let (origin, native) = native.expect("complete native parent");
        let ActorNotificationReceipts {
            termination,
            retirement,
        } = notifications.expect("both parent notifications");
        assert!(closed.is_ok() && cleanup.is_ok());
        assert!(sibling_incremented.is_ok() && sibling_admitted.is_ok());
        assert_eq!(
            sibling_reply
                .as_ref()
                .map(|reply| (reply.from, reply.message)),
            Some((sibling_address, 1))
        );
        assert_ne!(target_address, sibling_address);
        assert!(exported.is_ok() && incremented.is_ok() && admitted.is_ok());
        assert_eq!(origin.address(), parent_address);
        assert_eq!(
            reply.as_ref().map(|reply| (reply.from, reply.message)),
            Some((target_address, 1))
        );
        assert_eq!(target_stopped, Ok(()));
        assert_eq!(sibling_stopped, Ok(()));
        assert_eq!(parent_stopped, Ok(()));
        assert_eq!(
            target_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            target_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        assert_eq!(
            sibling_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            sibling_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        assert!(matches!(termination, Ok(())));
        assert!(matches!(retirement, Ok(())));
        let ActorRetirement::Completed {
            interpretation,
            source,
            settlements,
            ..
        } = &native
        else {
            panic!("parent retirement must be complete");
        };
        assert!(interpretation.is_none() && source.is_none());
        for settlement in settlements {
            assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
        }
        assert_eq!(native.completion(), Some(&Completion::Stopped));
        let custody = native.custody().expect("actual parent survives");
        assert_eq!(custody.descendants.len(), 2);
        assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
        assert!(custody.control.is_empty() && custody.user.is_empty());
        assert!(
            custody.unread_owner_cancellation.is_none() && custody.additional_failures.is_empty()
        );
        assert!(custody.received_interpretation.is_none() && custody.received_source.is_none());
        assert!(custody.source_index.is_none() && custody.acquired_ingress.is_none());
        assert!(custody.retirement_failures.is_empty() && custody.terminal_report.is_none());
        let mut child_addresses = custody
            .descendants
            .iter()
            .map(|child| child.origin.address())
            .collect::<Vec<_>>();
        let mut expected_addresses = vec![target_address, sibling_address];
        child_addresses.sort();
        expected_addresses.sort();
        assert_eq!(child_addresses, expected_addresses);
        for child in &custody.descendants {
            let ActorRetirement::Completed {
                interpretation,
                source,
                settlements,
                ..
            } = child.retirement.as_ref()
            else {
                panic!("child retirement must be complete");
            };
            assert!(interpretation.is_none() && source.is_none());
            for settlement in settlements {
                assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
            }
            assert_eq!(child.retirement.completion(), Some(&Completion::Stopped));
            let child_custody = child.retirement.custody().expect("actual child survives");
            assert!(child_custody.control.is_empty() && child_custody.user.is_empty());
            assert!(child_custody.unread_owner_cancellation.is_none());
            assert!(
                child_custody.received_interpretation.is_none()
                    && child_custody.received_source.is_none()
            );
            assert!(
                child_custody.source_index.is_none() && child_custody.acquired_ingress.is_none()
            );
            assert!(
                child_custody.retirement_failures.is_empty()
                    && child_custody.terminal_report.is_none()
            );
            assert!(
                child_custody.descendants.is_empty() && child_custody.child_failures.is_empty()
            );
            assert!(
                child_custody.capability_failures.is_empty()
                    && child_custody.additional_failures.is_empty()
            );
        }
    });
}

#[test]
fn public_node_native_open_refusal_preserves_usable_application_and_original_configuration() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let mut config = listening_configuration();
        let missing = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("certificates/absent-node-certificate.pem");
        let absent =
            fs::metadata(&missing).expect_err("isolated missing PEM fixture remains absent");
        let encoded = serde_json::to_string(&missing).expect("native file declaration");
        for (field, value) in [
            ("listen/timeout_ms", "0"),
            ("listen/exit_on_failure", "true"),
            ("transport/link/tls/listen_certificate", encoded.as_str()),
        ] {
            let changed = config.insert_json5(field, value);
            changed.expect("fixture-only immediate failure policy");
        }
        let original = config
            .transport()
            .link()
            .tls()
            .listen_certificate()
            .as_ref()
            .expect("supplied file")
            .as_ptr();
        let refused = Application::new(Counter::new().stop_on_shutdown())
            .open_node(config)
            .await;
        let Err((application, config, refusal)) = refused else {
            panic!("actual native PEM opening must fail");
        };
        let retained = config
            .transport()
            .link()
            .tls()
            .listen_certificate()
            .as_ref()
            .expect("original file retained")
            .as_ptr();
        let source = refusal
            .source()
            .expect("actual SDK cause remains inspectable");
        let NodeStartupRefusal::Open { cause } = &refusal else {
            panic!("actual SDK open failed after profile and host checks");
        };
        let native_source: &(dyn Error + 'static) = cause.as_ref();
        let exact_source = ptr::addr_eq(source, native_source);
        let (execution, receiving) = application
            .execute_with(|application| async move {
                let mut client = application.client::<CounterValue>().unwrap();
                let admitted = client.send(CounterMessage::Read(client.recipient())).await;
                let reply = if admitted.is_ok() {
                    client.receive().await
                } else {
                    None
                };
                let address = application.root().address();
                let stopped = application.shutdown();
                (admitted, reply, address, stopped, client)
            })
            .unwrap_or_else(|_| panic!("original local declaration remains usable"));
        execution.await;
        let (outcome, native, notifications, ()) = receiving.await.into_parts();
        let WorkOutcome::Completed {
            output: (admitted, reply, address, stopped, client),
            cleanup,
        } = outcome
        else {
            panic!("original local Work result");
        };
        let (origin, retirement) = native.expect("original native root");
        let notifications = notifications.expect("both original notifications");
        assert_eq!(absent.kind(), ErrorKind::NotFound);
        assert_eq!(retained, original);
        assert!(exact_source && cleanup.is_ok() && admitted.is_ok() && stopped.is_ok());
        assert_eq!(
            reply.as_ref().map(|reply| (reply.from, reply.message)),
            Some((address, 0))
        );
        assert!(matches!(&notifications.termination, Ok(())));
        assert!(matches!(&notifications.retirement, Ok(())));
        let behavior = completed_actor(origin, retirement);
        drop((behavior, client, config, refusal, notifications));
    });
}
#[test]
fn public_node_closed_session_preserves_original_move_only_work() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    host.block_on(async {
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_configuration())
            .await
            .unwrap_or_else(|_| panic!("actual Node opens"));
        let closed = node.close().await;
        let original: Arc<[u8]> = Arc::from([31_u8, 37]);
        let allocation = Arc::as_ptr(&original);
        let weak = Arc::downgrade(&original);
        let calls = Arc::new(AtomicUsize::new(0));
        let work_calls = Arc::clone(&calls);
        let (release, permission) = oneshot::channel::<()>();
        let (refused, retained, permission_before_disposal) = {
            let attempted = node.execute_with(move |application| {
                work_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    let permitted = permission.await;
                    let stopped = application.shutdown();
                    (permitted, stopped, original)
                }
            });
            let retained = weak.upgrade();
            let permission_before_disposal = release.is_closed();
            let refused = match attempted {
                Err(NodeExecutionRefusal::SessionClosed { work }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::SessionClosed { work: () })
                }
                Err(NodeExecutionRefusal::ExecutionIssued { work }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::ExecutionIssued { work: () })
                }
                Err(NodeExecutionRefusal::Host { work, cause }) => {
                    drop(work);
                    Some(NodeExecutionRefusal::Host { work: (), cause })
                }
                Ok(products) => {
                    drop(products);
                    None
                }
            };
            (refused, retained, permission_before_disposal)
        };
        let entries = calls.load(Ordering::SeqCst);
        let retained_allocation = retained.as_ref().map(Arc::as_ptr);
        let retained_bytes = retained.as_deref().map(<[u8]>::to_vec);
        drop(retained);
        let after_disposal = weak.upgrade();
        let permission_after_disposal = release.is_closed();
        assert!(closed.is_ok());
        assert!(matches!(
            &refused,
            Some(NodeExecutionRefusal::SessionClosed { .. })
        ));
        assert_eq!(entries, 0);
        assert_eq!(retained_allocation, Some(allocation));
        assert_eq!(retained_bytes.as_deref(), Some([31_u8, 37].as_slice()));
        assert!(!permission_before_disposal && permission_after_disposal);
        assert!(after_disposal.is_none());
    });
}
#[test]
fn public_node_missing_host_restores_the_declaration_for_original_work_retry() {
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let mut node = host
        .block_on(
            Application::new(Counter::new().stop_on_shutdown())
                .open_node(listening_configuration()),
        )
        .unwrap_or_else(|_| panic!("actual Node opens"));
    let original: Arc<[u8]> = Arc::from([41_u8, 43]);
    let allocation = Arc::as_ptr(&original);
    let weak = Arc::downgrade(&original);
    let calls = Arc::new(AtomicUsize::new(0));
    let work_calls = Arc::clone(&calls);
    let (release, permission) = oneshot::channel::<()>();
    let work = move |application: ApplicationHandle<
        Counter,
        <StopOnShutdown<Counter> as Behavior>::Event,
    >| {
        work_calls.fetch_add(1, Ordering::SeqCst);
        async move {
            let permitted = permission.await;
            let stopped = application.shutdown();
            (permitted, stopped, original)
        }
    };
    let (work, host_cause) = {
        let refused = node.execute_with(work);
        match refused {
            Err(NodeExecutionRefusal::Host { work, cause }) => (work, cause),
            Err(other) => {
                drop(other);
                panic!("actual absent-host classification");
            }
            Ok(products) => {
                drop(products);
                panic!("no entered host may issue execution");
            }
        }
    };
    let entries_after_refusal = calls.load(Ordering::SeqCst);
    let retained = weak.upgrade().expect("host refusal retains original Work");
    let retained_allocation = Arc::as_ptr(&retained);
    drop(retained);
    host.block_on(async {
        let released = release.send(());
        let (permitted, stopped, original, cleanup, native, notifications) = {
            let (execution, receiving) = node
                .execute_with(work)
                .unwrap_or_else(|_| panic!("actual restored declaration retries once"));
            execution.await;
            let (outcome, native, notifications, ()) = receiving.await.into_parts();
            let WorkOutcome::Completed {
                output: (permitted, stopped, original),
                cleanup,
            } = outcome
            else {
                panic!("same original Work executes after retry");
            };
            (permitted, stopped, original, cleanup, native, notifications)
        };
        let closed = node.close().await;
        let (origin, retirement) = native.expect("original native root after retry");
        let notifications = notifications.expect("both original notifications after retry");
        let entries_after_retry = calls.load(Ordering::SeqCst);
        let output_allocation = Arc::as_ptr(&original);
        assert!(
            host_cause.is_missing_context()
                && released.is_ok()
                && permitted.is_ok()
                && stopped.is_ok()
                && cleanup.is_ok()
                && closed.is_ok()
        );
        assert_eq!(entries_after_refusal, 0);
        assert_eq!(entries_after_retry, 1);
        assert_eq!(retained_allocation, allocation);
        assert_eq!(output_allocation, allocation);
        assert_eq!(original.as_ref(), [41_u8, 43]);
        assert!(matches!(&notifications.termination, Ok(())));
        assert!(matches!(&notifications.retirement, Ok(())));
        let behavior = completed_actor(origin, retirement);
        drop((behavior, original, notifications, host_cause));
    });
}

#[test]
fn public_node_absent_startup_host_preserves_original_inputs_and_usable_application() {
    let application = Application::new(Counter::new().stop_on_shutdown());
    let config = listening_configuration();
    let original_credential = config
        .transport()
        .link()
        .tls()
        .listen_certificate()
        .as_ref()
        .expect("actual declared certificate")
        .as_ptr();
    let original_configuration =
        serde_json::to_string(&config).expect("complete original public configuration view");
    let absent_host = Handle::try_current().expect_err("this ordinary test has no entered host");
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (first_poll, opened) = {
        let mut opening = pin!(application.open_node(config));
        let first = opening
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        match first {
            Poll::Ready(opened) => (Poll::Ready(()), opened),
            Poll::Pending => {
                let opened = host.block_on(opening.as_mut());
                (Poll::Pending, opened)
            }
        }
    };
    let (application, config, refusal) = match opened {
        Err(inputs) => inputs,
        Ok(mut node) => {
            let _closed = host.block_on(node.close());
            drop(node);
            panic!("unexpected native Session acquisition after acquiring close result");
        }
    };
    let retained_credential = config
        .transport()
        .link()
        .tls()
        .listen_certificate()
        .as_ref()
        .expect("actual original certificate retained")
        .as_ptr();
    let retained_configuration =
        serde_json::to_string(&config).expect("complete retained public configuration view");
    let actual_source = refusal
        .source()
        .and_then(|source| source.downcast_ref::<TryCurrentError>());
    let expected_source = match &refusal {
        NodeStartupRefusal::Host { cause } => Some(cause),
        _ => None,
    };
    let exact_source = match (actual_source, expected_source) {
        (Some(actual), Some(expected)) => ptr::eq(actual, expected),
        _ => false,
    };
    let (outcome, native, notifications, ()) = host.block_on(async {
        let (execution, receiving) = application
            .execute_with(|application| async move {
                let mut client = application.client::<CounterValue>().unwrap();
                let admitted = client.send(CounterMessage::Read(client.recipient())).await;
                let reply = if admitted.is_ok() {
                    client.receive().await
                } else {
                    None
                };
                let address = application.root().address();
                let stopped = application.shutdown();
                (admitted, reply, address, stopped, client)
            })
            .unwrap_or_else(|_| panic!("returned original local Application is usable"));
        execution.await;
        receiving.await.into_parts()
    });
    let WorkOutcome::Completed {
        output: (admitted, reply, address, stopped, client),
        cleanup,
    } = outcome
    else {
        panic!("original local Work output survives startup refusal");
    };
    let (origin, retirement) = native.expect("actual returned Application's complete native root");
    let notifications = notifications.expect("both actual original notifications");
    assert!(cleanup.is_ok() && admitted.is_ok() && stopped.is_ok());
    assert!(matches!(&notifications.termination, Ok(())));
    assert!(matches!(&notifications.retirement, Ok(())));
    let behavior = completed_actor(origin, retirement);
    assert_eq!(
        reply.as_ref().map(|reply| (reply.from, reply.message)),
        Some((address, 0))
    );
    assert!(matches!(first_poll, Poll::Ready(())));
    assert!(absent_host.is_missing_context());
    assert!(matches!(&refusal, NodeStartupRefusal::Host { cause } if cause.is_missing_context()));
    assert!(exact_source);
    assert_eq!(retained_credential, original_credential);
    assert_eq!(retained_configuration, original_configuration);
    drop((
        behavior,
        client,
        config,
        refusal,
        notifications,
        absent_host,
    ));
}

#[path = "hosting.rs"]
mod hosting;
