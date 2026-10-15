//! Native reply custody across cancellation of a disposable receive operation.
//! Reuses its actual actor, provider, configuration and ownership declarations.
use super::{
    Admission, Binding, EndpointRole, LedgerCommand, LedgerOperation, MailLedger, ProtectedMessage,
    ProviderProfile, Purpose, ReadRequestRef, Receipts, VerificationError, assert_completed,
    configuration, current_scope, metadata, recorded, reserved_address, same_recorded_ledger,
    same_scheduled_ledger, scheduled,
};
use bombay::actors::ActorExt;
use bombay::behavior::BehaviorBase;
use bombay::composition::{StashRoute, StashStatus};
use bombay::prelude::Application;
use bombay::remote::{ExportLimits, IngressError, verify};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use core::future::{Future, poll_fn};
use core::mem;
use core::ops::AsyncFn;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Wake;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::task::unconstrained;
use zenoh::query::Query;
use zenoh::sample::Locality;

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keep original native custody and the complete post-cleanup cancellation trace together"
)]
async fn cancelled_ledger_read<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &super::ProofScope<LedgerOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let root = MailLedger {
        entries: Vec::new(),
    }
    .with_stash(|ledger, command| match command {
        LedgerCommand::Read(_) if ledger.entries.is_empty() => StashRoute::Stash,
        LedgerCommand::Append(_) => StashRoute::Release,
        LedgerCommand::Read(_) => StashRoute::Deliver,
    })
    .stop_on_shutdown();
    let address = reserved_address();
    let mut node = Application::new(root)
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("configured local export Session"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let target = application.root().established_recipient();
            let origin = application.root().address();
            let publications = Arc::new(Mutex::new(Vec::<IngressError<Query>>::new()));
            let observed = Arc::clone(&publications);
            let mut export = application
                .export::<_, Receipts, Receipt>(
                    target,
                    "bombay/cancellation/read",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| observed.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual Node-associated Ledger export"));
            let sender = application.sender().unwrap();
            let sender_origin = sender.address();
            let binding = Binding {
                deployment: "two-node-read-study",
                caller: "caller",
                controller: "owner",
                actor: "ledger",
                host: "receiver",
                protocol: "ledger",
                target_assignment: "held-ledger-root",
                logical_session: 37,
            };
            let current_a = metadata(
                profile,
                "ledger",
                session.zid().to_string(),
                session.zid().to_string(),
            );
            let mut current_b = metadata(
                profile,
                "ledger",
                session.zid().to_string(),
                session.zid().to_string(),
            );
            current_b.request_sequence = current_a
                .request_sequence
                .checked_add(1)
                .expect("configured second test sequence");
            let bytes_a = serde_json::to_vec(&ReadRequestRef {
                bombay: &current_a,
                kind: Purpose::Request,
                operation: LedgerOperation::Read,
            })
            .unwrap();
            let bytes_b = serde_json::to_vec(&ReadRequestRef {
                bombay: &current_b,
                kind: Purpose::Request,
                operation: LedgerOperation::Read,
            })
            .unwrap();
            let records = vec![
                ProtectedMessage {
                    protected: bytes_a.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
                ProtectedMessage {
                    protected: bytes_b.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            ];
            let request_a = ProtectedMessage {
                protected: bytes_a.into_boxed_slice(),
                proof: Box::from([7, 11]),
            };
            let request_b = ProtectedMessage {
                protected: bytes_b.into_boxed_slice(),
                proof: Box::from([7, 11]),
            };
            let allocations = [
                (request_a.protected.as_ptr(), request_a.proof.as_ptr()),
                (request_b.protected.as_ptr(), request_b.proof.as_ptr()),
            ];
            let mut verified_a = None;
            let mut verified_b = None;
            verify(&request_a, &mut verified_a, |r| {
                provider(r, &records, binding)
            })
            .await;
            verify(&request_b, &mut verified_b, |r| {
                provider(r, &records, binding)
            })
            .await;
            let receipt_a = verified_a.take().unwrap().unwrap();
            let receipt_b = verified_b.take().unwrap().unwrap();
            let retained_a = export.retain(request_a.protected, request_a.proof, receipt_a);
            let retained_b = export.retain(request_b.protected, request_b.proof, receipt_b);
            let (Ok((a, None)), Ok((b, None))) = (&retained_a, &retained_b) else {
                panic!("two distinct verified request keys");
            };
            let deadline = Instant::now() + Duration::from_secs(30);
            // Unit discharges the returned Admission borrow; its native result stays in Export.
            let admitted_a = export
                .attempt(
                    a,
                    |receipt| {
                        current_scope(
                            projection(receipt),
                            &current_a,
                            &LedgerOperation::Read,
                            Purpose::Request,
                            deadline,
                        )?;
                        if identity(receipt).0 != &binding {
                            return Err(VerificationError::Invalid);
                        }
                        Ok(&current_a)
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            let pending_a = {
                let mut receiving = pin!(export.receive_native(a));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            // The whole receive operation is disposed; its export owner remains live.
            let admitted_b = export
                .attempt(
                    b,
                    |receipt| {
                        current_scope(
                            projection(receipt),
                            &current_b,
                            &LedgerOperation::Read,
                            Purpose::Request,
                            deadline,
                        )?;
                        if identity(receipt).0 != &binding {
                            return Err(VerificationError::Invalid);
                        }
                        Ok(&current_b)
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            let pending_b = {
                let mut receiving = pin!(export.receive_native(b));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
            let waker = Waker::from(Arc::clone(&wakes));
            let mut query = None;
            let mut completed = None;
            let (pending_inputs, issued_query, query_ready) = {
                let mut receiving = pin!(unconstrained(export.receive(&mut query, &mut completed)));
                let pending = receiving.as_mut().poll(&mut Context::from_waker(&waker));
                // The same Session runs its local callback before get returns.
                let issued = session
                    .get("bombay/cancellation/read")
                    .allowed_destination(Locality::SessionLocal)
                    .payload(records[0].protected.as_ref())
                    .attachment(records[0].proof.as_ref())
                    .await;
                let ready = receiving.as_mut().poll(&mut Context::from_waker(&waker));
                (pending, issued, ready)
            };
            let received_query = query.take();
            let query_completion = completed.take();
            let original = Box::<[u8]>::from([59, 61]);
            let append_allocation = original.as_ptr();
            let (pending_native, appended, fallback_shutdown, report, before, after, native_ready) = {
                let mut receiving = pin!(unconstrained(export.receive(&mut query, &mut completed)));
                let pending = receiving.as_mut().poll(&mut Context::from_waker(&waker));
                // Exclude the collection's initial self-wake from the producer oracle.
                let before = wakes.0.load(Ordering::SeqCst);
                let appended = sender.send(LedgerCommand::Append(original)).await;
                let fallback = if appended.is_err() {
                    Some(application.request_shutdown())
                } else {
                    None
                };
                let report = application.lifecycle().retirement().await;
                let after = wakes.0.load(Ordering::SeqCst);
                let ready = receiving.as_mut().poll(&mut Context::from_waker(&waker));
                (pending, appended, fallback, report, before, after, ready)
            };
            let resumed_a = {
                let mut receiving = pin!(export.receive_native(a));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            // Joining A's stopping replay leaves B's actual command inside the retired Stash.
            // Rechecking B must preserve its first accepted attempt, not send it again.
            let replayed_b = export
                .attempt(
                    b,
                    |receipt| {
                        current_scope(
                            projection(receipt),
                            &current_b,
                            &LedgerOperation::Read,
                            Purpose::Request,
                            deadline,
                        )?;
                        if identity(receipt).0 != &binding {
                            return Err(VerificationError::Invalid);
                        }
                        Ok(&current_b)
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            let retired_b = {
                let mut receiving = pin!(export.receive_native(b));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            (
                export,
                retained_a,
                retained_b,
                admitted_a,
                pending_a,
                admitted_b,
                pending_b,
                appended,
                append_allocation,
                sender_origin,
                origin,
                fallback_shutdown,
                report,
                resumed_a,
                replayed_b,
                retired_b,
                allocations,
                publications,
                pending_inputs,
                issued_query,
                query_ready,
                received_query,
                query_completion,
                pending_native,
                before,
                after,
                native_ready,
                completed,
                query,
            )
        })
        .unwrap_or_else(|_| panic!("actual local host"));
    let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Work"));
    let (
        export,
        retained_a,
        retained_b,
        admitted_a,
        pending_a,
        admitted_b,
        pending_b,
        appended,
        append_allocation,
        sender_origin,
        origin,
        fallback_shutdown,
        report,
        resumed_a,
        replayed_b,
        retired_b,
        allocations,
        publications,
        pending_inputs,
        issued_query,
        query_ready,
        received_query,
        query_completion,
        pending_native,
        before,
        after,
        native_ready,
        completed,
        query,
    ) = output;
    let (undeclared, session, mut incoming, retained) = export.undeclare().await;
    let mut requests = Vec::new();
    for (key, protected, proof, receipt, admission, native, mut client) in retained {
        client.close_admission();
        let mut tail = Vec::new();
        while let Some(original) = client.receive().await {
            tail.push(original);
        }
        requests.push((
            key, protected, proof, receipt, admission, native, client, tail,
        ));
    }
    let mut query_drain = Vec::new();
    let query_end = loop {
        match incoming.try_recv() {
            Ok(original) => query_drain.push(original),
            Err(cause) => break cause,
        }
    };
    let query_observation = received_query.as_ref().map(|received| {
        received.as_ref().map(|(query, _)| {
            (
                query.key_expr().as_str().to_owned(),
                query.payload().map(|bytes| bytes.to_bytes().into_owned()),
                query
                    .attachment()
                    .map(|bytes| bytes.to_bytes().into_owned()),
            )
        })
    });
    let unreceived_queries = query_drain.len();
    let refused_queries = mem::take(&mut *publications.lock().unwrap());
    let refusal_count = refused_queries.len();
    // This readiness-only query has no reply policy; surrender its complete owners.
    drop((received_query, query, query_drain, refused_queries));
    let query_reply = issued_query.as_ref().ok().map(|replies| replies.try_recv());
    drop(session);
    let closed = node.close().await;
    // Observe both independent admissions and A cancellation only after full cleanup.
    let (Ok((a, None)), Ok((b, None))) = (&retained_a, &retained_b) else {
        panic!("two independent actual issued keys");
    };
    assert!(matches!(pending_inputs, Poll::Pending));
    assert!(issued_query.is_ok());
    assert!(matches!(query_ready, Poll::Ready(())));
    assert!(query_completion.is_none());
    assert!(matches!(pending_native, Poll::Pending));
    assert!(
        after > before,
        "the actual native producer must wake its retained wait"
    );
    assert!(matches!(native_ready, Poll::Ready(())));
    assert_eq!(completed.as_ref(), Some(a));
    assert!(admitted_b.is_ok());
    assert!(matches!(pending_a, Poll::Pending));
    assert!(matches!(pending_b, Poll::Pending));
    assert!(matches!(retired_b, Poll::Pending));
    assert!(matches!(resumed_a, Poll::Ready(Some(()))));
    assert!(admitted_a.is_ok() && appended.is_ok() && replayed_b.is_ok());
    assert!(cleanup.is_ok() && undeclared.is_ok() && closed.is_ok());
    assert!(fallback_shutdown.is_none());
    assert_eq!(unreceived_queries, 0);
    assert!(matches!(query_end, mpsc::error::TryRecvError::Disconnected));
    let notifications = notifications.as_ref().unwrap();
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let (native_origin, retirement) = native.unwrap();
    assert_eq!(native_origin.address(), origin);
    let behavior = &retirement.custody().unwrap().behavior;
    let held = behavior.stashed_messages();
    let ledger = behavior.base();
    let [(from, original)] = ledger.entries.as_slice() else {
        panic!("one actual Append");
    };
    assert_eq!(*from, sender_origin);
    assert_eq!(original.as_ptr(), append_allocation);
    assert_eq!(original.as_ref(), [59, 61]);
    let first = requests.iter().find(|entry| &entry.0 == a).unwrap();
    let second = requests.iter().find(|entry| &entry.0 == b).unwrap();
    let Some(Some((query_key, query_payload, query_proof))) = &query_observation else {
        panic!("the actual local query and permit were acquired");
    };
    assert_eq!(query_key, "bombay/cancellation/read");
    assert_eq!(query_payload.as_deref(), Some(first.1.as_ref()));
    assert_eq!(query_proof.as_deref(), Some(first.2.as_ref()));
    let Some(Some(reply)) = &first.5 else {
        panic!("A retains its actual User");
    };
    assert_eq!(reply.from, origin);
    assert_eq!(reply.message.as_slice(), [sender_origin]);
    assert!(second.5.is_none());
    assert!(matches!(&first.4, Admission::Returned(Ok(()))));
    assert!(matches!(&second.4, Admission::Returned(Ok(()))));
    let reply_a = first.6.recipient();
    let reply_b = second.6.recipient();
    assert_ne!(reply_a, reply_b);
    assert_eq!(
        held, 1,
        "the actual retired Stash retains the unprocessed B command"
    );
    for row in &requests {
        assert_eq!(row.7.len(), 0);
    }
    for (entry, allocation) in [(first, allocations[0]), (second, allocations[1])] {
        assert_eq!(entry.1.as_ptr(), allocation.0);
        assert_eq!(entry.2.as_ptr(), allocation.1);
    }
    assert_eq!(refusal_count, 0);
    assert_completed((native_origin, retirement), None);
    drop((
        requests,
        retained_a,
        retained_b,
        publications,
        issued_query,
        query_reply,
    ));
}

#[test]
fn recorded_cancelled_read_keeps_original_request_owner() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(cancelled_ledger_read(
            ProviderProfile::Recorded,
            recorded::verify::<LedgerOperation>,
            recorded::projection,
            recorded::identity,
            same_recorded_ledger,
        ));
}
#[test]
fn scheduled_cancelled_read_keeps_original_request_owner() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(cancelled_ledger_read(
            ProviderProfile::Scheduled,
            scheduled::verify::<LedgerOperation>,
            scheduled::projection,
            scheduled::identity,
            same_scheduled_ledger,
        ));
}
