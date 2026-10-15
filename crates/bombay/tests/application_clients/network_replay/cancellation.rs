//! Native reply custody across cancellation of a disposable receive operation.
//! Reuses its actual actor, provider, configuration and ownership declarations.
use super::{
    Admission, AdmissionRefusal, Binding, EndpointRole, LedgerCommand, LedgerOperation, MailLedger,
    ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef, ReadRetention, Receipts, Retention,
    StaticExport, VerificationError, assert_completed, configuration, current_scope, metadata,
    recorded, reserved_address, scheduled,
};
use bombay::actors::ActorExt;
use bombay::behavior::BehaviorBase;
use bombay::composition::StashRoute;
use bombay::prelude::Application;
use bombay::remote::{try_retain, verify};
use bombay::{ActorFailureAssessment, RetirementAssessment, TrySendError};
use core::future::{Future, poll_fn};
use core::ops::AsyncFn;
use core::pin::pin;
use core::task::Poll;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, mpsc};

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
            let client = application
                .export_client::<_, Receipts>(target)
                .unwrap_or_else(|_| panic!("actual hosted Ledger target"));
            let reply_to = client.recipient();
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
            let capacity = Arc::new(Semaphore::new(2));
            let (incoming, received) = mpsc::channel(2);
            let publications = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&publications);
            let queryable = session
                .declare_queryable("bombay/cancellation/read")
                .callback(move |query| {
                    let retained = try_retain(&incoming, &capacity, query);
                    observed.lock().unwrap().push(retained);
                })
                .await
                .unwrap();
            let mut export = StaticExport {
                session: session.clone(),
                queryable,
                incoming: received,
                client,
                maximum_entries: NonZeroUsize::new(2).unwrap(),
                awaiting_native: None,
                entries: Vec::new(),
            };
            let retained_a = export.retain(
                request_a.protected,
                request_a.proof,
                receipt_a,
                identity,
                ReadRetention::ReuseFirst,
            );
            let retained_b = export.retain(
                request_b.protected,
                request_b.proof,
                receipt_b,
                identity,
                ReadRetention::ReuseFirst,
            );
            let (Ok(Retention::First { entry: a }), Ok(Retention::First { entry: b })) =
                (&retained_a, &retained_b)
            else {
                panic!("two distinct verified request keys");
            };
            let (a, b) = (*a, *b);
            let deadline = Instant::now() + Duration::from_secs(30);
            // Unit discharges only the temporary row loan; the native result stays in Entry.
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
                        Ok(())
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            let pending_a = {
                let mut receiving = pin!(export.receive_native(a));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            // The whole receive operation is disposed; its export owner remains live.
            let pending_b = export
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
                        Ok(())
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            let untouched_b = {
                let mut receiving = pin!(export.receive_native(b));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            let original = Box::<[u8]>::from([59, 61]);
            let append_allocation = original.as_ptr();
            let appended = sender.send(LedgerCommand::Append(original)).await;
            let fallback_shutdown = if appended.is_err() {
                Some(application.request_shutdown())
            } else {
                None
            };
            let report = application.lifecycle().retirement().await;
            let resumed_a = {
                let mut receiving = pin!(export.receive_native(a));
                poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await
            };
            // Joined actor sends have settled. Close only this reply endpoint and keep
            // any accepted prefix independently, including a reply stranded by the cut.
            export.client.close_admission();
            let mut remainder = Vec::new();
            while let Some(original) = export.client.receive().await {
                remainder.push(original);
            }
            let closed_b = export
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
                        Ok(())
                    },
                    LedgerCommand::Read,
                )
                .map(|_| ());
            (
                export,
                retained_a,
                retained_b,
                a,
                b,
                admitted_a,
                pending_a,
                pending_b,
                untouched_b,
                appended,
                append_allocation,
                sender_origin,
                origin,
                reply_to,
                fallback_shutdown,
                report,
                resumed_a,
                remainder,
                closed_b,
                allocations,
                publications,
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
        a,
        b,
        admitted_a,
        pending_a,
        pending_b,
        untouched_b,
        appended,
        append_allocation,
        sender_origin,
        origin,
        reply_to,
        fallback_shutdown,
        report,
        resumed_a,
        remainder,
        closed_b,
        allocations,
        publications,
    ) = output;
    let StaticExport {
        session,
        queryable,
        incoming,
        client,
        entries,
        ..
    } = export;
    let undeclared = queryable.undeclare().await;
    drop((session, incoming));
    let closed = node.close().await;
    // Intended guard-omission oracle is first, after every original cleanup output.
    assert!(matches!(&pending_b, Err(AdmissionRefusal::NativePending { entry }) if *entry == a));
    assert!(matches!(pending_a, Poll::Pending));
    assert!(matches!(untouched_b, Poll::Ready(())));
    assert!(matches!(resumed_a, Poll::Ready(())));
    assert!(admitted_a.is_ok() && appended.is_ok() && closed_b.is_ok());
    assert!(cleanup.is_ok() && undeclared.is_ok() && closed.is_ok());
    assert!(fallback_shutdown.is_none());
    let notifications = notifications.as_ref().unwrap();
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let (native_origin, retirement) = native.unwrap();
    assert_eq!(native_origin.address(), origin);
    let ledger = retirement.custody().unwrap().behavior.base();
    let [(from, original)] = ledger.entries.as_slice() else {
        panic!("one actual Append");
    };
    assert_eq!(*from, sender_origin);
    assert_eq!(original.as_ptr(), append_allocation);
    assert_eq!(original.as_ref(), [59, 61]);
    let Some(Some(reply)) = &entries[a].native else {
        panic!("A retains its actual User");
    };
    assert_eq!(reply.from, origin);
    assert_eq!(reply.message.as_slice(), [sender_origin]);
    assert!(entries[b].native.is_none());
    assert!(matches!(&entries[a].admission, Admission::Returned(Ok(()))));
    let Admission::Returned(Err(TrySendError::Closed(LedgerCommand::Read(original)))) =
        &entries[b].admission
    else {
        panic!("B retains original closed Read capability");
    };
    assert_eq!(original, &reply_to);
    assert_eq!(remainder.len(), 0);
    for (entry, allocation) in [(&entries[a], allocations[0]), (&entries[b], allocations[1])] {
        assert_eq!(entry.request.as_ptr(), allocation.0);
        assert_eq!(entry.request_proof.as_ptr(), allocation.1);
    }
    assert!(publications.lock().unwrap().is_empty());
    assert_completed((native_origin, retirement), None);
    drop((
        client,
        entries,
        retained_a,
        retained_b,
        remainder,
        publications,
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
        ));
}
