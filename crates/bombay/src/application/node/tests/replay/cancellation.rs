//! Cold Stash requests retain native replies across cancelled wire waits.
//! Direct native cancellation and wakeups belong to the owning Export tests.
use super::{
    Binding, EndpointRole, LedgerCommand, LedgerOperation, MailLedger, ProofScope,
    ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef, ReadRequest,
    ReadRequestRef, VerificationError, assert_completed, assert_notifications, assert_report,
    configuration, current_scope, limits, metadata, recorded, reserved_address,
    same_recorded_ledger, same_scheduled_ledger, scheduled,
};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Never, User};
use bombay::composition::{StashRoute, StashStatus, UnlimitedStash};
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Attempt, Export, ExportLimits, reply, verify};
use core::future::{Future, poll_fn};
use core::ops::AsyncFn;
use core::pin::pin;
use core::task::Poll;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

fn ledger_claim_allocations(scope: &ProofScope<LedgerOperation>) -> [*const u8; 10] {
    let claims = &scope.bombay;
    [
        claims.provider_profile.as_ptr(),
        claims.deployment.as_ptr(),
        claims.caller.as_ptr(),
        claims.caller_session.as_ptr(),
        claims.controller.as_ptr(),
        claims.actor.as_ptr(),
        claims.host.as_ptr(),
        claims.protocol.as_ptr(),
        claims.grant_issuer.as_ptr(),
        claims.runtime_generation.as_ptr(),
    ]
}

#[expect(
    clippy::too_many_lines,
    reason = "retain the complete cold Stash, wire cancellation, native originals and joined retirement trace"
)]
async fn stashed_ledger_reads<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<LedgerOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    proof: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let root = UnlimitedStash::new(
        MailLedger {
            entries: Vec::new(),
        },
        |ledger, command| match command {
            LedgerCommand::Read(_) if ledger.entries.is_empty() => StashRoute::Stash,
            LedgerCommand::Append(_) => StashRoute::Release,
            LedgerCommand::Read(_) => StashRoute::Deliver,
        },
    )
    .stop_on_shutdown();
    let address = reserved_address();
    let mut network = configuration(&address, &EndpointRole::Receiver);
    network.max_retained_queries = NonZeroUsize::new(3).unwrap();
    let node = Application::new(root)
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the actual cold Ledger Node opens"));
    let connection = node.session.clone();
    let runtime = node.runtime_id();
    let first = metadata(profile, "ledger", runtime.clone(), runtime.clone());
    let mut second = metadata(profile, "ledger", runtime.clone(), runtime.clone());
    second.request_sequence = first.request_sequence.checked_add(1).unwrap();
    let claims = [first, second];
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
    let requests = claims.each_ref().map(|claims| ProtectedMessage {
        protected: serde_json::to_vec(&ReadRequestRef {
            bombay: claims,
            kind: Purpose::Request,
            operation: LedgerOperation::Read,
        })
        .unwrap()
        .into_boxed_slice(),
        proof: Box::from([7, 11]),
    });
    let records = RefCell::new(
        requests
            .iter()
            .map(|request| ProtectedMessage {
                protected: request.protected.clone(),
                proof: request.proof.clone(),
            })
            .collect::<Vec<_>>(),
    );
    let allocations = RefCell::new(Vec::new());
    let receipts = RefCell::new(Vec::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = RefCell::new(Vec::new());
    let protected = Cell::new(0);
    let (announced, mut constructions) = mpsc::unbounded_channel();
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &Receipt, actual_runtime: &str, target: MailAddr| {
        let scope = projection(receipt);
        let expected = claims
            .iter()
            .find(|claims| claims.request_sequence == scope.bombay.request_sequence)
            .ok_or(VerificationError::Invalid)?;
        current_scope(
            scope,
            expected,
            &LedgerOperation::Read,
            Purpose::Request,
            deadline,
        )?;
        if actual_runtime != runtime
            || target.0 != expected.actor_generation
            || identity(receipt).0 != &binding
        {
            return Err(VerificationError::Invalid);
        }
        Ok(())
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/cancellation/read",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: NonZeroUsize::new(3).unwrap(),
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            serde_json::from_slice::<ReadRequest<LedgerOperation>>(bytes)
                .map(|request| request.operation)
        },
        reply(
            |LedgerOperation::Read| {
                let constructed = &constructed;
                let announced = &announced;
                Ok::<_, LedgerOperation>(move |reply_to| {
                    constructed.set(constructed.get() + 1);
                    let announcement = announced.send(());
                    announcement.expect("the real Work still awaits construction");
                    LedgerCommand::Read(reply_to)
                })
            },
            |receipt: &Receipt, original: &User<MailAddr, Vec<MailAddr>>| {
                encoded
                    .borrow_mut()
                    .push((original.from, original.message.clone()));
                let scope = projection(receipt);
                let value = original
                    .message
                    .iter()
                    .copied()
                    .map(u64::from)
                    .collect::<Vec<_>>();
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scope.bombay,
                    kind: Purpose::ReadReply,
                    operation: scope.operation,
                    value: &value,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], evidence: &[u8]| {
            allocations
                .borrow_mut()
                .push((bytes.as_ptr(), evidence.as_ptr()));
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: evidence.into(),
            };
            let receipt = provider(&original, &records.borrow(), binding).await;
            if let Ok(receipt) = &receipt {
                receipts.borrow_mut().push((
                    ledger_claim_allocations(projection(receipt)),
                    proof(receipt).map(<[u8]>::as_ptr),
                ));
            }
            receipt
        },
        async |bytes: &[u8], _: &Receipt| {
            protected.set(protected.get() + 1);
            records.borrow_mut().push(ProtectedMessage {
                protected: bytes.into(),
                proof: Box::from([13, 17]),
            });
            Ok::<_, Never>(Box::<[u8]>::from([13, 17]))
        },
        same_request,
        &current,
        &current,
    );
    let mut node = node.exports(export, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| {
            let connection = &connection;
            let requests = &requests;
            let encoded = &encoded;
            let provider = &provider;
            let records = &records;
            async move {
                let sender = application.sender().unwrap();
                let sender_origin = sender.address();
                let mut wire = Vec::new();
                let mut pending = Vec::new();
                let mut announcements = Vec::new();
                let mut encodings = Vec::new();
                // A real second Query must progress while A has no native User.
                // These are constructor announcements, not admission certificates.
                for request in requests {
                    let receiving = connection
                        .get("bombay/cancellation/read")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(5))
                        .payload(request.protected.to_vec())
                        .attachment(request.proof.to_vec())
                        .await
                        .unwrap();
                    announcements.push(constructions.recv().await);
                    encodings.push(encoded.borrow().len());
                    let observation = {
                        let mut wait = pin!(receiving.recv_async());
                        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx))).await
                    };
                    // Cancel only the borrowed wire wait; retain the real wire receiver.
                    pending.push(observation);
                    wire.push(receiving);
                }
                let original = Box::<[u8]>::from([59, 61]);
                let append_allocation = original.as_ptr();
                let appended = sender.send(LedgerCommand::Append(original)).await;
                let retirement = application.lifecycle().retirement().await;
                let mut replies = Vec::new();
                for receiving in wire {
                    let mut originals = Vec::new();
                    let end = loop {
                        match receiving.recv_async().await {
                            Ok(original) => originals.push(original),
                            Err(cause) => break cause,
                        }
                    };
                    replies.push((originals, end));
                }
                // B's first native EOF and actual target retirement precede this replay.
                let receiving = connection
                    .get("bombay/cancellation/read")
                    .allowed_destination(Locality::SessionLocal)
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(5))
                    .payload(requests[1].protected.to_vec())
                    .attachment(requests[1].proof.to_vec())
                    .await
                    .unwrap();
                let mut originals = Vec::new();
                let end = loop {
                    match receiving.recv_async().await {
                        Ok(original) => originals.push(original),
                        Err(cause) => break cause,
                    }
                };
                replies.push((originals, end));
                let mut reply_original = None;
                let mut reply_verification = None;
                if let Some(original) = replies[0].0.first()
                    && let Ok(sample) = original.result()
                {
                    reply_original = Some(ProtectedMessage {
                        protected: sample.payload().to_bytes().into_owned().into_boxed_slice(),
                        proof: sample
                            .attachment()
                            .unwrap()
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice(),
                    });
                    let records = records.borrow();
                    verify(
                        reply_original.as_ref().unwrap(),
                        &mut reply_verification,
                        |original| provider(original, &records, binding),
                    )
                    .await;
                }
                (
                    announcements,
                    encodings,
                    pending,
                    replies,
                    appended,
                    sender_origin,
                    append_allocation,
                    retirement,
                    reply_original,
                    reply_verification,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual cold Ledger host starts"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the original cold Node finalizes"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            announcements,
            encodings,
            pending,
            replies,
            appended,
            sender_origin,
            append_allocation,
            retirement,
            reply_original,
            reply_verification,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete Ledger Work remains"));
    let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
    let rows = rows
        .into_iter()
        .map(|row| {
            row.try_into_parts()
                .unwrap_or_else(|_| panic!("every producer returns its original request loans"))
        })
        .collect::<Vec<_>>();
    // Every law assertion follows actual withdrawal, close, reports and native ownership.
    assert!(cleanup.is_ok() && closed.is_ok());
    assert!(connection.is_closed());
    assert_notifications(&notifications);
    assert_report(retirement);
    assert_eq!(report, Some(retirement));
    assert!(polling.is_none() && disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty() && appended.is_ok());
    assert_eq!(announcements, [Some(()), Some(())]);
    assert_eq!(encodings, [0, 0]);
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(Poll::is_pending));
    assert_eq!(
        replies.iter().map(|row| row.0.len()).collect::<Vec<_>>(),
        [1, 0, 0]
    );
    let sample = replies[0].0[0].result().unwrap();
    assert_eq!(sample.key_expr().as_str(), "bombay/cancellation/read");
    let reply_original = reply_original.as_ref().unwrap();
    assert_eq!(
        reply_original.protected.as_ref(),
        sample.payload().to_bytes().as_ref()
    );
    assert_eq!(reply_original.proof.as_ref(), [13, 17]);
    let declared: ReadReply<Vec<u64>, LedgerOperation> =
        serde_json::from_slice(&reply_original.protected).unwrap();
    assert_eq!(declared.bombay, claims[0]);
    assert_eq!(declared.kind, Purpose::ReadReply);
    assert_eq!(declared.operation, LedgerOperation::Read);
    assert_eq!(declared.value, [u64::from(sender_origin)]);
    let verified = reply_verification.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(projection(verified).bombay, claims[0]);
    assert_eq!(
        (projection(verified).kind, projection(verified).operation),
        (Purpose::ReadReply, LedgerOperation::Read)
    );
    assert_eq!(identity(verified), (&binding, claims[0].request_sequence));
    if let Some(proof) = proof(verified) {
        assert_eq!(proof, [13, 17]);
    }
    let (origin, native) = native.expect("the complete original Stash root remains");
    let behavior = &native.custody().unwrap().behavior;
    assert_eq!(
        behavior.stashed_messages(),
        1,
        "B remains unprocessed inside the retired Stash"
    );
    let [(from, original)] = behavior.base().entries.as_slice() else {
        panic!("one original Append");
    };
    assert_eq!(
        (*from, original.as_ptr(), original.as_ref()),
        (sender_origin, append_allocation, &[59, 61][..])
    );
    assert_eq!(rows.len(), 3);
    let allocations = allocations.borrow();
    let receipts = receipts.borrow();
    assert_eq!((allocations.len(), receipts.len()), (3, 3));
    assert!(rows[0].0.is_some() && rows[1].0.is_some());
    assert_ne!(rows[0].0, rows[1].0);
    assert_eq!(rows[1].0, rows[2].0);
    let expected = [
        (&requests[0], &claims[0]),
        (&requests[1], &claims[1]),
        (&requests[1], &claims[1]),
    ];
    for (index, (row, (request, claims))) in rows.iter().zip(expected).enumerate() {
        assert_eq!(row.1.0.as_ref(), request.protected.as_ref());
        assert_eq!(row.1.1.as_ref(), request.proof.as_ref());
        assert_eq!((row.1.0.as_ptr(), row.1.1.as_ptr()), allocations[index]);
        let receipt = row.2.as_ref().unwrap().as_ref().unwrap();
        let scope = projection(receipt);
        assert_eq!(scope.bombay, *claims);
        assert_eq!(
            (scope.kind, scope.operation),
            (Purpose::Request, LedgerOperation::Read)
        );
        assert_eq!(identity(receipt), (&binding, scope.bombay.request_sequence));
        assert_eq!(
            (
                ledger_claim_allocations(scope),
                proof(receipt).map(<[u8]>::as_ptr)
            ),
            receipts[index]
        );
        if let Some(proof) = proof(receipt) {
            assert_eq!(proof, [7, 11]);
        }
        assert!(row.3.is_none());
        assert!(matches!(row.8, (None, None, None)));
        assert!(matches!(row.9, (None, None, None, None, None, None)));
    }
    let mut recipients = Vec::new();
    for row in &rows[..2] {
        let (constructor, client, admission, _, tail) = row.4.as_ref().unwrap();
        assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
        let Attempt::Returned(Ok(client)) = client else {
            panic!("the original native client remains");
        };
        recipients.push(client.address());
        assert!(tail.iter().all(Option::is_none));
    }
    assert_ne!(recipients[0], recipients[1]);
    assert_ne!(recipients[0], origin.address());
    assert_ne!(recipients[1], origin.address());
    let received_a = rows[0]
        .4
        .as_ref()
        .unwrap()
        .3
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(
        (received_a.from, received_a.message.as_slice()),
        (origin.address(), &[sender_origin][..])
    );
    assert!(matches!(rows[0].4.as_ref().unwrap().4.as_slice(), [None]));
    assert!(matches!(rows[1].4.as_ref().unwrap().3, Some(None)));
    assert!(rows[1].4.as_ref().unwrap().4.is_empty());
    assert!(rows[2].4.is_none() && rows[2].5.is_none() && rows[2].6.is_none());
    assert_eq!(
        (decoded.get(), constructed.get(), protected.get()),
        (2, 2, 1)
    );
    let encoded = encoded.borrow();
    let expected = [(origin.address(), vec![sender_origin])];
    assert_eq!(encoded.as_slice(), expected);
    assert!(matches!(rows[0].5, Some(Ok(_))) && matches!(rows[0].6, Some(Ok(_))));
    assert_eq!(rows[0].7.len(), 1);
    assert!(rows[0].7.iter().all(Result::is_ok));
    assert!(
        rows[1].5.is_none() && rows[1].6.is_none() && rows[1].7.is_empty() && rows[2].7.is_empty()
    );
    assert_completed((origin, native), None);
}

#[test]
fn recorded_stashed_reads_survive_a_cancelled_wire_wait() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                stashed_ledger_reads(
                    ProviderProfile::Recorded,
                    recorded::verify::<LedgerOperation>,
                    recorded::projection,
                    recorded::identity,
                    |_| None,
                    same_recorded_ledger,
                ),
            )
            .await
            .expect("the complete cold Ledger and cleanup trace terminates");
        });
}

#[test]
fn scheduled_stashed_reads_survive_a_cancelled_wire_wait() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                stashed_ledger_reads(
                    ProviderProfile::Scheduled,
                    scheduled::verify::<LedgerOperation>,
                    scheduled::projection,
                    scheduled::identity,
                    |receipt| Some(scheduled::proof(receipt)),
                    same_scheduled_ledger,
                ),
            )
            .await
            .expect("the complete cold Ledger and cleanup trace terminates");
        });
}
