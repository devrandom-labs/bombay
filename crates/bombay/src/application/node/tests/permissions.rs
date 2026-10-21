//! Native permission refusal survives redacted diagnostics and complete Node cleanup.
use super::custody::completed_actor;
use super::protocol::{
    Binding, CounterOperation, EndpointRole, LedgerOperation, ProofScope, ProtectedMessage,
    ProviderProfile, Purpose, ReadRequest, ReadRequestRef, VerificationError, configuration,
    current_scope, limits, metadata, recorded, same_recorded_counter, same_recorded_ledger,
    same_scheduled_counter, same_scheduled_ledger, scheduled,
};
use super::{Counter, CounterMessage, LedgerCommand, MailLedger};
use bombay::actors::ActorExt;
use bombay::behavior::{Never, User};
use bombay::remote::{Attempt, Export, ExportLimits, Metadata, reply};
use bombay::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment, TrySendError};
use core::ops::AsyncFn;
use serde::Serialize;
use std::cell::{BorrowError, Cell, Ref, RefCell};
use std::num::NonZeroUsize;
use std::sync::{Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use zenoh::Session;
use zenoh::query::{ConsolidationMode, Reply};
use zenoh::sample::Locality;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_diagnostics_preserve_opaque_commands_and_native_permission_causes() {
    // CounterMessage has no Debug implementation, including inside a native refusal.
    let attempts = [
        Attempt::Unattempted,
        Attempt::Uncertain,
        Attempt::Returned(Ok(())),
        Attempt::Returned(Err(TrySendError::Full(CounterMessage::Increment))),
        Attempt::Returned(Err(TrySendError::Closed(CounterMessage::Increment))),
    ];
    let diagnostics = attempts
        .iter()
        .map(|attempt| format!("{attempt:?}"))
        .collect::<Vec<_>>();
    assert_eq!(
        diagnostics,
        [
            "Unattempted",
            "Uncertain",
            "Returned(..)",
            "Returned(..)",
            "Returned(..)"
        ]
    );
    tokio::time::timeout(Duration::from_secs(40), borrowed_permission())
        .await
        .expect("the actual permission refusal and joined cleanup complete");
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "the exclusive authority loan deliberately remains held until its Query returns BorrowError; the verification table stays immutable during the provider wait"
)]
async fn borrowed_permission() {
    let permission = RefCell::new(());
    let checks = Cell::new(0);
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "permission-counter",
        logical_session: 359,
    };
    let maximum = NonZeroUsize::new(1).unwrap();
    let current = |_: &recorded::Receipt<CounterOperation>, _: &str, _: MailAddr| {
        checks.set(checks.get() + 1);
        permission.try_borrow()
    };
    let export = Export::new(
        "bombay/permission/read",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation)
        },
        reply(
            |CounterOperation::Read| {
                let constructed = &constructed;
                Ok::<_, CounterOperation>(move |reply_to| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Read(reply_to)
                })
            },
            |_: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                encoded.set(encoded.get() + 1);
                serde_json::to_vec(&original.message).map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            recorded::verify::<CounterOperation>(&original, &records.borrow(), binding).await
        },
        async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, request) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(
            ProviderProfile::Recorded,
            "counter",
            runtime.clone(),
            runtime,
        );
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        let original = ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([7, 11]),
        };
        records.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        configured
            .send(original)
            .unwrap_or_else(|_| panic!("Work retains the actual configured request"));
        Ok::<_, Never>(target)
    });
    let held = permission.borrow_mut();
    let outcome = node
        .run_with(|application| {
            let session = &session;
            async move {
                let request = request.await.expect("actual binding supplies the request");
                let receiving = session
                    .get("bombay/permission/read")
                    .allowed_destination(Locality::SessionLocal)
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(5))
                    .payload(request.protected.to_vec())
                    .attachment(request.proof.to_vec())
                    .await
                    .expect("the actual native query starts");
                let ending = receiving.recv_async().await;
                let shutdown = application.shutdown();
                (request, ending, shutdown)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node enters its host"));
    drop(held);
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, ending, shutdown), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual Work and its Query ending remain acquired"));
    cleanup.expect("actual Application cleanup joins");
    closed.expect("actual Node Session close succeeds");
    let (origin, native) = native.expect("the complete original Counter root remains");
    let _counter = completed_actor(origin, native);
    let report = report.expect("the actual root report remains independent");
    let notifications = notifications.expect("both actual notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty() && ending.is_err() && shutdown.is_ok() && session.is_closed());
    let [request] =
        <[_; 1]>::try_from(requests).unwrap_or_else(|_| panic!("one actual request is retained"));
    let (key, raw, verified, input, native, encoding, protection, publications, refusals, panics) =
        request
            .try_into_parts()
            .unwrap_or_else(|_| panic!("the actual request releases its producer loans"));
    let _cause: &BorrowError = refusals
        .0
        .as_ref()
        .expect("the original typed native permission cause remains inspectable");
    assert!(key.is_none());
    assert_eq!(raw.0, original.protected);
    assert_eq!(raw.1, original.proof);
    assert!(matches!(verified, Some(Ok(_))));
    assert!(input.is_none() && native.is_none() && encoding.is_none() && protection.is_none());
    assert!(publications.is_empty() && refusals.1.is_none() && refusals.2.is_none());
    assert!(matches!(panics, (None, None, None, None, None, None)));
    assert_eq!(
        (
            checks.get(),
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get()
        ),
        (1, 0, 0, 0, 0)
    );
}

#[derive(Debug)]
enum LocalAuthorityRefusal {
    Borrow { cause: BorrowError },
    Scope { cause: VerificationError },
    GrantRemoved,
    BindingMismatch,
}

fn local_current<'loan, Receipt, Operation: Eq>(
    authority: &'loan RefCell<Option<Binding>>,
    receipt: &Receipt,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    current: &Metadata,
    operation: &Operation,
    deadline: Instant,
) -> Result<Ref<'loan, Option<Binding>>, LocalAuthorityRefusal> {
    let guard = authority
        .try_borrow()
        .map_err(|cause| LocalAuthorityRefusal::Borrow { cause })?;
    current_scope(
        projection(receipt),
        current,
        operation,
        Purpose::Request,
        deadline,
    )
    .map_err(|cause| LocalAuthorityRefusal::Scope { cause })?;
    match guard.as_ref() {
        None => Err(LocalAuthorityRefusal::GrantRemoved),
        Some(binding) if binding != identity(receipt).0 => {
            Err(LocalAuthorityRefusal::BindingMismatch)
        }
        Some(_) => Ok(guard),
    }
}

fn permission_originals<Receipt, Operation: Serialize>(
    raw: &[u8],
    proof: &[u8],
    receipt: &Receipt,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
) -> (Vec<u8>, Vec<u8>, Vec<u8>, Binding, [*const u8; 12]) {
    let scope = projection(receipt);
    let claims = &scope.bombay;
    (
        raw.to_vec(),
        proof.to_vec(),
        serde_json::to_vec(&ReadRequestRef {
            bombay: claims,
            kind: scope.kind,
            operation: &scope.operation,
        })
        .unwrap(),
        *identity(receipt).0,
        [
            raw.as_ptr(),
            proof.as_ptr(),
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
        ],
    )
}

async fn permission_replies(
    session: &Session,
    path: &str,
    original: &ProtectedMessage,
) -> Vec<Reply> {
    let receiving = session
        .get(path)
        .allowed_destination(Locality::SessionLocal)
        .consolidation(ConsolidationMode::None)
        .timeout(Duration::from_secs(5))
        .payload(original.protected.to_vec())
        .attachment(original.proof.to_vec())
        .await
        .expect("the actual Query starts");
    let mut replies = Vec::new();
    while let Ok(reply) = receiving.recv_async().await {
        replies.push(reply);
    }
    replies
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "the first Query must observe the deliberately held exclusive authority loan; immutable provider records and all native originals survive the actual waits"
)]
async fn counter_permissions<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<CounterOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    proof: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let first_binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "permission-counter",
        logical_session: 41,
    };
    let second_binding = Binding {
        logical_session: 43,
        ..first_binding
    };
    let authority = RefCell::new(Some(first_binding));
    let provider_binding = Cell::new(first_binding);
    let current_claims = RefCell::new(None::<Metadata>);
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let before = RefCell::new(Vec::new());
    let guards = RefCell::new(Vec::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let checked = Cell::new(0);
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let claims = current_claims.borrow();
        let claims = claims.as_ref().expect("binding precedes serving");
        if claims.runtime_generation.as_ref() != runtime || claims.actor_generation != target.0 {
            return Err(LocalAuthorityRefusal::Scope {
                cause: VerificationError::Invalid,
            });
        }
        local_current(
            &authority,
            receipt,
            projection,
            identity,
            claims,
            &CounterOperation::Read,
            deadline,
        )
    };
    let maximum = NonZeroUsize::new(8).unwrap();
    let export = Export::new(
        "bombay/permission/counter",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            decoded.set(decoded.get() + 1);
            serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation)
        },
        reply(
            |CounterOperation::Read| {
                let authority = &authority;
                let guards = &guards;
                let constructed = &constructed;
                Ok::<_, CounterOperation>(move |reply_to| {
                    let observation = authority.try_borrow_mut().map(drop);
                    guards.borrow_mut().push(observation);
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Read(reply_to)
                })
            },
            |_: &Receipt, original: &User<MailAddr, u64>| {
                encoded.set(encoded.get() + 1);
                serde_json::to_vec(&original.message).map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let result = provider(&original, &records.borrow(), provider_binding.get()).await;
            if let Ok(receipt) = &result {
                before.borrow_mut().push(permission_originals(
                    bytes, proof, receipt, projection, identity,
                ));
            }
            result
        },
        async |_: &[u8], _: &Receipt| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        same_request,
        |receipt: &Receipt, runtime: &str, target: MailAddr| {
            checked.set(checked.get() + 1);
            current(receipt, runtime, target)
        },
        &current,
    );
    let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
    // Native timeout-task destruction may retain capacity after Query EOF.
    // This permission trace explicitly reserves all eight original queries.
    network.max_retained_queries = maximum;
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, request) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(profile, "counter", runtime.clone(), runtime);
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        let original = ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([7_u8, 11]),
        };
        records.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        *current_claims.borrow_mut() = Some(claims);
        configured
            .send(original)
            .unwrap_or_else(|_| panic!("Work receives the actual request"));
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let session = &session;
            let authority = &authority;
            let current_claims = &current_claims;
            let provider_binding = &provider_binding;
            async move {
                let original = request.await.expect("binding supplies the actual request");
                let sender = application.sender().unwrap();
                let primed = sender.send(CounterMessage::Increment).await;
                let path = "bombay/permission/counter";
                let mut replies = Vec::new();
                let held = authority.borrow_mut();
                replies.push(permission_replies(session, path, &original).await);
                drop(held);
                *authority.borrow_mut() = None;
                replies.push(permission_replies(session, path, &original).await);
                *authority.borrow_mut() = Some(first_binding);
                current_claims
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .authority_generation += 1;
                replies.push(permission_replies(session, path, &original).await);
                current_claims
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .authority_generation -= 1;
                replies.push(permission_replies(session, path, &original).await);
                *authority.borrow_mut() = None;
                replies.push(permission_replies(session, path, &original).await);
                *authority.borrow_mut() = Some(first_binding);
                replies.push(permission_replies(session, path, &original).await);
                provider_binding.set(second_binding);
                replies.push(permission_replies(session, path, &original).await);
                *authority.borrow_mut() = Some(second_binding);
                replies.push(permission_replies(session, path, &original).await);
                let shutdown = application.shutdown();
                (original, replies, primed, shutdown)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Counter Node enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, replies, primed, shutdown), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the actual Work remains complete"));
    cleanup.expect("actual Application cleanup joins");
    closed.expect("actual Session close succeeds");
    let (origin, native) = native.expect("the complete native root remains");
    let _counter = completed_actor(origin, native);
    let report = report.expect("the root report remains independent");
    let notifications = notifications.expect("both notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none() && session.is_closed());
    assert!(
        matches!(bound, Some(Ok(_)))
            && matches!(startup, Some(Ok(())))
            && matches!(withdrawal, Some(Ok(())))
    );
    assert!(diagnostics.is_empty() && primed.is_ok() && shutdown.is_ok());
    let guards = guards.borrow();
    assert_eq!(guards.len(), 2);
    assert!(
        guards.iter().all(Result::is_err),
        "permission excludes mutation during each actual native constructor"
    );
    let mut rows = requests
        .into_iter()
        .map(|request| {
            request
                .try_into_parts()
                .unwrap_or_else(|_| panic!("the actual request releases its producer loans"))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 8);
    let after = rows
        .iter()
        .map(|row| {
            permission_originals(
                &row.1.0,
                &row.1.1,
                row.2.as_ref().unwrap().as_ref().unwrap(),
                projection,
                identity,
            )
        })
        .collect::<Vec<_>>();
    let before = before.into_inner();
    assert_eq!(before, after);
    let current_claims = current_claims.borrow();
    let current_claims = current_claims.as_ref().unwrap();
    for row in &rows {
        let receipt = row.2.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(row.1.0, original.protected);
        assert_eq!(row.1.1, original.proof);
        assert_eq!(projection(receipt).bombay, *current_claims);
        assert_eq!(projection(receipt).operation, CounterOperation::Read);
        assert_eq!(projection(receipt).kind, Purpose::Request);
        if let Some(actual) = proof(receipt) {
            assert_eq!(actual, [7, 11]);
        }
        assert!(row.3.is_none());
        assert!(row.8.1.is_none() && row.8.2.is_none());
        assert!(matches!(row.9, (None, None, None, None, None, None)));
    }
    let LocalAuthorityRefusal::Borrow { cause } = rows[0].8.0.as_ref().unwrap() else {
        panic!("actual native BorrowError remains");
    };
    let _: &BorrowError = cause;
    assert!(matches!(
        rows[1].8.0,
        Some(LocalAuthorityRefusal::GrantRemoved)
    ));
    assert!(matches!(
        rows[2].8.0,
        Some(LocalAuthorityRefusal::Scope {
            cause: VerificationError::Invalid
        })
    ));
    assert!(matches!(
        rows[4].8.0,
        Some(LocalAuthorityRefusal::GrantRemoved)
    ));
    assert!(matches!(
        rows[6].8.0,
        Some(LocalAuthorityRefusal::BindingMismatch)
    ));
    for position in [0, 1, 2, 4, 6] {
        let row = &rows[position];
        assert!(
            row.0.is_none()
                && row.4.is_none()
                && row.5.is_none()
                && row.6.is_none()
                && row.7.is_empty()
        );
    }
    assert!(rows[3].0.is_some() && rows[7].0.is_some());
    assert_eq!(rows[3].0, rows[5].0);
    assert_ne!(rows[3].0, rows[7].0);
    assert_eq!(
        identity(rows[3].2.as_ref().unwrap().as_ref().unwrap()).0,
        &first_binding
    );
    assert_eq!(
        identity(rows[7].2.as_ref().unwrap().as_ref().unwrap()).0,
        &second_binding
    );
    let duplicate = &rows[5];
    assert!(
        duplicate.4.is_none()
            && duplicate.5.is_none()
            && duplicate.6.is_none()
            && duplicate.8.0.is_none()
    );
    let mut recipients = Vec::new();
    for position in [3, 7] {
        let row = &mut rows[position];
        let (constructor, client, admission, received, tail) =
            row.4.take().expect("the actual native request remains");
        assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
        let Attempt::Returned(Ok(client)) = client else {
            panic!("the actual reply client remains");
        };
        recipients.push(client.recipient());
        let original = received
            .expect("the native receive returned")
            .expect("the actual actor reply remains");
        assert_eq!((original.from, original.message), (origin.address(), 1));
        assert_ne!(client.address(), origin.address());
        assert!(matches!(tail.as_slice(), [None]));
        assert!(matches!(row.5.as_ref(), Some(Ok(_))) && matches!(row.6.as_ref(), Some(Ok(_))));
        assert!(row.8.0.is_none());
    }
    assert_ne!(recipients[0], recipients[1]);
    assert_eq!(
        (
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get(),
            checked.get()
        ),
        // Eight initial checks and two fresh native-attempt checks.
        (2, 2, 2, 2, 8 + 2)
    );
    assert_eq!(
        replies.iter().map(Vec::len).collect::<Vec<_>>(),
        [0, 0, 0, 1, 0, 1, 0, 1]
    );
    for (row, replies) in rows.iter().zip(&replies) {
        assert_eq!(row.7.len(), replies.len());
        assert!(row.7.iter().all(Result::is_ok));
        for reply in replies {
            let result = reply.result();
            let sample = result
                .as_ref()
                .expect("the actual protected reply is successful");
            let payload = sample.payload().to_bytes();
            let attachment = sample.attachment().unwrap().to_bytes();
            assert_eq!(payload.as_ref(), b"1");
            assert_eq!(attachment.as_ref(), [13, 17]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_counter_permissions_survive_cold_queries() {
    tokio::time::timeout(
        Duration::from_secs(40),
        counter_permissions(
            ProviderProfile::Recorded,
            recorded::verify::<CounterOperation>,
            recorded::projection,
            recorded::identity,
            |_| None,
            same_recorded_counter,
        ),
    )
    .await
    .expect("the actual authority trace and joined cleanup finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_counter_permissions_survive_cold_queries() {
    tokio::time::timeout(
        Duration::from_secs(40),
        counter_permissions(
            ProviderProfile::Scheduled,
            scheduled::verify::<CounterOperation>,
            scheduled::projection,
            scheduled::identity,
            |receipt| Some(scheduled::proof(receipt)),
            same_scheduled_counter,
        ),
    )
    .await
    .expect("the actual authority trace and joined cleanup finish");
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    clippy::await_holding_lock,
    reason = "the first Query must observe the actual held MutexGuard as WouldBlock; immutable provider records and complete native originals survive the actual waits"
)]
async fn ledger_permissions<Receipt>(
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
    let first_binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "ledger",
        host: "receiver",
        protocol: "ledger",
        target_assignment: "permission-ledger",
        logical_session: 41,
    };
    let second_binding = Binding {
        logical_session: 43,
        ..first_binding
    };
    let authority = Mutex::new(Some(first_binding));
    let provider_binding = Cell::new(first_binding);
    let current_claims = RefCell::new(None::<Metadata>);
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let before = RefCell::new(Vec::new());
    let guards = RefCell::new(Vec::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let checked = Cell::new(0);
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let claims = current_claims.borrow();
        let claims = claims.as_ref().expect("binding precedes serving");
        if claims.runtime_generation.as_ref() != runtime || claims.actor_generation != target.0 {
            return Err(SynchronizedAuthorityRefusal::Scope {
                cause: VerificationError::Invalid,
            });
        }
        synchronized_current(
            &authority,
            receipt,
            projection,
            identity,
            claims,
            &LedgerOperation::Read,
            deadline,
        )
    };
    let maximum = NonZeroUsize::new(8).unwrap();
    let export = Export::new(
        "bombay/permission/ledger",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
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
                let authority = &authority;
                let guards = &guards;
                let constructed = &constructed;
                Ok::<_, LedgerOperation>(move |reply_to| {
                    let observation = authority.try_lock().map(drop);
                    guards.borrow_mut().push(observation);
                    constructed.set(constructed.get() + 1);
                    LedgerCommand::Read(reply_to)
                })
            },
            |_: &Receipt, original: &User<MailAddr, Vec<MailAddr>>| {
                encoded.set(encoded.get() + 1);
                let origins = original
                    .message
                    .iter()
                    .copied()
                    .map(u64::from)
                    .collect::<Vec<_>>();
                serde_json::to_vec(&origins).map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let result = provider(&original, &records.borrow(), provider_binding.get()).await;
            if let Ok(receipt) = &result {
                before.borrow_mut().push(permission_originals(
                    bytes, proof, receipt, projection, identity,
                ));
            }
            result
        },
        async |_: &[u8], _: &Receipt| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        same_request,
        |receipt: &Receipt, runtime: &str, target: MailAddr| {
            checked.set(checked.get() + 1);
            current(receipt, runtime, target)
        },
        &current,
    );
    let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
    // Native timeout-task destruction may retain capacity after Query EOF.
    // This permission trace explicitly reserves all eight original queries.
    network.max_retained_queries = maximum;
    let node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(network)
    .await
    .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, request) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(profile, "ledger", runtime.clone(), runtime);
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        let original = ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: LedgerOperation::Read,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([7_u8, 11]),
        };
        records.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        *current_claims.borrow_mut() = Some(claims);
        configured
            .send(original)
            .unwrap_or_else(|_| panic!("Work receives the actual request"));
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let session = &session;
            let authority = &authority;
            let current_claims = &current_claims;
            let provider_binding = &provider_binding;
            async move {
                let original = request.await.expect("binding supplies the actual request");
                let sender = application.sender().unwrap();
                let caller = sender.address();
                let append = Box::<[u8]>::from([59_u8, 61]);
                let allocation = append.as_ptr();
                let primed = sender.send(LedgerCommand::Append(append)).await;
                let path = "bombay/permission/ledger";
                let mut replies = Vec::new();
                let held = authority.lock().unwrap();
                replies.push(permission_replies(session, path, &original).await);
                drop(held);
                *authority.lock().unwrap() = None;
                replies.push(permission_replies(session, path, &original).await);
                *authority.lock().unwrap() = Some(first_binding);
                current_claims
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .authority_generation += 1;
                replies.push(permission_replies(session, path, &original).await);
                current_claims
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .authority_generation -= 1;
                replies.push(permission_replies(session, path, &original).await);
                let retirement = application.lifecycle().retirement().await;
                *authority.lock().unwrap() = None;
                replies.push(permission_replies(session, path, &original).await);
                *authority.lock().unwrap() = Some(first_binding);
                replies.push(permission_replies(session, path, &original).await);
                provider_binding.set(second_binding);
                replies.push(permission_replies(session, path, &original).await);
                *authority.lock().unwrap() = Some(second_binding);
                replies.push(permission_replies(session, path, &original).await);
                (original, replies, primed, caller, allocation, retirement)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger Node enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, replies, primed, caller, allocation, retirement), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the actual Work remains complete"));
    cleanup.expect("actual Application cleanup joins");
    closed.expect("actual Session close succeeds");
    let (origin, native) = native.expect("the complete native root remains");
    let ledger = completed_actor(origin, native).into_inner();
    let report = report.expect("the root report remains independent");
    let notifications = notifications.expect("both notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none() && session.is_closed());
    assert!(
        matches!(bound, Some(Ok(_)))
            && matches!(startup, Some(Ok(())))
            && matches!(withdrawal, Some(Ok(())))
    );
    assert!(diagnostics.is_empty() && primed.is_ok());
    assert_eq!(report, retirement);
    let [(append_caller, append)] = ledger.entries.as_slice() else {
        panic!("the actual native Ledger retains its one original append");
    };
    assert_eq!(*append_caller, caller);
    assert_eq!(append.as_ref(), [59, 61]);
    assert_eq!(append.as_ptr(), allocation);
    let guards = guards.borrow();
    assert_eq!(guards.len(), 2);
    assert!(
        guards
            .iter()
            .all(|result| matches!(result, Err(TryLockError::WouldBlock))),
        "permission excludes mutation during each actual native constructor"
    );
    let mut rows = requests
        .into_iter()
        .map(|request| {
            request
                .try_into_parts()
                .unwrap_or_else(|_| panic!("the actual request releases its producer loans"))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 8);
    let after = rows
        .iter()
        .map(|row| {
            permission_originals(
                &row.1.0,
                &row.1.1,
                row.2.as_ref().unwrap().as_ref().unwrap(),
                projection,
                identity,
            )
        })
        .collect::<Vec<_>>();
    let before = before.into_inner();
    assert_eq!(before, after);
    let current_claims = current_claims.borrow();
    let current_claims = current_claims.as_ref().unwrap();
    for row in &rows {
        let receipt = row.2.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(row.1.0, original.protected);
        assert_eq!(row.1.1, original.proof);
        assert_eq!(projection(receipt).bombay, *current_claims);
        assert_eq!(projection(receipt).operation, LedgerOperation::Read);
        assert_eq!(projection(receipt).kind, Purpose::Request);
        if let Some(actual) = proof(receipt) {
            assert_eq!(actual, [7, 11]);
        }
        assert!(row.3.is_none());
        assert!(row.8.2.is_none());
        assert!(matches!(row.9, (None, None, None, None, None, None)));
    }
    let SynchronizedAuthorityRefusal::Lock { cause } = rows[0].8.0.as_ref().unwrap() else {
        panic!("actual native TryLockError remains");
    };
    assert!(matches!(cause, TryLockError::WouldBlock));
    assert!(matches!(
        rows[1].8.0,
        Some(SynchronizedAuthorityRefusal::GrantRemoved)
    ));
    assert!(matches!(
        rows[2].8.0,
        Some(SynchronizedAuthorityRefusal::Scope {
            cause: VerificationError::Invalid
        })
    ));
    assert!(matches!(
        rows[4].8.0,
        Some(SynchronizedAuthorityRefusal::GrantRemoved)
    ));
    assert!(matches!(
        rows[6].8.0,
        Some(SynchronizedAuthorityRefusal::BindingMismatch)
    ));
    for position in [0, 1, 2, 4, 6] {
        let row = &rows[position];
        assert!(
            row.0.is_none()
                && row.4.is_none()
                && row.5.is_none()
                && row.6.is_none()
                && row.7.is_empty()
        );
    }
    assert!(
        rows[5].8.1.is_none(),
        "cached replies end request authority before independently checking reply authority: {:?}",
        rows[5].8.1
    );
    assert!(rows.iter().all(|row| row.8.1.is_none()));
    assert!(rows[3].0.is_some() && rows[7].0.is_some());
    assert_eq!(rows[3].0, rows[5].0);
    assert_ne!(rows[3].0, rows[7].0);
    assert_eq!(
        identity(rows[3].2.as_ref().unwrap().as_ref().unwrap()).0,
        &first_binding
    );
    assert_eq!(
        identity(rows[7].2.as_ref().unwrap().as_ref().unwrap()).0,
        &second_binding
    );
    let duplicate = &rows[5];
    assert!(
        duplicate.4.is_none()
            && duplicate.5.is_none()
            && duplicate.6.is_none()
            && duplicate.8.0.is_none()
    );
    let mut recipients = Vec::new();
    for position in [3, 7] {
        let row = &mut rows[position];
        let (constructor, client, admission, received, tail) =
            row.4.take().expect("the actual native request remains");
        assert!(constructor.is_none());
        let Attempt::Returned(Ok(client)) = client else {
            panic!("the actual reply client remains");
        };
        let recipient = client.recipient();
        recipients.push(recipient.clone());
        assert_ne!(client.address(), origin.address());
        assert!(matches!(tail.as_slice(), [None]));
        match admission {
            Attempt::Returned(Ok(())) => {
                assert_eq!(position, 3);
                let original = received
                    .expect("the actual native read returned")
                    .expect("the actor's actual User remains");
                assert_eq!(original.from, origin.address());
                assert_eq!(original.message.as_slice(), [caller]);
                assert!(
                    matches!(row.5.as_ref(), Some(Ok(_))) && matches!(row.6.as_ref(), Some(Ok(_)))
                );
            }
            Attempt::Returned(Err(TrySendError::Closed(LedgerCommand::Read(returned)))) => {
                assert_eq!(position, 7);
                assert_eq!(returned, recipient);
                assert!(received.is_none() && row.5.is_none() && row.6.is_none());
            }
            _ => panic!(
                "A is admitted and freshly constructed B returns native Closed(Read) after actual retirement"
            ),
        }
        assert!(row.8.0.is_none());
    }
    assert_ne!(recipients[0], recipients[1]);
    assert_eq!(
        (
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get(),
            checked.get()
        ),
        (2, 2, 1, 1, 8 + 2)
    );
    assert_eq!(
        replies.iter().map(Vec::len).collect::<Vec<_>>(),
        [0, 0, 0, 1, 0, 1, 0, 0]
    );
    let expected = serde_json::to_vec(&[u64::from(caller)]).unwrap();
    for (row, replies) in rows.iter().zip(&replies) {
        assert_eq!(row.7.len(), replies.len());
        assert!(row.7.iter().all(Result::is_ok));
        for reply in replies {
            let result = reply.result();
            let sample = result
                .as_ref()
                .expect("the actual protected reply is successful");
            let payload = sample.payload().to_bytes();
            let attachment = sample.attachment().unwrap().to_bytes();
            assert_eq!(payload.as_ref(), expected.as_slice());
            assert_eq!(attachment.as_ref(), [13, 17]);
        }
    }
}

#[derive(Debug)]
enum SynchronizedAuthorityRefusal<'loan> {
    Lock {
        cause: TryLockError<MutexGuard<'loan, Option<Binding>>>,
    },
    Scope {
        cause: VerificationError,
    },
    GrantRemoved,
    BindingMismatch,
}

fn synchronized_current<'loan, Receipt, Operation: Eq>(
    authority: &'loan Mutex<Option<Binding>>,
    receipt: &Receipt,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    current: &Metadata,
    operation: &Operation,
    deadline: Instant,
) -> Result<MutexGuard<'loan, Option<Binding>>, SynchronizedAuthorityRefusal<'loan>> {
    let guard = authority
        .try_lock()
        .map_err(|cause| SynchronizedAuthorityRefusal::Lock { cause })?;
    current_scope(
        projection(receipt),
        current,
        operation,
        Purpose::Request,
        deadline,
    )
    .map_err(|cause| SynchronizedAuthorityRefusal::Scope { cause })?;
    match guard.as_ref() {
        None => Err(SynchronizedAuthorityRefusal::GrantRemoved),
        Some(binding) if binding != identity(receipt).0 => {
            Err(SynchronizedAuthorityRefusal::BindingMismatch)
        }
        Some(_) => Ok(guard),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_ledger_permissions_survive_cold_queries() {
    tokio::time::timeout(
        Duration::from_secs(40),
        ledger_permissions(
            ProviderProfile::Recorded,
            recorded::verify::<LedgerOperation>,
            recorded::projection,
            recorded::identity,
            |_| None,
            same_recorded_ledger,
        ),
    )
    .await
    .expect("the actual synchronized authority trace and joined cleanup finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_ledger_permissions_survive_cold_queries() {
    tokio::time::timeout(
        Duration::from_secs(40),
        ledger_permissions(
            ProviderProfile::Scheduled,
            scheduled::verify::<LedgerOperation>,
            scheduled::projection,
            scheduled::identity,
            |receipt| Some(scheduled::proof(receipt)),
            same_scheduled_ledger,
        ),
    )
    .await
    .expect("the actual synchronized authority trace and joined cleanup finish");
}
