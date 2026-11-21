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
use bombay::remote::{Attempt, Export, ExportLimits, Metadata, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment, TrySendError};
use core::ops::AsyncFn;
use serde::Serialize;
use std::cell::{BorrowError, Cell, Ref, RefCell};
use std::error::Error;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::ptr;
use std::str;
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
            refusals: RefusalRetention::Retain,
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
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, ending, shutdown), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual Work and its Query ending remain acquired"));
    cleanup.expect("actual Application cleanup joins");
    closed.expect("actual Node Session close succeeds");
    let (origin, native) = native.expect("the complete original Counter root remains");
    let actor = origin.address();
    let _counter = completed_actor(origin, native);
    let report = report.expect("the actual root report remains independent");
    let notifications = notifications.expect("both actual notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    let [request] =
        <[_; 1]>::try_from(requests).unwrap_or_else(|_| panic!("one actual request is retained"));
    let (key, raw, verified, input, native, encoding, protection, publications, refusals, panics) =
        request
            .try_into_parts()
            .unwrap_or_else(|_| panic!("the actual request releases its producer loans"));
    let cause: &BorrowError = refusals
        .0
        .as_ref()
        .expect("the original typed native permission cause remains inspectable");
    // Presentation starts only after actual join; no writer can hold retirement.
    let original_cause = ptr::from_ref(cause);
    let original_pair = (raw.0.as_ptr(), raw.1.as_ptr());
    let receipt = verified.as_ref().unwrap().as_ref().unwrap();
    let (authority, sequence) = recorded::identity(receipt);
    let original_authority = ptr::from_ref(authority);
    let scope = recorded::projection(receipt);
    let declared: ReadRequest<CounterOperation> = serde_json::from_slice(&raw.0).unwrap();
    let runtime = session.zid().to_string();
    let present = |mut diagnostic: &mut [u8]| {
        let result = write!(
            diagnostic,
            "command={:?}; request={sequence}; deployment={}; caller={}; protocol={}; runtime={}; actor={}; permission={cause}",
            scope.operation,
            authority.deployment,
            authority.caller,
            authority.protocol,
            scope.bombay.runtime_generation,
            scope.bombay.actor_generation
        );
        (result, diagnostic.len())
    };
    let (failed, _) = present(&mut []);
    let mut diagnostic = [0_u8; 512];
    let (written, remaining) = present(&mut diagnostic);
    let text = str::from_utf8(&diagnostic[..diagnostic.len() - remaining]).unwrap();
    let native_text = cause.to_string();
    let expected = format!(
        "command=Read; request={}; deployment={}; caller={}; protocol={}; runtime={runtime}; actor={}; permission={native_text}",
        declared.bombay.request_sequence,
        binding.deployment,
        binding.caller,
        binding.protocol,
        actor.0
    );
    let failed_kind = failed.as_ref().err().map(io::Error::kind);
    let written = written.as_ref().map_err(io::Error::kind).copied();
    let retained_cause = refusals.0.as_ref().map(ptr::from_ref);
    let retained_pair = (raw.0.as_ptr(), raw.1.as_ptr());
    let retained_authority = ptr::from_ref(recorded::identity(receipt).0);
    let native_source = cause.source();
    assert_eq!(
        (
            failed_kind,
            written,
            text,
            retained_cause,
            retained_pair,
            retained_authority,
            recorded::identity(receipt)
        ),
        (
            Some(io::ErrorKind::WriteZero),
            Ok(()),
            expected.as_str(),
            Some(original_cause),
            original_pair,
            original_authority,
            (&binding, declared.bombay.request_sequence)
        )
    );
    assert!(native_source.is_none());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty() && ending.is_err() && shutdown.is_ok() && session.is_closed());
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
pub(super) enum LocalAuthorityRefusal {
    Borrow { cause: BorrowError },
    Scope { cause: VerificationError },
    GrantRemoved,
    BindingMismatch,
}

pub(super) fn local_current<'loan, Receipt, Operation: Eq>(
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
            refusals: RefusalRetention::Retain,
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
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
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
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
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
            refusals: RefusalRetention::Retain,
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
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
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
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
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

mod delegation {
    use super::super::requester::{CounterOperation as GrantOperation, Permission};
    use super::{
        Binding, Counter, CounterMessage, CounterOperation, EndpointRole, Metadata,
        ProtectedMessage, ProviderProfile, Purpose, VerificationError, completed_actor,
        configuration, limits, metadata, permission_replies,
    };
    use bombay::actors::ActorExt;
    use bombay::behavior::{Never, User};
    use bombay::remote::{Attempt, Export, ExportLimits, RefusalRetention, reply};
    use bombay::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
    use core::ops::AsyncFn;
    use serde::{Deserialize, Serialize};
    use std::cell::{BorrowError, Ref, RefCell};
    use std::future::{Ready, ready};
    use std::num::NonZeroUsize;
    use std::rc::Rc;
    use std::time::{Duration, Instant};
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Principal {
        Authority,
        Broker,
        Delegate,
        Caller,
    }

    impl Principal {
        fn name(self) -> &'static str {
            match self {
                Self::Authority => "authority",
                Self::Broker => "broker",
                Self::Delegate => "delegate",
                Self::Caller => "caller",
            }
        }
    }

    struct GrantScope {
        tenants: Vec<&'static str>,
        protocols: Vec<&'static str>,
        actors: Vec<(&'static str, &'static str)>,
        operations: Vec<GrantOperation>,
    }

    impl GrantScope {
        fn authority() -> Self {
            Self {
                tenants: vec!["shop", "workshop", "warehouse"],
                protocols: vec!["counter", "ledger", "audit"],
                actors: vec![
                    ("owner", "counter"),
                    ("owner", "ledger"),
                    ("owner", "audit"),
                ],
                operations: vec![GrantOperation::Read, GrantOperation::Increment],
            }
        }

        fn broker() -> Self {
            Self {
                tenants: vec!["shop", "workshop"],
                protocols: vec!["counter", "ledger"],
                actors: vec![("owner", "counter"), ("owner", "ledger")],
                operations: vec![GrantOperation::Read, GrantOperation::Increment],
            }
        }

        fn caller() -> Self {
            Self {
                tenants: vec!["shop"],
                protocols: vec!["counter"],
                actors: vec![("owner", "counter")],
                operations: vec![GrantOperation::Read],
            }
        }
    }

    struct Grant {
        id: u8,
        parent: Option<u8>,
        issuer: Principal,
        subject: Principal,
        issuer_generation: u64,
        scope: GrantScope,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Dimension {
        Tenant,
        Protocol,
        Actor,
        Operation,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Edge {
        Authority,
        Delegate,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum GrantCase {
        Permitted,
        AuthorityScope(Dimension),
        DelegatedScope(Dimension),
        TooDeep,
        Direct,
        AuthorityIssuer,
        AuthorityParent,
        DifferentIssuer,
        DifferentParent,
        AuthorityRevoked,
        AuthorityRestored,
        BrokerRevoked,
        BrokerRestored,
        AuthorityStale,
        AuthorityRotated,
        AuthorityStillStale,
        BrokerStale,
        BrokerRotated,
        BrokerStillStale,
        Progress,
    }

    const CASES: [GrantCase; 26] = [
        GrantCase::Permitted,
        GrantCase::AuthorityScope(Dimension::Tenant),
        GrantCase::AuthorityScope(Dimension::Protocol),
        GrantCase::AuthorityScope(Dimension::Actor),
        GrantCase::AuthorityScope(Dimension::Operation),
        GrantCase::DelegatedScope(Dimension::Tenant),
        GrantCase::DelegatedScope(Dimension::Protocol),
        GrantCase::DelegatedScope(Dimension::Actor),
        GrantCase::DelegatedScope(Dimension::Operation),
        GrantCase::TooDeep,
        GrantCase::Direct,
        GrantCase::AuthorityIssuer,
        GrantCase::AuthorityParent,
        GrantCase::DifferentIssuer,
        GrantCase::DifferentParent,
        GrantCase::AuthorityRevoked,
        GrantCase::AuthorityRestored,
        GrantCase::BrokerRevoked,
        GrantCase::BrokerRestored,
        GrantCase::AuthorityStale,
        GrantCase::AuthorityRotated,
        GrantCase::AuthorityStillStale,
        GrantCase::BrokerStale,
        GrantCase::BrokerRotated,
        GrantCase::BrokerStillStale,
        GrantCase::Progress,
    ];

    #[derive(Debug, PartialEq, Eq)]
    enum NativeObservation {
        Absent,
        SelectedWithoutReply,
        Replied { from: MailAddr, value: u64 },
    }

    fn grants(case: GrantCase) -> Rc<[Grant]> {
        let mut broker = Grant {
            id: 11,
            parent: None,
            issuer: Principal::Authority,
            subject: Principal::Broker,
            issuer_generation: 1,
            scope: GrantScope::broker(),
        };
        let mut caller = Grant {
            id: 17,
            parent: Some(broker.id),
            issuer: Principal::Broker,
            subject: Principal::Caller,
            issuer_generation: 1,
            scope: GrantScope::caller(),
        };
        match case {
            GrantCase::AuthorityScope(dimension) => match dimension {
                Dimension::Tenant => broker.scope.tenants[1] = "foreign",
                Dimension::Protocol => broker.scope.protocols[1] = "foreign",
                Dimension::Actor => broker.scope.actors[1] = ("owner", "foreign"),
                Dimension::Operation => broker.scope.operations[1] = GrantOperation::Stop,
            },
            GrantCase::DelegatedScope(dimension) => match dimension {
                Dimension::Tenant => caller.scope.tenants.push("warehouse"),
                Dimension::Protocol => caller.scope.protocols.push("audit"),
                Dimension::Actor => caller.scope.actors.push(("owner", "audit")),
                Dimension::Operation => caller.scope.operations.push(GrantOperation::Stop),
            },
            GrantCase::TooDeep => {
                caller.subject = Principal::Delegate;
                let leaf = Grant {
                    id: 23,
                    parent: Some(caller.id),
                    issuer: Principal::Delegate,
                    subject: Principal::Caller,
                    issuer_generation: 1,
                    scope: GrantScope::caller(),
                };
                return vec![broker, caller, leaf].into();
            }
            GrantCase::Direct => {
                broker.subject = Principal::Caller;
                broker.scope = GrantScope::caller();
                return vec![broker].into();
            }
            GrantCase::AuthorityIssuer => broker.issuer = Principal::Broker,
            GrantCase::AuthorityParent => broker.parent = Some(29),
            GrantCase::DifferentIssuer => caller.issuer = Principal::Delegate,
            GrantCase::DifferentParent => caller.parent = Some(29),
            GrantCase::AuthorityRotated | GrantCase::BrokerStale | GrantCase::BrokerStillStale => {
                broker.issuer_generation = 2;
            }
            GrantCase::BrokerRotated | GrantCase::Progress => {
                broker.issuer_generation = 2;
                caller.issuer_generation = 2;
            }
            GrantCase::Permitted
            | GrantCase::AuthorityRevoked
            | GrantCase::AuthorityRestored
            | GrantCase::BrokerRevoked
            | GrantCase::BrokerRestored
            | GrantCase::AuthorityStale
            | GrantCase::AuthorityStillStale => {}
        }
        vec![broker, caller].into()
    }

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct DelegatedRequest {
        bombay: Metadata,
        tenant: Box<str>,
        kind: Purpose,
        operation: CounterOperation,
    }

    type Evidence = Vec<(ProtectedMessage, Rc<[Grant]>)>;

    fn grant_profile(profile: ProviderProfile) -> &'static str {
        match profile {
            ProviderProfile::Recorded => "delegated-record-test-v1",
            ProviderProfile::Scheduled => "delegated-scheduled-test-v1",
        }
    }

    fn authenticated(
        original: &ProtectedMessage,
        evidence: &Evidence,
        binding: Binding,
        profile: ProviderProfile,
    ) -> Result<(DelegatedRequest, Rc<[Grant]>), VerificationError> {
        let request: DelegatedRequest =
            serde_json::from_slice(&original.protected).map_err(VerificationError::Declaration)?;
        let claims = &request.bombay;
        if claims.provider_profile.as_ref() != grant_profile(profile)
            || claims.deployment.as_ref() != binding.deployment
            || claims.caller.as_ref() != binding.caller
            || claims.controller.as_ref() != binding.controller
            || claims.actor.as_ref() != binding.actor
            || claims.host.as_ref() != binding.host
            || claims.protocol.as_ref() != binding.protocol
        {
            return Err(VerificationError::Invalid);
        }
        let Some((_, grants)) = evidence.iter().find(|(record, _)| {
            record.protected == original.protected && record.proof == original.proof
        }) else {
            return Err(VerificationError::Invalid);
        };
        Ok((request, Rc::clone(grants)))
    }

    mod recorded {
        use super::{
            Binding, DelegatedRequest, Evidence, Grant, ProtectedMessage, ProviderProfile, Rc,
            Ready, VerificationError, authenticated, ready,
        };

        pub(super) struct Receipt {
            request: DelegatedRequest,
            grants: Rc<[Grant]>,
        }

        pub(super) fn verify(
            original: &ProtectedMessage,
            evidence: &Evidence,
            binding: Binding,
        ) -> Ready<Result<Receipt, VerificationError>> {
            ready(
                authenticated(original, evidence, binding, ProviderProfile::Recorded)
                    .map(|(request, grants)| Receipt { request, grants }),
            )
        }

        pub(super) fn projection(receipt: &Receipt) -> (&DelegatedRequest, &[Grant]) {
            (&receipt.request, &receipt.grants)
        }

        pub(super) fn same_request(left: &Receipt, right: &Receipt) -> bool {
            left.request.bombay == right.request.bombay
                && left.request.tenant == right.request.tenant
                && left.request.kind == right.request.kind
                && left.request.operation == right.request.operation
        }
    }

    mod scheduled {
        use super::{
            Binding, DelegatedRequest, Evidence, Grant, ProtectedMessage, ProviderProfile, Rc,
            VerificationError, authenticated, oneshot,
        };

        pub(super) struct Receipt {
            request: DelegatedRequest,
            grants: Rc<[Grant]>,
        }

        pub(super) async fn verify(
            original: &ProtectedMessage,
            evidence: &Evidence,
            binding: Binding,
        ) -> Result<Receipt, VerificationError> {
            let (published, received) = oneshot::channel();
            let verified = authenticated(original, evidence, binding, ProviderProfile::Scheduled)
                .map(|(request, grants)| Receipt { request, grants });
            let transfer = published.send(verified);
            match transfer {
                Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
                Err(original) => original,
            }
        }

        pub(super) fn projection(receipt: &Receipt) -> (&DelegatedRequest, &[Grant]) {
            (&receipt.request, &receipt.grants)
        }

        pub(super) fn same_request(left: &Receipt, right: &Receipt) -> bool {
            left.request.bombay == right.request.bombay
                && left.request.tenant == right.request.tenant
                && left.request.kind == right.request.kind
                && left.request.operation == right.request.operation
        }
    }

    struct Authority {
        principal: Principal,
        generation: u64,
        permission: Permission,
    }

    #[derive(Debug)]
    enum GrantRefusal {
        Borrow(BorrowError),
        Depth,
        Issuer,
        Parent,
        Scope { edge: Edge, dimension: Dimension },
        Revoked(Principal),
        Generation(Principal),
        Binding,
        Tenant,
        Request,
    }

    fn subset(child: &GrantScope, parent: &GrantScope) -> Option<Dimension> {
        if child
            .tenants
            .iter()
            .any(|tenant| !parent.tenants.contains(tenant))
        {
            Some(Dimension::Tenant)
        } else if child
            .protocols
            .iter()
            .any(|protocol| !parent.protocols.contains(protocol))
        {
            Some(Dimension::Protocol)
        } else if child
            .actors
            .iter()
            .any(|actor| !parent.actors.contains(actor))
        {
            Some(Dimension::Actor)
        } else if child
            .operations
            .iter()
            .any(|operation| !parent.operations.contains(operation))
        {
            Some(Dimension::Operation)
        } else {
            None
        }
    }

    fn configured_scope(claims: &Metadata, configured: &Metadata) -> bool {
        claims.wire_version == configured.wire_version
            && claims.schema_version == configured.schema_version
            && claims.provider_profile == configured.provider_profile
            && claims.deployment == configured.deployment
            && claims.caller == configured.caller
            && claims.caller_session == configured.caller_session
            && claims.controller == configured.controller
            && claims.actor == configured.actor
            && claims.host == configured.host
            && claims.protocol == configured.protocol
            && claims.runtime_generation == configured.runtime_generation
            && claims.actor_generation == configured.actor_generation
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the retained current-authority guard compares independent authenticated facts, configured export, actual target and deadline without a new policy wrapper"
    )]
    fn permitted<'authority, Receipt>(
        authorities: &'authority RefCell<Vec<Authority>>,
        receipt: &Receipt,
        projection: fn(&Receipt) -> (&DelegatedRequest, &[Grant]),
        configured: &Metadata,
        root_scope: &GrantScope,
        runtime: &str,
        target: MailAddr,
        deadline: Instant,
    ) -> Result<Ref<'authority, Vec<Authority>>, GrantRefusal> {
        let authority = authorities.try_borrow().map_err(GrantRefusal::Borrow)?;
        let (request, grants) = projection(receipt);
        if grants.len() != 2 {
            return Err(GrantRefusal::Depth);
        }
        let mut parent: Option<&Grant> = None;
        for grant in grants {
            let (scope, edge) = match parent {
                None => {
                    if grant.issuer != Principal::Authority {
                        return Err(GrantRefusal::Issuer);
                    }
                    if grant.parent.is_some() {
                        return Err(GrantRefusal::Parent);
                    }
                    (root_scope, Edge::Authority)
                }
                Some(parent) => {
                    if grant.issuer != parent.subject {
                        return Err(GrantRefusal::Issuer);
                    }
                    if grant.parent != Some(parent.id) {
                        return Err(GrantRefusal::Parent);
                    }
                    (&parent.scope, Edge::Delegate)
                }
            };
            if let Some(dimension) = subset(&grant.scope, scope) {
                return Err(GrantRefusal::Scope { edge, dimension });
            }
            let Some(current) = authority
                .iter()
                .find(|current| current.principal == grant.issuer)
            else {
                return Err(GrantRefusal::Issuer);
            };
            match current.permission {
                Permission::Revoked => return Err(GrantRefusal::Revoked(grant.issuer)),
                Permission::Granted => {}
            }
            if grant.issuer_generation != current.generation {
                return Err(GrantRefusal::Generation(grant.issuer));
            }
            parent = Some(grant);
        }
        let leaf = parent.expect("a nonempty bounded chain retains its actual final grant");
        if !configured_scope(&request.bombay, configured)
            || request.bombay.runtime_generation.as_ref() != runtime
            || request.bombay.actor_generation != target.0
            || request.bombay.caller.as_ref() != leaf.subject.name()
            || request.bombay.grant_issuer.as_ref() != leaf.issuer.name()
            || request.bombay.authority_generation != leaf.issuer_generation
        {
            return Err(GrantRefusal::Binding);
        }
        // This application assigns shop independently of deployment/controller
        // to precisely the capability certified by the Node binder above.
        if request.tenant.as_ref() != "shop" || !leaf.scope.tenants.contains(&"shop") {
            return Err(GrantRefusal::Tenant);
        }
        if request.kind != Purpose::Request
            || request.operation != CounterOperation::Read
            || !leaf
                .scope
                .protocols
                .contains(&request.bombay.protocol.as_ref())
            || !leaf.scope.actors.contains(&(
                request.bombay.controller.as_ref(),
                request.bombay.actor.as_ref(),
            ))
            || !leaf.scope.operations.contains(&GrantOperation::Read)
            || Instant::now() >= deadline
        {
            return Err(GrantRefusal::Request);
        }
        Ok(authority)
    }

    #[expect(
        clippy::result_large_err,
        clippy::too_many_lines,
        clippy::await_holding_refcell_ref,
        reason = "one finite real Application journey preserves both provider receipts and all original requests; authenticated evidence remains immutable across its scheduled provider wait"
    )]
    async fn delegated_permissions<Receipt>(
        profile: ProviderProfile,
        verify: impl AsyncFn(
            &ProtectedMessage,
            &Evidence,
            Binding,
        ) -> Result<Receipt, VerificationError>,
        projection: fn(&Receipt) -> (&DelegatedRequest, &[Grant]),
        same_request: fn(&Receipt, &Receipt) -> bool,
    ) {
        let binding = Binding {
            deployment: "two-node-read-study",
            caller: "caller",
            controller: "owner",
            actor: "counter",
            host: "receiver",
            protocol: "counter",
            target_assignment: "delegated-shop-counter",
            logical_session: 47,
        };
        let authorities = RefCell::new(vec![
            Authority {
                principal: Principal::Authority,
                generation: 1,
                permission: Permission::Granted,
            },
            Authority {
                principal: Principal::Broker,
                generation: 1,
                permission: Permission::Granted,
            },
            Authority {
                principal: Principal::Delegate,
                generation: 1,
                permission: Permission::Granted,
            },
        ]);
        let root_scope = GrantScope::authority();
        let evidence = RefCell::new(Evidence::new());
        let current_claims = RefCell::new(None::<Metadata>);
        let before = RefCell::new(Vec::new());
        let decoded = RefCell::new(Vec::new());
        let constructed = RefCell::new(Vec::new());
        let deadline = Instant::now() + Duration::from_secs(150);
        // Every Query may retain its native timeout charge until final cleanup.
        // Reserve the entire exact 26-query inventory at every preceding owner.
        let maximum = NonZeroUsize::new(CASES.len()).unwrap();
        let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
            let configured = current_claims.borrow();
            permitted(
                &authorities,
                receipt,
                projection,
                configured
                    .as_ref()
                    .expect("the actual target binding precedes serving"),
                &root_scope,
                runtime,
                target,
                deadline,
            )
        };
        let export = Export::new(
            "bombay/permission/delegated-counter",
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes| {
                let request: DelegatedRequest = serde_json::from_slice(bytes)?;
                decoded.borrow_mut().push(request.bombay.request_sequence);
                Ok::<_, serde_json::Error>(request)
            },
            reply(
                |request: DelegatedRequest| {
                    let constructed = &constructed;
                    Ok::<_, DelegatedRequest>(move |reply_to| {
                        constructed
                            .borrow_mut()
                            .push(request.bombay.request_sequence);
                        CounterMessage::Read(reply_to)
                    })
                },
                |_: &Receipt, original: &User<MailAddr, u64>| {
                    serde_json::to_vec(&original.message).map(Vec::into_boxed_slice)
                },
            ),
            async |bytes: &[u8], proof: &[u8]| {
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                let verified = verify(&original, &evidence.borrow(), binding).await;
                if let Ok(receipt) = &verified {
                    let (request, chain) = projection(receipt);
                    before.borrow_mut().push((
                        bytes.as_ptr(),
                        proof.as_ptr(),
                        serde_json::to_vec(request).unwrap(),
                        chain.as_ptr(),
                    ));
                }
                verified
            },
            async |_: &[u8], _: &Receipt| Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17])),
            same_request,
            &current,
            &current,
        );
        let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
        network.max_retained_queries = maximum;
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(network)
            .await
            .unwrap_or_else(|_| panic!("the actual tenant Counter Node opens"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let (configured, receiving) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let address = application.actor_address(&target).unwrap();
            let mut configured_claims =
                metadata(profile, "counter", runtime.clone(), runtime.clone());
            configured_claims.provider_profile = grant_profile(profile).into();
            configured_claims.actor_generation = address.0;
            let mut originals = Vec::new();
            let original_grants = grants(GrantCase::Permitted);
            let rotated_authority = grants(GrantCase::AuthorityRotated);
            let rotated_broker = grants(GrantCase::BrokerRotated);
            for (position, case) in CASES.iter().copied().enumerate() {
                let chain = match case {
                    GrantCase::Permitted
                    | GrantCase::AuthorityRevoked
                    | GrantCase::AuthorityRestored
                    | GrantCase::BrokerRevoked
                    | GrantCase::BrokerRestored
                    | GrantCase::AuthorityStale
                    | GrantCase::AuthorityStillStale => Rc::clone(&original_grants),
                    GrantCase::AuthorityRotated
                    | GrantCase::BrokerStale
                    | GrantCase::BrokerStillStale => Rc::clone(&rotated_authority),
                    GrantCase::BrokerRotated | GrantCase::Progress => Rc::clone(&rotated_broker),
                    _ => grants(case),
                };
                let leaf = chain.last().unwrap();
                let mut claims = metadata(profile, "counter", runtime.clone(), runtime.clone());
                claims.provider_profile = grant_profile(profile).into();
                claims.actor_generation = address.0;
                claims.request_sequence = u64::try_from(position).unwrap() + 1;
                claims.grant_issuer = leaf.issuer.name().into();
                claims.authority_generation = leaf.issuer_generation;
                let original = ProtectedMessage {
                    protected: serde_json::to_vec(&DelegatedRequest {
                        bombay: claims,
                        tenant: "shop".into(),
                        kind: Purpose::Request,
                        operation: CounterOperation::Read,
                    })
                    .unwrap()
                    .into_boxed_slice(),
                    proof: vec![7, u8::try_from(position).unwrap(), 11].into_boxed_slice(),
                };
                evidence.borrow_mut().push((
                    ProtectedMessage {
                        protected: original.protected.clone(),
                        proof: original.proof.clone(),
                    },
                    chain,
                ));
                originals.push(original);
            }
            *current_claims.borrow_mut() = Some(configured_claims);
            let delivered = configured.send(originals);
            delivered
                .unwrap_or_else(|_| panic!("Work receives the exact finite request inventory"));
            Ok::<_, Never>(target)
        });
        let outcome = node
            .run_with(|application| {
                let session = &session;
                let authorities = &authorities;
                async move {
                    let originals = receiving
                        .await
                        .expect("binding supplies all protected requests");
                    let mut replies = Vec::new();
                    for (case, original) in CASES.iter().zip(&originals) {
                        match case {
                            GrantCase::AuthorityRevoked
                            | GrantCase::AuthorityRestored
                            | GrantCase::BrokerRevoked
                            | GrantCase::BrokerRestored
                            | GrantCase::AuthorityStale
                            | GrantCase::BrokerStale => {
                                let mut current = authorities.borrow_mut();
                                match case {
                                    GrantCase::AuthorityRevoked => {
                                        current[0].permission = Permission::Revoked;
                                    }
                                    GrantCase::AuthorityRestored => {
                                        current[0].permission = Permission::Granted;
                                    }
                                    GrantCase::BrokerRevoked => {
                                        current[1].permission = Permission::Revoked;
                                    }
                                    GrantCase::BrokerRestored => {
                                        current[1].permission = Permission::Granted;
                                    }
                                    GrantCase::AuthorityStale => current[0].generation = 2,
                                    GrantCase::BrokerStale => current[1].generation = 2,
                                    _ => {
                                        unreachable!("only an authority update enters this branch")
                                    }
                                }
                            }
                            _ => {}
                        }
                        replies.push(
                            permission_replies(
                                session,
                                "bombay/permission/delegated-counter",
                                original,
                            )
                            .await,
                        );
                    }
                    let shutdown = application.shutdown();
                    (originals, replies, shutdown)
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the real tenant Application enters its host"));
        let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            node.into_parts()
                .unwrap_or_else(|_| panic!("all original Node finalization remains"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let ((originals, replies, shutdown), cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the actual Work finishes before native cleanup"));
        cleanup.expect("actual Application cleanup joins");
        closed.expect("actual Session disposal completes");
        let (origin, native) = native.expect("the complete original native Counter remains");
        let _counter = completed_actor(origin, native);
        let report = report.expect("the independent root report remains");
        let notifications = notifications.expect("both original publication receipts remain");
        let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(
            matches!(bound, Some(Ok(_)))
                && matches!(startup, Some(Ok(())))
                && matches!(withdrawal, Some(Ok(())))
        );
        assert!(shutdown.is_ok() && diagnostics.is_empty());
        assert!(
            polling.is_none()
                && disposal.is_none()
                && withdrawal_polling.is_none()
                && withdrawal_disposal.is_none()
                && session.is_closed()
        );
        let mut rows = requests
            .into_iter()
            .map(|request| {
                request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("each original request releases its producer loans"))
            })
            .collect::<Vec<_>>();
        // Every real Query and native cleanup have completed before this
        // complete trace is compared. A combined source inversion therefore
        // exposes every reached scope edge, rather than stopping at its first.
        let observed = rows
            .iter()
            .zip(CASES)
            .map(|(row, case)| {
                let native = match &row.4 {
                    None => NativeObservation::Absent,
                    Some((_, _, Attempt::Returned(Ok(())), Some(Some(original)), _)) => {
                        NativeObservation::Replied {
                            from: original.from,
                            value: original.message,
                        }
                    }
                    Some(_) => NativeObservation::SelectedWithoutReply,
                };
                (case, native)
            })
            .collect::<Vec<_>>();
        let expected = CASES
            .into_iter()
            .map(|case| {
                let native = match case {
                    GrantCase::Permitted
                    | GrantCase::AuthorityRestored
                    | GrantCase::BrokerRestored
                    | GrantCase::AuthorityRotated
                    | GrantCase::BrokerRotated
                    | GrantCase::Progress => NativeObservation::Replied {
                        from: origin.address(),
                        value: 0,
                    },
                    _ => NativeObservation::Absent,
                };
                (case, native)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            observed, expected,
            "all actual delegated native admissions and replies"
        );
        assert_eq!(rows.len(), CASES.len());
        assert_eq!(replies.len(), CASES.len());
        let before = before.into_inner();
        assert_eq!(before.len(), CASES.len());
        let mut admitted = Vec::new();
        for (position, (((row, case), original), replies)) in rows
            .iter_mut()
            .zip(CASES)
            .zip(&originals)
            .zip(&replies)
            .enumerate()
        {
            let receipt = row
                .2
                .as_ref()
                .expect("verification completed")
                .as_ref()
                .unwrap_or_else(|_| panic!("all fixture evidence is authentic"));
            let (request, chain) = projection(receipt);
            assert_eq!(row.1.0, original.protected);
            assert_eq!(row.1.1, original.proof);
            assert_eq!(
                (row.1.0.as_ptr(), row.1.1.as_ptr()),
                (before[position].0, before[position].1)
            );
            assert_eq!(serde_json::to_vec(request).unwrap(), before[position].2);
            assert_eq!(chain.as_ptr(), before[position].3);
            assert_eq!(request.tenant.as_ref(), "shop");
            assert_eq!(request.bombay.deployment.as_ref(), binding.deployment);
            assert_eq!(request.bombay.controller.as_ref(), binding.controller);
            assert_eq!(
                request.bombay.request_sequence,
                u64::try_from(position).unwrap() + 1
            );
            assert!(row.8.1.is_none() && row.8.2.is_none());
            assert!(matches!(row.9, (None, None, None, None, None, None)));
            match case {
                GrantCase::Permitted
                | GrantCase::AuthorityRestored
                | GrantCase::BrokerRestored
                | GrantCase::AuthorityRotated
                | GrantCase::BrokerRotated
                | GrantCase::Progress => {
                    let (constructor, client, admission, received, tail) = row
                        .4
                        .take()
                        .expect("a permitted delegated grant reaches the actual native request");
                    assert!(
                        constructor.is_none() && matches!(admission, Attempt::Returned(Ok(())))
                    );
                    let Attempt::Returned(Ok(client)) = client else {
                        panic!("the actual native reply client remains");
                    };
                    let original = received
                        .expect("native reception returned")
                        .expect("the actual Counter reply remains");
                    assert_eq!((original.from, original.message), (origin.address(), 0));
                    assert_ne!(client.address(), origin.address());
                    assert!(matches!(tail.as_slice(), [None]));
                    assert!(row.8.0.is_none());
                    assert_eq!(replies.len(), 1);
                    assert!(matches!(row.5, Some(Ok(_))) && matches!(row.6, Some(Ok(_))));
                    admitted.push(request.bombay.request_sequence);
                }
                _ => {
                    // This independent native absence oracle must fail before
                    // an error-tag assertion when a grant check is removed.
                    assert!(row.4.is_none(), "{case:?} reached native admission/reply");
                    assert!(row.3.is_none() && row.5.is_none() && row.6.is_none());
                    assert!(replies.is_empty() && row.7.is_empty());
                    let refusal = row
                        .8
                        .0
                        .as_ref()
                        .expect("the exact authorization refusal remains");
                    if let GrantRefusal::Borrow(cause) = refusal {
                        panic!("no concurrent authority loan is part of {case:?}: {cause}");
                    }
                    match case {
                        GrantCase::AuthorityScope(dimension) => assert!(matches!(refusal,
                            GrantRefusal::Scope { edge: Edge::Authority, dimension: actual }
                                if *actual == dimension)),
                        GrantCase::DelegatedScope(dimension) => assert!(matches!(refusal,
                            GrantRefusal::Scope { edge: Edge::Delegate, dimension: actual }
                                if *actual == dimension)),
                        GrantCase::TooDeep | GrantCase::Direct => {
                            assert!(matches!(refusal, GrantRefusal::Depth));
                        }
                        GrantCase::AuthorityIssuer | GrantCase::DifferentIssuer => {
                            assert!(matches!(refusal, GrantRefusal::Issuer));
                        }
                        GrantCase::AuthorityParent | GrantCase::DifferentParent => {
                            assert!(matches!(refusal, GrantRefusal::Parent));
                        }
                        GrantCase::AuthorityRevoked => assert!(matches!(
                            refusal,
                            GrantRefusal::Revoked(Principal::Authority)
                        )),
                        GrantCase::BrokerRevoked => {
                            assert!(matches!(refusal, GrantRefusal::Revoked(Principal::Broker)));
                        }
                        GrantCase::AuthorityStale | GrantCase::AuthorityStillStale => assert!(
                            matches!(refusal, GrantRefusal::Generation(Principal::Authority))
                        ),
                        GrantCase::BrokerStale | GrantCase::BrokerStillStale => assert!(matches!(
                            refusal,
                            GrantRefusal::Generation(Principal::Broker)
                        )),
                        _ => unreachable!("all permitted cases consumed actual native replies"),
                    }
                }
            }
            assert_eq!(row.7.len(), replies.len());
            assert!(row.7.iter().all(Result::is_ok));
            for reply in replies {
                let result = reply.result();
                let reply = result
                    .as_ref()
                    .expect("the actual protected reply is successful");
                assert_eq!(reply.payload().to_bytes().as_ref(), b"0");
                assert_eq!(reply.attachment().unwrap().to_bytes().as_ref(), [13, 17]);
            }
        }
        assert_eq!(admitted, [1, 17, 19, 21, 24, 26]);
        let decoded = decoded.into_inner();
        let constructed = constructed.into_inner();
        assert_eq!(decoded, admitted);
        assert_eq!(constructed, admitted);
    }

    #[test]
    fn recorded_delegation_preserves_both_scope_edges_and_current_authority() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(150),
                delegated_permissions(
                    ProviderProfile::Recorded,
                    recorded::verify,
                    recorded::projection,
                    recorded::same_request,
                ),
            )
            .await
            .expect("all recorded grants, native replies and joined cleanup finish");
        });
    }

    #[test]
    fn scheduled_delegation_preserves_both_scope_edges_and_current_authority() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(150),
                delegated_permissions(
                    ProviderProfile::Scheduled,
                    scheduled::verify,
                    scheduled::projection,
                    scheduled::same_request,
                ),
            )
            .await
            .expect("all scheduled grants, native replies and joined cleanup finish");
        });
    }
}
