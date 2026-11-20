//! Cold admission retains native Full through capacity and authority changes.
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::{LedgerCommand, MailLedger, Receipts};
use crate::RetirementOutcome;
use crate::application::{Application, MailboxCapacity};
use crate::remote::{
    Attempt, Export, ExportError, ExportLimits, Metadata, RefusalRetention, submission,
};
use crate::{
    ActorFailureAssessment, ActorRetirement, Completion, MailAddr, RetirementAssessment,
    TrySendError,
};
use behavior::Never;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::mpsc::channel;
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Append(Box<[u8]>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Permission {
    Granted,
    Revoked,
}

#[derive(Debug, PartialEq, Eq)]
enum AdmissionObservation {
    EncodingEntered,
    PermissionRefused,
}

#[derive(Debug)]
enum PermissionRefusal {
    Revoked,
    #[expect(
        dead_code,
        reason = "retain the actual typed scope refusal independently of the selected revocation trace"
    )]
    Scope(VerificationError),
}

#[derive(Debug)]
enum Refusal {
    Scope,
    Native,
    Capacity,
    #[expect(
        dead_code,
        reason = "preserve the original JSON failure rather than replacing an unselected typed cause"
    )]
    Json(serde_json::Error),
}

#[test]
fn held_verifiers_bound_excess_requests_before_recovering_capacity() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the existing native test host opens");
    runtime.block_on(async {
        timeout(Duration::from_secs(40), verifier_capacity())
            .await
            .expect("a watchdog supplies no capacity, progress or cleanup evidence");
    });
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one finite native trace retains complete input, admission and cleanup facts; its registered proof table is immutable throughout provider verification"
)]
async fn verifier_capacity() {
    let registered = RefCell::new(Vec::<ProtectedMessage>::new());
    let verified = RefCell::new(Vec::<ProtectedMessage>::new());
    let claims = RefCell::new(None::<Metadata>);
    let active = Cell::new(0);
    let peak = Cell::new(0);
    let invalid = Cell::new(0);
    let release = Semaphore::new(0);
    let (entered, mut entries) = channel(2);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "ledger",
        host: "receiver",
        protocol: "ledger",
        target_assignment: "ledger",
        logical_session: 1,
    };
    let current = |receipt: &recorded::Receipt<Append>, runtime: &str, target: MailAddr| {
        let scope = recorded::projection(receipt);
        let actual = claims.borrow();
        let actual = actual
            .as_ref()
            .expect("the certified target supplied its metadata");
        if scope.kind == Purpose::Request
            && scope.bombay.runtime_generation.as_ref() == runtime
            && scope.bombay.actor_generation == target.0
            && scope.bombay.caller_session == actual.caller_session
            && scope.bombay.authority_generation == actual.authority_generation
        {
            Ok(())
        } else {
            Err(Refusal::Scope)
        }
    };
    let maximum = NonZeroUsize::new(4).unwrap();
    let export = Export::new(
        "bombay/verifier-capacity",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: NonZeroUsize::new(2).unwrap(),
            diagnostics: maximum,
            refusals: RefusalRetention::Discharge,
        },
        limits(),
        |bytes| {
            let request: ReadRequest<Append> =
                serde_json::from_slice(bytes).map_err(Refusal::Json)?;
            if request.kind != Purpose::Request {
                return Err(Refusal::Scope);
            }
            Ok(request.operation)
        },
        submission(
            |Append(original)| Ok::<_, Append>(move || LedgerCommand::Append(original)),
            |receipt: &recorded::Receipt<Append>,
             native: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                if !matches!(native, Attempt::Returned(Ok(()))) {
                    return Err(Refusal::Native);
                }
                let scope = recorded::projection(receipt);
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scope.bombay,
                    kind: Purpose::Admitted,
                    operation: &scope.operation,
                    value: &(),
                })
                .map(Vec::into_boxed_slice)
                .map_err(Refusal::Json)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let position = verified.borrow().len();
            verified.borrow_mut().push(ProtectedMessage {
                protected: original.protected.clone(),
                proof: original.proof.clone(),
            });
            active.set(active.get() + 1);
            peak.set(peak.get().max(active.get()));
            if position < 2 {
                let published = entered.send(position).await;
                published.expect("Work observes each actual held provider");
                let permit = release.acquire().await.expect("Work owns the release");
                permit.forget();
            }
            let result = recorded::verify::<Append>(&original, &registered.borrow(), binding).await;
            if matches!(&result, Err(VerificationError::Invalid)) {
                invalid.set(invalid.get() + 1);
            }
            active.set(active.get() - 1);
            result
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<Append>| {
            let reply: ReadReply<(), Append> =
                serde_json::from_slice(bytes).map_err(Refusal::Json)?;
            let scope = recorded::projection(receipt);
            if reply.bombay != scope.bombay
                || reply.operation != scope.operation
                || reply.kind != Purpose::Admitted
            {
                return Err(Refusal::Scope);
            }
            Ok(Box::<[u8]>::from([127, 131]))
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
    network.max_retained_queries = maximum;
    let node = Application::new(MailLedger {
        entries: Vec::new(),
    })
    .open_node(network)
    .await
    .unwrap_or_else(|_| panic!("the actual bounded-verifier Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, inputs) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut actual = metadata(
            ProviderProfile::Recorded,
            "ledger",
            runtime.clone(),
            runtime,
        );
        actual.actor_generation = application.actor_address(&target).unwrap().0;
        let mut originals = Vec::new();
        for (sequence, payload) in (1..=4).zip([101_u8, 103, 109, 113]) {
            actual.request_sequence = sequence;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &Append(Box::from([payload])),
            })
            .unwrap()
            .into_boxed_slice();
            registered.borrow_mut().push(ProtectedMessage {
                protected: protected.clone(),
                proof: Box::from([137_u8, payload]),
            });
            let proof = match sequence {
                1 | 2 => Box::from([139_u8, payload]),
                _ => Box::from([137_u8, payload]),
            };
            originals.push(ProtectedMessage { protected, proof });
        }
        *claims.borrow_mut() = Some(actual);
        let published = configured.send(originals);
        published.unwrap_or_else(|_| panic!("Work awaits the four actual protected inputs"));
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let session = &session;
            let active = &active;
            let verified = &verified;
            let release = &release;
            async move {
                let originals = inputs.await.unwrap();
                let mut held = Vec::new();
                for original in &originals[..2] {
                    let incoming = session
                        .get("bombay/verifier-capacity")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(original.protected.to_vec())
                        .attachment(original.proof.to_vec())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(30))
                        .await
                        .expect("the held native query starts");
                    held.push(incoming);
                }
                let first = entries.recv().await;
                let second = entries.recv().await;
                let original = &originals[2];
                let overflow = session
                    .get("bombay/verifier-capacity")
                    .allowed_destination(Locality::SessionLocal)
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(30))
                    .await
                    .expect("the excess native query starts");
                let overflow = overflow.recv_async().await;
                let saturated = (active.get(), verified.borrow().len());
                release.add_permits(2);
                let mut refused = Vec::new();
                for incoming in held {
                    refused.push(incoming.recv_async().await);
                }
                let original = &originals[3];
                let permitted = session
                    .get("bombay/verifier-capacity")
                    .allowed_destination(Locality::SessionLocal)
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(30))
                    .await
                    .expect("the later permitted native query starts");
                let permitted_reply = permitted.recv_async().await;
                let permitted_end = permitted.recv_async().await;
                let mut ordinary = application
                    .client::<Receipts>()
                    .unwrap_or_else(|_| panic!("the independent Ledger client allocates"));
                let admission = ordinary
                    .send(LedgerCommand::Read(ordinary.recipient()))
                    .await;
                let reply = ordinary.receive().await;
                (
                    originals,
                    first,
                    second,
                    overflow,
                    saturated,
                    refused,
                    permitted_reply,
                    permitted_end,
                    admission,
                    reply,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the real Node execution and cleanup return"));
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("Node retains its actual close and export"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            originals,
            first,
            second,
            overflow,
            saturated,
            refused,
            permitted_reply,
            permitted_end,
            admission,
            reply,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("Work retains its complete native observations"));
    cleanup.expect("the Application cleanup task joins");
    closed.expect("the real Node Session closes");
    let (origin, native) = native.expect("the original Ledger retirement remains");
    let report = report.expect("the original root report remains");
    let notifications = notifications.expect("both notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(matches!(bound, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(session.is_closed() && admission.is_ok());
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Retained {
        custody,
        outcome: RetirementOutcome::Completed { .. },
    } = native
    else {
        panic!("the exact original Ledger state survives joined retirement");
    };
    assert_eq!((first, second), (Some(0), Some(1)));
    assert_eq!(
        saturated,
        (2, 2),
        "the excess Query cannot enter a third verifier"
    );
    assert_eq!((peak.get(), active.get()), (2, 0));
    assert_eq!(invalid.get(), 2);
    assert!(
        overflow.is_err(),
        "capacity refusal produces native EOF without a reply"
    );
    assert_eq!(refused.len(), 2);
    assert!(refused.iter().all(Result::is_err));
    let verifications = verified.borrow();
    assert_eq!(verifications.len(), 3);
    for expected in &originals[..2] {
        assert!(
            verifications[..2]
                .iter()
                .any(|actual| actual.protected == expected.protected
                    && actual.proof == expected.proof)
        );
    }
    assert_eq!(verifications[2].protected, originals[3].protected);
    assert_eq!(verifications[2].proof, originals[3].proof);
    assert!(matches!(
        diagnostics.as_slice(),
        [ExportError::RetentionCapacity]
    ));
    assert_eq!(custody.behavior.entries.len(), 1);
    assert_eq!(custody.behavior.entries[0].1.as_ref(), [113]);
    let read = reply.expect("the independent local Ledger Read returns");
    assert_eq!(read.from, origin.address());
    assert_eq!(read.message, [custody.behavior.entries[0].0]);
    let admitted = permitted_reply.expect("the later valid request actually replies");
    let result = admitted.result();
    let admitted = result.as_ref().expect("native admission publishes success");
    let proof = admitted
        .attachment()
        .expect("the native reply retains its actual proof")
        .to_bytes();
    assert_eq!(proof.as_ref(), [127, 131]);
    let bytes = admitted.payload().to_bytes();
    let admitted: ReadReply<(), Append> = serde_json::from_slice(&bytes).unwrap();
    let expected: ReadRequest<Append> = serde_json::from_slice(&originals[3].protected).unwrap();
    assert_eq!(admitted.bombay, expected.bombay);
    assert_eq!(admitted.operation, expected.operation);
    assert_eq!(admitted.kind, Purpose::Admitted);
    assert!(permitted_end.is_err());
    let [request] = <[_; 1]>::try_from(rows)
        .unwrap_or_else(|_| panic!("only the keyed admitted request remains retained"));
    let (key, raw, verification, input, native, encoded, protected, published, refusals, panics) =
        request
            .try_into_parts()
            .unwrap_or_else(|_| panic!("all request producer loans ended"));
    assert!(key.is_some());
    assert_eq!(raw.0, originals[3].protected);
    assert_eq!(raw.1, originals[3].proof);
    let receipt = verification.expect("the actual provider returned").unwrap();
    assert_eq!(recorded::projection(&receipt).bombay, expected.bombay);
    assert!(input.is_none() && matches!(refusals, (None, None, None)));
    assert!(matches!(panics, (None, None, None, None, None, None)));
    let (unsent, sender, admission) = native.expect("the one actual Append was attempted");
    assert!(unsent.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
    let Attempt::Returned(Ok(sender)) = sender else {
        panic!("the native sender remains");
    };
    assert_eq!(sender.address(), custody.behavior.entries[0].0);
    encoded
        .expect("the actual native result was encoded")
        .unwrap();
    let proof = protected.expect("the actual reply was protected").unwrap();
    assert_eq!(proof.as_ref(), [127, 131]);
    assert_eq!(published.len(), 1);
    assert!(published.iter().all(Result::is_ok));
}

#[test]
fn full_append_retries_the_same_message_after_capacity_returns() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                append_after_full(Permission::Granted),
            )
            .await
            .expect("the complete native admission and cleanup trace terminates");
        });
}

#[test]
fn revoked_full_append_keeps_the_original_without_admission() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                append_after_full(Permission::Revoked),
            )
            .await
            .expect("the complete native refusal and cleanup trace terminates");
        });
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one causal Full trace retains complete native custody; request and reply proof tables stay immutable through actual verification"
)]
async fn append_after_full(next_permission: Permission) {
    let requests = RefCell::new(Vec::<ProtectedMessage>::new());
    let replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let claims = RefCell::new(None::<Metadata>);
    let permission = RefCell::new(Permission::Granted);
    let allocations = RefCell::new(Vec::new());
    let constructors = Cell::new(0);
    let expected = Append(Box::from([107, 109]));
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "ledger",
        host: "receiver",
        protocol: "ledger",
        target_assignment: "ledger",
        logical_session: 1,
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let (entered, constructed) = oneshot::channel();
    let entered = RefCell::new(Some(entered));
    let (observed, observation) = oneshot::channel();
    let observed = RefCell::new(Some(observed));
    let current = |receipt: &recorded::Receipt<Append>, runtime: &str, target: MailAddr| {
        let scope = recorded::projection(receipt);
        let actual = claims.borrow();
        let actual = actual.as_ref().expect("actual binding supplied the scope");
        current_scope(scope, actual, &expected, Purpose::Request, deadline)
            .map_err(PermissionRefusal::Scope)?;
        if scope.bombay.runtime_generation.as_ref() != runtime
            || scope.bombay.actor_generation != target.0
        {
            return Err(PermissionRefusal::Scope(VerificationError::Invalid));
        }
        let loan = permission.borrow();
        match *loan {
            Permission::Granted => Ok(loan),
            Permission::Revoked => {
                if let Some(observed) = observed.borrow_mut().take() {
                    let published = observed.send(AdmissionObservation::PermissionRefused);
                    published.expect("the caller observes the actual permission refusal");
                }
                Err(PermissionRefusal::Revoked)
            }
        }
    };
    let export = Export::new(
        "bombay/full-append",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| {
            let request: ReadRequest<Append> =
                serde_json::from_slice(bytes).map_err(Refusal::Json)?;
            if request.kind != Purpose::Request {
                return Err(Refusal::Scope);
            }
            allocations.borrow_mut().push(request.operation.0.as_ptr());
            Ok(request.operation)
        },
        submission(
            |Append(original)| {
                let entered = &entered;
                let constructors = &constructors;
                Ok::<_, Append>(move || {
                    constructors.set(constructors.get() + 1);
                    if let Some(entered) = entered.borrow_mut().take() {
                        let published = entered.send(());
                        published.expect("the caller observes the actual constructor");
                    }
                    LedgerCommand::Append(original)
                })
            },
            |receipt: &recorded::Receipt<Append>,
             native: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                if let Some(observed) = observed.borrow_mut().take() {
                    let published = observed.send(AdmissionObservation::EncodingEntered);
                    published.expect("the caller observes actual encoder entry");
                }
                if !matches!(native, Attempt::Returned(Ok(()))) {
                    return Err(Refusal::Native);
                }
                let scope = recorded::projection(receipt);
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scope.bombay,
                    kind: Purpose::Admitted,
                    operation: &scope.operation,
                    value: &(),
                })
                .map(Vec::into_boxed_slice)
                .map_err(Refusal::Json)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            recorded::verify::<Append>(&original, &requests.borrow(), binding).await
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<Append>| {
            let reply: ReadReply<(), Append> =
                serde_json::from_slice(bytes).map_err(Refusal::Json)?;
            let scope = recorded::projection(receipt);
            if reply.bombay != scope.bombay
                || reply.operation != scope.operation
                || reply.kind != Purpose::Admitted
            {
                return Err(Refusal::Scope);
            }
            let mut replies = replies.borrow_mut();
            if replies.len() == maximum.get() {
                return Err(Refusal::Capacity);
            }
            let proof: Box<[u8]> = [13, 17].into();
            replies.push(ProtectedMessage {
                protected: bytes.into(),
                proof: proof.clone(),
            });
            Ok(proof)
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let node = Application::new(MailLedger {
        entries: Vec::new(),
    })
    .mailbox_capacity(MailboxCapacity::new(2).unwrap())
    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the actual bounded Ledger Node opens"));
    let runtime = node.runtime_id();
    // Deliberate owning-test access: one real local query on this exact Session.
    let session = node.session.clone();
    let (configured, configured_request) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut actual = metadata(
            ProviderProfile::Recorded,
            "ledger",
            runtime.clone(),
            runtime,
        );
        actual.actor_generation = application.actor_address(&target).unwrap().0;
        let protected = serde_json::to_vec(&ReadRequestRef {
            bombay: &actual,
            kind: Purpose::Request,
            operation: &expected,
        })
        .unwrap()
        .into_boxed_slice();
        let proof: Box<[u8]> = [7, 11].into();
        requests.borrow_mut().push(ProtectedMessage {
            protected: protected.clone(),
            proof: proof.clone(),
        });
        *claims.borrow_mut() = Some(actual);
        let published = configured.send(ProtectedMessage { protected, proof });
        published.unwrap_or_else(|_| panic!("the caller awaits the actual configured request"));
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let session = &session;
            let permission = &permission;
            async move {
                let request = configured_request.await.unwrap();
                let mut ordinary = application
                    .client::<Receipts>()
                    .unwrap_or_else(|_| panic!("the independent Ledger client allocates"));
                let (release_worker, released) = mpsc::channel::<()>();
                let (announced, worker_entered) = oneshot::channel();
                let worker = tokio::spawn(async move {
                    let announced = announced.send(());
                    let released = released.recv();
                    (announced, released)
                });
                let worker_entered = worker_entered.await;
                // No actor worker can drain either actual admitted prefix message.
                let prefix = (0..2)
                    .map(|_| ordinary.try_send(LedgerCommand::Append(Box::from([101]))))
                    .collect::<Vec<_>>();
                let full = ordinary.try_send(LedgerCommand::Append(Box::from([103])));
                let incoming = session
                    .get("bombay/full-append")
                    .allowed_destination(Locality::SessionLocal)
                    .payload(request.protected.to_vec())
                    .attachment(request.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(20))
                    .await
                    .expect("the actual native query starts");
                constructed
                    .await
                    .expect("the actual native constructor runs");
                *permission.borrow_mut() = next_permission;
                drop(release_worker);
                let worker = worker.await;
                // This observes entry/refusal, never certifies request completion.
                let observation = observation
                    .await
                    .expect("the finite admission path advances");
                let admission = ordinary
                    .send(LedgerCommand::Read(ordinary.recipient()))
                    .await;
                let reply = if admission.is_ok() {
                    ordinary.receive().await
                } else {
                    None
                };
                (
                    worker_entered,
                    worker,
                    prefix,
                    full,
                    observation,
                    admission,
                    reply,
                    incoming,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Node execution starts"));
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the real Node acquired cleanup and close"));
    let (work, root_report, native, notifications, ()) = outcome.into_parts();
    let ((worker_entered, worker, prefix, full, observation, admission, reply, incoming), cleanup) =
        work.try_into_completed()
            .unwrap_or_else(|_| panic!("the original caller output survives cleanup"));
    cleanup.expect("the Application cleanup task joins");
    closed.expect("the actual Node connection closes");
    let (_origin, native) = native.expect("the original Ledger retirement survives");
    let (_inputs, _declarations, startup, withdrawal, diagnostics, rows) = export.into_parts();
    bound.expect("actual actor binding acquired").unwrap();
    startup.expect("actual declaration attempted").unwrap();
    withdrawal.expect("actual declaration withdrawn").unwrap();
    let mut publications = Vec::new();
    while let Ok(Some(original)) = incoming.try_recv() {
        publications.push(original);
    }
    let notifications = notifications.expect("both native notification results survive");
    let root_report = root_report.expect("the joined root retains its actual report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(worker_entered.is_ok());
    let (announced, released) = worker.expect("the actual blocked worker joins");
    assert!(announced.is_ok() && released.is_err());
    assert_eq!(prefix.len(), 2);
    assert!(prefix.iter().all(Result::is_ok));
    let Err(TrySendError::Full(LedgerCommand::Append(original))) = full else {
        panic!("the independent actual mailbox was full before the query");
    };
    assert_eq!(original.as_ref(), [103]);
    assert!(admission.is_ok() && session.is_closed() && diagnostics.is_empty());
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Retained {
        custody,
        outcome: RetirementOutcome::Completed { .. },
    } = native
    else {
        panic!("complete original Ledger state survives retirement");
    };
    let reply = reply.expect("the independent terminal Ledger Read returns");
    let expected_count = match next_permission {
        Permission::Granted => 3,
        Permission::Revoked => 2,
    };
    assert_eq!(
        custody.behavior.entries.len(),
        expected_count,
        "a permitted Full is retried once; a revoked Full is never admitted"
    );
    assert_eq!(reply.message.len(), expected_count);
    assert_eq!(constructors.get(), 1);
    let allocation = {
        let allocations = allocations.borrow();
        assert_eq!(allocations.len(), 1);
        allocations[0]
    };
    let [request] = <[_; 1]>::try_from(rows)
        .unwrap_or_else(|_| panic!("exactly one original request is retained"));
    let (
        _,
        original,
        verified,
        decoded,
        native,
        encoded,
        protected,
        published,
        (refused, reply_refusal, refusal),
        panics,
    ) = request
        .try_into_parts()
        .unwrap_or_else(|_| panic!("joined execution released every producer loan"));
    let receipt = verified.expect("verification attempted").unwrap();
    assert_eq!(original.0, requests.borrow()[0].protected);
    assert_eq!(original.1, requests.borrow()[0].proof);
    assert!(decoded.is_none() && reply_refusal.is_none() && refusal.is_none());
    assert!(matches!(panics, (None, None, None, None, None, None)));
    let (unsent, sender, attempt) = native.expect("the actual Append was selected");
    assert!(unsent.is_none());
    let Attempt::Returned(Ok(sender)) = sender else {
        panic!("one original sender allocated");
    };
    match next_permission {
        Permission::Granted => {
            assert_eq!(observation, AdmissionObservation::EncodingEntered);
            assert!(refused.is_none() && matches!(attempt, Attempt::Returned(Ok(()))));
            let (origin, accepted) = &custody.behavior.entries[2];
            assert_eq!(*origin, sender.address());
            assert_eq!(accepted.as_ptr(), allocation);
            assert_eq!(accepted.as_ref(), [107, 109]);
            assert_eq!(reply.message[2], sender.address());
            encoded.expect("actual admitted encoding").unwrap();
            protected.expect("actual protection").unwrap();
            assert_eq!(published.len(), 1);
            assert!(published.iter().all(Result::is_ok));
            assert_eq!(publications.len(), 1);
            let actual = publications[0].result();
            let actual = actual.as_ref().expect("actual successful native reply");
            let bytes = actual.payload().to_bytes();
            let proof = actual.attachment().expect("actual proof").to_bytes();
            let original = ProtectedMessage {
                protected: bytes.as_ref().into(),
                proof: proof.as_ref().into(),
            };
            let validated = recorded::verify::<Append>(&original, &replies.borrow(), binding).await;
            validated.expect("the actual returned byte/proof pair validates");
        }
        Permission::Revoked => {
            assert_eq!(observation, AdmissionObservation::PermissionRefused);
            assert!(matches!(refused, Some(PermissionRefusal::Revoked)));
            let Attempt::Returned(Err(TrySendError::Full(LedgerCommand::Append(original)))) =
                attempt
            else {
                panic!("revocation preserves the actual original Full message");
            };
            assert_eq!(original.as_ptr(), allocation);
            assert_eq!(original.as_ref(), [107, 109]);
            assert!(encoded.is_none() && protected.is_none() && published.is_empty());
            assert!(publications.is_empty());
        }
    }
    assert_eq!(recorded::projection(&receipt).operation, expected);
}
