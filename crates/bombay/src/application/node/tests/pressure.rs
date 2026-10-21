//! Cold admission retains native Full through capacity and authority changes.
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::{LedgerCommand, MailLedger, Receipts};
use crate::application::{Application, MailboxCapacity};
use crate::remote::{Attempt, Export, ExportLimits, Metadata, submission};
use crate::{ActorRetirement, Completion, MailAddr, TrySendError};
use behavior::Never;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
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
    Scope(VerificationError),
}

#[derive(Debug)]
enum Refusal {
    Scope,
    Native,
    Capacity,
    Json(serde_json::Error),
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
    let (export, bound, closed) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the real Node acquired cleanup and close"));
    let (work, native, notifications, ()) = outcome.into_parts();
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
    let ActorRetirement::Completed { custody, .. } = native else {
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
    let allocations = allocations.borrow();
    assert_eq!(allocations.len(), 1);
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
            assert_eq!(accepted.as_ptr(), allocations[0]);
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
            assert_eq!(original.as_ptr(), allocations[0]);
            assert_eq!(original.as_ref(), [107, 109]);
            assert!(encoded.is_none() && protected.is_none() && published.is_empty());
            assert!(publications.is_empty());
        }
    }
    assert_eq!(recorded::projection(&receipt).operation, expected);
}
