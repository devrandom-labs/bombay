//! Cold Node replay preserves real Task requests, protected replies and native custody.
//! Recorded/Scheduled are finite test providers, not shipping authentication.
use super::counter::CounterValue;
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Never, Protocol, User};
use bombay::lifecycle::{TaskResult, TaskState};
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{
    Attempt, Export, ExportError, ExportLimits, IngressError, Metadata, RequestKey, json, reply,
    try_retain, verify,
};
use bombay::{
    ActorNotificationReceipts, ActorRetirement, ActorRetirementReport, ApplicationCleanupError,
    ApplicationClient, RootOrigin, SendError, TrySendError,
};
use core::ops::AsyncFn;
use serde::{Serialize, de::DeserializeOwned};
use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::future::{Future, Ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use zenoh::handlers::FifoChannel;
use zenoh::query::{ConsolidationMode, Query, Reply};
use zenoh::{Error as ZenohError, Session};

#[path = "../../../../tests/application_support/mod.rs"]
mod application_support;
use super::protocol::*;
use application_support::assert_completed;

struct ProtectedReply<Operation, Receipt, Value> {
    issued: Result<(), ZenohError>,
    originals: Vec<(Reply, OwnedSemaphorePermit)>,
    publications: Arc<Mutex<Vec<Result<(), IngressError<Reply>>>>>,
    original: Option<ProtectedMessage>,
    shape: Option<Result<(), json::Error>>,
    declaration: Option<Result<ReadReply<Value, Operation>, serde_json::Error>>,
    verification: Option<Result<Receipt, VerificationError>>,
    scope: Option<Result<(), VerificationError>>,
}

struct ReadExchange<Operation, Receipt, Value> {
    current: Metadata,
    binding: Binding,
    caller: String,
    receiver: String,
    batch: Option<User<MailAddr, TaskResult<Vec<ProtectedMessage>>>>,
    allocations: Vec<(*const u8, *const u8)>,
    begun: Result<(), SendError<BatchCommand>>,
    replies: Vec<ProtectedReply<Operation, Receipt, Value>>,
    first_retirement: Option<ActorRetirementReport>,
    cleanup: Result<(), ApplicationCleanupError>,
    report: Option<ActorRetirementReport>,
    native: Result<
        (
            RootOrigin<SenderRoot>,
            ActorRetirement<SenderRoot, Never, Vec<Never>>,
        ),
        oneshot::error::RecvError,
    >,
    notifications: Result<ActorNotificationReceipts, oneshot::error::RecvError>,
    closed: Result<(), ZenohError>,
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one peer trace preserves actual typed Task, protected replies and all independent cleanup products"
)]
async fn read_queries<Operation, Receipt, Value, FirstRetirement>(
    address: &str,
    receiver: &str,
    protocol: &str,
    binding: Binding,
    operation: Operation,
    profile: ProviderProfile,
    expected: Value,
    records: &RefCell<Vec<ProtectedMessage>>,
    claims: &RefCell<Option<Metadata>>,
    mut first_retirement: Option<FirstRetirement>,
    provider: &impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
) -> ReadExchange<Operation, Receipt, Value>
where
    Operation: Copy + Eq + Serialize + DeserializeOwned,
    Value: Serialize + DeserializeOwned,
    FirstRetirement: Future<Output = ActorRetirementReport>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut sender = Application::new(RequestTask::new().stop_on_shutdown())
        .open_node(configuration(address, &EndpointRole::Caller))
        .await
        .unwrap_or_else(|_| panic!("the actual Task Node opens"));
    let connection = sender.session.clone();
    let caller = sender.runtime_id();
    let current = metadata(profile, protocol, caller.clone(), receiver.to_owned());
    *claims.borrow_mut() = Some(metadata(
        profile,
        protocol,
        caller.clone(),
        receiver.to_owned(),
    ));
    let request = serde_json::to_vec(&ReadRequestRef {
        bombay: &current,
        kind: Purpose::Request,
        operation,
    })
    .unwrap();
    let response = serde_json::to_vec(&ReadReplyRef {
        bombay: &current,
        kind: Purpose::ReadReply,
        operation,
        value: &expected,
    })
    .unwrap();
    records.borrow_mut().extend([
        ProtectedMessage {
            protected: request.clone().into_boxed_slice(),
            proof: Box::from([7, 11]),
        },
        ProtectedMessage {
            protected: response.into_boxed_slice(),
            proof: Box::from([13, 17]),
        },
    ]);
    let outcome = sender
        .run_with(|application| {
            let connection = &connection;
            let current = &current;
            async move {
                let querier = connection
                    .declare_querier("bombay/replay/read")
                    .await
                    .unwrap();
                let matching = querier
                    .matching_listener()
                    .with(FifoChannel::new(2))
                    .await
                    .unwrap();
                loop {
                    let status = matching
                        .recv_async()
                        .await
                        .expect("the real declaration is discovered");
                    if status.matching() {
                        break;
                    }
                }
                matching
                    .undeclare()
                    .await
                    .expect("the matching listener withdraws");
                querier
                    .undeclare()
                    .await
                    .expect("the setup querier withdraws");
                let originals = (0..2)
                    .map(|_| ProtectedMessage {
                        protected: request.clone().into_boxed_slice(),
                        proof: Box::from([7, 11]),
                    })
                    .collect::<Vec<_>>();
                let allocations = originals
                    .iter()
                    .map(|r| (r.protected.as_ptr(), r.proof.as_ptr()))
                    .collect();
                let mut client = application.external::<RequestBatch>().unwrap();
                let begun = client
                    .send(
                        &application.root().established_recipient(),
                        BatchCommand::Complete {
                            result: originals,
                            reply_to: client.recipient(),
                        },
                    )
                    .await;
                let batch = if begun.is_ok() {
                    client.receive().await
                } else {
                    None
                };
                let mut replies = Vec::new();
                let mut retired = None;
                if let Some(User {
                    message: TaskResult::Completed(originals),
                    ..
                }) = &batch
                {
                    for request in originals {
                        let capacity = Arc::new(Semaphore::new(2));
                        let (published, mut received) = mpsc::channel(2);
                        let publications = Arc::new(Mutex::new(Vec::new()));
                        let observed = Arc::clone(&publications);
                        let issued = connection
                            .get("bombay/replay/read")
                            .consolidation(ConsolidationMode::None)
                            .timeout(Duration::from_secs(5))
                            .payload(request.protected.to_vec())
                            .attachment(request.proof.to_vec())
                            .callback(move |reply| {
                                let original = try_retain(&published, &capacity, reply);
                                observed.lock().unwrap().push(original);
                            })
                            .await;
                        let mut acquired = ProtectedReply {
                            issued,
                            originals: Vec::new(),
                            publications,
                            original: None,
                            shape: None,
                            declaration: None,
                            verification: None,
                            scope: None,
                        };
                        while let Some(original) = received.recv().await {
                            acquired.originals.push(original);
                        }
                        if let Some((reply, _permit)) = acquired.originals.first()
                            && let Ok(sample) = reply.result()
                        {
                            acquired.original = Some(ProtectedMessage {
                                protected: sample
                                    .payload()
                                    .to_bytes()
                                    .into_owned()
                                    .into_boxed_slice(),
                                proof: sample
                                    .attachment()
                                    .map(|proof| proof.to_bytes().into_owned().into_boxed_slice())
                                    .unwrap_or_default(),
                            });
                            let original = acquired.original.as_ref().unwrap();
                            acquired.shape =
                                Some(json::check(&original.protected, &original.proof, &limits()));
                            if matches!(acquired.shape, Some(Ok(()))) {
                                acquired.declaration =
                                    Some(serde_json::from_slice(&original.protected));
                                let records = records.borrow();
                                verify(original, &mut acquired.verification, |r| {
                                    provider(r, &records, binding)
                                })
                                .await;
                                if let Some(Ok(receipt)) = &acquired.verification {
                                    acquired.scope = Some(current_scope(
                                        projection(receipt),
                                        current,
                                        &operation,
                                        Purpose::ReadReply,
                                        deadline,
                                    ));
                                }
                            }
                        }
                        if matches!(acquired.verification, Some(Ok(_)))
                            && retired.is_none()
                            && let Some(retirement) = first_retirement.take()
                        {
                            retired = Some(retirement.await);
                        }
                        replies.push(acquired);
                    }
                }
                (batch, allocations, begun, replies, retired, client)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Task Node enters its host"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((batch, allocations, begun, replies, first_retirement, client), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Task Work completes"));
    drop(client);
    let closed = sender.close().await;
    ReadExchange {
        current,
        binding,
        caller,
        receiver: receiver.to_owned(),
        batch,
        allocations,
        begun,
        replies,
        first_retirement,
        cleanup,
        report,
        native,
        notifications,
        closed,
    }
}

fn assert_exchange<Operation, Receipt, Value>(
    exchange: &ReadExchange<Operation, Receipt, Value>,
    expected: &Value,
    operation: Operation,
    identity: fn(&Receipt) -> (&Binding, u64),
    proof: fn(&Receipt) -> Option<&[u8]>,
) where
    Operation: Copy + Eq + Debug,
    Value: Eq + Debug,
{
    assert!(exchange.cleanup.is_ok() && exchange.closed.is_ok());
    assert_notifications(&exchange.notifications);
    let report = exchange.report.expect("the actual Task report is retained");
    assert_report(report);
    let (_, native) = exchange
        .native
        .as_ref()
        .expect("the original Task root is retained");
    let custody = native
        .custody()
        .expect("the complete original Task remains owned");
    assert_eq!(custody.behavior.base().state(), TaskState::Completed);
    assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty());
    assert!(exchange.begun.is_ok());
    assert_ne!(exchange.caller, exchange.receiver);
    assert_eq!(exchange.current.caller_session.as_ref(), exchange.caller);
    assert_eq!(
        exchange.current.runtime_generation.as_ref(),
        exchange.receiver
    );
    assert_eq!(
        exchange.current.actor_generation,
        MailAddr::APPLICATION_ROOT.0
    );
    let batch = exchange
        .batch
        .as_ref()
        .expect("the actual typed Task batch is received");
    assert_eq!(batch.from, MailAddr::APPLICATION_ROOT);
    let TaskResult::Completed(originals) = &batch.message else {
        panic!("the Task completes its original batch");
    };
    assert_eq!(originals.len(), 2);
    assert_eq!(exchange.allocations.len(), originals.len());
    for (original, allocation) in originals.iter().zip(&exchange.allocations) {
        assert_eq!(
            (original.protected.as_ptr(), original.proof.as_ptr()),
            *allocation
        );
    }
    assert_eq!(exchange.replies.len(), 2);
    for reply in &exchange.replies {
        let publications = reply.publications.lock().unwrap();
        assert!(reply.issued.is_ok());
        assert_eq!(publications.len(), 1);
        assert!(publications.iter().all(Result::is_ok));
        assert_eq!(reply.originals.len(), 1);
        assert!(matches!(reply.shape, Some(Ok(()))) && matches!(reply.scope, Some(Ok(()))));
        let original = reply
            .original
            .as_ref()
            .expect("actual protected reply bytes remain");
        let sample = reply.originals[0]
            .0
            .result()
            .expect("the actual Query reply succeeds");
        let payload = sample.payload().to_bytes();
        assert_eq!(original.protected.as_ref(), payload.as_ref());
        assert_eq!(sample.key_expr().as_str(), "bombay/replay/read");
        assert_eq!(original.proof.as_ref(), [13, 17]);
        let declared = reply.declaration.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(declared.bombay, exchange.current);
        assert_eq!(declared.kind, Purpose::ReadReply);
        assert_eq!(declared.operation, operation);
        assert_eq!(&declared.value, expected);
        assert_verification(reply.verification.as_ref());
        let receipt = reply.verification.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(
            identity(receipt),
            (&exchange.binding, exchange.current.request_sequence)
        );
        if let Some(actual) = proof(receipt) {
            assert_eq!(actual, [13, 17]);
        }
    }
}

fn assert_verification<Receipt>(verification: Option<&Result<Receipt, VerificationError>>) {
    match verification {
        Some(Ok(_)) => {}
        Some(Err(cause)) => panic!("original provider refusal: {cause:?}"),
        None => panic!("the actual provider result was not acquired"),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one actual Counter Node trace retains protected replies and original native outcomes before inspection"
)]
async fn read_counter<Receipt>(
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
    let address = reserved_address();
    let records = RefCell::new(Vec::new());
    let claims = RefCell::new(None::<Metadata>);
    let allocations = RefCell::new(Vec::new());
    let encoded = RefCell::new(Vec::new());
    let constructors = Cell::new(0);
    let decoded = Cell::new(0);
    let protected = Cell::new(0);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "counter-root-test-assignment",
        logical_session: 41,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let current = claims.borrow();
        let actual = current.as_ref().ok_or(VerificationError::Invalid)?;
        current_scope(
            projection(receipt),
            actual,
            &CounterOperation::Read,
            Purpose::Request,
            deadline,
        )?;
        if actual.runtime_generation.as_ref() != runtime || actual.actor_generation != target.0 {
            return Err(VerificationError::Invalid);
        }
        Ok(current)
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/replay/read",
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
                let constructors = &constructors;
                Ok::<_, CounterOperation>(move |reply_to| {
                    constructors.set(constructors.get() + 1);
                    CounterMessage::Read(reply_to)
                })
            },
            |receipt: &Receipt, original: &User<MailAddr, u64>| {
                encoded.borrow_mut().push((original.from, original.message));
                let scope = projection(receipt);
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &scope.bombay,
                    kind: Purpose::ReadReply,
                    operation: scope.operation,
                    value: &original.message,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            allocations
                .borrow_mut()
                .push((bytes.as_ptr(), proof.as_ptr()));
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            provider(&original, &records.borrow(), binding).await
        },
        async |_: &[u8], _: &Receipt| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        same_request,
        &current,
        &current,
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual receiving Counter Node opens"));
    let runtime = node.runtime_id();
    let mut node = node.exports(export, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| {
            let address = &address;
            let runtime = &runtime;
            let records = &records;
            let claims = &claims;
            let provider = &provider;
            async move {
                let sender = application.sender().unwrap();
                let primed = sender.send(CounterMessage::Increment).await;
                let exchange = read_queries(
                    address,
                    runtime,
                    "counter",
                    binding,
                    CounterOperation::Read,
                    profile,
                    1_u64,
                    records,
                    claims,
                    None::<Ready<ActorRetirementReport>>,
                    provider,
                    projection,
                )
                .await;
                let shutdown = application.shutdown();
                let retirement = application.lifecycle().retirement().await;
                (exchange, primed, shutdown, retirement)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the receiving Counter Node enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Counter Node finalizes"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((exchange, primed, shutdown, retirement), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Counter Work completes"));
    cleanup.expect("actual Counter cleanup joins");
    closed.expect("actual Counter connection closes");
    assert_notifications(&notifications);
    assert_report(retirement);
    assert_eq!(report, Some(retirement));
    assert!(primed.is_ok() && shutdown.is_ok() && polling.is_none() && disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert_exchange(&exchange, &1, CounterOperation::Read, identity, proof);
    assert!(exchange.first_retirement.is_none());
    let (origin, native) = native.expect("the complete Counter root remains owned");
    let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    let [first, second] =
        <[_; 2]>::try_from(rows).unwrap_or_else(|_| panic!("two actual requests remain retained"));
    let first = first
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the first request releases all producer loans"));
    let second = second
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the duplicate releases all producer loans"));
    assert!(first.0.is_some());
    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    let batch = exchange
        .batch
        .as_ref()
        .expect("the actual Task batch remains acquired");
    let TaskResult::Completed(originals) = &batch.message else {
        panic!("the actual Task completed its original request batch");
    };
    for (raw, original) in [&first.1, &second.1].into_iter().zip(originals) {
        assert_eq!(raw.0.as_ref(), original.protected.as_ref());
        assert_eq!(raw.1.as_ref(), original.proof.as_ref());
    }
    let allocations = allocations.borrow();
    assert_eq!(
        allocations.as_slice(),
        [
            (first.1.0.as_ptr(), first.1.1.as_ptr()),
            (second.1.0.as_ptr(), second.1.1.as_ptr())
        ]
    );
    for receipt in [&first.2, &second.2] {
        assert_verification(receipt.as_ref());
        let receipt = receipt.as_ref().unwrap().as_ref().unwrap();
        let scope = projection(receipt);
        assert_eq!(scope.bombay, exchange.current);
        assert_eq!(scope.kind, Purpose::Request);
        assert_eq!(scope.operation, CounterOperation::Read);
        assert_eq!(
            identity(receipt),
            (&binding, exchange.current.request_sequence)
        );
        if let Some(actual) = proof(receipt) {
            assert_eq!(actual, [7, 11]);
        }
    }
    let (constructor, client, admission, received, tail) =
        first.4.expect("the actual first native read remains");
    assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
    let client = match client {
        Attempt::Returned(Ok(client)) => client,
        _ => panic!("the original native reply client remains"),
    };
    let received = received
        .expect("the first native reception was acquired")
        .expect("the actual first User remains");
    assert_eq!((received.from, received.message), (origin.address(), 1));
    assert_ne!(client.address(), origin.address());
    assert!(matches!(tail.as_slice(), [None]));
    assert!(second.3.is_none() && second.4.is_none() && second.5.is_none() && second.6.is_none());
    assert_eq!(
        (decoded.get(), constructors.get(), protected.get()),
        (1, 1, 1)
    );
    let encoded = encoded.borrow();
    assert_eq!(encoded.as_slice(), [(origin.address(), 1)]);
    assert!(matches!(first.5, Some(Ok(_))) && matches!(first.6, Some(Ok(_))));
    assert_eq!((first.7.len(), second.7.len()), (1, 1));
    assert!(first.7.iter().all(Result::is_ok) && second.7.iter().all(Result::is_ok));
    assert!(matches!(first.8, (None, None, None)) && matches!(second.8, (None, None, None)));
    assert!(matches!(first.9, (None, None, None, None, None, None)));
    assert!(matches!(second.9, (None, None, None, None, None, None)));
    assert_completed((origin, native), None);
    assert_completed(exchange.native.unwrap(), None);
}

#[expect(
    clippy::too_many_lines,
    reason = "one terminating Ledger trace retains original append, native read and actual post-retirement replay"
)]
async fn read_ledger<Receipt>(
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
    let address = reserved_address();
    let records = RefCell::new(Vec::new());
    let claims = RefCell::new(None::<Metadata>);
    let allocations = RefCell::new(Vec::new());
    let encoded = RefCell::new(Vec::new());
    let constructors = Cell::new(0);
    let decoded = Cell::new(0);
    let protected = Cell::new(0);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "ledger",
        host: "receiver",
        protocol: "ledger",
        target_assignment: "ledger-root-test-assignment",
        logical_session: 41,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &Receipt, runtime: &str, target: MailAddr| {
        let current = claims.borrow();
        let actual = current.as_ref().ok_or(VerificationError::Invalid)?;
        current_scope(
            projection(receipt),
            actual,
            &LedgerOperation::Read,
            Purpose::Request,
            deadline,
        )?;
        if actual.runtime_generation.as_ref() != runtime || actual.actor_generation != target.0 {
            return Err(VerificationError::Invalid);
        }
        Ok(current)
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/replay/read",
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
                let constructors = &constructors;
                Ok::<_, LedgerOperation>(move |reply_to| {
                    constructors.set(constructors.get() + 1);
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
        async |bytes: &[u8], proof: &[u8]| {
            allocations
                .borrow_mut()
                .push((bytes.as_ptr(), proof.as_ptr()));
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            provider(&original, &records.borrow(), binding).await
        },
        async |_: &[u8], _: &Receipt| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
        },
        same_request,
        &current,
        &current,
    );
    let node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration(&address, &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the actual receiving Ledger Node opens"));
    let runtime = node.runtime_id();
    let mut node = node.exports(export, async |application| {
        Ok::<_, Never>(application.root().established_recipient())
    });
    let outcome = node
        .run_with(|application| {
            let address = &address;
            let runtime = &runtime;
            let records = &records;
            let claims = &claims;
            let provider = &provider;
            async move {
                let sender = application.sender().unwrap();
                let caller = sender.address();
                let original = Box::<[u8]>::from([59, 61]);
                let allocation = original.as_ptr();
                let primed = sender.send(LedgerCommand::Append(original)).await;
                let expected = vec![u64::from(caller)];
                let exchange = read_queries(
                    address,
                    runtime,
                    "ledger",
                    binding,
                    LedgerOperation::Read,
                    profile,
                    expected,
                    records,
                    claims,
                    Some(application.lifecycle().retirement()),
                    provider,
                    projection,
                )
                .await;
                (exchange, primed, caller, allocation)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the receiving Ledger Node enters its host"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Ledger Node finalizes"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((exchange, primed, caller, allocation), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original Ledger Work completes"));
    cleanup.expect("actual Ledger cleanup joins");
    closed.expect("actual Ledger connection closes");
    assert_notifications(&notifications);
    let retirement = exchange
        .first_retirement
        .expect("actual Ledger retirement precedes the duplicate");
    assert_report(retirement);
    assert_eq!(report, Some(retirement));
    assert!(primed.is_ok() && polling.is_none() && disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))));
    assert_exchange(
        &exchange,
        &vec![u64::from(caller)],
        LedgerOperation::Read,
        identity,
        proof,
    );
    let (origin, native) = native.expect("the complete Ledger root remains owned");
    let ledger = native.custody().unwrap().behavior.base();
    let [(sender, original)] = ledger.entries.as_slice() else {
        panic!("the original append remains in the Ledger");
    };
    assert_eq!(*sender, caller);
    assert_eq!(original.as_ptr(), allocation);
    assert_eq!(original.as_ref(), [59, 61]);
    let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    let [first, second] =
        <[_; 2]>::try_from(rows).unwrap_or_else(|_| panic!("two actual requests remain retained"));
    let first = first
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the first request releases all producer loans"));
    let second = second
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the duplicate releases all producer loans"));
    assert!(first.0.is_some());
    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    let batch = exchange
        .batch
        .as_ref()
        .expect("the actual Task batch remains acquired");
    let TaskResult::Completed(originals) = &batch.message else {
        panic!("the actual Task completed its original request batch");
    };
    for (raw, original) in [&first.1, &second.1].into_iter().zip(originals) {
        assert_eq!(raw.0.as_ref(), original.protected.as_ref());
        assert_eq!(raw.1.as_ref(), original.proof.as_ref());
    }
    let allocations = allocations.borrow();
    assert_eq!(
        allocations.as_slice(),
        [
            (first.1.0.as_ptr(), first.1.1.as_ptr()),
            (second.1.0.as_ptr(), second.1.1.as_ptr())
        ]
    );
    for receipt in [&first.2, &second.2] {
        assert_verification(receipt.as_ref());
        let receipt = receipt.as_ref().unwrap().as_ref().unwrap();
        let scope = projection(receipt);
        assert_eq!(scope.bombay, exchange.current);
        assert_eq!(scope.kind, Purpose::Request);
        assert_eq!(scope.operation, LedgerOperation::Read);
        assert_eq!(
            identity(receipt),
            (&binding, exchange.current.request_sequence)
        );
        if let Some(actual) = proof(receipt) {
            assert_eq!(actual, [7, 11]);
        }
    }
    let (constructor, client, admission, received, tail) =
        first.4.expect("the actual first native read remains");
    assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
    let client = match client {
        Attempt::Returned(Ok(client)) => client,
        _ => panic!("the original native reply client remains"),
    };
    let received = received
        .expect("the first native reception was acquired")
        .expect("the actual first User remains");
    assert_eq!(received.from, origin.address());
    assert_eq!(received.message.as_slice(), [caller]);
    assert_ne!(client.address(), origin.address());
    assert!(matches!(tail.as_slice(), [None]));
    assert!(second.3.is_none() && second.4.is_none() && second.5.is_none() && second.6.is_none());
    assert_eq!(
        (decoded.get(), constructors.get(), protected.get()),
        (1, 1, 1)
    );
    let encoded = encoded.borrow();
    assert_eq!(encoded.as_slice(), [(origin.address(), vec![caller])]);
    assert!(matches!(first.5, Some(Ok(_))) && matches!(first.6, Some(Ok(_))));
    assert_eq!((first.7.len(), second.7.len()), (1, 1));
    assert!(first.7.iter().all(Result::is_ok) && second.7.iter().all(Result::is_ok));
    assert!(matches!(first.8, (None, None, None)) && matches!(second.8, (None, None, None)));
    assert!(matches!(first.9, (None, None, None, None, None, None)));
    assert!(matches!(second.9, (None, None, None, None, None, None)));
    assert_completed((origin, native), None);
    assert_completed(exchange.native.unwrap(), None);
}
#[test]
fn recorded_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(40),
                read_counter(
                    ProviderProfile::Recorded,
                    recorded::verify::<CounterOperation>,
                    recorded::projection,
                    recorded::identity,
                    |_| None,
                    same_recorded_counter,
                ),
            )
            .await
            .expect("the complete replay and cleanup trace terminates");
        });
}
#[test]
fn scheduled_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(40),
                read_counter(
                    ProviderProfile::Scheduled,
                    scheduled::verify::<CounterOperation>,
                    scheduled::projection,
                    scheduled::identity,
                    |receipt| Some(scheduled::proof(receipt)),
                    same_scheduled_counter,
                ),
            )
            .await
            .expect("the complete replay and cleanup trace terminates");
        });
}
#[test]
fn recorded_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(40),
                read_ledger(
                    ProviderProfile::Recorded,
                    recorded::verify::<LedgerOperation>,
                    recorded::projection,
                    recorded::identity,
                    |_| None,
                    same_recorded_ledger,
                ),
            )
            .await
            .expect("the complete replay and cleanup trace terminates");
        });
}
#[test]
fn scheduled_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(40),
                read_ledger(
                    ProviderProfile::Scheduled,
                    scheduled::verify::<LedgerOperation>,
                    scheduled::projection,
                    scheduled::identity,
                    |receipt| Some(scheduled::proof(receipt)),
                    same_scheduled_ledger,
                ),
            )
            .await
            .expect("the complete replay and cleanup trace terminates");
        });
}

#[path = "replay/association.rs"]
mod association;

#[path = "replay/cancellation.rs"]
mod cancellation;
