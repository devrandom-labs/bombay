use super::declaration::{Commands, Delivery, Reception, reply, submission};
use super::query::Exports;
use super::{Attempt, Export, ExportLimits, RequestKey};
use crate::actors::ActorExt;
use crate::{
    ActorFailureAssessment, Application, ApplicationHandle, Completion, MailAddr, MailboxCapacity,
    RetirementAssessment, TrySendError,
};
use behavior::{Behavior, EstablishedRecipient, Never, User};
use behavior_actors::StopOnShutdown;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use futures_util::FutureExt;
use futures_util::future::{Either, OptionFuture};
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Wake;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;
use zenoh::Session;
use zenoh::query::{ConsolidationMode, Reply as QueryReply};
use zenoh::sample::Locality;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;
use counter::{Counter, CounterIncremented, CounterMessage, CounterValue};

#[path = "../../../tests/protected_counter/custody.rs"]
mod custody;
#[path = "../../../tests/application_clients/protocol.rs"]
#[expect(
    dead_code,
    reason = "reuse the owning nominal provider and native configuration fixture"
)]
mod protocol;
use custody::completed_actor;
use protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, current_scope, limits, metadata, recorded,
};

type Event = <StopOnShutdown<Counter> as Behavior>::Event;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Command {
    Increment,
    Read,
}

#[path = "comparison.rs"]
mod comparison;

fn request_originals(
    protected: &[u8],
    proof: &[u8],
    receipt: &recorded::Receipt<Command>,
) -> (Vec<u8>, Vec<u8>, Vec<u8>, Binding, [*const u8; 12]) {
    let scope = recorded::projection(receipt);
    let claims = &scope.bombay;
    (
        protected.to_vec(),
        proof.to_vec(),
        serde_json::to_vec(&ReadRequestRef {
            bombay: claims,
            kind: scope.kind,
            operation: &scope.operation,
        })
        .unwrap(),
        *recorded::identity(receipt).0,
        [
            protected.as_ptr(),
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

async fn query_replies<Declarations>(
    export: &mut Declarations,
    application: &ApplicationHandle<Counter, Event>,
    session: &Session,
    path: &str,
    originals: &[&ProtectedMessage],
) -> Vec<Vec<QueryReply>>
where
    Declarations: Exports<Counter, Event, (), EstablishedRecipient<Counter>>,
{
    let target = application.root().established_recipient();
    let controls = export
        .preflight(application, &target, &mut Vec::new())
        .unwrap_or_else(|()| panic!("the exact live Counter owns this declaration"));
    export
        .declare(session)
        .await
        .unwrap_or_else(|()| panic!("the actual native declaration opens"));
    // A unit-test input only; it neither publishes nor certifies Application cleanup.
    let (cancellation, uncancelled) = oneshot::channel::<()>();
    let replies = {
        let serving = export.serve(controls, session.zid().to_string(), uncancelled.shared());
        let mut serving = pin!(serving);
        let exchange = async {
            let mut replies = Vec::new();
            for original in originals {
                let receiving = session
                    .get(path)
                    .allowed_destination(Locality::SessionLocal)
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(5))
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .await
                    .expect("the real local Query starts");
                let mut observed = Vec::new();
                while let Ok(reply) = receiving.recv_async().await {
                    observed.push(reply);
                }
                replies.push(observed);
            }
            replies
        };
        tokio::select! {
            biased;
            replies = exchange => replies,
            () = serving.as_mut() => panic!("serving outlives the real exchange"),
        }
    };
    export.withdraw().await;
    drop(cancellation);
    replies
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_keys_preserve_declarations_across_reply_protocols() {
    timeout(Duration::from_secs(40), redeclared_counter())
        .await
        .expect("the actual declarations, replies and joined cleanup finish");
}

#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one redeclaration trace preserves complete original request refusals and native cleanup before its independent key comparisons"
)]
async fn redeclared_counter() {
    let mut network = configuration("tls/localhost:0", &EndpointRole::Receiver);
    // All five native Queries may retain timeout captures beyond reply EOF.
    network.max_retained_queries = NonZeroUsize::new(5).unwrap();
    let session = zenoh::open(network.to_configuration().unwrap())
        .await
        .expect("the actual private native Session opens");
    let before = RefCell::new(Vec::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let authorized = Cell::new(0);
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| {
            let session = &session;
            let before = &before;
            let decoded = &decoded;
            let constructed = &constructed;
            let encoded = &encoded;
            let protected = &protected;
            let authorized = &authorized;
            async move {
                let target = application.root().established_recipient();
                let origin = application.actor_address(&target).unwrap();
                let runtime = session.zid().to_string();
                let mut claims = metadata(
                    ProviderProfile::Recorded,
                    "counter",
                    runtime.clone(),
                    runtime,
                );
                claims.actor_generation = origin.0;
                let binding = Binding {
                    deployment: "two-node-read-study",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "one-live-counter",
                    logical_session: 367,
                };
                let records =
                    [Command::Increment, Command::Read].map(|operation| ProtectedMessage {
                        protected: serde_json::to_vec(&ReadRequestRef {
                            bombay: &claims,
                            kind: Purpose::Request,
                            operation,
                        })
                        .unwrap()
                        .into_boxed_slice(),
                        proof: Box::from([7_u8, 11]),
                    });
                let maximum = NonZeroUsize::new(2).unwrap();
                let export_limits = ExportLimits {
                    queued_queries: maximum,
                    retained_queries: maximum,
                    retained_requests: maximum,
                    diagnostics: maximum,
                };
                let decode = |bytes: &[u8]| {
                    decoded.set(decoded.get() + 1);
                    serde_json::from_slice::<ReadRequest<Command>>(bytes)
                };
                let verify = async |bytes: &[u8], proof: &[u8]| {
                    let original = ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    };
                    let result = recorded::verify::<Command>(&original, &records, binding).await;
                    if let Ok(receipt) = &result {
                        before
                            .borrow_mut()
                            .push(request_originals(bytes, proof, receipt));
                    }
                    result
                };
                let protect = async |_: &[u8], _: &recorded::Receipt<Command>| {
                    protected.set(protected.get() + 1);
                    Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
                };
                let deadline = Instant::now() + Duration::from_secs(30);
                let current =
                    |receipt: &recorded::Receipt<Command>, runtime: &str, actor: MailAddr| {
                        let scope = recorded::projection(receipt);
                        if scope.bombay.runtime_generation.as_ref() != runtime
                            || scope.bombay.actor_generation != actor.0
                            || recorded::identity(receipt).0 != &binding
                        {
                            return Err(VerificationError::Invalid);
                        }
                        current_scope(scope, &claims, &scope.operation, Purpose::Request, deadline)
                    };
                let authorize_request =
                    |receipt: &recorded::Receipt<Command>, runtime: &str, actor: MailAddr| {
                        authorized.set(authorized.get() + 1);
                        current(receipt, runtime, actor)
                    };
                let same = |left: &recorded::Receipt<Command>,
                            right: &recorded::Receipt<Command>| {
                    recorded::identity(left) == recorded::identity(right)
                };
                let mut processing = Export::new(
                    "bombay/keys/processed",
                    export_limits,
                    limits(),
                    &decode,
                    reply(
                        |request: ReadRequest<Command>| match request.operation {
                            Command::Increment => Ok(move |reply_to| {
                                constructed.set(constructed.get() + 1);
                                CounterMessage::IncrementAndReply {
                                    request_sequence: request.bombay.request_sequence,
                                    reply_to,
                                }
                            }),
                            Command::Read => Err(request),
                        },
                        |_: &recorded::Receipt<Command>,
                         original: &User<MailAddr, CounterIncremented>| {
                            encoded.set(encoded.get() + 1);
                            Ok::<_, Never>(
                                original
                                    .message
                                    .request_sequence
                                    .to_ne_bytes()
                                    .to_vec()
                                    .into_boxed_slice(),
                            )
                        },
                    ),
                    &verify,
                    &protect,
                    same,
                    &authorize_request,
                    &current,
                );
                let reads = |path: &str| {
                    Export::new(
                        path,
                        export_limits,
                        limits(),
                        &decode,
                        reply(
                            |request: ReadRequest<Command>| match request.operation {
                                Command::Read => Ok(move |reply_to| {
                                    constructed.set(constructed.get() + 1);
                                    CounterMessage::Read(reply_to)
                                }),
                                Command::Increment => Err(request),
                            },
                            |_: &recorded::Receipt<Command>, original: &User<MailAddr, u64>| {
                                encoded.set(encoded.get() + 1);
                                Ok::<_, Never>(
                                    original.message.to_ne_bytes().to_vec().into_boxed_slice(),
                                )
                            },
                        ),
                        &verify,
                        &protect,
                        same,
                        &authorize_request,
                        &current,
                    )
                };
                let mut reading = reads("bombay/keys/value");
                let mut neighbor = reads("bombay/keys/neighbor");
                let mut renewed = reads("bombay/keys/value");
                let processed = query_replies(
                    &mut processing,
                    &application,
                    session,
                    "bombay/keys/processed",
                    &[&records[0]],
                )
                .await;
                let read = query_replies(
                    &mut reading,
                    &application,
                    session,
                    "bombay/keys/value",
                    &[&records[1], &records[1]],
                )
                .await;
                let adjacent = query_replies(
                    &mut neighbor,
                    &application,
                    session,
                    "bombay/keys/neighbor",
                    &[&records[1]],
                )
                .await;
                // The old same-path declaration has actually withdrawn and drained.
                let fresh = query_replies(
                    &mut renewed,
                    &application,
                    session,
                    "bombay/keys/value",
                    &[&records[1]],
                )
                .await;
                let mut read_rows = Vec::new();
                let mut declarations = Vec::new();
                for export in [reading, neighbor, renewed] {
                    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
                    declarations.push((startup, withdrawal, diagnostics));
                    for request in requests {
                        read_rows.push(request.try_into_parts().unwrap_or_else(|_| {
                            panic!("no producer loan survives real withdrawal")
                        }));
                    }
                }
                let (_, _, startup, withdrawal, diagnostics, requests) = processing.into_parts();
                declarations.push((startup, withdrawal, diagnostics));
                let [request] = <[_; 1]>::try_from(requests)
                    .unwrap_or_else(|_| panic!("one actual processing request"));
                let processing = request.try_into_parts().unwrap_or_else(|_| {
                    panic!("the processing producer releases its original request")
                });
                let shutdown = application.shutdown();
                (
                    processing,
                    read_rows,
                    declarations,
                    [processed, read, adjacent, fresh],
                    origin,
                    shutdown,
                    claims.request_sequence,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Application enters its host"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((processing, read_rows, declarations, replies, origin, shutdown, sequence), cleanup) =
        work.try_into_completed()
            .unwrap_or_else(|_| panic!("actual Work and joined cleanup remain acquired"));
    let closed = session.close().await;
    cleanup.expect("actual Application cleanup joins");
    closed.expect("the actual native Session closes");
    let (root_origin, root) = native.expect("the complete original Counter remains acquired");
    let root_address = root_origin.address();
    let _counter = completed_actor(root_origin, root);
    let report = report.expect("the actual root report remains independent");
    let notifications = notifications.expect("both actual notification receipts remain");
    let before = before.into_inner();
    assert!(shutdown.is_ok() && session.is_closed());
    assert_eq!(root_address, origin);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    for (startup, withdrawal, diagnostics) in declarations {
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty());
    }
    assert_eq!(
        replies.iter().map(Vec::len).collect::<Vec<_>>(),
        [1, 2, 1, 1]
    );
    for (index, declaration) in replies.iter().enumerate() {
        let expected = if index == 0 { sequence } else { 1 }.to_ne_bytes();
        for query in declaration {
            assert_eq!(query.len(), 1);
            for reply in query {
                let received = reply.result();
                let sample = received
                    .as_ref()
                    .expect("the actual query acquired a native reply");
                let bytes = sample.payload().to_bytes();
                let proof = sample
                    .attachment()
                    .expect("the actual protection proof remains")
                    .to_bytes();
                assert_eq!(bytes.as_ref(), expected);
                assert_eq!(proof.as_ref(), [13, 17]);
            }
        }
    }
    let (key, raw, verified, input, native, encoding, protection, publications, refusals, panics) =
        processing;
    let key = key.expect("the runtime issued the processing key");
    let receipt = verified
        .unwrap_or_else(|| panic!("the actual processing verification remains"))
        .unwrap_or_else(|_| panic!("the configured processing proof verifies"));
    let after = request_originals(&raw.0, &raw.1, &receipt);
    assert_eq!(after, before[0]);
    assert!(
        input.is_none()
            && encoding.as_ref().is_some_and(Result::is_ok)
            && protection.as_ref().is_some_and(Result::is_ok)
    );
    assert!(
        matches!(refusals, (None, None, None))
            && matches!(panics, (None, None, None, None, None, None))
    );
    assert_eq!(publications.len(), 1);
    assert!(publications.iter().all(Result::is_ok));
    let (constructor, client, admission, processed, tail) =
        native.expect("the actual processing operation remains");
    let Attempt::Returned(Ok(processing_client)) = client else {
        panic!("the original processing client remains");
    };
    assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
    assert_eq!(
        processed,
        Some(Some(User::new(
            origin,
            CounterIncremented {
                request_sequence: sequence
            }
        )))
    );
    assert_eq!(tail.len(), 1);
    assert!(tail[0].is_none());
    let mut keys: Vec<RequestKey<Counter>> = vec![key];
    let mut recipients = Vec::new();
    for (
        index,
        (key, raw, verified, input, native, encoding, protection, publications, refusals, panics),
    ) in read_rows.into_iter().enumerate()
    {
        keys.push(key.expect("the actual read or duplicate key remains"));
        let receipt = verified
            .unwrap_or_else(|| panic!("the original read verification remains"))
            .unwrap_or_else(|_| panic!("the configured Read proof verifies"));
        let after = request_originals(&raw.0, &raw.1, &receipt);
        assert_eq!(after, before[index + 1]);
        assert!(input.is_none());
        assert!(
            matches!(refusals, (None, None, None))
                && matches!(panics, (None, None, None, None, None, None))
        );
        assert_eq!(publications.len(), 1);
        assert!(publications.iter().all(Result::is_ok));
        if index == 1 {
            assert!(native.is_none() && encoding.is_none() && protection.is_none());
            continue;
        }
        assert!(
            encoding.as_ref().is_some_and(Result::is_ok)
                && protection.as_ref().is_some_and(Result::is_ok)
        );
        let (constructor, client, admission, value, tail) =
            native.expect("each canonical Read retains its actual operation");
        let Attempt::Returned(Ok(client)) = client else {
            panic!("the original Read client remains");
        };
        recipients.push(client.address());
        assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
        assert_eq!(value, Some(Some(User::new(origin, 1))));
        assert_eq!(tail.len(), 1);
        assert!(tail[0].is_none());
    }
    assert_eq!(before.len(), 5);
    let [processed, read, duplicate, neighbor, renewed] = keys.as_slice() else {
        panic!("all five original runtime keys remain");
    };
    assert_eq!(recipients.len(), 3);
    assert_ne!(recipients[0], recipients[1]);
    assert_ne!(recipients[0], recipients[2]);
    assert_ne!(recipients[1], recipients[2]);
    assert_ne!(processing_client.address(), recipients[0]);
    assert_eq!(
        (
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get(),
            authorized.get()
        ),
        // Five protected queries; four have a fresh native attempt.
        (4, 4, 4, 4, 5 + 4)
    );
    assert_eq!(read, duplicate);
    assert_ne!(
        read, renewed,
        "actual same-path redeclaration must have a fresh issuer"
    );
    assert_ne!(processed, read);
    assert_ne!(read, neighbor);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one capacity trace retains the exact constructor, reply endpoint, admission and terminal native tail through cleanup"
)]
fn full_read_reuses_its_original_reply_endpoint_once() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(40), async {
                let constructors = Cell::new(0);
                let commitments = Cell::new(0);
                let declarations = reply(
                    |()| {
                        Ok::<_, ()>(|reply_to| {
                            constructors.set(constructors.get() + 1);
                            CounterMessage::Read(reply_to)
                        })
                    },
                    |(): &(), original: &User<MailAddr, u64>| {
                        Ok::<_, Never>(original.message.to_ne_bytes().to_vec().into_boxed_slice())
                    },
                );
                let canonical_count = &commitments;
                let outcome = Application::new(Counter::new().stop_on_shutdown())
                    .mailbox_capacity(MailboxCapacity::new(2).unwrap())
                    .run_with(|application| async move {
                        let interface =
                            application.interface(application.root().established_recipient());
                        let mut ordinary = application
                            .client::<CounterValue>()
                            .unwrap_or_else(|_| panic!("the independent Counter client allocates"));
                        let mut native = Commands::<(), (), Counter>::select(&declarations, ())
                            .unwrap_or_else(|()| panic!("one actual Read constructor is selected"));
                        let prepared = Commands::<(), (), Counter>::prepare(
                            &declarations,
                            &mut native,
                            &interface,
                        );
                        prepared
                            .expect("Reply owns endpoint preparation")
                            .expect("the real reply endpoint allocates");
                        let (release_worker, released) = mpsc::channel::<()>();
                        let (announced, worker_entered) = oneshot::channel();
                        let worker = tokio::spawn(async move {
                            let announced = announced.send(());
                            let released = released.recv();
                            (announced, released)
                        });
                        let entered = worker_entered.await;
                        let prefix = (0..2)
                            .map(|_| ordinary.try_send(CounterMessage::Increment))
                            .collect::<Vec<_>>();
                        let independent = ordinary.try_send(CounterMessage::Increment);
                        let control = Some(application.lifecycle());
                        let canonical = || canonical_count.set(canonical_count.get() + 1);
                        let first = Delivery::prepare_attempt(
                            &declarations,
                            &mut native,
                            &control,
                            canonical,
                        )
                        .map(|attempt| attempt.map(|attempt| attempt()));
                        let retained = match &native.2 {
                            Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(
                                reply_to,
                            )))) => Some(reply_to.clone()),
                            _ => None,
                        };
                        let repeated_full = Delivery::prepare_attempt(
                            &declarations,
                            &mut native,
                            &control,
                            canonical,
                        )
                        .map(|attempt| attempt.map(|attempt| attempt()));
                        let still_retained = match &native.2 {
                            Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(
                                reply_to,
                            )))) => Some(reply_to.clone()),
                            _ => None,
                        };
                        drop(release_worker);
                        let worker = worker.await;
                        let fence = ordinary
                            .send(CounterMessage::Read(ordinary.recipient()))
                            .await;
                        let before = if fence.is_ok() {
                            ordinary.receive().await
                        } else {
                            None
                        };
                        let retried = Delivery::prepare_attempt(
                            &declarations,
                            &mut native,
                            &control,
                            canonical,
                        )
                        .map(|attempt| attempt.map(|attempt| attempt()));
                        let repeated = Delivery::prepare_attempt(
                            &declarations,
                            &mut native,
                            &control,
                            canonical,
                        )
                        .map(|attempt| attempt.map(|attempt| attempt()));
                        // Keep reply admission open until every preceding Read
                        // has completed its reply effect on the actual actor.
                        let fence_after = ordinary
                            .send(CounterMessage::Read(ordinary.recipient()))
                            .await;
                        let after = if fence_after.is_ok() {
                            ordinary.receive().await
                        } else {
                            None
                        };
                        if let Some(receiving) = Delivery::receive(
                            &declarations,
                            &mut native,
                            &control,
                            application.retirement(),
                        ) {
                            receiving.await;
                        }
                        if let Some(draining) = declarations.drain(&mut native) {
                            draining.await;
                        }
                        let shutdown = application.shutdown();
                        (
                            entered,
                            worker,
                            prefix,
                            independent,
                            first,
                            retained,
                            repeated_full,
                            still_retained,
                            fence,
                            before,
                            retried,
                            repeated,
                            fence_after,
                            after,
                            native,
                            shutdown,
                        )
                    })
                    .await
                    .unwrap_or_else(|_| panic!("the actual Counter Application starts"));
                let (work, root_report, root, notifications, ()) = outcome.into_parts();
                let (output, cleanup) = work
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("the original caller output survives cleanup"));
                cleanup.expect("the actual Application cleanup joins");
                let (origin, root) = root.expect("the original Counter retirement survives");
                let notifications = notifications.expect("both native notifications survive");
                let root_report = root_report.expect("the runtime retains its actual root report");
                assert_eq!(root_report.retirement(), RetirementAssessment::Established);
                assert_eq!(
                    root_report.failures(),
                    ActorFailureAssessment::NoFailuresFound
                );
                let (
                    entered,
                    worker,
                    prefix,
                    independent,
                    first,
                    retained,
                    repeated_full,
                    still_retained,
                    fence,
                    before,
                    retried,
                    repeated,
                    fence_after,
                    after,
                    (constructor, client, admission, received, tail),
                    shutdown,
                ) = output;
                assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
                assert!(root.custody().is_some() && shutdown.is_ok() && entered.is_ok());
                assert_eq!(root.completion(), Some(&Completion::Stopped));
                let (announced, released) = worker.expect("the blocked worker joins");
                assert!(announced.is_ok() && released.is_err());
                assert_eq!(prefix.len(), 2);
                assert!(prefix.iter().all(Result::is_ok));
                assert!(matches!(
                    independent,
                    Err(TrySendError::Full(CounterMessage::Increment))
                ));
                assert!(matches!(first, Ok(Some(()))) && matches!(repeated_full, Ok(Some(()))));
                assert!(fence.is_ok());
                let before = before.as_ref().map(|reply| (reply.from, reply.message));
                assert_eq!(before, Some((origin.address(), 2)));
                assert!(retained.is_some());
                assert_eq!(retained, still_retained);
                assert!(matches!(retried, Ok(Some(()))));
                assert!(matches!(repeated, Ok(None)));
                assert!(fence_after.is_ok());
                let after = after.as_ref().map(|reply| (reply.from, reply.message));
                assert_eq!(after, Some((origin.address(), 2)));
                assert!(
                    matches!(admission, Attempt::Returned(Ok(()))),
                    "native Full must be retried after capacity returns"
                );
                assert!(constructor.is_none());
                assert_eq!(constructors.get(), 1);
                assert_eq!(commitments.get(), 1);
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the one original reply client survives");
                };
                let endpoint = client.recipient();
                assert_eq!(retained.as_ref(), Some(&endpoint));
                let received = received
                    .as_ref()
                    .and_then(Option::as_ref)
                    .map(|reply| (reply.from, reply.message));
                assert_eq!(received, Some((origin.address(), 2)));
                assert_eq!(tail.len(), 1);
                assert!(tail[0].is_none());
            })
            .await
            .expect("the complete Counter admission and cleanup trace terminates");
        });
}

#[tokio::test(flavor = "current_thread")]
async fn acquired_first_eof_prevents_another_tail_receive() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let declarations = reply(
                |()| Ok::<_, ()>(CounterMessage::Read),
                |(): &(), original: &User<MailAddr, u64>| {
                    Ok::<_, Never>(original.message.to_ne_bytes().to_vec().into_boxed_slice())
                },
            );
            let interface = application.interface(application.root().established_recipient());
            let mut native = Commands::<(), (), Counter>::select(&declarations, ())
                .unwrap_or_else(|()| panic!("the actual Read constructor is selected"));
            let prepared =
                Commands::<(), (), Counter>::prepare(&declarations, &mut native, &interface);
            prepared
                .expect("reply preparation applies")
                .expect("the real endpoint allocates");
            if let Attempt::Returned(Ok(client)) = &native.1 {
                client.close_admission();
            }
            let control = Some(application.lifecycle());
            let mut commitment = None;
            let admitted = Delivery::prepare_attempt(&declarations, &mut native, &control, || {
                commitment = Some(());
            })
            .map(|attempt| attempt.map(|attempt| attempt()));
            if let Some(receiving) = Delivery::receive(
                &declarations,
                &mut native,
                &control,
                application.retirement(),
            ) {
                receiving.await;
            }
            let observed = if let Some(draining) = declarations.drain(&mut native) {
                let mut draining = pin!(draining);
                Some(poll_fn(|cx| Poll::Ready(draining.as_mut().poll(cx))).await)
            } else {
                None
            };
            let shutdown = application.shutdown();
            (commitment, admitted, observed, native, shutdown)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Application host is entered"));
    let (work, root_report, native_root, notifications, ()) = outcome.into_parts();
    let (
        (
            commitment,
            admitted,
            observed,
            (constructor, client, admission, received, tail),
            _shutdown,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the finite owning observation survives actual cleanup"));
    cleanup.expect("the Application cleanup task joins");
    let (origin, root) =
        native_root.expect("the actual root retirement remains independently owned");
    let root_report = root_report.expect("the runtime retains its actual root report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(root.custody().is_some());
    let notifications = notifications.expect("both actual notification receipts survive");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(commitment, Some(()));
    assert!(matches!(admitted, Ok(Some(()))));
    assert!(constructor.is_none());
    assert!(matches!(client, Attempt::Returned(Ok(_))));
    assert!(matches!(admission, Attempt::Returned(Ok(()))));
    assert!(matches!(received, Some(None)));
    assert_eq!(tail, []);
    assert_ne!(observed, Some(Poll::Pending));
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one cancellation trace observes the same pending native wait and its wake, independent FIFO fence, exact client and joined cleanup"
)]
async fn cancelled_native_wait_keeps_its_original_reply_client() {
    let outcome = timeout(
        Duration::from_secs(10),
        Application::new(Counter::new().stop_on_shutdown()).run_with(|application| async move {
            let declarations = reply(
                |()| Ok::<_, ()>(CounterMessage::Read),
                |(): &(), original: &User<MailAddr, u64>| {
                    Ok::<_, Never>(original.message.to_ne_bytes().to_vec().into_boxed_slice())
                },
            );
            let interface = application.interface(application.root().established_recipient());
            let mut native = Commands::<(), (), Counter>::select(&declarations, ())
                .unwrap_or_else(|()| panic!("the actual Read constructor is selected"));
            let prepared =
                Commands::<(), (), Counter>::prepare(&declarations, &mut native, &interface);
            prepared
                .expect("Reply owns preparation")
                .expect("the real reply endpoint allocates");
            let original_endpoint = match &native.1 {
                Attempt::Returned(Ok(client)) => Some(client.address()),
                _ => None,
            };
            let control = Some(application.lifecycle());
            let commitments = Cell::new(0);
            let canonical = || commitments.set(commitments.get() + 1);
            let admitted =
                Delivery::prepare_attempt(&declarations, &mut native, &control, canonical)
                    .map(|attempt| attempt.map(|attempt| attempt()));
            let cancelled = if let Some(receiving) = Delivery::receive(
                &declarations,
                &mut native,
                &control,
                application.retirement(),
            ) {
                let mut receiving = pin!(receiving);
                Some(poll_fn(|cx| Poll::Ready(receiving.as_mut().poll(cx))).await)
            } else {
                None
            };
            // The whole borrowing receive is now gone; the native tuple/client survives.
            let unreceived = native
                .3
                .as_ref()
                .map(|original| original.as_ref().map(|reply| (reply.from, reply.message)));
            let mut ordinary = application.client::<CounterValue>().unwrap();
            let (waiting, before, after, resumed, incremented, fenced, fence) = {
                let mut receiving = pin!(OptionFuture::from(Delivery::receive(
                    &declarations,
                    &mut native,
                    &control,
                    application.retirement(),
                )));
                let awakenings = Arc::new(ReplyWake(AtomicUsize::new(0)));
                let waker = Waker::from(Arc::clone(&awakenings));
                let waiting = receiving.as_mut().poll(&mut Context::from_waker(&waker));
                let before = awakenings.0.load(Ordering::SeqCst);
                let incremented = ordinary.send(CounterMessage::Increment).await;
                let fenced = ordinary
                    .send(CounterMessage::Read(ordinary.recipient()))
                    .await;
                let fence = ordinary.receive().await;
                // The root remains alive. Only its native reply can wake this wait.
                let after = awakenings.0.load(Ordering::SeqCst);
                let resumed = match waiting {
                    Poll::Pending => {
                        Some(receiving.as_mut().poll(&mut Context::from_waker(&waker)))
                    }
                    Poll::Ready(_) => None,
                };
                (waiting, before, after, resumed, incremented, fenced, fence)
            };
            if let Some(draining) = declarations.drain(&mut native) {
                draining.await;
            }
            ordinary.close_admission();
            let mut ordinary_tail = Vec::new();
            while let Some(original) = ordinary.receive().await {
                ordinary_tail.push(original);
            }
            let shutdown = application.shutdown();
            let commitments = commitments.get();
            move |origin: MailAddr| {
                assert!(shutdown.is_ok());
                assert_eq!(cancelled, Some(Poll::Pending));
                assert_eq!(unreceived, None);
                assert!(matches!(admitted, Ok(Some(()))));
                assert!(incremented.is_ok() && fenced.is_ok());
                assert_eq!(fence, Some(User::new(origin, 1)));
                assert_eq!(resumed, Some(Poll::Ready(Some(()))));
                let (constructor, client, admission, received, tail) = native;
                assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the original native reply client remains");
                };
                assert_eq!(Some(client.address()), original_endpoint);
                assert_ne!(client.address(), origin);
                assert_eq!(commitments, 1);
                assert_eq!(received, Some(Some(User::new(origin, 0))));
                assert!(matches!(tail.as_slice(), [None]));
                assert_eq!(ordinary_tail, []);
                assert!(waiting.is_pending() && after > before);
                drop(ordinary);
            }
        }),
    )
    .await
    .expect("the native cancellation, fence and cleanup terminate")
    .unwrap_or_else(|_| panic!("the actual Counter Application starts"));
    let (work, report, root, notifications, ()) = outcome.into_parts();
    let (observations, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete cancellation trace survives cleanup"));
    let (origin, root) = root.expect("the actual Counter retirement remains owned");
    let report = report.expect("the actual independent root report remains");
    let notifications = notifications.expect("both actual native notification receipts remain");
    assert!(cleanup.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    assert!(root.custody().is_some());
    observations(origin.address());
}

struct ReplyWake(AtomicUsize);

impl Wake for ReplyWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum Expiry {
    Decode,
    Construction,
    AfterAttempt,
}

#[derive(Debug, Error)]
#[error("permission expired")]
struct Expired;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decoding_expiry_prevents_the_first_native_attempt() {
    timeout(
        Duration::from_secs(40),
        first_attempt_expiry(Expiry::Decode),
    )
    .await
    .expect("the actual expiry, native query and cleanup trace finishes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn construction_expiry_prevents_the_first_native_attempt() {
    timeout(
        Duration::from_secs(40),
        first_attempt_expiry(Expiry::Construction),
    )
    .await
    .expect("the actual expiry, native query and cleanup trace finishes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_expiry_preserves_the_admitted_increment() {
    timeout(
        Duration::from_secs(40),
        first_attempt_expiry(Expiry::AfterAttempt),
    )
    .await
    .expect("the permitted native query and complete cleanup finish before the watchdog");
}

#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one concrete expiry trace retains original provider, decoded input, native facts and all cleanup before independent admission oracles"
)]
async fn first_attempt_expiry(expiry: Expiry) {
    let clock = Cell::new(Duration::ZERO);
    let deadline = Duration::from_secs(1);
    let decoded_count = Cell::new(0);
    let constructed = Cell::new(0);
    let before = RefCell::new(Vec::new());
    let session = zenoh::open(
        configuration("tls/localhost:0", &EndpointRole::Receiver)
            .to_configuration()
            .unwrap(),
    )
    .await
    .expect("the actual private native Session opens");
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| {
            let clock = &clock;
            let decoded_count = &decoded_count;
            let constructed = &constructed;
            let before = &before;
            let session = &session;
            async move {
                let target = application.root().established_recipient();
                let address = application.actor_address(&target).unwrap();
                let runtime = session.zid().to_string();
                let mut claims = metadata(ProviderProfile::Recorded, "counter", runtime.clone(), runtime);
                claims.actor_generation = address.0;
                let binding = Binding {
                    deployment: "two-node-read-study", caller: "caller", controller: "owner",
                    actor: "counter", host: "receiver", protocol: "counter",
                    target_assignment: "first-attempt-expiry", logical_session: 401,
                };
                let records = [ProtectedMessage {
                    protected: serde_json::to_vec(&ReadRequestRef {
                        bombay: &claims, kind: Purpose::Request, operation: Command::Increment,
                    }).unwrap().into_boxed_slice(),
                    proof: Box::from([7_u8, 11]),
                }];
                let maximum = NonZeroUsize::new(1).unwrap();
                let current = |_: &recorded::Receipt<Command>, _: &str, _: MailAddr| {
                    if clock.get() >= deadline { Err(Expired) } else { Ok(()) }
                };
                let mut export = Export::new(
                    "bombay/expiry/first", ExportLimits {
                        queued_queries: maximum, retained_queries: maximum,
                        retained_requests: maximum, diagnostics: maximum,
                    }, limits(),
                    |bytes: &[u8]| {
                        decoded_count.set(decoded_count.get() + 1);
                        let input = serde_json::from_slice::<ReadRequest<Command>>(bytes);
                        if let Expiry::Decode = expiry { clock.set(deadline); }
                        input
                    },
                    submission(
                        |request: ReadRequest<Command>| match request.operation {
                            Command::Increment => Ok(move || {
                                constructed.set(constructed.get() + 1);
                                if let Expiry::Construction = expiry { clock.set(deadline); }
                                CounterMessage::Increment
                            }),
                            Command::Read => Err(request),
                        },
                        |_: &recorded::Receipt<Command>, admission: &Attempt<Result<(), TrySendError<CounterMessage>>>| {
                            let admission = match admission {
                                Attempt::Unattempted => "unattempted", Attempt::Uncertain => "uncertain",
                                Attempt::Returned(Ok(())) => "admitted",
                                Attempt::Returned(Err(TrySendError::Full(_))) => "full",
                                Attempt::Returned(Err(TrySendError::Closed(_))) => "closed",
                            };
                            serde_json::to_vec(admission).map(Vec::into_boxed_slice)
                        },
                    ),
                    async |bytes: &[u8], proof: &[u8]| {
                        let original = ProtectedMessage { protected: bytes.into(), proof: proof.into() };
                        let result = recorded::verify::<Command>(&original, &records, binding).await;
                        if let Ok(receipt) = &result {
                            before.borrow_mut().push(request_originals(bytes, proof, receipt));
                        }
                        result
                    },
                    async |_: &[u8], _: &recorded::Receipt<Command>| Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17])),
                    |left, right| recorded::identity(left) == recorded::identity(right),
                    &current, &current,
                );
                let replies = query_replies(&mut export, &application, session,
                    "bombay/expiry/first", &[&records[0]]).await;
                if let Expiry::AfterAttempt = expiry { clock.set(deadline); }
                let mut ordinary = application.client::<CounterValue>().unwrap();
                let admitted = ordinary.send(CounterMessage::Read(ordinary.recipient())).await;
                let fence = if admitted.is_ok() { ordinary.receive().await } else { None };
                let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
                let requests = requests.into_iter().map(|request| request.try_into_parts()
                    .unwrap_or_else(|_| panic!("withdrawal releases each original producer loan")))
                    .collect::<Vec<_>>();
                let shutdown = application.shutdown();
                (replies, admitted, fence, ordinary, startup, withdrawal, diagnostics, requests, shutdown)
            }
        }).await.unwrap_or_else(|_| panic!("the actual Counter Application starts"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            replies,
            admitted,
            fence,
            mut ordinary,
            startup,
            withdrawal,
            diagnostics,
            requests,
            shutdown,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Work and cleanup remain acquired"));
    let closed = session.close().await;
    ordinary.close_admission();
    let mut ordinary_tail = Vec::new();
    while let Some(original) = ordinary.receive().await {
        ordinary_tail.push(original);
    }
    let (origin, root) = native.expect("the complete original Counter retirement remains");
    let address = origin.address();
    let report = report.expect("the independent root report remains");
    let notifications = notifications.expect("both original notification receipts remain");
    let [request] =
        <[_; 1]>::try_from(requests).unwrap_or_else(|_| panic!("one actual retained request"));
    let (key, raw, receipt, decoded, native, encoding, protection, publications, refusals, panics) =
        request;
    let receipt = receipt
        .expect("the provider result remains")
        .unwrap_or_else(|_| panic!("the real nominal provider accepts"));
    let after = request_originals(&raw.0, &raw.1, &receipt);
    let expected_originals = [after];
    let before = before.into_inner();
    cleanup.expect("the actual Application cleanup joins");
    closed.expect("the original native Session close succeeds");
    notifications
        .termination
        .expect("actual termination notification");
    notifications
        .retirement
        .expect("actual retirement notification");
    let _counter = completed_actor(origin, root);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let originals = (key, decoded, native);
    assert!(admitted.is_ok() && shutdown.is_ok());
    let fence = fence.expect("the independent FIFO Read completes");
    let expected = match expiry {
        Expiry::Decode | Expiry::Construction => 0,
        Expiry::AfterAttempt => 1,
    };
    assert_eq!(
        (fence.from, fence.message),
        (address, expected),
        "only permission current through the first native attempt may admit Increment"
    );
    assert_eq!(clock.get(), deadline);
    assert_eq!(decoded_count.get(), 1);
    assert_eq!(constructed.get(), 1);
    assert_eq!(before.as_slice(), expected_originals);
    assert!(matches!(panics, (None, None, None, None, None, None)));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert_eq!(diagnostics.len(), 0);
    assert_eq!(replies.len(), 1);
    assert_eq!(ordinary_tail.as_slice(), []);
    match expiry {
        Expiry::Decode | Expiry::Construction => {
            assert!(matches!(&refusals.0, Some(Expired)));
            assert!(refusals.1.is_none() && refusals.2.is_none());
            assert!(encoding.is_none() && protection.is_none());
            assert_eq!(publications.len(), 0);
            assert_eq!(replies[0].len(), 0);
            let (key, decoded, native) = originals;
            let (command, sender, admission) =
                native.expect("the exact constructed command stays in its original native owner");
            assert!(key.is_some() && decoded.is_none());
            assert!(matches!(
                command,
                Some(Either::Right(CounterMessage::Increment))
            ));
            assert!(matches!(sender, Attempt::Returned(Ok(_))));
            assert!(matches!(admission, Attempt::Unattempted));
        }
        Expiry::AfterAttempt => {
            let (key, decoded, native) = originals;
            let (constructor, sender, admission) =
                native.expect("the admitted original native operation remains");
            let encoded = encoding.expect("the native admission is encoded").unwrap();
            let proof = protection
                .expect("the actual encoding is protected")
                .unwrap();
            let expected = serde_json::to_vec("admitted").unwrap();
            assert!(key.is_some() && decoded.is_none() && constructor.is_none());
            assert!(matches!(sender, Attempt::Returned(Ok(_))));
            assert!(matches!(admission, Attempt::Returned(Ok(()))));
            assert!(refusals.0.is_none() && refusals.1.is_none() && refusals.2.is_none());
            assert_eq!(encoded.as_ref(), expected.as_slice());
            assert_eq!(proof.as_ref(), [13, 17]);
            assert_eq!(publications.len(), 1);
            assert!(publications.iter().all(Result::is_ok));
            assert_eq!(replies[0].len(), 1);
            let result = replies[0][0].result();
            let sample = result
                .as_ref()
                .expect("the original admission reply succeeds");
            let payload = sample.payload().to_bytes();
            let attachment = sample
                .attachment()
                .expect("the protected reply retains proof")
                .to_bytes();
            assert_eq!(sample.key_expr().as_str(), "bombay/expiry/first");
            assert_eq!(payload.as_ref(), encoded.as_ref());
            assert_eq!(attachment.as_ref(), proof.as_ref());
        }
    }
}
