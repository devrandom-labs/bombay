//! Actual receipt comparisons while the first native submission's protection is pending.
use super::super::query::Exports;
use super::super::request::Identity;
use super::super::{Attempt, Export, ExportError, ExportLimits, submission};
use super::counter::{Counter, CounterMessage, CounterValue};
use crate::actors::ActorExt;
use crate::{
    ActorFailureAssessment, ApplicationScope, MailAddr, RetirementAssessment, TrySendError,
};
use behavior::{EstablishedRecipient, Never};
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;
use futures_util::FutureExt;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::unconstrained;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    configuration, limits, metadata, recorded, reserved_address,
};
use super::{Command, Event};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Protection {
    Unpolled,
    Pending,
    Released,
}

type Comparisons = RefCell<Vec<(u64, u64, Protection)>>;
type Receipt<'a> = (
    recorded::Receipt<Command>,
    &'a Comparisons,
    &'a Cell<Protection>,
);

fn same_request(left: &Receipt<'_>, right: &Receipt<'_>) -> bool {
    let first = recorded::identity(&left.0);
    let second = recorded::identity(&right.0);
    left.1.borrow_mut().push((first.1, second.1, left.2.get()));
    first == second
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_protection_keeps_canonical_receipt_readable() {
    tokio::time::timeout(Duration::from_secs(10), compare())
        .await
        .expect("setup and real cleanup finish before native query deadlines");
}

async fn compare() {
    let address = reserved_address();
    let mut network = configuration(&address, &EndpointRole::Receiver);
    network.max_retained_queries = NonZeroUsize::new(4).unwrap();
    let session = zenoh::open(network.to_configuration().unwrap())
        .await
        .unwrap();
    let runtime = session.zid().to_string();
    let mut claims = metadata(
        ProviderProfile::Recorded,
        "counter",
        runtime.clone(),
        runtime.clone(),
    );
    claims.actor_generation = MailAddr::APPLICATION_ROOT.0;
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter",
        host: "receiver",
        protocol: "counter",
        target_assignment: "root",
        logical_session: 317,
    };
    let records = [Command::Increment, Command::Read].map(|operation| ProtectedMessage {
        protected: serde_json::to_vec(&ReadRequestRef {
            bombay: &claims,
            kind: Purpose::Request,
            operation,
        })
        .unwrap()
        .into_boxed_slice(),
        proof: Box::from([41_u8]),
    });
    let comparisons = RefCell::new(Vec::new());
    let protection = Cell::new(Protection::Unpolled);
    let decoded = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let (release, gate) = oneshot::channel();
    let gate = RefCell::new(Some(gate));
    // This fixture input never publishes cancellation or claims Application completion.
    let (cancellation, uncancelled) = oneshot::channel::<()>();
    let uncancelled = uncancelled.shared();
    let scope = ApplicationScope::new();
    let mut values = scope.external::<CounterValue>().unwrap();
    let application = scope.with_root(Counter::new().stop_on_shutdown());
    let outcome = application
        .run_with(|application| {
            let session = &session;
            let runtime = &runtime;
            let records = &records;
            let comparisons = &comparisons;
            let protection = &protection;
            let decoded = &decoded;
            let encoded = &encoded;
            let protected = &protected;
            let gate = &gate;
            let values = &mut values;
            async move {
                let maximum = NonZeroUsize::new(4).unwrap();
                let mut export =
                        Export::new(
                            "bombay/pending-comparison",
                            ExportLimits {
                                queued_queries: maximum,
                                retained_queries: maximum,
                                retained_requests: maximum,
                                diagnostics: maximum,
                            },
                            limits(),
                            move |bytes| {
                                decoded.set(decoded.get() + 1);
                                serde_json::from_slice::<ReadRequest<Command>>(bytes)
                                    .map(|request| request.operation)
                            },
                            submission(
                                |command| match command {
                                    Command::Increment => Ok(|| CounterMessage::Increment),
                                    command => Err(command),
                                },
                                move |_: &Receipt<'_>,
                                      admission: &Attempt<
                                    Result<(), TrySendError<CounterMessage>>,
                                >| {
                                    encoded.set(encoded.get() + 1);
                                    match admission {
                                        Attempt::Returned(Ok(())) => {
                                            Ok::<_, ()>(Box::<[u8]>::from([43_u8]))
                                        }
                                        _ => Err(()),
                                    }
                                },
                            ),
                            async move |bytes: &[u8], proof: &[u8]| {
                                let original = ProtectedMessage {
                                    protected: bytes.into(),
                                    proof: proof.into(),
                                };
                                recorded::verify::<Command>(&original, records, binding)
                                    .await
                                    .map(|receipt| (receipt, comparisons, protection))
                            },
                            async move |_: &[u8], _: &Receipt<'_>| {
                                protected.set(protected.get() + 1);
                                let original = gate
                                    .borrow_mut()
                                    .take()
                                    .expect("one original protection producer");
                                let mut original = pin!(original);
                                poll_fn(|cx| {
                                    let observed = original.as_mut().poll(cx);
                                    protection.set(match &observed {
                                        Poll::Pending => Protection::Pending,
                                        Poll::Ready(_) => Protection::Released,
                                    });
                                    observed
                                })
                                .await
                                .expect("unconditional fixture release");
                                Ok::<_, Never>(Box::<[u8]>::from([47_u8]))
                            },
                            same_request,
                            |_: &Receipt<'_>, _: &str, _: MailAddr| Ok::<_, Never>(()),
                            |_: &Receipt<'_>, _: &str, _: MailAddr| Ok::<_, Never>(()),
                        );
                let target = application.root().established_recipient();
                let controls = export
                    .preflight(&application, &target, &mut Vec::new())
                    .unwrap();
                <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::declare(
                    &mut export,
                    &session,
                )
                .await
                .expect("actual native declaration");
                let mut replies = Vec::new();
                for (index, original) in [&records[0], &records[0], &records[1]]
                    .into_iter()
                    .enumerate()
                {
                    let receiving = session
                        .get("bombay/pending-comparison")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(original.protected.to_vec())
                        .attachment(original.proof.to_vec())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(20))
                        .await
                        .expect("actual native query");
                    replies.push(receiving);
                    // Setup only: leave the exact native Query/permit originals in their queue.
                    while export.active.as_ref().unwrap().0.len() != index + 1 {
                        tokio::task::yield_now().await;
                    }
                }
                let (first, snapshot, observed) = {
                    // Select an unconstrained unit host for this finite, prequeued poll.
                    // This comparison does not establish ordinary runtime fairness.
                    let serving =
                        <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::serve(
                            &mut export,
                            controls,
                            runtime.clone(),
                            uncancelled,
                        );
                    let mut serving = pin!(unconstrained(serving));
                    let first = poll_fn(|cx| Poll::Ready(serving.as_mut().poll(cx))).await;
                    let snapshot = (protection.get(), comparisons.borrow().clone());
                    let released = release.send(());
                    let exchange = async {
                        let mut observed = Vec::new();
                        for receiving in &replies {
                            let mut originals = Vec::new();
                            while let Ok(original) = receiving.recv_async().await {
                                originals.push(original);
                            }
                            observed.push(originals);
                        }
                        let admission = values
                            .send(&target, CounterMessage::Read(values.recipient()))
                            .await;
                        let native_value = values.receive().await;
                        let shutdown = application.shutdown();
                        (observed, admission, native_value, shutdown)
                    };
                    let observed = tokio::select! {
                        biased;
                        observed = exchange => observed,
                        () = serving.as_mut() => panic!("serving remains live through the actual exchange"),
                    };
                    (first, snapshot, (released, observed))
                };
                <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::withdraw(
                    &mut export,
                )
                .await;
                (export, first, snapshot, observed)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Application host is entered"));
    let (work, root_report, native, notifications, ()) = outcome.into_parts();
    let ((export, first, snapshot, (released, (replies, admission, value, shutdown))), cleanup) =
        work.try_into_completed()
            .unwrap_or_else(|_| panic!("actual Work and native cleanup complete"));
    let closed = session.close().await;
    drop(cancellation);
    cleanup.expect("the actual Application cleanup joins");
    closed.expect("the independent native Session closes");
    let (origin, root) = native.expect("original native root remains acquired");
    let root_report = root_report.expect("the runtime retains its actual root report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(root.custody().is_some());
    let notifications = notifications.expect("both actual receipts remain acquired");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    let (_, _, startup, withdrawal, diagnostics, entries) = export.into_parts();
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(entries.len(), 3);
    assert_eq!(first, Poll::Pending);
    assert_eq!(snapshot.0, Protection::Pending);
    assert!(released.is_ok() && shutdown.is_ok() && admission.is_ok());
    let value = value.expect("independent actual Counter reply");
    assert_eq!(value.message, 1);
    assert_eq!((decoded.get(), encoded.get(), protected.get()), (1, 1, 1));
    assert_eq!(replies.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1, 0]);
    for reply in replies.iter().flatten() {
        let received = reply.result();
        let original = received.as_ref().expect("actual successful native reply");
        assert_eq!(original.payload().to_bytes().as_ref(), [43]);
        assert_eq!(original.attachment().unwrap().to_bytes().as_ref(), [47]);
    }
    assert!(matches!(
        *entries[0].custody.identity.borrow(),
        Identity::Canonical(_)
    ));
    assert!(matches!(
        *entries[1].custody.identity.borrow(),
        Identity::Duplicate(_)
    ));
    assert!(matches!(
        *entries[2].custody.refusal.borrow(),
        Some(ExportError::Conflict)
    ));
    assert_eq!(
        snapshot.1,
        [(23, 23, Protection::Pending), (23, 23, Protection::Pending)]
    );
}
