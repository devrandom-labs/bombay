//! Withdrawal authority, original results and actual pending-query retirement.
use super::super::query::Exports;
use super::counter::{Counter, CounterIncremented, CounterMessage, CounterProcessed, CounterValue};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, current_scope, limits, metadata, recorded,
};
use super::{
    Attempt, Command, Event, Export, ExportError, ExportLimits, RefusalRetention, completed_actor,
    reply, request_originals,
};
use crate::actors::ActorExt;
use crate::observe;
use crate::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
use behavior::{EstablishedRecipient, Never, User};
use core::future::{Future, IntoFuture, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use futures_util::FutureExt;
use futures_util::future::join;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Wake;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::oneshot;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

// This unbound owner exercises only authority/result operations. The Node
// isolation witness separately exercises the real constructor and interpreter.
fn unbound_export() -> Export<(), (), ()> {
    let (authority, intent) = observe::pair();
    let (publication, withdrawal) = observe::pair();
    Export {
        inputs: (),
        declarations: (),
        startup: None,
        withdrawal,
        withdrawal_publication: Some(publication),
        withdrawal_authority: Some(authority),
        withdrawal_intent: intent,
        ingress: Arc::new(Mutex::new(Vec::new())),
        entries: RefCell::new(Vec::new()),
        declaration: RefCell::new(None),
        active: None,
    }
}

#[derive(Debug, Error)]
#[error("private native withdrawal detail")]
struct WithdrawalRefusal {
    original: Box<[u8]>,
    disposals: Arc<AtomicUsize>,
}

impl Drop for WithdrawalRefusal {
    fn drop(&mut self) {
        self.disposals.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn borrowed_withdrawal_wait_keeps_the_original_refusal() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let duplicate = export.take_withdrawal();
    let operation = withdraw();
    assert!(duplicate.is_none());
    assert_eq!(intent.try_get(), None);
    let mut operation = pin!(operation);
    let mut context = Context::from_waker(Waker::noop());
    let first = operation.as_mut().poll(&mut context);
    assert!(first.is_pending());
    assert_eq!(intent.try_get(), Some(()));
    let cancelled = operation.as_mut().now_or_never();
    assert!(cancelled.is_none());

    let disposals = Arc::new(AtomicUsize::new(0));
    let original = Box::new(WithdrawalRefusal {
        original: Box::from([137_u8, 139]),
        disposals: Arc::clone(&disposals),
    });
    let error_address = ptr::from_ref(original.as_ref());
    let payload_address = original.original.as_ptr();
    let original: Box<dyn Error + Send + Sync> = original;
    export.complete_withdrawal(Some(Err(original)));
    let observed = operation.as_mut().poll(&mut context);
    let Poll::Ready(Some(Err(observed))) = observed else {
        panic!("the retained operation observes the actual acquired refusal");
    };
    // Repeat finalization cannot replace the acquired Err with settled absence.
    export.complete_withdrawal(None);
    let final_result = export.into_parts().3.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(&observed, &final_result));
    assert!(matches!(observed.as_ref(), ExportError::Withdrawal { .. }));
    assert_eq!(observed.to_string(), "network withdrawal was refused");
    assert_eq!(
        format!("{observed:?}"),
        "ExportError(network withdrawal was refused)"
    );
    let source = observed
        .source()
        .expect("the root retains its original native cause");
    let source = source.downcast_ref::<WithdrawalRefusal>().unwrap();
    assert!(ptr::eq(source, error_address));
    assert_eq!(source.original.as_ptr(), payload_address);
    assert_eq!(source.original.as_ref(), [137, 139]);
    assert_eq!(source.to_string(), "private native withdrawal detail");
    assert_eq!(disposals.load(Ordering::SeqCst), 0);
    let forward =
        |withdrawal: Result<(), Arc<ExportError>>| -> Result<(), Box<dyn Error + Send + Sync>> {
            withdrawal?;
            Ok(())
        };
    let forwarded = forward(Err(Arc::clone(&observed))).unwrap_err();
    let shared = forwarded.downcast_ref::<Arc<ExportError>>().unwrap();
    assert!(Arc::ptr_eq(shared, &observed));
    let source = forwarded
        .source()
        .unwrap()
        .downcast_ref::<WithdrawalRefusal>()
        .unwrap();
    assert!(ptr::eq(source, error_address));
    drop(forwarded);
    drop(observed);
    assert_eq!(disposals.load(Ordering::SeqCst), 0);
    drop(final_result);
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
}

#[test]
fn unstarted_withdrawal_and_owner_surrender_publish_no_result() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let completion = export.withdrawal.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    drop(operation);
    assert_eq!(intent.try_get(), None);
    drop(export);
    let observed = completion.into_future().now_or_never();
    assert!(observed.is_none());
}

#[test]
fn started_operation_surrender_keeps_the_owner_result() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    {
        let mut operation = pin!(operation);
        let mut context = Context::from_waker(Waker::noop());
        let polled = operation.as_mut().poll(&mut context);
        assert!(polled.is_pending());
    }
    assert_eq!(intent.try_get(), Some(()));
    export.complete_withdrawal(Some(Ok(())));
    let retained = export.into_parts().3;
    assert!(matches!(retained, Some(Ok(()))));
}

#[test]
fn settled_absence_differs_from_pending_withdrawal() {
    let mut export = unbound_export();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let mut operation = pin!(operation);
    let mut context = Context::from_waker(Waker::noop());
    let pending = operation.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    export.complete_withdrawal(None);
    let observed = operation.as_mut().poll(&mut context);
    assert!(matches!(observed, Poll::Ready(None)));
    let retained = export.into_parts().3;
    assert!(retained.is_none());
}

#[test]
fn late_withdrawal_reads_the_completed_native_success() {
    let mut export = unbound_export();
    export.complete_withdrawal(Some(Ok(())));
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let observed = operation.now_or_never();
    assert!(matches!(observed, Some(Some(Ok(())))));
    export.complete_withdrawal(None);
    let retained = export.into_parts().3;
    assert!(matches!(retained, Some(Ok(()))));
}

struct WithdrawalWake;

impl Wake for WithdrawalWake {
    fn wake(self: Arc<Self>) {
        panic!("withdrawal observer notification panicked");
    }
}

#[test]
fn observer_notification_panic_keeps_the_original_native_refusal() {
    let mut export = unbound_export();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let mut operation = pin!(operation);
    let waker = Waker::from(Arc::new(WithdrawalWake));
    let mut context = Context::from_waker(&waker);
    let pending = operation.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    let original: Box<dyn Error + Send + Sync> = Box::new(WithdrawalRefusal {
        original: Box::from([149_u8, 151]),
        disposals: Arc::new(AtomicUsize::new(0)),
    });
    let address = ptr::from_ref(original.as_ref());
    let notification = catch_unwind(AssertUnwindSafe(|| {
        export.complete_withdrawal(Some(Err(original)));
    }));
    assert!(notification.is_err());
    let mut context = Context::from_waker(Waker::noop());
    let observed = operation.as_mut().poll(&mut context);
    let Poll::Ready(Some(Err(observed))) = observed else {
        panic!("publication retains the actual refusal before waking the observer");
    };
    export.complete_withdrawal(None);
    let retained = export.into_parts().3.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(&observed, &retained));
    assert!(matches!(retained.as_ref(), ExportError::Withdrawal { .. }));
    let source = retained
        .source()
        .expect("the acquired native cause survives notification panic");
    assert!(ptr::addr_eq(source, address));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one native cutoff trace retains pending provider, original request, allocation and actor outcomes through actual cleanup"
)]
async fn requested_withdrawal_releases_the_pending_query_allocation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let session = zenoh::open(configuration("tls/localhost:0", &EndpointRole::Receiver).to_configuration().unwrap())
            .await.expect("the real native Session opens");
        let outcome = Application::new(Counter::new().stop_on_shutdown())
            .run_with(|application| {
                let session = &session;
                async move {
                    let target = application.root().established_recipient();
                    let origin = application.actor_address(&target).unwrap();
                    let runtime = session.zid().to_string();
                    let mut claims = metadata(ProviderProfile::Recorded, "counter", runtime.clone(), runtime.clone());
                    claims.actor_generation = origin.0;
                    let sequence = claims.request_sequence;
                    let binding = Binding {
                        deployment: "two-node-read-study", caller: "caller", controller: "owner",
                        actor: "counter", host: "receiver", protocol: "counter",
                        target_assignment: "root", logical_session: 431,
                    };
                    let record = ProtectedMessage {
                        protected: serde_json::to_vec(&ReadRequestRef {
                            bombay: &claims, kind: Purpose::Request, operation: Command::Increment,
                        }).unwrap().into_boxed_slice(),
                        proof: Box::from([173_u8, 179]),
                    };
                    let originals = RefCell::new(None);
                    let encoded = Cell::new(None);
                    let (mut release, gate) = oneshot::channel::<()>();
                    let gate = RefCell::new(Some(gate));
                    let (entered, pending_protection) = oneshot::channel();
                    let entered = RefCell::new(Some(entered));
                    let deadline = Instant::now() + Duration::from_secs(25);
                    let current = |receipt: &recorded::Receipt<Command>, runtime: &str, actor: MailAddr| {
                        let scope = recorded::projection(receipt);
                        if scope.bombay.runtime_generation.as_ref() != runtime
                            || scope.bombay.actor_generation != actor.0
                        { return Err(VerificationError::Invalid); }
                        current_scope(scope, &claims, &Command::Increment, Purpose::Request, deadline)
                    };
                    let maximum = NonZeroUsize::new(2).unwrap();
                    let mut export = Export::new(
                        "bombay/withdrawal/allocation",
                        ExportLimits {
                            refusals: RefusalRetention::Retain, queued_queries: maximum,
                            retained_queries: maximum, retained_requests: maximum, diagnostics: maximum,
                        },
                        limits(),
                        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<Command>>(bytes)
                            .map(|request| request.operation),
                        reply(
                            |command| match command {
                                Command::Increment => Ok(move |reply_to: EstablishedRecipient<CounterProcessed>| {
                                    CounterMessage::IncrementAndReply { request_sequence: sequence, reply_to }
                                }),
                                command @ Command::Read => Err(command),
                            },
                            |_: &recorded::Receipt<Command>, original: &User<MailAddr, CounterIncremented>| {
                                let encoding = original.message.request_sequence.to_be_bytes().to_vec().into_boxed_slice();
                                encoded.set(Some(encoding.as_ptr()));
                                Ok::<_, Never>(encoding)
                            },
                        ),
                        async |bytes: &[u8], proof: &[u8]| {
                            let original = ProtectedMessage { protected: bytes.into(), proof: proof.into() };
                            let verified = recorded::verify::<Command>(&original, slice::from_ref(&record), binding).await;
                            if let Ok(receipt) = &verified {
                                *originals.borrow_mut() = Some(request_originals(bytes, proof, receipt));
                            }
                            verified
                        },
                        async |_: &[u8], _: &recorded::Receipt<Command>| {
                            let original = gate.borrow_mut().take().expect("one actual protection producer");
                            let mut original = pin!(original);
                            poll_fn(|context| {
                                let observed = original.as_mut().poll(context);
                                if observed.is_pending()
                                    && let Some(publication) = entered.borrow_mut().take()
                                {
                                    let published = publication.send(());
                                    published.expect("the real Pending poll has an independent observer");
                                }
                                observed
                            }).await.expect("only an explicit release could complete protection");
                            Ok::<_, Never>(Box::<[u8]>::from([181_u8]))
                        },
                        |left, right| recorded::identity(left) == recorded::identity(right),
                        &current, &current,
                    );
                    let controls = export.preflight(&application, &target, &mut Vec::new()).unwrap();
                    <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::declare(&mut export, session)
                        .await.expect("the production declaration opens");
                    let allocation = export.active.as_ref().unwrap().1.clone();
                    let declared_owners = allocation.strong_count();
                    let querier = session.declare_querier("bombay/withdrawal/allocation")
                        .allowed_destination(Locality::SessionLocal).await.unwrap();
                    let matching_before = querier.matching_status().await;
                    let receiving = session.get("bombay/withdrawal/allocation")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None).timeout(Duration::from_secs(5))
                        .payload(record.protected.to_vec()).attachment(record.proof.to_vec())
                        .await.expect("the actual local Query finishes its production callback");
                    let charged = allocation.upgrade().map(|owner| owner.available_permits());
                    let withdraw = export.take_withdrawal().unwrap();
                    // This fixture never publishes or certifies Application cleanup.
                    let (cancellation, uncancelled) = oneshot::channel::<()>();
                    let (observed, ()) = join(
                        async {
                            pending_protection.await.expect("protection actually returned Pending after the native actor reply");
                            let mut operation = pin!(withdraw());
                            let pending = poll_fn(|context| Poll::Ready(operation.as_mut().poll(context))).await;
                            let cancelled_wait = operation.as_mut().now_or_never();
                            let withdrawal = operation.await;
                            (pending, cancelled_wait, withdrawal)
                        },
                        <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::serve(
                            &mut export, controls, runtime, uncancelled.shared(),
                        ),
                    ).await;
                    // Capture BEFORE extraction or repeated withdrawal can release a missed Query.
                    let retained_allocation = allocation.upgrade().map(|owner| owner.available_permits());
                    release.closed().await;
                    let surrendered_release = release.send(());
                    let matching_after = querier.matching_status().await;
                    let inspection = querier.undeclare().await;
                    let mut replies = Vec::new();
                    let ending = loop {
                        match receiving.recv_async().await {
                            Ok(original) => replies.push(original),
                            Err(ending) => break ending,
                        }
                    };
                    <_ as Exports<Counter, Event, (), EstablishedRecipient<Counter>>>::withdraw(&mut export).await;
                    drop(cancellation);
                    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
                    let originals = originals.into_inner().expect("verification acquired exact original buffers");
                    let mut values = application.client::<CounterValue>().unwrap();
                    let first_admission = values.send(CounterMessage::Read(values.recipient())).await;
                    let first_value = values.receive().await;
                    let increment = values.send(CounterMessage::Increment).await;
                    let second_admission = values.send(CounterMessage::Read(values.recipient())).await;
                    let second_value = values.receive().await;
                    let shutdown = application.shutdown();
                    (
                        (startup, withdrawal, diagnostics, requests),
                        (origin, sequence, record, originals, encoded.get()),
                        (declared_owners, maximum, charged, retained_allocation), observed,
                        (matching_before, matching_after, inspection), (replies, ending, surrendered_release),
                        (first_admission, first_value, increment, second_admission, second_value, shutdown),
                    )
                }
            }).await.unwrap_or_else(|_| panic!("the real Counter Application starts"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (observations, cleanup) = work.try_into_completed()
            .unwrap_or_else(|_| panic!("Work and actual Application cleanup complete"));
        let closed = session.close().await;
        let (
            (startup, withdrawal, diagnostics, requests),
            (origin, sequence, record, originals, encoded),
            (declared_owners, maximum, charged, retained_allocation),
            (pending, cancelled_wait, observed),
            (matching_before, matching_after, inspection), (replies, _ending, surrendered_release),
            (first_admission, first_value, increment, second_admission, second_value, shutdown),
        ) = observations;
        let first_value = first_value.expect("the same live Counter answers after withdrawal");
        let second_value = second_value.expect("the same live Counter completes further work");
        assert_eq!((first_value.from, first_value.message), (origin, 1));
        assert_eq!((second_value.from, second_value.message), (origin, 2));
        assert!(first_admission.is_ok() && increment.is_ok() && second_admission.is_ok() && shutdown.is_ok());
        assert!(cleanup.is_ok() && closed.is_ok());
        let report = report.expect("the actual root report remains");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let (root_origin, root) = native.expect("the exact native Counter remains");
        assert_eq!(root_origin.address(), origin);
        let _counter = completed_actor(root_origin, root);
        let notifications = notifications.expect("both real notification results remain");
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(pending.is_pending() && cancelled_wait.is_none());
        assert!(matches!(&observed, Some(Ok(()))) && matches!(&withdrawal, Some(Ok(()))));
        assert!(matches!(&startup, Some(Ok(()))) && diagnostics.is_empty());
        let before = matching_before.unwrap().matching();
        let after = matching_after.unwrap().matching();
        assert!(before && !after && inspection.is_ok());
        assert!(replies.is_empty() && surrendered_release.is_err());
        assert_eq!(requests.len(), 1);
        let request = requests.into_iter().next().unwrap().try_into_parts()
            .unwrap_or_else(|_| panic!("surrendered producers release their original Request loan"));
        assert!(request.0.is_some());
        assert_eq!((request.1.0.as_ref(), request.1.1.as_ref()), (record.protected.as_ref(), record.proof.as_ref()));
        let receipt = request.2.as_ref().unwrap().as_ref().unwrap();
        let extracted = request_originals(&request.1.0, &request.1.1, receipt);
        assert_eq!(extracted, originals);
        assert!(request.3.is_none());
        let (construction, client, admission, processed, tail) = request.4.unwrap();
        assert!(construction.is_none() && matches!(&client, Attempt::Returned(Ok(_))));
        assert!(matches!(&admission, Attempt::Returned(Ok(()))));
        let expected = Some(Some(User::new(origin, CounterIncremented { request_sequence: sequence })));
        assert_eq!(processed, expected);
        assert_eq!(tail, [None]);
        let encoding = request.5.unwrap().unwrap();
        assert_eq!(encoding.as_ref(), sequence.to_be_bytes());
        assert_eq!(Some(encoding.as_ptr()), encoded);
        assert!(request.6.is_none() && request.7.is_empty());
        assert!(matches!(&request.8, (None, None, None)));
        assert!(matches!(&request.9, (None, None, None, None, None, None)));
        assert!(declared_owners > 0);
        assert_eq!(charged, Some(maximum.get() - 1));
        // This measures one original allocation, not all tasks or callback quiescence.
        assert!(retained_allocation.is_none(), "pending withdrawal releases the original query-count allocation before extraction");
    }).await.expect("watchdog only; actual native, actor and cleanup outcomes prove the law");
}
