//! Coexisting decoder and withdrawal notification causes retain joined cleanup.
use super::super::custody::completed_actor;
use super::super::protocol::{
    Binding, CounterOperation, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply,
    ReadReplyRef, ReadRequest, ReadRequestRef, VerificationError, configuration, limits, metadata,
    recorded,
};
use super::super::{Counter, CounterMessage};
use super::actors::{ActorExports, Creator, Exports as CreatorExports, topology};
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationScope};
use crate::behavior::{BehaviorBase, Never, User};
use crate::remote::{Attempt, Export, ExportLimits, RefusalRetention, reply, submission};
use crate::{
    ActorFailureAssessment, ActorRetirement, MailAddr, RetirementAssessment, RetirementOutcome,
    TrySendError, WorkOutcome,
};
use core::future::{Future, pending, poll_fn};
use core::task::{Context, Poll};
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

struct WithdrawalWake {
    original: Mutex<Option<Box<u64>>>,
    calls: AtomicUsize,
}

impl Wake for WithdrawalWake {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let original = self.original.lock().unwrap().take();
        if let Some(original) = original {
            panic_any(original);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "a real healthy Node establishes the extraction preconditions before independently owned panic payloads test consuming custody"
)]
#[expect(
    clippy::result_large_err,
    reason = "the owning reply selector requires the complete original ReadRequest on rejection"
)]
async fn joined_node_extraction_transfers_each_original_panic() {
    timeout(Duration::from_secs(40), async {
        let binding = Binding {
            deployment: "node-extraction",
            caller: "caller",
            controller: "owner",
            actor: "counter",
            host: "receiver",
            protocol: "counter",
            target_assignment: "root",
            logical_session: 467,
        };
        // No requests are registered; unsolicited traffic is denied by the real verifier.
        let records: [ProtectedMessage; 0] = [];
        let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
        let maximum = NonZeroUsize::new(2).unwrap();
        let current =
            |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
                let scope = recorded::projection(receipt);
                if scope.kind == Purpose::Request
                    && scope.operation == CounterOperation::Read
                    && scope.bombay.runtime_generation.as_ref() == runtime
                    && scope.bombay.actor_generation == target.0
                    && recorded::identity(receipt).0 == &binding
                {
                    Ok(())
                } else {
                    Err(VerificationError::Invalid)
                }
            };
        let export = Export::new(
            "bombay/node-extraction/counter",
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes),
            reply(
                |request: ReadRequest<CounterOperation>| match request.operation {
                    CounterOperation::Read => {
                        Ok::<_, ReadRequest<CounterOperation>>(CounterMessage::Read)
                    }
                },
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    serde_json::to_vec(&ReadReplyRef {
                        bombay: &recorded::projection(receipt).bombay,
                        kind: Purpose::ReadReply,
                        operation: CounterOperation::Read,
                        value: &original.message,
                    })
                    .map(Vec::into_boxed_slice)
                },
            ),
            async |bytes: &[u8], proof: &[u8]| {
                recorded::verify::<CounterOperation>(
                    &ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    },
                    &records,
                    binding,
                )
                .await
            },
            async |bytes: &[u8], receipt: &recorded::Receipt<CounterOperation>| {
                let reply: ReadReply<u64, CounterOperation> =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                if reply.bombay != recorded::projection(receipt).bombay
                    || reply.kind != Purpose::ReadReply
                    || reply.operation != CounterOperation::Read
                {
                    return Err(VerificationError::Invalid);
                }
                let mut replies = protected_replies.borrow_mut();
                if replies.len() >= maximum.get() {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([13, 17]);
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
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("the real extraction-only Counter Node opens"));
        let mut node = node.exports(export, async |application| {
            Ok::<_, VerificationError>(application.root().established_recipient())
        });
        let outcome = node
            .run_with(|application| async move {
                let target = application.root().established_recipient();
                let stopped = application.shutdown();
                let retired = application.lifecycle().retirement().await;
                (target, stopped, retired)
            })
            .await
            .unwrap_or_else(|_| panic!("the real healthy Node completes before custody seeding"));
        assert!(matches!(&node.closed, Some(Ok(()))) && node.session.is_closed());
        assert!(outcome.cleanup().is_some_and(Result::is_ok));
        let healthy = outcome
            .root_report()
            .expect("actual healthy retirement precedes custody seeding");
        assert_eq!(healthy.retirement(), RetirementAssessment::Established);
        assert_eq!(healthy.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(node.poll_panic.is_none() && node.disposal_panic.is_none());
        assert!(node.withdrawal_poll_panic.is_none() && node.withdrawal_disposal_panic.is_none());

        // Extraction-only custody proof: these are preowned originals, not induced native failures.
        let polling = Box::new(401_u64);
        let disposal = Box::new([2_u8, 3, 5]);
        let withdrawal_polling = Box::new(409_u32);
        let withdrawal_disposal = Box::new([11_u16, 13]);
        let polling_allocation = ptr::from_ref(polling.as_ref());
        let disposal_allocation = ptr::from_ref(disposal.as_ref());
        let withdrawal_polling_allocation = ptr::from_ref(withdrawal_polling.as_ref());
        let withdrawal_disposal_allocation = ptr::from_ref(withdrawal_disposal.as_ref());
        node.poll_panic = Some(Box::new(polling));
        node.disposal_panic = Some(Box::new(disposal));
        node.withdrawal_poll_panic = Some(Box::new(withdrawal_polling));
        node.withdrawal_disposal_panic = Some(Box::new(withdrawal_disposal));
        let (export, binding, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            node.into_parts().unwrap_or_else(|_| {
                panic!("the consuming public method transfers all seven values")
            });
        let polling = polling.expect("execution poll original transfers");
        let polling = polling
            .downcast_ref::<Box<u64>>()
            .expect("execution poll keeps its concrete original type");
        let disposal = disposal.expect("execution disposal original transfers");
        let disposal = disposal
            .downcast_ref::<Box<[u8; 3]>>()
            .expect("execution disposal keeps its concrete original type");
        let withdrawal_polling = withdrawal_polling.expect("withdrawal poll original transfers");
        let withdrawal_polling = withdrawal_polling
            .downcast_ref::<Box<u32>>()
            .expect("withdrawal poll keeps its concrete original type");
        let withdrawal_disposal =
            withdrawal_disposal.expect("withdrawal disposal original transfers");
        let withdrawal_disposal = withdrawal_disposal
            .downcast_ref::<Box<[u16; 2]>>()
            .expect("withdrawal disposal keeps its concrete original type");
        assert_eq!(
            (ptr::from_ref(polling.as_ref()), **polling),
            (polling_allocation, 401)
        );
        assert_eq!(
            (ptr::from_ref(disposal.as_ref()), **disposal),
            (disposal_allocation, [2, 3, 5])
        );
        assert_eq!(
            (
                ptr::from_ref(withdrawal_polling.as_ref()),
                **withdrawal_polling
            ),
            (withdrawal_polling_allocation, 409)
        );
        assert_eq!(
            (
                ptr::from_ref(withdrawal_disposal.as_ref()),
                **withdrawal_disposal
            ),
            (withdrawal_disposal_allocation, [11, 13])
        );
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let ((expected, stopped, retired), cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("healthy Work and cleanup remain original"));
        let target = binding
            .expect("binding was invoked")
            .expect("the exact root binding succeeded");
        assert_eq!(target, expected);
        assert_eq!(report, Some(retired));
        assert!(closed.is_ok() && cleanup.is_ok() && stopped.is_ok());
        let (origin, native) = native.expect("the original healthy native root remains");
        let _counter = completed_actor(origin, native);
        let notifications = notifications.expect("both healthy notification receipts remain");
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        let (_, _, declared, withdrawn, diagnostics, requests) = export.into_parts();
        assert!(matches!(declared, Some(Ok(()))) && matches!(withdrawn, Some(Ok(()))));
        assert!(
            diagnostics.is_empty() && requests.is_empty() && protected_replies.borrow().is_empty()
        );
    })
    .await
    .expect("healthy finalization and consuming custody finish within the watchdog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one causal two-export trace retains both original panic allocations and every independent cleanup fact"
)]
async fn decoder_and_withdrawal_notification_panics_keep_joined_node_finalization() {
    Box::pin(timeout(Duration::from_secs(40), async {
        let records = RefCell::new(Vec::<ProtectedMessage>::new());
        let joined = RefCell::new(None);
        let retirement = RefCell::new(None);
        let addresses = RefCell::new(None);
        let binding = Binding {
            deployment: "withdrawal-notification", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "children", logical_session: 457,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<()>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &records.borrow(), binding,
            ).await
        };
        let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
            { Ok(()) } else { Err(()) }
        };
        let encode = |_: &recorded::Receipt<()>, _: &Attempt<Result<(), TrySendError<CounterMessage>>>| {
            Ok::<_, Never>(Box::<[u8]>::from([179_u8]))
        };
        let protect = async |_: &[u8], _: &recorded::Receipt<()>| {
            Ok::<_, Never>(Box::<[u8]>::from([181_u8]))
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let bounds = ExportLimits {
            refusals: RefusalRetention::Retain, queued_queries: maximum,
            retained_queries: maximum, retained_requests: maximum, diagnostics: maximum,
        };
        let mut sibling = Export::new(
            "bombay/notification/sibling", bounds, limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<()>>(bytes).map(|request| request.operation),
            submission(|()| Ok::<_, ()>(|| CounterMessage::Increment), &encode),
            &verify, &protect,
            |left, right| recorded::identity(left) == recorded::identity(right), &current, &current,
        );
        let withdraw = sibling.take_withdrawal().expect("one real sibling withdrawal authority");
        let observation = RefCell::new(Box::pin(withdraw()));
        let notification = Box::new(463_u64);
        let notification_allocation = ptr::from_ref(notification.as_ref());
        let wake = Arc::new(WithdrawalWake {
            original: Mutex::new(Some(notification)), calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&wake));
        let first_poll = RefCell::new(None);
        let decoder = Box::new(461_u64);
        let decoder_allocation = ptr::from_ref(decoder.as_ref());
        let decoder = RefCell::new(Some(decoder));
        let first = Export::new(
            "bombay/notification/decoder", bounds, limits(),
            |_: &[u8]| -> Result<(), Never> {
                let mut observation = observation.borrow_mut();
                let mut context = Context::from_waker(&waker);
                let observed = observation.as_mut().poll(&mut context);
                *first_poll.borrow_mut() = Some(observed);
                let original = decoder.borrow_mut().take().expect("one original decoder cause");
                panic_any(original);
            },
            submission(|()| Ok::<_, ()>(|| CounterMessage::Increment), &encode),
            &verify, &protect,
            |left, right| recorded::identity(left) == recorded::identity(right), &current, &current,
        );
        let scope = ApplicationScope::new();
        let mut exports = scope.external::<CreatorExports>().unwrap();
        let application = scope.with_topology(
            Creator::default().stop_on_shutdown(), topology::Topology::new(),
        );
        let node = application.open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
            .await.unwrap_or_else(|_| panic!("the actual two-export Node opens"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let (configured, configuration_received) = oneshot::channel();
        let mut node = node.exports((first, sibling), async |application| {
            let admitted = exports.send(
                &application.root().established_recipient(), exports.recipient(),
            ).await;
            admitted.expect("Creator receives its real proof request");
            let original = exports.receive().await.expect("the original child proofs arrive");
            let ActorExports { counter, sibling, ledger } = &original.message;
            *addresses.borrow_mut() = Some([
                application.actor_address(&counter.recipient()).unwrap(),
                application.actor_address(&sibling.recipient()).unwrap(),
                application.actor_address(&ledger.recipient()).unwrap(),
            ]);
            let target = counter.recipient();
            let mut claims = metadata(
                ProviderProfile::Recorded, binding.protocol, runtime.clone(), runtime.clone(),
            );
            claims.deployment = binding.deployment.into();
            claims.caller = binding.caller.into();
            claims.controller = binding.controller.into();
            claims.actor = binding.actor.into();
            claims.host = binding.host.into();
            claims.actor_generation = application.actor_address(&target).unwrap().0;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &claims, kind: Purpose::Request, operation: &(),
            }).unwrap().into_boxed_slice();
            let proof = Box::<[u8]>::from([173_u8]);
            records.borrow_mut().push(ProtectedMessage { protected: protected.clone(), proof: proof.clone() });
            *joined.borrow_mut() = application.cleanup();
            *retirement.borrow_mut() = Some(application.lifecycle().retirement());
            let published = configured.send((protected, proof));
            published.expect("Work awaits the actual configured request");
            Ok::<_, Never>((counter.clone(), sibling.clone()))
        });
        let mut operation = Box::pin(node.run_with(|_| async {
            let (protected, proof) = configuration_received.await.unwrap();
            let incoming = session.get("bombay/notification/decoder")
                .allowed_destination(Locality::SessionLocal)
                .payload(protected.into_vec()).attachment(proof.into_vec())
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20)).await
                .expect("a genuine native query enters the first decoder");
            pending::<()>().await;
            incoming
        }));
        let caught = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                original => Poll::Ready(original),
            }
        }).await;
        let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
        let cleanup = joined.take().expect("binding retained independent cleanup").await;
        let retired = retirement.take().expect("binding retained independent retirement").await;
        let acquired_close = node.closed.as_ref().map(|closed| match closed {
            Ok(()) => Ok(()),
            Err(cause) => Err(ptr::from_ref(cause.as_ref())),
        });
        let closed_before_rescue = session.is_closed();
        let rescue = if acquired_close.is_none() { Some(node.close().await) } else { None };
        let mut observation = observation.borrow_mut();
        let mut context = Context::from_waker(Waker::noop());
        let withdrawn = observation.as_mut().poll(&mut context);
        let original_decoder = node.poll_panic.as_ref()
            .and_then(|cause| cause.downcast_ref::<Box<u64>>());
        assert!(cleanup.is_ok() && disposed.is_ok() && session.is_closed());
        assert!(rescue.as_ref().is_none_or(Result::is_ok));
        assert_eq!(retired.retirement(), RetirementAssessment::Established);
        assert!(matches!(*first_poll.borrow(), Some(Poll::Pending)));
        assert!(matches!(withdrawn, Poll::Ready(Some(Ok(())))));
        assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
        let original_decoder = original_decoder.expect("Node keeps the original decoder cause");
        assert_eq!(ptr::from_ref(original_decoder.as_ref()), decoder_allocation);
        assert_eq!(**original_decoder, 461);
        if let Err(original) = &caught {
            let original = original.downcast_ref::<Box<u64>>()
                .expect("the second notification cause is the actual escaped original");
            assert_eq!(ptr::from_ref(original.as_ref()), notification_allocation);
            assert_eq!(**original, 463);
        }
        eprintln!(
            "before rescue: outcome returned={}, acquired close={acquired_close:?}, native closed={closed_before_rescue}",
            matches!(&caught, Ok(Poll::Ready(Ok(_)))),
        );
        assert!(
            matches!(&caught, Ok(Poll::Ready(Ok(_)))) && matches!(acquired_close, Some(Ok(()))) && closed_before_rescue,
            "coexisting decoder and withdrawal notification panics must preserve the acquired outcome and joined Node close before rescue"
        );
        let Ok(Poll::Ready(Ok(outcome))) = caught else {
            panic!("the acquired Application outcome survives both original causes");
        };
        let ((first, sibling), binding, closed, poll_panic, disposal_panic,
            withdrawal_poll_panic, withdrawal_disposal_panic) = node.into_parts()
            .unwrap_or_else(|_| panic!("joined Node transfers all seven original values"));
        let notification = withdrawal_poll_panic
            .expect("successful finalization retains the original withdrawal notification cause");
        let notification = notification.downcast_ref::<Box<u64>>()
            .expect("the retained notification payload keeps its original concrete type");
        let polling = poll_panic.expect("the original execution poll cause also transfers");
        let polling = polling.downcast_ref::<Box<u64>>().unwrap();
        assert_eq!(
            (ptr::from_ref(polling.as_ref()), ptr::from_ref(notification.as_ref())),
            (decoder_allocation, notification_allocation),
            "the two independent original allocations survive finalization and transfer",
        );
        assert_eq!((**polling, **notification), (461, 463));
        assert!(disposal_panic.is_none() && withdrawal_disposal_panic.is_none());
        assert!(closed.is_ok() && rescue.is_none());
        let Some(Ok((bound_first, bound_sibling))) = binding else {
            panic!("the exact two successful binding proofs remain owned");
        };
        let (work, report, native, notifications, ()) = outcome.into_parts();
        assert!(matches!(work, WorkOutcome::Interrupted { cleanup: Ok(()) }));
        assert_eq!(report, Some(retired));
        assert_eq!(retired.failures(), ActorFailureAssessment::NoFailuresFound);
        let notifications = notifications.expect("both actual root notification receipts transfer");
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        let (origin, native) = native.expect("the original Creator and all child retirements transfer");
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Retained {
            custody, outcome: RetirementOutcome::OwnerCancelled { .. },
        } = native else {
            panic!("execution disposal retains the exact owner-cancelled Creator outcome");
        };
        assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
        assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
        let creator = custody.behavior.base();
        assert_eq!(creator.counters.len(), 2);
        assert!(creator.ledger.is_some());
        assert_eq!(
            (creator.counters[0].recipient(), creator.counters[1].recipient()),
            (bound_first.recipient(), bound_sibling.recipient()),
        );
        let addresses = addresses.borrow().expect("binding recorded all three actual child addresses");
        assert_eq!(custody.descendants.len(), 3);
        let mut counters = Vec::new();
        let mut ledgers = Vec::new();
        for child in &custody.descendants {
            match child {
                topology::RootRetirements::Counter { origin, retirement } => {
                    counters.push(origin.address());
                    let ActorRetirement::Retained {
                        custody, outcome: RetirementOutcome::OwnerCancelled { .. },
                    } = retirement.as_ref() else {
                        panic!("each original Counter retains its actual owner cancellation");
                    };
                    assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
                    assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
                }
                topology::RootRetirements::MailLedger { origin, retirement } => {
                    ledgers.push(origin.address());
                    let ActorRetirement::Retained {
                        custody, outcome: RetirementOutcome::OwnerCancelled { .. },
                    } = retirement.as_ref() else {
                        panic!("the original Ledger retains its actual owner cancellation");
                    };
                    assert_eq!(custody.behavior.entries, []);
                    assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
                    assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
                }
            }
        }
        counters.sort_unstable();
        let mut expected_counters = [addresses[0], addresses[1]];
        expected_counters.sort_unstable();
        assert_eq!(counters, expected_counters);
        assert_eq!(ledgers, [addresses[2]]);
        let (_, _, first_startup, first_withdrawal, first_diagnostics, first_requests) = first.into_parts();
        let (_, _, sibling_startup, sibling_withdrawal, sibling_diagnostics, sibling_requests) = sibling.into_parts();
        assert!(matches!(first_startup, Some(Ok(()))) && matches!(first_withdrawal, Some(Ok(()))));
        assert!(matches!(sibling_startup, Some(Ok(()))) && matches!(sibling_withdrawal, Some(Ok(()))));
        assert!(first_diagnostics.is_empty() && sibling_diagnostics.is_empty());
        assert_eq!((first_requests.len(), sibling_requests.len()), (1, 0));
        let first_request = first_requests.into_iter().next().unwrap().try_into_parts()
            .unwrap_or_else(|_| panic!("disposal releases every original request producer loan"));
        let expected = records.borrow();
        let [expected] = expected.as_slice() else {
            panic!("one independently configured protected request remains");
        };
        assert!(first_request.0.is_none());
        assert_eq!(first_request.1.0.as_ref(), expected.protected.as_ref());
        assert_eq!(first_request.1.1.as_ref(), expected.proof.as_ref());
        let receipt = first_request.2.as_ref().expect("the original verification returned").as_ref().unwrap();
        assert_eq!(recorded::projection(receipt).bombay.actor_generation, addresses[0].0);
        assert_eq!(recorded::projection(receipt).kind, Purpose::Request);
    })).await.expect("the complete two-cause trace and unconditional rescue finish");
}
