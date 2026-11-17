//! Coexisting decoder and withdrawal notification causes retain joined cleanup.
use super::super::CounterMessage;
use super::super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    configuration, limits, metadata, recorded,
};
use super::actors::{ActorExports, Creator, Exports as CreatorExports, topology};
use crate::actors::ActorExt;
use crate::application::ApplicationScope;
use crate::behavior::Never;
use crate::remote::{Attempt, Export, ExportLimits, RefusalRetention, submission};
use crate::{MailAddr, RetirementAssessment, TrySendError};
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
    clippy::await_holding_refcell_ref,
    reason = "one causal two-export trace retains both original panic allocations and every independent cleanup fact"
)]
async fn decoder_and_withdrawal_notification_panics_keep_joined_node_finalization() {
    Box::pin(timeout(Duration::from_secs(40), async {
        let records = RefCell::new(Vec::<ProtectedMessage>::new());
        let joined = RefCell::new(None);
        let retirement = RefCell::new(None);
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
            let ActorExports { counter, sibling, .. } = &original.message;
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
            matches!(caught, Ok(Poll::Ready(Ok(_)))) && matches!(acquired_close, Some(Ok(()))) && closed_before_rescue,
            "coexisting decoder and withdrawal notification panics must preserve the acquired outcome and joined Node close before rescue"
        );
    })).await.expect("the complete two-cause trace and unconditional rescue finish");
}
