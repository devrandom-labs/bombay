//! Request one public export's withdrawal while its sibling and actors remain live.
use super::super::CounterMessage;
use super::super::counter::{CounterIncremented, CounterProcessed, CounterValue};
use super::super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, limits, metadata, recorded,
};
use super::Command;
use super::actors::{ActorExports, Creator, Exports as CreatorExports, topology};
use crate::actors::ActorExt;
use crate::application::ApplicationScope;
use crate::behavior::{EstablishedRecipient, Never, User};
use crate::remote::{Attempt, Export, ExportLimits, RefusalRetention, reply};
use crate::{
    ActorFailureAssessment, ActorRetirement, Completion, MailAddr, RetirementAssessment,
    RetirementOutcome,
};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use futures_util::FutureExt;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

struct HeldProtection<'scope> {
    entered: Option<oneshot::Sender<()>>,
    disposals: &'scope Cell<usize>,
}

impl Future for HeldProtection<'_> {
    type Output = Result<Box<[u8]>, Never>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(publication) = self.get_mut().entered.take() {
            let published = publication.send(());
            published.expect("Work observes this actual protection poll returning Pending");
        }
        Poll::Pending
    }
}

impl Drop for HeldProtection<'_> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
    }
}

enum WithdrawalObservation {
    Awaited,
    Surrendered,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requested_export_withdraws_without_stopping_its_sibling() {
    export_withdrawal(WithdrawalObservation::Awaited).await;
}

#[test]
fn surrendered_withdrawal_observer_keeps_native_result_and_sibling_progress() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the withdrawal observation runs on the existing two-worker host");
    runtime.block_on(export_withdrawal(WithdrawalObservation::Surrendered));
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one causal withdrawal trace retains actual native/provider/application originals; its configured receipt table stays immutable during verification"
)]
async fn export_withdrawal(observation: WithdrawalObservation) {
    Box::pin(tokio::time::timeout(Duration::from_secs(40), async {
        let records = RefCell::new(Vec::<ProtectedMessage>::new());
        let binding = Binding {
            deployment: "export-withdrawal", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "children", logical_session: 419,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<Command>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &records.borrow(), binding,
            ).await
        };
        let current = |receipt: &recorded::Receipt<Command>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.operation == Command::Increment
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
        let same = |left: &recorded::Receipt<Command>, right: &recorded::Receipt<Command>| {
            recorded::identity(left) == recorded::identity(right)
        };
        let decode = |bytes: &[u8]| {
            serde_json::from_slice::<ReadRequest<Command>>(bytes).map(|request| request.operation)
        };
        let encode = |_: &recorded::Receipt<Command>, original: &User<MailAddr, CounterIncremented>| {
            Ok::<_, Never>(original.message.request_sequence.to_be_bytes().to_vec().into_boxed_slice())
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let bounds = ExportLimits {
            refusals: RefusalRetention::Retain, queued_queries: maximum,
            retained_queries: maximum, retained_requests: maximum, diagnostics: maximum,
        };
        let (entered, pending_protection) = oneshot::channel();
        let entered = RefCell::new(Some(entered));
        let disposals = Cell::new(0);
        let mut first = Export::new(
            "bombay/withdrawal/first", bounds, limits(), &decode,
            reply(
                |command| match command {
                    Command::Increment => Ok(|reply_to: EstablishedRecipient<CounterProcessed>| {
                        CounterMessage::IncrementAndReply { request_sequence: 421, reply_to }
                    }),
                    command @ Command::Read => Err(command),
                },
                &encode,
            ),
            &verify,
            |_: &[u8], _: &recorded::Receipt<Command>| HeldProtection {
                entered: entered.borrow_mut().take(), disposals: &disposals,
            },
            same, &current, &current,
        );
        let second = Export::new(
            "bombay/withdrawal/sibling", bounds, limits(), &decode,
            reply(
                |command| match command {
                    Command::Increment => Ok(|reply_to: EstablishedRecipient<CounterProcessed>| {
                        CounterMessage::IncrementAndReply { request_sequence: 431, reply_to }
                    }),
                    command @ Command::Read => Err(command),
                },
                &encode,
            ),
            &verify,
            async |_: &[u8], _: &recorded::Receipt<Command>| {
                Ok::<_, Never>(Box::<[u8]>::from([89_u8]))
            },
            same, &current, &current,
        );
        let scope = ApplicationScope::new();
        let mut exports = scope.external::<CreatorExports>().unwrap();
        let mut values = scope.external::<CounterValue>().unwrap();
        let application = scope.with_topology(
            Creator::default().stop_on_shutdown(), topology::Topology::new(),
        );
        let node = application
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
            .await.unwrap_or_else(|_| panic!("the public Node opens its shared connection"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let withdraw = first.take_withdrawal()
            .expect("the first export retains one exact withdrawal authority");
        let second_authority = first.take_withdrawal();
        assert!(second_authority.is_none());
        let (configured, configuration_received) = oneshot::channel();
        let connection = &session;
        let registered = &records;
        let mut node = node.exports((first, second), async move |application| {
            let creation_admission = exports.send(
                &application.root().established_recipient(), exports.recipient(),
            ).await;
            let original = exports.receive().await.expect("Creator supplies its real child proofs");
            let ActorExports { counter, sibling, ledger } = &original.message;
            let mut requests = Vec::new();
            for target in [counter.recipient(), sibling.recipient()] {
                let mut claims = metadata(
                    ProviderProfile::Recorded, binding.protocol, runtime.clone(), runtime.clone(),
                );
                claims.deployment = binding.deployment.into();
                claims.caller = binding.caller.into();
                claims.controller = binding.controller.into();
                claims.actor = binding.actor.into();
                claims.host = binding.host.into();
                claims.actor_generation = application.actor_address(&target).unwrap().0;
                let original = ProtectedMessage {
                    protected: serde_json::to_vec(&ReadRequestRef {
                        bombay: &claims, kind: Purpose::Request, operation: Command::Increment,
                    }).unwrap().into_boxed_slice(),
                    proof: Box::from([83_u8]),
                };
                requests.push((original.protected.to_vec(), original.proof.to_vec()));
                registered.borrow_mut().push(original);
            }
            let addresses = [
                application.actor_address(&counter.recipient()).unwrap(),
                application.actor_address(&sibling.recipient()).unwrap(),
                application.actor_address(&ledger.recipient()).unwrap(),
            ];
            let targets = (counter.clone(), sibling.clone());
            let publication = configured.send((creation_admission, original, requests, addresses));
            publication.unwrap_or_else(|_| panic!("Work awaits configured real actor proofs"));
            Ok::<_, Never>(targets)
        });
        let outcome = node.run_with(async move |application| {
            let (creation_admission, actors, requests, addresses) = configuration_received.await
                .expect("actual startup declares both exports before Work");
            let first_querier = connection.declare_querier("bombay/withdrawal/first")
                .allowed_destination(Locality::SessionLocal).await
                .expect("native first-declaration inspection is configured before cancellation");
            let sibling_querier = connection.declare_querier("bombay/withdrawal/sibling")
                .allowed_destination(Locality::SessionLocal).await
                .expect("native sibling-declaration inspection is configured before cancellation");
            let first_receiving = connection.get("bombay/withdrawal/first")
                .allowed_destination(Locality::SessionLocal)
                .payload(requests[0].0.clone()).attachment(requests[0].1.clone())
                .consolidation(ConsolidationMode::None).timeout(Duration::from_secs(5))
                .await.expect("first native query starts before the isolation cutoff");
            pending_protection.await.expect("first protection actually returned Pending");
            let withdrawal_result = match &observation {
                WithdrawalObservation::Awaited => Poll::Ready(withdraw().await),
                WithdrawalObservation::Surrendered => {
                    let mut operation = pin!(withdraw());
                    let mut context = Context::from_waker(Waker::noop());
                    operation.as_mut().poll(&mut context)
                }
            };
            // The whole started observer has left scope before native owner progress.
            let mut first_replies = Vec::new();
            let first_ending = loop {
                match first_receiving.recv_async().await {
                    Ok(original) => first_replies.push(original),
                    Err(ending) => break ending,
                }
            };
            let first_matching = first_querier.matching_status().await;
            let sibling_matching = sibling_querier.matching_status().await;
            let first_inspection = first_querier.undeclare().await;
            let sibling_inspection = sibling_querier.undeclare().await;
            // Retain native refusal, including immediate Get failure in the adverse control.
            let submitted = connection.get("bombay/withdrawal/sibling")
                .allowed_destination(Locality::SessionLocal)
                .payload(requests[1].0.clone()).attachment(requests[1].1.clone())
                .consolidation(ConsolidationMode::None).timeout(Duration::from_secs(5)).await;
            let mut sibling_replies = Vec::new();
            let (submitted, sibling_ending) = match submitted {
                Ok(receiving) => {
                    let ending = loop {
                        match receiving.recv_async().await {
                            Ok(original) => sibling_replies.push(original),
                            Err(ending) => break ending,
                        }
                    };
                    (Ok(()), Some(ending))
                }
                Err(cause) => (Err(cause), None),
            };
            // These local requests remain independent of the native submission/result path.
            let first_admission = values.send(
                &actors.message.counter.recipient(), CounterMessage::Read(values.recipient()),
            ).await;
            let first_value = values.receive().await;
            let sibling_admission = values.send(
                &actors.message.sibling.recipient(), CounterMessage::Read(values.recipient()),
            ).await;
            let sibling_value = values.receive().await;
            let session_open = !connection.is_closed();
            let cleanup_pending = application.cleanup()
                .expect("Work shares actual cleanup observation").now_or_never();
            let shutdown = application.shutdown();
            (
                observation, creation_admission, actors, requests, addresses, withdrawal_result, first_replies, first_ending,
                submitted, sibling_replies, sibling_ending, first_admission, first_value,
                sibling_admission, sibling_value, session_open, cleanup_pending, shutdown,
                first_matching, sibling_matching, first_inspection, sibling_inspection,
            )
        }).await.unwrap_or_else(|_| panic!("the public Node execution starts"));
        let ((first, second), binding_result, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node.into_parts()
            .unwrap_or_else(|_| panic!("the Node retains its actual joined finalization"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (observations, cleanup) = work.try_into_completed()
            .unwrap_or_else(|_| panic!("real Work completes before joined cleanup extraction"));
        let (
            observation, creation_admission, actors, requests, addresses, withdrawal_result, first_replies, first_ending,
            submitted, sibling_replies, sibling_ending, first_admission, first_value,
            sibling_admission, sibling_value, session_open, cleanup_pending, shutdown,
            first_matching, sibling_matching, first_inspection, sibling_inspection,
        ) = observations;
        let (origin, native) = native.expect("the complete original Creator and child retirements remain");
        let notifications = notifications.expect("both original notification results remain");
        let (_, _, first_started, first_withdrawal, first_diagnostics, first_requests) = first.into_parts();
        let (_, _, sibling_started, sibling_withdrawal, sibling_diagnostics, sibling_requests) = second.into_parts();
        let first_value = first_value.expect("the independent first Counter read answers");
        let sibling_value = sibling_value.expect("the independent sibling Counter read answers");
        assert_eq!((first_value.from, first_value.message), (addresses[0], 1));
        // The intended shared-close adverse control must fail THIS effect oracle after cleanup.
        assert_eq!(
            (sibling_value.from, sibling_value.message),
            (addresses[1], 1),
            "the sibling remote increment must execute after first withdrawal and before owner cleanup",
        );
        assert!(submitted.is_ok() && sibling_ending.is_some());
        assert_eq!(sibling_replies.len(), 1, "the still-live sibling export must return its real native reply");
        assert!(cleanup.is_ok() && closed.is_ok() && session.is_closed());
        assert!(creation_admission.is_ok() && shutdown.is_ok());
        assert!(matches!(binding_result, Some(Ok(_))));
        assert!(polling.is_none() && disposal.is_none());
        assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
        assert_ne!(actors.message.counter.recipient(), actors.message.sibling.recipient());
        assert!(first_admission.is_ok() && sibling_admission.is_ok());
        assert!(first_inspection.is_ok() && sibling_inspection.is_ok());
        let first_matching = first_matching.expect("actual first declaration status remains");
        let sibling_matching = sibling_matching.expect("actual sibling declaration status remains");
        assert_eq!(
            (first_matching.matching(), sibling_matching.matching()),
            (false, true),
            "withdrawal must remove only the first native declaration",
        );
        match observation {
            WithdrawalObservation::Awaited => {
                assert!(matches!(&withdrawal_result, Poll::Ready(Some(Ok(())))));
            }
            WithdrawalObservation::Surrendered => {
                assert!(withdrawal_result.is_pending(), "the whole pending observer was surrendered");
            }
        }
        assert!(first_replies.is_empty());
        assert_eq!(first_ending.to_string(), "receiving on a closed channel");
        let received = sibling_replies[0].result();
        let reply = received.as_ref().unwrap();
        assert_eq!(reply.payload().to_bytes().as_ref(), 431_u64.to_be_bytes());
        assert_eq!(reply.attachment().unwrap().to_bytes().as_ref(), [89]);
        assert!(session_open && cleanup_pending.is_none());
        assert_eq!(disposals.get(), 1);
        assert!(matches!(&first_started, Some(Ok(()))) && matches!(&first_withdrawal, Some(Ok(()))));
        assert!(matches!(&sibling_started, Some(Ok(()))) && matches!(&sibling_withdrawal, Some(Ok(()))));
        assert!(first_diagnostics.is_empty() && sibling_diagnostics.is_empty());
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(native.completion(), Some(&Completion::Stopped));
        let report = report.expect("actual joined Creator assessment remains");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        let custody = native.custody().expect("original Creator custody remains");
        assert_eq!(custody.descendants.len(), 3);
        let mut counter_origins = Vec::new();
        let mut ledger_origins = Vec::new();
        for child in &custody.descendants {
            match child {
                topology::RootRetirements::Counter { origin, retirement } => {
                    counter_origins.push(origin.address());
                    assert!(matches!(retirement.as_ref(), ActorRetirement::Retained {
                        outcome: RetirementOutcome::OwnerCancelled { .. }, ..
                    }));
                }
                topology::RootRetirements::MailLedger { origin, retirement } => {
                    ledger_origins.push(origin.address());
                    assert!(matches!(retirement.as_ref(), ActorRetirement::Retained {
                        outcome: RetirementOutcome::OwnerCancelled { .. }, ..
                    }));
                }
            }
        }
        counter_origins.sort_unstable();
        let mut expected_counters = [addresses[0], addresses[1]];
        expected_counters.sort_unstable();
        assert_eq!(counter_origins, expected_counters);
        assert_eq!(ledger_origins, [addresses[2]]);
        assert_eq!(first_requests.len(), 1);
        assert_eq!(sibling_requests.len(), 1);
        let first = first_requests.into_iter().next().unwrap().try_into_parts()
            .unwrap_or_else(|_| panic!("cancelled producer loans ended before original extraction"));
        assert!(first.0.is_some());
        assert_eq!((first.1.0.as_ref(), first.1.1.as_ref()), (requests[0].0.as_slice(), requests[0].1.as_slice()));
        let receipt = first.2.as_ref().unwrap().as_ref().unwrap();
        let expected: ReadRequest<Command> = serde_json::from_slice(&requests[0].0).unwrap();
        assert_eq!(&recorded::projection(receipt).bombay, &expected.bombay);
        assert_eq!(recorded::identity(receipt).0, &binding);
        assert_eq!(recorded::projection(receipt).bombay.actor_generation, first_value.from.0);
        assert!(first.3.is_none());
        let (construction, client, admission, processed, tail) = first.4.unwrap();
        assert!(construction.is_none() && matches!(&client, Attempt::Returned(Ok(_))));
        assert!(matches!(&admission, Attempt::Returned(Ok(()))));
        assert_eq!(processed, Some(Some(User::new(first_value.from, CounterIncremented { request_sequence: 421 }))));
        assert_eq!(tail, [None]);
        let first_encoding = first.5.unwrap().unwrap();
        assert_eq!(first_encoding.as_ref(), 421_u64.to_be_bytes());
        assert!(first.6.is_none() && first.7.is_empty());
        assert!(matches!(&first.8, (None, None, None)));
        assert!(matches!(&first.9, (None, None, None, None, None, None)));
        let sibling = sibling_requests.into_iter().next().unwrap().try_into_parts()
            .unwrap_or_else(|_| panic!("the completed sibling producer releases original custody"));
        assert!(sibling.0.is_some() && sibling.2.as_ref().is_some_and(Result::is_ok));
        let receipt = sibling.2.as_ref().unwrap().as_ref().unwrap();
        let expected: ReadRequest<Command> = serde_json::from_slice(&requests[1].0).unwrap();
        assert_eq!(&recorded::projection(receipt).bombay, &expected.bombay);
        assert_eq!(recorded::identity(receipt).0, &binding);
        assert_eq!((sibling.1.0.as_ref(), sibling.1.1.as_ref()), (requests[1].0.as_slice(), requests[1].1.as_slice()));
        assert!(sibling.3.is_none());
        let (construction, client, admission, processed, tail) = sibling.4.unwrap();
        assert!(construction.is_none() && matches!(&client, Attempt::Returned(Ok(_))));
        assert!(matches!(&admission, Attempt::Returned(Ok(()))));
        assert_eq!(processed, Some(Some(User::new(sibling_value.from, CounterIncremented { request_sequence: 431 }))));
        assert_eq!(tail, [None]);
        let sibling_encoding = sibling.5.unwrap().unwrap();
        let sibling_protection = sibling.6.unwrap().unwrap();
        assert_eq!(sibling_encoding.as_ref(), 431_u64.to_be_bytes());
        assert_eq!(sibling_protection.as_ref(), [89]);
        assert_eq!(sibling.7.len(), 1);
        assert!(sibling.7.iter().all(Result::is_ok));
        assert!(matches!(&sibling.8, (None, None, None)));
        assert!(matches!(&sibling.9, (None, None, None, None, None, None)));
        // This scope cannot inspect request.completed or callback permits; no such baseline is claimed.
    })).await.expect("watchdog only; native replies, local actor effects and acquired cleanup prove the law");
}
