//! Proposed complete SDK-free consumer; not mounted until its source review.
use super::super::counter::{Counter, CounterMessage, CounterProcessed, CounterValue};
use super::super::ledger::{LedgerCommand, MailLedger, Receipts};
use super::actors::{ActorExports, Counters, CountersRole, Creator, Exports, topology};
use super::{assert_exports, assert_retirement};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EstablishedRecipient, Protocol, User};
use bombay::entity::{DirectoryConfig, EntityCapacity, EntityId, EntityMetrics, EntityShutdown};
use bombay::prelude::{Completion, ShutdownRejection};
use bombay::remote::{Attempt, Export, ExportError, ExportLimits, reply, stop, submission};
use bombay::{
    ActorFailureAssessment, ActorRetirement, ActorRetirementReport, ApplicationClient,
    ApplicationScope, ApplicationSender, ExternalActorError, MailAddr, RetirementAssessment,
    TrySendError,
};
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[path = "peer.rs"]
mod peer;
#[path = "../protocol.rs"]
mod protocol;
#[path = "refusals.rs"]
mod refusals;
#[path = "wire.rs"]
mod wire;

use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, configuration, limits, recorded,
    reserved_address, scheduled,
};
use wire::{CounterInput, LedgerInput, Refusal};

#[derive(Clone, Copy)]
enum Presence {
    Supplied,
    Absent,
}
#[derive(Clone, Copy)]
enum ProviderStage {
    Verification,
    Protection,
}
type CounterReply<Replies: Protocol<Addr = MailAddr>> = (
    Attempt<
        Result<
            ApplicationClient<EstablishedRecipient<Counter>, Replies>,
            (EstablishedRecipient<Counter>, ExternalActorError),
        >,
    >,
    Attempt<Result<(), TrySendError<CounterMessage>>>,
    Option<Option<User<MailAddr, <Replies as Protocol>::Msg>>>,
    Vec<Option<User<MailAddr, <Replies as Protocol>::Msg>>>,
);
type LedgerReply = (
    Attempt<
        Result<
            ApplicationClient<EstablishedRecipient<MailLedger>, Receipts>,
            (EstablishedRecipient<MailLedger>, ExternalActorError),
        >,
    >,
    Attempt<Result<(), TrySendError<LedgerCommand>>>,
    Option<Option<User<MailAddr, Vec<MailAddr>>>>,
    Vec<Option<User<MailAddr, Vec<MailAddr>>>>,
);
type LedgerSubmission = (
    Attempt<
        Result<
            ApplicationSender<EstablishedRecipient<MailLedger>>,
            (EstablishedRecipient<MailLedger>, ExternalActorError),
        >,
    >,
    Attempt<Result<(), TrySendError<LedgerCommand>>>,
);
enum CounterOutcome<Increment, Read> {
    Processed {
        unsent: Option<Increment>,
        native: CounterReply<CounterProcessed>,
    },
    Value {
        unsent: Option<Read>,
        native: CounterReply<CounterValue>,
    },
    Stopped {
        admission: Attempt<Result<(), ShutdownRejection>>,
        retirement: Option<ActorRetirementReport>,
    },
}
enum LedgerOutcome<Append, Read> {
    Appended {
        unsent: Option<Append>,
        native: LedgerSubmission,
    },
    Read {
        unsent: Option<Read>,
        native: LedgerReply,
    },
}

fn same_counter(
    left: &recorded::Receipt<CounterInput>,
    right: &recorded::Receipt<CounterInput>,
) -> bool {
    recorded::identity(left) == recorded::identity(right)
}
fn same_ledger(
    left: &scheduled::Receipt<LedgerInput>,
    right: &scheduled::Receipt<LedgerInput>,
) -> bool {
    scheduled::identity(left) == scheduled::identity(right)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_exports_retain_borrowed_work_and_all_originals() {
    tokio::time::timeout(
        Duration::from_secs(10),
        complete(Presence::Supplied, ProviderStage::Protection),
    )
    .await
    .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_exports_run_with_genuine_work_absence() {
    tokio::time::timeout(
        Duration::from_secs(10),
        complete(Presence::Absent, ProviderStage::Protection),
    )
    .await
    .expect("finite consumer trace completes before any native query deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_verification_allows_other_actor_requests() {
    tokio::time::timeout(
        Duration::from_secs(10),
        complete(Presence::Absent, ProviderStage::Verification),
    )
    .await
    .expect("finite consumer trace completes before any native query deadline");
}

async fn complete(presence: Presence, pending: ProviderStage) {
    let address = reserved_address();
    let reports = Arc::new(Mutex::new(Vec::new()));
    let scope = ApplicationScope::new();
    let mut exports = scope.external::<Exports>().unwrap();
    let mut values = scope.external::<CounterValue>().unwrap();
    let mut sibling_values = scope.external::<CounterValue>().unwrap();
    let application = scope
        .with_topology(
            Creator::default().stop_on_shutdown(),
            topology::Topology::new(),
        )
        .entity_family(
            CountersRole,
            Counters {
                reports: Arc::clone(&reports),
            },
            DirectoryConfig::default(),
            EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap()),
        );
    let mut network = configuration(&address, &EndpointRole::Receiver);
    network.max_retained_queries = NonZeroUsize::new(16).unwrap();
    let node = application
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("actual Node opens"));
    let receiver_id = node.runtime_id();
    let peer = peer::Peer::open(&address).await;
    let caller_id = peer.runtime_id();
    let requests = RefCell::new(Vec::new());
    let ledger_requests = RefCell::new(Vec::new());
    let replies = RefCell::new(Vec::new());
    let ledger_replies = RefCell::new(Vec::new());
    let decoded_allocation = RefCell::new(None);
    let (entered, observed_pending) = oneshot::channel();
    let (released, gate) = oneshot::channel();
    let provider_gate = RefCell::new(Some((entered, gate)));
    let deadline = Instant::now() + Duration::from_secs(30);
    let maximum = NonZeroUsize::new(8).unwrap();
    let verify_counter = async |bytes: &[u8], proof: &[u8]| {
        let gate = match pending {
            ProviderStage::Verification => provider_gate.borrow_mut().take(),
            ProviderStage::Protection => None,
        };
        if let Some((entered, gate)) = gate {
            let published = entered.send(());
            published.expect("private peer observes the pending verification producer");
            gate.await
                .expect("Ledger progress releases Counter verification");
        }
        let original = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        recorded::verify::<CounterInput>(&original, &requests.borrow(), wire::binding("counter"))
            .await
    };
    let verify_ledger = async |bytes: &[u8], proof: &[u8]| {
        let original = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        scheduled::verify::<LedgerInput>(
            &original,
            &ledger_requests.borrow(),
            wire::binding("ledger"),
        )
        .await
    };
    let protect_counter = async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
        let gate = match pending {
            ProviderStage::Protection => provider_gate.borrow_mut().take(),
            ProviderStage::Verification => None,
        };
        if let Some((entered, gate)) = gate {
            let published = entered.send(());
            published.expect("private peer observes the actual pending producer");
            gate.await
                .expect("Ledger progress releases Counter protection");
        }
        wire::protect(bytes, recorded::projection(receipt), &replies, maximum)
    };
    let protect_ledger = async |bytes: &[u8], receipt: &scheduled::Receipt<LedgerInput>| {
        let proof = wire::protect(
            bytes,
            scheduled::projection(receipt),
            &ledger_replies,
            maximum,
        );
        let (published, received) = oneshot::channel();
        let transferred = published.send(proof);
        transferred
            .unwrap_or_else(|_| panic!("the borrowed scheduled protector keeps its receiver"));
        received.await.expect("actual scheduled protector result")
    };
    let current_counter =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
            wire::current(
                recorded::projection(receipt),
                runtime,
                target,
                wire::binding("counter"),
                &caller_id,
                deadline,
            )
        };
    let current_ledger =
        |receipt: &scheduled::Receipt<LedgerInput>, runtime: &str, target: MailAddr| {
            wire::current(
                scheduled::projection(receipt),
                runtime,
                target,
                wire::binding("ledger"),
                &caller_id,
                deadline,
            )
        };
    let export_limits = ExportLimits {
        queued_queries: maximum,
        retained_queries: maximum,
        retained_requests: maximum,
        diagnostics: maximum,
    };
    let counter = Export::new(
        peer::COUNTER,
        export_limits,
        limits(),
        wire::decode_counter,
        (
            reply(
                wire::increment,
                wire::processed,
                |unsent, client, admission, received, tail| CounterOutcome::Processed {
                    unsent,
                    native: (client, admission, received, tail),
                },
            ),
            (
                reply(
                    wire::counter_read,
                    wire::value,
                    |unsent, client, admission, received, tail| CounterOutcome::Value {
                        unsent,
                        native: (client, admission, received, tail),
                    },
                ),
                stop(
                    wire::counter_stop,
                    wire::stopped,
                    |admission, retirement| CounterOutcome::Stopped {
                        admission,
                        retirement,
                    },
                ),
            ),
        ),
        &verify_counter,
        &protect_counter,
        same_counter,
        &current_counter,
        &current_counter,
    );
    let ledger = Export::new(
        peer::LEDGER,
        export_limits,
        limits(),
        |bytes| wire::decode_ledger(bytes, &decoded_allocation),
        (
            submission(wire::append, wire::appended, |unsent, sender, admission| {
                LedgerOutcome::Appended {
                    unsent,
                    native: (sender, admission),
                }
            }),
            reply(
                wire::ledger_read,
                wire::receipts,
                |unsent, client, admission, received, tail| LedgerOutcome::Read {
                    unsent,
                    native: (client, admission, received, tail),
                },
            ),
        ),
        &verify_ledger,
        &protect_ledger,
        same_ledger,
        &current_ledger,
        &current_ledger,
    );
    let bound = RefCell::new(None);
    let (configured, configured_requests) = oneshot::channel();
    let (lifecycle, root_lifecycle) = oneshot::channel();
    let mut node = node.exports((counter, ledger), async |application| {
        let admitted = exports
            .send(
                &application.root().established_recipient(),
                exports.recipient(),
            )
            .await;
        let original = exports
            .receive()
            .await
            .expect("actual Creator returns its established child proofs");
        let ActorExports {
            counter,
            sibling,
            ledger,
        } = &original.message;
        let sibling_admission = sibling_values
            .send(
                &sibling.recipient(),
                CounterMessage::Read(sibling_values.recipient()),
            )
            .await;
        let sibling_value = sibling_values.receive().await;
        let counter_address = application
            .actor_address(&counter.recipient())
            .expect("actual owned Counter address");
        let ledger_address = application
            .actor_address(&ledger.recipient())
            .expect("actual owned Ledger address");
        let message = |profile, protocol, actor, sequence| {
            wire::claims(profile, protocol, &caller_id, &receiver_id, actor, sequence)
        };
        let increment = wire::register(
            message(ProviderProfile::Recorded, "counter", counter_address, 257),
            CounterInput::Increment(257),
            &requests,
        );
        let conflict = wire::conflict(&increment, &requests);
        let read = wire::register(
            message(ProviderProfile::Recorded, "counter", counter_address, 263),
            CounterInput::Read,
            &requests,
        );
        let stop = wire::register(
            message(ProviderProfile::Recorded, "counter", counter_address, 269),
            CounterInput::Stop,
            &requests,
        );
        let append = wire::register(
            message(ProviderProfile::Scheduled, "ledger", ledger_address, 271),
            LedgerInput::Append(Box::from([227, 229])),
            &ledger_requests,
        );
        let ledger_read = wire::register(
            message(ProviderProfile::Scheduled, "ledger", ledger_address, 277),
            LedgerInput::Read,
            &ledger_requests,
        );
        let publication = configured.send(peer::Requests {
            increment,
            conflict,
            read,
            stop,
            append,
            ledger_read,
        });
        publication.unwrap_or_else(|_| panic!("the native peer awaits actual scopes"));
        let publication = lifecycle.send(application.lifecycle());
        publication.unwrap_or_else(|_| panic!("the independent lifecycle consumer remains owned"));
        let interface = application.interface(());
        let account = application.entities(CountersRole).entity(281);
        let mut family = interface
            .client_to::<_, CounterProcessed>(account)
            .unwrap_or_else(|_| panic!("the actual family reply client allocates"));
        let family_admission = family
            .send(CounterMessage::IncrementAndReply {
                request_sequence: 283,
                reply_to: family.recipient(),
            })
            .await;
        let family_reply = family.receive().await;
        let targets = (counter.clone(), ledger.clone());
        *bound.borrow_mut() = Some((
            admitted,
            original,
            family_admission,
            family_reply,
            (sibling_admission, sibling_value),
        ));
        Ok::<_, Refusal>(targets)
    });
    let (finished, peer_finished) = oneshot::channel();
    let (server_closed, server_retired) = oneshot::channel();
    let exchange = async {
        let requests = configured_requests
            .await
            .expect("actual binder's configured requests");
        let lifecycle = root_lifecycle
            .await
            .expect("actual root lifecycle projection");
        peer.exchange(
            requests,
            observed_pending,
            released,
            pending,
            || match presence {
                Presence::Supplied => {
                    let published = finished.send(());
                    published.expect("supplied Work awaits actual peer completion");
                    None
                }
                Presence::Absent => Some(lifecycle.request_shutdown()),
            },
            server_retired,
        )
        .await
    };
    let configuration = String::from("borrowed caller configuration");
    let work_observations = RefCell::new(Vec::new());
    let running = async {
        let outcome = match presence {
            Presence::Supplied => {
                let configuration = &configuration;
                let bound = &bound;
                let work_observations = &work_observations;
                let values = &mut values;
                let outcome = node
                    .run_with(move |application| async move {
                        peer_finished
                            .await
                            .expect("real peer completed without a Work-owned service loop");
                        let bound = bound.borrow();
                        let (_, exported, _, _, _) = bound.as_ref().expect("binding precedes Work");
                        let admission = values
                            .send(
                                &exported.message.sibling.recipient(),
                                CounterMessage::Read(values.recipient()),
                            )
                            .await;
                        let value = values.receive().await;
                        work_observations.borrow_mut().push((admission, value));
                        let shutdown = application.request_shutdown();
                        (configuration.as_str(), shutdown)
                    })
                    .await
                    .unwrap_or_else(|_| panic!("supplied Node execution is issued"));
                let (work, native, notes, families) = outcome.into_parts();
                let ((configuration, shutdown), cleanup) = work
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("original borrowed Work result remains complete"));
                (
                    Some(configuration),
                    Some(shutdown),
                    cleanup,
                    native,
                    notes,
                    families,
                )
            }
            Presence::Absent => {
                let outcome = node
                    .run()
                    .await
                    .unwrap_or_else(|_| panic!("genuine absent Node execution is issued"));
                let (work, native, notes, families) = outcome.into_parts();
                let (absence, cleanup) = work
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("actual startup grant retains work absence"));
                match absence {
                    None => {}
                    Some(never) => match never {},
                }
                (None, None, cleanup, native, notes, families)
            }
        };
        let publication = server_closed.send(());
        publication.expect("the private peer awaits actual Node completion and close");
        outcome
    };
    let (execution, (peer, absent_shutdown)) = tokio::join!(running, exchange);
    // No explicit drop, native close, undeclaration or borrow-ending scope.
    let ((counter, ledger), binding_result, closed) = node.into_parts().unwrap_or_else(|_| {
        panic!("actual joined Node has transferred its application and binder")
    });
    let (work, supplied_shutdown, cleanup, native, notifications, (family, ())) = execution;
    let (origin, native) = native.expect("original joined root and descendant reports");
    let notifications = notifications.expect("both original notification results");
    let (_, family) = family.expect("the original nonempty family report");
    let (family_shutdown, family_metrics, family_disposal) = family.into_parts();
    let bound = bound.borrow();
    let bound = bound
        .as_ref()
        .expect("original binding observations remain independent");
    let counter_parts = counter.into_parts();
    let ledger_parts = ledger.into_parts();
    let wire_replies = peer.protected();
    cleanup.expect("application cleanup joins");
    closed.expect("actual Node connection closes");
    peer.closed.expect("actual peer closes independently");
    let (counter_target, ledger_target) = binding_result
        .expect("actual binding was acquired")
        .expect("actual binding succeeds");
    assert_eq!(
        counter_target.recipient(),
        bound.1.message.counter.recipient()
    );
    assert_eq!(
        ledger_target.recipient(),
        bound.1.message.ledger.recipient()
    );
    assert!(bound.0.is_ok() && bound.2.is_ok() && bound.4.0.is_ok());
    let sibling_value = bound
        .4
        .1
        .as_ref()
        .expect("actual binding-time sibling read");
    assert_eq!(sibling_value.message, 0);
    assert_eq!(
        bound
            .3
            .as_ref()
            .expect("actual family increment reply")
            .message
            .request_sequence,
        283
    );
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_retirement(&native);
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    notifications
        .termination
        .expect("actual termination notification");
    notifications
        .retirement
        .expect("actual retirement notification");
    match presence {
        Presence::Supplied => {
            assert_eq!(work, Some(configuration.as_str()));
            assert_eq!(supplied_shutdown, Some(Ok(())));
            let observations = work_observations.borrow();
            let [(admission, Some(value))] = observations.as_slice() else {
                panic!("actual sibling isolation control");
            };
            assert!(admission.is_ok());
            assert_eq!((value.from, value.message), (sibling_value.from, 0));
            assert_ne!(value.from, bound.3.as_ref().unwrap().from);
        }
        Presence::Absent => {
            assert_eq!(absent_shutdown, Some(Ok(())));
            assert!(work.is_none() && supplied_shutdown.is_none());
            let observations = work_observations.borrow();
            assert!(observations.is_empty());
        }
    }
    assert_eq!(
        family_metrics,
        EntityMetrics {
            activations: 1,
            hydration_failures: 0,
            launch_failures: 0,
            capacity_refusals: 0,
            forced_retirements: 0,
            peak_hydrations: 1,
            residents: 0
        }
    );
    let EntityShutdown::Settled {
        represented,
        entities,
    } = family_shutdown
    else {
        panic!("the exact family reaches its real shutdown barrier");
    };
    assert_eq!(represented, 1);
    assert_eq!(entities, [EntityId::new(281)]);
    assert!(family_disposal.is_none());
    // Consuming extraction invokes the selected application report policy only
    // after every independent application/family/notification/close fact is owned.
    let (
        _counter_inputs,
        counter_declarations,
        counter_declared,
        counter_withdrawn,
        counter_ingress,
        counter_requests,
    ) = counter_parts;
    let (
        _ledger_inputs,
        ledger_declarations,
        ledger_declared,
        ledger_withdrawn,
        ledger_ingress,
        ledger_requests,
    ) = ledger_parts;
    counter_declared
        .expect("actual Counter declaration attempted")
        .expect("Counter declaration succeeded");
    ledger_declared
        .expect("actual Ledger declaration attempted")
        .expect("Ledger declaration succeeded");
    counter_withdrawn
        .expect("actual Counter declaration withdrawn")
        .expect("Counter withdrawal succeeded");
    ledger_withdrawn
        .expect("actual Ledger declaration withdrawn")
        .expect("Ledger withdrawal succeeded");
    assert!(counter_ingress.is_empty() && ledger_ingress.is_empty());
    assert_eq!(counter_requests.len(), 5);
    assert_eq!(ledger_requests.len(), 2);
    let mut processed = Vec::new();
    let mut read = Vec::new();
    let mut stopped = Vec::new();
    let mut reused = Vec::new();
    let mut conflicts = Vec::new();
    for request in counter_requests {
        let (
            key,
            original,
            verified,
            decoded,
            native,
            encoding,
            protection,
            publications,
            (request_refusal, reply_refusal, refusal),
            (verification_poll, verification_disposal, protection_poll, protection_disposal),
        ) = request.into_parts(&counter_declarations);
        assert!(
            verification_poll.is_none()
                && verification_disposal.is_none()
                && protection_poll.is_none()
                && protection_disposal.is_none()
        );
        let receipt = verified
            .expect("Counter verification was attempted")
            .expect("actual original receipt");
        let registered = requests.borrow();
        assert!(
            registered
                .iter()
                .any(|registered| registered.protected == original.0
                    && registered.proof == original.1)
        );
        let declared: wire::Request<CounterInput> = serde_json::from_slice(&original.0).unwrap();
        let scope = recorded::projection(&receipt);
        assert_eq!(scope.bombay, declared.bombay);
        assert_eq!(scope.kind, declared.kind);
        assert_eq!(scope.operation, declared.operation);
        assert!(decoded.is_none()); // Every successful input transferred to its native constructor.
        assert!(request_refusal.is_none() && reply_refusal.is_none());
        match native {
            Some(CounterOutcome::Processed { unsent, native }) => {
                assert!(unsent.is_none() && refusal.is_none());
                assert_eq!(scope.bombay.request_sequence, 257);
                assert_eq!(scope.operation, CounterInput::Increment(257));
                let (client, admission, received, tail) = native;
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("actual processing client");
                };
                let original = received
                    .expect("actual receive attempted")
                    .expect("actual correlated reply");
                assert_eq!(original.message.request_sequence, 257);
                assert!(matches!(tail.last(), Some(None)));
                assert_eq!(tail.len(), 1);
                processed.push((key.expect("installed logical request"), original, client));
            }
            Some(CounterOutcome::Value { unsent, native }) => {
                assert!(unsent.is_none() && refusal.is_none());
                assert_eq!(scope.bombay.request_sequence, 263);
                assert_eq!(scope.operation, CounterInput::Read);
                let (client, admission, received, tail) = native;
                assert!(matches!(client, Attempt::Returned(Ok(_))));
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                assert_eq!(tail.len(), 1);
                assert!(matches!(tail.last(), Some(None)));
                read.push((
                    key.expect("installed Read request"),
                    received
                        .expect("value receive attempted")
                        .expect("actual value reply"),
                ));
            }
            Some(CounterOutcome::Stopped {
                admission,
                retirement,
            }) => {
                assert!(refusal.is_none());
                assert_eq!(scope.bombay.request_sequence, 269);
                assert_eq!(scope.operation, CounterInput::Stop);
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                let retirement =
                    retirement.expect("exact child joined report, independently acquired");
                assert_eq!(retirement.retirement(), RetirementAssessment::Established);
                assert_eq!(
                    retirement.failures(),
                    ActorFailureAssessment::NoFailuresFound
                );
                stopped.push((key.expect("installed Stop request"), retirement));
            }
            None => match refusal {
                Some(ExportError::Conflict) => {
                    assert!(encoding.is_none() && protection.is_none() && publications.is_empty());
                    assert_eq!(scope.bombay.request_sequence, 257);
                    assert_eq!(original.0.last(), Some(&b' '));
                    assert!(key.is_none());
                    conflicts.push((receipt, original));
                    continue;
                }
                None => {
                    assert!(encoding.is_none() && protection.is_none());
                    assert_eq!(publications.len(), 1);
                    assert!(publications.iter().all(Result::is_ok));
                    assert_eq!(scope.bombay.request_sequence, 257);
                    assert_eq!(scope.operation, CounterInput::Increment(257));
                    reused.push(key.expect("duplicate points to the installed original key"));
                    continue;
                }
                original => panic!("unexpected original refusal: {original:?}"),
            },
        }
        let encoded = encoding
            .expect("actual encoder was invoked")
            .expect("actual encoded reply");
        let proof = protection
            .expect("actual borrowed protector was invoked")
            .expect("actual returned proof");
        let registered = replies.borrow();
        assert!(
            registered
                .iter()
                .any(|registered| registered.protected == encoded && registered.proof == proof)
        );
        assert_eq!(publications.len(), 1);
        assert!(publications.iter().all(Result::is_ok));
    }
    assert_eq!(
        (
            processed.len(),
            read.len(),
            stopped.len(),
            reused.len(),
            conflicts.len()
        ),
        (1, 1, 1, 1, 1)
    );
    assert_eq!(processed[0].0, reused[0]);
    assert_ne!(processed[0].0, read[0].0);
    assert_ne!(processed[0].0, stopped[0].0);
    assert_ne!(read[0].0, stopped[0].0);
    assert_eq!(
        (read[0].1.from, read[0].1.message),
        (processed[0].1.from, 1)
    );
    let mut append_sender = None;
    let mut ledger_reply = None;
    let mut append_key = None;
    let mut ledger_key = None;
    for request in ledger_requests {
        let (
            key,
            original,
            verified,
            decoded,
            native,
            encoding,
            protection,
            publications,
            (request_refusal, reply_refusal, refusal),
            (verification_poll, verification_disposal, protection_poll, protection_disposal),
        ) = request.into_parts(&ledger_declarations);
        assert!(
            verification_poll.is_none()
                && verification_disposal.is_none()
                && protection_poll.is_none()
                && protection_disposal.is_none()
        );
        let receipt = verified
            .expect("Ledger verification attempted")
            .expect("actual nominal scheduled receipt");
        let declared: wire::Request<LedgerInput> = serde_json::from_slice(&original.0).unwrap();
        let scope = scheduled::projection(&receipt);
        assert_eq!(scope.bombay, declared.bombay);
        assert_eq!(scope.kind, declared.kind);
        assert_eq!(scope.operation, declared.operation);
        assert_eq!(scheduled::proof(&receipt), original.1);
        assert!(
            decoded.is_none()
                && request_refusal.is_none()
                && reply_refusal.is_none()
                && refusal.is_none()
        );
        match native.expect("both Ledger operations were selected") {
            LedgerOutcome::Appended {
                unsent,
                native: (sender, admission),
            } => {
                assert_eq!(scope.bombay.request_sequence, 271);
                let expected = LedgerInput::Append(Box::from([227, 229]));
                assert_eq!(scope.operation, expected);
                assert!(unsent.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
                let Attempt::Returned(Ok(sender)) = sender else {
                    panic!("original native sender");
                };
                append_sender = Some(sender.address());
                append_key = key;
            }
            LedgerOutcome::Read {
                unsent,
                native: (client, admission, received, tail),
            } => {
                assert_eq!(scope.bombay.request_sequence, 277);
                assert_eq!(scope.operation, LedgerInput::Read);
                assert!(unsent.is_none() && matches!(client, Attempt::Returned(Ok(_))));
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                assert_eq!(tail.len(), 1);
                assert!(matches!(tail.last(), Some(None)));
                ledger_key = key;
                ledger_reply = Some(
                    received
                        .expect("terminal read attempted")
                        .expect("actual terminal reply"),
                );
            }
        }
        let encoded = encoding
            .expect("Ledger encoder invoked")
            .expect("actual encoded reply");
        let proof = protection
            .expect("scheduled protector invoked")
            .expect("actual scheduled proof");
        let registered = ledger_replies.borrow();
        assert!(
            registered
                .iter()
                .any(|registered| registered.protected == encoded && registered.proof == proof)
        );
        assert_eq!(publications.len(), 1);
        assert!(publications.iter().all(Result::is_ok));
    }
    let append_key = append_key.expect("installed Append request");
    let ledger_key = ledger_key.expect("installed terminal Read request");
    assert_ne!(append_key, ledger_key);
    let ledger_reply = ledger_reply.expect("actual terminal Ledger message");
    assert_eq!(
        ledger_reply.message,
        [append_sender.expect("actual one-way sender")]
    );
    assert_eq!(wire_replies.len(), 6);
    let registered_counter = replies.borrow();
    let registered_ledger = ledger_replies.borrow();
    for original in &wire_replies {
        assert!(
            registered_counter
                .iter()
                .chain(registered_ledger.iter())
                .any(|registered| registered.protected == original.protected
                    && registered.proof == original.proof)
        );
    }
    let increment = &processed[0].1;
    let expected = [
        serde_json::json!({ "kind":"admitted", "operation":{"append":[227,229]}, "value":"admitted", "sequence":271, "actor":ledger_reply.from.0 }),
        serde_json::json!({ "kind":"read_reply", "operation":"read", "value":[append_sender.unwrap().0], "sequence":277, "actor":ledger_reply.from.0 }),
        serde_json::json!({ "kind":"processed", "operation":{"increment":257}, "value":257, "sequence":257, "actor":increment.from.0 }),
        serde_json::json!({ "kind":"processed", "operation":{"increment":257}, "value":257, "sequence":257, "actor":increment.from.0 }),
        serde_json::json!({ "kind":"read_reply", "operation":"read", "value":1, "sequence":263, "actor":increment.from.0 }),
        serde_json::json!({ "kind":"stopped", "operation":"stop", "value":"retired", "sequence":269, "actor":increment.from.0 }),
    ];
    for (original, expected) in wire_replies.iter().zip(&expected) {
        let actual: serde_json::Value = serde_json::from_slice(&original.protected).unwrap();
        assert_eq!(actual["kind"], expected["kind"]);
        assert_eq!(actual["operation"], expected["operation"]);
        assert_eq!(actual["value"], expected["value"]);
        let protocol = if expected["actor"] == serde_json::json!(ledger_reply.from.0) {
            "ledger"
        } else {
            "counter"
        };
        let profile = match protocol {
            "ledger" => ProviderProfile::Scheduled,
            _ => ProviderProfile::Recorded,
        };
        let actor = MailAddr(expected["actor"].as_u64().unwrap());
        let claims = wire::claims(
            profile,
            protocol,
            &caller_id,
            &receiver_id,
            actor,
            expected["sequence"].as_u64().unwrap(),
        );
        let claims = serde_json::to_value(claims).unwrap();
        assert_eq!(actual["bombay"], claims);
        assert_eq!(actual.as_object().unwrap().len(), 4);
    }
    assert_eq!(wire_replies[2].protected, wire_replies[3].protected);
    assert_eq!(wire_replies[2].proof, wire_replies[3].proof);
    let observed_value: serde_json::Value =
        serde_json::from_slice(&wire_replies[4].protected).unwrap();
    assert_eq!(observed_value["value"], 1);
    let custody = native
        .custody()
        .expect("original Creator behavior and children");
    assert_exports(custody.behavior.base(), &bound.1.message);
    assert_eq!(custody.descendants.len(), 3);
    let allocation = *decoded_allocation.borrow();
    let allocation = allocation.expect("one actual selected Append allocation was decoded");
    let mut counter_origins = Vec::new();
    let mut ledger_origins = Vec::new();
    for child in &custody.descendants {
        match child {
            topology::RootRetirements::Counter { origin, retirement } => {
                assert_retirement(retirement);
                counter_origins.push(origin.address());
                if origin.address() == processed[0].1.from {
                    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
                } else {
                    assert!(matches!(
                        retirement.as_ref(),
                        ActorRetirement::OwnerCancelled { .. }
                    ));
                }
            }
            topology::RootRetirements::MailLedger { origin, retirement } => {
                ledger_origins.push(origin.address());
                assert_eq!(origin.address(), ledger_reply.from);
                assert_retirement(retirement);
                assert_eq!(retirement.completion(), Some(&Completion::Stopped));
                let [(sender, original)] = retirement
                    .custody()
                    .expect("original terminal ledger")
                    .behavior
                    .base()
                    .entries
                    .as_slice()
                else {
                    panic!("one actual original append");
                };
                assert_eq!(original.as_ptr(), allocation);
                assert_eq!(original.as_ref(), [227, 229]);
                assert_eq!(*sender, append_sender.unwrap());
            }
        }
    }
    counter_origins.sort_unstable();
    let mut expected_counters = vec![processed[0].1.from, sibling_value.from];
    expected_counters.sort_unstable();
    assert_eq!(counter_origins, expected_counters);
    assert_eq!(ledger_origins, [ledger_reply.from]);
    let reports = Arc::try_unwrap(reports)
        .unwrap_or_else(|_| panic!("joined family releases its definition"))
        .into_inner()
        .unwrap();
    let [super::actors::CounterReport::Retired(id, _, native, report)] = reports.as_slice() else {
        panic!("the original family callback receives exactly one actual retirement");
    };
    assert_eq!(*id.get(), 281);
    assert_retirement(native.as_ref().expect("actual native family original"));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}
