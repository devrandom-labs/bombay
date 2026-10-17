//! Actual refusal consumers: original causes, unused Work and acquired native facts coexist.
use super::super::super::counter::{CounterMessage, CounterProcessed, CounterValue};
use super::super::actors::{ActorExports, Counters, CountersRole, Creator, Exports, topology};
use super::super::{assert_exports, assert_retirement};
use super::{CounterOutcome, LedgerOutcome, peer, protocol, wire};
use bombay::actors::ActorExt;
use bombay::behavior::BehaviorBase;
use bombay::entity::{DirectoryConfig, EntityCapacity, EntityId, EntityMetrics, EntityShutdown};
use bombay::remote::{Attempt, Export, ExportError, ExportLimits};
use bombay::remote::{reply, stop, submission};
use bombay::{
    ActorFailureAssessment, ActorRetirement, ApplicationScope, MailAddr, RetirementAssessment,
    WorkOutcome,
};
use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, configuration, limits, recorded,
    reserved_address, scheduled,
};
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::{CounterInput, LedgerInput};

#[path = "faults.rs"]
mod faults;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefusalStage {
    Binding,
    Declaration,
    Verification,
    Protection,
    Request,
    Reply,
    VerifiedDisposal,
    RefusedDisposal,
}

#[derive(Debug)]
struct Withheld<'scope> {
    scope: &'scope str,
    original: Box<[u8]>,
}
#[derive(Debug)]
enum VerificationRefusal<'scope> {
    Provider(protocol::VerificationError),
    Withheld(Withheld<'scope>),
}
#[derive(Debug)]
enum ProtectionRefusal<'scope> {
    Provider(wire::Refusal),
    Withheld(Withheld<'scope>),
}
#[derive(Debug)]
enum PermissionRefusal<'scope> {
    Scope(wire::Refusal),
    Withheld(Withheld<'scope>),
}

fn withhold<'scope>(
    scope: &'scope str,
    original: &RefCell<Option<Box<[u8]>>>,
    returned: &RefCell<Option<oneshot::Sender<()>>>,
) -> Withheld<'scope> {
    let original = original
        .borrow_mut()
        .take()
        .expect("one actual owned refusal cause");
    let publication = returned
        .borrow_mut()
        .take()
        .expect("one actual producer refusal")
        .send(());
    publication.expect("the private peer observes the actual returned cause");
    Withheld { scope, original }
}

fn assert_withheld(refusal: Withheld<'_>, scope: &str, allocation: *const u8) {
    assert_eq!(refusal.scope, scope);
    assert_eq!(refusal.original.as_ptr(), allocation);
    assert_eq!(refusal.original.as_ref(), [37, 41]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_refusal_preserves_unused_work_and_borrowed_cause() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Binding))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_declaration_refusal_withdraws_the_first_before_close() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Declaration))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_refusal_preserves_the_original_provider_cause() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Verification))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protection_refusal_preserves_the_acquired_native_reply() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Protection))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_refusal_preserves_the_original_permission_cause() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Request))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_refusal_preserves_the_native_reply_and_current_cause() {
    tokio::time::timeout(Duration::from_secs(10), refused(RefusalStage::Reply))
        .await
        .expect("finite consumer trace completes before any native query deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_receipt_survives_its_producer_destructor_panic() {
    tokio::time::timeout(
        Duration::from_secs(10),
        refused(RefusalStage::VerifiedDisposal),
    )
    .await
    .expect("finite Ready-Ok disposal trace completes before native expiry");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_receipt_survives_its_producer_destructor_panic() {
    tokio::time::timeout(
        Duration::from_secs(10),
        refused(RefusalStage::RefusedDisposal),
    )
    .await
    .expect("finite Ready-Err disposal trace completes before native expiry");
}

async fn refused(stage: RefusalStage) {
    let address = reserved_address();
    let reports = Arc::new(Mutex::new(Vec::new()));
    let scope = ApplicationScope::new();
    let mut exported = scope.external::<Exports>().unwrap();
    let mut values = scope.external::<CounterValue>().unwrap();
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
    let node = application
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("actual refusal Node opens"));
    let receiver_id = node.runtime_id();
    let peer = peer::Peer::open(&address).await;
    let caller_id = peer.runtime_id();
    let configuration = String::from("borrowed refusal scope");
    let original: Box<[u8]> = [37, 41].into();
    let allocation = original.as_ptr();
    let original_cause = RefCell::new(Some(original));
    let (returned, producer_returned) = oneshot::channel();
    let returned = RefCell::new(Some(returned));
    let requests = RefCell::new(Vec::new());
    let replies = RefCell::new(Vec::new());
    let ledger_requests = RefCell::new(Vec::new());
    let ledger_replies = RefCell::new(Vec::new());
    let ledger_allocation = RefCell::new(None);
    let maximum = NonZeroUsize::new(8).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let producer_panic = Arc::new(vec![61_u8, 67]);
    let disposed = RefCell::new(Vec::new());
    let verify_counter = |bytes: &[u8], proof: &[u8]| {
        let result = match stage {
            RefusalStage::Verification | RefusalStage::RefusedDisposal => Err(
                VerificationRefusal::Withheld(withhold(&configuration, &original_cause, &returned)),
            ),
            _ => {
                let request = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                recorded::verify::<CounterInput>(
                    &request,
                    &requests.borrow(),
                    wire::binding("counter"),
                )
                .into_inner()
                .map_err(VerificationRefusal::Provider)
            }
        };
        let panic = match stage {
            RefusalStage::VerifiedDisposal | RefusalStage::RefusedDisposal => {
                Some(Arc::clone(&producer_panic))
            }
            _ => None,
        };
        faults::Verification {
            result: Some(result),
            panic,
            returned: &returned,
            disposed: &disposed,
            stage,
        }
    };
    let protect_counter =
        async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| match stage {
            RefusalStage::Protection => Err(ProtectionRefusal::Withheld(withhold(
                &configuration,
                &original_cause,
                &returned,
            ))),
            _ => wire::protect(bytes, recorded::projection(receipt), &replies, maximum)
                .map_err(ProtectionRefusal::Provider),
        };
    let verify_ledger = async |bytes: &[u8], proof: &[u8]| {
        let request = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        scheduled::verify::<LedgerInput>(
            &request,
            &ledger_requests.borrow(),
            wire::binding("ledger"),
        )
        .await
    };
    let protect_ledger = async |bytes: &[u8], receipt: &scheduled::Receipt<LedgerInput>| {
        wire::protect(
            bytes,
            scheduled::projection(receipt),
            &ledger_replies,
            maximum,
        )
    };
    let current_request =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| match stage {
            RefusalStage::Request => Err(PermissionRefusal::Withheld(withhold(
                &configuration,
                &original_cause,
                &returned,
            ))),
            _ => wire::current(
                recorded::projection(receipt),
                runtime,
                target,
                wire::binding("counter"),
                &caller_id,
                deadline,
            )
            .map_err(PermissionRefusal::Scope),
        };
    let current_reply =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| match stage {
            RefusalStage::Reply => Err(PermissionRefusal::Withheld(withhold(
                &configuration,
                &original_cause,
                &returned,
            ))),
            _ => wire::current(
                recorded::projection(receipt),
                runtime,
                target,
                wire::binding("counter"),
                &caller_id,
                deadline,
            )
            .map_err(PermissionRefusal::Scope),
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
        super::same_counter,
        &current_request,
        &current_reply,
    );
    let key = match stage {
        RefusalStage::Declaration => "",
        _ => peer::LEDGER,
    };
    let ledger = Export::new(
        key,
        export_limits,
        limits(),
        |bytes| wire::decode_ledger(bytes, &ledger_allocation),
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
        super::same_ledger,
        &current_ledger,
        &current_ledger,
    );
    let binding = RefCell::new(None);
    let (published, received) = oneshot::channel();
    let mut node = node.exports((counter, ledger), async |application| {
        let admission = exported
            .send(
                &application.root().established_recipient(),
                exported.recipient(),
            )
            .await;
        let actual = exported
            .receive()
            .await
            .expect("actual established child proof reply");
        let ActorExports {
            counter,
            sibling,
            ledger,
        } = &actual.message;
        let target = application
            .actor_address(&counter.recipient())
            .expect("the actual associated Counter address");
        let sibling_address = application
            .actor_address(&sibling.recipient())
            .expect("the actual associated sibling address");
        let ledger_address = application
            .actor_address(&ledger.recipient())
            .expect("the actual associated Ledger address");
        let request = wire::register(
            wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_id,
                &receiver_id,
                target,
                293,
            ),
            CounterInput::Increment(293),
            &requests,
        );
        let mut family = application
            .interface(())
            .client_to::<_, CounterProcessed>(application.entities(CountersRole).entity(307))
            .unwrap_or_else(|_| panic!("the same concrete family client allocates"));
        let family_admission = family
            .send(CounterMessage::IncrementAndReply {
                request_sequence: 311,
                reply_to: family.recipient(),
            })
            .await;
        let family_reply = family.receive().await;
        let targets = (counter.clone(), ledger.clone());
        let lifecycle = application.lifecycle();
        let read_target = counter.clone();
        *binding.borrow_mut() = Some((
            admission,
            actual,
            family_admission,
            family_reply,
            (target, sibling_address, ledger_address),
        ));
        let publication = published.send((request, lifecycle, read_target));
        publication.unwrap_or_else(|_| panic!("the actual private peer waits for child binding"));
        match stage {
            RefusalStage::Binding => Err(withhold(&configuration, &original_cause, &returned)),
            _ => Ok(targets),
        }
    });
    let (finished, peer_finished) = oneshot::channel();
    let (server_closed, server_retired) = oneshot::channel();
    let counter_observation = RefCell::new(None);
    let exchange = async {
        let (request, lifecycle, target) = received
            .await
            .expect("actual child binding preceded refusal");
        match stage {
            RefusalStage::Binding | RefusalStage::Declaration => (peer.close().await, None),
            _ => {
                let (peer, shutdown) = peer
                    .rejected(
                        request,
                        producer_returned,
                        async || {
                            let admission = values
                                .send(
                                    &target.recipient(),
                                    CounterMessage::Read(values.recipient()),
                                )
                                .await;
                            let original = values.receive().await;
                            *counter_observation.borrow_mut() = Some((admission, original));
                            lifecycle.request_shutdown()
                        },
                        finished,
                        server_retired,
                    )
                    .await;
                (peer, Some(shutdown))
            }
        }
    };
    let invoked = RefCell::new(Vec::new());
    let payload: Box<[u8]> = [53, 59].into();
    let payload_address = payload.as_ptr();
    let execution = async {
        let outcome = node
            .run_with(|application| {
                invoked.borrow_mut().push(payload.as_ptr());
                let configuration = &configuration;
                async move {
                    peer_finished
                        .await
                        .expect("actual refusal trace precedes Work completion");
                    let report = application.lifecycle().retirement().await;
                    (configuration.as_str(), payload, report)
                }
            })
            .await;
        let publication = server_closed.send(());
        (outcome, publication)
    };
    let ((outcome, publication), (peer, stopped)) = tokio::join!(execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("actual public execution was issued"));
    let ((counter, ledger), bound, closed) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual issued refusal cleanup and close were acquired"));
    let (work, native, notifications, (family, ())) = outcome.into_parts();
    let (_, native) = native.expect("actual root with every descendant original");
    let notifications = notifications.expect("both actual original notifications");
    let (_, family) = family.expect("actual nonempty family cleanup result");
    let (shutdown, metrics, disposal) = family.into_parts();
    let binding = binding.borrow();
    let (admission, actual, family_admission, family_reply, addresses) =
        binding.as_ref().expect("actual binding trace acquired");
    let (_counter_inputs, counter_declarations, declared, withdrawn, ingress, counter_requests) =
        counter.into_parts();
    let (
        _ledger_inputs,
        _ledger_declarations,
        ledger_declared,
        ledger_withdrawn,
        ledger_ingress,
        ledger_requests,
    ) = ledger.into_parts();
    closed.expect("actual Node connection close result");
    peer.closed
        .as_ref()
        .expect("actual peer connection closes independently");
    assert_eq!(peer.received(), 0);
    assert!(admission.is_ok() && family_admission.is_ok());
    assert_eq!(
        family_reply
            .as_ref()
            .expect("actual family reply before refusal")
            .message
            .request_sequence,
        311
    );
    assert_retirement(&native);
    assert_exports(
        native
            .custody()
            .expect("actual original Creator")
            .behavior
            .base(),
        &actual.message,
    );
    let custody = native
        .custody()
        .expect("complete original descendant custody");
    assert_eq!(custody.descendants.len(), 3);
    let mut counter_origins = Vec::new();
    let mut ledger_origins = Vec::new();
    for child in &custody.descendants {
        match child {
            topology::RootRetirements::Counter { origin, retirement } => {
                counter_origins.push(origin.address());
                assert_retirement(retirement.as_ref());
                assert!(matches!(
                    retirement.as_ref(),
                    ActorRetirement::OwnerCancelled { .. }
                ));
            }
            topology::RootRetirements::MailLedger { origin, retirement } => {
                ledger_origins.push(origin.address());
                assert_retirement(retirement.as_ref());
                assert!(matches!(
                    retirement.as_ref(),
                    ActorRetirement::OwnerCancelled { .. }
                ));
                let entries = &retirement
                    .custody()
                    .expect("original untouched Ledger")
                    .behavior
                    .entries;
                assert!(entries.is_empty());
            }
        }
    }
    counter_origins.sort_unstable();
    let mut expected_counters = vec![addresses.0, addresses.1];
    expected_counters.sort_unstable();
    assert_eq!(counter_origins, expected_counters);
    assert_eq!(ledger_origins, [addresses.2]);
    notifications
        .termination
        .expect("termination fact survives refusal");
    notifications
        .retirement
        .expect("joined retirement fact survives refusal");
    let EntityShutdown::Settled {
        represented,
        entities,
    } = shutdown
    else {
        panic!("actual family settled");
    };
    assert_eq!(represented, 1);
    assert_eq!(entities, [EntityId::new(307)]);
    assert_eq!(
        metrics,
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
    assert!(
        disposal.is_none()
            && ingress.is_empty()
            && ledger_ingress.is_empty()
            && ledger_requests.is_empty()
    );
    match stage {
        RefusalStage::Binding => {
            let Err(refusal) = bound.expect("actual binding was attempted") else {
                panic!("actual binding refused");
            };
            assert_withheld(refusal, &configuration, allocation);
            assert!(
                declared.is_none()
                    && withdrawn.is_none()
                    && ledger_declared.is_none()
                    && ledger_withdrawn.is_none()
            );
            assert!(counter_requests.is_empty());
        }
        RefusalStage::Declaration => {
            bound
                .expect("actual binding acquired")
                .expect("actual binding succeeded");
            declared
                .expect("first declaration attempted")
                .expect("first declaration succeeded");
            withdrawn
                .expect("first declaration withdrawn after second failed")
                .expect("actual withdrawal succeeded");
            let Some(Err(ExportError::Declaration { cause })) = ledger_declared else {
                panic!("actual malformed native declaration cause");
            };
            let diagnostic = cause.to_string();
            assert!(diagnostic.starts_with("Invalid Key Expr ``: empty chunks are forbidden, as well as leading and trailing slashes"));
            assert!(ledger_withdrawn.is_none() && counter_requests.is_empty());
        }
        _ => {
            publication.expect("the private peer awaits actual Node completion and close");
            let observation = counter_observation.borrow();
            let (admission, original) = observation
                .as_ref()
                .expect("independent native Counter read");
            let original = original
                .as_ref()
                .expect("actual Counter state after producer refusal");
            assert!(admission.is_ok());
            let expected_value = match stage {
                RefusalStage::Verification
                | RefusalStage::Request
                | RefusalStage::VerifiedDisposal
                | RefusalStage::RefusedDisposal => 0,
                RefusalStage::Protection | RefusalStage::Reply => 1,
                _ => unreachable!("only queried refusals acquire a native Counter read"),
            };
            assert_eq!(
                (original.from, original.message),
                (addresses.0, expected_value)
            );
            bound
                .expect("actual binding acquired")
                .expect("actual binding succeeded");
            declared
                .expect("Counter declaration attempted")
                .expect("Counter declaration succeeded");
            withdrawn
                .expect("Counter declaration withdrawn")
                .expect("Counter withdrawal succeeded");
            ledger_declared
                .expect("Ledger declaration attempted")
                .expect("Ledger declaration succeeded");
            ledger_withdrawn
                .expect("Ledger declaration withdrawn")
                .expect("Ledger withdrawal succeeded");
            let [request] = <[_; 1]>::try_from(counter_requests)
                .unwrap_or_else(|_| panic!("one actual refused request"));
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
            let registered = requests.borrow();
            assert_eq!(registered.len(), 1);
            assert_eq!(original.0, registered[0].protected);
            assert_eq!(original.1, registered[0].proof);
            let declared: wire::Request<CounterInput> =
                serde_json::from_slice(&original.0).unwrap();
            assert_eq!(declared.bombay.actor_generation, addresses.0.0);
            assert_eq!(declared.bombay.request_sequence, 293);
            assert_eq!(declared.kind, protocol::Purpose::Request);
            assert_eq!(declared.operation, CounterInput::Increment(293));
            if let Some(Ok(receipt)) = &verified {
                let scope = recorded::projection(receipt);
                assert_eq!(scope.bombay, declared.bombay);
                assert_eq!(scope.kind, declared.kind);
                assert_eq!(scope.operation, declared.operation);
            }
            assert!(decoded.is_none() && refusal.is_none() && publications.is_empty());
            assert!(protection_poll.is_none() && protection_disposal.is_none());
            match stage {
                RefusalStage::VerifiedDisposal | RefusalStage::RefusedDisposal => {
                    assert!(verification_poll.is_none());
                    let payload = verification_disposal.expect(
                        "actual provider disposal unwinds after the original Ready result is acquired",
                    );
                    let payload = payload
                        .downcast_ref::<Arc<Vec<u8>>>()
                        .expect("exact native panic payload type");
                    assert!(Arc::ptr_eq(payload, &producer_panic));
                    assert_eq!(payload.as_slice(), [61, 67]);
                }
                _ => assert!(verification_poll.is_none() && verification_disposal.is_none()),
            }
            match stage {
                RefusalStage::Verification | RefusalStage::RefusedDisposal => {
                    let Err(VerificationRefusal::Withheld(cause)) =
                        verified.expect("actual verifier result")
                    else {
                        panic!("original verifier refusal");
                    };
                    assert_withheld(cause, &configuration, allocation);
                    assert!(
                        key.is_none()
                            && native.is_none()
                            && encoding.is_none()
                            && protection.is_none()
                    );
                    assert!(request_refusal.is_none() && reply_refusal.is_none());
                }
                RefusalStage::VerifiedDisposal => {
                    verified
                        .expect("actual verification result survived producer disposal")
                        .expect("the original verified receipt survived");
                    let original = original_cause.borrow();
                    let original = original
                        .as_ref()
                        .expect("the non-refusing provider leaves this independent cause unused");
                    assert_eq!(original.as_ptr(), allocation);
                    assert_eq!(original.as_ref(), [37, 41]);
                    assert!(
                        key.is_none()
                            && native.is_none()
                            && encoding.is_none()
                            && protection.is_none()
                            && request_refusal.is_none()
                            && reply_refusal.is_none()
                    );
                }
                RefusalStage::Request => {
                    verified
                        .expect("actual verification result")
                        .expect("actual receipt");
                    let Some(PermissionRefusal::Withheld(cause)) = request_refusal else {
                        panic!("original request current-check refusal");
                    };
                    assert_withheld(cause, &configuration, allocation);
                    assert!(
                        key.is_none()
                            && native.is_none()
                            && encoding.is_none()
                            && protection.is_none()
                            && reply_refusal.is_none()
                    );
                }
                RefusalStage::Protection | RefusalStage::Reply => {
                    verified
                        .expect("actual verification result")
                        .expect("actual receipt");
                    let Some(CounterOutcome::Processed {
                        unsent,
                        native: (client, admission, received, tail),
                    }) = native
                    else {
                        panic!("acquired native processing result survives refusal");
                    };
                    assert!(key.is_some() && unsent.is_none() && request_refusal.is_none());
                    assert!(matches!(client, Attempt::Returned(Ok(_))));
                    assert!(matches!(admission, Attempt::Returned(Ok(()))));
                    let original = received
                        .expect("actual receive attempted")
                        .expect("actual native reply");
                    assert_eq!(
                        (original.from, original.message.request_sequence),
                        (addresses.0, 293)
                    );
                    assert_eq!(tail.len(), 1);
                    assert!(matches!(tail.last(), Some(None)));
                    encoding
                        .expect("actual reply encoding acquired")
                        .expect("pure reply encoding succeeded");
                    match stage {
                        RefusalStage::Protection => {
                            let Err(ProtectionRefusal::Withheld(cause)) =
                                protection.expect("actual protection result")
                            else {
                                panic!("original protection cause");
                            };
                            assert_withheld(cause, &configuration, allocation);
                            assert!(reply_refusal.is_none());
                        }
                        RefusalStage::Reply => {
                            protection
                                .expect("actual protection result")
                                .expect("actual protected proof");
                            let Some(PermissionRefusal::Withheld(cause)) = reply_refusal else {
                                panic!("original fresh reply current-check cause");
                            };
                            assert_withheld(cause, &configuration, allocation);
                        }
                        _ => unreachable!("only the two acquired-native refusal cases enter"),
                    }
                }
                _ => unreachable!("binding and declaration refusals have no request"),
            }
            assert_eq!(stopped, Some(Ok(())));
        }
    }
    let disposals = disposed.borrow();
    match stage {
        RefusalStage::Binding | RefusalStage::Declaration => assert!(disposals.is_empty()),
        _ => assert_eq!(disposals.as_slice(), [stage]),
    }
    match (stage, work) {
        (
            RefusalStage::Binding | RefusalStage::Declaration,
            WorkOutcome::NotInvoked { work, cleanup, .. },
        ) => {
            cleanup.expect("unused Work retains its actual cleanup result");
            let invocations = invoked.borrow();
            assert!(invocations.is_empty());
            let _original_work = work;
        }
        (
            _,
            WorkOutcome::Completed {
                output: (scope, payload, report),
                cleanup,
            },
        ) => {
            cleanup.expect("actual Work cleanup result");
            assert_eq!(scope, configuration);
            assert_eq!(payload.as_ptr(), payload_address);
            assert_eq!(payload.as_ref(), [53, 59]);
            let invocations = invoked.borrow();
            assert_eq!(invocations.as_slice(), [payload_address]);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
        }
        _ => {
            panic!("original unused Work or original completed output must survive actual refusal")
        }
    }
    let reports = Arc::try_unwrap(reports)
        .unwrap_or_else(|_| panic!("joined family releases its definition"))
        .into_inner()
        .unwrap();
    let [super::super::actors::CounterReport::Retired(id, _, native, report)] = reports.as_slice()
    else {
        panic!("exactly one original family native retirement after refusal");
    };
    assert_eq!(*id.get(), 307);
    assert_retirement(native.as_ref().expect("original family native task joins"));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}
