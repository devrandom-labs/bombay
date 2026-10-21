//! Cold-export constructor refusal at the owning finite address source.
use super::ledger::{LedgerCommand, MailLedger, Receipts};
use super::protocol::{
    self, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReplyRef, ReadRequest,
    ReadRequestRef, configuration, limits, recorded, reserved_address,
};
use crate::actors::ActorExt;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::{ApplicationScope, ExternalActorError, MailboxCapacity};
use crate::remote::{Attempt, Export, ExportError, ExportLimits, Metadata, submission};
use crate::{
    ActorFailureAssessment, ActorRetirement, Application, Completion, RetirementAssessment,
    TrySendError, WorkOutcome,
};
use behavior::{AllocationRejection, BehaviorBase, ClassifySettlement, SettlementStatus};
use futures_util::future::Either;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::ptr;
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};
use zenoh::Config;
use zenoh::bytes::ZBytes;
use zenoh::query::ConsolidationMode;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Append(Box<[u8]>);

#[derive(Debug)]
enum Refusal {
    Scope,
    Native,
    #[expect(
        dead_code,
        reason = "retain the original typed JSON refusal even when the valid trace does not select it"
    )]
    Json(serde_json::Error),
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_endpoint_keeps_the_unused_original_append_constructor() {
    tokio::time::timeout(Duration::from_secs(10), constructor_refusal())
        .await
        .expect("finite constructor trace completes before either native query deadline");
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one complete constructor trace retains all native originals; the immutable provider table remains borrowed through its actual verification"
)]
async fn constructor_refusal() {
    let address = reserved_address();
    let scope = ApplicationScope {
        allocations: ApplicationAddresses::from_next(u64::MAX - 2),
        client_capacity: MailboxCapacity::default(),
        mailbox_capacity: MailboxCapacity::default(),
    };
    // This existing endpoint can read and retire the Ledger without any later allocation.
    let mut receipts = scope.external::<Receipts>().unwrap();
    let node = scope
        .with_root(MailLedger {
            entries: Vec::new(),
        })
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("actual constructor-refusal Node opens"));
    let receiver = node.runtime_id();
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let config = Config::from_json5(
        &serde_json::json!({
            "mode":"peer", "connect":{"endpoints":[address]},
            "scouting":{"multicast":{"enabled":false,"listen":false},"gossip":{"enabled":false}},
            "transport":{"link":{"protocols":["tls"],"tls":{
                "root_ca_certificate":certificates.join("root-ca.pem"),
                "connect_private_key":certificates.join("caller-private-key.pem"),
                "connect_certificate":certificates.join("caller-certificate.pem"),
                "enable_mtls":true,"verify_name_on_connect":true,"use_public_pki":false
            }}}
        })
        .to_string(),
    )
    .unwrap();
    let peer = zenoh::open(config).await.unwrap();
    let caller = peer.zid().to_string();
    let requests = RefCell::new(Vec::new());
    let allocations = RefCell::new(Vec::new());
    let binding = protocol::Binding {
        deployment: "constructor-refusal",
        caller: "caller",
        controller: "owner",
        actor: "ledger",
        host: "receiver",
        protocol: "ledger",
        target_assignment: "ledger",
        logical_session: 251,
    };
    let verify = async |bytes: &[u8], proof: &[u8]| {
        let original = ProtectedMessage {
            protected: bytes.into(),
            proof: proof.into(),
        };
        recorded::verify::<Append>(&original, &requests.borrow(), binding).await
    };
    let current = |receipt: &recorded::Receipt<Append>, runtime: &str, target: MailAddr| {
        let scope = recorded::projection(receipt);
        if scope.kind == Purpose::Request
            && scope.bombay.runtime_generation.as_ref() == runtime
            && scope.bombay.actor_generation == target.0
            && scope.bombay.caller_session.as_ref() == caller
        {
            Ok(())
        } else {
            Err(Refusal::Scope)
        }
    };
    let protect = async |bytes: &[u8], receipt: &recorded::Receipt<Append>| {
        let scope: protocol::ProofScope<Append> =
            serde_json::from_slice(bytes).map_err(Refusal::Json)?;
        let original = recorded::projection(receipt);
        if scope.bombay == original.bombay
            && scope.operation == original.operation
            && scope.kind == Purpose::Admitted
        {
            Ok(Box::<[u8]>::from([101, 103]))
        } else {
            Err(Refusal::Scope)
        }
    };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/constructor-refusal",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| {
            let request: ReadRequest<Append> =
                serde_json::from_slice(bytes).map_err(Refusal::Json)?;
            if request.kind != Purpose::Request {
                return Err(Refusal::Scope);
            }
            allocations.borrow_mut().push(request.operation.0.as_ptr());
            Ok(request.operation)
        },
        submission(
            |Append(original)| Ok::<_, Append>(move || LedgerCommand::Append(original)),
            |receipt: &recorded::Receipt<Append>,
             native: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
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
        &verify,
        &protect,
        |left: &recorded::Receipt<Append>, right: &recorded::Receipt<Append>| {
            recorded::identity(left) == recorded::identity(right)
        },
        &current,
        &current,
    );
    let (configured, configured_requests) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let actor = application
            .actor_address(&target)
            .expect("actual owned root address");
        let mut originals = Vec::new();
        for (sequence, payload) in [(317, [107, 109]), (331, [113, 127])] {
            let mut claims: Metadata = protocol::metadata(
                ProviderProfile::Recorded,
                "ledger",
                caller.clone(),
                receiver.clone(),
            );
            claims.deployment = binding.deployment.into();
            claims.actor_generation = actor.0;
            claims.request_sequence = sequence;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: Append(payload.into()),
            })
            .unwrap()
            .into_boxed_slice();
            let proof: Box<[u8]> = [131, 137].into();
            requests.borrow_mut().push(ProtectedMessage {
                protected: protected.clone(),
                proof: proof.clone(),
            });
            originals.push(ProtectedMessage { protected, proof });
        }
        let publication = configured.send((originals, target.clone()));
        publication.unwrap_or_else(|_| panic!("private peer awaits actual root binding"));
        Ok::<_, Refusal>(target)
    });
    let (server_closed, server_retired) = oneshot::channel();
    let execution = async {
        let outcome = node.run().await;
        let publication = server_closed.send(());
        publication.expect("private peer awaits actual Node completion");
        outcome
    };
    let exchange = async {
        let (originals, target) = configured_requests.await.unwrap();
        let querier = peer
            .declare_querier("bombay/constructor-refusal")
            .await
            .unwrap();
        let listener = querier.matching_listener().await.unwrap();
        loop {
            let status = listener.recv_async().await.unwrap();
            if status.matching() {
                break;
            }
        }
        let withdrawal = listener.undeclare().await;
        let retirement = querier.undeclare().await;
        let mut publications = Vec::new();
        for original in originals {
            let replies = peer
                .get("bombay/constructor-refusal")
                .payload(ZBytes::from(original.protected.as_ref()))
                .attachment(ZBytes::from(original.proof.as_ref()))
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20))
                .await
                .unwrap();
            let mut acquired = Vec::new();
            let closure = loop {
                match replies.recv_async().await {
                    Ok(original) => acquired.push(original),
                    Err(original) => break original,
                }
            };
            publications.push((acquired, closure));
        }
        let admission = receipts
            .send(&target, LedgerCommand::Read(receipts.recipient()))
            .await;
        let original = receipts.receive().await;
        server_retired
            .await
            .expect("Node actually acquired cleanup and close");
        let closed = peer.close().await;
        (
            withdrawal,
            retirement,
            publications,
            admission,
            original,
            closed,
        )
    };
    let (outcome, (withdrawal, retirement, publications, admission, original, peer_closed)) =
        tokio::join!(execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("actual absent-Work execution issued"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("issued cleanup acquired"));
    let (work, root_report, native, notifications, ()) = outcome.into_parts();
    let (absence, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual work absence"));
    match absence {
        None => {}
        Some(never) => match never {},
    }
    let (origin, native) = native.expect("original terminal Ledger");
    let notifications = notifications.expect("both independent notification results");
    let (_inputs, _declarations, declared, withdrawn, diagnostics, retained_requests) =
        export.into_parts();
    let target = bound
        .expect("actual binding acquired")
        .expect("actual root binding succeeded");
    cleanup.expect("actual application cleanup");
    closed.expect("actual Node close");
    peer_closed.expect("independent peer close");
    withdrawal.expect("native listener withdrawn");
    retirement.expect("native querier retired");
    let root_report = root_report.expect("the joined root retains its actual report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(polling.is_none() && disposal.is_none());
    declared
        .expect("actual declaration issued")
        .expect("actual declaration succeeded");
    withdrawn
        .expect("actual withdrawal issued")
        .expect("actual withdrawal succeeded");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    assert!(diagnostics.is_empty() && admission.is_ok());
    assert_eq!(publications.len(), 2);
    assert_eq!(publications[0].0.len(), 1);
    assert!(publications[1].0.is_empty());
    let allocations = allocations.borrow();
    assert_eq!(allocations.len(), 2);
    let original = original.expect("actual independent Ledger Read reply");
    assert_eq!(original.from, origin.address());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        settlements,
        ..
    } = &native
    else {
        panic!("complete original Ledger custody and successful terminal outcome");
    };
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    assert_eq!(custody.descendants, []);
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    let [(sender, accepted)] = custody.behavior.entries.as_slice() else {
        panic!("one real append");
    };
    assert_eq!(accepted.as_ptr(), allocations[0]);
    assert_eq!(accepted.as_ref(), [107, 109]);
    assert_eq!(original.message, [*sender]);
    assert_eq!(retained_requests.len(), 2);
    let actual_publication = publications[0].0[0].result();
    let actual_publication = actual_publication
        .as_ref()
        .expect("actual successful native acknowledgment");
    let actual_bytes = actual_publication.payload().to_bytes();
    let actual_proof = actual_publication
        .attachment()
        .expect("actual original proof attachment")
        .to_bytes();
    let mut keys = Vec::new();
    for (index, request) in retained_requests.into_iter().enumerate() {
        let (
            key,
            raw,
            verified,
            decoded,
            native,
            encoding,
            protection,
            published,
            (request_refusal, reply_refusal, refusal),
            (
                verification_factory,
                verification_poll,
                verification_disposal,
                protection_factory,
                protection_poll,
                protection_disposal,
            ),
        ) = request
            .try_into_parts()
            .unwrap_or_else(|_| panic!("joined cleanup releases every actual request loan"));
        assert!(
            verification_factory.is_none()
                && verification_poll.is_none()
                && verification_disposal.is_none()
                && protection_factory.is_none()
                && protection_poll.is_none()
                && protection_disposal.is_none()
        );
        let originals = requests.borrow();
        assert_eq!(originals.len(), 2);
        assert_eq!(raw.0, originals[index].protected);
        assert_eq!(raw.1, originals[index].proof);
        let receipt = verified
            .expect("actual verification")
            .expect("actual receipt");
        let scope = recorded::projection(&receipt);
        let declared: ReadRequest<Append> = serde_json::from_slice(&raw.0).unwrap();
        assert_eq!(scope.bombay, declared.bombay);
        assert_eq!(scope.kind, declared.kind);
        assert_eq!(scope.operation, declared.operation);
        assert!(decoded.is_none() && request_refusal.is_none() && reply_refusal.is_none());
        let (unsent, constructor, admission) = native.expect("actual native constructor result");
        match index {
            0 => {
                let Attempt::Returned(Ok(native_sender)) = constructor else {
                    panic!("last actual allocation succeeds");
                };
                assert_eq!(native_sender.address(), *sender);
                assert!(matches!(admission, Attempt::Returned(Ok(()))));
                assert!(unsent.is_none() && refusal.is_none());
                let encoded = encoding
                    .expect("actual encoding")
                    .expect("actual encoded acknowledgment");
                let protected = protection
                    .expect("actual protection")
                    .expect("actual protected proof");
                assert_eq!(encoded.as_ref(), actual_bytes.as_ref());
                assert_eq!(protected.as_ref(), actual_proof.as_ref());
                let observed: protocol::ReadReply<(), Append> =
                    serde_json::from_slice(&actual_bytes).unwrap();
                assert_eq!(observed.bombay, scope.bombay);
                assert_eq!(observed.kind, Purpose::Admitted);
                assert_eq!(observed.operation, scope.operation);
                assert_eq!(observed.value, ());
                assert_eq!(published.len(), 1);
                assert!(published.iter().all(Result::is_ok));
                keys.push(key.expect("only accepted constructor installs a replay key"));
            }
            1 => {
                let Attempt::Returned(Err((
                    original_target,
                    ExternalActorError::Allocation(AllocationRejection::Exhausted),
                ))) = constructor
                else {
                    panic!("actual address exhaustion cause");
                };
                assert_eq!(original_target, target);
                let Some(Either::Left(constructor)) = unsent else {
                    panic!("original selected FnOnce remains unused");
                };
                let original = constructor();
                let LedgerCommand::Append(rejected) = original else {
                    panic!("exact original append constructor");
                };
                assert_eq!(rejected.as_ptr(), allocations[1]);
                assert_eq!(rejected.as_ref(), [113, 127]);
                assert!(matches!(admission, Attempt::Unattempted));
                assert!(
                    key.is_none()
                        && encoding.is_none()
                        && protection.is_none()
                        && published.is_empty()
                );
                assert!(refusal.is_none());
            }
            _ => unreachable!("two actual acquisitions"),
        }
    }
    assert_eq!(keys.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "four distinct native constructor refusals retain every original input, cause and joined cleanup before their independent oracles"
)]
async fn cold_declarations_preserve_native_constructor_refusals() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let maximum = NonZeroUsize::new(2).unwrap();
        let valid = ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        };
        let excessive = NonZeroUsize::new(
            Semaphore::MAX_PERMITS
                .checked_add(1)
                .expect("the native semaphore bound has a representable successor"),
        )
        .unwrap();
        let configurations = [
            (
                "bombay/refusals/queued",
                ExportLimits {
                    queued_queries: excessive,
                    ..valid
                },
            ),
            (
                "bombay/refusals/retained",
                ExportLimits {
                    retained_queries: excessive,
                    ..valid
                },
            ),
            (
                "bombay/refusals/storage",
                ExportLimits {
                    retained_requests: NonZeroUsize::new(usize::MAX).unwrap(),
                    ..valid
                },
            ),
            ("", valid),
        ];
        let binding = protocol::Binding {
            deployment: "constructor-refusal",
            caller: "caller",
            controller: "owner",
            actor: "ledger",
            host: "receiver",
            protocol: "ledger",
            target_assignment: "ledger",
            logical_session: 251,
        };
        let registered: [ProtectedMessage; 0] = [];
        let verified = Cell::new(0);
        let protected = Cell::new(0);
        let invoked = Cell::new(0);
        let expected_targets = RefCell::new(Vec::new());
        let verify = async |bytes: &[u8], proof: &[u8]| {
            verified.set(verified.get() + 1);
            recorded::verify::<Append>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &registered,
                binding,
            )
            .await
        };
        // No request is registered and this fixture issues no reply proof.
        let protect = async |_: &[u8], _: &recorded::Receipt<Append>| {
            protected.set(protected.get() + 1);
            Err::<Box<[u8]>, _>(Refusal::Scope)
        };
        let current = |receipt: &recorded::Receipt<Append>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &binding
            {
                Ok(())
            } else {
                Err(Refusal::Scope)
            }
        };
        let declaration = |key: &str, export_limits| {
            Export::new(
            key, export_limits, limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<Append>>(bytes)
                .map(|request| request.operation).map_err(Refusal::Json),
            submission(
                |Append(original)| Ok::<_, Append>(move || LedgerCommand::Append(original)),
                |receipt: &recorded::Receipt<Append>,
                 admission: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                    if !matches!(admission, Attempt::Returned(Ok(()))) {
                        return Err(Refusal::Native);
                    }
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef {
                        bombay: &scope.bombay, kind: Purpose::Admitted,
                        operation: &scope.operation, value: &(),
                    }).map(Vec::into_boxed_slice).map_err(Refusal::Json)
                },
            ),
            &verify, &protect,
            |left: &recorded::Receipt<Append>, right: &recorded::Receipt<Append>| {
                recorded::identity(left) == recorded::identity(right)
            },
            &current, &current,
        )
        };
        let mut observations = Vec::new();
        for (key, export_limits) in configurations {
            let node = Application::new(
                MailLedger {
                    entries: Vec::new(),
                }
                .stop_on_shutdown(),
            )
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("the actual native refusal Node opens"));
            let connection = node.session.clone();
            let initially_closed = connection.is_closed();
            let mut node = node.exports(declaration(key, export_limits), async |application| {
                let target = application.root().established_recipient();
                let checked = application.actor_address(&target);
                expected_targets
                    .borrow_mut()
                    .push((target.clone(), checked));
                Ok::<_, Refusal>(target)
            });
            // Unexpected successful declaration still shuts down through actual Work.
            let outcome = node
                .run_with(|application| {
                    invoked.set(invoked.get() + 1);
                    async move {
                        let shutdown = application.shutdown();
                        let retired = application.lifecycle().retirement().await;
                        (shutdown, retired)
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("the actual refusal host accepts execution"));
            let (export, bound, closed, polling, disposal) = node
                .into_parts()
                .unwrap_or_else(|_| panic!("startup always acquires withdrawal and native close"));
            let (inputs, declarations, startup, withdrawal, diagnostics, requests) =
                export.into_parts();
            let (work, report, native, notifications, ()) = outcome.into_parts();
            observations.push((
                connection,
                initially_closed,
                inputs,
                declarations,
                startup,
                withdrawal,
                diagnostics,
                requests,
                bound,
                closed,
                polling,
                disposal,
                work,
                report,
                native,
                notifications,
            ));
        }
        // Every actual Node has acquired cleanup and close, including any unexpected success.
        let expected_targets = expected_targets.borrow();
        let mut causes = Vec::new();
        for (((key, export_limits), expected), observation) in configurations
            .into_iter()
            .zip(expected_targets.iter())
            .zip(observations)
        {
            let (
                connection,
                initially_closed,
                inputs,
                _declarations,
                startup,
                withdrawal,
                diagnostics,
                requests,
                bound,
                closed,
                polling,
                disposal,
                work,
                report,
                native,
                notifications,
            ) = observation;
            let target = bound
                .expect("the actual target binding ran")
                .expect("the owned Ledger binding succeeds");
            let report = report.expect("the actual root report remains independent");
            let (origin, native) = native.expect("the complete native Ledger remains");
            let notifications = notifications.expect("both actual notifications remain");
            assert!(!initially_closed && connection.is_closed() && closed.is_ok());
            assert_eq!(inputs.0, key);
            assert_eq!(inputs.1, export_limits);
            assert_eq!(target, expected.0);
            assert!(matches!(&expected.1, Ok(address) if *address == origin.address()));
            assert!(polling.is_none() && disposal.is_none());
            assert!(matches!(
                &work,
                WorkOutcome::NotInvoked {
                    startup_error: None,
                    cleanup: Ok(()),
                    ..
                }
            ));
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            assert!(withdrawal.is_none() && diagnostics.is_empty() && requests.is_empty());
            let ActorRetirement::OwnerCancelled {
                custody,
                settlements,
                interpretation,
                source,
                ..
            } = &native
            else {
                panic!("startup refusal retains actual owner-cancelled root custody");
            };
            assert!(interpretation.is_none() && source.is_none());
            assert_eq!(custody.child_failures, []);
            assert!(custody.capability_failures.is_empty());
            assert!(
                custody.additional_failures.is_empty() && custody.retirement_failures.is_empty()
            );
            assert_eq!(custody.descendants, []);
            assert_eq!(custody.behavior.base().entries, []);
            assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
            causes.push(startup);
        }
        assert_eq!((invoked.get(), verified.get(), protected.get()), (0, 0, 0));
        let [queued, retained, storage, invalid] = <[_; 4]>::try_from(causes)
            .unwrap_or_else(|_| panic!("all four original constructor outcomes remain"));
        assert!(matches!(&queued, Some(Err(ExportError::QueryCapacity))));
        assert!(matches!(&retained, Some(Err(ExportError::QueryCapacity))));
        let Some(Err(cause @ ExportError::Storage { cause: original })) = &storage else {
            panic!("actual retained storage refusal remains typed");
        };
        let source = cause
            .source()
            .expect("original native reserve cause remains exposed");
        assert_eq!(ptr::from_ref(source).addr(), ptr::from_ref(original).addr());
        let Some(Err(cause @ ExportError::Declaration { cause: original })) = &invalid else {
            panic!("actual malformed-key declaration refusal remains typed");
        };
        let source = cause
            .source()
            .expect("original native declaration cause remains exposed");
        assert_eq!(
            ptr::from_ref(source).addr(),
            ptr::from_ref(original.as_ref()).addr()
        );
        let display = cause.to_string();
        let diagnostic = format!("{cause:?}");
        assert_eq!(display, "network declaration was refused");
        assert_eq!(diagnostic, "ExportError(network declaration was refused)");
    })
    .await
    .expect("finite constructor setup and all native cleanup finish");
}
