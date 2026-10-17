//! Controlled request-scope comparison; no shipping format or public API selection.

use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, EstablishedRecipient};
use bombay::remote::verify;
use bombay::{ActorNotificationReceipts, Application, Completion};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Increment,
    Read,
    Stop,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LedgerOperation {
    Append,
    Read,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
enum ProofProfile {
    #[serde(rename = "counter-record-test-v1")]
    Recorded,
    #[serde(rename = "counter-scheduled-test-v1")]
    Scheduled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProofPurpose {
    Request,
    ReadReply,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BombayMetadata {
    wire_version: u64,
    schema_version: u64,
    provider_profile: ProofProfile,
    deployment: Box<str>,
    caller: Box<str>,
    controller: Box<str>,
    actor: Box<str>,
    host: Box<str>,
    protocol: Box<str>,
    grant_issuer: Box<str>,
    runtime_generation: u64,
    actor_generation: u64,
    authority_generation: u64,
    request_sequence: u64,
}

// Only the selected Read request is admitted by this comparison, never Append/Stop.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest<Operation> {
    bombay: BombayMetadata,
    kind: ProofPurpose,
    operation: Operation,
}

struct OriginalRequest {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
struct ProtectedRecord {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}

#[derive(Debug)]
enum VerificationError {
    Invalid,
    Malformed(serde_json::Error),
    Unavailable(oneshot::error::RecvError),
}

#[derive(Clone, Copy)]
struct HostingBinding {
    deployment: &'static str,
    caller: &'static str,
    controller: &'static str,
    actor: &'static str,
    host: &'static str,
    protocol: &'static str,
    runtime_generation: u64,
    actor_generation: u64,
}

struct RequestAuthority<Operation> {
    binding: HostingBinding,
    grant_issuer: &'static str,
    authority_generation: u64,
    profile: ProofProfile,
    purpose: ProofPurpose,
    operation: Operation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScopeRejection {
    Binding,
    Grant,
    Profile,
    Purpose,
    Endpoints,
    Operation,
    Version,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestCase {
    Accepted,
    WrongScope,
    WrongProfile,
    WrongPurpose,
    Retargeted,
}

// Shared finite lookup equation, not the entire existing Counter seven-meaning codec.
fn registered_request<Operation: DeserializeOwned + Eq>(
    original: &OriginalRequest,
    records: &[ProtectedRecord],
    profile: ProofProfile,
    operation: &Operation,
) -> Result<ReadRequest<Operation>, VerificationError> {
    let request: ReadRequest<Operation> =
        serde_json::from_slice(&original.protected).map_err(VerificationError::Malformed)?;
    if request.kind != ProofPurpose::Request
        || &request.operation != operation
        || request.bombay.provider_profile != profile
        || request.bombay.wire_version != 1
        || request.bombay.schema_version != 1
        || !records
            .iter()
            .any(|record| record.protected == original.protected && record.proof == original.proof)
    {
        return Err(VerificationError::Invalid);
    }
    Ok(request)
}

mod recorded {
    use super::*;
    pub(super) struct Receipt<Operation> {
        request: ReadRequest<Operation>,
        producer: &'static str,
        intended_consumer: &'static str,
    }
    pub(super) fn projection<Operation>(
        receipt: &Receipt<Operation>,
    ) -> (&ReadRequest<Operation>, &'static str, &'static str) {
        (
            &receipt.request,
            receipt.producer,
            receipt.intended_consumer,
        )
    }
    #[expect(
        clippy::unused_async,
        reason = "defer verification and receipt creation until the actual producer is polled"
    )]
    pub(super) async fn verify<Operation: DeserializeOwned + Eq>(
        original: &OriginalRequest,
        records: &[ProtectedRecord],
        operation: Operation,
        producer: &'static str,
        intended_consumer: &'static str,
    ) -> Result<Receipt<Operation>, VerificationError> {
        registered_request(original, records, ProofProfile::Recorded, &operation).and_then(
            |request| {
                if request.bombay.caller.as_ref() != producer
                    || request.bombay.host.as_ref() != intended_consumer
                {
                    return Err(VerificationError::Invalid);
                }
                Ok(Receipt {
                    request,
                    producer,
                    intended_consumer,
                })
            },
        )
    }
}

mod scheduled {
    use super::*;
    pub(super) struct Receipt<Operation> {
        request: ReadRequest<Operation>,
        producer: &'static str,
        intended_consumer: &'static str,
        evidence: Box<[u8]>,
    }
    pub(super) fn projection<Operation>(
        receipt: &Receipt<Operation>,
    ) -> (&ReadRequest<Operation>, &'static str, &'static str) {
        (
            &receipt.request,
            receipt.producer,
            receipt.intended_consumer,
        )
    }
    pub(super) fn evidence<Operation>(receipt: &Receipt<Operation>) -> &[u8] {
        &receipt.evidence
    }
    pub(super) async fn verify<Operation: DeserializeOwned + Eq>(
        original: &OriginalRequest,
        records: &[ProtectedRecord],
        operation: Operation,
        producer: &'static str,
        intended_consumer: &'static str,
    ) -> Result<Receipt<Operation>, VerificationError> {
        let verified = registered_request(original, records, ProofProfile::Scheduled, &operation)
            .and_then(|request| {
                if request.bombay.caller.as_ref() != producer
                    || request.bombay.host.as_ref() != intended_consumer
                {
                    return Err(VerificationError::Invalid);
                }
                Ok(Receipt {
                    request,
                    producer,
                    intended_consumer,
                    evidence: Box::from([19]),
                })
            });
        let (publication, receiving) = oneshot::channel();
        let published = publication.send(verified);
        match published {
            Ok(()) => {}
            Err(original) => return original,
        }
        receiving.await.map_err(VerificationError::Unavailable)?
    }
}

fn check_request_scope<Operation: Eq>(
    proof: (&ReadRequest<Operation>, &str, &str),
    authority: &RequestAuthority<Operation>,
) -> Result<(), ScopeRejection> {
    let (request, producer, intended_consumer) = proof;
    let declared = &request.bombay;
    let current = authority.binding;
    if declared.wire_version != 1 || declared.schema_version != 1 {
        return Err(ScopeRejection::Version);
    }
    if declared.deployment.as_ref() != current.deployment
        || declared.caller.as_ref() != current.caller
        || declared.controller.as_ref() != current.controller
        || declared.actor.as_ref() != current.actor
        || declared.host.as_ref() != current.host
        || declared.protocol.as_ref() != current.protocol
        || declared.runtime_generation != current.runtime_generation
        || declared.actor_generation != current.actor_generation
    {
        return Err(ScopeRejection::Binding);
    }
    if declared.grant_issuer.as_ref() != authority.grant_issuer
        || declared.authority_generation != authority.authority_generation
    {
        return Err(ScopeRejection::Grant);
    }
    if declared.provider_profile != authority.profile {
        return Err(ScopeRejection::Profile);
    }
    if request.kind != authority.purpose {
        return Err(ScopeRejection::Purpose);
    }
    if producer != current.caller || intended_consumer != current.host {
        return Err(ScopeRejection::Endpoints);
    }
    if request.operation != authority.operation {
        return Err(ScopeRejection::Operation);
    }
    Ok(())
}

fn request_authority<Operation>(
    actor: &'static str,
    protocol: &'static str,
    profile: ProofProfile,
    operation: Operation,
    case: RequestCase,
) -> RequestAuthority<Operation> {
    let mut authority = RequestAuthority {
        binding: HostingBinding {
            deployment: "comparison",
            caller: "caller",
            controller: "owner",
            actor,
            host: "host",
            protocol,
            runtime_generation: 3,
            actor_generation: 5,
        },
        grant_issuer: "issuer",
        authority_generation: 7,
        profile,
        purpose: ProofPurpose::Request,
        operation,
    };
    match case {
        RequestCase::Accepted | RequestCase::Retargeted => {}
        RequestCase::WrongScope => authority.binding.actor = "other-export",
        RequestCase::WrongProfile => {
            authority.profile = match profile {
                ProofProfile::Recorded => ProofProfile::Scheduled,
                ProofProfile::Scheduled => ProofProfile::Recorded,
            }
        }
        RequestCase::WrongPurpose => authority.purpose = ProofPurpose::ReadReply,
    }
    authority
}

#[expect(
    clippy::too_many_arguments,
    reason = "compare two actual protocol loans and opaque projections without a forwarding product"
)]
#[expect(
    clippy::too_many_lines,
    reason = "retain both actual applications and complete native and notification custody in one causal scope"
)]
async fn counter_and_ledger_reads<CounterReceipt, LedgerReceipt>(
    counter_original: &OriginalRequest,
    ledger_original: &OriginalRequest,
    counter_acquired: Option<&Result<CounterReceipt, VerificationError>>,
    ledger_acquired: Option<&Result<LedgerReceipt, VerificationError>>,
    counter_projection: fn(
        &CounterReceipt,
    ) -> (&ReadRequest<CounterOperation>, &'static str, &'static str),
    ledger_projection: fn(
        &LedgerReceipt,
    ) -> (&ReadRequest<LedgerOperation>, &'static str, &'static str),
    profile: ProofProfile,
    case: RequestCase,
) {
    let counter_allocation = counter_original.protected.as_ptr();
    let counter_proof = counter_original.proof.as_ptr();
    let ledger_allocation = ledger_original.protected.as_ptr();
    let ledger_proof = ledger_original.proof.as_ptr();
    let counter_verified = counter_acquired
        .expect("actual acquired result")
        .as_ref()
        .unwrap_or_else(|_| panic!("registered Counter request"));
    let ledger_verified = ledger_acquired
        .expect("actual acquired result")
        .as_ref()
        .unwrap_or_else(|_| panic!("registered Ledger request"));
    let run_counter = async |selected: Option<(HostingBinding, EstablishedRecipient<Counter>)>| {
        Application::new(Counter::new().stop_on_shutdown())
            .run_with(|application| async move {
                let mut replies = application.client().expect("actual Counter reply owner");
                let caller = application
                    .external::<Receipts>()
                    .expect("actual Counter origin");
                let mut authority =
                    request_authority("counter", "counter", profile, CounterOperation::Read, case);
                let target = match selected {
                    Some((binding, target)) => {
                        authority.binding = binding;
                        target
                    }
                    None => application.root().established_recipient(),
                };
                let checked = check_request_scope(counter_projection(counter_verified), &authority);
                let admitted = match checked {
                    Ok(()) => {
                        Some(caller.try_send(&target, CounterMessage::Read(replies.recipient())))
                    }
                    Err(_) => None,
                };
                let read = match &admitted {
                    Some(Ok(())) => replies.receive().await,
                    _ => None,
                };
                replies.close_admission();
                let exhausted = replies.receive().await;
                let shutdown = application.shutdown();
                (
                    checked, admitted, read, exhausted, shutdown, caller, replies,
                )
            })
            .await
    };
    let (counter_result, spare) = match case {
        RequestCase::Retargeted => {
            let (work, native, notices, ()) = Box::pin(
                Application::new(Counter::new().stop_on_shutdown()).run_with(
                    |application| async move {
                        let binding = request_authority(
                            "spare-counter",
                            "counter",
                            profile,
                            CounterOperation::Read,
                            RequestCase::Accepted,
                        )
                        .binding;
                        let hosted = (binding, application.root().established_recipient());
                        let counter = run_counter(Some(hosted)).await;
                        let shutdown = application.shutdown();
                        (counter, shutdown)
                    },
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("entered spare host"))
            .into_parts();
            let ((counter, shutdown), cleanup) = work
                .try_into_completed()
                .unwrap_or_else(|_| panic!("original spare work"));
            (counter, Some((shutdown, cleanup, native, notices)))
        }
        _ => (run_counter(None).await, None),
    };
    let ledger_case = match case {
        RequestCase::Retargeted => RequestCase::Accepted,
        original => original,
    };
    let ledger = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let mut replies = application.client().expect("actual Ledger reply owner");
        let caller = application
            .external::<Receipts>()
            .expect("actual Ledger origin");
        let target = application.root().established_recipient();
        let authority = request_authority(
            "ledger",
            "ledger",
            profile,
            LedgerOperation::Read,
            ledger_case,
        );
        let checked = check_request_scope(ledger_projection(ledger_verified), &authority);
        let admitted = match checked {
            Ok(()) => Some(caller.try_send(&target, LedgerCommand::Read(replies.recipient()))),
            Err(_) => None,
        };
        let read = match &admitted {
            Some(Ok(())) => replies.receive().await,
            _ => None,
        };
        replies.close_admission();
        let exhausted = replies.receive().await;
        let shutdown = match &admitted {
            Some(Ok(())) => None,
            _ => Some(application.shutdown()),
        };
        (
            checked, admitted, read, exhausted, shutdown, caller, replies,
        )
    })
    .await;
    // Every actual Application receiving product is acquired before semantic oracles.
    let (counter_work, counter_native, counter_notifications, ()) = counter_result
        .unwrap_or_else(|_| panic!("entered Counter host"))
        .into_parts();
    let (ledger_work, ledger_native, ledger_notifications, ()) = ledger
        .unwrap_or_else(|_| panic!("entered Ledger host"))
        .into_parts();
    let (
        (
            counter_checked,
            counter_admitted,
            counter_read,
            counter_exhausted,
            counter_shutdown,
            counter_caller,
            counter_replies,
        ),
        counter_cleanup,
    ) = counter_work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Counter work"));
    let (
        (
            ledger_checked,
            ledger_admitted,
            ledger_read,
            ledger_exhausted,
            ledger_shutdown,
            ledger_caller,
            ledger_replies,
        ),
        ledger_cleanup,
    ) = ledger_work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Ledger work"));
    assert!(matches!(counter_cleanup, Ok(())));
    assert!(matches!(ledger_cleanup, Ok(())));
    assert!(matches!(
        counter_notifications,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(())
        })
    ));
    assert!(matches!(
        ledger_notifications,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(())
        })
    ));
    let (counter_origin, counter_retirement) = counter_native.expect("original Counter native");
    let (ledger_origin, ledger_retirement) = ledger_native.expect("original Ledger native");
    if let Some(read) = &counter_read {
        assert_eq!(read.from, counter_origin.address());
    }
    if let Some(read) = &ledger_read {
        assert_eq!(read.from, ledger_origin.address());
    }
    assert_eq!(counter_retirement.completion(), Some(&Completion::Stopped));
    assert_eq!(ledger_retirement.completion(), Some(&Completion::Stopped));
    let counter_custody = counter_retirement
        .custody()
        .expect("complete Counter custody");
    let ledger_custody = ledger_retirement
        .custody()
        .expect("complete Ledger custody");
    assert_eq!(counter_custody.descendants, []);
    assert_eq!(counter_custody.child_failures, []);
    assert_eq!(ledger_custody.descendants, []);
    assert_eq!(ledger_custody.child_failures, []);
    assert_eq!(ledger_custody.behavior.base().entries.as_slice(), &[]);
    if let Some((shutdown, cleanup, native, notices)) = spare {
        assert_eq!(shutdown, Ok(()));
        assert!(matches!(cleanup, Ok(())));
        assert!(matches!(
            notices,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(())
            })
        ));
        let (_, retirement) = native.expect("original spare native");
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        let custody = retirement.custody().expect("complete spare custody");
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
    }
    let expected = match case {
        RequestCase::Accepted => Ok(()),
        RequestCase::WrongScope | RequestCase::Retargeted => Err(ScopeRejection::Binding),
        RequestCase::WrongProfile => Err(ScopeRejection::Profile),
        RequestCase::WrongPurpose => Err(ScopeRejection::Purpose),
    };
    match case {
        RequestCase::Accepted => {
            assert!(matches!(counter_admitted, Some(Ok(()))));
            assert_eq!(counter_read.as_ref().map(|read| read.message), Some(0));
        }
        RequestCase::WrongScope
        | RequestCase::WrongProfile
        | RequestCase::WrongPurpose
        | RequestCase::Retargeted => {
            assert!(counter_read.is_none());
            assert!(counter_admitted.is_none());
        }
    }
    match ledger_case {
        RequestCase::Accepted => {
            assert!(matches!(ledger_admitted, Some(Ok(()))));
            assert_eq!(
                ledger_read.as_ref().map(|read| read.message.as_slice()),
                Some(&[][..])
            );
        }
        RequestCase::WrongScope
        | RequestCase::WrongProfile
        | RequestCase::WrongPurpose
        | RequestCase::Retargeted => {
            assert!(ledger_read.is_none());
            assert!(ledger_admitted.is_none());
        }
    }
    assert_eq!(counter_checked, expected);
    assert_eq!(
        ledger_checked,
        match case {
            RequestCase::Retargeted => Ok(()),
            _ => expected,
        }
    );
    assert!(counter_exhausted.is_none());
    assert!(ledger_exhausted.is_none());
    assert_eq!(counter_shutdown, Ok(()));
    match ledger_case {
        RequestCase::Accepted => assert!(ledger_shutdown.is_none()),
        _ => assert_eq!(ledger_shutdown, Some(Ok(()))),
    }
    assert_ne!(counter_caller.address(), counter_replies.address());
    assert_ne!(ledger_caller.address(), ledger_replies.address());
    assert_eq!(
        counter_projection(counter_verified)
            .0
            .bombay
            .request_sequence,
        41
    );
    assert_eq!(
        ledger_projection(ledger_verified).0.bombay.request_sequence,
        43
    );
    let (counter_bytes, ledger_bytes) = match profile {
        ProofProfile::Recorded => (COUNTER_RECORDED, LEDGER_RECORDED),
        ProofProfile::Scheduled => (COUNTER_SCHEDULED, LEDGER_SCHEDULED),
    };
    assert_eq!(counter_original.protected.as_ref(), counter_bytes);
    assert_eq!(counter_original.proof.as_ref(), &[41]);
    assert_eq!(ledger_original.protected.as_ref(), ledger_bytes);
    assert_eq!(ledger_original.proof.as_ref(), &[43]);
    assert_eq!(counter_original.protected.as_ptr(), counter_allocation);
    assert_eq!(counter_original.proof.as_ptr(), counter_proof);
    assert_eq!(ledger_original.protected.as_ptr(), ledger_allocation);
    assert_eq!(ledger_original.proof.as_ptr(), ledger_proof);
}

const COUNTER_RECORDED: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":3,"actor_generation":5,"authority_generation":7,"request_sequence":41},"kind":"request","operation":"read"}"#;
const LEDGER_RECORDED: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":3,"actor_generation":5,"authority_generation":7,"request_sequence":43},"kind":"request","operation":"read"}"#;
const COUNTER_SCHEDULED: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-scheduled-test-v1","deployment":"comparison","caller":"caller","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":3,"actor_generation":5,"authority_generation":7,"request_sequence":41},"kind":"request","operation":"read"}"#;
const LEDGER_SCHEDULED: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-scheduled-test-v1","deployment":"comparison","caller":"caller","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":3,"actor_generation":5,"authority_generation":7,"request_sequence":43},"kind":"request","operation":"read"}"#;

#[test]
fn malformed_request_retains_the_original_decoder_error() {
    let original = OriginalRequest {
        protected: Box::from(&b"{"[..]),
        proof: Box::from([41]),
    };
    let decoded = registered_request(
        &original,
        &[],
        ProofProfile::Recorded,
        &CounterOperation::Read,
    );
    let Err(VerificationError::Malformed(cause)) = decoded else {
        panic!("truncated JSON retains the actual decoder cause");
    };
    assert!(cause.is_eof());
    assert_eq!(original.protected.as_ref(), b"{");
    assert_eq!(original.proof.as_ref(), &[41]);
}

async fn counter_and_ledger_request_authority(profile: ProofProfile) {
    for case in [
        RequestCase::Accepted,
        RequestCase::WrongScope,
        RequestCase::WrongProfile,
        RequestCase::WrongPurpose,
        RequestCase::Retargeted,
    ] {
        let (counter_bytes, ledger_bytes) = match profile {
            ProofProfile::Recorded => (COUNTER_RECORDED, LEDGER_RECORDED),
            ProofProfile::Scheduled => (COUNTER_SCHEDULED, LEDGER_SCHEDULED),
        };
        let counter_original = OriginalRequest {
            protected: counter_bytes.into(),
            proof: Box::from([41]),
        };
        let ledger_original = OriginalRequest {
            protected: ledger_bytes.into(),
            proof: Box::from([43]),
        };
        let records = [
            ProtectedRecord {
                protected: counter_bytes.into(),
                proof: Box::from([41]),
            },
            ProtectedRecord {
                protected: ledger_bytes.into(),
                proof: Box::from([43]),
            },
        ];
        match profile {
            ProofProfile::Recorded => {
                let mut counter = None;
                let mut ledger = None;
                verify(&counter_original, &mut counter, |original| {
                    recorded::verify(original, &records, CounterOperation::Read, "caller", "host")
                })
                .await;
                verify(&ledger_original, &mut ledger, |original| {
                    recorded::verify(original, &records, LedgerOperation::Read, "caller", "host")
                })
                .await;
                counter_and_ledger_reads(
                    &counter_original,
                    &ledger_original,
                    counter.as_ref(),
                    ledger.as_ref(),
                    recorded::projection,
                    recorded::projection,
                    profile,
                    case,
                )
                .await;
            }
            ProofProfile::Scheduled => {
                let mut counter = None;
                let mut ledger = None;
                verify(&counter_original, &mut counter, |original| {
                    scheduled::verify(original, &records, CounterOperation::Read, "caller", "host")
                })
                .await;
                verify(&ledger_original, &mut ledger, |original| {
                    scheduled::verify(original, &records, LedgerOperation::Read, "caller", "host")
                })
                .await;
                counter_and_ledger_reads(
                    &counter_original,
                    &ledger_original,
                    counter.as_ref(),
                    ledger.as_ref(),
                    scheduled::projection,
                    scheduled::projection,
                    profile,
                    case,
                )
                .await;
                assert_eq!(
                    scheduled::evidence(counter.as_ref().unwrap().as_ref().unwrap()),
                    &[19]
                );
                assert_eq!(
                    scheduled::evidence(ledger.as_ref().unwrap().as_ref().unwrap()),
                    &[19]
                );
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn counter_and_ledger_recorded_request_scope() {
    counter_and_ledger_request_authority(ProofProfile::Recorded).await;
}

#[tokio::test(flavor = "current_thread")]
async fn counter_and_ledger_scheduled_request_scope() {
    counter_and_ledger_request_authority(ProofProfile::Scheduled).await;
}
