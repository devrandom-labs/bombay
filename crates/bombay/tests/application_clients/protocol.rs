use bombay::behavior::{EstablishedRecipient, MessageProtocol};
use bombay::lifecycle::{Task, TaskMessage, TaskResult};
use bombay::prelude::{MailAddr, StopOnShutdown};
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use bombay::remote::{Metadata, json};
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, ActorRetirementReport, RetirementAssessment,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::env;
use std::future::{Ready, ready};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) struct ProtectedMessage {
    pub(super) protected: Box<[u8]>,
    pub(super) proof: Box<[u8]>,
}
pub(super) type RequestBatch = MessageProtocol<MailAddr, TaskResult<Vec<ProtectedMessage>>>;
pub(super) type RequestTask =
    Task<MailAddr, Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
pub(super) type BatchCommand =
    TaskMessage<Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
pub(super) type SenderRoot = StopOnShutdown<RequestTask>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum CounterOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum LedgerOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Purpose {
    Request,
    ReadReply,
    Processed,
    Admitted,
    Stopped,
}
#[derive(Clone, Copy)]
pub(super) enum ProviderProfile {
    Recorded,
    Scheduled,
}
impl ProviderProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Recorded => "read-record-test-v1",
            Self::Scheduled => "read-scheduled-test-v1",
        }
    }
}
#[derive(Clone, Copy)]
// Observation selector only: Resend requires an actual production retention mutant.
pub(super) enum ReadRetention {
    ReuseFirst,
    Resend,
}
pub(super) fn read_retention() -> ReadRetention {
    match env::var("BOMBAY_NODE_READ_REPLAY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("reuse-first") => ReadRetention::ReuseFirst,
        Ok("resend") => ReadRetention::Resend,
        original => panic!("unsupported explicit test comparison: {original:?}"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadRequest<Operation> {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Operation,
}
#[derive(Serialize)]
pub(super) struct ReadRequestRef<'a, Operation> {
    pub(super) bombay: &'a Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadReply<Value, Operation> {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Operation,
    pub(super) value: Value,
}
#[derive(Serialize)]
pub(super) struct ReadReplyRef<'a, Value, Operation> {
    pub(super) bombay: &'a Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Operation,
    pub(super) value: &'a Value,
}
// Provider scope ignores protocol-owned value fields; concrete DTO decoding above
// is independent. Matching the entire registered byte/proof pair remains mandatory.
#[derive(Deserialize)]
pub(super) struct ProofScope<Operation> {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Operation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) deployment: &'static str,
    pub(super) caller: &'static str,
    pub(super) controller: &'static str,
    pub(super) actor: &'static str,
    pub(super) host: &'static str,
    pub(super) protocol: &'static str,
    pub(super) target_assignment: &'static str,
    pub(super) logical_session: u64,
}

#[derive(Debug)]
pub(super) enum VerificationError {
    Invalid,
    Declaration(serde_json::Error),
    Unavailable(oneshot::error::RecvError),
}
fn registered<Operation: DeserializeOwned>(
    original: &ProtectedMessage,
    records: &[ProtectedMessage],
    profile: ProviderProfile,
    binding: Binding,
) -> Result<ProofScope<Operation>, VerificationError> {
    let scope: ProofScope<Operation> =
        serde_json::from_slice(&original.protected).map_err(VerificationError::Declaration)?;
    if scope.bombay.provider_profile.as_ref() != profile.name()
        || scope.bombay.deployment.as_ref() != binding.deployment
        || scope.bombay.caller.as_ref() != binding.caller
        || scope.bombay.controller.as_ref() != binding.controller
        || scope.bombay.actor.as_ref() != binding.actor
        || scope.bombay.host.as_ref() != binding.host
        || scope.bombay.protocol.as_ref() != binding.protocol
        || !records
            .iter()
            .any(|record| record.protected == original.protected && record.proof == original.proof)
    {
        return Err(VerificationError::Invalid);
    }
    Ok(scope)
}
pub(super) mod recorded {
    use super::*;
    pub(in super::super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
        binding: Binding,
    }
    pub(in super::super) fn identity<Operation>(receipt: &Receipt<Operation>) -> (&Binding, u64) {
        (&receipt.binding, receipt.scope.bombay.request_sequence)
    }
    pub(in super::super) fn projection<Operation>(
        receipt: &Receipt<Operation>,
    ) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(in super::super) fn verify<Operation: DeserializeOwned>(
        original: &ProtectedMessage,
        records: &[ProtectedMessage],
        binding: Binding,
    ) -> Ready<Result<Receipt<Operation>, VerificationError>> {
        ready(
            registered(original, records, ProviderProfile::Recorded, binding)
                .map(|scope| Receipt { scope, binding }),
        )
    }
}
pub(super) mod scheduled {
    use super::*;
    pub(in super::super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
        binding: Binding,
        original_proof: Box<[u8]>,
    }
    pub(in super::super) fn identity<Operation>(receipt: &Receipt<Operation>) -> (&Binding, u64) {
        (&receipt.binding, receipt.scope.bombay.request_sequence)
    }
    pub(in super::super) fn projection<Operation>(
        receipt: &Receipt<Operation>,
    ) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(in super::super) fn proof<Operation>(receipt: &Receipt<Operation>) -> &[u8] {
        &receipt.original_proof
    }
    pub(in super::super) async fn verify<Operation: DeserializeOwned>(
        original: &ProtectedMessage,
        records: &[ProtectedMessage],
        binding: Binding,
    ) -> Result<Receipt<Operation>, VerificationError> {
        let (published, received) = oneshot::channel();
        let original =
            registered(original, records, ProviderProfile::Scheduled, binding).map(|scope| {
                Receipt {
                    scope,
                    binding,
                    original_proof: original.proof.clone(),
                }
            });
        let transfer = published.send(original);
        match transfer {
            Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
            Err(original) => original,
        }
    }
}

pub(super) fn configuration(address: &str, role: &EndpointRole) -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let (certificate, private_key) = match role {
        EndpointRole::Receiver => ("recipient-certificate.pem", "recipient-private-key.pem"),
        EndpointRole::Caller => ("caller-certificate.pem", "caller-private-key.pem"),
    };
    let endpoints = ConnectionEndpoints {
        addresses: vec![
            address
                .strip_prefix("tls/")
                .expect("the fixture uses TLS")
                .to_owned(),
        ],
        certificate: CredentialSource::File(certificates.join(certificate)),
        private_key: CredentialSource::File(certificates.join(private_key)),
    };
    let layout = match role {
        EndpointRole::Receiver => NetworkLayout::Peer {
            connect: None,
            listen: Some(endpoints),
        },
        EndpointRole::Caller => NetworkLayout::Peer {
            connect: Some(endpoints),
            listen: None,
        },
    };
    Network {
        layout,
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}
pub(super) enum EndpointRole {
    Caller,
    Receiver,
}
pub(super) fn reserved_address() -> String {
    let reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    format!("tls/localhost:{}", address.port())
}
pub(super) fn metadata(
    profile: ProviderProfile,
    protocol: &str,
    caller: String,
    receiver: String,
) -> Metadata {
    Metadata {
        wire_version: 1,
        schema_version: 1,
        provider_profile: profile.name().into(),
        deployment: "two-node-read-study".into(),
        caller: "caller".into(),
        caller_session: caller.into(),
        controller: "owner".into(),
        actor: protocol.into(),
        host: "receiver".into(),
        protocol: protocol.into(),
        grant_issuer: "issuer".into(),
        runtime_generation: receiver.into(),
        actor_generation: 0,
        authority_generation: 19,
        request_sequence: 23,
    }
}
pub(super) fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(64).unwrap(),
        token_bytes: NonZeroUsize::new(128).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    }
}
pub(super) fn current_scope<Operation: Eq>(
    scope: &ProofScope<Operation>,
    current: &Metadata,
    operation: &Operation,
    purpose: Purpose,
    deadline: Instant,
) -> Result<(), VerificationError> {
    if scope.bombay != *current
        || scope.operation != *operation
        || scope.kind != purpose
        || Instant::now() >= deadline
    {
        return Err(VerificationError::Invalid);
    }
    Ok(())
}

pub(super) fn assert_notifications(
    receipts: &Result<ActorNotificationReceipts, oneshot::error::RecvError>,
) {
    let receipts = receipts.as_ref().unwrap();
    assert!(receipts.termination.is_ok());
    assert!(receipts.retirement.is_ok());
}
pub(super) fn assert_report(report: ActorRetirementReport) {
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}

pub(super) fn same_recorded_counter(
    a: &recorded::Receipt<CounterOperation>,
    b: &recorded::Receipt<CounterOperation>,
) -> bool {
    recorded::identity(a) == recorded::identity(b)
}
pub(super) fn same_scheduled_counter(
    a: &scheduled::Receipt<CounterOperation>,
    b: &scheduled::Receipt<CounterOperation>,
) -> bool {
    scheduled::identity(a) == scheduled::identity(b)
}
pub(super) fn same_recorded_ledger(
    a: &recorded::Receipt<LedgerOperation>,
    b: &recorded::Receipt<LedgerOperation>,
) -> bool {
    recorded::identity(a) == recorded::identity(b)
}
pub(super) fn same_scheduled_ledger(
    a: &scheduled::Receipt<LedgerOperation>,
    b: &scheduled::Receipt<LedgerOperation>,
) -> bool {
    scheduled::identity(a) == scheduled::identity(b)
}
