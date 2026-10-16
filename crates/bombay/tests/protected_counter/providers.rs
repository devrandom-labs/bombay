//! Finite read providers shared by native custody and isolated sharing-cost tests.
use super::requester::{Binding, CounterOperation as NativeOperation, Identity, Scope};
use bombay::remote::{Metadata, json};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::future::{Ready, ready};
use std::num::NonZeroUsize;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) struct ProtectedMessage {
    pub(super) protected: Box<[u8]>,
    pub(super) proof: Box<[u8]>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum CounterOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Purpose {
    Request,
    ReadReply,
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadRequest {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: CounterOperation,
}
#[derive(Serialize)]
pub(super) struct ReadRequestRef<'a> {
    pub(super) bombay: &'a Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: CounterOperation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadReply {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: CounterOperation,
    pub(super) value: u64,
}
#[derive(Serialize)]
pub(super) struct ReadReplyRef<'a> {
    pub(super) bombay: &'a Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: CounterOperation,
    pub(super) value: &'a u64,
}
// Provider scope ignores protocol-owned value fields; concrete DTO decoding above
// is independent. Matching the entire registered byte/proof pair remains mandatory.
#[derive(Deserialize)]
pub(super) struct ProofScope {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: CounterOperation,
}
#[derive(Debug)]
pub(super) enum VerificationError {
    Invalid,
    ProofCapacity,
    Declaration(serde_json::Error),
    Unavailable(oneshot::error::RecvError),
}
fn registered(
    original: &ProtectedMessage,
    records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    profile: ProviderProfile,
) -> Result<(ProofScope, Scope), VerificationError> {
    let claims: ProofScope =
        serde_json::from_slice(&original.protected).map_err(VerificationError::Declaration)?;
    let records = records.borrow();
    let Some((scope, _)) = records.iter().find(|(_, record)| {
        record.protected == original.protected && record.proof == original.proof
    }) else {
        return Err(VerificationError::Invalid);
    };
    if claims.bombay.provider_profile.as_ref() != profile.name() {
        return Err(VerificationError::Invalid);
    }
    Ok((claims, *scope))
}
fn request_proof(
    bytes: &[u8],
    configured: &Metadata,
    scope: Scope,
    records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    profile: ProviderProfile,
) -> Result<Box<[u8]>, VerificationError> {
    let declared: ReadRequest =
        serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
    let binding = scope.identity.binding;
    if declared.bombay != *configured
        || configured.provider_profile.as_ref() != profile.name()
        || declared.kind != Purpose::Request
        || declared.operation != CounterOperation::Read
        || configured.deployment.as_ref() != binding.deployment
        || configured.caller.as_ref() != binding.caller
        || configured.controller.as_ref() != binding.controller
        || configured.actor.as_ref() != binding.actor
        || configured.host.as_ref() != binding.host
        || configured.protocol.as_ref() != binding.protocol
        || configured.actor_generation != binding.actor_generation
        || configured.grant_issuer.as_ref() != scope.grant_issuer
        || configured.authority_generation != scope.authority_generation
        || configured.request_sequence != scope.identity.sequence
        || scope.operation != NativeOperation::Read
    {
        return Err(VerificationError::Invalid);
    }
    let mut records = records.borrow_mut();
    if records.len() >= 2 {
        return Err(VerificationError::ProofCapacity);
    }
    let proof = Box::<[u8]>::from([7, 11]);
    records.push((
        scope,
        ProtectedMessage {
            protected: bytes.into(),
            proof: proof.clone(),
        },
    ));
    Ok(proof)
}
pub(super) mod recorded {
    use super::*;
    pub(in super::super) struct Receipt {
        claims: ProofScope,
        logical: Scope,
    }
    pub(in super::super) fn projection(receipt: &Receipt) -> (&ProofScope, Scope) {
        (&receipt.claims, receipt.logical)
    }
    pub(in super::super) fn protect_request(
        bytes: &[u8],
        configured: &Metadata,
        scope: Scope,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Ready<Result<Box<[u8]>, VerificationError>> {
        ready(request_proof(
            bytes,
            configured,
            scope,
            records,
            ProviderProfile::Recorded,
        ))
    }
    pub(in super::super) fn protect(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Ready<Result<Box<[u8]>, VerificationError>> {
        let mut records = records.borrow_mut();
        let result = if records.len() < 2 {
            let proof = Box::<[u8]>::from([13, 17]);
            records.push((
                request.logical,
                ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                },
            ));
            Ok(proof)
        } else {
            Err(VerificationError::ProofCapacity)
        };
        ready(result)
    }
    pub(in super::super) fn verify(
        original: &ProtectedMessage,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Ready<Result<Receipt, VerificationError>> {
        ready(
            registered(original, records, ProviderProfile::Recorded)
                .map(|(claims, logical)| Receipt { claims, logical }),
        )
    }
    pub(in super::super) async fn protect_when_released(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
        release: oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError> {
        release.await.map_err(VerificationError::Unavailable)?;
        protect(bytes, request, records).await
    }
}
pub(super) mod scheduled {
    use super::*;
    pub(in super::super) struct Receipt {
        claims: ProofScope,
        logical: Scope,
        original_proof: Box<[u8]>,
    }
    pub(in super::super) fn projection(receipt: &Receipt) -> (&ProofScope, Scope) {
        (&receipt.claims, receipt.logical)
    }
    pub(in super::super) fn proof(receipt: &Receipt) -> &[u8] {
        &receipt.original_proof
    }
    pub(in super::super) async fn protect_request(
        bytes: &[u8],
        configured: &Metadata,
        scope: Scope,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError> {
        let (published, received) = oneshot::channel();
        let result = request_proof(
            bytes,
            configured,
            scope,
            records,
            ProviderProfile::Scheduled,
        );
        let transfer = published.send(result);
        match transfer {
            Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
            Err(original) => original,
        }
    }
    pub(in super::super) async fn protect(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError> {
        let (published, received) = oneshot::channel();
        let result = {
            let mut records = records.borrow_mut();
            if records.len() < 2 {
                let proof = Box::<[u8]>::from([13, 17]);
                records.push((
                    request.logical,
                    ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.clone(),
                    },
                ));
                Ok(proof)
            } else {
                Err(VerificationError::ProofCapacity)
            }
        };
        let transfer = published.send(result);
        match transfer {
            Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
            Err(original) => original,
        }
    }
    pub(in super::super) async fn verify(
        original: &ProtectedMessage,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError> {
        let (published, received) = oneshot::channel();
        let original =
            registered(original, records, ProviderProfile::Scheduled).map(|(claims, logical)| {
                Receipt {
                    claims,
                    logical,
                    original_proof: original.proof.clone(),
                }
            });
        let transfer = published.send(original);
        match transfer {
            Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
            Err(original) => original,
        }
    }
    pub(in super::super) async fn protect_when_released(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
        release: oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError> {
        release.await.map_err(VerificationError::Unavailable)?;
        protect(bytes, request, records).await
    }
}

pub(super) fn metadata(profile: ProviderProfile, caller: String, receiver: String) -> Metadata {
    Metadata {
        wire_version: 1,
        schema_version: 1,
        provider_profile: profile.name().into(),
        deployment: "two-node-read-study".into(),
        caller: "caller".into(),
        caller_session: caller.into(),
        controller: "owner".into(),
        actor: "counter".into(),
        host: "receiver".into(),
        protocol: "counter".into(),
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

pub(super) fn logical_scope() -> Scope {
    Scope {
        identity: Identity {
            binding: Binding {
                deployment: "two-node-read-study",
                caller: "caller",
                controller: "owner",
                actor: "counter",
                host: "receiver",
                protocol: "counter",
                runtime_generation: 41,
                actor_generation: 0,
            },
            sequence: 23,
        },
        operation: NativeOperation::Read,
        grant_issuer: "issuer",
        authority_generation: 19,
    }
}
