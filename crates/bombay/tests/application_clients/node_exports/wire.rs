//! Concrete application messages and the two existing, borrowed proof families.
use super::super::super::counter::{
    CounterIncremented, CounterMessage, CounterProcessed, CounterValue,
};
use super::super::super::ledger::{LedgerCommand, Receipts};
use super::protocol::{
    Binding, ProofScope, ProtectedMessage, ProviderProfile, Purpose, metadata, recorded, scheduled,
};
use bombay::behavior::{EstablishedRecipient, User};
use bombay::prelude::ShutdownRejection;
use bombay::remote::{Attempt, Metadata};
use bombay::{
    ActorFailureAssessment, ActorRetirementReport, MailAddr, RetirementAssessment, TrySendError,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::time::Instant;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum CounterInput {
    Increment(u64),
    Read,
    Stop,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum LedgerInput {
    Append(Box<[u8]>),
    Read,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request<Input> {
    pub(super) bombay: Metadata,
    pub(super) kind: Purpose,
    pub(super) operation: Input,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reply<Claims, Input, Value> {
    bombay: Claims,
    kind: Purpose,
    operation: Input,
    value: Value,
}

#[derive(Debug)]
pub(super) enum Refusal {
    Scope,
    Capacity,
    Native,
    Json(serde_json::Error),
}

pub(super) fn binding(protocol: &'static str) -> Binding {
    Binding {
        deployment: "combined-node-consumer",
        caller: "caller",
        controller: "owner",
        actor: protocol,
        host: "receiver",
        protocol,
        target_assignment: protocol,
        logical_session: 251,
    }
}

pub(super) fn claims(
    profile: ProviderProfile,
    protocol: &'static str,
    caller: &str,
    receiver: &str,
    actor: MailAddr,
    sequence: u64,
) -> Metadata {
    let mut claims = metadata(profile, protocol, caller.to_owned(), receiver.to_owned());
    claims.deployment = binding(protocol).deployment.into();
    claims.actor_generation = actor.0;
    claims.request_sequence = sequence;
    claims
}

pub(super) fn register<Input: Serialize>(
    claims: Metadata,
    input: Input,
    records: &RefCell<Vec<ProtectedMessage>>,
) -> ProtectedMessage {
    let protected = serde_json::to_vec(&Request {
        bombay: claims,
        kind: Purpose::Request,
        operation: input,
    })
    .unwrap()
    .into_boxed_slice();
    let proof: Box<[u8]> = [181, 191].into();
    let original = ProtectedMessage { protected, proof };
    records.borrow_mut().push(ProtectedMessage {
        protected: original.protected.clone(),
        proof: original.proof.clone(),
    });
    original
}

pub(super) fn conflict(
    original: &ProtectedMessage,
    records: &RefCell<Vec<ProtectedMessage>>,
) -> ProtectedMessage {
    let mut protected = original.protected.to_vec();
    protected.push(b' ');
    let original = ProtectedMessage {
        protected: protected.into_boxed_slice(),
        proof: original.proof.clone(),
    };
    records.borrow_mut().push(ProtectedMessage {
        protected: original.protected.clone(),
        proof: original.proof.clone(),
    });
    original
}

pub(super) fn increment(
    input: CounterInput,
) -> Result<impl FnOnce(EstablishedRecipient<CounterProcessed>) -> CounterMessage, CounterInput> {
    match input {
        CounterInput::Increment(request_sequence) => {
            Ok(move |reply_to| CounterMessage::IncrementAndReply {
                request_sequence,
                reply_to,
            })
        }
        original => Err(original),
    }
}

pub(super) fn counter_read(
    input: CounterInput,
) -> Result<impl FnOnce(EstablishedRecipient<CounterValue>) -> CounterMessage, CounterInput> {
    match input {
        CounterInput::Read => Ok(CounterMessage::Read),
        original => Err(original),
    }
}

pub(super) fn counter_stop(input: CounterInput) -> Result<(), CounterInput> {
    match input {
        CounterInput::Stop => Ok(()),
        original => Err(original),
    }
}

pub(super) fn append(input: LedgerInput) -> Result<impl FnOnce() -> LedgerCommand, LedgerInput> {
    match input {
        LedgerInput::Append(original) => Ok(move || LedgerCommand::Append(original)),
        original => Err(original),
    }
}

pub(super) fn ledger_read(
    input: LedgerInput,
) -> Result<impl FnOnce(EstablishedRecipient<Receipts>) -> LedgerCommand, LedgerInput> {
    match input {
        LedgerInput::Read => Ok(LedgerCommand::Read),
        original => Err(original),
    }
}

pub(super) fn decode_counter(bytes: &[u8]) -> Result<CounterInput, Refusal> {
    let request: Request<CounterInput> = serde_json::from_slice(bytes).map_err(Refusal::Json)?;
    match request.kind {
        Purpose::Request => Ok(request.operation),
        _ => Err(Refusal::Scope),
    }
}

pub(super) fn decode_ledger(
    bytes: &[u8],
    allocation: &RefCell<Option<*const u8>>,
) -> Result<LedgerInput, Refusal> {
    let request: Request<LedgerInput> = serde_json::from_slice(bytes).map_err(Refusal::Json)?;
    if request.kind != Purpose::Request {
        return Err(Refusal::Scope);
    }
    if let LedgerInput::Append(original) = &request.operation {
        *allocation.borrow_mut() = Some(original.as_ptr());
    }
    Ok(request.operation)
}

pub(super) fn current<Input>(
    scope: &ProofScope<Input>,
    runtime: &str,
    target: MailAddr,
    expected: Binding,
    caller: &str,
    deadline: Instant,
) -> Result<(), Refusal> {
    let claims = &scope.bombay;
    if claims.wire_version != 1
        || claims.schema_version != 1
        || claims.deployment.as_ref() != expected.deployment
        || claims.caller.as_ref() != expected.caller
        || claims.controller.as_ref() != expected.controller
        || claims.actor.as_ref() != expected.actor
        || claims.host.as_ref() != expected.host
        || claims.protocol.as_ref() != expected.protocol
        || claims.runtime_generation.as_ref() != runtime
        || claims.actor_generation != target.0
        || claims.authority_generation != 19
        || scope.kind != Purpose::Request
        || claims.grant_issuer.as_ref() != "issuer"
        || claims.caller_session.as_ref() != caller
        || Instant::now() >= deadline
    {
        return Err(Refusal::Scope);
    }
    Ok(())
}

fn encode<Input: Serialize, Value: Serialize>(
    scope: &ProofScope<Input>,
    kind: Purpose,
    value: Value,
) -> Result<Box<[u8]>, Refusal> {
    serde_json::to_vec(&Reply {
        bombay: &scope.bombay,
        kind,
        operation: &scope.operation,
        value,
    })
    .map(Vec::into_boxed_slice)
    .map_err(Refusal::Json)
}

pub(super) fn processed(
    receipt: &recorded::Receipt<CounterInput>,
    original: &User<MailAddr, CounterIncremented>,
) -> Result<Box<[u8]>, Refusal> {
    let scope = recorded::projection(receipt);
    let CounterInput::Increment(sequence) = scope.operation else {
        return Err(Refusal::Scope);
    };
    if original.from.0 != scope.bombay.actor_generation
        || original.message.request_sequence != sequence
    {
        return Err(Refusal::Scope);
    }
    encode(scope, Purpose::Processed, sequence)
}

pub(super) fn value(
    receipt: &recorded::Receipt<CounterInput>,
    original: &User<MailAddr, u64>,
) -> Result<Box<[u8]>, Refusal> {
    let scope = recorded::projection(receipt);
    if scope.operation != CounterInput::Read || original.from.0 != scope.bombay.actor_generation {
        return Err(Refusal::Scope);
    }
    encode(scope, Purpose::ReadReply, original.message)
}

pub(super) fn stopped(
    receipt: &recorded::Receipt<CounterInput>,
    admission: &Attempt<Result<(), ShutdownRejection>>,
    report: &Option<ActorRetirementReport>,
) -> Result<Box<[u8]>, Refusal> {
    if !matches!(admission, Attempt::Returned(Ok(()))) {
        return Err(Refusal::Native);
    }
    let Some(report) = report else {
        return Err(Refusal::Native);
    };
    if report.retirement() != RetirementAssessment::Established
        || report.failures() != ActorFailureAssessment::NoFailuresFound
    {
        return Err(Refusal::Native);
    }
    encode(recorded::projection(receipt), Purpose::Stopped, "retired")
}

pub(super) fn appended(
    receipt: &scheduled::Receipt<LedgerInput>,
    admission: &Attempt<Result<(), TrySendError<LedgerCommand>>>,
) -> Result<Box<[u8]>, Refusal> {
    match admission {
        Attempt::Returned(Ok(())) => encode(
            scheduled::projection(receipt),
            Purpose::Admitted,
            "admitted",
        ),
        _ => Err(Refusal::Native),
    }
}

pub(super) fn receipts(
    receipt: &scheduled::Receipt<LedgerInput>,
    original: &User<MailAddr, Vec<MailAddr>>,
) -> Result<Box<[u8]>, Refusal> {
    let scope = scheduled::projection(receipt);
    if scope.operation != LedgerInput::Read || original.from.0 != scope.bombay.actor_generation {
        return Err(Refusal::Scope);
    }
    encode(
        scope,
        Purpose::ReadReply,
        original
            .message
            .iter()
            .map(|origin| origin.0)
            .collect::<Vec<_>>(),
    )
}

pub(super) fn protect<Input: Serialize>(
    bytes: &[u8],
    scope: &ProofScope<Input>,
    records: &RefCell<Vec<ProtectedMessage>>,
    maximum: NonZeroUsize,
) -> Result<Box<[u8]>, Refusal> {
    // A distinct decoded reply is checked against the actual request receipt. The
    // finite registry authenticates the entire original byte/proof pair, not a hash.
    let reply: Reply<Metadata, serde_json::Value, serde_json::Value> =
        serde_json::from_slice(bytes).map_err(Refusal::Json)?;
    if reply.bombay != scope.bombay
        || reply.kind == Purpose::Request
        || reply.operation != serde_json::to_value(&scope.operation).map_err(Refusal::Json)?
    {
        return Err(Refusal::Scope);
    }
    let mut records = records.borrow_mut();
    if records.len() >= maximum.get() {
        return Err(Refusal::Capacity);
    }
    let proof: Box<[u8]> = [193, 197].into();
    records.push(ProtectedMessage {
        protected: bytes.into(),
        proof: proof.clone(),
    });
    Ok(proof)
}
