//! Untrusted protected Counter syntax; provider verification owns authority.

use super::CounterRequest;
use json_event_parser::{JsonEvent, JsonSyntaxError, LowLevelJsonParser};
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use std::io::Cursor;
use std::num::NonZeroUsize;
use thiserror::Error;

/// Explicit acceptance limits. Token length is checked after materialization.
/// Parser states are a separate limit, not a synonym for logical nesting.
pub(super) struct Limits {
    pub(super) input_bytes: NonZeroUsize,
    pub(super) token_bytes: NonZeroUsize,
    pub(super) nesting: NonZeroUsize,
    pub(super) parser_states: NonZeroUsize,
    pub(super) identifier_bytes: NonZeroUsize,
    pub(super) proof_bytes: NonZeroUsize,
}

#[derive(Debug, Error)]
pub(super) enum DecodeError {
    #[error("protected input exceeds its byte limit")]
    InputBytes,
    #[error("proof exceeds its byte limit")]
    ProofBytes,
    #[error("protected input must be an object")]
    ObjectRequired,
    #[error("protected token exceeds its byte limit")]
    TokenBytes,
    #[error("protected input exceeds its logical nesting limit")]
    Nesting,
    #[error("malformed protected JSON")]
    Lexical(JsonSyntaxError),
    #[error("incomplete protected JSON")]
    Incomplete,
    #[error("malformed protected declarations")]
    Declaration(JsonError),
    #[error("unsupported wire/schema version")]
    Version {
        wire_version: u64,
        schema_version: u64,
    },
    #[error("unsupported protected meaning")]
    Kind(Box<str>),
    #[error("unsupported provider profile")]
    Profile(Box<str>),
    #[error("malformed protected body")]
    Body(JsonError),
    #[error("operation does not match protected meaning")]
    Operation(Box<str>),
    #[error("unsupported verified refusal reason")]
    Reason(Box<str>),
    #[error("unsupported retirement assessment")]
    Retirement(Box<str>),
    #[error("unsupported failure assessment")]
    Failures(Box<str>),
    #[error("protected identifier exceeds its byte limit")]
    Identifier { field: &'static str, bytes: usize },
}

#[derive(Debug)]
pub(super) enum Message {
    Request(RequestBody),
    Refused(Refused),
    MailboxAdmitted(MailboxAdmitted),
    IncrementProcessed(IncrementProcessed),
    ReadReply(ReadReply),
    StopAccepted(StopAccepted),
    RetirementReport(RetirementReport),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RequestBody {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
}

impl RequestBody {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "request" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "increment" | "read" | "stop") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Refused {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
    pub(super) reason: Box<str>,
}

impl Refused {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "refused" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "increment" | "read" | "stop") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        if !matches!(
            self.reason.as_ref(),
            "binding"
                | "grant"
                | "conflict"
                | "retention_full"
                | "revoked"
                | "expired"
                | "count"
                | "individual_bytes"
                | "total_bytes"
                | "accounting"
                | "exhausted"
        ) {
            return Err(DecodeError::Reason(self.reason));
        }
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MailboxAdmitted {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
}

impl MailboxAdmitted {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "mailbox_admitted" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "increment" | "read") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IncrementProcessed {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
}

impl IncrementProcessed {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "increment_processed" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "increment") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReadReply {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
    pub(super) value: u64,
}

impl ReadReply {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "read_reply" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "read") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StopAccepted {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
}

impl StopAccepted {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "stop_accepted" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "stop") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetirementReport {
    pub(super) wire_version: u64,
    pub(super) schema_version: u64,
    pub(super) provider_profile: Box<str>,
    pub(super) kind: Box<str>,
    pub(super) deployment: Box<str>,
    pub(super) caller: Box<str>,
    pub(super) controller: Box<str>,
    pub(super) actor: Box<str>,
    pub(super) host: Box<str>,
    pub(super) protocol: Box<str>,
    pub(super) grant_issuer: Box<str>,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
    pub(super) authority_generation: u64,
    pub(super) request_sequence: u64,
    pub(super) operation: Box<str>,
    pub(super) retirement: Box<str>,
    pub(super) failures: Box<str>,
}

impl RetirementReport {
    fn checked(self, limits: &Limits) -> Result<Self, DecodeError> {
        if self.kind.as_ref() != "retirement_report" {
            return Err(DecodeError::Kind(self.kind));
        }
        if !matches!(self.operation.as_ref(), "stop") {
            return Err(DecodeError::Operation(self.operation));
        }
        check_identifiers(
            [
                ("deployment", self.deployment.as_ref()),
                ("caller", self.caller.as_ref()),
                ("controller", self.controller.as_ref()),
                ("actor", self.actor.as_ref()),
                ("host", self.host.as_ref()),
                ("protocol", self.protocol.as_ref()),
                ("grant_issuer", self.grant_issuer.as_ref()),
            ],
            limits,
        )?;
        if !matches!(self.retirement.as_ref(), "established" | "not_established") {
            return Err(DecodeError::Retirement(self.retirement));
        }
        if !matches!(
            self.failures.as_ref(),
            "failures_found" | "no_failures_found" | "incomplete"
        ) {
            return Err(DecodeError::Failures(self.failures));
        }
        Ok(self)
    }
}

fn check_identifiers(
    identifiers: [(&'static str, &str); 7],
    limits: &Limits,
) -> Result<(), DecodeError> {
    for (field, identifier) in identifiers {
        if identifier.len() > limits.identifier_bytes.get() {
            return Err(DecodeError::Identifier {
                field,
                bytes: identifier.len(),
            });
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct Versions {
    wire_version: u64,
    schema_version: u64,
}
#[derive(Deserialize)]
struct Meaning {
    kind: Box<str>,
    provider_profile: Box<str>,
}

fn check_lexical(original: &[u8], limits: &Limits) -> Result<(), DecodeError> {
    if !original.trim_ascii_start().starts_with(b"{") {
        return Err(DecodeError::ObjectRequired);
    }
    // The crate tests > max before pushing; subtract one for the stated cap.
    let mut parser = LowLevelJsonParser::new().with_max_stack_size(limits.parser_states.get() - 1);
    let mut remaining = original;
    let mut depth = 0;
    loop {
        let next = parser.parse_next(remaining, true);
        remaining = &remaining[next.consumed_bytes..];
        match next.event {
            Some(Err(cause)) => return Err(DecodeError::Lexical(cause)),
            Some(Ok(JsonEvent::StartObject | JsonEvent::StartArray)) => {
                depth += 1;
                if depth > limits.nesting.get() {
                    return Err(DecodeError::Nesting);
                }
            }
            Some(Ok(JsonEvent::EndObject | JsonEvent::EndArray)) => depth -= 1,
            Some(Ok(
                JsonEvent::String(token) | JsonEvent::ObjectKey(token) | JsonEvent::Number(token),
            )) => {
                if token.len() > limits.token_bytes.get() {
                    return Err(DecodeError::TokenBytes);
                }
            }
            Some(Ok(JsonEvent::Eof)) => return Ok(()),
            Some(Ok(JsonEvent::Boolean(_) | JsonEvent::Null)) => {}
            None => return Err(DecodeError::Incomplete),
        }
    }
}

/// Borrow originals; every local refusal leaves their custody with the caller.
/// Success is an untrusted external representation, never verified authority.
pub(super) fn decode(original: &CounterRequest, limits: &Limits) -> Result<Message, DecodeError> {
    if original.protected.len() > limits.input_bytes.get() {
        return Err(DecodeError::InputBytes);
    }
    if original.proof.len() > limits.proof_bytes.get() {
        return Err(DecodeError::ProofBytes);
    }
    check_lexical(&original.protected, limits)?;
    let versions: Versions =
        serde_json::from_slice(&original.protected).map_err(DecodeError::Declaration)?;
    if (versions.wire_version, versions.schema_version) != (1, 1) {
        return Err(DecodeError::Version {
            wire_version: versions.wire_version,
            schema_version: versions.schema_version,
        });
    }
    let meaning: Meaning =
        serde_json::from_slice(&original.protected).map_err(DecodeError::Declaration)?;
    if !matches!(
        meaning.kind.as_ref(),
        "request"
            | "refused"
            | "mailbox_admitted"
            | "increment_processed"
            | "read_reply"
            | "stop_accepted"
            | "retirement_report"
    ) {
        return Err(DecodeError::Kind(meaning.kind));
    }
    if !matches!(
        meaning.provider_profile.as_ref(),
        "counter-record-test-v1" | "counter-scheduled-test-v1"
    ) {
        return Err(DecodeError::Profile(meaning.provider_profile));
    }
    match meaning.kind.as_ref() {
        "request" => serde_json::from_slice::<RequestBody>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::Request),
        "refused" => serde_json::from_slice::<Refused>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::Refused),
        "mailbox_admitted" => serde_json::from_slice::<MailboxAdmitted>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::MailboxAdmitted),
        "increment_processed" => serde_json::from_slice::<IncrementProcessed>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::IncrementProcessed),
        "read_reply" => serde_json::from_slice::<ReadReply>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::ReadReply),
        "stop_accepted" => serde_json::from_slice::<StopAccepted>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::StopAccepted),
        "retirement_report" => serde_json::from_slice::<RetirementReport>(&original.protected)
            .map_err(DecodeError::Body)?
            .checked(limits)
            .map(Message::RetirementReport),
        _ => Err(DecodeError::Kind(meaning.kind)),
    }
}

/// A returned length exposes only a fully written prefix. An error exposes none.
/// The concrete message remains borrowed even when `Cursor` returns `WriteZero`.
pub(super) fn encode(original: &Message, destination: &mut [u8]) -> Result<usize, JsonError> {
    let mut cursor = Cursor::new(destination);
    match original {
        Message::Request(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::Refused(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::MailboxAdmitted(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::IncrementProcessed(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::ReadReply(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::StopAccepted(body) => serde_json::to_writer(&mut cursor, body)?,
        Message::RetirementReport(body) => serde_json::to_writer(&mut cursor, body)?,
    }
    Ok(usize::try_from(cursor.position()).expect("Cursor position is within the supplied slice"))
}

#[cfg(test)]
mod tests;
