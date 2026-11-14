//! Finite TEST deployment prototype; no shipping identity/shutdown certification.
use bombay::remote::{Metadata, json};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::fmt;
use std::rc::Rc;
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const CONFIGURATION_BYTES: u64 = 262_144;
const PROFILE: &str = "remote-counter-finite-pairs-v1";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Serve,
    Call,
}
/// An actual public binding projection, not an authorization token.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) role: Role,
    pub(crate) runtime: String,
    pub(crate) actor: u64,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Purpose {
    Request,
    Reply,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    Increment,
    Read,
    Stop,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfiguredRecord {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
// Only the header projection; complete domain DTO decoding remains separate.
#[derive(Deserialize)]
struct Claims {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    profile: String,
    revision: u64,
    caller: Binding,
    receiver: Binding,
    grants: Vec<Metadata>,
    records: Vec<ConfiguredRecord>,
}

// Exact pair plus its validated configured grant; no Deserialize or public fields.
struct Record {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
    grant: Rc<Metadata>,
}
/// Validated finite profile, complete paired slots and actual local binding.
/// No Deserialize, public fields or mutable projection can bypass construction.
pub(crate) struct Deployment {
    caller: Binding,
    records: BTreeMap<(Operation, Purpose), Record>,
}
/// Construction follows exact-pair validation; caller cannot manufacture claims.
pub(crate) struct Receipt {
    grant: Rc<Metadata>,
    operation: Operation,
}
impl fmt::Debug for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receipt").finish_non_exhaustive()
    }
}
impl Receipt {
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.grant
    }
    pub(crate) fn operation(&self) -> Operation {
        self.operation
    }
}

#[derive(Error)]
pub(crate) enum Refusal {
    #[error("test deployment input failed")]
    Io {
        #[source]
        cause: std::io::Error,
    },
    #[error("test deployment JSON was refused")]
    Json {
        #[source]
        cause: serde_json::Error,
    },
    #[error("test deployment framing was refused")]
    Shape {
        #[source]
        cause: json::Error,
    },
    #[error("one bounded test deployment configuration is required")]
    Configuration,
    #[error("test deployment does not match the actual local binding")]
    Binding,
    #[error("test deployment scope is not configured")]
    Scope,
    #[error("all six operation and purpose slots are required exactly once")]
    Slots,
    #[error("each operation requires its own request correlation")]
    Correlation,
    #[error("request and reply metadata must match for each operation")]
    Pairing,
    #[error("the exact protected bytes and proof are not configured")]
    Proof,
}
impl fmt::Debug for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Refusal")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl Configuration {
    fn validate(self, actual: &Binding, limits: &json::Limits) -> Result<Deployment, Refusal> {
        let local = match actual.role {
            Role::Serve => &self.receiver,
            Role::Call => &self.caller,
        };
        if local != actual || self.receiver.role != Role::Serve || self.caller.role != Role::Call {
            return Err(Refusal::Binding);
        }
        if self.profile != PROFILE
            || self.caller.runtime.is_empty()
            || self.receiver.runtime.is_empty()
        {
            return Err(Refusal::Configuration);
        }
        if self.grants.len() != 3 || self.records.len() != 6 {
            return Err(Refusal::Slots);
        }
        for grant in &self.grants {
            if grant.wire_version != 1
                || grant.schema_version != 1
                || grant.provider_profile.as_ref() != PROFILE
                || grant.caller_session.as_ref() != self.caller.runtime.as_str()
                || grant.runtime_generation.as_ref() != self.receiver.runtime.as_str()
                || grant.actor_generation != self.receiver.actor
                || grant.authority_generation != self.revision
                || [
                    &grant.deployment,
                    &grant.caller,
                    &grant.controller,
                    &grant.actor,
                    &grant.host,
                    &grant.protocol,
                    &grant.grant_issuer,
                ]
                .iter()
                .any(|name| name.is_empty())
            {
                return Err(Refusal::Scope);
            }
        }
        // Move each configured grant once; paired records/receipts share that
        // immutable authority instead of copying Metadata or keeping raw state.
        let grants = self.grants.into_iter().map(Rc::new).collect::<Vec<_>>();
        let mut records = BTreeMap::new();
        for record in self.records {
            json::check(&record.protected, &record.proof, limits)
                .map_err(|cause| Refusal::Shape { cause })?;
            let claims: Claims = serde_json::from_slice(&record.protected)
                .map_err(|cause| Refusal::Json { cause })?;
            let grant = grants
                .iter()
                .find(|grant| grant.as_ref() == &claims.bombay)
                .ok_or(Refusal::Scope)?;
            // Equality explicitly discharges the parsed header in favor of the
            // matching configured grant; original protected/proof bytes remain.
            match records.entry((claims.operation, claims.kind)) {
                Entry::Vacant(slot) => {
                    slot.insert(Record {
                        protected: record.protected,
                        proof: record.proof,
                        grant: Rc::clone(grant),
                    });
                }
                Entry::Occupied(_) => return Err(Refusal::Slots),
            }
        }
        let mut correlations = BTreeSet::new();
        for operation in [Operation::Increment, Operation::Read, Operation::Stop] {
            let request = records
                .get(&(operation, Purpose::Request))
                .ok_or(Refusal::Slots)?;
            let reply = records
                .get(&(operation, Purpose::Reply))
                .ok_or(Refusal::Slots)?;
            if request.grant != reply.grant {
                return Err(Refusal::Pairing);
            }
            if !correlations.insert(request.grant.request_sequence) {
                return Err(Refusal::Correlation);
            }
        }
        // Receiver runtime/address and authority revision remain in every
        // matched grant; role/profile checks are discharged by validation.
        Ok(Deployment {
            caller: self.caller,
            records,
        })
    }
}

impl Deployment {
    pub(crate) fn metadata(&self, operation: Operation) -> Result<&Metadata, Refusal> {
        self.records
            .get(&(operation, Purpose::Request))
            .map(|record| record.grant.as_ref())
            .ok_or(Refusal::Slots)
    }
    pub(crate) fn caller(&self) -> &Binding {
        &self.caller
    }
    pub(crate) fn protect(&self, purpose: Purpose, protected: &[u8]) -> Result<Box<[u8]>, Refusal> {
        let ((_, kind), known) = self
            .records
            .iter()
            .find(|(_, known)| known.protected.as_ref() == protected)
            .ok_or(Refusal::Proof)?;
        if *kind != purpose {
            return Err(Refusal::Scope);
        }
        Ok(known.proof.clone())
    }
    pub(crate) fn verify(
        &self,
        purpose: Purpose,
        protected: &[u8],
        proof: &[u8],
    ) -> Result<Receipt, Refusal> {
        let ((operation, kind), known) = self
            .records
            .iter()
            .find(|(_, known)| {
                known.protected.as_ref() == protected && known.proof.as_ref() == proof
            })
            .ok_or(Refusal::Proof)?;
        if *kind != purpose {
            return Err(Refusal::Scope);
        }
        Ok(Receipt {
            grant: Rc::clone(&known.grant),
            operation: *operation,
        })
    }
}

/// Read one bounded trusted assignment for the actual local binding.
/// The application supplies a cancellable Unix socket.
/// Rejected unverified input bytes are explicitly surrendered; `actual` stays outside.
pub(crate) async fn configure(
    actual: &Binding,
    input: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
    limits: &json::Limits,
) -> Result<Deployment, Refusal> {
    let mut binding = serde_json::to_vec(actual).map_err(|cause| Refusal::Json { cause })?;
    binding.push(b'\n');
    output
        .write_all(&binding)
        .await
        .map_err(|cause| Refusal::Io { cause })?;
    output
        .flush()
        .await
        .map_err(|cause| Refusal::Io { cause })?;
    let mut configuration = Vec::new();
    input
        .take(CONFIGURATION_BYTES + 1)
        .read_until(b'\n', &mut configuration)
        .await
        .map_err(|cause| Refusal::Io { cause })?;
    if configuration.is_empty()
        || configuration.len() as u64 > CONFIGURATION_BYTES
        || configuration.last() != Some(&b'\n')
    {
        return Err(Refusal::Configuration);
    }
    let configuration: Configuration =
        serde_json::from_slice(&configuration).map_err(|cause| Refusal::Json { cause })?;
    configuration.validate(actual, limits)
}
