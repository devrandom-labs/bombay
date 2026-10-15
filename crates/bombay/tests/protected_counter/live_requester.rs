//! Live existing Requester consumes a verified, typed protected Counter Read reply.
//! Positive finite test providers and one configured logical binding only.
use super::{
    Binding, Counter, CounterMessage, CounterOperation as NativeOperation, CounterProofPurpose,
    CounterProtectedMeaning, CounterRequester, CounterRequesterMessage, CounterRequesterState,
    CounterRequests, CounterServiceInput, CounterValue, Identity, Permission, Scope,
    completed_actor,
};
use alloc_count::AllocSnapshot;
use bombay::ApplicationScope;
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Admission, ExportError, ExportLimits, Metadata, json, try_retain, verify};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use core::ops::AsyncFn;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::future::{Future, Ready, poll_fn, ready};
use std::mem::size_of;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh_config::Config;

#[derive(Debug)]
struct ProtectedMessage {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Purpose {
    Request,
    ReadReply,
}
#[derive(Clone, Copy)]
enum ProviderProfile {
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
struct ReadRequest {
    bombay: Metadata,
    kind: Purpose,
    operation: CounterOperation,
}
#[derive(Serialize)]
struct ReadRequestRef<'a> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: CounterOperation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply {
    bombay: Metadata,
    kind: Purpose,
    operation: CounterOperation,
    value: u64,
}
#[derive(Serialize)]
struct ReadReplyRef<'a> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: CounterOperation,
    value: &'a u64,
}
// Provider scope ignores protocol-owned value fields; concrete DTO decoding above
// is independent. Matching the entire registered byte/proof pair remains mandatory.
#[derive(Deserialize)]
struct ProofScope {
    bombay: Metadata,
    kind: Purpose,
    operation: CounterOperation,
}
#[derive(Debug)]
enum VerificationError {
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
mod recorded {
    use super::*;
    pub(super) struct Receipt {
        claims: ProofScope,
        logical: Scope,
    }
    pub(super) fn projection(receipt: &Receipt) -> (&ProofScope, Scope) {
        (&receipt.claims, receipt.logical)
    }
    pub(super) fn protect_request(
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
    pub(super) fn protect(
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
    pub(super) fn verify(
        original: &ProtectedMessage,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Ready<Result<Receipt, VerificationError>> {
        ready(
            registered(original, records, ProviderProfile::Recorded)
                .map(|(claims, logical)| Receipt { claims, logical }),
        )
    }
    pub(super) async fn protect_when_released(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
        release: oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError> {
        release.await.map_err(VerificationError::Unavailable)?;
        protect(bytes, request, records).await
    }
}
mod scheduled {
    use super::*;
    pub(super) struct Receipt {
        claims: ProofScope,
        logical: Scope,
        original_proof: Box<[u8]>,
    }
    pub(super) fn projection(receipt: &Receipt) -> (&ProofScope, Scope) {
        (&receipt.claims, receipt.logical)
    }
    pub(super) fn proof(receipt: &Receipt) -> &[u8] {
        &receipt.original_proof
    }
    pub(super) async fn protect_request(
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
    pub(super) async fn protect(
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
    pub(super) async fn verify(
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
    pub(super) async fn protect_when_released(
        bytes: &[u8],
        request: &Receipt,
        records: &RefCell<Vec<(Scope, ProtectedMessage)>>,
        release: oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError> {
        release.await.map_err(VerificationError::Unavailable)?;
        protect(bytes, request, records).await
    }
}

fn configuration(address: &str, role: &EndpointRole) -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certificates");
    let (listen, connect, certificate, key, certificate_owner, key_owner) = match role {
        EndpointRole::Receiver => (
            vec![address],
            vec![],
            "listen_certificate",
            "listen_private_key",
            "recipient-certificate.pem",
            "recipient-private-key.pem",
        ),
        EndpointRole::Caller => (
            vec![],
            vec![address],
            "connect_certificate",
            "connect_private_key",
            "caller-certificate.pem",
            "caller-private-key.pem",
        ),
    };
    let mut tls = serde_json::json!({
        "root_ca_certificate": certificates.join("root-ca.pem"),
        "enable_mtls": true, "verify_name_on_connect": true, "use_public_pki": false
    });
    tls[certificate] = serde_json::json!(certificates.join(certificate_owner));
    tls[key] = serde_json::json!(certificates.join(key_owner));
    let declaration = serde_json::json!({
        "mode": "peer", "queries_max_retained": 2,
        "listen": {"endpoints": listen},
        "connect": {"endpoints": connect, "timeout_ms": 2000, "exit_on_failure": true},
        "scouting": {"multicast": {"enabled": false, "listen": false}, "gossip": {"enabled": false}},
        "transport": {"link": {"protocols": ["tls"], "tls": tls}}
    }).to_string();
    Config::from_deserializer(&mut serde_json::Deserializer::from_str(&declaration)).unwrap()
}
enum EndpointRole {
    Caller,
    Receiver,
}
fn reserved_address() -> String {
    let reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    format!("tls/localhost:{}", address.port())
}
fn metadata(profile: ProviderProfile, caller: String, receiver: String) -> Metadata {
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
fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(64).unwrap(),
        token_bytes: NonZeroUsize::new(128).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    }
}
fn current_scope(
    scope: &ProofScope,
    current: &Metadata,
    operation: &CounterOperation,
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

fn logical_scope() -> Scope {
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

#[expect(
    clippy::too_many_lines,
    reason = "one causal trace preserves actual Actions, protected originals, both Nodes and complete native custody through the final requester-state oracle"
)]
async fn live_read<Receipt>(
    profile: ProviderProfile,
    request_protector: impl AsyncFn(
        &[u8],
        &Metadata,
        Scope,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError>,
    protector: impl AsyncFn(
        &[u8],
        &Receipt,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
    projection: fn(&Receipt) -> (&ProofScope, Scope),
    evidence: fn(&Receipt) -> Option<&[u8]>,
) {
    let address = reserved_address();
    let mut receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("explicit receiver Node startup"));
    let (execution, receiving) = receiver.execute_with(|receiver, receiver_session| async move {
        let target = receiver.root().established_recipient();
        let target_address = receiver.root().address();
        let mut counter = receiver.export_client::<_, CounterValue>(target.clone())
            .unwrap_or_else(|_| panic!("certified actual receiving root"));
        let primed = counter.try_send(CounterMessage::Increment);
        let scope = logical_scope();
        let application = ApplicationScope::new();
        let mut requests = application.external::<CounterRequests>().unwrap();
        let legacy_values = application.external::<CounterValue>().unwrap();
        let requester = CounterRequester { service: requests.recipient(), counter: target,
            replies: legacy_values.recipient(), state: CounterRequesterState::Ready };
        let capacity = Arc::new(Semaphore::new(2));
        let (incoming, mut queries) = mpsc::channel(2);
        let publications = Arc::new(Mutex::new(Vec::new()));
        let recorded_publications = Arc::clone(&publications);
        let queryable = receiver_session.declare_queryable("bombay/live/read")
            .callback(move |query| {
                let original = try_retain(&incoming, &capacity, query);
                recorded_publications.lock().unwrap().push(original);
            }).await.unwrap();
        let mut caller = application.with_root(requester.stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Caller)).await
            .unwrap_or_else(|_| panic!("explicit caller Node startup"));
        let service_requests = &mut requests;
        let query_receiving = &mut queries;
        let (caller_execution, caller_receiving) = caller.execute_with(|caller, caller_session| async move {
            let requester = caller.root().established_recipient();
            let requester_address = caller.root().address();
            let command = caller.export_client::<_, CounterRequests>(requester)
                .unwrap_or_else(|_| panic!("certified actual live requester"));
            let caller_id = caller_session.zid();
            let receiver_id = receiver_session.zid();
            let current = metadata(profile, caller_id.to_string(), receiver_id.to_string());
            let deadline = Instant::now() + Duration::from_secs(30);
            let permission = Permission::Granted;
            // Explicit finite provider assignment: the legacy logical Scope and
            // complete SDK declarations coexist. No cast/hash maps ZID into Scope,
            // and transport identity is not the logical binding/replay namespace.
            let records = RefCell::new(Vec::with_capacity(2));
            let initial_records = records.borrow().len();
            let protection_calls = Cell::new(0);
            let begun = command.try_send(CounterRequesterMessage::BeginProtected { correlation: 23, scope });
            let (replies, mut receiving_reply) = mpsc::channel(2);
            let reply_capacity = Arc::new(Semaphore::new(2));
            let reply_publications = Arc::new(Mutex::new(Vec::new()));
            let callback_replies = Arc::clone(&reply_publications);
            // A driver timeout retains its actual cause and earns no semantic
            // credit. No native Query is issued until the real Actions trigger.
            let trigger = if begun.is_ok() { Some(timeout(Duration::from_secs(3), service_requests.receive()).await) }
                else { None };
            let mut original = None;
            let mut original_allocations = None;
            if matches!(&trigger, Some(Ok(Some(User { message: CounterServiceInput::Verify(23), .. })))) {
                let request_bytes = serde_json::to_vec(&ReadRequestRef { bombay: &current,
                    kind: Purpose::Request, operation: CounterOperation::Read }).unwrap().into_boxed_slice();
                let mut protection = None;
                verify(&request_bytes, &mut protection, |bytes| {
                    protection_calls.set(protection_calls.get() + 1);
                    request_protector(bytes, &current, scope, &records)
                }).await;
                original = Some(match protection.unwrap() {
                    Ok(proof) => {
                        let message = ProtectedMessage { protected: request_bytes, proof };
                        original_allocations = Some((message.protected.as_ptr(), message.proof.as_ptr()));
                        Ok(message)
                    }
                    Err(cause) => Err((request_bytes, cause)),
                });
            }
            let mut wire_reply = None;
            let issued = if let Some(Ok(original)) = &original {
                Some(caller_session.get("bombay/live/read").consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(3)).payload(original.protected.to_vec()).attachment(original.proof.to_vec())
                    .callback(move |reply| {
                        let retained = try_retain(&replies, &reply_capacity, reply);
                        callback_replies.lock().unwrap().push(retained);
                    }).await)
            } else { None };
            let query = if matches!(&issued, Some(Ok(()))) { tokio::select! {
                query = query_receiving.recv() => query,
                reply = receiving_reply.recv(), if wire_reply.is_none() => { wire_reply = reply; None }
            }} else { None };
            let mut received_request = None;
            let mut request_shape = None;
            let mut request_declaration = None;
            let mut request_verification = None;
            let mut request_scope = None;
            let mut admitted = None;
            let mut native_reply = None;
            let mut reply_publication = None;
            let mut produced_reply = None;
            let mut protection = None;
            if let (Some(Ok(Some(User { message: CounterServiceInput::Verify(23), .. }))), Some((query, _))) = (&trigger, &query) {
                if let (Some(bytes), Some(proof)) = (query.payload(), query.attachment()) {
                    received_request = Some(ProtectedMessage { protected: bytes.to_bytes().into_owned().into_boxed_slice(),
                        proof: proof.to_bytes().into_owned().into_boxed_slice() });
                    let received = received_request.as_ref().unwrap();
                    request_shape = Some(json::check(&received.protected, &received.proof, &limits()));
                    if matches!(&request_shape, Some(Ok(()))) {
                        request_declaration = Some(serde_json::from_slice::<ReadRequest>(&received.protected));
                        verify(received, &mut request_verification, |r| provider(r, &records)).await;
                    }
                    if let (Some(Ok(declared)), Some(Ok(receipt))) = (&request_declaration, &request_verification) {
                        let (claims, authorized_logical) = projection(receipt);
                        request_scope = Some(current_scope(claims, &current, &CounterOperation::Read, Purpose::Request, deadline));
                        if matches!(&request_scope, Some(Ok(()))) && declared.bombay == current && declared.kind == Purpose::Request
                            && declared.operation == CounterOperation::Read && matches!(permission, Permission::Granted)
                            && authorized_logical == scope && scope.identity.sequence == current.request_sequence && scope.operation == NativeOperation::Read {
                            admitted = Some(counter.try_send(CounterMessage::Read(counter.recipient())));
                            if matches!(&admitted, Some(Ok(()))) {
                                native_reply = tokio::select! {
                                    result = counter.receive() => result,
                                    reply = receiving_reply.recv() => { wire_reply = reply; None }
                                };
                            }
                            if let Some(reply) = &native_reply {
                                let bytes = serde_json::to_vec(&ReadReplyRef { bombay: &current, kind: Purpose::ReadReply,
                                    operation: CounterOperation::Read, value: &reply.message }).unwrap();
                                produced_reply = Some(bytes);
                                let inputs = (produced_reply.as_ref().unwrap().as_slice(), receipt, &records);
                                verify(&inputs, &mut protection, |(bytes, request, records)| protector(bytes, request, records)).await;
                                if let Some(Ok(proof)) = &protection {
                                    reply_publication = Some(query.reply("bombay/live/read", produced_reply.as_ref().unwrap().as_slice()).attachment(proof.as_ref()).await);
                                }
                            }
                        }
                    }
                }
                if reply_publication.is_none() { reply_publication = Some(query.reply_err("protected read refused").await); }
            }
            if matches!(&issued, Some(Ok(()))) && wire_reply.is_none() { wire_reply = receiving_reply.recv().await; }
            let mut received_reply = None;
            let mut reply_shape = None;
            let mut reply_declaration = None;
            let mut reply_verification = None;
            let mut reply_scope = None;
            let mut delivered = None;
            if let Some((reply, _)) = &wire_reply
                && let Ok(sample) = reply.result() {
                    received_reply = Some(ProtectedMessage { protected: sample.payload().to_bytes().into_owned().into_boxed_slice(),
                        proof: sample.attachment().map(|p| p.to_bytes().into_owned().into_boxed_slice()).unwrap_or_default() });
                    let received = received_reply.as_ref().unwrap();
                    reply_shape = Some(json::check(&received.protected, &received.proof, &limits()));
                    if matches!(&reply_shape, Some(Ok(()))) {
                        reply_declaration = Some(serde_json::from_slice::<ReadReply>(&received.protected));
                        verify(received, &mut reply_verification, |r| provider(r, &records)).await;
                    }
                    if let (Some(Ok(declared)), Some(Ok(receipt))) = (&reply_declaration, &reply_verification) {
                        let (claims, authorized_logical) = projection(receipt);
                        reply_scope = Some(current_scope(claims, &current, &CounterOperation::Read, Purpose::ReadReply, deadline));
                        if matches!(&reply_scope, Some(Ok(()))) && declared.bombay == current && declared.kind == Purpose::ReadReply
                            && declared.operation == CounterOperation::Read && matches!(permission, Permission::Granted) && authorized_logical == scope {
                            delivered = Some(command.try_send(CounterRequesterMessage::ProtectedReply { correlation: 23, scope,
                                meaning: CounterProtectedMeaning::ReadReply(declared.value) }));
                        }
                    }
            }
            let mut additional_reply = None;
            let acknowledged = if matches!(&delivered, Some(Ok(()))) { tokio::select! {
                acknowledged = service_requests.receive() => acknowledged,
                reply = receiving_reply.recv() => { additional_reply = reply; None }
            }} else { None };
            let stopped = caller.request_shutdown();
            let report = caller.lifecycle().retirement().await;
            (current, original, original_allocations, initial_records, protection_calls.into_inner(), begun, trigger, issued, query,
                received_request, request_shape, request_declaration, request_verification, request_scope,
                admitted, native_reply, produced_reply, protection, reply_publication, wire_reply, received_reply, reply_shape,
                reply_declaration, reply_verification, reply_scope, delivered, acknowledged, additional_reply,
                reply_publications, stopped, report, command, requester_address, caller_id, receiver_id)
        }).unwrap_or_else(|_| panic!("caller Work host entry"));
        let ((), (outcome, caller_native, caller_notifications)) = tokio::join!(caller_execution, caller_receiving);
        let (read, caller_cleanup) = outcome.try_into_completed().unwrap_or_else(|_| panic!("original caller Work"));
        let caller_closed = caller.close().await;
        let undeclared = queryable.undeclare().await;
        let mut remaining_queries = Vec::new();
        while let Ok(original) = queries.try_recv() { remaining_queries.push(original); }
        let stopped = receiver.request_shutdown();
        let report = receiver.lifecycle().retirement().await;
        (read, caller_native, caller_notifications, caller_cleanup, caller_closed, undeclared,
            remaining_queries, requests, legacy_values, publications, primed, stopped, report, target_address, scope)
    }).unwrap_or_else(|_| panic!("receiver Work host entry"));
    let ((), (outcome, receiver_native, receiver_notifications)) =
        tokio::join!(execution, receiving);
    let (output, receiver_cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original receiver Work"));
    let receiver_closed = receiver.close().await;
    let (
        read,
        caller_native,
        caller_notifications,
        caller_cleanup,
        caller_closed,
        undeclared,
        remaining_queries,
        requests,
        legacy_values,
        publications,
        primed,
        receiver_stopped,
        receiver_report,
        target_address,
        configured_scope,
    ) = output;
    let (
        current,
        original,
        original_allocations,
        initial_records,
        protection_calls,
        begun,
        trigger,
        issued,
        query,
        received_request,
        request_shape,
        request_declaration,
        request_verification,
        request_scope,
        admitted,
        native_reply,
        produced_reply,
        protection,
        reply_publication,
        wire_reply,
        received_reply,
        reply_shape,
        reply_declaration,
        reply_verification,
        reply_scope,
        delivered,
        acknowledged,
        additional_reply,
        reply_publications,
        caller_stopped,
        caller_report,
        command,
        requester_address,
        caller_id,
        receiver_id,
    ) = read;
    // All actual Work/native/notification/Queryable/Session cleanup is acquired
    // before observing the domain outcome. Original Query/Reply tuples remain here.
    assert!(receiver_cleanup.is_ok() && caller_cleanup.is_ok());
    assert!(receiver_closed.is_ok() && caller_closed.is_ok() && undeclared.is_ok());
    for notifications in [&caller_notifications, &receiver_notifications] {
        let notifications = notifications.as_ref().unwrap();
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    }
    for report in [caller_report, receiver_report] {
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
    let (origin, retirement) = caller_native.unwrap();
    let requester = completed_actor(origin, retirement).into_inner();
    let (origin, retirement) = receiver_native.unwrap();
    let receiver_actor = completed_actor(origin, retirement);
    // This delivery oracle remains finite when the obsolete constant proof is
    // substituted: peer refusal sends a native error, and no ACK is awaited.
    assert!(
        matches!(&delivered, Some(Ok(()))),
        "actor-issued request protection must enable checked reply delivery"
    );
    assert_eq!(initial_records, 0);
    assert_eq!(protection_calls, 1);
    let original = original.as_ref().unwrap().as_ref().unwrap();
    let original_allocations = original_allocations.unwrap();
    assert!(primed.is_ok() && begun.is_ok() && caller_stopped.is_ok() && receiver_stopped.is_ok());
    assert!(matches!(&issued, Some(Ok(()))));
    assert_eq!(target_address, MailAddr::APPLICATION_ROOT);
    assert_eq!(requester_address, MailAddr::APPLICATION_ROOT);
    let caller_id_text = caller_id.to_string();
    let receiver_id_text = receiver_id.to_string();
    assert_ne!(caller_id, receiver_id);
    assert_eq!(current.caller_session.as_ref(), caller_id_text);
    assert_eq!(current.runtime_generation.as_ref(), receiver_id_text);
    let binding = configured_scope.identity.binding;
    assert_eq!(current.deployment.as_ref(), binding.deployment);
    assert_eq!(current.caller.as_ref(), binding.caller);
    assert_eq!(current.controller.as_ref(), binding.controller);
    assert_eq!(current.actor.as_ref(), binding.actor);
    assert_eq!(current.host.as_ref(), binding.host);
    assert_eq!(current.protocol.as_ref(), binding.protocol);
    assert_eq!(current.grant_issuer.as_ref(), configured_scope.grant_issuer);
    assert_eq!(
        current.authority_generation,
        configured_scope.authority_generation
    );
    assert_eq!(current.request_sequence, configured_scope.identity.sequence);
    assert_eq!(original.protected.as_ptr(), original_allocations.0);
    assert_eq!(original.proof.as_ptr(), original_allocations.1);
    let trigger = trigger
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(trigger.from, requester_address);
    assert!(matches!(trigger.message, CounterServiceInput::Verify(23)));
    assert!(remaining_queries.is_empty());
    let publications = publications.lock().unwrap();
    assert_eq!(publications.len(), 1);
    for original in publications.iter() {
        assert!(original.is_ok());
    }
    let reply_publications = reply_publications.lock().unwrap();
    assert_eq!(reply_publications.len(), 1);
    for original in reply_publications.iter() {
        assert!(original.is_ok());
    }
    let (query, _query_permit) = query.as_ref().unwrap();
    let native_request = query.payload().unwrap().to_bytes();
    let native_proof = query.attachment().unwrap().to_bytes();
    assert_eq!(native_request.as_ref(), original.protected.as_ref());
    assert_eq!(native_proof.as_ref(), original.proof.as_ref());
    let received = received_request.as_ref().unwrap();
    assert_eq!(received.protected.as_ref(), original.protected.as_ref());
    assert_eq!(received.proof.as_ref(), original.proof.as_ref());
    assert!(matches!(&request_shape, Some(Ok(()))));
    assert!(matches!(&request_scope, Some(Ok(()))));
    let declared = request_declaration.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(declared.bombay, current);
    assert_eq!(declared.kind, Purpose::Request);
    assert_eq!(declared.operation, CounterOperation::Read);
    assert_receipt(request_verification.as_ref(), evidence, &[7, 11]);
    assert!(matches!(&admitted, Some(Ok(()))));
    let native = native_reply.as_ref().unwrap();
    assert_eq!(native.from, target_address);
    assert_eq!(native.message, 1);
    assert!(matches!(&reply_publication, Some(Ok(()))));
    let (wire, _reply_permit) = wire_reply.as_ref().unwrap();
    let sample = wire.result().unwrap();
    assert_eq!(sample.key_expr().as_str(), "bombay/live/read");
    let raw_reply = received_reply.as_ref().unwrap();
    assert_eq!(
        produced_reply.as_ref().unwrap().as_slice(),
        raw_reply.protected.as_ref()
    );
    match &protection {
        Some(Ok(proof)) => assert_eq!(proof.as_ref(), raw_reply.proof.as_ref()),
        _ => panic!("actual reply proof must be retained after producer completion"),
    }
    let native_reply_bytes = sample.payload().to_bytes();
    let native_reply_proof = sample.attachment().unwrap().to_bytes();
    assert_eq!(raw_reply.protected.as_ref(), native_reply_bytes.as_ref());
    assert_eq!(raw_reply.proof.as_ref(), native_reply_proof.as_ref());
    assert!(matches!(&reply_shape, Some(Ok(()))));
    assert!(matches!(&reply_scope, Some(Ok(()))));
    let declared = reply_declaration.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(declared.bombay, current);
    assert_eq!(declared.kind, Purpose::ReadReply);
    assert_eq!(declared.operation, CounterOperation::Read);
    assert_eq!(declared.value, native.message);
    assert_receipt(reply_verification.as_ref(), evidence, &[13, 17]);
    assert!(matches!(&delivered, Some(Ok(()))));
    assert!(additional_reply.is_none());
    let acknowledged = acknowledged.as_ref().unwrap();
    assert_eq!(acknowledged.from, requester_address);
    assert!(matches!(
        acknowledged.message,
        CounterServiceInput::ProtectedConsumed {
            correlation: 23,
            purpose: CounterProofPurpose::ReadReply,
        }
    ));
    let CounterRequesterState::Protected {
        correlation,
        scope,
        receipts,
    } = &requester.state
    else {
        panic!("the genuine requester retains its protected state");
    };
    assert_eq!(*correlation, 23);
    assert_eq!(*scope, configured_scope);
    // Independent final actor state is the forwarding-law oracle, not a label
    // placed by the service after publication. The actor also emitted the fence.
    assert_eq!(receipts.as_slice(), [CounterProtectedMeaning::ReadReply(1)]);
    drop((command, requests, legacy_values, receiver_actor, requester));
}

fn assert_receipt<Receipt>(
    result: Option<&Result<Receipt, VerificationError>>,
    evidence: fn(&Receipt) -> Option<&[u8]>,
    expected_proof: &[u8],
) {
    match result {
        Some(Ok(receipt)) => {
            if let Some(proof) = evidence(receipt) {
                assert_eq!(proof, expected_proof);
            }
        }
        Some(Err(VerificationError::Declaration(original))) => {
            panic!("original declaration cause: {original}")
        }
        Some(Err(VerificationError::Unavailable(original))) => {
            panic!("original publication cause: {original}")
        }
        Some(Err(VerificationError::ProofCapacity)) => {
            panic!("finite proof record capacity exhausted")
        }
        Some(Err(VerificationError::Invalid)) => panic!("registered bytes/profile refused"),
        None => panic!("the original verification result was not acquired"),
    }
}

async fn request_protection_refusals(
    profile: ProviderProfile,
    protect: impl AsyncFn(
        &[u8],
        &Metadata,
        Scope,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
) {
    let configured = metadata(profile, "caller-session".into(), "receiver-session".into());
    let scope = logical_scope();
    let records = RefCell::new(Vec::with_capacity(2));
    let mut caller = metadata(profile, "caller-session".into(), "receiver-session".into());
    caller.caller = "foreign-caller".into();
    let mut controller = metadata(profile, "caller-session".into(), "receiver-session".into());
    controller.controller = "foreign-controller".into();
    let mut wrong_profile = metadata(profile, "caller-session".into(), "receiver-session".into());
    wrong_profile.provider_profile = "unconfigured-provider".into();
    let mut foreign_scope = scope;
    foreign_scope.identity.binding.actor = "foreign-actor";
    let mut other_generation = scope;
    other_generation.identity.binding.actor_generation += 1;
    let mut refusals = Vec::new();
    for (claims, trusted, purpose, expected_scope) in [
        (&caller, &configured, Purpose::Request, scope),
        (&controller, &configured, Purpose::Request, scope),
        (&configured, &configured, Purpose::ReadReply, scope),
        (&wrong_profile, &wrong_profile, Purpose::Request, scope),
        (&configured, &configured, Purpose::Request, foreign_scope),
        (&configured, &configured, Purpose::Request, other_generation),
    ] {
        let bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: claims,
            kind: purpose,
            operation: CounterOperation::Read,
        })
        .unwrap();
        let mut result = None;
        verify(&bytes, &mut result, |bytes| {
            protect(bytes, trusted, expected_scope, &records)
        })
        .await;
        refusals.push((bytes, result, records.borrow().len()));
    }
    assert_eq!(refusals.len(), 6);
    for (bytes, result, retained) in &refusals {
        assert!(!bytes.is_empty());
        assert!(matches!(result, Some(Err(VerificationError::Invalid))));
        assert_eq!(
            *retained, 0,
            "refused declarations cannot register proof authority"
        );
    }
    assert!(records.borrow().is_empty());
}

#[test]
fn recorded_outbound_request_protection_refuses_untrusted_declarations() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(request_protection_refusals(
            ProviderProfile::Recorded,
            recorded::protect_request,
        ));
}

#[test]
fn scheduled_outbound_request_protection_refuses_untrusted_declarations() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(request_protection_refusals(
            ProviderProfile::Scheduled,
            scheduled::protect_request,
        ));
}

#[test]
fn recorded_live_requester_stores_checked_read_reply() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(live_read(
            ProviderProfile::Recorded,
            recorded::protect_request,
            recorded::verify,
            recorded::protect,
            recorded::projection,
            |_| None,
        ));
}
#[test]
fn scheduled_live_requester_stores_checked_read_reply() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(live_read(
            ProviderProfile::Scheduled,
            scheduled::protect_request,
            scheduled::verify,
            scheduled::protect,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}

// Local ownership comparison only: no Query traffic, protected publication,
// production identity or finished remote Application interface is exercised.
#[expect(
    clippy::too_many_lines,
    reason = "retain both concrete receipts, native rows and all cleanup before the ownership assertions"
)]
async fn pending_protection<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError>,
    protector: impl AsyncFn(
        &[u8],
        &Receipt,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
        oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError>,
    projection: fn(&Receipt) -> (&ProofScope, Scope),
    evidence: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Rc<Receipt>, &Rc<Receipt>) -> bool,
) {
    let address = reserved_address();
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("explicit local comparison Node"));
    let refusals = Arc::new(Mutex::new(Vec::new()));
    let recorded_refusals = Arc::clone(&refusals);
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let current = metadata(
                profile,
                session.zid().to_string(),
                session.zid().to_string(),
            );
            let mut second = metadata(
                profile,
                session.zid().to_string(),
                session.zid().to_string(),
            );
            second.request_sequence = 24;
            let first_scope = logical_scope();
            let mut second_scope = first_scope;
            second_scope.identity.sequence = 24;
            let first_bytes = serde_json::to_vec(&ReadRequestRef {
                bombay: &current,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap();
            let second_bytes = serde_json::to_vec(&ReadRequestRef {
                bombay: &second,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap();
            // Each configured table permits one request and its protected reply.
            // Separate finite tables authenticate a newer authority declaration
            // for the same identity, and the explicit conflicting spelling.
            let first_records = RefCell::new(vec![(
                first_scope,
                ProtectedMessage {
                    protected: first_bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            )]);
            let second_records = RefCell::new(vec![(
                second_scope,
                ProtectedMessage {
                    protected: second_bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            )]);
            let mut duplicate_scope = first_scope;
            duplicate_scope.authority_generation = 20;
            let duplicate_records = RefCell::new(vec![(
                duplicate_scope,
                ProtectedMessage {
                    protected: first_bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            )]);
            let mut conflict_bytes = first_bytes.clone();
            conflict_bytes.push(b' ');
            let conflict_records = RefCell::new(vec![(
                first_scope,
                ProtectedMessage {
                    protected: conflict_bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            )]);
            let mut verification = Vec::new();
            for (bytes, records) in [
                (&first_bytes, &first_records),
                (&second_bytes, &second_records),
                (&first_bytes, &duplicate_records),
                (&conflict_bytes, &conflict_records),
            ] {
                let original = ProtectedMessage {
                    protected: bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                };
                let allocations = (original.protected.as_ptr(), original.proof.as_ptr());
                let mut receipt = None;
                verify(&original, &mut receipt, |request| {
                    provider(request, records)
                })
                .await;
                let (receipt, sharing) = match receipt {
                    Some(Ok(receipt)) => {
                        let before = AllocSnapshot::now();
                        let receipt = Rc::new(receipt);
                        (receipt, before.elapsed())
                    }
                    _ => panic!("explicit registered provider evidence"),
                };
                verification.push((original, receipt, allocations, sharing));
            }
            let mut export = application
                .export::<_, CounterValue, Rc<Receipt>>(
                    application.root().established_recipient(),
                    "bombay/protection/read",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| recorded_refusals.lock().unwrap().push(original),
                )
                .await
                .unwrap_or_else(|_| panic!("actual Node-associated export"));
            let mut ordinary = application
                .export_client::<_, CounterValue>(application.root().established_recipient())
                .unwrap_or_else(|_| panic!("actual ordinary Counter caller"));
            let primed = ordinary.try_send(CounterMessage::Increment);
            let (original, receipt, first_allocations, first_sharing) = verification.remove(0);
            let receipt_allocation = Rc::as_ptr(&receipt);
            let claim_allocation = projection(&receipt).0.bombay.caller.as_ptr();
            let proof_allocation = evidence(&receipt).map(|proof| proof.as_ptr());
            let first = export
                .retain(original.protected, original.proof, receipt)
                .unwrap_or_else(|_| panic!("first retained request"));
            let deadline = Instant::now() + Duration::from_secs(30);
            let admitted_first = export
                .attempt(
                    &first.0,
                    |receipt| {
                        current_scope(
                            projection(receipt).0,
                            &current,
                            &CounterOperation::Read,
                            Purpose::Request,
                            deadline,
                        )?;
                        Ok::<_, VerificationError>(&current)
                    },
                    CounterMessage::Read,
                )
                .map(|_| ());
            let received_first = export.receive_native(&first.0).await;
            let request = export.request(&first.0).unwrap();
            let before = AllocSnapshot::now();
            let consumer = Rc::clone(request.2);
            let reader_sharing = before.elapsed();
            let same_receipt = Rc::as_ptr(&consumer);
            let references = Rc::strong_count(&consumer);
            let native_first = request.4.as_ref().and_then(Option::as_ref).unwrap();
            let reply_bytes = serde_json::to_vec(&ReadReplyRef {
                bombay: &current,
                kind: Purpose::ReadReply,
                operation: CounterOperation::Read,
                value: &native_first.message,
            })
            .unwrap();
            let calls = Cell::new(0);
            let (release, released) = oneshot::channel();
            let mut protection = None;
            let inputs = (reply_bytes.as_slice(), consumer.as_ref(), &first_records);
            let (pending, still_pending, released, completed) = {
                let mut operation = pin!(verify(
                    &inputs,
                    &mut protection,
                    |(bytes, receipt, records)| {
                        calls.set(calls.get() + 1);
                        protector(bytes, receipt, records, released)
                    }
                ));
                let pending = {
                    let mut wait = pin!(poll_fn(|context| operation.as_mut().poll(context)));
                    wait.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                };
                let begun_calls = calls.get();
                let advanced = ordinary.try_send(CounterMessage::Increment);
                let (original, receipt, second_allocations, second_sharing) =
                    verification.remove(0);
                let second_pointer = Rc::as_ptr(&receipt);
                let retained_second = export
                    .retain(original.protected, original.proof, receipt)
                    .unwrap_or_else(|_| {
                        panic!("second retained request while protection is pending")
                    });
                let admitted_second = export
                    .attempt(
                        &retained_second.0,
                        |receipt| {
                            current_scope(
                                projection(receipt).0,
                                &second,
                                &CounterOperation::Read,
                                Purpose::Request,
                                deadline,
                            )?;
                            Ok::<_, VerificationError>(&second)
                        },
                        CounterMessage::Read,
                    )
                    .map(|_| ());
                let received_second = export.receive_native(&retained_second.0).await;
                let (original, receipt, duplicate_allocations, duplicate_sharing) =
                    verification.remove(0);
                let duplicate = export.retain(original.protected, original.proof, receipt);
                let (original, receipt, conflict_allocations, conflict_sharing) =
                    verification.remove(0);
                let conflict = export.retain(original.protected, original.proof, receipt);
                let still_pending = {
                    let mut wait = pin!(poll_fn(|context| operation.as_mut().poll(context)));
                    wait.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                };
                let held_references = Rc::strong_count(&consumer);
                let released = release.send(());
                operation.await;
                (
                    pending,
                    still_pending,
                    released,
                    (
                        advanced,
                        retained_second,
                        admitted_second,
                        received_second,
                        second_allocations,
                        second_pointer,
                        duplicate,
                        duplicate_allocations,
                        conflict,
                        conflict_allocations,
                        [second_sharing, duplicate_sharing, conflict_sharing],
                        held_references,
                        begun_calls,
                    ),
                )
            };
            drop(consumer);
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            ordinary.close_admission();
            let mut ordinary_tail = Vec::new();
            while let Some(original) = ordinary.receive().await {
                ordinary_tail.push(original);
            }
            let (undeclared, session, mut incoming, retained) = export.undeclare().await;
            let mut rows = Vec::new();
            for (key, protected, proof, receipt, admission, native, mut client) in retained {
                let pointer = Rc::as_ptr(&receipt);
                let references = Rc::strong_count(&receipt);
                let original = Rc::try_unwrap(receipt);
                client.close_admission();
                let mut tail = Vec::new();
                while let Some(value) = client.receive().await {
                    tail.push(value);
                }
                rows.push((
                    key, protected, proof, original, admission, native, client, tail, pointer,
                    references,
                ));
            }
            let mut queries = Vec::new();
            let end = loop {
                match incoming.try_recv() {
                    Ok(original) => queries.push(original),
                    Err(cause) => break cause,
                }
            };
            move || {
                assert!(undeclared.is_ok() && stopped.is_ok());
                assert_eq!(report.retirement(), RetirementAssessment::Established);
                assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                assert_eq!(pending, Poll::Pending);
                assert_eq!(still_pending, Poll::Pending);
                assert!(released.is_ok());
                assert_eq!(
                    calls.get(),
                    1,
                    "cancelled borrowed waits must not restart protection"
                );
                assert_eq!(references, 2);
                assert_eq!(same_receipt, receipt_allocation);
                let (
                    advanced,
                    retained_second,
                    admitted_second,
                    received_second,
                    second_allocations,
                    second_pointer,
                    duplicate,
                    duplicate_allocations,
                    conflict,
                    conflict_allocations,
                    other_sharing,
                    held_references,
                    begun_calls,
                ) = completed;
                assert_eq!(
                    begun_calls, 1,
                    "protection must begin before the second request"
                );
                assert_eq!(held_references, 2);
                assert!(ordinary_tail.is_empty());
                for sharing in [first_sharing].into_iter().chain(other_sharing) {
                    assert_eq!(sharing.alloc_calls, 1);
                    assert_eq!(sharing.realloc_calls, 0);
                    assert!(sharing.bytes_allocated >= size_of::<Receipt>());
                    eprintln!(
                        "receipt sharing requested {} bytes",
                        sharing.bytes_allocated
                    );
                }
                assert_eq!(reader_sharing.alloc_calls, 0);
                assert_eq!(reader_sharing.realloc_calls, 0);
                assert_eq!(reader_sharing.bytes_allocated, 0);
                assert!(
                    primed.is_ok()
                        && advanced.is_ok()
                        && admitted_first.is_ok()
                        && admitted_second.is_ok()
                );
                assert_eq!(received_first, Some(()));
                assert_eq!(received_second, Some(()));
                assert_eq!(rows.len(), 2);
                for (row, expected, pointers, expected_receipt) in [
                    (&rows[0], 1, first_allocations, receipt_allocation),
                    (&rows[1], 2, second_allocations, second_pointer),
                ] {
                    assert_eq!(row.1.as_ptr(), pointers.0);
                    assert_eq!(row.2.as_ptr(), pointers.1);
                    assert!(row.3.is_ok());
                    assert!(matches!(row.4, Admission::Returned(Ok(()))));
                    assert_eq!(
                        row.5.as_ref().and_then(Option::as_ref).unwrap().message,
                        expected
                    );
                    assert_eq!(
                        row.5.as_ref().and_then(Option::as_ref).unwrap().from,
                        MailAddr::APPLICATION_ROOT
                    );
                    assert!(row.7.is_empty());
                    assert_eq!(row.8, expected_receipt);
                    assert_eq!(row.9, 1);
                }
                assert_eq!(rows[0].0, first.0);
                assert_eq!(rows[1].0, retained_second.0);
                let recovered = rows[0]
                    .3
                    .as_ref()
                    .unwrap_or_else(|_| panic!("original receipt still shared"));
                assert_eq!(projection(recovered).0.bombay, current);
                assert_eq!(
                    projection(recovered).0.bombay.caller.as_ptr(),
                    claim_allocation
                );
                assert_eq!(
                    evidence(recovered).map(|proof| proof.as_ptr()),
                    proof_allocation
                );
                let duplicate = duplicate.as_ref().unwrap_or_else(|_| {
                    panic!("duplicate must recover its original receipt and buffers")
                });
                assert_eq!(duplicate.0, first.0);
                let original = duplicate.1.as_ref().unwrap();
                let duplicate_scope = projection(&original.2).1;
                assert_eq!(duplicate_scope.identity, projection(recovered).1.identity);
                assert_eq!(duplicate_scope.authority_generation, 20);
                assert_eq!(projection(recovered).1.authority_generation, 19);
                assert_eq!(
                    (original.0.as_ptr(), original.1.as_ptr()),
                    duplicate_allocations
                );
                let Err((original, cause)) = &conflict else {
                    panic!("same identity must refuse changed bytes");
                };
                assert!(matches!(cause, ExportError::Conflict));
                assert_eq!(
                    (original.0.as_ptr(), original.1.as_ptr()),
                    conflict_allocations
                );
                assert!(matches!(&protection, Some(Ok(proof)) if proof.as_ref() == [13, 17]));
                let produced: ReadReply = serde_json::from_slice(&reply_bytes).unwrap();
                assert_eq!(produced.value, 1);
                assert_eq!(produced.bombay, current);
                assert_eq!(
                    projection(
                        rows[1]
                            .3
                            .as_ref()
                            .unwrap_or_else(|_| panic!("second receipt still shared"))
                    )
                    .0
                    .bombay,
                    second
                );
                assert_eq!(first_records.borrow().len(), 2);
                assert_eq!(second_records.borrow().len(), 1);
                assert_eq!(conflict_records.borrow().len(), 1);
                assert_eq!(duplicate_records.borrow().len(), 1);
                assert!(queries.is_empty());
                assert!(matches!(end, mpsc::error::TryRecvError::Disconnected));
                assert!(refusals.lock().unwrap().is_empty());
                drop((session, ordinary, rows));
            }
        })
        .unwrap_or_else(|_| panic!("actual comparison Work host"));
    let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
    let (observations, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("complete original comparison Work"));
    let closed = node.close().await;
    assert!(cleanup.is_ok() && closed.is_ok());
    let notifications = notifications.as_ref().unwrap();
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    let (origin, retirement) = native.unwrap();
    let _actor = completed_actor(origin, retirement);
    observations();
}

#[test]
fn recorded_pending_protection_preserves_other_requests_and_replay() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(pending_protection(
            ProviderProfile::Recorded,
            recorded::verify,
            recorded::protect_when_released,
            recorded::projection,
            |_| None,
            |left, right| {
                recorded::projection(left).1.identity == recorded::projection(right).1.identity
            },
        ));
}

#[test]
fn scheduled_pending_protection_preserves_other_requests_and_replay() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(pending_protection(
            ProviderProfile::Scheduled,
            scheduled::verify,
            scheduled::protect_when_released,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
            |left, right| {
                scheduled::projection(left).1.identity == scheduled::projection(right).1.identity
            },
        ));
}
