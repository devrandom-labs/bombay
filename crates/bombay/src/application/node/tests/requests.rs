//! Live existing Requester consumes a verified, typed protected Counter Read reply.
//! Positive finite test providers and one configured logical binding only.
use super::{
    Binding, Counter, CounterMessage, CounterOperation as NativeOperation, CounterProofPurpose,
    CounterProtectedMeaning, CounterRequester, CounterRequesterMessage, CounterRequesterState,
    CounterRequests, CounterServiceInput, CounterValue, Identity, Permission, Scope,
    completed_actor,
};
use bombay::ApplicationScope;
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Metadata, json, try_retain, verify};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use core::ops::AsyncFn;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::future::{Ready, ready};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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
mod recorded {
    use super::*;
    pub(super) struct Receipt {
        claims: ProofScope,
        logical: Scope,
    }
    pub(super) fn projection(receipt: &Receipt) -> (&ProofScope, Scope) {
        (&receipt.claims, receipt.logical)
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
}

fn configuration(address: &str, role: &EndpointRole) -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
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
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "preserve the original fixture's borrowed validation signature during owner relocation"
)]
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
#[expect(
    clippy::similar_names,
    reason = "retain distinct native receiver/received values and receipt projection/reply protection in the original trace"
)]
async fn live_read<Receipt>(
    profile: ProviderProfile,
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
    let receiver_connection = receiver.session.clone();
    let receiver_session = &receiver_connection;
    let (execution, receiving) = receiver.execute_with(|receiver| async move {
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
        let caller_connection = caller.session.clone();
        let caller_session = &caller_connection;
        let (caller_execution, caller_receiving) = caller.execute_with(|caller| async move {
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
            let request_bytes = serde_json::to_vec(&ReadRequestRef { bombay: &current,
                kind: Purpose::Request, operation: CounterOperation::Read }).unwrap();
            let mut records = Vec::with_capacity(2);
            records.push((scope, ProtectedMessage { protected: request_bytes.clone().into_boxed_slice(), proof: Box::from([7, 11]) }));
            let records = RefCell::new(records);
            let original = ProtectedMessage { protected: request_bytes.into_boxed_slice(), proof: Box::from([7, 11]) };
            let original_allocations = (original.protected.as_ptr(), original.proof.as_ptr());
            let begun = command.try_send(CounterRequesterMessage::BeginProtected { correlation: 23, scope });
            let (replies, mut receiving_reply) = mpsc::channel(2);
            let reply_capacity = Arc::new(Semaphore::new(2));
            let reply_publications = Arc::new(Mutex::new(Vec::new()));
            let callback_replies = Arc::clone(&reply_publications);
            // A driver timeout retains its actual cause and earns no semantic
            // credit. No native Query is issued until the real Actions trigger.
            let trigger = if begun.is_ok() { Some(timeout(Duration::from_secs(3), service_requests.receive()).await) }
                else { None };
            let mut wire_reply = None;
            let issued = if matches!(&trigger, Some(Ok(Some(User { message: CounterServiceInput::Verify(23), .. })))) {
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
            (current, original, original_allocations, begun, trigger, issued, query,
                received_request, request_shape, request_declaration, request_verification, request_scope,
                admitted, native_reply, produced_reply, protection, reply_publication, wire_reply, received_reply, reply_shape,
                reply_declaration, reply_verification, reply_scope, delivered, acknowledged, additional_reply,
                reply_publications, stopped, report, command, requester_address, caller_id, receiver_id)
        }).unwrap_or_else(|_| panic!("caller Work host entry"));
        let ((), outcome) = tokio::join!(caller_execution, caller_receiving);
        let (outcome, caller_native, caller_notifications, ()) = outcome.into_parts();
        let (read, caller_cleanup) = outcome.try_into_completed().unwrap_or_else(|_| panic!("original caller Work"));
        let caller_closed = caller.close().await;
        let undeclared = queryable.undeclare().await;
        let mut remaining_queries = Vec::new();
        while let Ok(original) = queries.try_recv() { remaining_queries.push(original); }
        let stopped = receiver.request_shutdown();
        let report = receiver.lifecycle().retirement().await;
        (read, caller_native, caller_notifications, caller_cleanup, caller_closed, undeclared,
            remaining_queries, requests, legacy_values, publications, primed, stopped, report, target_address, scope, caller_connection)
    }).unwrap_or_else(|_| panic!("receiver Work host entry"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, receiver_native, receiver_notifications, ()) = outcome.into_parts();
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
        caller_connection,
    ) = output;
    let (
        current,
        original,
        original_allocations,
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
    assert!(receiver_connection.is_closed());
    assert!(caller_connection.is_closed());
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

#[test]
fn recorded_live_requester_stores_checked_read_reply() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(live_read(
            ProviderProfile::Recorded,
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
            scheduled::verify,
            scheduled::protect,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}
