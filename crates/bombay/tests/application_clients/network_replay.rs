//! Two real Nodes, typed Task Actions and a terminating Ledger replay comparison.
//! Finite profiles and this Read-only schema are test inputs, not a shipping contract.
//! Positive provider equations only: fixed current authority/permission and one
//! exact replay key; no stale/unavailable/revocation/window/Stop witness here.
use super::counter::CounterValue;
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, EstablishedRecipient, MessageProtocol, Never, Protocol, User,
};
use bombay::lifecycle::{Task, TaskMessage, TaskResult, TaskState};
use bombay::prelude::{Application, MailAddr, StopOnShutdown};
use bombay::remote::{
    Admission, Export, ExportError, ExportLimits, IngressError, Metadata, RequestKey,
    RequestRefusal, json, try_retain, verify,
};
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, ActorRetirementReport,
    ApplicationClient, RetirementAssessment, RootOrigin, SendError, TrySendError,
};
use core::ops::AsyncFn;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::env;
use std::fmt::Debug;
use std::future::{Future, Ready, ready};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use zenoh::query::{ConsolidationMode, Query, Reply};
use zenoh::{Error as ZenohError, Session};
use zenoh_config::Config;

#[path = "../application_support/mod.rs"]
mod application_support;
use application_support::assert_completed;

#[derive(Debug)]
struct ProtectedMessage {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
type RequestBatch = MessageProtocol<MailAddr, TaskResult<Vec<ProtectedMessage>>>;
type RequestTask = Task<MailAddr, Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
type BatchCommand = TaskMessage<Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
type SenderRoot = StopOnShutdown<RequestTask>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LedgerOperation {
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
#[derive(Clone, Copy)]
// Observation selector only: Resend requires an actual production retention mutant.
enum ReadRetention {
    ReuseFirst,
    Resend,
}
fn read_retention() -> ReadRetention {
    match env::var("BOMBAY_NODE_READ_REPLAY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("reuse-first") => ReadRetention::ReuseFirst,
        Ok("resend") => ReadRetention::Resend,
        original => panic!("unsupported explicit test comparison: {original:?}"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest<Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Serialize)]
struct ReadRequestRef<'a, Operation> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply<Value, Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
    value: Value,
}
#[derive(Serialize)]
struct ReadReplyRef<'a, Value, Operation> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
    value: &'a Value,
}
// Provider scope ignores protocol-owned value fields; concrete DTO decoding above
// is independent. Matching the entire registered byte/proof pair remains mandatory.
#[derive(Deserialize)]
struct ProofScope<Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Binding {
    deployment: &'static str,
    caller: &'static str,
    controller: &'static str,
    actor: &'static str,
    host: &'static str,
    protocol: &'static str,
    target_assignment: &'static str,
    logical_session: u64,
}

#[derive(Debug)]
enum VerificationError {
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
mod recorded {
    use super::*;
    pub(super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
        binding: Binding,
    }
    pub(super) fn identity<Operation>(receipt: &Receipt<Operation>) -> (&Binding, u64) {
        (&receipt.binding, receipt.scope.bombay.request_sequence)
    }
    pub(super) fn projection<Operation>(receipt: &Receipt<Operation>) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(super) fn verify<Operation: DeserializeOwned>(
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
mod scheduled {
    use super::*;
    pub(super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
        binding: Binding,
        original_proof: Box<[u8]>,
    }
    pub(super) fn identity<Operation>(receipt: &Receipt<Operation>) -> (&Binding, u64) {
        (&receipt.binding, receipt.scope.bombay.request_sequence)
    }
    pub(super) fn projection<Operation>(receipt: &Receipt<Operation>) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(super) fn proof<Operation>(receipt: &Receipt<Operation>) -> &[u8] {
        &receipt.original_proof
    }
    pub(super) async fn verify<Operation: DeserializeOwned>(
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
fn metadata(
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
fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(64).unwrap(),
        token_bytes: NonZeroUsize::new(128).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    }
}
fn current_scope<Operation: Eq>(
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

struct ReadAcquisition<Operation, Receipt, WireValue, Replies: Protocol<Addr = MailAddr>> {
    invocation: Result<(), ZenohError>,
    query_publication: Option<IngressError<Query>>,
    #[expect(
        clippy::option_option,
        reason = "retain not acquired, acquired closure and acquired Query independently"
    )]
    query: Option<Option<(Query, OwnedSemaphorePermit)>>,
    original: Option<ProtectedMessage>,
    shape: Option<Result<(), json::Error>>,
    declaration: Option<Result<ReadRequest<Operation>, serde_json::Error>>,
    verification: Option<Result<Receipt, VerificationError>>,
    scope: Option<Result<(), VerificationError>>,
    attempt: Option<Result<(), RequestRefusal<VerificationError>>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain first or duplicate originals and every exact refusal in the acquisition that received them"
    )]
    retained: Option<
        Result<
            (RequestKey<Replies>, Option<(Box<[u8]>, Box<[u8]>, Receipt)>),
            ((Box<[u8]>, Box<[u8]>, Receipt), ExportError),
        >,
    >,
    original_allocations: Option<(*const u8, *const u8)>,
    source: Option<usize>,
    #[expect(
        clippy::option_option,
        reason = "distinguish no native acquisition from an acquired closed stream"
    )]
    native_acquisition: Option<Option<()>>,
    reply_publication: Option<Result<(), ZenohError>>,
    reply: Option<(Reply, OwnedSemaphorePermit)>,
    reply_original: Option<ProtectedMessage>,
    reply_shape: Option<Result<(), json::Error>>,
    reply_declaration: Option<Result<ReadReply<WireValue, Operation>, serde_json::Error>>,
    reply_verification: Option<Result<Receipt, VerificationError>>,
    reply_scope: Option<Result<(), VerificationError>>,
}
struct ReadStudy<Target, Replies, Operation, Receipt, WireValue>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    current: Metadata,
    binding: Binding,
    target: MailAddr,
    caller_zid: String,
    receiver_zid: String,
    batch: Option<User<MailAddr, TaskResult<Vec<ProtectedMessage>>>>,
    batch_allocations: Vec<(*const u8, *const u8)>,
    begun: Result<(), SendError<BatchCommand>>,
    acquisitions: Vec<ReadAcquisition<Operation, Receipt, WireValue, Replies>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain every affine request original, native client and drained tail without another owner"
    )]
    requests: Vec<(
        RequestKey<Replies>,
        Box<[u8]>,
        Box<[u8]>,
        Receipt,
        Admission<Target::Msg>,
        Option<Option<User<MailAddr, Replies::Msg>>>,
        ApplicationClient<EstablishedRecipient<Target>, Replies>,
        Vec<User<MailAddr, Replies::Msg>>,
    )>,
    first_retirement: Option<ActorRetirementReport>,
    #[expect(
        clippy::type_complexity,
        reason = "retain each original Query refusal and its native callback publication"
    )]
    query_callbacks: Arc<Mutex<Vec<Result<(), mpsc::error::TrySendError<IngressError<Query>>>>>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the original Reply and native ingress cause for every callback"
    )]
    reply_callbacks: Vec<Arc<Mutex<Vec<Result<(), IngressError<Reply>>>>>>,
    query_drain: Vec<(Query, OwnedSemaphorePermit)>,
    query_end: mpsc::error::TryRecvError,
    refusal_drain: Vec<IngressError<Query>>,
    refusal_end: mpsc::error::TryRecvError,
    undeclared: Result<(), ZenohError>,
    sender_cleanup: Result<(), bombay::ApplicationCleanupError>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the concrete Task root and every original native retirement lane"
    )]
    sender_native: Result<
        (
            RootOrigin<SenderRoot>,
            ActorRetirement<SenderRoot, Never, Vec<Never>>,
        ),
        oneshot::error::RecvError,
    >,
    sender_notifications: Result<ActorNotificationReceipts, oneshot::error::RecvError>,
    sender_closed: Result<(), ZenohError>,
}

impl<Target, Replies, Operation, Receipt, WireValue>
    ReadStudy<Target, Replies, Operation, Receipt, WireValue>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    fn native_outcomes(&self) -> impl Iterator<Item = &User<MailAddr, Replies::Msg>> {
        self.requests
            .iter()
            .filter_map(|entry| entry.5.as_ref().and_then(Option::as_ref))
    }

    fn request_original<'a>(
        &'a self,
        acquired: &'a ReadAcquisition<Operation, Receipt, WireValue, Replies>,
    ) -> Option<(&'a [u8], &'a [u8], &'a Receipt)> {
        match acquired.retained.as_ref()? {
            Ok((key, None)) => {
                let (_, protected, proof, receipt, _, _, _, _) =
                    self.requests.iter().find(|entry| &entry.0 == key)?;
                Some((protected, proof, receipt))
            }
            Ok((_, Some(original))) | Err((original, _)) => {
                Some((&original.0, &original.1, &original.2))
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity,
    reason = "one causal trace keeps explicit policy inputs and all original acquisition and cleanup products together"
)]
async fn read_study<Target, Replies, Operation, Receipt, WireValue, FirstRetirement>(
    mut export: Export<Target, Replies, Receipt>,
    mut announcements: mpsc::Receiver<IngressError<Query>>,
    query_callbacks: Arc<Mutex<Vec<Result<(), mpsc::error::TrySendError<IngressError<Query>>>>>>,
    receiver_session: &Session,
    address: &str,
    target: MailAddr,
    protocol: &str,
    binding: Binding,
    operation: Operation,
    profile: ProviderProfile,
    expected: WireValue,
    read: fn(EstablishedRecipient<Replies>) -> Target::Msg,
    encode: fn(&Replies::Msg) -> WireValue,
    mut first_retirement: Option<FirstRetirement>,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
    identity: fn(&Receipt) -> (&Binding, u64),
) -> ReadStudy<Target, Replies, Operation, Receipt, WireValue>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
    Operation: Copy + Eq + Serialize + DeserializeOwned,
    WireValue: Serialize + DeserializeOwned,
    FirstRetirement: Future<Output = ActorRetirementReport>,
{
    let mut sender = Application::new(RequestTask::new().stop_on_shutdown())
        .open_node(configuration(address, &EndpointRole::Caller))
        .await
        .unwrap_or_else(|_| panic!("explicit caller Node setup"));
    // The owner is outside borrowed Sender Work; no fresh runtime is opened.
    let retained_export = &mut export;
    let refused_queries = &mut announcements;
    let (execution, receiving) = sender.execute_with(|application, caller_session| async move {
        let caller_zid = caller_session.zid().to_string();
        let receiver_zid = receiver_session.zid().to_string();
        let current = metadata(profile, protocol, caller_zid.clone(), receiver_zid.clone());
        let deadline = Instant::now() + Duration::from_secs(30);
        let request_bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: &current, kind: Purpose::Request, operation,
        }).unwrap();
        let reply_bytes = serde_json::to_vec(&ReadReplyRef {
            bombay: &current, kind: Purpose::ReadReply, operation, value: &expected,
        }).unwrap();
        let records = vec![
            ProtectedMessage { protected: request_bytes.clone().into_boxed_slice(), proof: Box::from([7, 11]) },
            ProtectedMessage { protected: reply_bytes.into_boxed_slice(), proof: Box::from([13, 17]) },
        ];
        let originals = (0..2).map(|_| ProtectedMessage {
            protected: request_bytes.clone().into_boxed_slice(), proof: Box::from([7, 11]),
        }).collect::<Vec<_>>();
        let batch_allocations = originals.iter().map(|r| (r.protected.as_ptr(), r.proof.as_ptr())).collect();
        let mut requests = application.external::<RequestBatch>().unwrap();
        let begun = requests.send(&application.root().established_recipient(), BatchCommand::Complete {
            result: originals, reply_to: requests.recipient(),
        }).await;
        let batch = if begun.is_ok() { requests.receive().await } else { None };
        let mut acquisitions = Vec::new();
        let mut reply_callbacks = Vec::new();
        let mut joined_first = None;
        if let Some(User { message: TaskResult::Completed(originals), .. }) = &batch {
            for request in originals {
                let reply_capacity = Arc::new(Semaphore::new(2));
                let (incoming_reply, mut received_reply) = mpsc::channel(2);
                let publications = Arc::new(Mutex::new(Vec::new()));
                let callback_publications = Arc::clone(&publications);
                let invocation = caller_session.get("bombay/replay/read")
                    .consolidation(ConsolidationMode::None).timeout(Duration::from_secs(3))
                    .payload(request.protected.to_vec()).attachment(request.proof.to_vec())
                    .callback(move |reply| {
                        let retained = try_retain(&incoming_reply, &reply_capacity, reply);
                        callback_publications.lock().unwrap().push(retained);
                    }).await;
                let mut acquired = ReadAcquisition {
                    invocation, query_publication: None, query: None, original: None, shape: None,
                    declaration: None, verification: None, scope: None, attempt: None,
                    retained: None, original_allocations: None, source: None, native_acquisition: None,
                    reply_publication: None, reply: None, reply_original: None, reply_shape: None, reply_declaration: None,
                    reply_verification: None, reply_scope: None,
                };
                if acquired.invocation.is_ok() {
                    tokio::select! {
                        () = retained_export.receive_query(&mut acquired.query) => {},
                        refusal = refused_queries.recv() => acquired.query_publication = refusal,
                        reply = received_reply.recv() => acquired.reply = reply,
                    }
                    if let Some(Some((query, _permit))) = &acquired.query {
                            if let (Some(payload), Some(proof)) = (query.payload(), query.attachment()) {
                                acquired.original = Some(ProtectedMessage {
                                    protected: payload.to_bytes().into_owned().into_boxed_slice(),
                                    proof: proof.to_bytes().into_owned().into_boxed_slice(),
                                });
                                let original = acquired.original.as_ref().unwrap();
                                acquired.original_allocations = Some((original.protected.as_ptr(), original.proof.as_ptr()));
                                acquired.shape = Some(json::check(&original.protected, &original.proof, &limits()));
                                if matches!(&acquired.shape, Some(Ok(()))) {
                                    acquired.declaration = Some(serde_json::from_slice(&original.protected));
                                    verify(original, &mut acquired.verification, |r| provider(r, &records, binding)).await;
                                }
                                if let (Some(Ok(declared)), Some(Ok(receipt))) = (&acquired.declaration, &acquired.verification) {
                                    acquired.scope = Some(current_scope(projection(receipt), &current, &operation,
                                        Purpose::Request, deadline));
                                    if matches!(&acquired.scope, Some(Ok(()))) && declared.bombay == current
                                        && declared.kind == Purpose::Request && declared.operation == operation
                                        && identity(receipt).0 == &binding {
                                        let original = acquired.original.take().unwrap();
                                        let Some(Ok(receipt)) = acquired.verification.take() else {
                                            panic!("the observed original success moves into retained ownership");
                                        };
                                        acquired.retained = Some(retained_export.retain(
                                            original.protected, original.proof, receipt,
                                        ));
                                        let key = match &acquired.retained {
                                            Some(Ok((key, duplicate))) => {
                                                if duplicate.is_none() {
                                                    acquired.attempt = Some(retained_export.attempt(key, |receipt| {
                                                        current_scope(projection(receipt), &current, &operation, Purpose::Request, deadline)?;
                                                        Ok(&current)
                                                    }, read).map(|_| ()));
                                                    tokio::select! {
                                                        original = retained_export.receive_native(key) => acquired.native_acquisition = Some(original),
                                                        reply = received_reply.recv() => acquired.reply = reply,
                                                    }
                                                }
                                                Some(key)
                                            }
                                            Some(Err(_)) | None => None,
                                        };
                                        let source = key.and_then(|key| retained_export.request(key))
                                            .and_then(|request| request.4.as_ref().and_then(Option::as_ref));
                                        if let Some(source) = source {
                                            acquired.source = Some(ptr::from_ref(source).addr());
                                            let actual = encode(&source.message);
                                            let bytes = serde_json::to_vec(&ReadReplyRef {
                                                bombay: &current, kind: Purpose::ReadReply, operation, value: &actual,
                                            }).unwrap();
                                            acquired.reply_publication = Some(query.reply("bombay/replay/read", bytes)
                                                .attachment(vec![13, 17]).await);
                                        } else {
                                            acquired.reply_publication = Some(query.reply_err("native read refused").await);
                                        }
                                    }
                                }
                            }
                            if acquired.reply_publication.is_none() {
                                acquired.reply_publication = Some(query.reply_err("protected read refused").await);
                            }
                    } else if let Some(refusal) = &acquired.query_publication {
                        let query = match refusal {
                            IngressError::Capacity { original, .. }
                            | IngressError::Queue { cause: mpsc::error::TrySendError::Full((original, _))
                                | mpsc::error::TrySendError::Closed((original, _)) } => original,
                        };
                        acquired.reply_publication = Some(query.reply_err("retained query refused").await);
                    }
                    if acquired.reply.is_none() { acquired.reply = received_reply.recv().await; }
                    if let Some((reply, _permit)) = &acquired.reply
                        && let Ok(sample) = reply.result() {
                            acquired.reply_original = Some(ProtectedMessage {
                                protected: sample.payload().to_bytes().into_owned().into_boxed_slice(),
                                proof: sample.attachment().map(|p| p.to_bytes().into_owned().into_boxed_slice()).unwrap_or_default(),
                            });
                            let original = acquired.reply_original.as_ref().unwrap();
                            acquired.reply_shape = Some(json::check(&original.protected, &original.proof, &limits()));
                            if matches!(&acquired.reply_shape, Some(Ok(()))) {
                                acquired.reply_declaration = Some(serde_json::from_slice(&original.protected));
                                verify(original, &mut acquired.reply_verification, |r| provider(r, &records, binding)).await;
                                if let Some(Ok(receipt)) = &acquired.reply_verification {
                                    acquired.reply_scope = Some(current_scope(projection(receipt), &current, &operation,
                                        Purpose::ReadReply, deadline));
                                }
                            }
                    }
                }
                if acquired.source.is_some()
                    && joined_first.is_none()
                    && let Some(retirement) = first_retirement.take() {
                    joined_first = Some(retirement.await);
                }
                reply_callbacks.push(publications);
                acquisitions.push(acquired);
            }
        }
        (current, caller_zid, receiver_zid, batch, batch_allocations, begun,
            acquisitions, reply_callbacks, joined_first, requests)
    }).unwrap_or_else(|_| panic!("the concrete sender Application enters its host"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, sender_native, sender_notifications, ()) = outcome.into_parts();
    let (output, sender_cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original sender Work is independently received"));
    let (
        current,
        caller_zid,
        receiver_zid,
        batch,
        batch_allocations,
        begun,
        acquisitions,
        reply_callbacks,
        joined_first,
        requests,
    ) = output;
    drop(requests);
    let sender_closed = sender.close().await;
    let (undeclared, session, mut incoming, retained) = export.undeclare().await;
    let mut requests = Vec::new();
    for (key, protected, proof, receipt, admission, native, mut client) in retained {
        client.close_admission();
        let mut tail = Vec::new();
        while let Some(original) = client.receive().await {
            tail.push(original);
        }
        requests.push((
            key, protected, proof, receipt, admission, native, client, tail,
        ));
    }
    let mut query_drain = Vec::new();
    let query_end = loop {
        match incoming.try_recv() {
            Ok(original) => query_drain.push(original),
            Err(cause) => break cause,
        }
    };
    let mut refusal_drain = Vec::new();
    let refusal_end = loop {
        match announcements.try_recv() {
            Ok(original) => refusal_drain.push(original),
            Err(cause) => break cause,
        }
    };
    drop(session);
    ReadStudy {
        current,
        binding,
        target,
        caller_zid,
        receiver_zid,
        batch,
        batch_allocations,
        begun,
        acquisitions,
        requests,
        first_retirement: joined_first,
        query_callbacks,
        reply_callbacks,
        query_drain,
        query_end,
        refusal_drain,
        refusal_end,
        undeclared,
        sender_cleanup,
        sender_native,
        sender_notifications,
        sender_closed,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "observe the complete independent request, provider, native and cleanup trace before accepting the replay result"
)]
fn assert_study<Target, Replies, Operation, Receipt, WireValue>(
    study: &ReadStudy<Target, Replies, Operation, Receipt, WireValue>,
    expected: &WireValue,
    operation: Operation,
    retention: ReadRetention,
    identity: fn(&Receipt) -> (&Binding, u64),
) where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
    Operation: Copy + Eq + Debug,
    WireValue: Eq + Debug,
{
    assert!(study.sender_cleanup.is_ok());
    assert!(study.sender_closed.is_ok());
    let notifications = study.sender_notifications.as_ref().unwrap();
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_, retirement) = study.sender_native.as_ref().unwrap();
    let custody = retirement.custody().unwrap();
    assert_eq!(custody.behavior.base().state(), TaskState::Completed);
    assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty());
    assert!(study.begun.is_ok() && study.undeclared.is_ok());
    assert!(study.query_drain.is_empty() && study.refusal_drain.is_empty());
    for request in &study.requests {
        assert!(request.7.is_empty());
    }
    assert!(matches!(
        study.query_end,
        mpsc::error::TryRecvError::Disconnected
    ));
    assert!(matches!(
        study.refusal_end,
        mpsc::error::TryRecvError::Disconnected
    ));
    assert_ne!(study.caller_zid, study.receiver_zid);
    assert_eq!(study.current.caller_session.as_ref(), study.caller_zid);
    assert_eq!(
        study.current.runtime_generation.as_ref(),
        study.receiver_zid
    );
    assert_eq!(study.current.actor_generation, 0);
    assert_eq!(study.target, MailAddr::APPLICATION_ROOT);
    let batch = study.batch.as_ref().unwrap();
    assert_eq!(batch.from, MailAddr::APPLICATION_ROOT);
    let TaskResult::Completed(originals) = &batch.message else {
        panic!("the actual Task sends its complete batch");
    };
    assert_eq!(originals.len(), 2);
    for (original, (protected, proof)) in originals.iter().zip(&study.batch_allocations) {
        assert_eq!(original.protected.as_ptr(), *protected);
        assert_eq!(original.proof.as_ptr(), *proof);
    }
    let callbacks = study.query_callbacks.lock().unwrap();
    assert_eq!(callbacks.len(), 0);
    for result in callbacks.iter() {
        assert!(result.is_ok());
    }
    assert_eq!(study.acquisitions.len(), 2);
    let first = &study.acquisitions[0];
    let Some(Ok((first_key, _))) = &first.retained else {
        panic!("first actual retained key");
    };
    let first_admission = &study
        .requests
        .iter()
        .find(|entry| &entry.0 == first_key)
        .unwrap()
        .4;
    assert!(matches!(first_admission, Admission::Returned(Ok(()))));
    assert!(matches!(first.attempt, Some(Ok(()))));
    assert!(matches!(first.native_acquisition, Some(Some(()))));
    assert!(study.native_outcomes().next().is_some());
    for actual in study.native_outcomes() {
        assert_eq!(actual.from, study.target);
    }
    for (index, acquisition) in study.acquisitions.iter().enumerate() {
        assert!(acquisition.invocation.is_ok());
        assert!(acquisition.query_publication.is_none());
        assert!(matches!(&acquisition.query, Some(Some(_))));
        assert!(matches!(&acquisition.shape, Some(Ok(()))));
        if let Some(original) = &acquisition.verification {
            assert_verification(Some(original));
        }
        let (protected, proof, receipt) = study.request_original(acquisition).unwrap();
        let (logical_binding, sequence) = identity(receipt);
        assert_eq!(logical_binding, &study.binding);
        assert_eq!(sequence, study.current.request_sequence);
        assert_eq!(protected, originals[index].protected.as_ref());
        assert_eq!(proof, originals[index].proof.as_ref());
        let (protected_allocation, proof_allocation) = acquisition.original_allocations.unwrap();
        assert_eq!(protected.as_ptr(), protected_allocation);
        assert_eq!(proof.as_ptr(), proof_allocation);
        if let Some(Err(refusal)) = &acquisition.attempt {
            match refusal {
                RequestRefusal::Authority { cause } => {
                    panic!("original current authority refusal: {cause:?}")
                }
                RequestRefusal::UnknownRequest => panic!("the export refused its own issued key"),
            }
        }
        if let Some(Err((_, refusal))) = &acquisition.retained {
            panic!("actual production retention refusal: {refusal}");
        }

        let declared = acquisition.declaration.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(declared.bombay, study.current);
        assert_eq!(declared.kind, Purpose::Request);
        assert_eq!(declared.operation, operation);
        assert!(matches!(&acquisition.scope, Some(Ok(()))));
        assert!(matches!(&acquisition.reply_publication, Some(Ok(()))));
        let publications = study.reply_callbacks[index].lock().unwrap();
        assert_eq!(publications.len(), 1);
        for result in publications.iter() {
            assert!(result.is_ok());
        }
        let (reply, _permit) = acquisition.reply.as_ref().unwrap();
        if let Some(Ok(declared)) = &acquisition.reply_declaration {
            let sample = reply.result().unwrap();
            assert_eq!(sample.key_expr().as_str(), "bombay/replay/read");
            let raw = acquisition.reply_original.as_ref().unwrap();
            let actual_payload = sample.payload().to_bytes();
            assert_eq!(raw.protected.as_ref(), actual_payload.as_ref());
            assert_eq!(raw.proof.as_ref(), [13, 17]);
            assert_eq!(declared.bombay, study.current);
            assert_eq!(declared.kind, Purpose::ReadReply);
            assert_eq!(declared.operation, operation);
            assert_eq!(&declared.value, expected);
            assert!(matches!(&acquisition.reply_shape, Some(Ok(()))));
            assert_verification(acquisition.reply_verification.as_ref());
            assert!(acquisition.reply_original.is_some());
            assert!(matches!(&acquisition.reply_scope, Some(Ok(()))));
        } else {
            assert!(matches!(retention, ReadRetention::Resend));
            assert_eq!(index, 1);
            let refusal = reply.result().unwrap_err();
            let actual_refusal = refusal.payload().to_bytes();
            assert_eq!(actual_refusal.as_ref(), b"native read refused");
        }
    }
}

fn assert_verification<Receipt>(verification: Option<&Result<Receipt, VerificationError>>) {
    match verification {
        Some(Ok(_)) => {}
        Some(Err(VerificationError::Declaration(cause))) => panic!("original declaration: {cause}"),
        Some(Err(VerificationError::Unavailable(cause))) => {
            panic!("original provider publication: {cause}")
        }
        Some(Err(VerificationError::Invalid)) => panic!("exact byte/proof/profile refused"),
        None => panic!("the actual provider result was not acquired"),
    }
}
fn assert_notifications(receipts: &Result<ActorNotificationReceipts, oneshot::error::RecvError>) {
    let receipts = receipts.as_ref().unwrap();
    assert!(receipts.termination.is_ok());
    assert!(receipts.retirement.is_ok());
}
fn assert_report(report: ActorRetirementReport) {
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the complete independent Counter trace and acquired cleanup products in one witness"
)]
async fn counter_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<CounterOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    proof: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let retention = read_retention();
    let address = reserved_address();
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("explicit receiver Node setup"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let target = application.root().established_recipient();
            let target_address = application.root().address();
            let (announced, announcements) = mpsc::channel(2);
            let query_callbacks = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&query_callbacks);
            let export = application
                .export::<_, CounterValue, Receipt>(
                    target,
                    "bombay/replay/read",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| {
                        let published = announced.try_send(original);
                        observed.lock().unwrap().push(published);
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("actual Node-associated export"));
            let sender = application.sender().unwrap();
            let primed = sender.send(CounterMessage::Increment).await;
            let study = read_study(
                export,
                announcements,
                query_callbacks,
                session,
                &address,
                target_address,
                "counter",
                Binding {
                    deployment: "two-node-read-study",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "counter-root-test-assignment",
                    logical_session: 41,
                },
                CounterOperation::Read,
                profile,
                1,
                CounterMessage::Read,
                |value| *value,
                None::<Ready<ActorRetirementReport>>,
                provider,
                projection,
                identity,
            )
            .await;
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (study, primed, stopped, report)
        })
        .unwrap_or_else(|_| panic!("receiver host entry"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, native, notifications, ()) = outcome.into_parts();
    let ((study, primed, stopped, report), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original receiver Work output"));
    let closed = node.close().await;
    let native = native.unwrap();
    assert!(closed.is_ok() && cleanup.is_ok());
    assert_notifications(&notifications);
    assert_report(report);
    assert!(primed.is_ok() && stopped.is_ok());
    assert_study(&study, &1, CounterOperation::Read, retention, identity);
    assert_provider_proof(&study, proof);
    let first = study.native_outcomes().next().unwrap();
    assert_eq!(first.from, study.target);
    assert_eq!(first.message, 1);
    let first_source = study.acquisitions[0]
        .source
        .expect("first actual in-owner native observation");
    let sources = study
        .acquisitions
        .iter()
        .map(|a| a.source)
        .collect::<Vec<_>>();
    let attempts = study
        .acquisitions
        .iter()
        .filter(|a| a.attempt.is_some())
        .count();
    let native_outcome_count = study.native_outcomes().count();
    let sender_native = study.sender_native.unwrap();
    assert_completed(native, None);
    assert_completed(sender_native, None);
    // Counter Read is nonmutating: this observes local attempts and two real
    // correlated Replies, not an actor-internal read counter or a replay protocol.
    assert_eq!(
        attempts, 1,
        "a duplicate must reuse the first actual native outcome"
    );
    assert_eq!(native_outcome_count, 1);
    assert_eq!(sources, [Some(first_source), Some(first_source)]);
    assert!(study.acquisitions[1].attempt.is_none());
}

#[expect(
    clippy::too_many_lines,
    reason = "the terminating Ledger trace preserves all native originals through cleanup before its replay oracle"
)]
async fn ledger_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &[ProtectedMessage],
        Binding,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<LedgerOperation>,
    identity: fn(&Receipt) -> (&Binding, u64),
    proof: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Receipt, &Receipt) -> bool,
) {
    let retention = read_retention();
    let address = reserved_address();
    let mut node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration(&address, &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("explicit Ledger Node setup"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let target = application.root().established_recipient();
            let target_address = application.root().address();
            let (announced, announcements) = mpsc::channel(2);
            let query_callbacks = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&query_callbacks);
            let export = application
                .export::<_, Receipts, Receipt>(
                    target,
                    "bombay/replay/read",
                    ExportLimits {
                        queued_queries: NonZeroUsize::new(2).unwrap(),
                        retained_queries: NonZeroUsize::new(2).unwrap(),
                        retained_requests: NonZeroUsize::new(2).unwrap(),
                    },
                    same_request,
                    move |original| {
                        let published = announced.try_send(original);
                        observed.lock().unwrap().push(published);
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("actual Node-associated export"));
            let sender = application.sender().unwrap();
            let caller = sender.address();
            let original = Box::<[u8]>::from([59, 61]);
            let allocation = original.as_ptr();
            let primed = sender.send(LedgerCommand::Append(original)).await;
            let expected = vec![u64::from(caller)];
            let study = read_study(
                export,
                announcements,
                query_callbacks,
                session,
                &address,
                target_address,
                "ledger",
                Binding {
                    deployment: "two-node-read-study",
                    caller: "caller",
                    controller: "owner",
                    actor: "ledger",
                    host: "receiver",
                    protocol: "ledger",
                    target_assignment: "ledger-root-test-assignment",
                    logical_session: 41,
                },
                LedgerOperation::Read,
                profile,
                expected,
                LedgerCommand::Read,
                |origins| origins.iter().copied().map(u64::from).collect::<Vec<_>>(),
                Some(application.lifecycle().retirement()),
                provider,
                projection,
                identity,
            )
            .await;
            (study, primed, caller, allocation)
        })
        .unwrap_or_else(|_| panic!("Ledger host entry"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, native, notifications, ()) = outcome.into_parts();
    let ((study, primed, caller, allocation), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Ledger Work output"));
    let closed = node.close().await;
    let native = native.unwrap();
    let (_, retirement) = &native;
    let ledger = retirement.custody().unwrap().behavior.base();
    assert!(closed.is_ok() && cleanup.is_ok());
    assert_notifications(&notifications);
    let first_report = study.first_retirement.unwrap();
    assert_report(first_report);
    assert!(primed.is_ok());
    let [(origin, original)] = ledger.entries.as_slice() else {
        panic!("one original append survives");
    };
    assert_eq!(*origin, caller);
    assert_eq!(original.as_ptr(), allocation);
    assert_eq!(original.as_ref(), [59, 61]);
    assert_study(
        &study,
        &vec![u64::from(caller)],
        LedgerOperation::Read,
        retention,
        identity,
    );
    assert_provider_proof(&study, proof);
    let first = study.native_outcomes().next().unwrap();
    assert_eq!(first.from, study.target);
    assert_eq!(first.message.as_slice(), [caller]);
    let first_source = study.acquisitions[0]
        .source
        .expect("first actual in-owner native observation");
    let sources = study
        .acquisitions
        .iter()
        .map(|a| a.source)
        .collect::<Vec<_>>();
    let attempts = study
        .acquisitions
        .iter()
        .filter(|a| a.attempt.is_some())
        .count();
    let second = &study.acquisitions[1];
    match retention {
        ReadRetention::Resend => {
            // The stateless baseline must establish the genuine native cause
            // before the intended one-attempt oracle; a timeout is not this fact.
            let Some(Ok((key, _))) = &second.retained else {
                panic!("second actual retained key");
            };
            let entry = study.requests.iter().find(|entry| &entry.0 == key).unwrap();
            let Admission::Returned(Err(TrySendError::Closed(LedgerCommand::Read(original)))) =
                &entry.4
            else {
                panic!("the second actual attempt returns its original Closed(Read)");
            };
            let expected_reply = entry.6.recipient();
            assert_eq!(original, &expected_reply);
            let (reply, _) = second.reply.as_ref().unwrap();
            assert!(reply.result().is_err());
        }
        ReadRetention::ReuseFirst => {
            assert!(second.attempt.is_none());
            let (reply, _) = second.reply.as_ref().unwrap();
            assert!(reply.result().is_ok());
        }
    }
    let native_outcome_count = study.native_outcomes().count();
    let sender_native = study.sender_native.unwrap();
    assert_completed(native, None);
    assert_completed(sender_native, None);
    assert_eq!(
        attempts, 1,
        "the terminating Ledger's duplicate must reuse its first native outcome"
    );
    assert_eq!(native_outcome_count, 1);
    assert_eq!(sources, [Some(first_source), Some(first_source)]);
}

fn assert_provider_proof<Target, Replies, Operation, Receipt, Value>(
    study: &ReadStudy<Target, Replies, Operation, Receipt, Value>,
    proof: fn(&Receipt) -> Option<&[u8]>,
) where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    for acquired in &study.acquisitions {
        if let Some((_, _, receipt)) = study.request_original(acquired)
            && let Some(original_proof) = proof(receipt)
        {
            assert_eq!(original_proof, [7, 11]);
        }
        if let Some(Ok(receipt)) = &acquired.reply_verification
            && let Some(original_proof) = proof(receipt)
        {
            assert_eq!(original_proof, [13, 17]);
        }
    }
}

fn same_recorded_counter(
    a: &recorded::Receipt<CounterOperation>,
    b: &recorded::Receipt<CounterOperation>,
) -> bool {
    recorded::identity(a) == recorded::identity(b)
}
fn same_scheduled_counter(
    a: &scheduled::Receipt<CounterOperation>,
    b: &scheduled::Receipt<CounterOperation>,
) -> bool {
    scheduled::identity(a) == scheduled::identity(b)
}
fn same_recorded_ledger(
    a: &recorded::Receipt<LedgerOperation>,
    b: &recorded::Receipt<LedgerOperation>,
) -> bool {
    recorded::identity(a) == recorded::identity(b)
}
fn same_scheduled_ledger(
    a: &scheduled::Receipt<LedgerOperation>,
    b: &scheduled::Receipt<LedgerOperation>,
) -> bool {
    scheduled::identity(a) == scheduled::identity(b)
}

#[test]
fn recorded_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Recorded,
            recorded::verify::<CounterOperation>,
            recorded::projection,
            recorded::identity,
            |_| None,
            same_recorded_counter,
        ));
}
#[test]
fn scheduled_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<CounterOperation>,
            scheduled::projection,
            scheduled::identity,
            |receipt| Some(scheduled::proof(receipt)),
            same_scheduled_counter,
        ));
}
#[test]
fn recorded_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Recorded,
            recorded::verify::<LedgerOperation>,
            recorded::projection,
            recorded::identity,
            |_| None,
            same_recorded_ledger,
        ));
}
#[test]
fn scheduled_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<LedgerOperation>,
            scheduled::projection,
            scheduled::identity,
            |receipt| Some(scheduled::proof(receipt)),
            same_scheduled_ledger,
        ));
}

#[path = "network_replay/cancellation.rs"]
mod cancellation;

#[path = "network_replay/construction.rs"]
mod construction;

#[path = "network_replay/refusals.rs"]
mod refusals;

#[path = "network_replay/permissions.rs"]
mod permissions;

#[path = "network_replay/association.rs"]
mod association;

#[path = "network_replay/pressure.rs"]
mod pressure;
