//! Local test evidence only: no cryptography, remote command admission or production provider.

use bombay::behavior::{
    BehaviorSettlements, EstablishedDelivery, MessageProtocol, NoSends, Step, User,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, SendError};
use core::ops::AsyncFn;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{runtime::Builder, sync::oneshot};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Increment,
    Read,
}

// These are finite fixture values, not production wire names or generation issuers.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Scope {
    caller: String,
    deployment: String,
    purpose: String,
    controller: String,
    actor: String,
    protocol: String,
    operation: Operation,
    host: String,
    runtime: u64,
    actor_generation: u64,
    grant_issuer: String,
    authority_generation: u64,
}

impl Scope {
    fn configured() -> Self {
        Self {
            caller: "alice".into(),
            deployment: "test-deployment".into(),
            purpose: "command".into(),
            controller: "controller".into(),
            actor: "counter".into(),
            protocol: "counter-protocol".into(),
            operation: Operation::Increment,
            host: "delegated-node".into(),
            runtime: 3,
            actor_generation: 5,
            grant_issuer: "grant-controller".into(),
            authority_generation: 7,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ClaimedRequest {
    scope: Scope,
    amount: u64,
}

struct VerificationRequest {
    correlation: usize,
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
type VerificationRequests = MessageProtocol<MailAddr, VerificationRequest>;

#[derive(Debug, Error)]
enum VerificationError {
    #[error("invalid test evidence")]
    Invalid(#[source] Option<serde_json::Error>),
    #[error("stale test evidence")]
    Stale,
    #[error("test evidence unavailable")]
    Unavailable(#[source] Option<oneshot::error::RecvError>),
}

mod record_provider {
    use super::{ClaimedRequest, Scope, VerificationError};

    pub struct Receipt {
        claim: ClaimedRequest,
        record: usize,
    }
    impl Receipt {
        pub fn amount(&self) -> u64 {
            self.claim.amount
        }
        pub fn record(&self) -> usize {
            self.record
        }
    }
    pub fn scope(receipt: &Receipt) -> &Scope {
        &receipt.claim.scope
    }

    #[expect(
        clippy::type_complexity,
        reason = "the finite table owns separate proof and protected byte allocations"
    )]
    pub fn verify(
        records: &[(Box<[u8]>, Box<[u8]>)],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<Receipt, VerificationError> {
        match proof {
            b"stale" => return Err(VerificationError::Stale),
            b"unavailable" => return Err(VerificationError::Unavailable(None)),
            _ => {}
        }
        for (record, (known_proof, known_bytes)) in records.iter().enumerate() {
            if known_proof.as_ref() == proof && known_bytes.as_ref() == protected {
                let claim = serde_json::from_slice(protected)
                    .map_err(|cause| VerificationError::Invalid(Some(cause)))?;
                return Ok(Receipt { claim, record });
            }
        }
        Err(VerificationError::Invalid(None))
    }
}

mod scheduled_provider {
    use super::{ClaimedRequest, Scope, VerificationError, oneshot};

    pub struct Receipt {
        query: usize,
        claim: ClaimedRequest,
    }
    impl Receipt {
        pub fn amount(&self) -> u64 {
            self.claim.amount
        }
        pub fn query(&self) -> usize {
            self.query
        }
    }
    pub fn scope(receipt: &Receipt) -> &Scope {
        &receipt.claim.scope
    }

    #[expect(
        clippy::type_complexity,
        reason = "the finite table owns separate proof and protected byte allocations"
    )]
    pub async fn verify(
        records: &[(Box<[u8]>, Box<[u8]>)],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<Receipt, VerificationError> {
        let (publication, receiving) = oneshot::channel();
        if proof == b"unavailable" {
            drop(publication);
        } else {
            let query = records.iter().position(|(known_proof, known_bytes)| {
                known_proof.as_ref() == proof && known_bytes.as_ref() == protected
            });
            let transferred = publication.send(query);
            assert!(
                transferred.is_ok(),
                "the original transaction receiver is owned"
            );
        }
        let query = receiving
            .await
            .map_err(|cause| VerificationError::Unavailable(Some(cause)))?;
        if proof == b"stale" {
            return Err(VerificationError::Stale);
        }
        let Some(query) = query else {
            return Err(VerificationError::Invalid(None));
        };
        let claim = serde_json::from_slice(protected)
            .map_err(|cause| VerificationError::Invalid(Some(cause)))?;
        Ok(Receipt { query, claim })
    }
}

#[derive(Clone, Copy, Debug)]
enum EvidenceCase {
    Accepted,
    Invalid,
    Stale,
    Unavailable,
    MalformedJson,
    RetainedProof,
    Caller,
    Deployment,
    Purpose,
    Controller,
    Actor,
    Protocol,
    Operation,
    Host,
    Runtime,
    ActorGeneration,
    GrantIssuer,
    AuthorityGeneration,
}

const CASES: [EvidenceCase; 18] = [
    EvidenceCase::Accepted,
    EvidenceCase::Invalid,
    EvidenceCase::Stale,
    EvidenceCase::Unavailable,
    EvidenceCase::MalformedJson,
    EvidenceCase::RetainedProof,
    EvidenceCase::Caller,
    EvidenceCase::Deployment,
    EvidenceCase::Purpose,
    EvidenceCase::Controller,
    EvidenceCase::Actor,
    EvidenceCase::Protocol,
    EvidenceCase::Operation,
    EvidenceCase::Host,
    EvidenceCase::Runtime,
    EvidenceCase::ActorGeneration,
    EvidenceCase::GrantIssuer,
    EvidenceCase::AuthorityGeneration,
];

fn request_for(case: EvidenceCase, correlation: usize) -> VerificationRequest {
    let mut scope = Scope::configured();
    match case {
        EvidenceCase::Caller | EvidenceCase::RetainedProof => scope.caller = "mallory".into(),
        EvidenceCase::Deployment => scope.deployment = "other-deployment".into(),
        EvidenceCase::Purpose => scope.purpose = "other-purpose".into(),
        EvidenceCase::Controller => scope.controller = "other-controller".into(),
        EvidenceCase::Actor => scope.actor = "other-actor".into(),
        EvidenceCase::Protocol => scope.protocol = "other-protocol".into(),
        EvidenceCase::Operation => scope.operation = Operation::Read,
        EvidenceCase::Host => scope.host = "other-node".into(),
        EvidenceCase::Runtime => scope.runtime = 4,
        EvidenceCase::ActorGeneration => scope.actor_generation = 6,
        EvidenceCase::GrantIssuer => scope.grant_issuer = "other-grant-controller".into(),
        EvidenceCase::AuthorityGeneration => scope.authority_generation = 8,
        EvidenceCase::Accepted
        | EvidenceCase::Invalid
        | EvidenceCase::Stale
        | EvidenceCase::Unavailable
        | EvidenceCase::MalformedJson => {}
    }
    let protected = match case {
        EvidenceCase::MalformedJson => b"{".to_vec(),
        _ => serde_json::to_vec(&ClaimedRequest { scope, amount: 2 }).unwrap(),
    }
    .into_boxed_slice();
    let proof = match case {
        EvidenceCase::Invalid => b"unknown".to_vec(),
        EvidenceCase::Stale => b"stale".to_vec(),
        EvidenceCase::Unavailable => b"unavailable".to_vec(),
        EvidenceCase::RetainedProof => b"issued-Accepted".to_vec(),
        _ => format!("issued-{case:?}").into_bytes(),
    }
    .into_boxed_slice();
    VerificationRequest {
        correlation,
        protected,
        proof,
    }
}

#[expect(
    clippy::type_complexity,
    reason = "proof and protected bytes are independent original allocations"
)]
fn configured_records() -> Vec<(Box<[u8]>, Box<[u8]>)> {
    CASES
        .into_iter()
        .filter_map(|case| match case {
            EvidenceCase::Invalid
            | EvidenceCase::Stale
            | EvidenceCase::Unavailable
            | EvidenceCase::RetainedProof => None,
            _ => {
                let request = request_for(case, 0);
                Some((request.proof, request.protected))
            }
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScopeMatch {
    Matched,
    Mismatched,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProcessingObservation {
    Accepted,
    ScopeRejected,
    Invalid,
    Stale,
    Unavailable,
}

#[derive(Debug, PartialEq, Eq)]
struct ProcessingReceipt {
    correlation: usize,
    observation: ProcessingObservation,
    value: u64,
}
type ProcessingReplies = MessageProtocol<MailAddr, ProcessingReceipt>;

fn processing_observation<R>(
    evidence: &Result<(R, ScopeMatch), VerificationError>,
) -> ProcessingObservation {
    match evidence {
        Ok((_, ScopeMatch::Matched)) => ProcessingObservation::Accepted,
        Ok((_, ScopeMatch::Mismatched)) => ProcessingObservation::ScopeRejected,
        Err(VerificationError::Invalid(_)) => ProcessingObservation::Invalid,
        Err(VerificationError::Stale) => ProcessingObservation::Stale,
        Err(VerificationError::Unavailable(_)) => ProcessingObservation::Unavailable,
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "completion owns the full distinct provider result beside the original request"
)]
enum CounterCommand {
    RequestVerification(
        VerificationRequest,
        EstablishedRecipient<VerificationRequests>,
    ),
    VerificationCompleted {
        original: VerificationRequest,
        evidence: Result<(record_provider::Receipt, ScopeMatch), VerificationError>,
        reply_to: EstablishedRecipient<ProcessingReplies>,
    },
}
fn begin_counter(
    request: VerificationRequest,
    service: EstablishedRecipient<VerificationRequests>,
) -> CounterCommand {
    CounterCommand::RequestVerification(request, service)
}
fn complete_counter(
    original: VerificationRequest,
    evidence: Result<(record_provider::Receipt, ScopeMatch), VerificationError>,
    reply_to: EstablishedRecipient<ProcessingReplies>,
) -> CounterCommand {
    CounterCommand::VerificationCompleted {
        original,
        evidence,
        reply_to,
    }
}

#[derive(Default)]
struct Counter {
    total: u64,
    requested: Vec<MailAddr>,
    #[expect(
        clippy::type_complexity,
        reason = "retain actual sender, original raw request and complete distinct provider result together"
    )]
    originals: Vec<(
        MailAddr,
        VerificationRequest,
        Result<(record_provider::Receipt, ScopeMatch), VerificationError>,
    )>,
}
#[bombay::actor(sends = pub(crate) {
    verification: Vec<EstablishedDelivery<VerificationRequests>>,
    processing: Vec<EstablishedDelivery<ProcessingReplies>>,
})]
impl Counter {
    fn receive(&mut self, from: MailAddr, command: CounterCommand) -> BehaviorActed<Self> {
        match command {
            CounterCommand::RequestVerification(request, service) => {
                self.requested.push(from);
                Ok(CounterActions::send_verification(
                    Actions::cont(),
                    EstablishedDelivery::new(service, request),
                ))
            }
            CounterCommand::VerificationCompleted {
                original,
                evidence,
                reply_to,
            } => {
                if let Ok((receipt, ScopeMatch::Matched)) = &evidence {
                    self.total += receipt.amount();
                }
                let receipt = ProcessingReceipt {
                    correlation: original.correlation,
                    observation: processing_observation(&evidence),
                    value: self.total,
                };
                self.originals.push((from, original, evidence));
                Ok(CounterActions::send_processing(
                    Actions::cont(),
                    EstablishedDelivery::new(reply_to, receipt),
                ))
            }
        }
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "completion owns the full distinct provider result beside the original request"
)]
enum ArithmeticCommand {
    RequestVerification(
        VerificationRequest,
        EstablishedRecipient<VerificationRequests>,
    ),
    VerificationCompleted {
        original: VerificationRequest,
        evidence: Result<(scheduled_provider::Receipt, ScopeMatch), VerificationError>,
        reply_to: EstablishedRecipient<ProcessingReplies>,
    },
}
fn begin_arithmetic(
    request: VerificationRequest,
    service: EstablishedRecipient<VerificationRequests>,
) -> ArithmeticCommand {
    ArithmeticCommand::RequestVerification(request, service)
}
fn complete_arithmetic(
    original: VerificationRequest,
    evidence: Result<(scheduled_provider::Receipt, ScopeMatch), VerificationError>,
    reply_to: EstablishedRecipient<ProcessingReplies>,
) -> ArithmeticCommand {
    ArithmeticCommand::VerificationCompleted {
        original,
        evidence,
        reply_to,
    }
}
struct Arithmetic {
    product: u64,
    requested: Vec<MailAddr>,
    #[expect(
        clippy::type_complexity,
        reason = "retain actual sender, original raw request and complete distinct provider result together"
    )]
    originals: Vec<(
        MailAddr,
        VerificationRequest,
        Result<(scheduled_provider::Receipt, ScopeMatch), VerificationError>,
    )>,
}
#[bombay::actor(sends = pub(crate) {
    verification: Vec<EstablishedDelivery<VerificationRequests>>,
    processing: Vec<EstablishedDelivery<ProcessingReplies>>,
})]
impl Arithmetic {
    fn receive(&mut self, from: MailAddr, command: ArithmeticCommand) -> BehaviorActed<Self> {
        match command {
            ArithmeticCommand::RequestVerification(request, service) => {
                self.requested.push(from);
                Ok(ArithmeticActions::send_verification(
                    Actions::cont(),
                    EstablishedDelivery::new(service, request),
                ))
            }
            ArithmeticCommand::VerificationCompleted {
                original,
                evidence,
                reply_to,
            } => {
                if let Ok((receipt, ScopeMatch::Matched)) = &evidence {
                    self.product *= receipt.amount();
                }
                let receipt = ProcessingReceipt {
                    correlation: original.correlation,
                    observation: processing_observation(&evidence),
                    value: self.product,
                };
                self.originals.push((from, original, evidence));
                Ok(ArithmeticActions::send_processing(
                    Actions::cont(),
                    EstablishedDelivery::new(reply_to, receipt),
                ))
            }
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "explicit service inputs preserve independent provider, scope, constructor and exact recipient"
)]
#[expect(
    clippy::type_complexity,
    reason = "the ordinary constructor consumes both original request and complete typed provider result"
)]
async fn complete_verification<R, P, V>(
    request: VerificationRequest,
    service: &ExternalActor<VerificationRequests>,
    expected: &Scope,
    verify: V,
    project_scope: fn(&R) -> &Scope,
    complete: fn(
        VerificationRequest,
        Result<(R, ScopeMatch), VerificationError>,
        EstablishedRecipient<ProcessingReplies>,
    ) -> P::Msg,
    emitter: &EstablishedRecipient<P>,
    processing: EstablishedRecipient<ProcessingReplies>,
) -> Result<(), SendError<P::Msg>>
where
    P: Protocol<Addr = MailAddr>,
    P::Msg: Send + 'static,
    V: AsyncFn(&[u8], &[u8]) -> Result<R, VerificationError>,
{
    let evidence = verify(&request.protected, &request.proof).await;
    let evidence = evidence.map(|original| {
        let scope = if project_scope(&original) == expected {
            ScopeMatch::Matched
        } else {
            ScopeMatch::Mismatched
        };
        (original, scope)
    });
    let completion = complete(request, evidence, processing);
    service.send(emitter, completion).await
}

#[expect(
    clippy::too_many_arguments,
    reason = "ordinary typed service configuration remains explicit instead of adding a wrapper"
)]
#[expect(
    clippy::type_complexity,
    reason = "the ordinary constructor consumes both original request and complete typed provider result"
)]
async fn exchange<R, P, V>(
    verifier: &mut ExternalActor<VerificationRequests>,
    processor: &mut ExternalActor<ProcessingReplies>,
    emitter: &EstablishedRecipient<P>,
    address: MailAddr,
    expected: &Scope,
    verify: V,
    projection: fn(&R) -> &Scope,
    begin: fn(VerificationRequest, EstablishedRecipient<VerificationRequests>) -> P::Msg,
    complete: fn(
        VerificationRequest,
        Result<(R, ScopeMatch), VerificationError>,
        EstablishedRecipient<ProcessingReplies>,
    ) -> P::Msg,
) -> Vec<(
    EvidenceCase,
    usize,
    usize,
    User<MailAddr, ProcessingReceipt>,
)>
where
    P: Protocol<Addr = MailAddr>,
    P::Msg: Send + 'static,
    V: AsyncFn(&[u8], &[u8]) -> Result<R, VerificationError>,
{
    let mut observed = Vec::new();
    for (correlation, case) in CASES.into_iter().enumerate() {
        let request = request_for(case, correlation);
        let protected = request.protected.as_ptr() as usize;
        let proof = request.proof.as_ptr() as usize;
        let command = begin(request, verifier.recipient());
        let admitted = processor.send(emitter, command).await;
        admitted.expect("the genuine requestor accepts its command");
        let received = verifier
            .receive()
            .await
            .expect("the Actions request reached the real service");
        assert_eq!(received.from, address);
        assert_eq!(received.message.correlation, correlation);
        assert_eq!(received.message.protected.as_ptr() as usize, protected);
        assert_eq!(received.message.proof.as_ptr() as usize, proof);
        let completed = complete_verification(
            received.message,
            verifier,
            expected,
            &verify,
            projection,
            complete,
            emitter,
            processor.recipient(),
        )
        .await;
        completed.expect("the original typed result reaches its exact requestor");
        let receipt = processor
            .receive()
            .await
            .expect("pure Actions acknowledge processing");
        assert_eq!(receipt.from, address);
        observed.push((case, protected, proof, receipt));
    }
    verifier.close_admission();
    let remaining_requests = verifier.receive().await;
    assert!(
        remaining_requests.is_none(),
        "no duplicate verification request"
    );
    processor.close_admission();
    let remaining_processing = processor.receive().await;
    assert!(
        remaining_processing.is_none(),
        "no duplicate processing observation"
    );
    observed
}

#[derive(Clone, Copy, Debug)]
enum DeploymentMode {
    ExplicitTest,
    ProductionRequired,
}
#[derive(Clone, Copy, Debug)]
enum ProviderProfile {
    Fixture,
}
#[derive(Debug, PartialEq, Eq)]
enum StartupRefusal {
    MissingProvider,
    FakeInProduction,
}
#[derive(Debug, PartialEq, Eq)]
enum StartupObservation {
    ApplicationCreated,
    ExportEstablished,
}

fn authorize_startup(
    mode: DeploymentMode,
    profile: Option<ProviderProfile>,
) -> Result<(), StartupRefusal> {
    match (mode, profile) {
        (_, None) => Err(StartupRefusal::MissingProvider),
        (DeploymentMode::ProductionRequired, Some(ProviderProfile::Fixture)) => {
            Err(StartupRefusal::FakeInProduction)
        }
        (DeploymentMode::ExplicitTest, Some(ProviderProfile::Fixture)) => Ok(()),
    }
}

fn joined_native<B>(
    origin: RootOrigin<B>,
    retirement: ActorRetirement<B, Never, ()>,
    address: MailAddr,
) -> (B, Vec<B::Settlements>)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        behavior,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        completion,
        capability_failures,
        unread_owner_cancellation,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = retirement
    else {
        panic!("the original complete native state remains acquired")
    };
    assert_eq!(origin.address(), address);
    assert_eq!(completion, Completion::Stopped);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants.len(), 0);
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    (behavior, settlements)
}

fn expected_observation(case: EvidenceCase) -> ProcessingObservation {
    match case {
        EvidenceCase::Accepted => ProcessingObservation::Accepted,
        EvidenceCase::Invalid | EvidenceCase::MalformedJson | EvidenceCase::RetainedProof => {
            ProcessingObservation::Invalid
        }
        EvidenceCase::Stale => ProcessingObservation::Stale,
        EvidenceCase::Unavailable => ProcessingObservation::Unavailable,
        EvidenceCase::Caller
        | EvidenceCase::Deployment
        | EvidenceCase::Purpose
        | EvidenceCase::Controller
        | EvidenceCase::Actor
        | EvidenceCase::Protocol
        | EvidenceCase::Operation
        | EvidenceCase::Host
        | EvidenceCase::Runtime
        | EvidenceCase::ActorGeneration
        | EvidenceCase::GrantIssuer
        | EvidenceCase::AuthorityGeneration => ProcessingObservation::ScopeRejected,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "actual startup, Actions, service trace and entire native custody remain one temporal witness"
)]
async fn run_counter(
    mode: DeploymentMode,
    profile: Option<ProviderProfile>,
    counter: Counter,
    startup: &mut Vec<StartupObservation>,
) -> Result<Counter, (StartupRefusal, Counter)> {
    if let Err(refusal) = authorize_startup(mode, profile) {
        return Err((refusal, counter));
    }
    let application = Application::new(counter.stop_on_shutdown());
    startup.push(StartupObservation::ApplicationCreated);
    let records = configured_records();
    let expected = Scope::configured();
    let configured = &records;
    let scope = &expected;
    let received = application
        .run_with::<Never, _, _, _, _, _>(|application| async move {
            let interface = application.interface(());
            let mut verifier = interface.external::<VerificationRequests>().unwrap();
            let mut processor = interface.external::<ProcessingReplies>().unwrap();
            let emitter = application.root().established_recipient();
            startup.push(StartupObservation::ExportEstablished);
            let address = application.root().address();
            let observed = exchange(
                &mut verifier,
                &mut processor,
                &emitter,
                address,
                scope,
                async |protected, proof| record_provider::verify(configured, protected, proof),
                record_provider::scope,
                begin_counter,
                complete_counter,
            )
            .await;
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            (address, verifier.address(), processor.address(), observed)
        })
        .await
        .unwrap_or_else(|_| panic!("the explicit entered host owns the Application"));
    let (
        ApplicationOutcome::Completed {
            output: (address, verifier, processor, observed),
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = received
    else {
        panic!("original Work/native/both notifications are acquired independently")
    };
    let (returned, settlements) = joined_native(origin, native, address);
    let [final_actions] = settlements.as_slice() else {
        panic!("the final retained Stop product")
    };
    assert!(final_actions.creations.is_empty());
    assert!(matches!(final_actions.sends.owned, NoSends));
    assert_eq!(final_actions.sends.inner.verification.len(), 0);
    assert_eq!(final_actions.sends.inner.processing.len(), 0);
    assert!(matches!(final_actions.become_, Step::Stop(_)));
    let returned = returned.into_inner();
    assert_eq!(returned.requested.len(), CASES.len());
    assert!(returned.requested.iter().all(|&from| from == processor));
    assert_eq!(returned.originals.len(), CASES.len());
    assert_eq!(observed.len(), CASES.len());
    for (correlation, ((case, protected, proof, processing), (from, original, evidence))) in
        observed.into_iter().zip(&returned.originals).enumerate()
    {
        assert_eq!(original.correlation, correlation);
        assert_eq!(*from, verifier);
        assert_eq!(original.protected.as_ptr() as usize, protected);
        assert_eq!(original.proof.as_ptr() as usize, proof);
        assert_eq!(processing.message.correlation, original.correlation);
        assert_eq!(
            processing.message.observation,
            expected_observation(case),
            "{case:?}: processing preserves verification and scope"
        );
        assert_eq!(
            processing.message.value, 2,
            "{case:?}: rejected evidence cannot change the counter"
        );
        match (case, evidence) {
            (EvidenceCase::Unavailable, Err(VerificationError::Unavailable(None)))
            | (EvidenceCase::Stale, Err(VerificationError::Stale))
            | (
                EvidenceCase::Invalid | EvidenceCase::RetainedProof,
                Err(VerificationError::Invalid(None)),
            ) => {}
            (EvidenceCase::MalformedJson, Err(VerificationError::Invalid(Some(cause)))) => {
                assert!(cause.is_eof());
                assert_eq!(cause.line(), 1);
                assert_eq!(cause.column(), 1);
            }
            (EvidenceCase::Accepted, Ok((receipt, ScopeMatch::Matched))) => {
                assert_eq!(record_provider::scope(receipt), &expected);
                assert_eq!(records[receipt.record()].0, original.proof);
                assert_eq!(records[receipt.record()].1, original.protected);
            }
            (_, Ok((receipt, ScopeMatch::Mismatched))) => {
                assert_ne!(record_provider::scope(receipt), &expected);
                assert_eq!(records[receipt.record()].0, original.proof);
                assert_eq!(records[receipt.record()].1, original.protected);
            }
            _ => panic!("{case:?}: the original owning evidence and cause stay exact"),
        }
    }
    assert_eq!(
        returned.total, 2,
        "only matching verified evidence changes the counter"
    );
    Ok(returned)
}

#[test]
fn record_service_preserves_typed_verification_and_processing() {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let mut startup = Vec::new();
    let received = host.block_on(run_counter(
        DeploymentMode::ExplicitTest,
        Some(ProviderProfile::Fixture),
        Counter::default(),
        &mut startup,
    ));
    let Ok(original) = received else {
        panic!("the explicit test provider starts")
    };
    assert_eq!(
        startup,
        [
            StartupObservation::ApplicationCreated,
            StartupObservation::ExportEstablished
        ]
    );
    drop(original);
}

#[expect(
    clippy::too_many_lines,
    reason = "actual startup, Actions, service trace and entire native custody remain one temporal witness"
)]
async fn run_arithmetic(
    mode: DeploymentMode,
    profile: Option<ProviderProfile>,
    arithmetic: Arithmetic,
    startup: &mut Vec<StartupObservation>,
) -> Result<Arithmetic, (StartupRefusal, Arithmetic)> {
    if let Err(refusal) = authorize_startup(mode, profile) {
        return Err((refusal, arithmetic));
    }
    let application = Application::new(arithmetic.stop_on_shutdown());
    startup.push(StartupObservation::ApplicationCreated);
    let records = configured_records();
    let expected = Scope::configured();
    let configured = &records;
    let scope = &expected;
    let received = application
        .run_with::<Never, _, _, _, _, _>(|application| async move {
            let interface = application.interface(());
            let mut verifier = interface.external::<VerificationRequests>().unwrap();
            let mut processor = interface.external::<ProcessingReplies>().unwrap();
            let emitter = application.root().established_recipient();
            startup.push(StartupObservation::ExportEstablished);
            let address = application.root().address();
            let observed = exchange(
                &mut verifier,
                &mut processor,
                &emitter,
                address,
                scope,
                async |protected, proof| {
                    scheduled_provider::verify(configured, protected, proof).await
                },
                scheduled_provider::scope,
                begin_arithmetic,
                complete_arithmetic,
            )
            .await;
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            (address, verifier.address(), processor.address(), observed)
        })
        .await
        .unwrap_or_else(|_| panic!("the explicit entered host owns the Application"));
    let (
        ApplicationOutcome::Completed {
            output: (address, verifier, processor, observed),
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = received
    else {
        panic!("original Work/native/both notifications are acquired independently")
    };
    let (returned, settlements) = joined_native(origin, native, address);
    let [final_actions] = settlements.as_slice() else {
        panic!("the final retained Stop product")
    };
    assert!(final_actions.creations.is_empty());
    assert!(matches!(final_actions.sends.owned, NoSends));
    assert_eq!(final_actions.sends.inner.verification.len(), 0);
    assert_eq!(final_actions.sends.inner.processing.len(), 0);
    assert!(matches!(final_actions.become_, Step::Stop(_)));
    let returned = returned.into_inner();
    assert_eq!(returned.requested.len(), CASES.len());
    assert!(returned.requested.iter().all(|&from| from == processor));
    assert_eq!(returned.originals.len(), CASES.len());
    assert_eq!(observed.len(), CASES.len());
    for (correlation, ((case, protected, proof, processing), (from, original, evidence))) in
        observed.into_iter().zip(&returned.originals).enumerate()
    {
        assert_eq!(original.correlation, correlation);
        assert_eq!(*from, verifier);
        assert_eq!(original.protected.as_ptr() as usize, protected);
        assert_eq!(original.proof.as_ptr() as usize, proof);
        assert_eq!(processing.message.correlation, original.correlation);
        assert_eq!(
            processing.message.observation,
            expected_observation(case),
            "{case:?}: processing preserves verification and scope"
        );
        assert_eq!(
            processing.message.value, 6,
            "{case:?}: rejected evidence cannot change the arithmetic"
        );
        match (case, evidence) {
            (EvidenceCase::Unavailable, Err(VerificationError::Unavailable(Some(_))))
            | (EvidenceCase::Stale, Err(VerificationError::Stale))
            | (
                EvidenceCase::Invalid | EvidenceCase::RetainedProof,
                Err(VerificationError::Invalid(None)),
            ) => {}
            (EvidenceCase::MalformedJson, Err(VerificationError::Invalid(Some(cause)))) => {
                assert!(cause.is_eof());
                assert_eq!(cause.line(), 1);
                assert_eq!(cause.column(), 1);
            }
            (EvidenceCase::Accepted, Ok((receipt, ScopeMatch::Matched))) => {
                assert_eq!(scheduled_provider::scope(receipt), &expected);
                assert_eq!(records[receipt.query()].0, original.proof);
                assert_eq!(records[receipt.query()].1, original.protected);
            }
            (_, Ok((receipt, ScopeMatch::Mismatched))) => {
                assert_ne!(scheduled_provider::scope(receipt), &expected);
                assert_eq!(records[receipt.query()].0, original.proof);
                assert_eq!(records[receipt.query()].1, original.protected);
            }
            _ => panic!("{case:?}: the original owning evidence and cause stay exact"),
        }
    }
    assert_eq!(
        returned.product, 6,
        "only matching verified evidence changes the arithmetic"
    );
    Ok(returned)
}

#[test]
fn scheduled_service_preserves_typed_verification_and_processing() {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let mut startup = Vec::new();
    let arithmetic = Arithmetic {
        product: 3,
        requested: Vec::new(),
        originals: Vec::new(),
    };
    let received = host.block_on(run_arithmetic(
        DeploymentMode::ExplicitTest,
        Some(ProviderProfile::Fixture),
        arithmetic,
        &mut startup,
    ));
    let Ok(original) = received else {
        panic!("the explicit test provider starts")
    };
    assert_eq!(
        startup,
        [
            StartupObservation::ApplicationCreated,
            StartupObservation::ExportEstablished
        ]
    );
    drop(original);
}

#[test]
fn production_refuses_fake_or_missing_provider_before_application_export() {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    for (mode, profile, expected) in [
        (
            DeploymentMode::ProductionRequired,
            Some(ProviderProfile::Fixture),
            StartupRefusal::FakeInProduction,
        ),
        (
            DeploymentMode::ProductionRequired,
            None,
            StartupRefusal::MissingProvider,
        ),
        (
            DeploymentMode::ExplicitTest,
            None,
            StartupRefusal::MissingProvider,
        ),
    ] {
        let cold = Counter {
            total: 0,
            requested: Vec::with_capacity(3),
            originals: Vec::new(),
        };
        let original_allocation = cold.requested.as_ptr();
        let mut startup = Vec::new();
        let received = host.block_on(run_counter(mode, profile, cold, &mut startup));
        assert!(
            startup.is_empty(),
            "startup refusal precedes actual Application creation and export"
        );
        let Err((refusal, original)) = received else {
            panic!("production has no fixture fallback")
        };
        assert_eq!(refusal, expected);
        assert_eq!(original.total, 0);
        assert_eq!(original.requested.as_ptr(), original_allocation);
        assert_eq!(original.requested.len(), 0);
        assert_eq!(original.originals.len(), 0);
        drop(original);
    }
}
