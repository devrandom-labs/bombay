//! Counter requires an explicit local test profile; production identity is not established.
//! The optional protected-transport test fixture crosses configured TLS Sessions.
//! Services run beside Work; received facts stay in caller-owned storage.
//! Finite byte conventions/limits are test inputs, not production defaults.
//! No production KERI identity, durability, fresh global issuer or whole-heap bound.

#[cfg(test)]
use alloc_count::AllocCounter;
use bombay::behavior::{
    Behavior, BehaviorSettlements, ClassifySettlement, Here, Ingress, MessageProtocol,
    SettlementStatus,
};
use bombay::prelude::*;
use bombay::{
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, ActorRetirementReport,
    ApplicationLifecycle, InstalledActor, RetirementAssessment, TrySendError, WorkOutcome,
};
use core::ops::AsyncFn;
use futures_util::FutureExt;
use futures_util::future::maybe_done;
use serde_json::error::Category;
use std::collections::TryReserveError;
use std::env;
use std::error::Error;
use std::fmt;
#[cfg(test)]
use std::future::pending;
use std::future::{Future, poll_fn};
use std::mem::{replace, size_of};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
#[cfg(test)]
use std::panic::resume_unwind;
use std::pin::pin;
#[cfg(test)]
use std::ptr;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::{
    runtime::{Builder, Handle, Id, Runtime, TryCurrentError},
    sync::oneshot,
};

#[cfg(all(test, feature = "protected-transport"))]
#[path = "transport.rs"]
mod remote_counter;

#[path = "protected/mod.rs"]
mod protected;

#[path = "../../../../examples/counter/src/counter.rs"]
mod counter;
use counter::{Counter, CounterMessage, CounterProcessed, CounterSends, CounterValue};

struct CounterRequest {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterOperation {
    Increment,
    Read,
    Stop,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Binding {
    deployment: &'static str,
    caller: &'static str,
    controller: &'static str,
    actor: &'static str,
    host: &'static str,
    protocol: &'static str,
    runtime_generation: u64,
    actor_generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity {
    binding: Binding,
    sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Scope {
    identity: Identity,
    operation: CounterOperation,
    grant_issuer: &'static str,
    authority_generation: u64,
}
#[derive(Clone, Copy)]
enum Permission {
    Granted,
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(test)]
enum CounterWorkCase {
    Healthy,
    CounterBeforePublication,
    CounterAfterPublication,
    CounterWhileProcessingPending,
    RequesterBeforePublication,
    RequesterAfterPublication,
    RequesterCompletedAfterPublication,
    RequesterAfterReplyBytes,
    RequesterAfterReplyVerification,
    RequesterAfterReplySend,
    ConstructorRefusal,
}
#[derive(Clone, Copy)]
enum StartupMode {
    ExplicitTest,
    ProductionRequired,
}
#[derive(Debug)]
enum StartupRefusal {
    TestProvider,
    MissingAssignment,
    Exhausted,
    Allocation(TryReserveError),
    Accounting,
    Grant,
    RuntimeUnavailable(TryCurrentError),
    RuntimeMismatch { expected: Id, entered: Id },
}
impl fmt::Display for StartupRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TestProvider => {
                formatter.write_str("production requires an established provider")
            }
            Self::MissingAssignment => formatter.write_str("the actor assignment is missing"),
            Self::Exhausted => formatter.write_str("the actor assignment sequence is exhausted"),
            Self::Allocation(cause) => {
                write!(formatter, "Counter storage reservation failed: {cause}")
            }
            Self::Accounting => {
                formatter.write_str("Counter resource accounting cannot be established")
            }
            Self::Grant => formatter.write_str("the configured Counter grant is invalid"),
            Self::RuntimeUnavailable(cause) => {
                write!(formatter, "Counter host is unavailable: {cause}")
            }
            Self::RuntimeMismatch { expected, entered } => write!(
                formatter,
                "Counter assignment belongs to runtime {expected}, not entered runtime {entered}"
            ),
        }
    }
}
impl Error for StartupRefusal {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Allocation(cause) => Some(cause),
            Self::RuntimeUnavailable(cause) => Some(cause),
            Self::TestProvider
            | Self::MissingAssignment
            | Self::Exhausted
            | Self::Accounting
            | Self::Grant
            | Self::RuntimeMismatch { .. } => None,
        }
    }
}
#[derive(Clone, Copy)]
enum AuthorityFreshness {
    Current,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the healthy executable selects current records; both owning provider consumers exercise stale authority"
        )
    )]
    Stale,
}
#[derive(Clone, Copy)]
enum ProviderAvailability {
    Available,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the healthy executable selects available providers; both owning consumers acquire genuine receipt closure"
        )
    )]
    Unavailable,
}
#[derive(thiserror::Error)]
enum VerificationError {
    #[error("malformed protected Counter JSON: {category}", category = match .0 {
        protected::DecodeError::Declaration(original) | protected::DecodeError::Body(original) => match original.classify() {
            Category::Io => "I/O", Category::Syntax => "syntax",
            Category::Data => "schema", Category::Eof => "incomplete",
        },
        _ => "bounded declaration or lexical refusal",
    })]
    Malformed(protected::DecodeError),
    #[error("invalid protected Counter proof or binding")]
    Invalid,
    #[error("stale protected Counter authority")]
    Stale,
    #[error("protected Counter verification result was unavailable: {0}")]
    Unavailable(oneshot::error::RecvError),
}
impl fmt::Debug for VerificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterRefusal {
    Invalid,
    Stale,
    Unavailable,
    Binding,
    Grant,
    Conflict,
    RetentionFull,
    Revoked,
    Expired,
    Count,
    IndividualBytes,
    TotalBytes,
    Accounting,
    Exhausted,
}
// Trusted finite authority survives the actual local Application executions.
// It proves only this explicit profile contract; no cold restart durability.
struct AssignmentAuthority<'host> {
    host: &'host Runtime,
    runtime_generation: u64,
    grant_issuer: &'static str,
    authority_generation: u64,
    permitted_operations: &'static [CounterOperation],
    next: Option<u64>,
}
impl AssignmentAuthority<'_> {
    fn check_host(&self) -> Result<(), StartupRefusal> {
        let entered = Handle::try_current().map_err(StartupRefusal::RuntimeUnavailable)?;
        let expected = self.host.handle().id();
        if entered.id() != expected {
            return Err(StartupRefusal::RuntimeMismatch {
                expected,
                entered: entered.id(),
            });
        }
        Ok(())
    }
    fn issue(&mut self) -> Result<(Binding, Instant), StartupRefusal> {
        self.check_host()?;
        if self.grant_issuer.is_empty() || self.permitted_operations.is_empty() {
            return Err(StartupRefusal::Grant);
        }
        let Some(actor) = self.next else {
            return Err(StartupRefusal::Exhausted);
        };
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(60))
            .ok_or(StartupRefusal::Accounting)?;
        self.next = actor.checked_add(1);
        Ok((
            Binding {
                deployment: "test-deployment",
                caller: "caller",
                controller: "controller",
                actor: "counter",
                host: "counter-host",
                protocol: "counter-protocol",
                runtime_generation: self.runtime_generation,
                actor_generation: actor,
            },
            deadline,
        ))
    }
}
struct CounterLimits {
    incoming: NonZeroUsize,
    retained: NonZeroUsize,
    individual: NonZeroUsize,
    total: NonZeroUsize,
    verification_maximum: usize,
}
enum CounterStopAuthority {
    Root(ApplicationLifecycle<Counter, <StopOnShutdown<Counter> as Behavior>::Event>),
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The root executable retains the same native child capability exercised by the owning InstalledActor Application tests."
        )
    )]
    Child(InstalledActor<StopOnShutdown<Counter>>),
}
impl CounterStopAuthority {
    fn request_shutdown(&self) -> Result<(), ShutdownRejection> {
        match self {
            Self::Root(root) => root.request_shutdown(),
            Self::Child(child) => child.request_shutdown(Ingress::<_, Here>::new()),
        }
    }
    async fn retirement(&self) -> ActorRetirementReport {
        match self {
            Self::Root(root) => root.retirement().await,
            Self::Child(child) => child.retirement().await,
        }
    }
}
enum CounterCommand {
    Message(CounterMessage),
    Stop,
}
enum CounterDelivery {
    Ready(CounterCommand),
    Full(CounterMessage),
    PossiblyAdmitted,
    IncrementAdmitted,
    ReadAdmitted,
    Closed(TrySendError<CounterMessage>),
    StopRequested(Result<(), ShutdownRejection>),
    Refused {
        command: CounterCommand,
        refusal: CounterRefusal,
    },
}
enum Verification<R> {
    Awaiting,
    Acquired(Result<R, VerificationError>),
    Native {
        receipt: R,
        delivery: CounterDelivery,
    },
    Refused {
        evidence: Result<R, VerificationError>,
        refusal: CounterRefusal,
    },
    Duplicate {
        receipt: R,
        report: CounterReport,
    },
}
struct PendingAdmission<R> {
    correlation: u64,
    reservation_bytes: usize,
    original: CounterRequest,
    verification: Verification<R>,
}
enum CounterReplayRetention {
    Pending(u64),
    Consumed(CounterReport),
}
struct CounterReplayRecord {
    identity: Identity,
    protected: Box<[u8]>,
    retention: CounterReplayRetention,
}
// One incoming vector and one live replay vector; no per-request observation.
struct CounterAuthorization<R> {
    binding: Binding,
    grant_issuer: &'static str,
    authority_generation: u64,
    permitted_operations: &'static [CounterOperation],
    projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
    profile: CounterProofProfile,
    deadline: Instant,
    target: EstablishedRecipient<Counter>,
    stop: Option<CounterStopAuthority>,
    processing: Option<EstablishedRecipient<CounterProcessed>>,
    permission: Permission,
    limits: CounterLimits,
    pending: Vec<PendingAdmission<R>>,
    retained: Vec<CounterReplayRecord>,
    reserved_bytes: usize,
    next_correlation: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterStatus {
    IncrementAdmitted,
    ReadAdmitted,
    StopAccepted,
    StopRejected(ShutdownRejection),
    PossiblyAdmitted,
    Closed,
    Refused(CounterRefusal),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CounterReport {
    correlation: u64,
    status: CounterStatus,
}
// Full products never become actor messages; only the outside caller consumes.
struct CounterAdmission<R> {
    original: CounterRequest,
    verification: Verification<R>,
}
impl<R> CounterAuthorization<R> {
    #[expect(
        clippy::too_many_arguments,
        reason = "Existing binding, independent grant, receipt projection, receiver profile and resource inputs remain explicit."
    )]
    fn new(
        mode: StartupMode,
        assignment: Option<(Binding, Instant, EstablishedRecipient<Counter>)>,
        grant_issuer: &'static str,
        authority_generation: u64,
        permitted_operations: &'static [CounterOperation],
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
        limits: CounterLimits,
    ) -> Result<Self, StartupRefusal> {
        if let StartupMode::ProductionRequired = mode {
            return Err(StartupRefusal::TestProvider);
        }
        let Some((binding, deadline, target)) = assignment else {
            return Err(StartupRefusal::MissingAssignment);
        };
        if grant_issuer.is_empty() || permitted_operations.is_empty() {
            return Err(StartupRefusal::Grant);
        }
        let mut pending = Vec::new();
        let mut retained = Vec::new();
        pending
            .try_reserve_exact(limits.incoming.get())
            .map_err(StartupRefusal::Allocation)?;
        retained
            .try_reserve_exact(limits.retained.get())
            .map_err(StartupRefusal::Allocation)?;
        let reserved_bytes = pending
            .capacity()
            .checked_mul(size_of::<PendingAdmission<R>>())
            .and_then(|bytes| {
                retained
                    .capacity()
                    .checked_mul(size_of::<CounterReplayRecord>())
                    .and_then(|retained| bytes.checked_add(retained))
            })
            .ok_or(StartupRefusal::Accounting)?;
        if reserved_bytes > limits.total.get() {
            return Err(StartupRefusal::Accounting);
        }
        Ok(Self {
            binding,
            grant_issuer,
            authority_generation,
            permitted_operations,
            projection,
            profile,
            deadline,
            target,
            stop: None,
            processing: None,
            permission: Permission::Granted,
            limits,
            pending,
            retained,
            reserved_bytes,
            next_correlation: Some(0),
        })
    }
    fn reserve(
        &mut self,
        original: CounterRequest,
    ) -> Result<u64, (CounterRefusal, CounterRequest)> {
        let refusal = if self.pending.len() >= self.limits.incoming.get() {
            Some(CounterRefusal::Count)
        } else {
            None
        };
        let charge = original
            .protected
            .len()
            .checked_add(original.proof.len())
            .and_then(|bytes| bytes.checked_add(self.limits.verification_maximum));
        let refusal = refusal.or_else(|| match charge {
            None => Some(CounterRefusal::Accounting),
            Some(bytes) if bytes > self.limits.individual.get() => {
                Some(CounterRefusal::IndividualBytes)
            }
            Some(bytes) => match self.reserved_bytes.checked_add(bytes) {
                None => Some(CounterRefusal::Accounting),
                Some(total) if total > self.limits.total.get() => Some(CounterRefusal::TotalBytes),
                Some(_) => None,
            },
        });
        if let Some(refusal) = refusal {
            return Err((refusal, original));
        }
        let Some(correlation) = self.next_correlation else {
            return Err((CounterRefusal::Exhausted, original));
        };
        let reservation_bytes = charge.expect("checked before allocation/provider polling");
        self.next_correlation = correlation.checked_add(1);
        self.reserved_bytes += reservation_bytes;
        self.pending.push(PendingAdmission {
            correlation,
            reservation_bytes,
            original,
            verification: Verification::Awaiting,
        });
        Ok(correlation)
    }
    #[expect(
        clippy::too_many_lines,
        reason = "One admission association preserves original receipt, replay facts and native ownership before disposition."
    )]
    fn associate(
        &mut self,
        position: usize,
        service: &ExternalActor<CounterRequests>,
        replies: &EstablishedRecipient<CounterValue>,
    ) {
        let Verification::Acquired(evidence) = &self.pending[position].verification else {
            return;
        };
        let verified = match evidence {
            Ok(receipt) => self.request_scope(receipt),
            Err(VerificationError::Invalid | VerificationError::Malformed(_)) => {
                Err(CounterRefusal::Invalid)
            }
            Err(VerificationError::Stale) => Err(CounterRefusal::Stale),
            Err(VerificationError::Unavailable(_)) => Err(CounterRefusal::Unavailable),
        };
        let (identity, operation) = match verified {
            Err(refusal) => {
                self.refuse(position, refusal);
                return;
            }
            Ok(scope) => (scope.identity, scope.operation),
        };
        let found = self
            .retained
            .binary_search_by_key(&identity.sequence, |entry| entry.identity.sequence);
        if let Ok(retained_position) = found {
            if self.retained[retained_position].protected.as_ref()
                != self.pending[position].original.protected.as_ref()
            {
                self.refuse(position, CounterRefusal::Conflict);
                return;
            }
            let report = match self.retained[retained_position].retention {
                CounterReplayRetention::Pending(correlation) => {
                    self.attempt(correlation, service);
                    self.report(correlation)
                }
                CounterReplayRetention::Consumed(report) => Some(report),
            };
            let Some(mut report) = report else {
                return;
            }; // known native Full
            report.correlation = self.pending[position].correlation;
            let prior = replace(
                &mut self.pending[position].verification,
                Verification::Awaiting,
            );
            let Verification::Acquired(Ok(receipt)) = prior else {
                unreachable!("borrowed actual receipt");
            };
            self.pending[position].verification = Verification::Duplicate { receipt, report };
            return;
        }
        if let Some(refusal) = self.command_refusal(operation) {
            self.refuse(position, refusal);
            return;
        }
        if self.retained.len() >= self.limits.retained.get() {
            self.refuse(position, CounterRefusal::RetentionFull);
            return;
        }
        let comparison_bytes = self.pending[position].original.protected.len();
        let Some(total) = self.reserved_bytes.checked_add(comparison_bytes) else {
            self.refuse(position, CounterRefusal::Accounting);
            return;
        };
        if total > self.limits.total.get() {
            self.refuse(position, CounterRefusal::TotalBytes);
            return;
        }
        let protected = Box::from(self.pending[position].original.protected.as_ref());
        let correlation = self.pending[position].correlation;
        self.reserved_bytes = total;
        let Verification::Acquired(Ok(receipt)) = replace(
            &mut self.pending[position].verification,
            Verification::Awaiting,
        ) else {
            unreachable!("actual original receipt");
        };
        let command = match operation {
            CounterOperation::Increment => CounterCommand::Message(match &self.processing {
                Some(reply_to) => CounterMessage::IncrementAndReply {
                    request_sequence: identity.sequence,
                    reply_to: reply_to.clone(),
                },
                None => CounterMessage::Increment,
            }),
            CounterOperation::Read => {
                CounterCommand::Message(CounterMessage::Read(replies.clone()))
            }
            CounterOperation::Stop => CounterCommand::Stop,
        };
        self.pending[position].verification = Verification::Native {
            receipt,
            delivery: CounterDelivery::Ready(command),
        };
        let Err(position) = found else {
            unreachable!("new identity");
        };
        // Complete original-bearing entry exists BEFORE any native external call.
        self.retained.insert(
            position,
            CounterReplayRecord {
                identity,
                protected,
                retention: CounterReplayRetention::Pending(correlation),
            },
        );
    }
    fn refuse(&mut self, position: usize, refusal: CounterRefusal) {
        let prior = replace(
            &mut self.pending[position].verification,
            Verification::Awaiting,
        );
        match prior {
            Verification::Acquired(evidence) => {
                self.pending[position].verification = Verification::Refused { evidence, refusal }
            }
            retained => self.pending[position].verification = retained,
        }
    }
    fn request_scope<'receipt>(
        &self,
        receipt: &'receipt R,
    ) -> Result<&'receipt Scope, CounterRefusal> {
        let (scope, profile, purpose) = (self.projection)(receipt);
        if profile != self.profile || purpose != CounterProofPurpose::Request {
            return Err(CounterRefusal::Invalid);
        }
        if scope.identity.binding != self.binding {
            return Err(CounterRefusal::Binding);
        }
        if scope.grant_issuer != self.grant_issuer
            || scope.authority_generation != self.authority_generation
        {
            return Err(CounterRefusal::Grant);
        }
        Ok(scope)
    }
    fn command_refusal(&self, operation: CounterOperation) -> Option<CounterRefusal> {
        if !self.permitted_operations.contains(&operation)
            || (operation == CounterOperation::Stop && self.stop.is_none())
        {
            Some(CounterRefusal::Grant)
        } else {
            None
        }
    }
    fn attempt(&mut self, correlation: u64, service: &ExternalActor<CounterRequests>) {
        self.attempt_at(correlation, Instant::now(), service);
    }
    // Controlled equality/Full tests can call this; the real path samples above.
    fn attempt_at(
        &mut self,
        correlation: u64,
        now: Instant,
        service: &ExternalActor<CounterRequests>,
    ) {
        let scope_refusal = self
            .pending
            .iter()
            .find(|entry| entry.correlation == correlation)
            .and_then(|pending| match &pending.verification {
                Verification::Native { receipt, .. } => match self.request_scope(receipt) {
                    Ok(scope) => self.command_refusal(scope.operation),
                    Err(refusal) => Some(refusal),
                },
                _ => None,
            });
        let Some(pending) = self
            .pending
            .iter_mut()
            .find(|entry| entry.correlation == correlation)
        else {
            return;
        };
        let Verification::Native { delivery, .. } = &mut pending.verification else {
            return;
        };
        let refusal = scope_refusal.or_else(|| match self.permission {
            Permission::Revoked => Some(CounterRefusal::Revoked),
            Permission::Granted if now >= self.deadline => Some(CounterRefusal::Expired),
            Permission::Granted => None,
        });
        let prior = replace(delivery, CounterDelivery::PossiblyAdmitted);
        *delivery = match prior {
            CounterDelivery::Ready(CounterCommand::Message(command))
            | CounterDelivery::Full(command) => {
                if let Some(refusal) = refusal {
                    CounterDelivery::Refused {
                        command: CounterCommand::Message(command),
                        refusal,
                    }
                } else {
                    let admitted = match &command {
                        CounterMessage::Increment | CounterMessage::IncrementAndReply { .. } => {
                            CounterDelivery::IncrementAdmitted
                        }
                        CounterMessage::Read(_) => CounterDelivery::ReadAdmitted,
                    };
                    match service.try_send(&self.target, command) {
                        Ok(()) => admitted,
                        Err(TrySendError::Full(command)) => CounterDelivery::Full(command),
                        Err(error @ TrySendError::Closed(_)) => CounterDelivery::Closed(error),
                    }
                }
            }
            CounterDelivery::Ready(CounterCommand::Stop) => match refusal {
                Some(refusal) => CounterDelivery::Refused {
                    command: CounterCommand::Stop,
                    refusal,
                },
                None => match &self.stop {
                    Some(stop) => CounterDelivery::StopRequested(stop.request_shutdown()),
                    None => CounterDelivery::Refused {
                        command: CounterCommand::Stop,
                        refusal: CounterRefusal::Grant,
                    },
                },
            },
            settled @ (CounterDelivery::IncrementAdmitted
            | CounterDelivery::ReadAdmitted
            | CounterDelivery::PossiblyAdmitted
            | CounterDelivery::Closed(_)
            | CounterDelivery::StopRequested(_)
            | CounterDelivery::Refused { .. }) => settled,
        };
    }
    fn report(&self, correlation: u64) -> Option<CounterReport> {
        let pending = self
            .pending
            .iter()
            .find(|entry| entry.correlation == correlation)?;
        let status = match &pending.verification {
            Verification::Native {
                delivery: CounterDelivery::IncrementAdmitted,
                ..
            } => CounterStatus::IncrementAdmitted,
            Verification::Native {
                delivery: CounterDelivery::ReadAdmitted,
                ..
            } => CounterStatus::ReadAdmitted,
            Verification::Native {
                delivery: CounterDelivery::StopRequested(result),
                ..
            } => match result {
                Ok(()) => CounterStatus::StopAccepted,
                Err(original) => CounterStatus::StopRejected(*original),
            },
            Verification::Native {
                delivery: CounterDelivery::PossiblyAdmitted,
                ..
            } => CounterStatus::PossiblyAdmitted,
            Verification::Native {
                delivery: CounterDelivery::Closed(_),
                ..
            } => CounterStatus::Closed,
            Verification::Native {
                delivery: CounterDelivery::Refused { refusal, .. },
                ..
            }
            | Verification::Refused { refusal, .. } => CounterStatus::Refused(*refusal),
            Verification::Duplicate { report, .. } => return Some(*report),
            Verification::Awaiting
            | Verification::Acquired(_)
            | Verification::Native {
                delivery: CounterDelivery::Ready(_) | CounterDelivery::Full(_),
                ..
            } => return None,
        };
        Some(CounterReport {
            correlation,
            status,
        })
    }
    // Only the outside caller consumes; an actor acknowledgement cannot release.
    fn consume(&mut self, correlation: u64) -> Option<CounterAdmission<R>> {
        let report = self.report(correlation)?;
        let position = self
            .pending
            .iter()
            .position(|entry| entry.correlation == correlation)?;
        let pending = self.pending.remove(position);
        self.reserved_bytes -= pending.reservation_bytes;
        for retained in &mut self.retained {
            if matches!(retained.retention, CounterReplayRetention::Pending(expected) if expected == correlation)
            {
                retained.retention = CounterReplayRetention::Consumed(report);
            }
        }
        Some(CounterAdmission {
            original: pending.original,
            verification: pending.verification,
        })
    }
}

// Borrow only the entry: the same owner can still observe permission updates.
// One actual provider call owns its original release future/cause; no mutex.
async fn acquire_verification<'request, R, F>(
    pending: &'request mut PendingAdmission<R>,
    provider: impl FnOnce(&'request CounterRequest) -> F,
) where
    F: Future<Output = Result<R, VerificationError>>,
{
    if matches!(pending.verification, Verification::Awaiting) {
        let (original, verification) = (&pending.original, &mut pending.verification);
        let mut provider_verification = pin!(provider(original));
        // Commit the returned verification before disposing its actual producer.
        poll_fn(
            |context| match provider_verification.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(acquired) => {
                    *verification = Verification::Acquired(acquired);
                    Poll::Ready(())
                }
            },
        )
        .await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterProofProfile {
    RecordedTest,
    ScheduledTest,
}
impl CounterProofProfile {
    fn name(self) -> &'static str {
        match self {
            Self::RecordedTest => "counter-record-test-v1",
            Self::ScheduledTest => "counter-scheduled-test-v1",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterProofPurpose {
    Request,
    Refused,
    MailboxAdmitted,
    IncrementProcessed,
    ReadReply,
    StopAccepted,
    RetirementReport,
}
impl CounterProofPurpose {
    fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Refused => "refused",
            Self::MailboxAdmitted => "mailbox_admitted",
            Self::IncrementProcessed => "increment_processed",
            Self::ReadReply => "read_reply",
            Self::StopAccepted => "stop_accepted",
            Self::RetirementReport => "retirement_report",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterProtectedMeaning {
    Request,
    Refused(CounterRefusal),
    IncrementAdmitted,
    ReadAdmitted,
    IncrementProcessed,
    ReadReply(u64),
    StopAccepted,
    RetirementReport {
        retirement: RetirementAssessment,
        failures: ActorFailureAssessment,
    },
}
impl CounterProtectedMeaning {
    fn purpose(self) -> CounterProofPurpose {
        match self {
            Self::Request => CounterProofPurpose::Request,
            Self::Refused(_) => CounterProofPurpose::Refused,
            Self::IncrementAdmitted | Self::ReadAdmitted => CounterProofPurpose::MailboxAdmitted,
            Self::IncrementProcessed => CounterProofPurpose::IncrementProcessed,
            Self::ReadReply(_) => CounterProofPurpose::ReadReply,
            Self::StopAccepted => CounterProofPurpose::StopAccepted,
            Self::RetirementReport { .. } => CounterProofPurpose::RetirementReport,
        }
    }
}
#[derive(Clone, PartialEq, Eq)]
struct ProtectedRecord {
    scope: Scope,
    profile: CounterProofProfile,
    purpose: CounterProofPurpose,
    producer: &'static str,
    intended_consumer: &'static str,
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
fn counter_operation_name(operation: CounterOperation) -> &'static str {
    match operation {
        CounterOperation::Increment => "increment",
        CounterOperation::Read => "read",
        CounterOperation::Stop => "stop",
    }
}
fn counter_refusal_name(refusal: CounterRefusal) -> Option<&'static str> {
    match refusal {
        CounterRefusal::Binding => Some("binding"),
        CounterRefusal::Grant => Some("grant"),
        CounterRefusal::Conflict => Some("conflict"),
        CounterRefusal::RetentionFull => Some("retention_full"),
        CounterRefusal::Revoked => Some("revoked"),
        CounterRefusal::Expired => Some("expired"),
        CounterRefusal::Count => Some("count"),
        CounterRefusal::IndividualBytes => Some("individual_bytes"),
        CounterRefusal::TotalBytes => Some("total_bytes"),
        CounterRefusal::Accounting => Some("accounting"),
        CounterRefusal::Exhausted => Some("exhausted"),
        CounterRefusal::Invalid | CounterRefusal::Stale | CounterRefusal::Unavailable => None,
    }
}
#[expect(
    clippy::too_many_lines,
    reason = "Seven approved nominal Serde constructors share declared scalars without a second header, trait or macro."
)]
fn counter_json_message(
    scope: Scope,
    profile: CounterProofProfile,
    meaning: CounterProtectedMeaning,
) -> Option<protected::Message> {
    match (meaning, scope.operation) {
        (CounterProtectedMeaning::Request | CounterProtectedMeaning::Refused(_), _)
        | (
            CounterProtectedMeaning::IncrementAdmitted
            | CounterProtectedMeaning::IncrementProcessed,
            CounterOperation::Increment,
        )
        | (
            CounterProtectedMeaning::ReadAdmitted | CounterProtectedMeaning::ReadReply(_),
            CounterOperation::Read,
        )
        | (
            CounterProtectedMeaning::StopAccepted
            | CounterProtectedMeaning::RetirementReport { .. },
            CounterOperation::Stop,
        ) => {}
        _ => return None,
    }
    let wire_version = 1;
    let schema_version = 1;
    let provider_profile = Box::from(profile.name());
    let kind = Box::from(meaning.purpose().name());
    let binding = scope.identity.binding;
    let deployment = Box::from(binding.deployment);
    let caller = Box::from(binding.caller);
    let controller = Box::from(binding.controller);
    let actor = Box::from(binding.actor);
    let host = Box::from(binding.host);
    let protocol = Box::from(binding.protocol);
    let grant_issuer = Box::from(scope.grant_issuer);
    let runtime_generation = binding.runtime_generation;
    let actor_generation = binding.actor_generation;
    let authority_generation = scope.authority_generation;
    let request_sequence = scope.identity.sequence;
    let operation = Box::from(counter_operation_name(scope.operation));
    Some(match meaning {
        CounterProtectedMeaning::Request => protected::Message::Request(protected::RequestBody {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
        }),
        CounterProtectedMeaning::Refused(refusal) => {
            protected::Message::Refused(protected::Refused {
                wire_version,
                schema_version,
                provider_profile,
                kind,
                deployment,
                caller,
                controller,
                actor,
                host,
                protocol,
                grant_issuer,
                runtime_generation,
                actor_generation,
                authority_generation,
                request_sequence,
                operation,
                reason: Box::from(counter_refusal_name(refusal)?),
            })
        }
        CounterProtectedMeaning::IncrementAdmitted | CounterProtectedMeaning::ReadAdmitted => {
            protected::Message::MailboxAdmitted(protected::MailboxAdmitted {
                wire_version,
                schema_version,
                provider_profile,
                kind,
                deployment,
                caller,
                controller,
                actor,
                host,
                protocol,
                grant_issuer,
                runtime_generation,
                actor_generation,
                authority_generation,
                request_sequence,
                operation,
            })
        }
        CounterProtectedMeaning::IncrementProcessed => {
            protected::Message::IncrementProcessed(protected::IncrementProcessed {
                wire_version,
                schema_version,
                provider_profile,
                kind,
                deployment,
                caller,
                controller,
                actor,
                host,
                protocol,
                grant_issuer,
                runtime_generation,
                actor_generation,
                authority_generation,
                request_sequence,
                operation,
            })
        }
        CounterProtectedMeaning::ReadReply(value) => {
            protected::Message::ReadReply(protected::ReadReply {
                wire_version,
                schema_version,
                provider_profile,
                kind,
                deployment,
                caller,
                controller,
                actor,
                host,
                protocol,
                grant_issuer,
                runtime_generation,
                actor_generation,
                authority_generation,
                request_sequence,
                operation,
                value,
            })
        }
        CounterProtectedMeaning::StopAccepted => {
            protected::Message::StopAccepted(protected::StopAccepted {
                wire_version,
                schema_version,
                provider_profile,
                kind,
                deployment,
                caller,
                controller,
                actor,
                host,
                protocol,
                grant_issuer,
                runtime_generation,
                actor_generation,
                authority_generation,
                request_sequence,
                operation,
            })
        }
        CounterProtectedMeaning::RetirementReport {
            retirement,
            failures,
        } => protected::Message::RetirementReport(protected::RetirementReport {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            retirement: Box::from(match retirement {
                RetirementAssessment::Established => "established",
                RetirementAssessment::NotEstablished => "not_established",
            }),
            failures: Box::from(match failures {
                ActorFailureAssessment::FailuresFound => "failures_found",
                ActorFailureAssessment::NoFailuresFound => "no_failures_found",
                ActorFailureAssessment::Incomplete => "incomplete",
            }),
        }),
    })
}
#[derive(thiserror::Error)]
enum CounterEncodingError {
    #[error("unsupported protected Counter meaning: {purpose}", purpose = .0.purpose().name())]
    UnsupportedMeaning(CounterProtectedMeaning),
    #[error("protected Counter encoding failed: {category:?}", category = .0.classify())]
    Encoding(serde_json::Error),
}
impl fmt::Debug for CounterEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}
fn counter_record(
    scope: Scope,
    profile: CounterProofProfile,
    meaning: CounterProtectedMeaning,
    proof: &[u8],
    buffer: &mut [u8],
) -> Result<ProtectedRecord, CounterEncodingError> {
    let message = counter_json_message(scope, profile, meaning)
        .ok_or(CounterEncodingError::UnsupportedMeaning(meaning))?;
    let length = protected::encode(&message, buffer).map_err(CounterEncodingError::Encoding)?;
    let (producer, intended_consumer) = match meaning.purpose() {
        CounterProofPurpose::Request => {
            (scope.identity.binding.caller, scope.identity.binding.host)
        }
        _ => (scope.identity.binding.host, scope.identity.binding.caller),
    };
    Ok(ProtectedRecord {
        scope,
        profile,
        purpose: meaning.purpose(),
        producer,
        intended_consumer,
        protected: Box::from(&buffer[..length]),
        proof: Box::from(proof),
    })
}
#[expect(
    clippy::too_many_lines,
    reason = "One exhaustive seven-meaning comparison owns all sixteen declared scalar bindings and their original typed meaning."
)]
fn counter_registered_meaning(
    message: &protected::Message,
    record: &ProtectedRecord,
    profile: CounterProofProfile,
    purpose: CounterProofPurpose,
    producer: &str,
    intended_consumer: &str,
) -> Result<CounterProtectedMeaning, VerificationError> {
    let (
        wire_version,
        schema_version,
        provider_profile,
        kind,
        deployment,
        caller,
        controller,
        actor,
        host,
        protocol,
        grant_issuer,
        runtime_generation,
        actor_generation,
        authority_generation,
        request_sequence,
        operation,
    ) = match message {
        protected::Message::Request(protected::RequestBody {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::Refused(protected::Refused {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::MailboxAdmitted(protected::MailboxAdmitted {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::IncrementProcessed(protected::IncrementProcessed {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::ReadReply(protected::ReadReply {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::StopAccepted(protected::StopAccepted {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        })
        | protected::Message::RetirementReport(protected::RetirementReport {
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
            ..
        }) => (
            wire_version,
            schema_version,
            provider_profile,
            kind,
            deployment,
            caller,
            controller,
            actor,
            host,
            protocol,
            grant_issuer,
            runtime_generation,
            actor_generation,
            authority_generation,
            request_sequence,
            operation,
        ),
    };
    let scope = record.scope;
    let binding = scope.identity.binding;
    let (required_producer, required_consumer) = match purpose {
        CounterProofPurpose::Request => (binding.caller, binding.host),
        _ => (binding.host, binding.caller),
    };
    if *wire_version != 1
        || *schema_version != 1
        || provider_profile.as_ref() != profile.name()
        || record.profile != profile
        || kind.as_ref() != purpose.name()
        || record.purpose != purpose
        || record.producer != producer
        || record.intended_consumer != intended_consumer
        || producer != required_producer
        || intended_consumer != required_consumer
        || deployment.as_ref() != binding.deployment
        || caller.as_ref() != binding.caller
        || controller.as_ref() != binding.controller
        || actor.as_ref() != binding.actor
        || host.as_ref() != binding.host
        || protocol.as_ref() != binding.protocol
        || grant_issuer.as_ref() != scope.grant_issuer
        || *runtime_generation != binding.runtime_generation
        || *actor_generation != binding.actor_generation
        || *authority_generation != scope.authority_generation
        || *request_sequence != scope.identity.sequence
        || operation.as_ref() != counter_operation_name(scope.operation)
    {
        return Err(VerificationError::Invalid);
    }
    Ok(match message {
        protected::Message::Request(_) => CounterProtectedMeaning::Request,
        protected::Message::MailboxAdmitted(_) => match scope.operation {
            CounterOperation::Increment => CounterProtectedMeaning::IncrementAdmitted,
            CounterOperation::Read => CounterProtectedMeaning::ReadAdmitted,
            CounterOperation::Stop => return Err(VerificationError::Invalid),
        },
        protected::Message::IncrementProcessed(_) => match scope.operation {
            CounterOperation::Increment => CounterProtectedMeaning::IncrementProcessed,
            _ => return Err(VerificationError::Invalid),
        },
        protected::Message::ReadReply(reply) => match scope.operation {
            CounterOperation::Read => CounterProtectedMeaning::ReadReply(reply.value),
            _ => return Err(VerificationError::Invalid),
        },
        protected::Message::StopAccepted(_) => match scope.operation {
            CounterOperation::Stop => CounterProtectedMeaning::StopAccepted,
            _ => return Err(VerificationError::Invalid),
        },
        protected::Message::Refused(reply) => {
            CounterProtectedMeaning::Refused(match reply.reason.as_ref() {
                "binding" => CounterRefusal::Binding,
                "grant" => CounterRefusal::Grant,
                "conflict" => CounterRefusal::Conflict,
                "retention_full" => CounterRefusal::RetentionFull,
                "revoked" => CounterRefusal::Revoked,
                "expired" => CounterRefusal::Expired,
                "count" => CounterRefusal::Count,
                "individual_bytes" => CounterRefusal::IndividualBytes,
                "total_bytes" => CounterRefusal::TotalBytes,
                "accounting" => CounterRefusal::Accounting,
                "exhausted" => CounterRefusal::Exhausted,
                _ => return Err(VerificationError::Invalid),
            })
        }
        protected::Message::RetirementReport(reply) => {
            if scope.operation != CounterOperation::Stop {
                return Err(VerificationError::Invalid);
            }
            CounterProtectedMeaning::RetirementReport {
                retirement: match reply.retirement.as_ref() {
                    "established" => RetirementAssessment::Established,
                    "not_established" => RetirementAssessment::NotEstablished,
                    _ => return Err(VerificationError::Invalid),
                },
                failures: match reply.failures.as_ref() {
                    "failures_found" => ActorFailureAssessment::FailuresFound,
                    "no_failures_found" => ActorFailureAssessment::NoFailuresFound,
                    "incomplete" => ActorFailureAssessment::Incomplete,
                    _ => return Err(VerificationError::Invalid),
                },
            }
        }
    })
}

// Explicit fixtures for this local test profile; never production defaults.
fn counter_test_json_limits() -> protected::Limits {
    protected::Limits {
        input_bytes: NonZeroUsize::new(8192).expect("explicit input bytes"),
        token_bytes: NonZeroUsize::new(64).expect("explicit token bytes"),
        nesting: NonZeroUsize::new(8).expect("explicit logical nesting"),
        parser_states: NonZeroUsize::new(16).expect("explicit parser states"),
        identifier_bytes: NonZeroUsize::new(64).expect("explicit identifier bytes"),
        proof_bytes: NonZeroUsize::new(64).expect("explicit proof bytes"),
    }
}

mod record_provider {
    use super::{
        AuthorityFreshness, CounterProofProfile, CounterProofPurpose, CounterProtectedMeaning,
        CounterRequest, ProtectedRecord, ProviderAvailability, Scope, VerificationError,
        counter_registered_meaning, oneshot, protected,
    };
    pub(super) struct Receipt {
        scope: Scope,
        profile: CounterProofProfile,
        producer: &'static str,
        intended_consumer: &'static str,
        meaning: CounterProtectedMeaning,
    }
    pub(super) fn projection(
        receipt: &Receipt,
    ) -> (&Scope, CounterProofProfile, CounterProofPurpose) {
        (&receipt.scope, receipt.profile, receipt.meaning.purpose())
    }
    pub(super) fn meaning(receipt: &Receipt) -> CounterProtectedMeaning {
        receipt.meaning
    }
    pub(super) fn endpoints(receipt: &Receipt) -> (&'static str, &'static str) {
        (receipt.producer, receipt.intended_consumer)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Exact original, provider configuration, freshness, availability, profile, purpose, both endpoints and lexical bounds remain independent explicit inputs."
    )]
    pub(super) async fn verify(
        original: &CounterRequest,
        configured: &[ProtectedRecord],
        freshness: AuthorityFreshness,
        availability: ProviderAvailability,
        profile: CounterProofProfile,
        purpose: CounterProofPurpose,
        producer: &str,
        intended_consumer: &str,
        limits: &protected::Limits,
    ) -> Result<Receipt, VerificationError> {
        let message = protected::decode(original, limits).map_err(VerificationError::Malformed)?;
        match (availability, freshness) {
            (ProviderAvailability::Unavailable, _) => {
                let (publication, receiving) = oneshot::channel::<Receipt>();
                drop(publication);
                receiving.await.map_err(VerificationError::Unavailable)
            }
            (ProviderAvailability::Available, AuthorityFreshness::Stale) => {
                Err(VerificationError::Stale)
            }
            (ProviderAvailability::Available, AuthorityFreshness::Current) => {
                let Some(record) = configured.iter().find(|record| {
                    record.protected.as_ref() == original.protected.as_ref()
                        && record.proof.as_ref() == original.proof.as_ref()
                }) else {
                    return Err(VerificationError::Invalid);
                };
                let meaning = counter_registered_meaning(
                    &message,
                    record,
                    profile,
                    purpose,
                    producer,
                    intended_consumer,
                )?;
                Ok(Receipt {
                    scope: record.scope,
                    profile: record.profile,
                    producer: record.producer,
                    intended_consumer: record.intended_consumer,
                    meaning,
                })
            }
        }
    }
}

mod scheduled_provider {
    use super::{
        AuthorityFreshness, CounterProofProfile, CounterProofPurpose, CounterProtectedMeaning,
        CounterRequest, ProtectedRecord, ProviderAvailability, Scope, VerificationError,
        counter_registered_meaning, oneshot, protected,
    };
    pub(super) struct Receipt {
        scope: Scope,
        profile: CounterProofProfile,
        producer: &'static str,
        intended_consumer: &'static str,
        meaning: CounterProtectedMeaning,
        #[expect(
            dead_code,
            reason = "provider-owned evidence remains owned through the original caller receipt"
        )]
        evidence: Box<[u8]>,
    }
    pub(super) fn projection(
        receipt: &Receipt,
    ) -> (&Scope, CounterProofProfile, CounterProofPurpose) {
        (&receipt.scope, receipt.profile, receipt.meaning.purpose())
    }
    pub(super) fn meaning(receipt: &Receipt) -> CounterProtectedMeaning {
        receipt.meaning
    }
    pub(super) fn endpoints(receipt: &Receipt) -> (&'static str, &'static str) {
        (receipt.producer, receipt.intended_consumer)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Exact original, provider configuration, freshness, availability, profile, purpose, both endpoints and lexical bounds remain independent explicit inputs."
    )]
    pub(super) async fn verify(
        original: &CounterRequest,
        configured: &[ProtectedRecord],
        freshness: AuthorityFreshness,
        availability: ProviderAvailability,
        profile: CounterProofProfile,
        purpose: CounterProofPurpose,
        producer: &str,
        intended_consumer: &str,
        limits: &protected::Limits,
    ) -> Result<Receipt, VerificationError> {
        let message = protected::decode(original, limits).map_err(VerificationError::Malformed)?;
        let (publication, receiving) = oneshot::channel();
        match availability {
            ProviderAvailability::Unavailable => drop(publication),
            ProviderAvailability::Available => {
                let verified = match freshness {
                    AuthorityFreshness::Stale => Err(VerificationError::Stale),
                    AuthorityFreshness::Current => (|| {
                        let Some(record) = configured.iter().find(|record| {
                            record.protected.as_ref() == original.protected.as_ref()
                                && record.proof.as_ref() == original.proof.as_ref()
                        }) else {
                            return Err(VerificationError::Invalid);
                        };
                        let meaning = counter_registered_meaning(
                            &message,
                            record,
                            profile,
                            purpose,
                            producer,
                            intended_consumer,
                        )?;
                        Ok(Receipt {
                            scope: record.scope,
                            profile: record.profile,
                            producer: record.producer,
                            intended_consumer: record.intended_consumer,
                            meaning,
                            evidence: vec![19].into_boxed_slice(),
                        })
                    })(),
                };
                match publication.send(verified) {
                    Ok(()) => {}
                    Err(original) => return original,
                }
            }
        }
        receiving.await.map_err(VerificationError::Unavailable)?
    }
}

enum CounterServiceInput {
    Verify(u64),
    #[cfg(test)]
    ReportConsumed(u64),
    #[cfg(test)]
    ReadObserved {
        correlation: u64,
        value: u64,
    },
    ProtectedConsumed {
        correlation: u64,
        purpose: CounterProofPurpose,
    },
}
type CounterRequests = MessageProtocol<MailAddr, CounterServiceInput>;
enum CounterRequesterMessage {
    BeginProtected {
        correlation: u64,
        scope: Scope,
    },
    ProtectedReply {
        correlation: u64,
        scope: Scope,
        meaning: CounterProtectedMeaning,
    },
    #[cfg(test)]
    Begin(u64),
    #[cfg(test)]
    VerificationCompleted(CounterReport),
    #[cfg(test)]
    ReadObserved {
        correlation: u64,
        value: u64,
    },
}
enum CounterRequesterState {
    Protected {
        correlation: u64,
        scope: Scope,
        receipts: Vec<CounterProtectedMeaning>,
    },
    Ready,
    #[cfg(test)]
    Verifying(u64),
    #[cfg(test)]
    WaitingRead(CounterReport),
    #[cfg(test)]
    Complete {
        report: CounterReport,
        observation: Option<u64>,
    },
}
#[derive(thiserror::Error)]
enum CounterRequesterError {
    #[error("unexpected {kind}", kind = match .0.as_ref() {
        CounterRequesterMessage::BeginProtected { .. } => "protected verification trigger",
        CounterRequesterMessage::ProtectedReply { .. } => "protected completion",
        #[cfg(test)]
        CounterRequesterMessage::Begin(_) => "verification trigger",
        #[cfg(test)]
        CounterRequesterMessage::VerificationCompleted(_) => "verification completion",
        #[cfg(test)]
        CounterRequesterMessage::ReadObserved { .. } => "Counter snapshot",
    })]
    UnexpectedMessage(Box<CounterRequesterMessage>),
}
impl fmt::Debug for CounterRequesterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CounterRequesterError({self})")
    }
}
struct CounterRequester {
    service: EstablishedRecipient<CounterRequests>,
    #[cfg(test)]
    counter: EstablishedRecipient<Counter>,
    #[cfg(test)]
    replies: EstablishedRecipient<CounterValue>,
    state: CounterRequesterState,
}
#[bombay::actor(
    sends = {
        verification: Vec<EstablishedDelivery<CounterRequests>>,
        reads: Vec<EstablishedDelivery<Counter>>,
    },
    error = CounterRequesterError,
)]
impl CounterRequester {
    fn receive(&mut self, message: CounterRequesterMessage) -> BehaviorActed<Self> {
        match message {
            CounterRequesterMessage::BeginProtected { correlation, scope } => {
                if !matches!(self.state, CounterRequesterState::Ready) {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::BeginProtected { correlation, scope },
                    )));
                }
                self.state = CounterRequesterState::Protected {
                    correlation,
                    scope,
                    receipts: Vec::new(),
                };
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::Verify(correlation),
                )))
            }
            CounterRequesterMessage::ProtectedReply {
                correlation,
                scope,
                meaning,
            } => {
                let CounterRequesterState::Protected {
                    correlation: expected,
                    scope: captured,
                    receipts,
                } = &mut self.state
                else {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::ProtectedReply {
                            correlation,
                            scope,
                            meaning,
                        },
                    )));
                };
                let compatible = matches!(
                    (scope.operation, meaning),
                    (_, CounterProtectedMeaning::Refused(_))
                        | (
                            CounterOperation::Increment,
                            CounterProtectedMeaning::IncrementAdmitted
                                | CounterProtectedMeaning::IncrementProcessed,
                        )
                        | (
                            CounterOperation::Read,
                            CounterProtectedMeaning::ReadAdmitted
                                | CounterProtectedMeaning::ReadReply(_),
                        )
                        | (
                            CounterOperation::Stop,
                            CounterProtectedMeaning::StopAccepted
                                | CounterProtectedMeaning::RetirementReport { .. },
                        )
                );
                let contradicted = receipts.iter().any(|prior| {
                    matches!(
                        (prior, meaning),
                        (CounterProtectedMeaning::Refused(_), _)
                            | (_, CounterProtectedMeaning::Refused(_))
                    )
                });
                if *expected != correlation
                    || *captured != scope
                    || !compatible
                    || contradicted
                    || receipts
                        .iter()
                        .any(|prior| prior.purpose() == meaning.purpose())
                {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::ProtectedReply {
                            correlation,
                            scope,
                            meaning,
                        },
                    )));
                }
                receipts.push(meaning);
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::ProtectedConsumed {
                        correlation,
                        purpose: meaning.purpose(),
                    },
                )))
            }
            #[cfg(test)]
            admission => self.receive_admission_report(admission),
        }
    }
}
#[cfg(test)]
impl CounterRequester {
    fn receive_admission_report(
        &mut self,
        message: CounterRequesterMessage,
    ) -> BehaviorActed<Self> {
        match message {
            CounterRequesterMessage::Begin(correlation) => {
                if !matches!(self.state, CounterRequesterState::Ready) {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::Begin(correlation),
                    )));
                }
                self.state = CounterRequesterState::Verifying(correlation);
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::Verify(correlation),
                )))
            }
            CounterRequesterMessage::VerificationCompleted(report) => {
                if !matches!(self.state, CounterRequesterState::Verifying(expected) if expected == report.correlation)
                {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::VerificationCompleted(report),
                    )));
                }
                self.state = match report.status {
                    CounterStatus::IncrementAdmitted | CounterStatus::ReadAdmitted => {
                        CounterRequesterState::WaitingRead(report)
                    }
                    CounterStatus::StopAccepted | CounterStatus::StopRejected(_) => {
                        CounterRequesterState::Complete {
                            report,
                            observation: None,
                        }
                    }
                    CounterStatus::PossiblyAdmitted
                    | CounterStatus::Closed
                    | CounterStatus::Refused(_) => CounterRequesterState::Complete {
                        report,
                        observation: None,
                    },
                };
                let actions = Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::ReportConsumed(report.correlation),
                ));
                match report.status {
                    // Read is an independent native snapshot, never an Increment acknowledgement.
                    CounterStatus::IncrementAdmitted => {
                        Ok(actions.send_reads(EstablishedDelivery::new(
                            self.counter.clone(),
                            CounterMessage::Read(self.replies.clone()),
                        )))
                    }
                    CounterStatus::ReadAdmitted
                    | CounterStatus::StopAccepted
                    | CounterStatus::StopRejected(_)
                    | CounterStatus::PossiblyAdmitted
                    | CounterStatus::Closed
                    | CounterStatus::Refused(_) => Ok(actions),
                }
            }
            CounterRequesterMessage::ReadObserved { correlation, value } => {
                let prior = replace(&mut self.state, CounterRequesterState::Ready);
                match prior {
                    CounterRequesterState::WaitingRead(report)
                        if report.correlation == correlation =>
                    {
                        self.state = CounterRequesterState::Complete {
                            report,
                            observation: Some(value),
                        };
                        Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                            self.service.clone(),
                            CounterServiceInput::ReadObserved { correlation, value },
                        )))
                    }
                    retained => {
                        self.state = retained;
                        Err(CounterRequesterError::UnexpectedMessage(Box::new(
                            CounterRequesterMessage::ReadObserved { correlation, value },
                        )))
                    }
                }
            }
            message => Err(CounterRequesterError::UnexpectedMessage(Box::new(message))),
        }
    }
}
#[cfg(test)]
fn complete_counter(report: CounterReport) -> CounterRequesterMessage {
    CounterRequesterMessage::VerificationCompleted(report)
}
// Actual native custody, not a reduced shadow outcome. Every native field is inspected.
fn completed_actor<B>(origin: RootOrigin<B>, retirement: ActorRetirement<B, Never, Vec<Never>>) -> B
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Settlements: ClassifySettlement,
{
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                behavior,
                control,
                user,
                descendants,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("actual native actor failed; no clean result is synthesized");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(child_failures.as_slice(), []);
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
    let assessment = settlements.settlement_status();
    assert_eq!(assessment, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants.as_slice(), [] as [Never; 0]);
    assert_eq!(completion, Completion::Stopped);
    behavior
}

#[cfg(test)]
fn assert_cancelled_actor<B>(
    origin: RootOrigin<B>,
    retirement: &ActorRetirement<B, Never, Vec<Never>>,
) where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                capability_failures,
                retirement_failures,
                additional_failures,
                descendants,
                terminal_report,
                ..
            },
        ..
    } = retirement
    else {
        panic!("original owner-cancellation retirement is required");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(capability_failures.is_empty());
    assert!(retirement_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert_eq!(descendants.as_slice(), [] as [Never; 0]);
    assert!(terminal_report.is_none());
    // This match borrows the full original. Acquired source and queued ingress
    // remain owned there, with no empty-lane or processed-message claim.
}

// Caller-owned concurrent futures, not new executor tasks. Work only transfers
// existing native ports and awaits the existing permission to return. The real
// service remains outside Work, alongside every received original and native product.
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "Preserve one sequential acquisition and explicit independent verification inputs."
)]
async fn run_counter_profile<R>(
    mode: StartupMode,
    assignment: Option<(&AssignmentAuthority<'_>, Binding, Instant)>,
    configured: &[ProtectedRecord],
    provider: impl AsyncFn(&CounterRequest, &[ProtectedRecord]) -> Result<R, VerificationError>,
    projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
    verification_maximum: usize,
    execution_case: CounterWorkCase,
    profile: CounterProofProfile,
    expected_reports: [CounterStatus; 3],
    expected_snapshot: u64,
) -> Result<(), StartupRefusal> {
    if let StartupMode::ProductionRequired = mode {
        return Err(StartupRefusal::TestProvider);
    }
    let Some((authority, binding, deadline)) = assignment else {
        return Err(StartupRefusal::MissingAssignment);
    };
    authority.check_host()?;
    if authority.grant_issuer.is_empty() || authority.permitted_operations.is_empty() {
        return Err(StartupRefusal::Grant);
    }
    // This one acquisition block is also invoked by owning fault cases. Opaque
    // Work/native types stay inferred; validation runs only after all receiving.
    let acquisition = async {
        let counter_fault = Box::new(execution_case);
        let counter_fault_pointer = ptr::from_ref(counter_fault.as_ref()).cast::<()>();
        let (ports_publication, mut ports) = oneshot::channel();
        let (permitted, permission) = oneshot::channel();
        let (execution, counter_receiving) = Application::new(Counter::new().stop_on_shutdown())
            .execute_with(|application| async move {
                if execution_case == CounterWorkCase::CounterBeforePublication {
                    resume_unwind(counter_fault);
                }
                let lifecycle = application.lifecycle();
                let address = application.root().address();
                let target = application.root().established_recipient();
                let interface = application.interface(());
                let requests = interface
                    .external::<CounterRequests>()
                    .expect("explicit service port");
                let replies = interface
                    .external::<CounterValue>()
                    .expect("one serial reply port");
                let publication =
                    ports_publication.send((lifecycle, address, target, requests, replies));
                if execution_case == CounterWorkCase::CounterAfterPublication {
                    tokio::task::yield_now().await;
                    resume_unwind(counter_fault);
                }
                let permission = permission.await;
                (publication, permission)
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("entered explicit host is required");
            });
        let execution = maybe_done(AssertUnwindSafe(execution).catch_unwind());
        tokio::pin!(execution);
        let mut service = None;
        let mut originals = Vec::new();
        let mut allocations = Vec::new();
        let mut target_address = None;
        let mut reserve_refusals = Vec::new();
        let mut requester_products = Vec::new();
        let mut observations = Vec::new();
        // Polling execution first makes its actual pre/post-publication fault
        // observable. MaybeDone drops that failed future and owns its original cause.
        let handoff = tokio::select! {
            biased;
            () = execution.as_mut() => (&mut ports).now_or_never(),
            ports = &mut ports => Some(ports),
        };
        let mut counter_handoff = None;
        let mut stopped = (None, None);
        match handoff {
            Some(Err(error)) => counter_handoff = Some(Err(error)),
            Some(Ok((lifecycle, address, target, mut requests, mut replies))) => {
                counter_handoff = Some(Ok(()));
                target_address = Some(address);
                if execution.as_mut().output_mut().is_none() {
                    let limits = CounterLimits {
                        incoming: NonZeroUsize::new(2).expect("finite input"),
                        retained: NonZeroUsize::new(2).expect("finite input"),
                        individual: NonZeroUsize::new(131_072).expect("finite input"),
                        total: NonZeroUsize::new(match execution_case {
                            CounterWorkCase::ConstructorRefusal => 1,
                            _ => 262_144,
                        })
                        .expect("finite input"),
                        verification_maximum,
                    };
                    service = Some(CounterAuthorization::new(
                        StartupMode::ExplicitTest,
                        Some((binding, deadline, target.clone())),
                        authority.grant_issuer,
                        authority.authority_generation,
                        authority.permitted_operations,
                        projection,
                        profile,
                        limits,
                    ));
                    if let Some(Ok(authorization)) = service.as_mut() {
                        let caller_records = configured_counter_grants(binding, profile);
                        for index in [0, 0, 1] {
                            let caller_record =
                                configured.get(index).unwrap_or(&caller_records[index]);
                            let original = counter_request(caller_record);
                            allocations.push((
                                original.protected.as_ptr(),
                                original.proof.as_ptr(),
                                original.protected.clone(),
                            ));
                            let correlation = match authorization.reserve(original) {
                                Ok(correlation) => correlation,
                                Err(original) => {
                                    reserve_refusals.push(original);
                                    break;
                                }
                            };
                            let requester = CounterRequester {
                                service: requests.recipient(),
                                counter: target.clone(),
                                replies: replies.recipient(),
                                state: CounterRequesterState::Ready,
                            };
                            let fault = Box::new(execution_case);
                            let fault_pointer = ptr::from_ref(fault.as_ref()).cast::<()>();
                            let (publication, mut requester_ports) = oneshot::channel();
                            let (requester_permitted, permission) = oneshot::channel();
                            let (requester_execution, receiving) =
                                Application::new(requester.stop_on_shutdown())
                                    .execute_with(|application| async move {
                                        if execution_case
                                            == CounterWorkCase::RequesterBeforePublication
                                        {
                                            resume_unwind(fault);
                                        }
                                        let publication = publication.send((
                                            application.lifecycle(),
                                            application.root().address(),
                                            application.root().established_recipient(),
                                        ));
                                        if execution_case
                                            == CounterWorkCase::RequesterAfterPublication
                                        {
                                            tokio::task::yield_now().await;
                                            resume_unwind(fault);
                                        }
                                        let permission = permission.await;
                                        (publication, permission)
                                    })
                                    .unwrap_or_else(|original| {
                                        drop(original);
                                        panic!("entered explicit host is required");
                                    });
                            let requester_execution =
                                maybe_done(AssertUnwindSafe(requester_execution).catch_unwind());
                            tokio::pin!(requester_execution);
                            let handoff = tokio::select! {
                                biased;
                                () = requester_execution.as_mut() => (&mut requester_ports).now_or_never(),
                                ports = &mut requester_ports => Some(ports),
                            };
                            let mut requester_handoff = None;
                            let mut requester_address = None;
                            let mut report_reservation = None;
                            // Every acquired service fact is written to outside storage
                            // before its next await, so cancelling a borrowed wait loses none.
                            let (
                                mut begun,
                                mut triggered,
                                mut report,
                                mut completion,
                                mut acknowledgement,
                                mut snapshot,
                                mut refusal_read,
                                mut notification,
                                mut observed,
                                mut requester_stopped,
                                mut requester_permission,
                            ) = (
                                None, None, None, None, None, None, None, None, None, None, None,
                            );
                            match handoff {
                                Some(Err(error)) => requester_handoff = Some(Err(error)),
                                Some(Ok((lifecycle, address, recipient))) => {
                                    requester_handoff = Some(Ok(()));
                                    requester_address = Some(address);
                                    if requester_execution.as_mut().output_mut().is_none() {
                                        let service_wait = async {
                                            begun = Some(
                                                requests
                                                    .send(
                                                        &recipient,
                                                        CounterRequesterMessage::Begin(correlation),
                                                    )
                                                    .await,
                                            );
                                            if matches!(&begun, Some(Ok(()))) {
                                                triggered = requests.receive().await;
                                            }
                                            match &triggered {
                                                Some(delivery) if matches!(delivery.message, CounterServiceInput::Verify(actual) if actual == correlation) =>
                                                {
                                                    if let Some(position) = authorization
                                                        .pending
                                                        .iter()
                                                        .position(|entry| {
                                                            entry.correlation == correlation
                                                        })
                                                    {
                                                        acquire_verification(
                                                            &mut authorization.pending[position],
                                                            |original| {
                                                                provider(original, configured)
                                                            },
                                                        )
                                                        .await;
                                                        authorization.associate(
                                                            position,
                                                            &requests,
                                                            &replies.recipient(),
                                                        );
                                                        authorization
                                                            .attempt(correlation, &requests);
                                                    }
                                                    report = authorization.report(correlation);
                                                }
                                                Some(_) | None => {}
                                            }
                                            if let Some(report) = report {
                                                report_reservation =
                                                    Some((authorization.reserved_bytes, None));
                                                completion = Some(
                                                    requests
                                                        .send(&recipient, complete_counter(report))
                                                        .await,
                                                );
                                            }
                                            if matches!(&completion, Some(Ok(()))) {
                                                acknowledgement = requests.receive().await;
                                                if let Some((_, after)) = &mut report_reservation {
                                                    *after = Some(authorization.reserved_bytes);
                                                }
                                            }
                                            if matches!(
                                                report,
                                                Some(CounterReport {
                                                    status: CounterStatus::IncrementAdmitted
                                                        | CounterStatus::ReadAdmitted,
                                                    ..
                                                })
                                            ) {
                                                snapshot = replies.receive().await;
                                                if let Some(snapshot) = &snapshot {
                                                    notification = Some(requests.send(&recipient, CounterRequesterMessage::ReadObserved {
                                                        correlation, value: snapshot.message,
                                                    }).await);
                                                }
                                                if matches!(&notification, Some(Ok(()))) {
                                                    observed = requests.receive().await;
                                                }
                                            }
                                            if matches!(
                                                report,
                                                Some(CounterReport {
                                                    status: CounterStatus::Refused(_),
                                                    ..
                                                })
                                            ) {
                                                refusal_read = Some(
                                                    requests
                                                        .send(
                                                            &target,
                                                            CounterMessage::Read(
                                                                replies.recipient(),
                                                            ),
                                                        )
                                                        .await,
                                                );
                                                if matches!(&refusal_read, Some(Ok(()))) {
                                                    snapshot = replies.receive().await;
                                                }
                                            }
                                            requester_stopped = Some(lifecycle.request_shutdown());
                                            requester_permission =
                                                Some(requester_permitted.send(()));
                                        };
                                        tokio::pin!(service_wait);
                                        tokio::select! {
                                            biased;
                                            () = service_wait.as_mut() => {},
                                            () = execution.as_mut() => {},
                                            () = requester_execution.as_mut() => {},
                                        }
                                    }
                                    if matches!(execution.as_mut().output_mut(), Some(Err(_)))
                                        && requester_stopped.is_none()
                                    {
                                        requester_stopped = Some(lifecycle.request_shutdown());
                                    }
                                }
                                None => {}
                            }
                            requester_execution.as_mut().await;
                            let requester_result = requester_execution
                                .as_mut()
                                .take_output()
                                .expect("acquired execution output");
                            let product = receiving.await.into_parts();
                            requester_products.push((
                                requester_result,
                                product,
                                requester_handoff,
                                fault_pointer,
                            ));
                            observations.push((
                                requester_address,
                                begun,
                                triggered,
                                report,
                                completion,
                                (acknowledgement, report_reservation),
                                snapshot,
                                refusal_read,
                                notification,
                                observed,
                                requester_stopped,
                                requester_permission,
                            ));
                            if matches!(requester_products.last(), Some((Err(_), _, _, _)))
                                || execution.as_mut().output_mut().is_some()
                            {
                                break;
                            }
                            originals.push(authorization.consume(correlation));
                        }
                    }
                    stopped = (Some(lifecycle.request_shutdown()), Some(permitted.send(())));
                }
            }
            None => {}
        }
        execution.as_mut().await;
        let counter_execution = execution
            .as_mut()
            .take_output()
            .expect("acquired execution output");
        let counter_product = counter_receiving.await.into_parts();
        (
            counter_execution,
            counter_product,
            counter_handoff,
            counter_fault_pointer,
            stopped,
            service,
            originals,
            allocations,
            target_address,
            reserve_refusals,
            requester_products,
            observations,
        )
    };
    let acquired = acquisition.await;
    let (
        counter_execution,
        counter_product,
        counter_handoff,
        counter_fault_pointer,
        stopped,
        service,
        originals,
        allocations,
        target_address,
        reserve_refusals,
        requester_products,
        observations,
    ) = acquired;
    // Fault cases inspect the SAME acquired originals before any healthy validator.
    match execution_case {
        CounterWorkCase::CounterBeforePublication | CounterWorkCase::CounterAfterPublication => {
            let payload = counter_execution.expect_err("actual execution panic");
            assert_eq!(
                ptr::from_ref(payload.as_ref()).cast::<()>(),
                counter_fault_pointer
            );
            assert!(matches!(
                (&execution_case, &counter_handoff),
                (CounterWorkCase::CounterBeforePublication, Some(Err(_)))
                    | (CounterWorkCase::CounterAfterPublication, Some(Ok(())))
            ));
            let (
                WorkOutcome::Interrupted { cleanup: Ok(()) },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = counter_product
            else {
                panic!("original failed Work coexists with native and both notifications");
            };
            assert_cancelled_actor(origin, &retirement);
            for (execution, product, handoff, _) in &requester_products {
                assert!(execution.is_ok());
                let (
                    WorkOutcome::Completed {
                        cleanup: Ok(()),
                        output: (publication, permission),
                    },
                    Ok((origin, retirement)),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }),
                    (),
                ) = product
                else {
                    panic!("outer Work fault retains requester native and notifications");
                };
                assert!(publication.is_ok());
                assert!(permission.is_err());
                assert!(matches!(handoff, Some(Ok(()))));
                let ActorRetirement::Completed {
                    custody:
                        ActorCustody {
                            capability_failures,
                            retirement_failures,
                            additional_failures,
                            ..
                        },
                    completion: Completion::Stopped,
                    ..
                } = retirement
                else {
                    panic!("exact requester stopped after outer Work failure");
                };
                assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
                assert!(capability_failures.is_empty());
                assert!(retirement_failures.is_empty());
                assert!(additional_failures.is_empty());
            }
            drop((
                origin,
                retirement,
                payload,
                counter_handoff,
                requester_products,
                service,
                observations,
                allocations,
                originals,
                reserve_refusals,
            ));
            return Ok(());
        }
        CounterWorkCase::RequesterBeforePublication
        | CounterWorkCase::RequesterAfterPublication => {
            let [(execution, product, handoff, pointer)] =
                requester_products.try_into().unwrap_or_else(|original| {
                    drop(original);
                    panic!("one actual failed requester");
                });
            let payload = execution.expect_err("actual requester execution panic");
            assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), pointer);
            assert!(matches!(
                (&execution_case, &handoff),
                (CounterWorkCase::RequesterBeforePublication, Some(Err(_)))
                    | (CounterWorkCase::RequesterAfterPublication, Some(Ok(())))
            ));
            let (
                WorkOutcome::Interrupted { cleanup: Ok(()) },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = product
            else {
                panic!("failed requester original native and both notifications");
            };
            assert_cancelled_actor(origin, &retirement);
            drop((origin, retirement, payload, handoff));
            let Some(Ok(authorization)) = &service else {
                panic!("original service owner");
            };
            let [(protected, proof, byte)] = allocations.as_slice() else {
                panic!("one original reserved before Begin");
            };
            let [pending] = authorization.pending.as_slice() else {
                panic!("original survives failed Work");
            };
            assert_eq!(pending.original.protected.as_ptr(), *protected);
            assert_eq!(pending.original.proof.as_ptr(), *proof);
            assert_eq!(pending.original.protected.as_ref(), byte.as_ref());
            assert_eq!(pending.original.proof.as_ref(), &[41]);
            let (
                WorkOutcome::Completed {
                    cleanup: Ok(()),
                    output: counter_work_receipts,
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = counter_product
            else {
                panic!("remaining Counter cleanup conserves all acquired facts");
            };
            assert!(counter_execution.is_ok());
            let counter = completed_actor(origin, retirement);
            drop((
                counter,
                counter_work_receipts,
                service,
                observations,
                allocations,
                originals,
                reserve_refusals,
            ));
            return Ok(());
        }
        CounterWorkCase::CounterWhileProcessingPending
        | CounterWorkCase::RequesterCompletedAfterPublication
        | CounterWorkCase::RequesterAfterReplyBytes
        | CounterWorkCase::RequesterAfterReplyVerification
        | CounterWorkCase::RequesterAfterReplySend => {
            panic!("protected-only requester completion fixture")
        }
        CounterWorkCase::Healthy | CounterWorkCase::ConstructorRefusal => {}
    }
    // The original execution panic, if acquired, coexists with actual native
    // receiving and both notifications. No JoinError or clean product is forged.
    // These following assertions are the healthy demo oracle ONLY; owning panic
    // tests must inspect the actual Interrupted/cleanup/native products directly.
    // Every native product/full raw+R/native error is outside Work before assertions.
    assert!(counter_execution.is_ok());
    let (
        WorkOutcome::Completed {
            output: (publication, permission),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = counter_product
    else {
        panic!("actual Counter Work/native/notification outcomes remain distinct");
    };
    assert!(publication.is_ok());
    assert!(permission.is_ok());
    assert!(matches!(stopped, (Some(Ok(())), Some(Ok(())))));
    let _retired_counter = completed_actor(origin, retirement);
    let authorization = match service {
        Some(Ok(authorization)) => authorization,
        Some(Err(original)) => return Err(original),
        None => panic!("stored configuration result"),
    };
    assert_eq!(requester_products.len(), expected_reports.len());
    assert_eq!(observations.len(), expected_reports.len());
    for (_, _, _, _, _, _, snapshot, ..) in &observations {
        let Some(snapshot) = snapshot else {
            panic!("actual native snapshot remains owned");
        };
        assert_eq!(
            snapshot.message, expected_snapshot,
            "authenticated non-request or refused evidence changed the native Counter"
        );
    }
    for (((execution, product, handoff, _), expected_report), sent_report) in requester_products
        .into_iter()
        .zip(expected_reports)
        .zip(observations.iter().map(|(_, _, _, report, ..)| *report))
    {
        assert!(matches!(handoff, Some(Ok(()))));
        assert!(execution.is_ok());
        let (
            WorkOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = product
        else {
            panic!("original requester native and both notifications are required");
        };
        assert!(publication.is_ok());
        assert!(permission.is_ok());
        let requester = completed_actor(origin, retirement).into_inner();
        let CounterRequesterState::Complete {
            report,
            observation,
        } = requester.state
        else {
            panic!("actual requester consumed the selected result");
        };
        assert_eq!(
            Some(report),
            sent_report,
            "native requester report changed correlation"
        );
        assert_eq!(
            report.status, expected_report,
            "provider evidence classification was erased"
        );
        let expected_observation = match expected_report {
            CounterStatus::IncrementAdmitted | CounterStatus::ReadAdmitted => {
                Some(expected_snapshot)
            }
            CounterStatus::Refused(_)
            | CounterStatus::Closed
            | CounterStatus::PossiblyAdmitted
            | CounterStatus::StopAccepted
            | CounterStatus::StopRejected(_) => None,
        };
        assert_eq!(observation, expected_observation);
    }
    for (
        requester_address,
        begun,
        triggered,
        report,
        completion,
        (acknowledged, report_reservation),
        snapshot,
        refusal_read,
        notification,
        observed,
        stopped,
        permitted,
    ) in observations
    {
        assert!(matches!(begun, Some(Ok(()))));
        let Some(triggered) = triggered else {
            panic!("actual Verify delivery");
        };
        assert_eq!(Some(triggered.from), requester_address);
        assert!(matches!(triggered.message, CounterServiceInput::Verify(_)));
        assert!(matches!(completion, Some(Ok(()))));
        let Some(acknowledged) = acknowledged else {
            panic!("actual report acknowledgement");
        };
        assert_eq!(Some(acknowledged.from), requester_address);
        assert!(matches!(report_reservation, Some((before, Some(after))) if before == after));
        assert!(
            matches!(acknowledged.message, CounterServiceInput::ReportConsumed(actual) if Some(actual) == report.map(|report| report.correlation))
        );
        let Some(snapshot) = snapshot else {
            panic!("actual native snapshot for accepted or refused evidence");
        };
        assert_eq!(Some(snapshot.from), target_address);
        assert_eq!(
            snapshot.message, expected_snapshot,
            "provider evidence refusal changed the Counter"
        );
        match report.map(|report| report.status) {
            Some(CounterStatus::IncrementAdmitted | CounterStatus::ReadAdmitted) => {
                assert!(refusal_read.is_none());
                assert!(matches!(notification, Some(Ok(()))));
                let Some(observed) = observed else {
                    panic!("actual accepted snapshot consumed through Actions");
                };
                assert_eq!(Some(observed.from), requester_address);
                assert!(
                    matches!(observed.message, CounterServiceInput::ReadObserved { correlation, value } if Some(correlation) == report.map(|report| report.correlation) && value == expected_snapshot)
                );
            }
            Some(CounterStatus::Refused(_)) => {
                assert!(matches!(refusal_read, Some(Ok(()))));
                assert!(notification.is_none());
                assert!(observed.is_none());
            }
            Some(
                CounterStatus::Closed
                | CounterStatus::PossiblyAdmitted
                | CounterStatus::StopAccepted
                | CounterStatus::StopRejected(_),
            )
            | None => {
                panic!("this configured profile selected accepted or refused evidence");
            }
        }
        assert!(matches!(stopped, Some(Ok(()))));
        assert!(matches!(permitted, Some(Ok(()))));
    }
    assert!(reserve_refusals.is_empty());
    assert_eq!(originals.len(), 3);
    assert_eq!(allocations.len(), 3);
    for ((original, (protected_pointer, proof_pointer, protected)), expected_report) in
        originals.into_iter().zip(allocations).zip(expected_reports)
    {
        let Some(CounterAdmission {
            original,
            verification,
        }) = original
        else {
            panic!("full caller result");
        };
        assert_eq!(original.protected.as_ptr(), protected_pointer);
        assert_eq!(original.proof.as_ptr(), proof_pointer);
        assert_eq!(original.protected.as_ref(), protected.as_ref());
        let expected_proof = configured
            .iter()
            .find(|record| record.protected.as_ref() == original.protected.as_ref())
            .map_or(&[41][..], |record| record.proof.as_ref());
        assert_eq!(original.proof.as_ref(), expected_proof);
        match (expected_report, verification) {
            (
                expected_status @ (CounterStatus::IncrementAdmitted | CounterStatus::ReadAdmitted),
                Verification::Native {
                    receipt,
                    delivery: CounterDelivery::IncrementAdmitted | CounterDelivery::ReadAdmitted,
                },
            ) => {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.protected.as_ref() == original.protected.as_ref()
                            && record.proof.as_ref() == original.proof.as_ref()
                    })
                    .expect("independent configured protected authority");
                assert_eq!(projection(&receipt).0, &configured_receipt.scope);
                let expected_operation = match expected_status {
                    CounterStatus::IncrementAdmitted => CounterOperation::Increment,
                    CounterStatus::ReadAdmitted => CounterOperation::Read,
                    _ => unreachable!("the preceding pattern excludes other meanings"),
                };
                assert_eq!(projection(&receipt).0.operation, expected_operation);
            }
            (
                CounterStatus::IncrementAdmitted,
                Verification::Duplicate {
                    receipt,
                    report:
                        CounterReport {
                            status: CounterStatus::IncrementAdmitted,
                            ..
                        },
                },
            ) => {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.protected.as_ref() == original.protected.as_ref()
                            && record.proof.as_ref() == original.proof.as_ref()
                    })
                    .expect("independent configured protected authority");
                assert_eq!(projection(&receipt).0, &configured_receipt.scope);
            }
            (
                CounterStatus::Refused(CounterRefusal::Invalid),
                Verification::Refused {
                    evidence: Err(VerificationError::Invalid),
                    refusal: CounterRefusal::Invalid,
                },
            )
            | (
                CounterStatus::Refused(CounterRefusal::Stale),
                Verification::Refused {
                    evidence: Err(VerificationError::Stale),
                    refusal: CounterRefusal::Stale,
                },
            ) => {}
            (
                CounterStatus::Refused(CounterRefusal::Unavailable),
                Verification::Refused {
                    evidence: Err(VerificationError::Unavailable(original)),
                    refusal: CounterRefusal::Unavailable,
                },
            ) => {
                println!("actual provider receipt-closure cause: {original}");
            }
            (
                CounterStatus::Refused(expected),
                Verification::Refused {
                    evidence: Ok(receipt),
                    refusal,
                },
            ) if matches!(
                expected,
                CounterRefusal::Binding | CounterRefusal::Grant | CounterRefusal::Invalid
            ) =>
            {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.protected.as_ref() == original.protected.as_ref()
                            && record.proof.as_ref() == original.proof.as_ref()
                    })
                    .expect("independent refused Scope remains owned with actual R");
                assert_eq!(projection(&receipt).0, &configured_receipt.scope);
                assert_eq!(refusal, expected);
            }
            retained => match &retained {
                (
                    _,
                    Verification::Native {
                        delivery: CounterDelivery::Closed(cause),
                        ..
                    },
                ) => {
                    panic!("Counter admission rejected: {cause}");
                }
                (
                    _,
                    Verification::Native {
                        delivery: CounterDelivery::Refused { command, refusal },
                        ..
                    },
                ) => {
                    let operation = match command {
                        CounterCommand::Message(
                            CounterMessage::Increment | CounterMessage::IncrementAndReply { .. },
                        ) => CounterOperation::Increment,
                        CounterCommand::Message(CounterMessage::Read(_)) => CounterOperation::Read,
                        CounterCommand::Stop => CounterOperation::Stop,
                    };
                    panic!("Counter {operation:?} admission refused: {refusal:?}");
                }
                _ => {
                    drop(retained);
                    panic!("original evidence, literal expectation and report remain distinct");
                }
            },
        }
    }
    assert!(authorization.pending.is_empty());
    let expected_retention = [
        CounterStatus::IncrementAdmitted,
        CounterStatus::ReadAdmitted,
    ]
    .iter()
    .filter(|status| expected_reports.contains(status))
    .count();
    assert_eq!(authorization.retained.len(), expected_retention);
    Ok(())
}

#[cfg(test)]
fn configured_counter_grants(
    binding: Binding,
    profile: CounterProofProfile,
) -> [ProtectedRecord; 2] {
    [
        (0, CounterOperation::Increment),
        (1, CounterOperation::Read),
    ]
    .map(|(sequence, operation)| {
        let scope = Scope {
            identity: Identity { binding, sequence },
            operation,
            grant_issuer: "grant-controller",
            authority_generation: 7,
        };
        counter_record(
            scope,
            profile,
            CounterProtectedMeaning::Request,
            &[41],
            &mut [0; 8192],
        )
        .unwrap_or_else(|original| {
            drop(original);
            panic!("explicit valid configured request fixture");
        })
    })
}
fn counter_request(record: &ProtectedRecord) -> CounterRequest {
    CounterRequest {
        protected: record.protected.clone(),
        proof: record.proof.clone(),
    }
}

// Current reply authority is independent of native command admission.
#[expect(
    clippy::too_many_arguments,
    reason = "One current reply law receives explicit independently current authority and typed receipt projectors without a second authority wrapper."
)]
fn counter_protected_reply<Receipt>(
    binding: Binding,
    grant_issuer: &str,
    authority_generation: u64,
    configured_profile: CounterProofProfile,
    receipt: &Receipt,
    captured: Scope,
    purpose: CounterProofPurpose,
    projection: fn(&Receipt) -> (&Scope, CounterProofProfile, CounterProofPurpose),
    meaning: fn(&Receipt) -> CounterProtectedMeaning,
    endpoints: fn(&Receipt) -> (&'static str, &'static str),
    permitted: &[CounterProofPurpose],
) -> Result<CounterProtectedMeaning, CounterRefusal> {
    let (scope, profile, verified_purpose) = (projection)(receipt);
    if scope.identity.binding != binding || *scope != captured {
        return Err(CounterRefusal::Binding);
    }
    if scope.grant_issuer != grant_issuer
        || scope.authority_generation != authority_generation
        || !permitted.contains(&purpose)
    {
        return Err(CounterRefusal::Grant);
    }
    if profile != configured_profile
        || verified_purpose != purpose
        || endpoints(receipt) != (binding.host, binding.caller)
    {
        return Err(CounterRefusal::Invalid);
    }
    Ok(meaning(receipt))
}

// Current reply authority is independent of permission to execute a command.
impl<R> CounterAuthorization<R> {
    fn protected_reply(
        &self,
        receipt: &R,
        captured: Scope,
        purpose: CounterProofPurpose,
        meaning: fn(&R) -> CounterProtectedMeaning,
        endpoints: fn(&R) -> (&'static str, &'static str),
        permitted: &[CounterProofPurpose],
    ) -> Result<CounterProtectedMeaning, CounterRefusal> {
        counter_protected_reply(
            self.binding,
            self.grant_issuer,
            self.authority_generation,
            self.profile,
            receipt,
            captured,
            purpose,
            self.projection,
            meaning,
            endpoints,
            permitted,
        )
    }
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "One actual Application campaign retains original provider, native, Work and notification products before its independent oracles."
)]
async fn run_protected_counter_profile<R>(
    #[cfg(test)] execution_case: CounterWorkCase,
    authority: &AssignmentAuthority<'_>,
    binding: Binding,
    deadline: Instant,
    profile: CounterProofProfile,
    configured: &[ProtectedRecord],
    provider: &impl AsyncFn(
        &CounterRequest,
        &[ProtectedRecord],
        AuthorityFreshness,
        ProviderAvailability,
        CounterProofProfile,
        CounterProofPurpose,
        &str,
        &str,
        &protected::Limits,
    ) -> Result<R, VerificationError>,
    projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
    meaning: fn(&R) -> CounterProtectedMeaning,
    endpoints: fn(&R) -> (&'static str, &'static str),
) -> Result<(), StartupRefusal> {
    authority.check_host()?;
    #[cfg(test)]
    let counter_fault = Box::new(execution_case);
    #[cfg(test)]
    let counter_fault_pointer = ptr::from_ref(counter_fault.as_ref()).cast::<()>();
    let execute_counter = |#[cfg(test)] fault: Box<CounterWorkCase>| {
        let (publication, ports) = oneshot::channel();
        let (permitted, permission) = oneshot::channel();
        let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
            .execute_with(|application| async move {
                #[cfg(test)]
                if *fault == CounterWorkCase::CounterBeforePublication {
                    resume_unwind(fault);
                }
                let lifecycle = application.lifecycle();
                let target = application.root().established_recipient();
                let interface = application.interface(());
                let requests = interface
                    .external::<CounterRequests>()
                    .expect("configured service port");
                let publication = publication.send((lifecycle, target, interface, requests));
                #[cfg(test)]
                if *fault == CounterWorkCase::CounterAfterPublication {
                    tokio::task::yield_now().await;
                    resume_unwind(fault);
                }
                #[cfg(test)]
                if *fault == CounterWorkCase::CounterWhileProcessingPending {
                    let _permission = permission.await;
                    resume_unwind(fault);
                }
                let permission = permission.await;
                (publication, permission)
            })
            .unwrap_or_else(|_| panic!("the actual owning host is entered"));
        (execution, receiving, ports, permitted)
    };
    let (counter_execution, counter_receiving, counter_ports, counter_permit) = execute_counter(
        #[cfg(test)]
        counter_fault,
    );
    let (sibling_execution, sibling_receiving, sibling_ports, sibling_permit) = execute_counter(
        #[cfg(test)]
        Box::new(CounterWorkCase::Healthy),
    );
    #[cfg(test)]
    let mut first_requester_original_pointers = None;
    #[cfg(test)]
    let mut first_requester_shutdown = None;
    #[cfg(test)]
    let mut first_requester_repeated_execution = None;
    #[cfg(test)]
    let mut first_requester_exchange_readiness = None;
    #[cfg(test)]
    let mut counter_permit = Some(counter_permit);
    #[cfg(test)]
    let mut earlier_counter_release = None;
    #[cfg(test)]
    let mut counter_processing_before = None;
    #[cfg(test)]
    let mut counter_processing_after = None;
    #[cfg(test)]
    let mut counter_retirement_before_rescue = None;
    let service = async {
        let publications = tokio::join!(counter_ports, sibling_ports);
        let (counter_ports, sibling_ports) = match publications {
            (Ok(counter), Ok(sibling)) => (counter, sibling),
            publications => {
                let counter_stop = publications
                    .0
                    .as_ref()
                    .ok()
                    .map(|(lifecycle, ..)| lifecycle.request_shutdown());
                let sibling_stop = publications
                    .1
                    .as_ref()
                    .ok()
                    .map(|(lifecycle, ..)| lifecycle.request_shutdown());
                #[cfg(not(test))]
                let counter_release = counter_permit.send(());
                #[cfg(test)]
                let counter_release = counter_permit
                    .take()
                    .expect("original Counter permission")
                    .send(());
                let sibling_release = sibling_permit.send(());
                return Err((
                    publications,
                    counter_stop,
                    sibling_stop,
                    counter_release,
                    sibling_release,
                ));
            }
        };
        let (lifecycle, original_target, interface, mut requests) = counter_ports;
        let (sibling_lifecycle, sibling, sibling_interface, sibling_requests) = sibling_ports;
        let mut admissions = Vec::new();
        let mut encoding_results = Vec::new();
        let mut reserve_refusals = Vec::new();
        let mut requester_receipts = Vec::new();
        let mut processing_facts = Vec::new();
        let mut read_facts = Vec::new();
        let mut protected_facts = Vec::new();
        let mut original_audit = None;
        let mut original_retirement = None;
        let mut produced_retirement = None;
        let mut authorization = CounterAuthorization::new(
            StartupMode::ExplicitTest,
            Some((binding, deadline, original_target.clone())),
            authority.grant_issuer,
            authority.authority_generation,
            &[CounterOperation::Increment, CounterOperation::Read],
            projection,
            profile,
            CounterLimits {
                incoming: NonZeroUsize::new(2).expect("explicit fixture count"),
                retained: NonZeroUsize::new(3).expect("three distinct executed commands"),
                individual: NonZeroUsize::new(131_072).expect("explicit fixture bytes"),
                total: NonZeroUsize::new(262_144).expect("explicit fixture bytes"),
                verification_maximum: 65_536,
            },
        );

        let acquired = AssertUnwindSafe(async {
            let mut processing = interface.external::<CounterProcessed>().expect("request-owned processing port");
            let mut values = interface.external::<CounterValue>().expect("request-owned Read port");
            let mut audit_values = interface.external::<CounterValue>().expect("independent original-target audit port");
            let mut sibling_values = sibling_interface.external::<CounterValue>().expect("independent sibling audit port");
            let Ok(authorization) = &mut authorization else { panic!("finite consumer configuration"); };
            authorization.stop = Some(CounterStopAuthority::Root(lifecycle.clone()));
            authorization.processing = Some(processing.recipient());
            let permitted_replies = [CounterProofPurpose::Refused, CounterProofPurpose::MailboxAdmitted,
                CounterProofPurpose::IncrementProcessed, CounterProofPurpose::ReadReply,
                CounterProofPurpose::StopAccepted, CounterProofPurpose::RetirementReport];
            for sequence in [0, 0, 1, 2, 3] {
                if sequence == 0 && admissions.len() == 1 {
                    authorization.permitted_operations = &[CounterOperation::Read];
                }
                if sequence == 3 {
                    let sent = requests.send(&original_target, CounterMessage::Read(audit_values.recipient())).await;
                    let observed = audit_values.receive().await;
                    original_audit = Some((sent, observed));
                    authorization.permitted_operations = &[CounterOperation::Read, CounterOperation::Stop];
                }
                let request_record = configured.iter().find(|record|
                    record.purpose == CounterProofPurpose::Request && record.scope.identity.sequence == sequence)
                    .expect("independently configured complete request");
                let scope = request_record.scope;
                let original = counter_request(request_record);
                let pointers = (original.protected.as_ptr(), original.proof.as_ptr());
                let reservation = authorization.reserve(original);
                let correlation = match reservation {
                    Ok(correlation) => correlation,
                    Err(original) => { reserve_refusals.push(original); panic!("bounded original request"); }
                };
                let position = authorization.pending.iter().position(|entry| entry.correlation == correlation)
                    .expect("caller-owned reservation");
                let (publication, ports) = oneshot::channel();
                let (permitted, permission) = oneshot::channel();
                let mut permitted = Some(permitted);
                let mut requester_begun = None;
                let mut requester_release = None;
                let mut triggered = None;
                let mut produced = None;
                let mut delivered = Vec::new();
                let requester = CounterRequester { service: requests.recipient(),
                    #[cfg(test)] counter: original_target.clone(),
                    #[cfg(test)] replies: values.recipient(), state: CounterRequesterState::Ready };
                #[cfg(test)]
                let fault = Box::new(if requester_receipts.is_empty() { execution_case } else { CounterWorkCase::Healthy });
                #[cfg(test)]
                let fault_pointer = ptr::from_ref(fault.as_ref()).cast::<()>();
                #[cfg(test)]
                if requester_receipts.is_empty() { first_requester_original_pointers = Some((fault_pointer, pointers)); }
                let (execution, receiving) = Application::new(requester.stop_on_shutdown())
                    .execute_with(|application| async move {
                        #[cfg(test)]
                        if *fault == CounterWorkCase::RequesterBeforePublication { resume_unwind(fault); }
                        let lifecycle = application.lifecycle();
                        let target = application.root().established_recipient();
                        let sender = application.interface(()).external::<CounterRequests>().expect("typed completion sender");
                        let publication = publication.send((lifecycle, target, sender));
                        #[cfg(test)]
                        if matches!(*fault, CounterWorkCase::RequesterAfterPublication | CounterWorkCase::RequesterAfterReplyBytes
                            | CounterWorkCase::RequesterAfterReplyVerification | CounterWorkCase::RequesterAfterReplySend) {
                            let _permission = permission.await;
                            resume_unwind(fault);
                        }
                        let permission = permission.await;
                        (publication, permission)
                    }).unwrap_or_else(|_| panic!("existing requester Application"));
                let execution = maybe_done(AssertUnwindSafe(execution).catch_unwind());
                let ports = maybe_done(ports);
                tokio::pin!(execution, ports);
                tokio::select! {
                    biased;
                    () = execution.as_mut() => { let _observed = ports.as_mut().now_or_never(); }
                    () = ports.as_mut() => {}
                }
                let mut handoff = ports.as_mut().take_output();
                let mut acquired = None;
                if let Some(Ok(requester_ports)) = &handoff {
                    let (_, requester, sender) = requester_ports;
                    #[cfg(test)]
                    let (requester_lifecycle, ..) = requester_ports;
                    let exchange = async {
                        requester_begun = Some(sender.send(requester, CounterRequesterMessage::BeginProtected { correlation, scope }).await);
                        #[cfg(test)]
                        {
                            if execution_case == CounterWorkCase::RequesterCompletedAfterPublication {
                                first_requester_shutdown = Some(requester_lifecycle.request_shutdown());
                            }
                            if matches!(execution_case, CounterWorkCase::RequesterAfterPublication | CounterWorkCase::RequesterCompletedAfterPublication) {
                                requester_release = Some(permitted.take().expect("original requester permission").send(()));
                            }
                        }
                        triggered = Some(requests.receive().await);
                        let request_limits = counter_test_json_limits();
                        acquire_verification(&mut authorization.pending[position], |original| {
                            provider(original, configured, AuthorityFreshness::Current, ProviderAvailability::Available,
                                profile, CounterProofPurpose::Request, binding.caller, binding.host, &request_limits)
                        }).await;
                        authorization.associate(position, &requests, &values.recipient());
                        authorization.attempt(correlation, &requests);
                        let report = authorization.report(correlation).expect("native status is distinct from processing");
                        produced = Some(match report.status {
                            CounterStatus::IncrementAdmitted => {
                                match &authorization.pending[position].verification {
                                    Verification::Native { delivery: CounterDelivery::IncrementAdmitted, .. } => {
                                        #[cfg(test)]
                                        match execution_case {
                                            CounterWorkCase::CounterWhileProcessingPending => {
                                            let received = maybe_done(processing.receive());
                                            tokio::pin!(received);
                                            counter_processing_before = Some(received.as_mut().now_or_never());
                                            if counter_processing_before == Some(None) {
                                                earlier_counter_release = Some(counter_permit.take().expect("original Counter permission").send(()));
                                                counter_retirement_before_rescue = Some(lifecycle.retirement().await);
                                                counter_processing_after = Some(received.as_mut().now_or_never());
                                                first_requester_shutdown = Some(requester_lifecycle.request_shutdown());
                                                requester_release = Some(permitted.take().expect("original requester permission").send(()));
                                            }
                                            received.as_mut().await;
                                            processing_facts.push(received.as_mut().take_output().expect("actual processing receive output"));
                                            }
                                            _ => processing_facts.push(processing.receive().await),
                                        }
                                        #[cfg(not(test))]
                                        processing_facts.push(processing.receive().await);
                                    }
                                    Verification::Duplicate { .. } => {}
                                    _ => panic!("native or retained original processing result"),
                                }
                                assert_eq!(processing_facts.last().expect("original processing custody").as_ref().expect("actual processing Actions reply").message.request_sequence,
                                    scope.identity.sequence, "native processing correlation differs from captured request");
                                vec![CounterProtectedMeaning::IncrementAdmitted, CounterProtectedMeaning::IncrementProcessed]
                            }
                            CounterStatus::ReadAdmitted => {
                                read_facts.push(values.receive().await);
                                let value = read_facts.last().expect("original Read custody").as_ref().expect("actual per-request Read Actions reply").message;
                                vec![CounterProtectedMeaning::ReadAdmitted, CounterProtectedMeaning::ReadReply(value)]
                            }
                            CounterStatus::Refused(refusal) => vec![CounterProtectedMeaning::Refused(refusal)],
                            CounterStatus::StopAccepted => {
                                let report = authorization.stop.as_ref().expect("actual native stop authority").retirement().await;
                                original_retirement = lifecycle.retirement().now_or_never();
                                produced_retirement = Some(report);
                                assert_eq!(original_retirement, Some(report),
                                    "actual Stop authority did not retire the originally captured target");
                                authorization.permission = Permission::Revoked;
                                authorization.deadline = Instant::now();
                                vec![CounterProtectedMeaning::StopAccepted, CounterProtectedMeaning::RetirementReport {
                                    retirement: report.retirement(), failures: report.failures() }]
                            }
                            original => { panic!("healthy fixture retains unexpected native status {original:?}"); }
                        });
                        let admission = authorization.consume(correlation);
                        admissions.push((scope, admission, pointers, report));
                        for fact in produced.as_ref().expect("acquired produced meanings") {
                            let purpose = fact.purpose();
                            let encoded = counter_record(scope, profile, *fact, &[43], &mut [0; 8192]);
                            encoding_results.push(encoded);
                            let Ok(record) = encoding_results.last().expect("original encoding custody")
                                else { panic!("bounded actual fact encoding"); };
                            let original = counter_request(record);
                            let pointers = (original.protected.as_ptr(), original.proof.as_ptr());
                            protected_facts.push((original, None, pointers, None, None, *fact));
                            #[cfg(test)]
                            if execution_case == CounterWorkCase::RequesterAfterReplyBytes {
                                requester_release = Some(permitted.take().expect("original requester permission").send(()));
                                pending::<()>().await;
                            }
                            let (original, verified, _, emitted, consumed, _) = protected_facts.last_mut().expect("original reply custody");
                            *verified = Some(provider(original, configured, AuthorityFreshness::Current,
                                ProviderAvailability::Available, profile, purpose, binding.host,
                                binding.caller, &counter_test_json_limits()).await);
                            let Some(Ok(receipt)) = verified else { panic!("independently registered actual reply"); };
                            #[cfg(test)]
                            if execution_case == CounterWorkCase::RequesterAfterReplyVerification {
                                requester_release = Some(permitted.take().expect("original requester permission").send(()));
                                pending::<()>().await;
                            }
                            // Emission and consumption independently consult the same current authority.
                            *emitted = Some(authorization.protected_reply(receipt, scope, purpose, meaning, endpoints, &permitted_replies));
                            *consumed = Some(authorization.protected_reply(receipt, scope, purpose, meaning, endpoints, &permitted_replies));
                            delivered.push((purpose, None, None));
                            let (_, completion, acknowledged) = delivered.last_mut().expect("actual completion custody");
                            *completion = Some(match *consumed {
                                Some(Ok(meaning)) => sender.send(requester, CounterRequesterMessage::ProtectedReply { correlation, scope, meaning }).await,
                                original => panic!("current reply authority refused {original:?}"),
                            });
                            #[cfg(test)]
                            if execution_case == CounterWorkCase::RequesterAfterReplySend {
                                requester_release = Some(permitted.take().expect("original requester permission").send(()));
                                pending::<()>().await;
                            }
                            *acknowledged = Some(requests.receive().await);

                        }
                    };
                    {
                        let exchange = maybe_done(AssertUnwindSafe(exchange).catch_unwind());
                        tokio::pin!(exchange);
                        tokio::select! {
                            biased;
                            () = execution.as_mut() => {
                                #[cfg(test)]
                                let exchange_readiness = exchange.as_mut().now_or_never();
                                #[cfg(not(test))]
                                let _exchange_readiness = exchange.as_mut().now_or_never();
                                #[cfg(test)]
                                if requester_receipts.is_empty() {
                                    first_requester_exchange_readiness = Some(exchange_readiness);
                                }
                            }
                            () = exchange.as_mut() => {
                                #[cfg(test)]
                                if requester_receipts.is_empty() {
                                    first_requester_exchange_readiness = Some(Some(()));
                                }
                            }
                        };
                        acquired = exchange.as_mut().take_output();
                    }
                }
                let stopped = handoff.as_ref().and_then(|original| original.as_ref().ok())
                    .map(|(lifecycle, ..)| lifecycle.request_shutdown());
                let permitted = match requester_release.take() {
                    Some(original) => original,
                    None => permitted.take().expect("original requester permission").send(()),
                };
                execution.as_mut().await;
                let original_execution = execution.as_mut().take_output().expect("actual execution output");
                #[cfg(test)]
                if requester_receipts.is_empty() {
                    first_requester_repeated_execution = Some(execution.as_mut().take_output());
                }
                let native = receiving.await.into_parts();
                if handoff.is_none() {
                    let _observed = ports.as_mut().now_or_never();
                    handoff = ports.as_mut().take_output();
                }
                requester_receipts.push((original_execution, handoff, acquired,
                    requester_begun, triggered, produced, delivered, stopped, permitted, native));
                #[cfg(test)]
                if matches!(execution_case, CounterWorkCase::CounterWhileProcessingPending | CounterWorkCase::RequesterBeforePublication | CounterWorkCase::RequesterAfterPublication
                    | CounterWorkCase::RequesterCompletedAfterPublication | CounterWorkCase::RequesterAfterReplyBytes
                    | CounterWorkCase::RequesterAfterReplyVerification | CounterWorkCase::RequesterAfterReplySend) { break; }
            }
            let sibling_increment = sibling_requests.send(&sibling, CounterMessage::Increment).await;
            let sibling_read = sibling_requests.send(&sibling, CounterMessage::Read(sibling_values.recipient())).await;
            let sibling_value = if sibling_read.is_ok() {
                sibling_values.receive().await
            } else {
                None
            };
            (sibling_increment, sibling_read, sibling_value,
                audit_values.recipient(), sibling_values.recipient())
        }).catch_unwind().await;
        let original_cleanup = lifecycle.request_shutdown();
        let sibling_cleanup = sibling_lifecycle.request_shutdown();
        #[cfg(not(test))]
        let counter_release = counter_permit.send(());
        #[cfg(test)]
        let counter_release = match earlier_counter_release.take() {
            Some(original) => original,
            None => counter_permit
                .take()
                .expect("original Counter permission")
                .send(()),
        };
        let sibling_release = sibling_permit.send(());
        Ok((
            acquired,
            original_cleanup,
            sibling_cleanup,
            counter_release,
            sibling_release,
            admissions,
            requester_receipts,
            processing_facts,
            read_facts,
            protected_facts,
            original_audit,
            original_retirement,
            produced_retirement,
            authorization,
            encoding_results,
            reserve_refusals,
        ))
    };
    #[cfg(test)]
    let service = AssertUnwindSafe(service).catch_unwind();
    #[cfg(test)]
    let mut service_after_counter = None;
    #[cfg(test)]
    let (counter_execution, sibling_execution, service) = {
        let counter = maybe_done(AssertUnwindSafe(counter_execution).catch_unwind());
        let sibling = maybe_done(AssertUnwindSafe(sibling_execution).catch_unwind());
        let service = maybe_done(service);
        tokio::pin!(counter, sibling, service);
        if execution_case == CounterWorkCase::CounterWhileProcessingPending {
            tokio::select! { biased;
                () = counter.as_mut() => {},
                () = sibling.as_mut() => { counter.as_mut().await; },
                () = service.as_mut() => { counter.as_mut().await; },
            }
            service_after_counter = Some(service.as_mut().now_or_never());
        }
        tokio::join!(counter.as_mut(), sibling.as_mut(), service.as_mut());
        (
            counter
                .as_mut()
                .take_output()
                .expect("original Counter execution"),
            sibling
                .as_mut()
                .take_output()
                .expect("original sibling execution"),
            service
                .as_mut()
                .take_output()
                .expect("original service result"),
        )
    };
    #[cfg(not(test))]
    let (counter_execution, sibling_execution, service) = tokio::join!(
        AssertUnwindSafe(counter_execution).catch_unwind(),
        AssertUnwindSafe(sibling_execution).catch_unwind(),
        service
    );
    let counter_native = counter_receiving.await.into_parts();
    let sibling_native = sibling_receiving.await.into_parts();
    #[cfg(test)]
    if execution_case == CounterWorkCase::CounterBeforePublication {
        let payload = counter_execution
            .as_ref()
            .expect_err("original Counter Work panic");
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            counter_fault_pointer
        );
        let (
            WorkOutcome::Interrupted { cleanup: Ok(()) },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = &counter_native
        else {
            panic!("original Counter native and both notifications");
        };
        assert_cancelled_actor(*origin, retirement);
        let (
            WorkOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = sibling_native
        else {
            panic!("original sibling native and both notifications");
        };
        let _retired_sibling = completed_actor(origin, retirement);
        assert!(sibling_execution.is_ok() && publication.is_ok() && permission.is_ok());
        assert!(
            service.is_ok(),
            "closed Counter publication caused a secondary service panic after native cleanup"
        );
        let Ok(Err((publications, counter_stop, sibling_stop, counter_release, sibling_release))) =
            service
        else {
            panic!("original failed publication product");
        };
        assert!(
            matches!(publications, (Err(_), Ok(_))),
            "actual Counter RecvError is retained"
        );
        assert!(counter_stop.is_none() && matches!(sibling_stop, Some(Ok(()))));
        assert!(counter_release.is_err() && sibling_release.is_ok());
        return Ok(());
    }
    #[cfg(test)]
    let service = match service {
        Ok(original) => original,
        Err(original) => resume_unwind(original),
    };
    let Ok(service) = service else {
        panic!("healthy actual publication product");
    };
    #[cfg(test)]
    if execution_case == CounterWorkCase::CounterWhileProcessingPending {
        let payload = counter_execution
            .as_ref()
            .expect_err("original Counter Work panic");
        let (
            WorkOutcome::Interrupted { cleanup: Ok(()) },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = &counter_native
        else {
            panic!("original Counter native and both notifications");
        };
        let (
            acquired,
            original_stop,
            sibling_stop,
            counter_release,
            sibling_release,
            admissions,
            requesters,
            processing,
            reads,
            protected,
            _,
            _,
            _,
            authorization,
            encoded,
            refused,
        ) = &service;
        let [
            (
                execution,
                handoff,
                exchange,
                begun,
                triggered,
                produced,
                delivered,
                stopped,
                permitted,
                native,
            ),
        ] = requesters.as_slice()
        else {
            panic!("one original requester receiving owner");
        };
        let (
            WorkOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((requester_origin, requester_retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = native
        else {
            panic!("original requester Work/native/two notifications");
        };
        let (
            WorkOutcome::Completed {
                output: (sibling_publication, sibling_permission),
                cleanup: Ok(()),
            },
            Ok((sibling_origin, sibling_retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = sibling_native
        else {
            panic!("original sibling Work/native/two notifications");
        };
        let _retired_sibling = completed_actor(sibling_origin, sibling_retirement);
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            counter_fault_pointer
        );
        assert_cancelled_actor(*origin, retirement);
        assert!(execution.is_ok() && publication.is_ok() && permission.is_ok());
        assert!(
            sibling_execution.is_ok() && sibling_publication.is_ok() && sibling_permission.is_ok()
        );
        assert!(matches!(
            requester_retirement,
            ActorRetirement::Completed {
                completion: Completion::Stopped,
                ..
            }
        ));
        assert_eq!(requester_origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(counter_processing_before, Some(None));

        let report = counter_retirement_before_rescue.expect("exact Counter joined report");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        eprintln!(
            "Counter native OwnerCancelled: retirement={:?}, failures={:?}",
            report.retirement(),
            report.failures()
        );
        assert_eq!(service_after_counter, Some(None));
        let Ok((increment, read, value, _, _)) = acquired else {
            panic!("actual independent sibling progress");
        };
        assert!(increment.is_ok() && read.is_ok());
        assert_eq!(value.as_ref().expect("actual sibling reply").message, 1);
        assert!(
            matches!(original_stop, Err(ShutdownRejection::AlreadyStopped)) && sibling_stop.is_ok()
        );
        assert!(counter_release.is_ok() && sibling_release.is_ok());
        assert!(matches!(handoff, Some(Ok(_))) && exchange.is_none());
        assert!(matches!(begun, Some(Ok(()))) && matches!(triggered, Some(Some(_))));

        assert!(
            matches!(stopped, Some(Err(ShutdownRejection::AlreadyStopped))) && permitted.is_ok()
        );
        assert_eq!(first_requester_shutdown, Some(Ok(())));
        assert!(reads.is_empty() && refused.is_empty());
        let Ok(authorization) = authorization else {
            panic!("original authorization owner");
        };
        let (original, correlation, status) = match counter_processing_after {
            Some(None) => {
                assert!(admissions.is_empty() && processing.is_empty() && protected.is_empty());
                assert!(produced.is_none() && delivered.is_empty() && encoded.is_empty());
                let [entry] = authorization.pending.as_slice() else {
                    panic!("original admitted request custody");
                };
                let report = authorization
                    .report(entry.correlation)
                    .expect("current admitted status");
                (&entry.original, report.correlation, report.status)
            }
            Some(Some(())) => {
                let [Some(processed)] = processing.as_slice() else {
                    panic!("actual original processing reply");
                };
                let [(scope, Some(admission), _, report)] = admissions.as_slice() else {
                    panic!("original consumed admission");
                };
                assert!(authorization.pending.is_empty());
                assert_eq!(processed.message.request_sequence, scope.identity.sequence);
                assert_eq!(
                    produced.as_deref(),
                    Some(
                        [
                            CounterProtectedMeaning::IncrementAdmitted,
                            CounterProtectedMeaning::IncrementProcessed
                        ]
                        .as_slice()
                    )
                );
                assert!(encoded.iter().all(Result::is_ok));
                (&admission.original, report.correlation, report.status)
            }
            None => panic!("actual processing readiness was not acquired"),
        };
        assert_eq!(status, CounterStatus::IncrementAdmitted);
        let Some(Some(trigger)) = triggered else {
            panic!("actual requester trigger");
        };
        assert!(
            matches!(trigger.message, CounterServiceInput::Verify(expected) if expected == correlation)
        );
        let (_, pointers) = first_requester_original_pointers.expect("original request pointers");
        assert_eq!(
            (original.protected.as_ptr(), original.proof.as_ptr()),
            pointers
        );
        return Ok(());
    }
    #[cfg(test)]
    if matches!(
        execution_case,
        CounterWorkCase::RequesterBeforePublication
            | CounterWorkCase::RequesterAfterPublication
            | CounterWorkCase::RequesterCompletedAfterPublication
            | CounterWorkCase::RequesterAfterReplyBytes
            | CounterWorkCase::RequesterAfterReplyVerification
            | CounterWorkCase::RequesterAfterReplySend
    ) {
        let (
            acquired_service,
            original_stop,
            sibling_stop,
            counter_release,
            sibling_release,
            admissions,
            requester_receipts,
            processing_facts,
            read_facts,
            protected_facts,
            _,
            _,
            _,
            authorization,
            encoding_results,
            reserve_refusals,
        ) = &service;
        let [
            (
                execution,
                handoff,
                acquired,
                begun,
                triggered,
                produced,
                delivered,
                stopped,
                permitted,
                native,
            ),
        ] = requester_receipts.as_slice()
        else {
            panic!("one original requester execution and native receipt");
        };
        let (fault_pointer, pointers) =
            first_requester_original_pointers.expect("original pointers before publication");
        let (
            outcome,
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = native
        else {
            panic!("original requester native and both notifications");
        };
        if execution_case == CounterWorkCase::RequesterCompletedAfterPublication {
            assert!(execution.is_ok());
            let WorkOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            } = outcome
            else {
                panic!("actual completed requester Work");
            };
            assert!(publication.is_ok() && permission.is_ok());
            let ActorRetirement::Completed { completion, .. } = retirement else {
                panic!("actual completed requester native retirement");
            };
            assert_eq!(*completion, Completion::Stopped);
        } else {
            let payload = execution
                .as_ref()
                .expect_err("original requester Work panic");
            assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), fault_pointer);
            assert!(matches!(
                outcome,
                WorkOutcome::Interrupted { cleanup: Ok(()) }
            ));
        }
        match execution_case {
            CounterWorkCase::RequesterBeforePublication => {
                assert_cancelled_actor(*origin, retirement);
            }
            CounterWorkCase::RequesterAfterPublication
            | CounterWorkCase::RequesterAfterReplyBytes
            | CounterWorkCase::RequesterAfterReplyVerification
            | CounterWorkCase::RequesterAfterReplySend => match retirement {
                ActorRetirement::Completed { completion, .. } => {
                    assert!(matches!(completion, Completion::Stopped));
                }
                ActorRetirement::OwnerCancelled { .. } => {
                    assert_cancelled_actor(*origin, retirement);
                }
                _ => panic!("unexpected actual requester retirement"),
            },
            CounterWorkCase::RequesterCompletedAfterPublication => {}
            _ => unreachable!("three explicit requester acquisition fixtures"),
        }
        let Ok((
            sibling_increment,
            sibling_read,
            sibling_value,
            audit_recipient,
            sibling_audit_recipient,
        )) = acquired_service
        else {
            panic!("original sibling progress and audit capabilities");
        };
        assert!(sibling_increment.is_ok() && sibling_read.is_ok());
        assert_eq!(
            sibling_value
                .as_ref()
                .expect("actual sibling progress")
                .message,
            1
        );
        for (native, recipient) in [counter_native, sibling_native]
            .into_iter()
            .zip([audit_recipient, sibling_audit_recipient])
        {
            let (
                WorkOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = native
            else {
                panic!("remaining actual actor native and notifications");
            };
            let retired_actor = completed_actor(origin, retirement);
            assert!(publication.is_ok() && permission.is_ok());
            if matches!(
                execution_case,
                CounterWorkCase::RequesterAfterReplyBytes
                    | CounterWorkCase::RequesterAfterReplyVerification
                    | CounterWorkCase::RequesterAfterReplySend
            ) {
                let mut counter = retired_actor.into_inner();
                let observed = counter
                    .receive(origin.address(), CounterMessage::Read(recipient.clone()))
                    .expect("actual native Counter final Read Actions");
                let Actions {
                    sends:
                        CounterSends {
                            values,
                            processed_increments,
                        },
                    creates,
                    become_,
                } = observed;
                assert_eq!(values.len(), 1);
                assert_eq!(
                    values[0].message, 1,
                    "original Counter and sibling native values"
                );
                assert!(processed_increments.is_empty() && creates.is_empty());
                assert!(matches!(become_, Step::Continue));
            }
        }
        assert!(counter_execution.is_ok() && sibling_execution.is_ok());
        assert!(original_stop.is_ok() && sibling_stop.is_ok());
        assert!(counter_release.is_ok() && sibling_release.is_ok());
        assert!(
            matches!(first_requester_repeated_execution, Some(None)),
            "same completed execution output was not acquired twice"
        );
        if matches!(
            execution_case,
            CounterWorkCase::RequesterAfterPublication
                | CounterWorkCase::RequesterCompletedAfterPublication
                | CounterWorkCase::RequesterAfterReplyBytes
                | CounterWorkCase::RequesterAfterReplyVerification
                | CounterWorkCase::RequesterAfterReplySend
        ) {
            if execution_case == CounterWorkCase::RequesterCompletedAfterPublication {
                assert!(matches!(first_requester_shutdown, Some(Ok(()))));
            }
            let Ok(authorization) = authorization else {
                panic!("original authorization product");
            };
            let retained = authorization
                .pending
                .iter()
                .map(|entry| (&entry.original, entry.correlation))
                .chain(admissions.iter().filter_map(|(_, admission, _, report)| {
                    admission
                        .as_ref()
                        .map(|entry| (&entry.original, report.correlation))
                }))
                .find(|(original, _)| {
                    (original.protected.as_ptr(), original.proof.as_ptr()) == pointers
                });
            let Some((original, correlation)) = retained else {
                panic!("original protected request and proof retained");
            };
            assert!(matches!(handoff, Some(Ok(_))));
            assert!(
                matches!(begun, Some(Ok(()))),
                "original Begin result survived cancellation of its borrowed wait"
            );
            assert!(permitted.is_ok() && stopped.is_some());
            assert!(
                matches!(first_requester_exchange_readiness, Some(None)),
                "unfinished exchange was not relabeled completed"
            );
            assert!(
                acquired.is_none(),
                "unfinished exchange has no invented success or failure result"
            );
            if let Some(triggered) = triggered {
                assert!(matches!(triggered, Some(actual) if matches!(actual.message,
                    CounterServiceInput::Verify(observed) if observed == correlation)));
            }
            let scope = configured
                .iter()
                .find(|record| {
                    record.purpose == CounterProofPurpose::Request
                        && record.protected.as_ref() == original.protected.as_ref()
                })
                .expect("original configured request")
                .scope;
            if matches!(
                execution_case,
                CounterWorkCase::RequesterAfterReplyBytes
                    | CounterWorkCase::RequesterAfterReplyVerification
                    | CounterWorkCase::RequesterAfterReplySend
            ) {
                let [(admitted_scope, Some(admission), original_pointers, report)] =
                    admissions.as_slice()
                else {
                    panic!("original native admission before reply suspension");
                };
                assert_eq!(*admitted_scope, scope);
                assert_eq!(*original_pointers, pointers);
                assert_eq!(report.status, CounterStatus::IncrementAdmitted);
                assert!(matches!(
                    admission.verification,
                    Verification::Native {
                        delivery: CounterDelivery::IncrementAdmitted,
                        ..
                    }
                ));
                let [Some(processed)] = processing_facts.as_slice() else {
                    panic!("original actual processing before reply suspension");
                };
                assert_eq!(processed.message.request_sequence, scope.identity.sequence);
                assert!(
                    matches!(produced, Some(actual) if actual == &[CounterProtectedMeaning::IncrementAdmitted, CounterProtectedMeaning::IncrementProcessed])
                );
                let [(original, verified, _, emitted, consumed, actual)] =
                    protected_facts.as_slice()
                else {
                    panic!("one original reply before suspension");
                };
                assert_eq!(*actual, CounterProtectedMeaning::IncrementAdmitted);
                assert!(!original.protected.is_empty() && !original.proof.is_empty());
                match execution_case {
                    CounterWorkCase::RequesterAfterReplyBytes => {
                        assert!(
                            verified.is_none(),
                            "suspended verification did not invent a receipt"
                        );
                        assert!(emitted.is_none() && consumed.is_none() && delivered.is_empty());
                    }
                    CounterWorkCase::RequesterAfterReplyVerification => {
                        assert!(
                            matches!(verified, Some(Ok(_))),
                            "original provider receipt survived its borrowed wait"
                        );
                        assert!(emitted.is_none() && consumed.is_none() && delivered.is_empty());
                    }
                    CounterWorkCase::RequesterAfterReplySend => {
                        assert!(matches!(verified, Some(Ok(_))));
                        assert_eq!(*emitted, Some(Ok(*actual)));
                        assert_eq!(*consumed, Some(Ok(*actual)));
                        let [(purpose, completion, acknowledgement)] = delivered.as_slice() else {
                            panic!("original completion send before acknowledgement wait");
                        };
                        assert_eq!(*purpose, actual.purpose());
                        assert!(
                            matches!(completion, Some(Ok(()))),
                            "original completion result survived acknowledgement wait"
                        );
                        assert!(acknowledgement.is_none());
                    }
                    _ => unreachable!("three late acquired-fact fixtures"),
                }
            }
            for actual in processing_facts.iter().flatten() {
                assert_eq!(actual.message.request_sequence, scope.identity.sequence);
            }
            for (original, verified, pointers, emitted, consumed, fact) in protected_facts {
                assert_eq!(
                    (original.protected.as_ptr(), original.proof.as_ptr()),
                    *pointers
                );
                if let Some(Ok(receipt)) = verified {
                    assert_eq!(projection(receipt), (&scope, profile, fact.purpose()));
                    assert_eq!(meaning(receipt), *fact);
                }
                if let Some(actual) = emitted {
                    assert_eq!(*actual, Ok(*fact));
                }
                if let Some(actual) = consumed {
                    assert_eq!(*actual, Ok(*fact));
                }
            }
            for (purpose, completion, acknowledgement) in delivered {
                if let Some(actual) = completion {
                    assert!(actual.is_ok());
                }
                if let Some(actual) = acknowledgement {
                    assert!(matches!(actual, Some(actual) if matches!(actual.message,
                        CounterServiceInput::ProtectedConsumed { correlation: observed, purpose: observed_purpose }
                        if observed == correlation && observed_purpose == *purpose)));
                }
            }
            if let Some(produced) = produced {
                assert!(delivered.len() <= produced.len());
            }
            return Ok(());
        }
        assert!(
            acquired.is_none(),
            "closed publication did not start or panic a second exchange"
        );
        assert!(
            matches!(handoff, Some(Err(_))),
            "actual requester RecvError is retained"
        );
        let Ok(authorization) = authorization else {
            panic!("original authorization product");
        };
        let [pending] = authorization.pending.as_slice() else {
            panic!("one original pending request");
        };
        assert_eq!(
            (
                pending.original.protected.as_ptr(),
                pending.original.proof.as_ptr()
            ),
            pointers
        );
        assert!(matches!(pending.verification, Verification::Awaiting));
        assert!(processing_facts.is_empty() && read_facts.is_empty() && protected_facts.is_empty());
        assert!(encoding_results.is_empty() && reserve_refusals.is_empty());
        return Ok(());
    }
    // All original Work/native/notification outcomes have arrived before any oracle.
    let (
        acquired,
        original_cleanup,
        sibling_cleanup,
        counter_release,
        sibling_release,
        admissions,
        requester_receipts,
        processing_facts,
        read_facts,
        protected_facts,
        original_audit,
        original_retirement,
        produced_retirement,
        authorization,
        encoding_results,
        reserve_refusals,
    ) = service;
    assert_eq!(
        admissions[1].3.status, admissions[0].3.status,
        "repeated identity lost acquired native admission after command membership withdrawal"
    );
    let Some((Ok(()), Some(audit))) = original_audit else {
        panic!("independent original target audit");
    };
    assert_eq!(
        audit.message, 1,
        "protected admission affected a different Application root"
    );
    let Some(original_retirement) = original_retirement else {
        panic!("original target retirement was unavailable while Work remained live");
    };
    assert_eq!(Some(original_retirement), produced_retirement);
    assert_eq!(processing_facts.len(), 1);
    assert_eq!(
        processing_facts[0]
            .as_ref()
            .expect("actual processing fact")
            .message
            .request_sequence,
        0
    );
    let Ok((
        sibling_increment,
        sibling_read,
        sibling_value,
        audit_recipient,
        sibling_audit_recipient,
    )) = acquired
    else {
        panic!("complete actual service result retained");
    };
    let counters = [counter_native, sibling_native].map(|native| {
        let (
            WorkOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = native
        else {
            panic!("full Counter Work/native/two-notification outcomes");
        };
        (
            completed_actor(origin, retirement).into_inner(),
            publication,
            permission,
        )
    });
    let requesters = requester_receipts
        .into_iter()
        .map(
            |(
                execution,
                handoff,
                acquired,
                begun,
                triggered,
                produced,
                delivered,
                stopped,
                permitted,
                native,
            )| {
                let Some(Ok(_)) = handoff else {
                    panic!("healthy actual requester publication");
                };
                let consumed = (
                    acquired, begun, triggered, produced, delivered, stopped, permitted,
                );
                let (
                    WorkOutcome::Completed {
                        output: (publication, permission),
                        cleanup: Ok(()),
                    },
                    Ok((origin, retirement)),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }),
                    (),
                ) = native
                else {
                    panic!("full requester Work/native/two-notification outcomes");
                };
                (
                    completed_actor(origin, retirement).into_inner(),
                    execution,
                    consumed,
                    publication,
                    permission,
                )
            },
        )
        .collect::<Vec<_>>();
    assert_eq!(
        original_retirement.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        original_retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(counter_execution.is_ok() && sibling_execution.is_ok());
    assert_eq!(original_cleanup, Err(ShutdownRejection::AlreadyStopped));
    assert!(sibling_cleanup.is_ok() && counter_release.is_ok() && sibling_release.is_ok());
    assert!(sibling_increment.is_ok() && sibling_read.is_ok());
    let sibling_value = sibling_value.expect("actual sibling progress");
    assert_eq!(sibling_value.message, 1);
    for ((mut counter, publication, permission), (source, recipient)) in counters.into_iter().zip([
        (audit.from, audit_recipient),
        (sibling_value.from, sibling_audit_recipient),
    ]) {
        let read = counter
            .receive(source, CounterMessage::Read(recipient))
            .expect("native final Counter pure fold");
        let Actions {
            sends:
                CounterSends {
                    values,
                    processed_increments,
                },
            creates,
            become_,
        } = read;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].message, 1);
        assert!(processed_increments.is_empty() && creates.is_empty());
        assert!(matches!(become_, Step::Continue));
        assert!(publication.is_ok() && permission.is_ok());
    }
    assert_eq!(read_facts.len(), 1);
    assert_eq!(read_facts[0].as_ref().expect("actual Read fact").message, 1);
    assert_eq!(admissions.len(), 5);
    let Ok(authorization) = authorization else {
        panic!("original constructor result retained");
    };
    assert!(reserve_refusals.is_empty());
    assert_eq!(encoding_results.len(), 9);
    assert!(encoding_results.iter().all(Result::is_ok));
    assert!(authorization.pending.is_empty());
    assert_eq!(authorization.retained.len(), 3);
    for (
        (scope, admission, pointers, report),
        (requester, execution, consumed, publication, permission),
    ) in admissions.into_iter().zip(requesters)
    {
        let CounterRequesterState::Protected {
            correlation,
            scope: captured,
            receipts,
        } = requester.state
        else {
            panic!("typed requester consumed protected facts");
        };
        assert_eq!(captured, scope);
        assert_eq!(correlation, report.correlation);
        assert!(execution.is_ok() && publication.is_ok() && permission.is_ok());
        let (
            Some(Ok(())),
            Some(begun),
            Some(triggered),
            Some(produced),
            delivered,
            Some(stopped),
            permitted,
        ) = consumed
        else {
            panic!("full typed consumption outcome");
        };
        assert!(begun.is_ok() && stopped.is_ok() && permitted.is_ok());
        let triggered = triggered.expect("actual verification trigger");
        assert!(matches!(&triggered.message,
            CounterServiceInput::Verify(actual) if *actual == correlation));
        assert_eq!(receipts, produced);
        for (purpose, completion, acknowledged) in delivered {
            let (Some(completion), Some(acknowledged)) = (completion, acknowledged) else {
                panic!("full acquired completion and acknowledgement");
            };
            assert!(completion.is_ok());
            let acknowledged = acknowledged.expect("actual typed acknowledgement");
            assert!(matches!(&acknowledged.message,
                CounterServiceInput::ProtectedConsumed { correlation: actual, purpose: actual_purpose }
                    if *actual == correlation && *actual_purpose == purpose));
        }
        let admission = admission.expect("original request and native result retained");
        assert_eq!(
            (
                admission.original.protected.as_ptr(),
                admission.original.proof.as_ptr()
            ),
            pointers
        );
        let receipt = match (scope.operation, report.status, admission.verification) {
            (
                CounterOperation::Increment,
                CounterStatus::IncrementAdmitted,
                Verification::Duplicate {
                    receipt,
                    report:
                        CounterReport {
                            status: CounterStatus::IncrementAdmitted,
                            ..
                        },
                }
                | Verification::Native {
                    receipt,
                    delivery: CounterDelivery::IncrementAdmitted,
                },
            )
            | (
                CounterOperation::Read,
                CounterStatus::ReadAdmitted,
                Verification::Native {
                    receipt,
                    delivery: CounterDelivery::ReadAdmitted,
                },
            )
            | (
                CounterOperation::Stop,
                CounterStatus::StopAccepted,
                Verification::Native {
                    receipt,
                    delivery: CounterDelivery::StopRequested(Ok(())),
                },
            )
            | (
                CounterOperation::Stop,
                CounterStatus::Refused(CounterRefusal::Grant),
                Verification::Refused {
                    evidence: Ok(receipt),
                    refusal: CounterRefusal::Grant,
                },
            ) => receipt,
            (
                _,
                _,
                Verification::Native {
                    delivery: CounterDelivery::Closed(original),
                    ..
                },
            ) => {
                panic!("actual closed native command: {original}");
            }
            (
                _,
                _,
                Verification::Native {
                    delivery: CounterDelivery::Refused { command, refusal },
                    ..
                },
            ) => {
                let operation = match command {
                    CounterCommand::Message(
                        CounterMessage::Increment | CounterMessage::IncrementAndReply { .. },
                    ) => CounterOperation::Increment,
                    CounterCommand::Message(CounterMessage::Read(_)) => CounterOperation::Read,
                    CounterCommand::Stop => CounterOperation::Stop,
                };
                panic!("native {operation:?} remained refused: {refusal:?}");
            }
            original => {
                drop(original);
                panic!("exact request custody and native meaning");
            }
        };
        assert_eq!(
            projection(&receipt),
            (&scope, profile, CounterProofPurpose::Request)
        );
    }
    assert_eq!(protected_facts.len(), 9);
    for (original, receipt, pointers, emitted, consumed, actual) in protected_facts {
        assert_eq!(
            (original.protected.as_ptr(), original.proof.as_ptr()),
            pointers
        );
        assert_eq!(emitted, Some(Ok(actual)));
        assert_eq!(consumed, Some(Ok(actual)));
        let Some(Ok(receipt)) = receipt else {
            panic!("original reply verification result retained");
        };
        assert_eq!(meaning(&receipt), actual);
    }
    Ok(())
}

fn configured_protected_counter(
    binding: Binding,
    grant_issuer: &'static str,
    authority_generation: u64,
    profile: CounterProofProfile,
) -> Vec<ProtectedRecord> {
    let scopes = [
        (0, CounterOperation::Increment),
        (1, CounterOperation::Read),
        (2, CounterOperation::Stop),
        (3, CounterOperation::Stop),
    ]
    .map(|(sequence, operation)| Scope {
        identity: Identity { binding, sequence },
        operation,
        grant_issuer,
        authority_generation,
    });
    let requests = scopes
        .iter()
        .copied()
        .map(|scope| (scope, CounterProtectedMeaning::Request));
    let replies = [
        (scopes[0], CounterProtectedMeaning::IncrementAdmitted),
        (scopes[0], CounterProtectedMeaning::IncrementProcessed),
        (scopes[1], CounterProtectedMeaning::ReadAdmitted),
        (scopes[1], CounterProtectedMeaning::ReadReply(1)),
        (
            scopes[2],
            CounterProtectedMeaning::Refused(CounterRefusal::Grant),
        ),
        (scopes[3], CounterProtectedMeaning::StopAccepted),
        (
            scopes[3],
            CounterProtectedMeaning::RetirementReport {
                retirement: RetirementAssessment::Established,
                failures: ActorFailureAssessment::NoFailuresFound,
            },
        ),
    ];
    requests
        .chain(replies)
        .map(|(scope, meaning)| {
            let proof: &[u8] = match meaning {
                CounterProtectedMeaning::Request => &[41],
                _ => &[43],
            };
            counter_record(scope, profile, meaning, proof, &mut [0; 8192]).unwrap_or_else(
                |original| {
                    drop(original);
                    panic!("finite independent fixture registration");
                },
            )
        })
        .collect()
}

// Owning execution-fault and startup-refusal tests for this executable.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_scope_reaches_both_private_provider_consumers() {
        let binding = Binding {
            deployment: "test-deployment",
            caller: "caller",
            controller: "controller",
            actor: "counter",
            host: "counter-host",
            protocol: "counter-protocol",
            runtime_generation: 23,
            actor_generation: 29,
        };
        let scope = Scope {
            identity: Identity {
                binding,
                sequence: 0,
            },
            operation: CounterOperation::Increment,
            grant_issuer: "grant-controller",
            authority_generation: 7,
        };
        let configured = [counter_record(
            scope,
            CounterProofProfile::RecordedTest,
            CounterProtectedMeaning::Request,
            &[41],
            &mut [0; 8192],
        )
        .unwrap_or_else(|original| {
            drop(original);
            panic!("registered JSON fixture");
        })];
        let original = counter_request(&configured[0]);
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let recorded = host
            .block_on(record_provider::verify(
                &original,
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
                CounterProofProfile::RecordedTest,
                CounterProofPurpose::Request,
                binding.caller,
                binding.host,
                &counter_test_json_limits(),
            ))
            .unwrap_or_else(|_| panic!("the independently configured record verifies"));
        let scheduled_configuration = [counter_record(
            scope,
            CounterProofProfile::ScheduledTest,
            CounterProtectedMeaning::Request,
            &[41],
            &mut [0; 8192],
        )
        .unwrap_or_else(|original| {
            drop(original);
            panic!("scheduled JSON fixture");
        })];
        let scheduled_original = counter_request(&scheduled_configuration[0]);
        let scheduled = host
            .block_on(scheduled_provider::verify(
                &scheduled_original,
                &scheduled_configuration,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
                CounterProofProfile::ScheduledTest,
                CounterProofPurpose::Request,
                binding.caller,
                binding.host,
                &counter_test_json_limits(),
            ))
            .unwrap_or_else(|_| panic!("the independently configured scheduled evidence verifies"));
        assert_eq!(record_provider::projection(&recorded).0, &scope);
        assert_eq!(scheduled_provider::projection(&scheduled).0, &scope);
    }
    use std::cell::{Ref, RefCell};

    #[test]
    fn actual_execution_faults_conserve_original_causes_native_and_notifications() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        for execution_case in [
            CounterWorkCase::CounterBeforePublication,
            CounterWorkCase::CounterAfterPublication,
            CounterWorkCase::RequesterBeforePublication,
            CounterWorkCase::RequesterAfterPublication,
        ] {
            let (binding, deadline) = host
                .block_on(async { authority.issue() })
                .expect("configured finite authority");
            let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
            let acquired = host.block_on(run_counter_profile(
                StartupMode::ExplicitTest,
                Some((&authority, binding, deadline)),
                &configured,
                async |original, records| {
                    record_provider::verify(
                        original,
                        records,
                        AuthorityFreshness::Current,
                        ProviderAvailability::Available,
                        CounterProofProfile::RecordedTest,
                        CounterProofPurpose::Request,
                        binding.caller,
                        binding.host,
                        &counter_test_json_limits(),
                    )
                    .await
                },
                record_provider::projection,
                65_536,
                execution_case,
                CounterProofProfile::RecordedTest,
                [
                    CounterStatus::IncrementAdmitted,
                    CounterStatus::IncrementAdmitted,
                    CounterStatus::ReadAdmitted,
                ],
                1,
            ));
            acquired.expect("fault validator acquired all originals");
        }
    }

    #[test]
    fn original_constructor_refusal_survives_actual_application_cleanup() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("configured finite authority");
        let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
        let acquired = host.block_on(run_counter_profile(
            StartupMode::ExplicitTest,
            Some((&authority, binding, deadline)),
            &configured,
            async |original, records| {
                record_provider::verify(
                    original,
                    records,
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    CounterProofProfile::RecordedTest,
                    CounterProofPurpose::Request,
                    binding.caller,
                    binding.host,
                    &counter_test_json_limits(),
                )
                .await
            },
            record_provider::projection,
            65_536,
            CounterWorkCase::ConstructorRefusal,
            CounterProofProfile::RecordedTest,
            [
                CounterStatus::IncrementAdmitted,
                CounterStatus::IncrementAdmitted,
                CounterStatus::ReadAdmitted,
            ],
            1,
        ));
        assert!(matches!(acquired, Err(StartupRefusal::Accounting)));
    }

    #[test]
    fn actual_counter_admission_keeps_caller_local_non_send_receipt() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("configured finite authority");
        let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
        // This concrete receipt is a real non-Send read lease of the configured
        // authority record. No marker, Rc-cloned receipt or extra observation.
        let authority_records = RefCell::new(configured);
        let expected_records = authority_records.borrow();
        let acquired = host.block_on(run_counter_profile(
            StartupMode::ExplicitTest,
            Some((&authority, binding, deadline)),
            expected_records.as_slice(),
            async |original, expected| {
                let records = authority_records.borrow();
                let position = records.iter().position(|record| {
                    expected.contains(record)
                        && original.protected.as_ref() == record.protected.as_ref()
                        && original.proof.as_ref() == record.proof.as_ref()
                });
                match position {
                    Some(position) => Ok(Ref::map(records, |records| &records[position])),
                    None => Err(VerificationError::Invalid),
                }
            },
            |record| (&record.scope, record.profile, record.purpose),
            65_536,
            CounterWorkCase::Healthy,
            CounterProofProfile::RecordedTest,
            [
                CounterStatus::IncrementAdmitted,
                CounterStatus::IncrementAdmitted,
                CounterStatus::ReadAdmitted,
            ],
            1,
        ));
        acquired.expect("actual non-Send receipt consumer");
    }
}
#[cfg(test)]
mod pending_verification {
    use super::*;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Preserve the real pending wait, original receipt and sequential native acquisition before revocation oracles."
    )]
    fn canceled_borrowed_verification_observes_known_revocation() {
        for availability in [
            ProviderAvailability::Available,
            ProviderAvailability::Unavailable,
        ] {
            let host = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("explicit host");
            host.block_on(async {
                let provider_entries = AtomicUsize::new(0);
                let mut authority = AssignmentAuthority {
                    host: &host,
                    runtime_generation: 23,
                    grant_issuer: "grant-controller",
                    authority_generation: 7,
                    permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
                    next: Some(29),
                };
                let (binding, deadline) = authority
                    .issue()
                    .unwrap_or_else(|_| panic!("finite assignment"));
                let configured =
                    [
                        configured_counter_grants(binding, CounterProofProfile::RecordedTest)[0]
                            .clone(),
                    ];
                let records = &configured;
                let mut original = Some(counter_request(&records[0]));
                let request_allocations = original
                    .as_ref()
                    .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
                let mut counter_authorization = None;
                let mut reservation = None;
                let mut verification_observations = None;
                let original_storage = &mut original;
                let reservation_storage = &mut reservation;
                let observations_storage = &mut verification_observations;
                let verification_entries = &provider_entries;
                let authorization_storage = &mut counter_authorization;
                let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                    .execute_with(|application| async move {
                        let target = application.root().established_recipient();
                        let address = application.root().address();
                        let interface = application.interface(());
                        let requests = interface
                            .external::<CounterRequests>()
                            .expect("service port");
                        let mut replies =
                            interface.external::<CounterValue>().expect("snapshot port");
                        *authorization_storage = Some(CounterAuthorization::new(
                            StartupMode::ExplicitTest,
                            Some((binding, deadline, target.clone())),
                            "grant-controller",
                            7,
                            &[CounterOperation::Increment, CounterOperation::Read],
                            record_provider::projection,
                            CounterProofProfile::RecordedTest,
                            CounterLimits {
                                incoming: NonZeroUsize::new(2).expect("explicit count"),
                                retained: NonZeroUsize::new(2).expect("explicit count"),
                                individual: NonZeroUsize::new(131_072).expect("explicit bytes"),
                                total: NonZeroUsize::new(262_144).expect("explicit bytes"),
                                verification_maximum: 65_536,
                            },
                        ));
                        let Some(Ok(authorization)) = authorization_storage.as_mut() else {
                            return application.lifecycle().request_shutdown();
                        };
                        *reservation_storage = Some(
                            authorization
                                .reserve(original_storage.take().expect("caller-held original")),
                        );
                        let Some(Ok(correlation)) = reservation_storage.as_ref() else {
                            return application.lifecycle().request_shutdown();
                        };
                        let correlation = *correlation;
                        let held_bytes = authorization.reserved_bytes;
                        let (release, released) = oneshot::channel();
                        {
                            let pending = &mut authorization.pending[0];
                            let request_limits = counter_test_json_limits();
                            let mut verification = pin!(async {
                                if matches!(pending.verification, Verification::Awaiting) {
                                    match released.await {
                                        Ok(()) => {
                                            acquire_verification(pending, |original| {
                                                verification_entries.fetch_add(1, Ordering::SeqCst);
                                                record_provider::verify(
                                                    original,
                                                    records,
                                                    AuthorityFreshness::Current,
                                                    ProviderAvailability::Available,
                                                    CounterProofProfile::RecordedTest,
                                                    CounterProofPurpose::Request,
                                                    binding.caller,
                                                    binding.host,
                                                    &request_limits,
                                                )
                                            })
                                            .await;
                                        }
                                        Err(cause) => {
                                            pending.verification = Verification::Acquired(Err(
                                                VerificationError::Unavailable(cause),
                                            ));
                                        }
                                    }
                                }
                            });
                            let canceled_wait = verification.as_mut().now_or_never();
                            authorization.permission = Permission::Revoked;
                            let after_cancel = verification.as_mut().now_or_never();
                            let held_after_cancel = authorization.reserved_bytes;
                            let released = match availability {
                                ProviderAvailability::Available => Some(release.send(())),
                                ProviderAvailability::Unavailable => {
                                    drop(release);
                                    None
                                }
                            };
                            verification.await;
                            *observations_storage = Some((
                                canceled_wait,
                                after_cancel,
                                held_bytes,
                                held_after_cancel,
                                released,
                                None,
                                None,
                            ));
                        }
                        authorization.associate(0, &requests, &replies.recipient());
                        authorization.attempt(correlation, &requests);
                        let sent = requests
                            .send(&target, CounterMessage::Read(replies.recipient()))
                            .await;
                        let snapshot = replies.receive().await;
                        if let Some((_, _, _, _, _, native, admission)) = observations_storage {
                            *native = Some((sent, snapshot, address));
                            *admission = authorization.consume(correlation);
                        }
                        application.lifecycle().request_shutdown()
                    })
                    .unwrap_or_else(|_| panic!("entered host"));
                let execution = AssertUnwindSafe(execution).catch_unwind().await;
                let native = receiving.await.into_parts();
                let (
                    WorkOutcome::Completed {
                        output: stopped,
                        cleanup: Ok(()),
                    },
                    Ok((origin, retirement)),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }),
                    (),
                ) = native
                else {
                    panic!("actual native and both notification products");
                };
                let _retired_counter = completed_actor(origin, retirement);
                let Some((
                    canceled_wait,
                    after_cancel,
                    held_bytes,
                    held_after_cancel,
                    released,
                    Some((sent, Some(snapshot), address)),
                    admission,
                )) = verification_observations
                else {
                    panic!("original setup/reservation/native observations retained outside Work");
                };
                assert_eq!(snapshot.message, 0);
                assert_eq!(snapshot.from, address);
                assert!(execution.is_ok());
                assert!(stopped.is_ok());
                assert!(matches!(reservation, Some(Ok(_))));
                assert!(canceled_wait.is_none());
                assert!(after_cancel.is_none());
                assert_eq!(held_after_cancel, held_bytes);
                match availability {
                    ProviderAvailability::Available => {
                        assert!(matches!(released, Some(Ok(()))));
                        assert_eq!(provider_entries.load(Ordering::SeqCst), 1);
                    }
                    ProviderAvailability::Unavailable => {
                        assert!(released.is_none());
                        assert_eq!(provider_entries.load(Ordering::SeqCst), 0);
                    }
                }
                assert!(sent.is_ok());
                let Some(CounterAdmission {
                    original,
                    verification,
                }) = admission
                else {
                    panic!("full refused raw/R/native command");
                };
                assert_eq!(
                    Some((original.protected.as_ptr(), original.proof.as_ptr())),
                    request_allocations
                );
                assert_eq!(
                    (original.protected.as_ref(), original.proof.as_ref()),
                    (records[0].protected.as_ref(), records[0].proof.as_ref())
                );
                match (availability, &verification) {
                    (
                        ProviderAvailability::Available,
                        Verification::Native {
                            receipt,
                            delivery:
                                CounterDelivery::Refused {
                                    command: CounterCommand::Message(CounterMessage::Increment),
                                    refusal: CounterRefusal::Revoked,
                                },
                        },
                    ) => assert_eq!(record_provider::projection(receipt).0, &records[0].scope),
                    (
                        ProviderAvailability::Unavailable,
                        Verification::Refused {
                            evidence: Err(VerificationError::Unavailable(cause)),
                            refusal: CounterRefusal::Unavailable,
                        },
                    ) => assert_eq!(cause.to_string(), "channel closed"),
                    _ => panic!("original receipt or unavailable cause must remain classified"),
                }
                let Some(Ok(authorization)) = counter_authorization else {
                    panic!("original setup cause");
                };
                assert!(authorization.pending.is_empty());
                match availability {
                    ProviderAvailability::Available => assert_eq!(authorization.retained.len(), 1),
                    ProviderAvailability::Unavailable => assert!(authorization.retained.is_empty()),
                }
            });
        }
    }
}
// Real Full/denied/permitted controls for the authorized Counter.
// No copied admission engine; uses the exact configured owner and native mailbox.
#[cfg(test)]
mod full_retry {
    use super::*;

    #[derive(Clone, Copy)]
    enum CounterRetryPolicy {
        KnownRevocation,
        DeadlineReached,
        GrantIssuerChanged,
        AuthorityGenerationChanged,
        OperationRemoved,
        Granted,
    }

    #[test]
    fn full_counter_retry_checks_current_permission_and_expiry() {
        for policy in [
            CounterRetryPolicy::KnownRevocation,
            CounterRetryPolicy::DeadlineReached,
            CounterRetryPolicy::GrantIssuerChanged,
            CounterRetryPolicy::AuthorityGenerationChanged,
            CounterRetryPolicy::OperationRemoved,
            CounterRetryPolicy::Granted,
        ] {
            counter_full_retry(policy);
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Preserve the actual Full command through retry and acquire native retirement before permission oracles."
    )]
    fn counter_full_retry(policy: CounterRetryPolicy) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        host.block_on(async {
            let mut authority = AssignmentAuthority {
                host: &host,
                runtime_generation: 23,
                grant_issuer: "grant-controller",
                authority_generation: 7,
                permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
                next: Some(29),
            };
            let (binding, deadline) = authority.issue().expect("finite authorized assignment");
            let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
            let records = &configured;
            let mut original = Some(counter_request(&records[0]));
            let allocations = original
                .as_ref()
                .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
            let mut counter_authorization = None;
            let mut reservation = None;
            let mut observations = None;
            let original_storage = &mut original;
            let authorization_storage = &mut counter_authorization;
            let reservation_storage = &mut reservation;
            let observation_storage = &mut observations;
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let address = application.root().address();
                    let interface = application.interface(());
                    let requests = interface
                        .external::<CounterRequests>()
                        .expect("service port");
                    let mut replies = interface.external::<CounterValue>().expect("snapshot port");
                    *authorization_storage = Some(CounterAuthorization::new(
                        StartupMode::ExplicitTest,
                        Some((binding, deadline, target.clone())),
                        "grant-controller",
                        7,
                        &[CounterOperation::Increment, CounterOperation::Read],
                        record_provider::projection,
                        CounterProofProfile::RecordedTest,
                        CounterLimits {
                            incoming: NonZeroUsize::new(2).expect("finite input"),
                            retained: NonZeroUsize::new(2).expect("finite input"),
                            individual: NonZeroUsize::new(131_072).expect("finite input"),
                            total: NonZeroUsize::new(262_144).expect("finite input"),
                            verification_maximum: 65_536,
                        },
                    ));
                    let Some(Ok(authorization)) = authorization_storage.as_mut() else {
                        return application.lifecycle().request_shutdown();
                    };
                    *reservation_storage = Some(
                        authorization
                            .reserve(original_storage.take().expect("caller-owned original")),
                    );
                    let Some(Ok(correlation)) = reservation_storage.as_ref() else {
                        return application.lifecycle().request_shutdown();
                    };
                    let correlation = *correlation;
                    let request_limits = counter_test_json_limits();
                    acquire_verification(&mut authorization.pending[0], |original| {
                        record_provider::verify(
                            original,
                            records,
                            AuthorityFreshness::Current,
                            ProviderAvailability::Available,
                            CounterProofProfile::RecordedTest,
                            CounterProofPurpose::Request,
                            binding.caller,
                            binding.host,
                            &request_limits,
                        )
                    })
                    .await;
                    authorization.associate(0, &requests, &replies.recipient());
                    // No await occurs while filling: this current-thread actor
                    // cannot drain until owning try_send actually returns Full.
                    // This bound matches the selected Application default in
                    // crates/bombay/src/local/ingress.rs; it sets no new limit.
                    let mut accepted = 0_u64;
                    let mut fill_rejection = None;
                    for _ in 0..=1_024 {
                        match requests.try_send(&target, CounterMessage::Increment) {
                            Ok(()) => accepted += 1,
                            Err(rejection @ (TrySendError::Full(_) | TrySendError::Closed(_))) => {
                                fill_rejection = Some(rejection);
                                break;
                            }
                        }
                    }
                    authorization.attempt(correlation, &requests);
                    let rejected_command = match &authorization.pending[0].verification {
                        Verification::Native {
                            delivery: CounterDelivery::Full(CounterMessage::Increment),
                            ..
                        } => Some(CounterOperation::Increment),
                        _ => None,
                    };
                    if let CounterRetryPolicy::KnownRevocation = policy {
                        authorization.permission = Permission::Revoked;
                    }
                    match policy {
                        CounterRetryPolicy::GrantIssuerChanged => {
                            authorization.grant_issuer = "other-grant-controller";
                        }
                        CounterRetryPolicy::AuthorityGenerationChanged => {
                            authorization.authority_generation = 8;
                        }
                        CounterRetryPolicy::OperationRemoved => {
                            authorization.permitted_operations = &[CounterOperation::Read];
                        }
                        CounterRetryPolicy::KnownRevocation
                        | CounterRetryPolicy::DeadlineReached
                        | CounterRetryPolicy::Granted => {}
                    }
                    let original_deadline = authorization.deadline;
                    let expired_at = match policy {
                        CounterRetryPolicy::DeadlineReached => {
                            // Test-only field update exercises actual internal sampling.
                            // It is not proof that production refreshes a granted deadline.
                            let expired_at = Instant::now();
                            authorization.deadline = expired_at;
                            Some(expired_at)
                        }
                        CounterRetryPolicy::KnownRevocation
                        | CounterRetryPolicy::Granted
                        | CounterRetryPolicy::GrantIssuerChanged
                        | CounterRetryPolicy::AuthorityGenerationChanged
                        | CounterRetryPolicy::OperationRemoved => None,
                    };
                    // Real Read is a FIFO observation of the accepted prefix;
                    // it drains capacity without pretending to acknowledge Increment.
                    let drained = requests
                        .send(&target, CounterMessage::Read(replies.recipient()))
                        .await;
                    let before = match &drained {
                        Ok(()) => replies.receive().await,
                        Err(_) => None,
                    };
                    authorization.attempt(correlation, &requests);
                    let sampled = requests
                        .send(&target, CounterMessage::Read(replies.recipient()))
                        .await;
                    let after = match &sampled {
                        Ok(()) => replies.receive().await,
                        Err(_) => None,
                    };
                    *observation_storage = Some((
                        accepted,
                        fill_rejection,
                        rejected_command,
                        drained,
                        before,
                        sampled,
                        after,
                        address,
                        original_deadline,
                        expired_at,
                    ));
                    // Full raw/R/native-error remains in the outside owner.
                    application.lifecycle().request_shutdown()
                })
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("entered explicit host");
                });
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let acquired = receiving.await.into_parts();
            let (
                WorkOutcome::Completed {
                    output: stopped,
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = acquired
            else {
                panic!("actual native and both notification products");
            };
            let _retired_counter = completed_actor(origin, retirement);
            let Some((
                accepted,
                fill_rejection,
                rejected_command,
                drained,
                Some(before),
                sampled,
                Some(after),
                address,
                original_deadline,
                expired_at,
            )) = observations
            else {
                panic!("original fill rejection and both actual Read observations");
            };
            // Native effect oracle FIRST, after complete native/notification custody.
            let expected = match policy {
                CounterRetryPolicy::Granted => accepted + 1,
                CounterRetryPolicy::KnownRevocation
                | CounterRetryPolicy::DeadlineReached
                | CounterRetryPolicy::GrantIssuerChanged
                | CounterRetryPolicy::AuthorityGenerationChanged
                | CounterRetryPolicy::OperationRemoved => accepted,
            };
            assert_eq!(
                after.message, expected,
                "full retry ignored current permission or expiry"
            );
            assert_eq!(before.message, accepted);
            assert_eq!((before.from, after.from), (address, address));
            assert!(execution.is_ok());
            assert!(stopped.is_ok());
            assert_eq!(accepted, 1_024);
            assert!(matches!(
                fill_rejection,
                Some(TrySendError::Full(CounterMessage::Increment))
            ));
            assert_eq!(rejected_command, Some(CounterOperation::Increment));
            assert!(drained.is_ok());
            assert!(sampled.is_ok());
            let Some(Ok(correlation)) = reservation else {
                panic!("original reservation result");
            };
            let Some(Ok(mut authorization)) = counter_authorization else {
                panic!("original configuration result");
            };
            let held = authorization.reserved_bytes;
            let admission = authorization.consume(correlation);
            let Some(CounterAdmission {
                original,
                verification: Verification::Native { receipt, delivery },
            }) = admission
            else {
                panic!("full original/raw/R/native command remain typed");
            };
            match (policy, delivery) {
                (
                    CounterRetryPolicy::KnownRevocation,
                    CounterDelivery::Refused {
                        command: CounterCommand::Message(CounterMessage::Increment),
                        refusal: CounterRefusal::Revoked,
                    },
                )
                | (
                    CounterRetryPolicy::DeadlineReached,
                    CounterDelivery::Refused {
                        command: CounterCommand::Message(CounterMessage::Increment),
                        refusal: CounterRefusal::Expired,
                    },
                )
                | (
                    CounterRetryPolicy::GrantIssuerChanged
                    | CounterRetryPolicy::AuthorityGenerationChanged
                    | CounterRetryPolicy::OperationRemoved,
                    CounterDelivery::Refused {
                        command: CounterCommand::Message(CounterMessage::Increment),
                        refusal: CounterRefusal::Grant,
                    },
                )
                | (CounterRetryPolicy::Granted, CounterDelivery::IncrementAdmitted) => {}
                (_, retained) => {
                    drop(retained);
                    panic!("original native delivery classification");
                }
            }
            match (policy, expired_at) {
                (CounterRetryPolicy::DeadlineReached, Some(expired_at)) => {
                    assert_eq!(authorization.deadline, expired_at);
                    assert_eq!(original_deadline, deadline);
                }
                (
                    CounterRetryPolicy::KnownRevocation
                    | CounterRetryPolicy::Granted
                    | CounterRetryPolicy::GrantIssuerChanged
                    | CounterRetryPolicy::AuthorityGenerationChanged
                    | CounterRetryPolicy::OperationRemoved,
                    None,
                ) => {
                    assert_eq!(authorization.deadline, original_deadline);
                }
                _ => panic!("exact deadline observations"),
            }
            assert_eq!(
                Some((original.protected.as_ptr(), original.proof.as_ptr())),
                allocations
            );
            assert_eq!(
                (original.protected.as_ref(), original.proof.as_ref()),
                (records[0].protected.as_ref(), records[0].proof.as_ref())
            );
            assert_eq!(record_provider::projection(&receipt).0, &records[0].scope);
            assert!(authorization.pending.is_empty());
            assert_eq!(authorization.retained.len(), 1);
            assert!(authorization.reserved_bytes < held);
        });
    }
}

#[cfg(test)]
#[global_allocator]
static COUNTER_COMPONENT_ALLOCATIONS: AllocCounter<dhat::Alloc> = AllocCounter(dhat::Alloc);

#[cfg(test)]
mod allocation_profile {
    use super::*;
    use alloc_count::AllocSnapshot;
    use std::env::VarError;
    use std::pin::pin;
    use std::process::{self, Command};
    use tokio::runtime::Runtime;

    // Provider-only calibration inside the already isolated owning test process.
    // Caller originals/configuration and the existing host precede the profiler.
    fn measure_counter_provider_refusals<R>(
        host: &Runtime,
        originals: &[CounterRequest; 4],
        configured: &[ProtectedRecord],
        profile: CounterProofProfile,
        binding: Binding,
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
    ) -> [(Result<R, VerificationError>, (u64, u64)); 4] {
        let cases = [
            (
                &originals[0],
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
            ),
            (
                &originals[1],
                AuthorityFreshness::Stale,
                ProviderAvailability::Available,
            ),
            (
                &originals[2],
                AuthorityFreshness::Current,
                ProviderAvailability::Unavailable,
            ),
            (
                &originals[3],
                AuthorityFreshness::Stale,
                ProviderAvailability::Unavailable,
            ),
        ];
        let profiler = dhat::Profiler::builder().testing().build();
        let acquired = cases.map(|(original, freshness, availability)| {
            requested_allocations(|| {
                host.block_on(provider(
                    original,
                    configured,
                    freshness,
                    availability,
                    profile,
                    CounterProofPurpose::Request,
                    binding.caller,
                    binding.host,
                    &counter_test_json_limits(),
                ))
            })
        });
        drop(profiler);
        acquired
    }
    #[expect(
        clippy::type_complexity,
        reason = "Keep the fixed four original provider results beside their independent allocation windows."
    )]
    fn assert_counter_provider_allocations<R>(
        acquired: &[(Result<R, VerificationError>, (u64, u64)); 4],
    ) {
        for ((original, requested), expected) in acquired.iter().zip([
            CounterRefusal::Invalid,
            CounterRefusal::Stale,
            CounterRefusal::Unavailable,
            CounterRefusal::Unavailable,
        ]) {
            match (original, expected) {
                (Err(VerificationError::Invalid), CounterRefusal::Invalid)
                | (Err(VerificationError::Stale), CounterRefusal::Stale) => {}
                (Err(VerificationError::Unavailable(cause)), CounterRefusal::Unavailable) => {
                    println!("actual provider receipt-closure cause: {cause}");
                }
                _ => panic!("original typed provider refusal must match its literal case"),
            }
            assert!(
                requested.1 <= 65_536,
                "actual refused provider requested bytes exceed declared verifier budget"
            );
            println!("provider refusal {expected:?}: requested {requested:?}");
        }
    }

    enum AllocationRole {
        Parent,
        Isolated,
    }
    #[derive(Debug, PartialEq, Eq)]
    enum AllocationProfileOutcome {
        Complete,
        RoleRefused,
        NoNamedTest,
        Failed,
    }
    enum IdentityOrder {
        Increasing,
        Decreasing,
    }
    #[derive(Clone, Copy)]
    enum CounterByteLimit {
        Individual,
        Aggregate,
    }
    struct CounterByteRefusalObservation<R> {
        limit: CounterByteLimit,
        reservation: Result<u64, (CounterRefusal, CounterRequest)>,
        original: Option<CounterAdmission<R>>,
        provider_entries: usize,
        allocations: (*const u8, *const u8),
    }
    struct CounterAllocationObservations<R> {
        authorization: Result<CounterAuthorization<R>, StartupRefusal>,
        header_requests: (u64, u64),
        header_charge: usize,
        constructor_capacities: Option<(usize, usize)>,
        incoming_requests: (u64, u64),
        reservations: [Option<Result<u64, (CounterRefusal, CounterRequest)>>; 2],
        unsubmitted: [Option<CounterRequest>; 2],
        comparison_requests: [(u64, u64); 2],
        verifier_requests: [(u64, u64); 2],
        count_refusal: Option<Result<u64, (CounterRefusal, CounterRequest)>>,
        count_original: Option<CounterAdmission<R>>,
        count_provider_entries: usize,
        overflow_allocations: (*const u8, *const u8),
        canceled_wait: Option<()>,
        second_pending_poll: Option<()>,
        reserved_before_cancel: usize,
        reserved_after_cancel: usize,
        verification_release: Option<Result<(), ()>>,
        reserved_unread: usize,
        reserved_after_report: usize,
        reserved_after_consume: usize,
        originals: [Option<CounterAdmission<R>>; 2],
        original_allocations: [(*const u8, *const u8); 2],
        duplicate_original: Option<CounterAdmission<R>>,
        duplicate_reservation: Option<Result<u64, (CounterRefusal, CounterRequest)>>,
        duplicate_allocations: Option<(*const u8, *const u8)>,
        duplicate_requests: (u64, u64),
        duplicate_ledger_len: usize,
        byte_refusals: [Option<CounterByteRefusalObservation<R>>; 2],
    }
    fn requested_allocations<T>(allocate: impl FnOnce() -> T) -> (T, (u64, u64)) {
        let before = dhat::HeapStats::get();
        let owned = allocate();
        let after = dhat::HeapStats::get();
        (
            owned,
            (
                after.total_blocks - before.total_blocks,
                after.total_bytes - before.total_bytes,
            ),
        )
    }
    fn requested_thread_allocations(allocate: impl FnOnce()) -> (u64, u64) {
        let before = AllocSnapshot::now();
        allocate();
        let requested = before.elapsed();
        let calls = requested
            .alloc_calls
            .checked_add(requested.realloc_calls)
            .expect("finite synchronous allocation and reallocation count");
        (
            u64::try_from(calls).expect("finite synchronous requested allocation count"),
            u64::try_from(requested.bytes_allocated)
                .expect("finite synchronous requested allocation bytes"),
        )
    }
    fn counter_requested_fixture(protected: u8, configured: &[ProtectedRecord]) -> CounterRequest {
        match protected {
            b'I' => counter_request(&configured[0]),
            b'R' => counter_request(&configured[1]),
            _ => CounterRequest {
                protected: Box::from([protected]),
                proof: Box::from([41]),
            },
        }
    }
    #[expect(
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "Preserve contiguous requested-allocation windows and all acquired originals before assertions."
    )]
    async fn measure_counter_custody<R>(
        binding: Binding,
        deadline: Instant,
        target: EstablishedRecipient<Counter>,
        requests: &ExternalActor<CounterRequests>,
        replies: &EstablishedRecipient<CounterValue>,
        configured: &[ProtectedRecord],
        provider: &impl AsyncFn(&CounterRequest, &[ProtectedRecord]) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
        order: IdentityOrder,
    ) -> CounterAllocationObservations<R> {
        // Refused inputs and the release channel belong to the test caller.
        let overflow = counter_requested_fixture(b'X', configured);
        let overflow_allocations = (overflow.protected.as_ptr(), overflow.proof.as_ptr());
        let byte_overflows = [
            counter_requested_fixture(b'Y', configured),
            counter_requested_fixture(b'Y', configured),
        ];
        let (release, released) = oneshot::channel();
        let profiler = dhat::Profiler::builder()
            .file_name(
                env::temp_dir().join(format!("bombay-counter-allocations-{}.json", process::id())),
            )
            .trim_backtraces(None)
            .eprint_json()
            .build();
        let (authorization, header_requests) = requested_allocations(|| {
            CounterAuthorization::new(
                StartupMode::ExplicitTest,
                Some((binding, deadline, target)),
                "grant-controller",
                7,
                &[CounterOperation::Increment, CounterOperation::Read],
                projection,
                profile,
                CounterLimits {
                    incoming: NonZeroUsize::new(2).expect("explicit count"),
                    retained: NonZeroUsize::new(2).expect("explicit count"),
                    individual: NonZeroUsize::new(131_072).expect("explicit bytes"),
                    total: NonZeroUsize::new(262_144).expect("explicit bytes"),
                    verification_maximum: 65_536,
                },
            )
        });
        let (originals, incoming_requests) = requested_allocations(|| match order {
            IdentityOrder::Increasing => [
                counter_requested_fixture(b'I', configured),
                counter_requested_fixture(b'R', configured),
            ],
            IdentityOrder::Decreasing => [
                counter_requested_fixture(b'R', configured),
                counter_requested_fixture(b'I', configured),
            ],
        });
        let original_allocations = originals
            .each_ref()
            .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
        let mut measured = CounterAllocationObservations {
            authorization,
            header_requests,
            header_charge: 0,
            constructor_capacities: None,
            incoming_requests,
            reservations: [None, None],
            unsubmitted: originals.map(Some),
            comparison_requests: [(0, 0); 2],
            verifier_requests: [(0, 0); 2],
            count_refusal: None,
            count_original: None,
            count_provider_entries: 0,
            overflow_allocations,
            canceled_wait: None,
            second_pending_poll: None,
            reserved_before_cancel: 0,
            reserved_after_cancel: 0,
            verification_release: None,
            reserved_unread: 0,
            reserved_after_report: 0,
            reserved_after_consume: 0,
            originals: [None, None],
            original_allocations,
            duplicate_original: None,
            duplicate_reservation: None,
            duplicate_allocations: None,
            duplicate_requests: (0, 0),
            duplicate_ledger_len: 0,
            byte_refusals: [None, None],
        };
        let Ok(authorization) = &mut measured.authorization else {
            drop(profiler);
            return measured;
        };
        measured.header_charge = authorization.reserved_bytes;
        measured.constructor_capacities = Some((
            authorization.pending.capacity(),
            authorization.retained.capacity(),
        ));
        for (original, reservation) in measured
            .unsubmitted
            .iter_mut()
            .zip(&mut measured.reservations)
        {
            if let Some(original) = original.take() {
                *reservation = Some(authorization.reserve(original));
            }
        }
        measured.count_refusal = Some(authorization.reserve(overflow));
        if let Some(Ok(correlation)) = &measured.count_refusal {
            let position = authorization
                .pending
                .iter()
                .position(|pending| pending.correlation == *correlation)
                .expect("actual admitted extra original");
            measured.count_provider_entries += 1;
            acquire_verification(&mut authorization.pending[position], |original| {
                provider(original, configured)
            })
            .await;
            authorization.associate(position, requests, replies);
            measured.count_original = authorization.consume(*correlation);
        }
        if measured
            .reservations
            .iter()
            .any(|reservation| !matches!(reservation, Some(Ok(_))))
        {
            drop(profiler);
            return measured;
        }
        measured.reserved_before_cancel = authorization.reserved_bytes;
        {
            let before = dhat::HeapStats::get();
            let pending = &mut authorization.pending[0];
            let mut verification = pin!(async {
                if matches!(pending.verification, Verification::Awaiting) {
                    match released.await {
                        Ok(()) => {
                            acquire_verification(pending, |original| {
                                provider(original, configured)
                            })
                            .await;
                        }
                        Err(cause) => {
                            pending.verification =
                                Verification::Acquired(Err(VerificationError::Unavailable(cause)));
                        }
                    }
                }
            });
            measured.canceled_wait = verification.as_mut().now_or_never();
            measured.second_pending_poll = verification.as_mut().now_or_never();
            measured.reserved_after_cancel = authorization.reserved_bytes;
            measured.verification_release = Some(release.send(()));
            verification.await;
            let after = dhat::HeapStats::get();
            measured.verifier_requests[0] = (
                after.total_blocks - before.total_blocks,
                after.total_bytes - before.total_bytes,
            );
        }
        let before = dhat::HeapStats::get();
        acquire_verification(&mut authorization.pending[1], |original| {
            provider(original, configured)
        })
        .await;
        let after = dhat::HeapStats::get();
        measured.verifier_requests[1] = (
            after.total_blocks - before.total_blocks,
            after.total_bytes - before.total_bytes,
        );
        drop(profiler);
        authorization.permission = Permission::Revoked;
        for position in 0..2 {
            let requested = requested_thread_allocations(|| {
                authorization.associate(position, requests, replies);
            });
            measured.comparison_requests[position] = requested;
            let correlation = authorization.pending[position].correlation;
            authorization.attempt(correlation, requests);
        }
        measured.reserved_unread = authorization.reserved_bytes;
        for pending in &authorization.pending {
            let _report = authorization.report(pending.correlation);
        }
        measured.reserved_after_report = authorization.reserved_bytes;
        let correlations = measured.reservations.each_ref().map(|reservation| {
            let Some(Ok(correlation)) = reservation else {
                unreachable!("actual retained original");
            };
            *correlation
        });
        for (original, correlation) in measured.originals.iter_mut().zip(correlations) {
            *original = authorization.consume(correlation);
        }
        measured.reserved_after_consume = authorization.reserved_bytes;
        let duplicate = counter_requested_fixture(b'I', configured);
        measured.duplicate_allocations =
            Some((duplicate.protected.as_ptr(), duplicate.proof.as_ptr()));
        measured.duplicate_reservation = Some(authorization.reserve(duplicate));
        if let Some(Ok(correlation)) = &measured.duplicate_reservation {
            let correlation = *correlation;
            let position = authorization
                .pending
                .iter()
                .position(|pending| pending.correlation == correlation)
                .expect("actual duplicate input");
            acquire_verification(&mut authorization.pending[position], |original| {
                provider(original, configured)
            })
            .await;
            let storage = requested_thread_allocations(|| {
                authorization.associate(position, requests, replies);
            });
            measured.duplicate_requests = storage;
            measured.duplicate_ledger_len = authorization.retained.len();
            measured.duplicate_original = authorization.consume(correlation);
        }
        for ((observation, limit), original) in measured
            .byte_refusals
            .iter_mut()
            .zip([CounterByteLimit::Individual, CounterByteLimit::Aggregate])
            .zip(byte_overflows)
        {
            let allocations = (original.protected.as_ptr(), original.proof.as_ptr());
            let previous_limits = (authorization.limits.individual, authorization.limits.total);
            match limit {
                CounterByteLimit::Individual => {
                    authorization.limits.individual = NonZeroUsize::new(65_537)
                        .expect("one byte below the explicit original-plus-verifier charge");
                }
                CounterByteLimit::Aggregate => {
                    authorization.limits.total = NonZeroUsize::new(
                        authorization
                            .reserved_bytes
                            .checked_add(65_537)
                            .expect("finite aggregate capacity plus one"),
                    )
                    .expect("nonzero budget");
                }
            }
            let reservation = authorization.reserve(original);
            let mut provider_entries = 0;
            let mut consumed = None;
            if let Ok(correlation) = &reservation {
                let position = authorization
                    .pending
                    .iter()
                    .position(|pending| pending.correlation == *correlation)
                    .expect("actual admitted extra input");
                provider_entries += 1;
                acquire_verification(&mut authorization.pending[position], |original| {
                    provider(original, configured)
                })
                .await;
                authorization.associate(position, requests, replies);
                consumed = authorization.consume(*correlation);
            }
            *observation = Some(CounterByteRefusalObservation {
                limit,
                reservation,
                original: consumed,
                provider_entries,
                allocations,
            });
            (authorization.limits.individual, authorization.limits.total) = previous_limits;
        }
        measured
    }
    fn assert_counter_storage<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        configured: &[ProtectedRecord],
    ) {
        let Ok(authorization) = &measured.authorization else {
            panic!("original constructor result retained");
        };
        assert_eq!(
            measured.count_provider_entries, 0,
            "Count refuses before the real provider is entered"
        );
        let Some((pending_capacity, retained_capacity)) = measured.constructor_capacities else {
            panic!("actual constructor capacities");
        };
        let headers = pending_capacity
            .checked_mul(size_of::<PendingAdmission<R>>())
            .and_then(|pending| {
                retained_capacity
                    .checked_mul(size_of::<CounterReplayRecord>())
                    .and_then(|retained| pending.checked_add(retained))
            })
            .expect("finite real storage");
        assert!(
            measured.header_requests.1 <= measured.header_charge as u64,
            "physical requested headers are covered by recorded constructor reservation"
        );
        assert_eq!(measured.header_charge, headers);
        let protected_bytes: usize = configured.iter().map(|record| record.protected.len()).sum();
        let proof_bytes: usize = configured.iter().map(|record| record.proof.len()).sum();
        let incoming_bytes = protected_bytes + proof_bytes;
        assert_eq!(measured.incoming_requests, (4, incoming_bytes as u64));
        assert!(
            measured
                .reservations
                .iter()
                .all(|reservation| matches!(reservation, Some(Ok(_))))
        );
        assert!(measured.unsubmitted.iter().all(Option::is_none));
        let Some(Err((CounterRefusal::Count, overflow))) = &measured.count_refusal else {
            panic!("actual Count overflow custody");
        };
        assert_eq!(
            (overflow.protected.as_ptr(), overflow.proof.as_ptr()),
            measured.overflow_allocations
        );
        assert_eq!(overflow.protected.as_ref(), b"X");
        assert_eq!(overflow.proof.as_ref(), &[41]);
        assert!(measured.canceled_wait.is_none());
        assert!(measured.second_pending_poll.is_none());
        assert_eq!(
            measured.reserved_before_cancel,
            headers + incoming_bytes + 2 * 65_536
        );
        assert_eq!(
            measured.reserved_after_cancel,
            measured.reserved_before_cancel
        );
        assert!(matches!(measured.verification_release, Some(Ok(()))));
        for requested in measured.verifier_requests {
            assert!(
                requested.1 <= 65_536,
                "actual provider requests fit the already declared verifier budget"
            );
        }
        let comparison_requested: u64 = measured
            .comparison_requests
            .iter()
            .map(|requested| requested.1)
            .sum();
        eprintln!(
            "Counter comparison allocation windows {:?}; protected bytes {protected_bytes}",
            measured.comparison_requests
        );
        assert_eq!(comparison_requested, protected_bytes as u64);
        assert_eq!(
            measured
                .reserved_unread
                .checked_sub(measured.reserved_after_cancel)
                .expect("comparison-only charge increase") as u64,
            comparison_requested,
            "protected comparison allocation is independently charged"
        );
        assert_eq!(
            measured.reserved_unread,
            headers + incoming_bytes + 2 * 65_536 + protected_bytes
        );
        assert_eq!(measured.reserved_after_report, measured.reserved_unread);
        assert_eq!(measured.reserved_after_consume, headers + protected_bytes);
        assert_counter_refused_commands(measured, projection, configured);
        assert_counter_duplicate_receipt(measured, projection, configured);
        assert_counter_byte_refusals(measured);
        assert!(authorization.pending.is_empty());
        assert_eq!(authorization.retained.len(), 2);
        let finite_requested = measured.header_requests.1
            + measured.incoming_requests.1
            + measured
                .verifier_requests
                .iter()
                .map(|requested| requested.1)
                .sum::<u64>()
            + comparison_requested;
        assert!(finite_requested <= measured.reserved_unread as u64);
        assert!(measured.reserved_unread <= 262_144);
        println!(
            "Counter component requests {finite_requested}; headers {headers}; verifier {:?}; recorded reservation {}",
            measured.verifier_requests, measured.reserved_unread
        );
    }
    fn assert_counter_refused_commands<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        configured: &[ProtectedRecord],
    ) {
        for (admission, allocation) in measured.originals.iter().zip(measured.original_allocations)
        {
            let Some(CounterAdmission {
                original,
                verification:
                    Verification::Native {
                        receipt,
                        delivery:
                            CounterDelivery::Refused {
                                command,
                                refusal: CounterRefusal::Revoked,
                            },
                    },
            }) = admission
            else {
                panic!("full original actual raw/R/native refusal product");
            };
            assert_eq!(
                (original.protected.as_ptr(), original.proof.as_ptr()),
                allocation
            );
            assert_eq!(original.proof.as_ref(), &[41]);
            let expected = configured
                .iter()
                .find(|record| {
                    record.protected.as_ref() == original.protected.as_ref()
                        && record.proof.as_ref() == original.proof.as_ref()
                })
                .expect("independently configured original authority record");
            assert_eq!(projection(receipt).0, &expected.scope);
            let operation = projection(receipt).0.operation;
            assert_eq!(expected.scope.operation, operation);
            assert_eq!(original.protected.as_ref(), expected.protected.as_ref());
            assert!(matches!(
                (operation, command),
                (
                    CounterOperation::Increment,
                    CounterCommand::Message(CounterMessage::Increment)
                ) | (
                    CounterOperation::Read,
                    CounterCommand::Message(CounterMessage::Read(_))
                )
            ));
        }
    }
    fn assert_counter_duplicate_receipt<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        configured: &[ProtectedRecord],
    ) {
        assert_eq!(measured.duplicate_requests, (0, 0));
        assert_eq!(measured.duplicate_ledger_len, 2);
        assert!(matches!(measured.duplicate_reservation, Some(Ok(_))));
        let Some(CounterAdmission {
            original: duplicate,
            ..
        }) = &measured.duplicate_original
        else {
            panic!("actual duplicate original");
        };
        assert_eq!(
            Some((duplicate.protected.as_ptr(), duplicate.proof.as_ptr())),
            measured.duplicate_allocations
        );
        assert_eq!(
            duplicate.protected.as_ref(),
            configured[0].protected.as_ref()
        );
        assert_eq!(duplicate.proof.as_ref(), &[41]);
        let Some(CounterAdmission {
            verification: Verification::Duplicate { receipt, report },
            ..
        }) = &measured.duplicate_original
        else {
            panic!("complete original duplicate receipt and report");
        };
        let expected = configured
            .iter()
            .find(|record| {
                record.scope.operation == CounterOperation::Increment
                    && record.proof.as_ref() == [41]
            })
            .expect("independent duplicate authority record");
        assert_eq!(projection(receipt).0, &expected.scope);
        let Some(Ok(correlation)) = &measured.duplicate_reservation else {
            panic!("original local duplicate correlation");
        };
        assert_eq!(
            *report,
            CounterReport {
                correlation: *correlation,
                status: CounterStatus::Refused(CounterRefusal::Revoked)
            }
        );
    }
    fn assert_counter_byte_refusals<R>(measured: &CounterAllocationObservations<R>) {
        for observed in &measured.byte_refusals {
            let Some(observed) = observed else {
                panic!("actual selected limit observation");
            };
            assert_eq!(
                observed.provider_entries, 0,
                "byte limit refuses before the actual provider enters"
            );
            let expected = match observed.limit {
                CounterByteLimit::Individual => CounterRefusal::IndividualBytes,
                CounterByteLimit::Aggregate => CounterRefusal::TotalBytes,
            };
            let Err((refusal, original)) = &observed.reservation else {
                panic!("actual original byte refusal");
            };
            assert_eq!(*refusal, expected);
            assert_eq!(
                (original.protected.as_ptr(), original.proof.as_ptr()),
                observed.allocations
            );
            assert_eq!(original.protected.as_ref(), b"Y");
            assert_eq!(original.proof.as_ref(), &[41]);
            assert!(observed.original.is_none());
        }
    }
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep isolated-role checks and both concrete provider acquisitions before native and allocation assertions."
    )]
    fn counter_component_allocations_cover_declared_custody() {
        let role = match env::var("BOMBAY_COUNTER_ALLOCATION_ROLE") {
            Err(VarError::NotPresent) => AllocationRole::Parent,
            Ok(value) if value == "isolated" => AllocationRole::Isolated,
            value => panic!("unknown Counter allocation role {value:?}"),
        };
        if let AllocationRole::Parent = role {
            let executable = env::current_exe().expect("actual owning test executable");
            let test = "allocation_profile::counter_component_allocations_cover_declared_custody";
            for (role, filter, expected) in [
                ("unknown", test, AllocationProfileOutcome::RoleRefused),
                ("isolated", "", AllocationProfileOutcome::NoNamedTest),
                (
                    "isolated",
                    "allocation_profile::misspelled",
                    AllocationProfileOutcome::NoNamedTest,
                ),
                ("isolated", test, AllocationProfileOutcome::Complete),
            ] {
                let profile_execution = Command::new(&executable)
                    .args([filter, "--exact", "--test-threads=1", "--nocapture"])
                    .env("BOMBAY_COUNTER_ALLOCATION_ROLE", role)
                    .output()
                    .expect("actual isolated child");
                let stdout = String::from_utf8_lossy(&profile_execution.stdout);
                let stderr = String::from_utf8_lossy(&profile_execution.stderr);
                print!("{stdout}");
                eprint!("{stderr}");
                let observed = if stdout.contains("unknown Counter allocation role")
                    || stderr.contains("unknown Counter allocation role")
                {
                    AllocationProfileOutcome::RoleRefused
                } else if stdout.matches("running 1 test").count() != 1
                    || stdout.matches(&format!("test {test} ...")).count() != 1
                {
                    AllocationProfileOutcome::NoNamedTest
                } else if profile_execution.status.success()
                    && stdout
                        .matches("Counter allocation profile complete")
                        .count()
                        == 1
                {
                    AllocationProfileOutcome::Complete
                } else {
                    AllocationProfileOutcome::Failed
                };
                assert_eq!(
                    observed, expected,
                    "actual child status {:?}",
                    profile_execution.status
                );
                match expected {
                    AllocationProfileOutcome::Complete => {
                        assert!(profile_execution.status.success());
                    }
                    AllocationProfileOutcome::RoleRefused => {
                        assert!(!profile_execution.status.success());
                    }
                    AllocationProfileOutcome::NoNamedTest => {
                        assert!(profile_execution.status.success());
                        assert_eq!(stdout.matches("running 0 tests").count(), 1);
                    }
                    AllocationProfileOutcome::Failed => {
                        unreachable!("expected refusal or completion")
                    }
                }
            }
            return;
        }
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        host.block_on(async {
            let mut authority = AssignmentAuthority {
                host: &host,
                runtime_generation: 23,
                grant_issuer: "grant-controller",
                authority_generation: 7,
                permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
                next: Some(29),
            };
            let (binding, deadline) = authority
                .issue()
                .unwrap_or_else(|_| panic!("finite assignment"));
            let record_configuration =
                configured_counter_grants(binding, CounterProofProfile::RecordedTest);
            let scheduled_configuration =
                configured_counter_grants(binding, CounterProofProfile::ScheduledTest);
            let configured = &record_configuration;
            let scheduled_configured = &scheduled_configuration;
            let mut record = None;
            let mut scheduled = None;
            let mut snapshot = None;
            let mut requester_refusal = None;
            let requester_refusal_storage = &mut requester_refusal;
            let snapshot_storage = &mut snapshot;
            let record_storage = &mut record;
            let scheduled_storage = &mut scheduled;
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let interface = application.interface(());
                    let requests = interface
                        .external::<CounterRequests>()
                        .expect("real service port");
                    let mut replies = interface
                        .external::<CounterValue>()
                        .expect("real reply port");
                    let mut requester = CounterRequester {
                        service: requests.recipient(),
                        counter: target.clone(),
                        replies: replies.recipient(),
                        state: CounterRequesterState::Ready,
                    };
                    let original = CounterRequesterMessage::ProtectedReply {
                        correlation: 67,
                        scope: configured[0].scope,
                        meaning: CounterProtectedMeaning::IncrementAdmitted,
                    };
                    let profiler = dhat::Profiler::builder().testing().build();
                    let acquired = requested_allocations(|| {
                        requester.receive(MailAddr::APPLICATION_ROOT, original)
                    });
                    drop(profiler);
                    *requester_refusal_storage = Some((acquired, requester));
                    *record_storage = Some([
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            configured,
                            &async |original, records| {
                                record_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    CounterProofProfile::RecordedTest,
                                    CounterProofPurpose::Request,
                                    binding.caller,
                                    binding.host,
                                    &counter_test_json_limits(),
                                )
                                .await
                            },
                            record_provider::projection,
                            CounterProofProfile::RecordedTest,
                            IdentityOrder::Increasing,
                        )
                        .await,
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            configured,
                            &async |original, records| {
                                record_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    CounterProofProfile::RecordedTest,
                                    CounterProofPurpose::Request,
                                    binding.caller,
                                    binding.host,
                                    &counter_test_json_limits(),
                                )
                                .await
                            },
                            record_provider::projection,
                            CounterProofProfile::RecordedTest,
                            IdentityOrder::Decreasing,
                        )
                        .await,
                    ]);
                    *scheduled_storage = Some([
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            scheduled_configured,
                            &async |original, records| {
                                scheduled_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    CounterProofProfile::ScheduledTest,
                                    CounterProofPurpose::Request,
                                    binding.caller,
                                    binding.host,
                                    &counter_test_json_limits(),
                                )
                                .await
                            },
                            scheduled_provider::projection,
                            CounterProofProfile::ScheduledTest,
                            IdentityOrder::Increasing,
                        )
                        .await,
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            scheduled_configured,
                            &async |original, records| {
                                scheduled_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    CounterProofProfile::ScheduledTest,
                                    CounterProofPurpose::Request,
                                    binding.caller,
                                    binding.host,
                                    &counter_test_json_limits(),
                                )
                                .await
                            },
                            scheduled_provider::projection,
                            CounterProofProfile::ScheduledTest,
                            IdentityOrder::Decreasing,
                        )
                        .await,
                    ]);
                    let sent = requests
                        .send(&target, CounterMessage::Read(replies.recipient()))
                        .await;
                    *snapshot_storage = Some((sent, None));
                    if matches!(snapshot_storage, Some((Ok(()), _))) {
                        let observed = replies.receive().await;
                        if let Some((_, received)) = snapshot_storage {
                            *received = observed;
                        }
                    }
                    application.lifecycle().request_shutdown()
                })
                .unwrap_or_else(|_| panic!("entered host"));
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let native = receiving.await.into_parts();
            let (
                WorkOutcome::Completed {
                    output: stopped,
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = native
            else {
                panic!("actual full native and both notification products");
            };
            let _retired_counter = completed_actor(origin, retirement);
            assert!(execution.is_ok());
            assert!(stopped.is_ok());
            let Some((Ok(()), Some(snapshot))) = &snapshot else {
                panic!("original snapshot send and native received value");
            };
            assert_eq!(snapshot.from, MailAddr::APPLICATION_ROOT);
            assert_eq!(
                snapshot.message, 0,
                "no protected Counter increment occurred"
            );
            let Some((
                (Err(CounterRequesterError::UnexpectedMessage(original)), requested_allocation),
                requester,
            )) = requester_refusal
            else {
                panic!(
                    "original requester refusal and allocator window retained until native cleanup"
                );
            };
            assert!(
                matches!(*original, CounterRequesterMessage::ProtectedReply {
                correlation: 67, scope, meaning: CounterProtectedMeaning::IncrementAdmitted,
            } if scope == configured[0].scope)
            );
            assert!(matches!(requester.state, CounterRequesterState::Ready));
            assert_eq!(
                requested_allocation,
                (1, size_of::<CounterRequesterMessage>() as u64)
            );
            println!("actual requester refusal Box: requested {requested_allocation:?}");
            let Some(record) = record else {
                panic!("actual record-provider observations");
            };
            let Some(scheduled) = scheduled else {
                panic!("actual scheduled-provider observations");
            };
            for observed in &record {
                assert_counter_storage(observed, record_provider::projection, configured);
            }
            for observed in &scheduled {
                assert_counter_storage(
                    observed,
                    scheduled_provider::projection,
                    scheduled_configured,
                );
            }
        });
        // This code runs only in AllocationRole::Isolated, after the actual
        // Application/native component controls above, with no active profiler.
        let configured = configured_counter_grants(
            Binding {
                deployment: "test-deployment",
                caller: "caller",
                controller: "controller",
                actor: "counter",
                host: "counter-host",
                protocol: "counter-protocol",
                runtime_generation: 23,
                actor_generation: 29,
            },
            CounterProofProfile::RecordedTest,
        );
        let originals = [
            counter_request(
                &counter_record(
                    Scope {
                        identity: Identity {
                            sequence: 999,
                            ..configured[0].scope.identity
                        },
                        ..configured[0].scope
                    },
                    CounterProofProfile::RecordedTest,
                    CounterProtectedMeaning::Request,
                    &[41],
                    &mut [0; 8192],
                )
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("unregistered valid JSON");
                }),
            ),
            counter_requested_fixture(b'I', &configured),
            counter_requested_fixture(b'I', &configured),
            counter_requested_fixture(b'I', &configured),
        ];
        let record_refusals = measure_counter_provider_refusals(
            &host,
            &originals,
            &configured,
            CounterProofProfile::RecordedTest,
            configured[0].scope.identity.binding,
            &record_provider::verify,
        );
        let scheduled_refusals = measure_counter_provider_refusals(
            &host,
            &originals,
            &configured,
            CounterProofProfile::RecordedTest,
            configured[0].scope.identity.binding,
            &scheduled_provider::verify,
        );
        assert_counter_provider_allocations(&record_refusals);
        assert_counter_provider_allocations(&scheduled_refusals);

        println!("Counter allocation profile complete");
    }
}

#[cfg(test)]
// Local provider availability/freshness classifications only.
// This supplies no actor-admission or native-retirement acceptance evidence.
#[test]
fn finite_provider_contract_preserves_stale_and_unavailable() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("explicit host");
    host.block_on(async {
        let binding = Binding {
            deployment: "test-deployment",
            caller: "caller",
            controller: "controller",
            actor: "counter",
            host: "counter-host",
            protocol: "counter-protocol",
            runtime_generation: 23,
            actor_generation: 29,
        };
        let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
        let original = counter_request(&configured[0]);
        let record = record_provider::verify(
            &original,
            &configured,
            AuthorityFreshness::Stale,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        )
        .await;
        let scheduled = scheduled_provider::verify(
            &original,
            &configured,
            AuthorityFreshness::Stale,
            ProviderAvailability::Unavailable,
            CounterProofProfile::ScheduledTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        )
        .await;
        assert!(matches!(record, Err(VerificationError::Stale)));
        assert!(matches!(scheduled, Err(VerificationError::Unavailable(_))));
    });
}

// Explicit local provider cases through the existing outside-Work acquisition.
#[cfg(test)]
mod provider_refusals {
    use super::*;

    #[derive(Clone, Copy)]
    enum CounterEvidenceCase {
        Accepted,
        Invalid,
        Stale,
        UnavailableCurrent,
        UnavailableStale,
    }

    #[test]
    fn record_provider_outcomes_reach_the_actual_counter_requester() {
        counter_provider_outcomes(
            &record_provider::verify,
            record_provider::projection,
            CounterProofProfile::RecordedTest,
        );
    }

    #[test]
    fn scheduled_provider_outcomes_reach_the_actual_counter_requester() {
        counter_provider_outcomes(
            &scheduled_provider::verify,
            scheduled_provider::projection,
            CounterProofProfile::ScheduledTest,
        );
    }

    fn counter_provider_outcomes<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        for case in [
            CounterEvidenceCase::Accepted,
            CounterEvidenceCase::Invalid,
            CounterEvidenceCase::Stale,
            CounterEvidenceCase::UnavailableCurrent,
            CounterEvidenceCase::UnavailableStale,
        ] {
            let (binding, deadline) = host
                .block_on(async { authority.issue() })
                .expect("finite configured assignment");
            let configured = configured_counter_grants(binding, profile);
            let (records, freshness, availability, expected_reports, expected_snapshot) = match case
            {
                CounterEvidenceCase::Accepted => (
                    configured.as_slice(),
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    [
                        CounterStatus::IncrementAdmitted,
                        CounterStatus::IncrementAdmitted,
                        CounterStatus::ReadAdmitted,
                    ],
                    1,
                ),
                CounterEvidenceCase::Invalid => (
                    &[][..],
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    [CounterStatus::Refused(CounterRefusal::Invalid); 3],
                    0,
                ),
                CounterEvidenceCase::Stale => (
                    configured.as_slice(),
                    AuthorityFreshness::Stale,
                    ProviderAvailability::Available,
                    [CounterStatus::Refused(CounterRefusal::Stale); 3],
                    0,
                ),
                CounterEvidenceCase::UnavailableCurrent => (
                    configured.as_slice(),
                    AuthorityFreshness::Current,
                    ProviderAvailability::Unavailable,
                    [CounterStatus::Refused(CounterRefusal::Unavailable); 3],
                    0,
                ),
                CounterEvidenceCase::UnavailableStale => (
                    configured.as_slice(),
                    AuthorityFreshness::Stale,
                    ProviderAvailability::Unavailable,
                    [CounterStatus::Refused(CounterRefusal::Unavailable); 3],
                    0,
                ),
            };
            // The actual existing function owns acquisition and all native oracles.
            let result = host.block_on(run_counter_profile(
                StartupMode::ExplicitTest,
                Some((&authority, binding, deadline)),
                records,
                async |original, records| {
                    provider(
                        original,
                        records,
                        freshness,
                        availability,
                        profile,
                        CounterProofPurpose::Request,
                        binding.caller,
                        binding.host,
                        &counter_test_json_limits(),
                    )
                    .await
                },
                projection,
                65_536,
                CounterWorkCase::Healthy,
                profile,
                expected_reports,
                expected_snapshot,
            ));
            result.expect("configured provider profile refused startup");
        }
    }
}

#[cfg(test)]
mod complete_scope {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum CounterScopeCase {
        Valid,
        Deployment,
        Caller,
        Controller,
        Actor,
        Host,
        Protocol,
        RuntimeGeneration,
        ActorGeneration,
        GrantIssuer,
        AuthorityGeneration,
        IncrementOnly,
        ReadOnly,
        OldAssignment,
    }

    #[test]
    fn record_scopes_refuse_before_replay_and_native_mutation() {
        counter_scopes(
            &record_provider::verify,
            record_provider::projection,
            CounterProofProfile::RecordedTest,
        );
    }

    #[test]
    fn scheduled_scopes_refuse_before_replay_and_native_mutation() {
        counter_scopes(
            &scheduled_provider::verify,
            scheduled_provider::projection,
            CounterProofProfile::ScheduledTest,
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Explicit independent scope inputs use the existing full native acquisition before oracles."
    )]
    fn counter_scopes<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (previous, _) = host
            .block_on(async { authority.issue() })
            .expect("previous assignment");
        for case in [
            CounterScopeCase::Valid,
            CounterScopeCase::Deployment,
            CounterScopeCase::Caller,
            CounterScopeCase::Controller,
            CounterScopeCase::Actor,
            CounterScopeCase::Host,
            CounterScopeCase::Protocol,
            CounterScopeCase::RuntimeGeneration,
            CounterScopeCase::ActorGeneration,
            CounterScopeCase::GrantIssuer,
            CounterScopeCase::AuthorityGeneration,
            CounterScopeCase::IncrementOnly,
            CounterScopeCase::ReadOnly,
            CounterScopeCase::OldAssignment,
        ] {
            println!("actual finite Counter Scope case: {case:?}");
            authority.permitted_operations = &[CounterOperation::Increment, CounterOperation::Read];
            let (binding, deadline) = host
                .block_on(async { authority.issue() })
                .expect("new authorized assignment");
            let mut configured = configured_counter_grants(binding, profile);
            for record in &mut configured {
                let scope = &mut record.scope;
                match case {
                    CounterScopeCase::Valid
                    | CounterScopeCase::IncrementOnly
                    | CounterScopeCase::ReadOnly => {}
                    CounterScopeCase::Deployment => {
                        scope.identity.binding.deployment = "other-deployment";
                    }
                    CounterScopeCase::Caller => scope.identity.binding.caller = "other-caller",
                    CounterScopeCase::Controller => {
                        scope.identity.binding.controller = "other-controller";
                    }
                    CounterScopeCase::Actor => scope.identity.binding.actor = "other-actor",
                    CounterScopeCase::Host => scope.identity.binding.host = "other-host",
                    CounterScopeCase::Protocol => {
                        scope.identity.binding.protocol = "other-protocol";
                    }
                    CounterScopeCase::RuntimeGeneration => {
                        scope.identity.binding.runtime_generation = 24;
                    }
                    CounterScopeCase::ActorGeneration => {
                        scope.identity.binding.actor_generation = 0;
                    }
                    CounterScopeCase::GrantIssuer => scope.grant_issuer = "other-grant-controller",
                    CounterScopeCase::AuthorityGeneration => scope.authority_generation = 8,
                    CounterScopeCase::OldAssignment => scope.identity.binding = previous,
                }
            }
            for record in &mut configured {
                *record = counter_record(
                    record.scope,
                    profile,
                    CounterProtectedMeaning::Request,
                    &[41],
                    &mut [0; 8192],
                )
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("configured foreign Scope JSON");
                });
            }
            let (expected, snapshot) = match case {
                CounterScopeCase::Valid => (
                    [
                        CounterStatus::IncrementAdmitted,
                        CounterStatus::IncrementAdmitted,
                        CounterStatus::ReadAdmitted,
                    ],
                    1,
                ),
                CounterScopeCase::IncrementOnly => {
                    authority.permitted_operations = &[CounterOperation::Increment];
                    (
                        [
                            CounterStatus::IncrementAdmitted,
                            CounterStatus::IncrementAdmitted,
                            CounterStatus::Refused(CounterRefusal::Grant),
                        ],
                        1,
                    )
                }
                CounterScopeCase::ReadOnly => {
                    authority.permitted_operations = &[CounterOperation::Read];
                    (
                        [
                            CounterStatus::Refused(CounterRefusal::Grant),
                            CounterStatus::Refused(CounterRefusal::Grant),
                            CounterStatus::ReadAdmitted,
                        ],
                        0,
                    )
                }
                CounterScopeCase::GrantIssuer | CounterScopeCase::AuthorityGeneration => {
                    ([CounterStatus::Refused(CounterRefusal::Grant); 3], 0)
                }
                CounterScopeCase::Deployment
                | CounterScopeCase::Caller
                | CounterScopeCase::Controller
                | CounterScopeCase::Actor
                | CounterScopeCase::Host
                | CounterScopeCase::Protocol
                | CounterScopeCase::RuntimeGeneration
                | CounterScopeCase::ActorGeneration
                | CounterScopeCase::OldAssignment => {
                    ([CounterStatus::Refused(CounterRefusal::Binding); 3], 0)
                }
            };
            let original = host.block_on(run_counter_profile(
                StartupMode::ExplicitTest,
                Some((&authority, binding, deadline)),
                &configured,
                async |request, records| {
                    provider(
                        request,
                        records,
                        AuthorityFreshness::Current,
                        ProviderAvailability::Available,
                        profile,
                        CounterProofPurpose::Request,
                        configured[0].scope.identity.binding.caller,
                        configured[0].scope.identity.binding.host,
                        &counter_test_json_limits(),
                    )
                    .await
                },
                projection,
                65_536,
                CounterWorkCase::Healthy,
                profile,
                expected,
                snapshot,
            ));
            original.expect("selected finite Scope consumer preserves originals");
        }
    }
}

#[cfg(test)]
mod runtime_scope {
    use super::*;

    async fn counter_on_assignment(
        mode: StartupMode,
        assignment: Option<(&AssignmentAuthority<'_>, Binding, Instant)>,
        configured: &[ProtectedRecord],
    ) -> Result<(), StartupRefusal> {
        Box::pin(run_counter_profile(
            mode,
            assignment,
            configured,
            async |original, records| {
                record_provider::verify(
                    original,
                    records,
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    CounterProofProfile::RecordedTest,
                    CounterProofPurpose::Request,
                    assignment.expect("host guard precedes provider").1.caller,
                    assignment.expect("host guard precedes provider").1.host,
                    &counter_test_json_limits(),
                )
                .await
            },
            record_provider::projection,
            65_536,
            CounterWorkCase::Healthy,
            CounterProofProfile::RecordedTest,
            [
                CounterStatus::IncrementAdmitted,
                CounterStatus::IncrementAdmitted,
                CounterStatus::ReadAdmitted,
            ],
            1,
        ))
        .await
    }

    #[test]
    fn issued_assignment_cannot_move_between_live_hosts() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owning host");
        let other = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("other live host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("issued on owning host");
        let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
        let acquired = other.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, binding, deadline)),
            &configured,
        ));
        let entered = other.handle().id();
        let expected = host.handle().id();
        let Err(StartupRefusal::RuntimeMismatch {
            expected: refused_owner,
            entered: refused_host,
        }) = acquired
        else {
            panic!("moving an issued assignment must refuse before export");
        };
        assert_eq!(refused_owner, expected);
        assert_eq!(refused_host, entered);
        assert_ne!(expected, entered);
        assert_eq!(authority.next, Some(30));
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep original startup causes, independent native consumers and checked issuance observations together."
    )]
    fn cold_invalid_and_exhausted_authority_never_reissues_an_assignment() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owning host");
        let other = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("other live host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let cold = counter_on_assignment(StartupMode::ExplicitTest, None, &[])
            .now_or_never()
            .expect("cold refusal is ready");
        let production = counter_on_assignment(StartupMode::ProductionRequired, None, &[])
            .now_or_never()
            .expect("fake production refuses before startup");
        let unavailable = authority.issue();
        let wrong_host = other.block_on(async { authority.issue() });
        let next_after_refusals = authority.next;
        let (first, first_deadline) = host
            .block_on(async { authority.issue() })
            .expect("first finite assignment");
        let unavailable_consumer = counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &[],
        )
        .now_or_never()
        .expect("unentered consumer refuses immediately");
        let first_result = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &configured_counter_grants(first, CounterProofProfile::RecordedTest),
        ));
        authority.grant_issuer = "";
        let empty_issuer = host.block_on(async { authority.issue() });
        let invalid_consumer = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &configured_counter_grants(first, CounterProofProfile::RecordedTest),
        ));
        authority.grant_issuer = "grant-controller";
        authority.permitted_operations = &[];
        let empty_operations = host.block_on(async { authority.issue() });
        let empty_consumer = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &configured_counter_grants(first, CounterProofProfile::RecordedTest),
        ));
        let next_after_grant_refusals = authority.next;
        authority.permitted_operations = &[CounterOperation::Increment, CounterOperation::Read];
        let (replacement, replacement_deadline) = host
            .block_on(async { authority.issue() })
            .expect("replacement finite assignment");
        let replacement_result = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, replacement, replacement_deadline)),
            &configured_counter_grants(replacement, CounterProofProfile::RecordedTest),
        ));
        authority.next = Some(u64::MAX);
        let last = host.block_on(async { authority.issue() });
        let exhausted = host.block_on(async { authority.issue() });
        let exhausted_again = host.block_on(async { authority.issue() });
        assert!(matches!(cold, Err(StartupRefusal::MissingAssignment)));
        assert!(matches!(production, Err(StartupRefusal::TestProvider)));
        let Err(StartupRefusal::RuntimeUnavailable(cause)) = unavailable else {
            panic!("retain actual unavailable-host cause");
        };
        assert!(cause.is_missing_context());
        let Err(StartupRefusal::RuntimeMismatch { expected, entered }) = wrong_host else {
            panic!("wrong host cannot issue authority");
        };
        assert_eq!(expected, host.handle().id());
        assert_eq!(entered, other.handle().id());
        assert_eq!(next_after_refusals, Some(29));
        let Err(StartupRefusal::RuntimeUnavailable(cause)) = &unavailable_consumer else {
            panic!("consumer retains original host cause");
        };
        let reported = unavailable_consumer
            .as_ref()
            .err()
            .and_then(Error::source)
            .expect("actual host error source");
        assert_eq!(
            ptr::from_ref(reported).cast::<()>(),
            ptr::from_ref(cause).cast::<()>()
        );
        assert!(cause.is_missing_context());
        assert!(first_result.is_ok());
        assert!(replacement_result.is_ok());
        assert_eq!(first.runtime_generation, 23);
        assert_eq!(replacement.runtime_generation, first.runtime_generation);
        assert_eq!(first.actor_generation, 29);
        assert_eq!(replacement.actor_generation, 30);
        assert_eq!(next_after_grant_refusals, Some(30));
        assert!(matches!(empty_issuer, Err(StartupRefusal::Grant)));
        assert!(matches!(empty_operations, Err(StartupRefusal::Grant)));
        assert!(matches!(invalid_consumer, Err(StartupRefusal::Grant)));
        assert!(matches!(empty_consumer, Err(StartupRefusal::Grant)));
        let (last, _) = last.expect("last finite assignment can be issued");
        assert_eq!(last.actor_generation, u64::MAX);
        assert!(matches!(exhausted, Err(StartupRefusal::Exhausted)));
        assert!(matches!(exhausted_again, Err(StartupRefusal::Exhausted)));
        assert_eq!(authority.next, None);
    }
}

// Finite proof-record rotation: service facts stay outside Work.
// Requester result consumption is covered by the existing provider-outcome tests.
#[cfg(test)]
mod proof_rotation {
    use super::*;

    #[derive(Clone, Copy)]
    enum CounterProof {
        Original,
        Replacement,
        Retired,
    }

    #[test]
    fn record_proof_rotation_preserves_the_same_live_replay_owner() {
        counter_proof_rotation(
            &record_provider::verify,
            record_provider::projection,
            CounterProofProfile::RecordedTest,
        );
    }

    #[test]
    fn scheduled_proof_rotation_preserves_the_same_live_replay_owner() {
        counter_proof_rotation(
            &scheduled_provider::verify,
            scheduled_provider::projection,
            CounterProofProfile::ScheduledTest,
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Acquire every proof-rotation receipt and native retirement before comparing original authority facts."
    )]
    fn counter_proof_rotation<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        host.block_on(async {
            let mut authority = AssignmentAuthority {
                host: &host,
                runtime_generation: 23,
                grant_issuer: "grant-controller",
                authority_generation: 7,
                permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
                next: Some(29),
            };
            let (binding, deadline) = authority.issue().expect("finite authorized assignment");
            let (publication, ports) = oneshot::channel();
            let (permitted, permission) = oneshot::channel();
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let interface = application.interface(());
                    let requests = interface
                        .external::<CounterRequests>()
                        .expect("typed service port");
                    let replies = interface
                        .external::<CounterValue>()
                        .expect("serial native Read port");
                    let publication = publication.send((
                        application.lifecycle(),
                        application.root().address(),
                        application.root().established_recipient(),
                        requests,
                        replies,
                    ));
                    // Work owns only publication/permission, never service facts.
                    let permission = permission.await;
                    (publication, permission)
                })
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("entered explicit Counter host");
                });
            let rotation = async {
                let mut service = None;
                let mut allocations = Vec::new();
                let mut originals = Vec::new();
                let mut observations = Vec::new();
                let mut reserve_refusals = Vec::new();
                let mut stopped = None;
                let mut target_address = None;
                let handoff;
                match ports.await {
                    Err(original) => handoff = Err(original),
                    Ok((lifecycle, address, target, requests, mut replies)) => {
                        handoff = Ok(());
                        target_address = Some(address);
                        service = Some(CounterAuthorization::new(
                            StartupMode::ExplicitTest,
                            Some((binding, deadline, target.clone())),
                            "grant-controller",
                            7,
                            &[CounterOperation::Increment, CounterOperation::Read],
                            projection,
                            profile,
                            CounterLimits {
                                incoming: NonZeroUsize::new(2).expect("configured input"),
                                retained: NonZeroUsize::new(2).expect("configured input"),
                                individual: NonZeroUsize::new(131_072).expect("configured input"),
                                total: NonZeroUsize::new(262_144).expect("configured input"),
                                verification_maximum: 65_536,
                            },
                        ));
                        if let Some(Ok(authorization)) = service.as_mut() {
                            let mut configured = configured_counter_grants(binding, profile);
                            for proof in [
                                CounterProof::Original,
                                CounterProof::Replacement,
                                CounterProof::Retired,
                            ] {
                                if let CounterProof::Replacement = proof {
                                    // This is trusted configuration, not incoming bytes.
                                    // The same authorization owner/identity stays live.
                                    configured[0].proof = Box::from([42]);
                                }
                                let proof_byte = match proof {
                                    CounterProof::Original | CounterProof::Retired => 41,
                                    CounterProof::Replacement => 42,
                                };
                                let original = CounterRequest {
                                    protected: configured[0].protected.clone(),
                                    proof: Box::from([proof_byte]),
                                };
                                allocations.push((
                                    original.protected.as_ptr(),
                                    original.proof.as_ptr(),
                                    proof_byte,
                                ));
                                let correlation = match authorization.reserve(original) {
                                    Ok(correlation) => correlation,
                                    Err(original) => {
                                        reserve_refusals.push(original);
                                        break;
                                    }
                                };
                                let position = authorization
                                    .pending
                                    .iter()
                                    .position(|entry| entry.correlation == correlation)
                                    .expect("new owning reservation");
                                let request_limits = counter_test_json_limits();
                                acquire_verification(
                                    &mut authorization.pending[position],
                                    |original| {
                                        provider(
                                            original,
                                            &configured,
                                            AuthorityFreshness::Current,
                                            ProviderAvailability::Available,
                                            profile,
                                            CounterProofPurpose::Request,
                                            binding.caller,
                                            binding.host,
                                            &request_limits,
                                        )
                                    },
                                )
                                .await;
                                authorization.associate(position, &requests, &replies.recipient());
                                authorization.attempt(correlation, &requests);
                                let report = authorization.report(correlation);
                                let read = requests
                                    .send(&target, CounterMessage::Read(replies.recipient()))
                                    .await;
                                // Store the native send result BEFORE waiting for its reply.
                                observations.push((proof, correlation, report, read, None));
                                if let Some((_, _, _, Ok(()), snapshot)) = observations.last_mut() {
                                    *snapshot = replies.receive().await;
                                }
                                // Caller receives the complete original; ledger comparison
                                // remains live independently of incoming-result consumption.
                                let original = authorization.consume(correlation);
                                originals.push((proof, correlation, original));
                            }
                        }
                        stopped = Some(lifecycle.request_shutdown());
                    }
                }
                let permitted = permitted.send(());
                (
                    service,
                    allocations,
                    originals,
                    observations,
                    reserve_refusals,
                    stopped,
                    target_address,
                    handoff,
                    permitted,
                )
            };
            let (execution, acquired) =
                tokio::join!(AssertUnwindSafe(execution).catch_unwind(), rotation);
            let counter_product = receiving.await.into_parts();
            let (
                WorkOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = counter_product
            else {
                panic!("actual Counter native and both notification products");
            };
            let _retired_counter = completed_actor(origin, retirement);
            let (
                service,
                allocations,
                originals,
                observations,
                reserve_refusals,
                stopped,
                target_address,
                handoff,
                permitted,
            ) = acquired;
            // Every authoritative original and native receipt is now acquired.
            assert_eq!(observations.len(), 3);
            for (proof, correlation, report, read, snapshot) in observations {
                let Some(snapshot) = snapshot else {
                    panic!("actual live Counter snapshot");
                };
                assert_eq!(
                    snapshot.message, 1,
                    "proof rotation erased live replay retention"
                );
                assert_eq!(Some(snapshot.from), target_address);
                assert!(read.is_ok());
                let Some(report) = report else {
                    panic!("original report remains acquired");
                };
                assert_eq!(report.correlation, correlation);
                let expected = match proof {
                    CounterProof::Original | CounterProof::Replacement => {
                        CounterStatus::IncrementAdmitted
                    }
                    CounterProof::Retired => CounterStatus::Refused(CounterRefusal::Invalid),
                };
                assert_eq!(report.status, expected);
            }
            assert!(execution.is_ok());
            assert!(publication.is_ok());
            assert!(permission.is_ok());
            assert!(permitted.is_ok());
            assert!(handoff.is_ok());
            assert!(matches!(stopped, Some(Ok(()))));
            assert!(reserve_refusals.is_empty());
            assert_eq!(originals.len(), 3);
            assert_eq!(allocations.len(), originals.len());
            for ((proof, correlation, original), (protected, evidence, byte)) in
                originals.into_iter().zip(allocations)
            {
                let Some(CounterAdmission {
                    original,
                    verification,
                }) = original
                else {
                    panic!("full caller-owned original provider result");
                };
                assert_eq!(
                    (original.protected.as_ptr(), original.proof.as_ptr()),
                    (protected, evidence)
                );
                assert_eq!(
                    original.protected.as_ref(),
                    configured_counter_grants(binding, profile)[0]
                        .protected
                        .as_ref()
                );
                assert_eq!(original.proof.as_ref(), &[byte]);
                match (proof, verification) {
                    (
                        CounterProof::Original,
                        Verification::Native {
                            receipt,
                            delivery: CounterDelivery::IncrementAdmitted,
                        },
                    ) => {
                        assert_eq!(
                            projection(&receipt).0,
                            &configured_counter_grants(binding, profile)[0].scope
                        );
                    }
                    (CounterProof::Replacement, Verification::Duplicate { receipt, report }) => {
                        let expected_scope = configured_counter_grants(binding, profile)[0].scope;
                        assert_eq!(projection(&receipt).0, &expected_scope);
                        assert_eq!(report.correlation, correlation);
                        assert_eq!(report.status, CounterStatus::IncrementAdmitted);
                    }
                    (
                        CounterProof::Retired,
                        Verification::Refused {
                            evidence: Err(VerificationError::Invalid),
                            refusal: CounterRefusal::Invalid,
                        },
                    ) => {}
                    (_, original) => {
                        drop(original);
                        panic!("actual admitted, duplicate or invalid original");
                    }
                }
            }
            let Some(Ok(authorization)) = service else {
                panic!("outside-Work authorization remains owned");
            };
            assert_eq!(authorization.binding, binding);
            assert_eq!(authorization.deadline, deadline);
            assert!(authorization.pending.is_empty());
            assert_eq!(authorization.retained.len(), 1);
            assert_eq!(
                authorization.retained[0].identity,
                Identity {
                    binding,
                    sequence: 0
                }
            );
            assert_eq!(
                authorization.retained[0].protected.as_ref(),
                configured_counter_grants(binding, profile)[0]
                    .protected
                    .as_ref()
            );
            assert!(matches!(
                authorization.retained[0].retention,
                CounterReplayRetention::Consumed(CounterReport {
                    status: CounterStatus::IncrementAdmitted,
                    ..
                })
            ));
        });
    }
}

#[cfg(test)]
mod closed_counter {
    use super::*;
    use bombay::{ActorFailureAssessment, RetirementAssessment};

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Retire the exact target, acquire the native Closed command and join all owners before custody assertions."
    )]
    fn retired_target_returns_the_original_closed_command() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit owning host");
        host.block_on(async {
            let mut authority = AssignmentAuthority {
                host: &host,
                runtime_generation: 23,
                grant_issuer: "grant-controller",
                authority_generation: 7,
                permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
                next: Some(29),
            };
            let (binding, deadline) = authority
                .issue()
                .unwrap_or_else(|_| panic!("finite configured authority"));
            let configured = configured_counter_grants(binding, CounterProofProfile::RecordedTest);
            let mut original = Some(counter_request(&configured[0]));
            let pointers = original
                .as_ref()
                .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
            let (publication, ports) = oneshot::channel();
            let (permitted, permission) = oneshot::channel();
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let lifecycle = application.lifecycle();
                    let target = application.root().established_recipient();
                    let interface = application.interface(());
                    let requests = interface
                        .external::<CounterRequests>()
                        .expect("actual native service port");
                    let replies = interface
                        .external::<CounterValue>()
                        .expect("actual result constructor recipient");
                    let publication = publication.send((lifecycle, target, requests, replies));
                    let permission = permission.await;
                    (publication, permission)
                })
                .unwrap_or_else(|_| panic!("entered owning host"));
            let mut handoff = None;
            let mut stopped = None;
            let mut retirement = None;
            let mut authorization = None;
            let mut reservation = None;
            let mut report = None;
            let mut admission = None;
            let mut release = None;
            let service = async {
                match ports.await {
                    Ok((lifecycle, target, requests, replies)) => {
                        handoff = Some(Ok(()));
                        stopped = Some(lifecycle.request_shutdown());
                        retirement = Some(lifecycle.retirement().await);
                        authorization = Some(CounterAuthorization::new(
                            StartupMode::ExplicitTest,
                            Some((binding, deadline, target)),
                            "grant-controller",
                            7,
                            &[CounterOperation::Increment, CounterOperation::Read],
                            record_provider::projection,
                            CounterProofProfile::RecordedTest,
                            CounterLimits {
                                incoming: NonZeroUsize::new(1).expect("explicit incoming count"),
                                retained: NonZeroUsize::new(1).expect("explicit retained count"),
                                individual: NonZeroUsize::new(131_072).expect("explicit bytes"),
                                total: NonZeroUsize::new(262_144).expect("explicit bytes"),
                                verification_maximum: 65_536,
                            },
                        ));
                        if let Some(Ok(authorization)) = &mut authorization {
                            if let Some(original) = original.take() {
                                reservation = Some(authorization.reserve(original));
                            }
                            if let Some(Ok(correlation)) = &reservation
                                && let Some(position) = authorization
                                    .pending
                                    .iter()
                                    .position(|pending| pending.correlation == *correlation)
                            {
                                let request_limits = counter_test_json_limits();
                                acquire_verification(
                                    &mut authorization.pending[position],
                                    |original| {
                                        record_provider::verify(
                                            original,
                                            &configured,
                                            AuthorityFreshness::Current,
                                            ProviderAvailability::Available,
                                            CounterProofProfile::RecordedTest,
                                            CounterProofPurpose::Request,
                                            binding.caller,
                                            binding.host,
                                            &request_limits,
                                        )
                                    },
                                )
                                .await;
                                authorization.associate(position, &requests, &replies.recipient());
                                authorization.attempt(*correlation, &requests);
                                report = authorization.report(*correlation);
                                admission = authorization.consume(*correlation);
                            }
                        }
                    }
                    Err(cause) => handoff = Some(Err(cause)),
                }
                release = Some(permitted.send(()));
            };
            let (execution, ()) = tokio::join!(AssertUnwindSafe(execution).catch_unwind(), service);
            let native = receiving.await.into_parts();
            let (
                WorkOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, native_retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = native
            else {
                panic!("full original Work/native/two-notification products");
            };
            let _retired_counter = completed_actor(origin, native_retirement);
            assert!(execution.is_ok());
            assert!(publication.is_ok());
            assert!(permission.is_ok());
            assert!(matches!(handoff, Some(Ok(()))));
            assert!(matches!(stopped, Some(Ok(()))));
            assert!(matches!(release, Some(Ok(()))));
            let Some(retirement) = retirement else {
                panic!("actual joined-root retirement before admission");
            };
            assert_eq!(retirement.retirement(), RetirementAssessment::Established);
            assert_eq!(
                retirement.failures(),
                ActorFailureAssessment::NoFailuresFound
            );
            let Some(Ok(correlation)) = reservation else {
                panic!("actual local reservation preserves its original outcome");
            };
            assert_eq!(
                report,
                Some(CounterReport {
                    correlation,
                    status: CounterStatus::Closed,
                }),
                "retired exact target reports native Closed rather than Full or Accepted"
            );
            let Some(CounterAdmission {
                original: recovered,
                verification:
                    Verification::Native {
                        receipt,
                        delivery: CounterDelivery::Closed(original_error),
                    },
            }) = &admission
            else {
                panic!("original native Closed error and Increment command remain together");
            };
            let TrySendError::Closed(command) = original_error else {
                panic!("actual native refusal is Closed, with its command still owned");
            };
            assert!(matches!(command, CounterMessage::Increment));
            assert_eq!(
                recovered.protected.as_ref(),
                configured[0].protected.as_ref()
            );
            assert_eq!(recovered.proof.as_ref(), &[41]);
            assert_eq!(
                Some((recovered.protected.as_ptr(), recovered.proof.as_ptr())),
                pointers
            );
            assert_eq!(record_provider::projection(receipt).0, &configured[0].scope);
            assert!(original.is_none());
            let Some(Ok(authorization)) = &authorization else {
                panic!("actual original service constructor result");
            };
            assert!(authorization.pending.is_empty());
            assert_eq!(authorization.retained.len(), 1);
        });
    }
}

#[cfg(test)]
mod startup_sources {
    use super::*;
    use std::error::Error;

    #[test]
    fn startup_allocation_source_borrows_the_original_cause() {
        let mut requested = Vec::<u8>::new();
        let original = requested
            .try_reserve(usize::MAX)
            .expect_err("standard capacity overflow rejects without allocation");
        let refusal = StartupRefusal::Allocation(original);
        let StartupRefusal::Allocation(original) = &refusal else {
            unreachable!("actual allocation refusal");
        };
        let cause = Error::source(&refusal).expect("actual native allocation source");
        let reported_source = ptr::from_ref(cause).cast::<()>();
        let owned_source = ptr::from_ref(original).cast::<()>();
        assert_eq!(reported_source, owned_source);
    }
}

#[cfg(test)]
mod requester_diagnostics {
    use super::*;

    #[test]
    fn requester_error_preserves_messages_and_omits_private_values() {
        for (message, expected) in [
            (
                CounterRequesterMessage::Begin(u64::MAX),
                "unexpected verification trigger",
            ),
            (
                CounterRequesterMessage::VerificationCompleted(CounterReport {
                    correlation: u64::MAX,
                    status: CounterStatus::Refused(CounterRefusal::Expired),
                }),
                "unexpected verification completion",
            ),
            (
                CounterRequesterMessage::ReadObserved {
                    correlation: u64::MAX,
                    value: u64::MAX,
                },
                "unexpected Counter snapshot",
            ),
        ] {
            let error = CounterRequesterError::UnexpectedMessage(Box::new(message));
            let display = format!("{error}");
            let debug = format!("{error:?}");
            let expected_debug = format!("CounterRequesterError({expected})");
            let source = Error::source(&error);
            assert_eq!(display, expected);
            assert_eq!(debug, expected_debug);
            assert!(source.is_none());
            let CounterRequesterError::UnexpectedMessage(message) = error;
            match *message {
                CounterRequesterMessage::BeginProtected { correlation, scope } => {
                    assert_eq!(correlation, u64::MAX);
                    assert_eq!(scope.identity.sequence, u64::MAX);
                }
                CounterRequesterMessage::ProtectedReply {
                    correlation,
                    scope,
                    meaning,
                } => {
                    assert_eq!(correlation, u64::MAX);
                    assert_eq!(scope.identity.sequence, u64::MAX);
                    assert_eq!(meaning, CounterProtectedMeaning::ReadReply(u64::MAX));
                }
                CounterRequesterMessage::Begin(correlation) => assert_eq!(correlation, u64::MAX),
                CounterRequesterMessage::VerificationCompleted(report) => {
                    assert_eq!(
                        report,
                        CounterReport {
                            correlation: u64::MAX,
                            status: CounterStatus::Refused(CounterRefusal::Expired),
                        }
                    );
                }
                CounterRequesterMessage::ReadObserved { correlation, value } => {
                    assert_eq!((correlation, value), (u64::MAX, u64::MAX));
                }
            }
        }
    }
}

#[cfg(test)]
mod stop_admission {
    use super::*;

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Acquire exact original root/child native, Work and both notification products before every retirement and sibling oracle."
    )]
    fn root_stop_requires_its_own_grant_and_joined_report_while_work_is_held() {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let acquired = host.block_on(async {
            let execute_counter = || {
                let (published, ports) = oneshot::channel();
                let (permit, permission) = oneshot::channel();
                let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                    .execute_with(|application| async move {
                        let interface = application.interface(());
                        let publication = published.send((
                            application.lifecycle(),
                            application.root().established_recipient(),
                            interface.external::<CounterRequests>().unwrap(),
                            interface.external::<CounterValue>().unwrap(),
                        ));
                        let continued = permission.await;
                        (publication, continued)
                    })
                    .unwrap_or_else(|original| {
                        drop(original);
                        panic!("actual entered application host");
                    });
                (execution, receiving, ports, permit)
            };
            let (target_execution, target_receiving, target_ports, target_permit) =
                execute_counter();
            let (sibling_execution, sibling_receiving, sibling_ports, sibling_permit) =
                execute_counter();
            let service = async {
                let (lifecycle, target, requests, mut values) = target_ports.await.unwrap();
                let (sibling_lifecycle, sibling, sibling_requests, mut sibling_values) =
                    sibling_ports.await.unwrap();
                let binding = Binding {
                    deployment: "deployment",
                    caller: "caller",
                    controller: "controller",
                    actor: "counter",
                    host: "host",
                    protocol: "counter",
                    runtime_generation: 23,
                    actor_generation: 29,
                };
                let scope = Scope {
                    identity: Identity {
                        binding,
                        sequence: 11,
                    },
                    operation: CounterOperation::Stop,
                    grant_issuer: "creator",
                    authority_generation: 7,
                };
                let next_scope = Scope {
                    identity: Identity {
                        sequence: 13,
                        ..scope.identity
                    },
                    ..scope
                };
                let configured = [scope, next_scope].map(|scope| {
                    counter_record(
                        scope,
                        CounterProofProfile::RecordedTest,
                        CounterProtectedMeaning::Request,
                        &[41],
                        &mut [0; 8192],
                    )
                    .unwrap_or_else(|original| {
                        drop(original);
                        panic!("registered Stop JSON");
                    })
                });
                let mut authorization = CounterAuthorization::new(
                    StartupMode::ExplicitTest,
                    Some((
                        binding,
                        Instant::now() + Duration::from_secs(60),
                        target.clone(),
                    )),
                    "creator",
                    7,
                    &[CounterOperation::Increment, CounterOperation::Read],
                    record_provider::projection,
                    CounterProofProfile::RecordedTest,
                    CounterLimits {
                        incoming: NonZeroUsize::new(2).unwrap(),
                        retained: NonZeroUsize::new(2).unwrap(),
                        individual: NonZeroUsize::new(131_072).unwrap(),
                        total: NonZeroUsize::new(262_144).unwrap(),
                        verification_maximum: 65_536,
                    },
                )
                .unwrap();
                authorization.stop = Some(CounterStopAuthority::Root(lifecycle.clone()));
                let mut reports = Vec::new();
                let mut originals = Vec::new();
                let mut allocations = Vec::new();
                let mut target_after_refusal = None;
                for record in &configured {
                    let original = counter_request(record);
                    allocations.push((original.protected.as_ptr(), original.proof.as_ptr()));
                    let correlation = authorization
                        .reserve(original)
                        .unwrap_or_else(|_| panic!("finite reserved original"));
                    let position = authorization
                        .pending
                        .iter()
                        .position(|entry| entry.correlation == correlation)
                        .unwrap();
                    let request_limits = counter_test_json_limits();
                    acquire_verification(&mut authorization.pending[position], |original| {
                        record_provider::verify(
                            original,
                            &configured,
                            AuthorityFreshness::Current,
                            ProviderAvailability::Available,
                            CounterProofProfile::RecordedTest,
                            CounterProofPurpose::Request,
                            binding.caller,
                            binding.host,
                            &request_limits,
                        )
                    })
                    .await;
                    authorization.associate(position, &requests, &values.recipient());
                    authorization.attempt(correlation, &requests);
                    reports.push(authorization.report(correlation));
                    if record.scope.identity.sequence == 11 {
                        let increment = values.send(&target, CounterMessage::Increment).await;
                        let read = values
                            .send(&target, CounterMessage::Read(values.recipient()))
                            .await;
                        let snapshot = values.receive().await;
                        target_after_refusal = Some((increment, read, snapshot));
                        authorization.permitted_operations = &[CounterOperation::Stop];
                    }
                    originals.push(authorization.consume(correlation));
                }
                let joined = lifecycle.retirement().await;
                let sibling_increment = sibling_values
                    .send(&sibling, CounterMessage::Increment)
                    .await;
                let sibling_read = sibling_values
                    .send(&sibling, CounterMessage::Read(sibling_values.recipient()))
                    .await;
                let sibling_reply = sibling_values.receive().await;
                let sibling_stop = sibling_lifecycle.request_shutdown();
                let target_release = target_permit.send(());
                let sibling_release = sibling_permit.send(());
                (
                    reports,
                    originals,
                    allocations,
                    target_after_refusal,
                    joined,
                    sibling_increment,
                    sibling_read,
                    sibling_reply,
                    sibling_stop,
                    target_release,
                    sibling_release,
                    requests,
                    sibling_requests,
                )
            };
            let (target_work, sibling_work, service) = tokio::join!(
                AssertUnwindSafe(target_execution).catch_unwind(),
                AssertUnwindSafe(sibling_execution).catch_unwind(),
                service
            );
            let target_native = target_receiving.await.into_parts();
            let sibling_native = sibling_receiving.await.into_parts();
            (
                target_work,
                sibling_work,
                target_native,
                sibling_native,
                service,
            )
        });
        let (target_work, sibling_work, target_native, sibling_native, service) = acquired;
        let (
            reports,
            originals,
            allocations,
            target_after_refusal,
            joined,
            sibling_increment,
            sibling_read,
            sibling_reply,
            sibling_stop,
            target_release,
            sibling_release,
            requests,
            sibling_requests,
        ) = service;
        let [target_retired, sibling_retired] = [target_native, sibling_native].map(|product| {
            let (
                WorkOutcome::Completed {
                    output: (publication, continued),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = product
            else {
                panic!("complete original Work/native/two-notification product");
            };
            assert!(publication.is_ok());
            assert!(continued.is_ok());
            completed_actor(origin, retirement)
        });
        assert!(target_work.is_ok());
        assert!(sibling_work.is_ok());
        assert!(matches!(
            reports.as_slice(),
            [
                Some(CounterReport {
                    status: CounterStatus::Refused(CounterRefusal::Grant),
                    ..
                }),
                Some(CounterReport {
                    status: CounterStatus::StopAccepted,
                    ..
                })
            ]
        ));
        let Some((Ok(()), Ok(()), Some(target_reply))) = target_after_refusal else {
            panic!("denied Stop leaves target useful");
        };
        assert_eq!(target_reply.message, 1);
        assert_eq!(
            joined.retirement(),
            bombay::RetirementAssessment::Established
        );
        assert_eq!(
            joined.failures(),
            bombay::ActorFailureAssessment::NoFailuresFound
        );
        assert!(sibling_increment.is_ok());
        assert!(sibling_read.is_ok());
        assert_eq!(sibling_reply.as_ref().unwrap().message, 1);
        assert_eq!(sibling_stop, Ok(()));
        assert!(target_release.is_ok());
        assert!(sibling_release.is_ok());
        for (retained, (protected, proof)) in originals.iter().zip(allocations) {
            let retained = retained.as_ref().unwrap();
            assert_eq!(retained.original.protected.as_ptr(), protected);
            assert_eq!(retained.original.proof.as_ptr(), proof);
        }
        drop((
            target_retired,
            sibling_retired,
            requests,
            sibling_requests,
            originals,
        ));
    }
    use bombay::behavior::{
        ActiveTurn, BehaviorBase, Births, ChildCreationOutcome, ClassifySettlement, CreateChild,
        CreationEvent, CreationSequence, CreationSettlement, CreationSettlements, Creations,
        EstablishedActor, InitializationTurn, InterpretEstablished, InterpretInstalledActor,
        ItemSettlement, SettledItem,
    };
    use core::future::Future;
    use core::task::{Context, Poll, Waker};

    pub(super) struct CounterChildren {
        pub(super) target: EstablishedActor<StopOnShutdown<Counter>>,
        pub(super) sibling: EstablishedActor<StopOnShutdown<Counter>>,
    }
    pub(super) type CounterExports = MessageProtocol<MailAddr, CounterChildren>;
    type CounterBirths =
        <Births<StopOnShutdown<Counter>> as CreationSettlements<MailAddr>>::Settlements;
    #[derive(Default)]
    pub(super) struct CounterParent {
        children: Vec<EstablishedActor<StopOnShutdown<Counter>>>,
        births: Vec<CounterBirths>,
    }
    impl Protocol for CounterParent {
        type Addr = MailAddr;
        type Msg = EstablishedRecipient<CounterExports>;
    }
    impl BehaviorBase for CounterParent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for CounterParent {
        type Protocol = Self;
        type Event =
            CreationEvent<MailAddr, StopOnShutdown<Counter>, EstablishedRecipient<CounterExports>>;
        type Sends = Vec<EstablishedDelivery<CounterExports>>;
        type Ph = Never;
        type Error = Never;
        type Birth = Births<StopOnShutdown<Counter>>;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let mut sequence = CreationSequence::new();
            let target = sequence.issue().unwrap();
            let sibling = sequence.issue().unwrap();
            Ok(Actions::create(
                Creations::one(CreateChild::birth(
                    target,
                    Counter::new().stop_on_shutdown(),
                ))
                .and(CreateChild::birth(
                    sibling,
                    Counter::new().stop_on_shutdown(),
                )),
            ))
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                CreationEvent::Settlements(settled) => {
                    let original = settled.into_settlement();
                    let CreationSettlement::Settled(children) = &original else {
                        panic!("actual birth traversal");
                    };
                    for child in children {
                        let SettledItem::Attempted(ItemSettlement::Accepted(
                            ChildCreationOutcome::Established(committed),
                        )) = child
                        else {
                            panic!("actual installed Counter export");
                        };
                        self.children.push(committed.actor());
                    }
                    self.births.push(original);
                    Ok(Actions::cont())
                }
                CreationEvent::User(reply) => {
                    let [target, sibling] = self.children.as_slice() else {
                        panic!("actual children committed before dependent export");
                    };
                    Ok(Actions::new(
                        vec![EstablishedDelivery::new(
                            reply.message,
                            CounterChildren {
                                target: target.clone(),
                                sibling: sibling.clone(),
                            },
                        )],
                        Creations::empty(),
                        Step::Continue,
                    ))
                }
            }
        }
    }
    #[derive(Default)]
    pub(super) struct CounterStopExport {
        pub(super) installed: Option<InstalledActor<StopOnShutdown<Counter>>>,
    }
    impl InterpretEstablished<Counter> for CounterStopExport {
        type Output = MailAddr;
        fn interpret_established(&mut self, endpoint: ActorRef<Counter>) -> Self::Output {
            endpoint.address()
        }
    }
    impl InterpretInstalledActor<StopOnShutdown<Counter>> for CounterStopExport {
        type Output = ();
        fn interpret_actor(&mut self, installed: InstalledActor<StopOnShutdown<Counter>>) {
            self.installed = Some(installed);
        }
    }
    bombay::topology! {
        pub(super) counter_children_application {
            root: CounterParent { Counter => counter; }
            counter {}
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Acquire exact original root/child native, Work and both notification products before every retirement and sibling oracle."
    )]
    fn child_stop_uses_the_installed_target_and_preserves_creator_and_sibling() {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let acquired = host.block_on(async {
            let (published, ports) = oneshot::channel();
            let (permit, permission) = oneshot::channel();
            let (execution, receiving) = Application::with_topology(
                CounterParent::default().stop_on_shutdown(),
                counter_children_application::Topology::new(),
            )
            .execute_with(|application| async move {
                let interface = application.interface(());
                let publication = published.send((
                    application.lifecycle(),
                    application.root().established_recipient(),
                    interface.external::<CounterExports>().unwrap(),
                    interface.external::<CounterRequests>().unwrap(),
                    interface.external::<CounterValue>().unwrap(),
                ));
                let continued = permission.await;
                (publication, continued)
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("actual entered child host");
            });
            let service = async {
                let (lifecycle, parent, mut exports, requests, mut values) = ports.await.unwrap();
                let exported = exports.send(&parent, exports.recipient()).await;
                let export = exports.receive().await.unwrap();
                let CounterChildren { target, sibling } = export.message;
                let mut target_export = CounterStopExport::default();
                let mut sibling_export = CounterStopExport::default();
                target.clone().interpret_actor(&mut target_export);
                sibling.clone().interpret_actor(&mut sibling_export);
                let target = target.recipient();
                let sibling = sibling.recipient();
                let installed = target_export.installed.unwrap();
                let mut retirement = Box::pin(installed.retirement());
                let mut borrowed = Box::pin(async { retirement.as_mut().await });
                let pending = borrowed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                drop(borrowed);
                let target_increment = values.send(&target, CounterMessage::Increment).await;
                let target_read = values
                    .send(&target, CounterMessage::Read(values.recipient()))
                    .await;
                let target_before = values.receive().await;
                let binding = Binding {
                    deployment: "deployment",
                    caller: "caller",
                    controller: "controller",
                    actor: "child",
                    host: "host",
                    protocol: "counter",
                    runtime_generation: 23,
                    actor_generation: 29,
                };
                let scope = Scope {
                    identity: Identity {
                        binding,
                        sequence: 11,
                    },
                    operation: CounterOperation::Stop,
                    grant_issuer: "creator",
                    authority_generation: 7,
                };
                let next_scope = Scope {
                    identity: Identity {
                        sequence: 13,
                        ..scope.identity
                    },
                    ..scope
                };
                let configured = [scope, next_scope].map(|scope| {
                    counter_record(
                        scope,
                        CounterProofProfile::RecordedTest,
                        CounterProtectedMeaning::Request,
                        &[41],
                        &mut [0; 8192],
                    )
                    .unwrap_or_else(|original| {
                        drop(original);
                        panic!("registered Stop JSON");
                    })
                });
                let mut authorization = CounterAuthorization::new(
                    StartupMode::ExplicitTest,
                    Some((
                        binding,
                        Instant::now() + Duration::from_secs(60),
                        target.clone(),
                    )),
                    "creator",
                    7,
                    &[CounterOperation::Increment, CounterOperation::Read],
                    record_provider::projection,
                    CounterProofProfile::RecordedTest,
                    CounterLimits {
                        incoming: NonZeroUsize::new(2).unwrap(),
                        retained: NonZeroUsize::new(2).unwrap(),
                        individual: NonZeroUsize::new(131_072).unwrap(),
                        total: NonZeroUsize::new(262_144).unwrap(),
                        verification_maximum: 65_536,
                    },
                )
                .unwrap();
                authorization.stop = Some(CounterStopAuthority::Child(installed));
                let mut reports = Vec::new();
                let mut originals = Vec::new();
                let mut target_after_refusal = None;
                for record in &configured {
                    let original = counter_request(record);
                    let pointers = (original.protected.as_ptr(), original.proof.as_ptr());
                    let correlation = authorization
                        .reserve(original)
                        .unwrap_or_else(|_| panic!("finite original reservation"));
                    let position = authorization
                        .pending
                        .iter()
                        .position(|entry| entry.correlation == correlation)
                        .unwrap();
                    let request_limits = counter_test_json_limits();
                    acquire_verification(&mut authorization.pending[position], |original| {
                        record_provider::verify(
                            original,
                            &configured,
                            AuthorityFreshness::Current,
                            ProviderAvailability::Available,
                            CounterProofProfile::RecordedTest,
                            CounterProofPurpose::Request,
                            binding.caller,
                            binding.host,
                            &request_limits,
                        )
                    })
                    .await;
                    authorization.associate(position, &requests, &values.recipient());
                    authorization.attempt(correlation, &requests);
                    reports.push(authorization.report(correlation));
                    if record.scope.identity.sequence == 11 {
                        let read = values
                            .send(&target, CounterMessage::Read(values.recipient()))
                            .await;
                        let reply = values.receive().await;
                        target_after_refusal = Some((read, reply));
                        authorization.permitted_operations = &[CounterOperation::Stop];
                    }
                    originals.push((authorization.consume(correlation), pointers));
                }
                let report = retirement.await;
                let sibling_increment = values.send(&sibling, CounterMessage::Increment).await;
                let sibling_read = values
                    .send(&sibling, CounterMessage::Read(values.recipient()))
                    .await;
                let sibling_reply = values.receive().await;
                let sibling_stop = sibling_export.installed.unwrap();
                let sibling_stopped = sibling_stop.request_shutdown(Ingress::<_, Here>::new());
                let sibling_retired = sibling_stop.retirement().await;
                let parent_stopped = lifecycle.request_shutdown();
                let released = permit.send(());
                (
                    exported,
                    pending,
                    target_increment,
                    target_read,
                    target_before,
                    target_after_refusal,
                    report,
                    sibling_increment,
                    sibling_read,
                    sibling_reply,
                    sibling_stopped,
                    sibling_retired,
                    parent_stopped,
                    released,
                    reports,
                    originals,
                    requests,
                    exports,
                    values,
                )
            };
            let (execution, observations) =
                tokio::join!(AssertUnwindSafe(execution).catch_unwind(), service);
            let native = receiving.await.into_parts();
            (execution, native, observations)
        });
        let (execution, native, observations) = acquired;
        let (
            exported,
            pending,
            target_increment,
            target_read,
            target_before,
            target_after_refusal,
            report,
            sibling_increment,
            sibling_read,
            sibling_reply,
            sibling_stopped,
            sibling_retired,
            parent_stopped,
            released,
            reports,
            originals,
            requests,
            exports,
            values,
        ) = observations;
        let (
            WorkOutcome::Completed {
                output: (publication, continued),
                cleanup: Ok(()),
            },
            Ok((origin, native)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = native
        else {
            panic!("complete parent Work/native/two-notification product");
        };
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            settlements,
            completion,
            interpretation,
            source,
        } = native
        else {
            panic!("complete original creator result");
        };
        assert!(execution.is_ok());
        assert!(publication.is_ok());
        assert!(continued.is_ok());
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert!(exported.is_ok());
        assert!(matches!(pending, Poll::Pending));
        assert!(target_increment.is_ok());
        assert!(target_read.is_ok());
        let target_before = target_before.unwrap();
        assert_eq!(target_before.message, 1);
        let Some((Ok(()), Some(target_after_refusal))) = target_after_refusal else {
            panic!("denied child Stop leaves original target useful");
        };
        assert_eq!(target_after_refusal.from, target_before.from);
        assert_eq!(target_after_refusal.message, 1);
        assert!(matches!(
            reports.as_slice(),
            [
                Some(CounterReport {
                    status: CounterStatus::Refused(CounterRefusal::Grant),
                    ..
                }),
                Some(CounterReport {
                    status: CounterStatus::StopAccepted,
                    ..
                })
            ]
        ));
        assert_eq!(
            report.retirement(),
            bombay::RetirementAssessment::Established
        );
        assert_eq!(
            report.failures(),
            bombay::ActorFailureAssessment::NoFailuresFound
        );
        assert!(sibling_increment.is_ok());
        assert!(sibling_read.is_ok());
        let sibling_reply = sibling_reply.unwrap();
        assert_eq!(sibling_reply.message, 1);
        assert_ne!(sibling_reply.from, target_before.from);
        assert_eq!(sibling_stopped, Ok(()));
        assert_eq!(
            sibling_retired.retirement(),
            bombay::RetirementAssessment::Established
        );
        assert_eq!(parent_stopped, Ok(()));
        assert!(released.is_ok());
        assert!(child_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(control.is_empty());
        assert_eq!(user.as_slice(), []);
        for settlement in &settlements {
            assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
        }
        let children = descendants
            .into_iter()
            .map(|terminal| {
                let counter_children_application::RootRetirements { origin, retirement } = terminal;
                let terminal = *retirement;
                let address = origin.address();
                let ActorRetirement::Completed {
                    custody:
                        ActorCustody {
                            behavior,
                            control,
                            user,
                            descendants,
                            child_failures,
                            capability_failures,
                            unread_owner_cancellation,
                            additional_failures,
                            received_interpretation,
                            received_source,
                            source_index,
                            acquired_ingress,
                            retirement_failures,
                            terminal_report,
                        },
                    settlements,
                    completion,
                    interpretation,
                    source,
                } = terminal
                else {
                    panic!("complete original child retirement");
                };
                assert_eq!(child_failures.as_slice(), []);
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(completion, Completion::Stopped);
                assert!(interpretation.is_none());
                assert!(source.is_none());
                assert!(additional_failures.is_empty());
                assert!(received_interpretation.is_none());
                assert!(received_source.is_none());
                assert!(source_index.is_none());
                assert!(acquired_ingress.is_none());
                assert!(retirement_failures.is_empty());
                assert!(terminal_report.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert_eq!(descendants.as_slice(), []);
                for settlement in &settlements {
                    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
                }
                (origin, address, behavior, settlements)
            })
            .collect::<Vec<_>>();
        let child_addresses = children
            .iter()
            .map(|(_, address, _, _)| *address)
            .collect::<Vec<_>>();
        assert_eq!(child_addresses.len(), 2);
        assert!(child_addresses.contains(&target_before.from));
        assert!(child_addresses.contains(&sibling_reply.from));
        for (original, pointers) in &originals {
            let original = original.as_ref().unwrap();
            assert_eq!(
                (
                    original.original.protected.as_ptr(),
                    original.original.proof.as_ptr()
                ),
                *pointers
            );
        }
        drop((
            behavior,
            settlements,
            children,
            originals,
            requests,
            exports,
            values,
        ));
    }
}

#[cfg(test)]
mod protected_application {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn recorded_current_reply_authority_survives_command_revocation_and_rejects_stale_context() {
        counter_reply_authority(
            &record_provider::verify,
            record_provider::projection,
            record_provider::meaning,
            record_provider::endpoints,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_current_reply_authority_survives_command_revocation_and_rejects_stale_context() {
        counter_reply_authority(
            &scheduled_provider::verify,
            scheduled_provider::projection,
            scheduled_provider::meaning,
            scheduled_provider::endpoints,
            CounterProofProfile::ScheduledTest,
        );
    }
    #[expect(
        clippy::too_many_lines,
        reason = "One actual Application trace retains native, Work and notification products before independent current reply-authority oracles."
    )]
    fn counter_reply_authority<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        meaning: fn(&R) -> CounterProtectedMeaning,
        endpoints: fn(&R) -> (&'static str, &'static str),
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actual host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("finite assignment");
        let scope = Scope {
            identity: Identity {
                binding,
                sequence: 37,
            },
            operation: CounterOperation::Stop,
            grant_issuer: authority.grant_issuer,
            authority_generation: authority.authority_generation,
        };
        let configured = [
            counter_record(
                scope,
                profile,
                CounterProtectedMeaning::Refused(CounterRefusal::Revoked),
                &[43],
                &mut [0; 8192],
            )
            .expect("configured revoked refusal proof"),
            counter_record(
                scope,
                profile,
                CounterProtectedMeaning::Refused(CounterRefusal::Expired),
                &[47],
                &mut [0; 8192],
            )
            .expect("configured expired refusal proof"),
        ];
        let originals = configured.each_ref().map(counter_request);
        let pointers = originals
            .each_ref()
            .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
        let acquired = host.block_on(async {
            let receipt = provider(
                &originals[0],
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
                profile,
                CounterProofPurpose::Refused,
                binding.host,
                binding.caller,
                &counter_test_json_limits(),
            )
            .await;
            let expired_receipt = provider(
                &originals[1],
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
                profile,
                CounterProofPurpose::Refused,
                binding.host,
                binding.caller,
                &counter_test_json_limits(),
            )
            .await;
            let stale = provider(
                &originals[0],
                &configured,
                AuthorityFreshness::Stale,
                ProviderAvailability::Available,
                profile,
                CounterProofPurpose::Refused,
                binding.host,
                binding.caller,
                &counter_test_json_limits(),
            )
            .await;
            let unavailable = provider(
                &originals[0],
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Unavailable,
                profile,
                CounterProofPurpose::Refused,
                binding.host,
                binding.caller,
                &counter_test_json_limits(),
            )
            .await;
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let authorization = CounterAuthorization::new(
                        StartupMode::ExplicitTest,
                        Some((binding, deadline, target)),
                        scope.grant_issuer,
                        scope.authority_generation,
                        &[CounterOperation::Increment, CounterOperation::Read],
                        projection,
                        profile,
                        CounterLimits {
                            incoming: NonZeroUsize::new(2).expect("fixture count"),
                            retained: NonZeroUsize::new(2).expect("fixture count"),
                            individual: NonZeroUsize::new(131_072).expect("fixture bytes"),
                            total: NonZeroUsize::new(262_144).expect("fixture bytes"),
                            verification_maximum: 65536,
                        },
                    );
                    let mut outcomes = Vec::new();
                    let mut authorization = authorization.expect("configured authority");
                    if let (Ok(receipt), Ok(expired_receipt)) = (&receipt, &expired_receipt) {
                        authorization.permission = Permission::Revoked;
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                        authorization.permission = Permission::Granted;
                        authorization.deadline = Instant::now();
                        outcomes.push(authorization.protected_reply(
                            expired_receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                        authorization.deadline = deadline;
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[],
                        ));
                        authorization.grant_issuer = "different-current-issuer";
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                        authorization.grant_issuer = scope.grant_issuer;
                        authorization.authority_generation += 1;
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                        authorization.authority_generation = scope.authority_generation;
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            Scope {
                                identity: Identity {
                                    sequence: 41,
                                    ..scope.identity
                                },
                                ..scope
                            },
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::ReadReply,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::ReadReply],
                        ));
                        authorization.profile = match profile {
                            CounterProofProfile::RecordedTest => CounterProofProfile::ScheduledTest,
                            CounterProofProfile::ScheduledTest => CounterProofProfile::RecordedTest,
                        };
                        outcomes.push(authorization.protected_reply(
                            receipt,
                            scope,
                            CounterProofPurpose::Refused,
                            meaning,
                            endpoints,
                            &[CounterProofPurpose::Refused],
                        ));
                    }
                    let stopped = application.lifecycle().request_shutdown();
                    (
                        receipt,
                        expired_receipt,
                        stale,
                        unavailable,
                        authorization,
                        outcomes,
                        stopped,
                    )
                })
                .unwrap_or_else(|_| panic!("actual host entered"));
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let native = receiving.await.into_parts();
            (execution, native)
        });
        let (
            execution,
            (
                WorkOutcome::Completed {
                    output:
                        (receipt, expired_receipt, stale, unavailable, authorization, outcomes, stopped),
                    cleanup: Ok(()),
                },
                Ok((origin, native)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ),
        ) = acquired
        else {
            panic!("all original Work/native/two-notification products");
        };
        let _counter = completed_actor(origin, native);
        assert!(execution.is_ok() && stopped.is_ok());
        assert_eq!(
            originals
                .each_ref()
                .map(|original| (original.protected.as_ptr(), original.proof.as_ptr())),
            pointers
        );
        let Ok(receipt) = receipt else {
            panic!("original authenticated refusal retained");
        };
        assert_eq!(
            projection(&receipt),
            (&scope, profile, CounterProofPurpose::Refused)
        );
        let Ok(expired_receipt) = expired_receipt else {
            panic!("original authenticated expiration refusal retained");
        };
        assert_eq!(
            projection(&expired_receipt),
            (&scope, profile, CounterProofPurpose::Refused)
        );
        assert!(matches!(stale, Err(VerificationError::Stale)));
        assert!(matches!(
            unavailable,
            Err(VerificationError::Unavailable(_))
        ));
        assert!(authorization.pending.is_empty() && authorization.retained.is_empty());
        assert_eq!(
            outcomes,
            [
                Ok(CounterProtectedMeaning::Refused(CounterRefusal::Revoked)),
                Ok(CounterProtectedMeaning::Refused(CounterRefusal::Expired)),
                Err(CounterRefusal::Grant),
                Err(CounterRefusal::Grant),
                Err(CounterRefusal::Grant),
                Err(CounterRefusal::Binding),
                Err(CounterRefusal::Invalid),
                Err(CounterRefusal::Invalid)
            ]
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep all acquired originals and complete typed Actions/native products before their independent owning-law assertions."
    )]
    fn registered_bytes_and_every_metadata_binding_are_checked_without_payload_logging() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actual host");
        let binding = Binding {
            deployment: "deployment",
            caller: "caller",
            controller: "controller",
            actor: "counter",
            host: "host",
            protocol: "counter",
            runtime_generation: 23,
            actor_generation: 29,
        };
        let scope = Scope {
            identity: Identity {
                binding,
                sequence: 37,
            },
            operation: CounterOperation::Increment,
            grant_issuer: "issuer",
            authority_generation: 7,
        };
        let configured = [counter_record(
            scope,
            CounterProofProfile::RecordedTest,
            CounterProtectedMeaning::Request,
            &[41],
            &mut [0; 8192],
        )
        .expect("explicit registered request")];
        let serialized =
            String::from_utf8(configured[0].protected.to_vec()).expect("Serde writes UTF-8 JSON");
        let fixtures = [
            ("\"wire_version\":1", "\"wire_version\":2"),
            ("\"schema_version\":1", "\"schema_version\":2"),
            ("counter-record-test-v1", "counter-scheduled-test-v1"),
            ("\"kind\":\"request\"", "\"kind\":\"unsupported\""),
            (
                "\"deployment\":\"deployment\"",
                "\"deployment\":\"foreign\"",
            ),
            ("\"caller\":\"caller\"", "\"caller\":\"foreign\""),
            (
                "\"controller\":\"controller\"",
                "\"controller\":\"foreign\"",
            ),
            ("\"actor\":\"counter\"", "\"actor\":\"foreign\""),
            ("\"host\":\"host\"", "\"host\":\"foreign\""),
            ("\"protocol\":\"counter\"", "\"protocol\":\"foreign\""),
            (
                "\"grant_issuer\":\"issuer\"",
                "\"grant_issuer\":\"foreign\"",
            ),
            ("\"runtime_generation\":23", "\"runtime_generation\":24"),
            ("\"actor_generation\":29", "\"actor_generation\":30"),
            ("\"authority_generation\":7", "\"authority_generation\":8"),
            ("\"request_sequence\":37", "\"request_sequence\":38"),
            ("\"operation\":\"increment\"", "\"operation\":\"read\""),
        ];
        let mut original_outcomes = Vec::new();
        for (original, replaced) in fixtures {
            let mut configured = configured.clone();
            configured[0].protected = serialized
                .replace(original, replaced)
                .into_bytes()
                .into_boxed_slice();
            let incoming = counter_request(&configured[0]);
            let pointers = (incoming.protected.as_ptr(), incoming.proof.as_ptr());
            let verified = host.block_on(record_provider::verify(
                &incoming,
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
                CounterProofProfile::RecordedTest,
                CounterProofPurpose::Request,
                binding.caller,
                binding.host,
                &counter_test_json_limits(),
            ));
            original_outcomes.push((incoming, pointers, verified));
        }
        let incoming = counter_request(&configured[0]);
        let wrong_profile = host.block_on(record_provider::verify(
            &incoming,
            &configured,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::ScheduledTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        ));
        let wrong_purpose = host.block_on(record_provider::verify(
            &incoming,
            &configured,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::ReadReply,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        ));
        let wrong_producer = host.block_on(record_provider::verify(
            &incoming,
            &configured,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::Request,
            binding.host,
            binding.caller,
            &counter_test_json_limits(),
        ));
        let whitespace = CounterRequest {
            protected: format!("  {serialized}\n").into_bytes().into_boxed_slice(),
            proof: incoming.proof.clone(),
        };
        let changed_bytes = host.block_on(record_provider::verify(
            &whitespace,
            &configured,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        ));
        let mut separately_registered = configured.clone();
        separately_registered[0].protected = whitespace.protected.clone();
        separately_registered[0].proof = Box::from([43]);
        let separately_protected = counter_request(&separately_registered[0]);
        let valid_spelling = host.block_on(record_provider::verify(
            &separately_protected,
            &separately_registered,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        ));
        let unknown = CounterRequest {
            protected: serialized
                .replacen('{', "{\"private-secret-field\":0,", 1)
                .into_bytes()
                .into_boxed_slice(),
            proof: incoming.proof.clone(),
        };
        let malformed = host.block_on(record_provider::verify(
            &unknown,
            &configured,
            AuthorityFreshness::Current,
            ProviderAvailability::Available,
            CounterProofProfile::RecordedTest,
            CounterProofPurpose::Request,
            binding.caller,
            binding.host,
            &counter_test_json_limits(),
        ));
        // Original inputs/results all outlive their verification; no native effect is claimed by this provider-only vector control.
        assert_eq!(original_outcomes.len(), 16);
        for (incoming, pointers, verified) in original_outcomes {
            assert_eq!(
                (incoming.protected.as_ptr(), incoming.proof.as_ptr()),
                pointers
            );
            assert!(matches!(
                verified,
                Err(VerificationError::Invalid | VerificationError::Malformed(_))
            ));
        }
        assert!(matches!(wrong_profile, Err(VerificationError::Invalid)));
        assert!(matches!(wrong_purpose, Err(VerificationError::Invalid)));
        assert!(matches!(wrong_producer, Err(VerificationError::Invalid)));
        assert!(matches!(changed_bytes, Err(VerificationError::Invalid)));
        let Ok(valid_spelling) = valid_spelling else {
            panic!("own exact bytes/proof registration accepts alternate JSON spelling");
        };
        assert_eq!(
            record_provider::projection(&valid_spelling),
            (
                &scope,
                CounterProofProfile::RecordedTest,
                CounterProofPurpose::Request
            )
        );
        let Err(VerificationError::Malformed(original)) = malformed else {
            panic!("original strict-schema cause retained");
        };
        let diagnostic = VerificationError::Malformed(original);
        assert_eq!(
            diagnostic.to_string(),
            "malformed protected Counter JSON: schema"
        );
        assert_eq!(format!("{diagnostic:?}"), diagnostic.to_string());
        let VerificationError::Malformed(protected::DecodeError::Body(original)) = diagnostic
        else {
            panic!("concrete original Serde cause retained");
        };
        assert_eq!(original.classify(), Category::Data);
        assert!(original.to_string().contains("private-secret-field"));
    }

    #[test]
    fn recorded_profile_couples_all_seven_meanings_to_native_facts() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit owning host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[
                CounterOperation::Increment,
                CounterOperation::Read,
                CounterOperation::Stop,
            ],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("configured finite assignment");
        let configured = configured_protected_counter(
            binding,
            authority.grant_issuer,
            authority.authority_generation,
            CounterProofProfile::RecordedTest,
        );
        let actual = host.block_on(run_protected_counter_profile(
            CounterWorkCase::Healthy,
            &authority,
            binding,
            deadline,
            CounterProofProfile::RecordedTest,
            &configured,
            &record_provider::verify,
            record_provider::projection,
            record_provider::meaning,
            record_provider::endpoints,
        ));
        actual.expect("full original public Application acquisition");
    }

    #[test]
    fn scheduled_profile_couples_all_seven_meanings_to_native_facts() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit owning host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[
                CounterOperation::Increment,
                CounterOperation::Read,
                CounterOperation::Stop,
            ],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("configured finite assignment");
        let configured = configured_protected_counter(
            binding,
            authority.grant_issuer,
            authority.authority_generation,
            CounterProofProfile::ScheduledTest,
        );
        let actual = host.block_on(run_protected_counter_profile(
            CounterWorkCase::Healthy,
            &authority,
            binding,
            deadline,
            CounterProofProfile::ScheduledTest,
            &configured,
            &scheduled_provider::verify,
            scheduled_provider::projection,
            scheduled_provider::meaning,
            scheduled_provider::endpoints,
        ));
        actual.expect("full original public Application acquisition");
    }

    #[test]
    fn encoding_refusals_preserve_borrowed_proof_and_concrete_write_zero() {
        let scope = Scope {
            identity: Identity {
                binding: Binding {
                    deployment: "deployment",
                    caller: "caller",
                    controller: "controller",
                    actor: "actor",
                    host: "host",
                    protocol: "counter",
                    runtime_generation: 23,
                    actor_generation: 29,
                },
                sequence: 37,
            },
            operation: CounterOperation::Increment,
            grant_issuer: "issuer",
            authority_generation: 7,
        };
        let proof = vec![41, 43, 47].into_boxed_slice();
        let allocation = proof.as_ptr();
        let unsupported = CounterProtectedMeaning::Refused(CounterRefusal::Unavailable);
        let denied = counter_record(
            scope,
            CounterProofProfile::RecordedTest,
            unsupported,
            &proof,
            &mut [0; 8192],
        );
        let refused = counter_record(
            scope,
            CounterProofProfile::RecordedTest,
            CounterProtectedMeaning::Request,
            &proof,
            &mut [0; 1],
        );
        let recovered = counter_record(
            scope,
            CounterProofProfile::RecordedTest,
            CounterProtectedMeaning::Request,
            &proof,
            &mut [0; 8192],
        );
        assert_eq!(proof.as_ptr(), allocation);
        assert_eq!(proof.as_ref(), [41, 43, 47]);
        assert!(
            matches!(denied, Err(CounterEncodingError::UnsupportedMeaning(actual)) if actual == unsupported)
        );
        let Err(CounterEncodingError::Encoding(original)) = refused else {
            panic!("original encoding failure");
        };
        assert_eq!(original.io_error_kind(), Some(ErrorKind::WriteZero));
        let Ok(recovered) = recovered else {
            panic!("original proof remains reusable after refusal");
        };
        assert_eq!(recovered.proof.as_ref(), proof.as_ref());
        assert_eq!(recovered.scope, scope);
    }
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep all acquired originals and complete typed Actions/native products before their independent owning-law assertions."
    )]
    fn requester_preserves_unordered_facts_and_rejects_crossed_repeated_and_contradictory_completion()
     {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("explicit host");
        let binding = Binding {
            deployment: "deployment",
            caller: "caller",
            controller: "controller",
            actor: "counter",
            host: "host",
            protocol: "counter",
            runtime_generation: 23,
            actor_generation: 29,
        };
        let scope = Scope {
            identity: Identity {
                binding,
                sequence: 37,
            },
            operation: CounterOperation::Increment,
            grant_issuer: "issuer",
            authority_generation: 7,
        };
        let acquired = host.block_on(async {
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with(|application| async move {
                    let target = application.root().established_recipient();
                    let interface = application.interface(());
                    let service = interface
                        .external::<CounterRequests>()
                        .expect("actual service capability");
                    let values = interface
                        .external::<CounterValue>()
                        .expect("actual Read capability");
                    let requester = || CounterRequester {
                        service: service.recipient(),
                        counter: target.clone(),
                        replies: values.recipient(),
                        state: CounterRequesterState::Ready,
                    };
                    let mut outcomes = Vec::new();
                    for (operation, first, second) in [
                        (
                            CounterOperation::Increment,
                            CounterProtectedMeaning::IncrementProcessed,
                            CounterProtectedMeaning::IncrementAdmitted,
                        ),
                        (
                            CounterOperation::Read,
                            CounterProtectedMeaning::ReadReply(41),
                            CounterProtectedMeaning::ReadAdmitted,
                        ),
                        (
                            CounterOperation::Stop,
                            CounterProtectedMeaning::RetirementReport {
                                retirement: RetirementAssessment::Established,
                                failures: ActorFailureAssessment::NoFailuresFound,
                            },
                            CounterProtectedMeaning::StopAccepted,
                        ),
                    ] {
                        let scope = Scope { operation, ..scope };
                        let mut requester = requester();
                        let begun = requester.receive(
                            MailAddr::APPLICATION_ROOT,
                            CounterRequesterMessage::BeginProtected {
                                correlation: 43,
                                scope,
                            },
                        );
                        let crossed = requester.receive(
                            MailAddr::APPLICATION_ROOT,
                            CounterRequesterMessage::ProtectedReply {
                                correlation: 47,
                                scope,
                                meaning: second,
                            },
                        );
                        let foreign_scope = Scope {
                            identity: Identity {
                                sequence: 53,
                                ..scope.identity
                            },
                            ..scope
                        };
                        let foreign = requester.receive(
                            MailAddr::APPLICATION_ROOT,
                            CounterRequesterMessage::ProtectedReply {
                                correlation: 43,
                                scope: foreign_scope,
                                meaning: second,
                            },
                        );
                        let actions = [
                            begun,
                            requester.receive(
                                MailAddr::APPLICATION_ROOT,
                                CounterRequesterMessage::ProtectedReply {
                                    correlation: 43,
                                    scope,
                                    meaning: first,
                                },
                            ),
                            requester.receive(
                                MailAddr::APPLICATION_ROOT,
                                CounterRequesterMessage::ProtectedReply {
                                    correlation: 43,
                                    scope,
                                    meaning: second,
                                },
                            ),
                        ];
                        let original_state = match &requester.state {
                            CounterRequesterState::Protected { receipts, .. } => receipts.clone(),
                            _ => panic!("captured pure state"),
                        };
                        let repeated = requester.receive(
                            MailAddr::APPLICATION_ROOT,
                            CounterRequesterMessage::ProtectedReply {
                                correlation: 43,
                                scope,
                                meaning: second,
                            },
                        );
                        let contradiction = requester.receive(
                            MailAddr::APPLICATION_ROOT,
                            CounterRequesterMessage::ProtectedReply {
                                correlation: 43,
                                scope,
                                meaning: CounterProtectedMeaning::Refused(CounterRefusal::Grant),
                            },
                        );
                        outcomes.push((
                            scope,
                            first,
                            second,
                            requester,
                            actions,
                            original_state,
                            crossed,
                            foreign,
                            repeated,
                            contradiction,
                        ));
                    }
                    let mut refused = requester();
                    let begun = refused.receive(
                        MailAddr::APPLICATION_ROOT,
                        CounterRequesterMessage::BeginProtected {
                            correlation: 59,
                            scope,
                        },
                    );
                    let denied = refused.receive(
                        MailAddr::APPLICATION_ROOT,
                        CounterRequesterMessage::ProtectedReply {
                            correlation: 59,
                            scope,
                            meaning: CounterProtectedMeaning::Refused(CounterRefusal::Grant),
                        },
                    );
                    let success = refused.receive(
                        MailAddr::APPLICATION_ROOT,
                        CounterRequesterMessage::ProtectedReply {
                            correlation: 59,
                            scope,
                            meaning: CounterProtectedMeaning::IncrementProcessed,
                        },
                    );
                    let mut crossed_purpose = requester();
                    let purpose_begun = crossed_purpose.receive(
                        MailAddr::APPLICATION_ROOT,
                        CounterRequesterMessage::BeginProtected {
                            correlation: 61,
                            scope,
                        },
                    );
                    let read = crossed_purpose.receive(
                        MailAddr::APPLICATION_ROOT,
                        CounterRequesterMessage::ProtectedReply {
                            correlation: 61,
                            scope,
                            meaning: CounterProtectedMeaning::ReadReply(67),
                        },
                    );
                    let stopped = application.lifecycle().request_shutdown();
                    (
                        outcomes,
                        refused,
                        begun,
                        denied,
                        success,
                        crossed_purpose,
                        purpose_begun,
                        read,
                        stopped,
                    )
                })
                .unwrap_or_else(|_| panic!("entered actual host"));
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let native = receiving.await.into_parts();
            (execution, native)
        });
        let (
            execution,
            (
                WorkOutcome::Completed {
                    output:
                        (
                            outcomes,
                            refused,
                            begun,
                            denied,
                            success,
                            crossed_purpose,
                            purpose_begun,
                            read,
                            stopped,
                        ),
                    cleanup: Ok(()),
                },
                Ok((origin, native)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ),
        ) = acquired
        else {
            panic!("all original Work/native/two-notification products");
        };
        let _counter = completed_actor(origin, native);
        assert!(execution.is_ok() && stopped.is_ok());
        for (
            scope,
            first,
            second,
            requester,
            actions,
            original_state,
            crossed,
            foreign,
            repeated,
            contradiction,
        ) in outcomes
        {
            let CounterRequesterState::Protected {
                correlation,
                scope: captured,
                receipts,
            } = requester.state
            else {
                panic!("state retained after refused completion");
            };
            assert_eq!(correlation, 43);
            assert_eq!(captured, scope);
            assert_eq!(receipts, [first, second]);
            assert_eq!(receipts, original_state);
            assert!(
                crossed.is_err() && foreign.is_err() && repeated.is_err() && contradiction.is_err()
            );
            for (index, original) in actions.into_iter().enumerate() {
                let Ok(Actions {
                    sends:
                        CounterRequesterSends {
                            verification,
                            reads,
                        },
                    creates,
                    become_,
                }) = original
                else {
                    panic!("complete typed Actions from pure fold");
                };
                assert!(reads.is_empty() && creates.is_empty());
                assert!(matches!(become_, Step::Continue));
                assert_eq!(verification.len(), 1);
                match (&verification[0].message, index) {
                    (CounterServiceInput::Verify(43), 0) => {}
                    (
                        CounterServiceInput::ProtectedConsumed {
                            correlation: 43,
                            purpose,
                        },
                        1,
                    ) => assert_eq!(*purpose, first.purpose()),
                    (
                        CounterServiceInput::ProtectedConsumed {
                            correlation: 43,
                            purpose,
                        },
                        2,
                    ) => assert_eq!(*purpose, second.purpose()),
                    _ => panic!("all typed semantic lanes"),
                }
            }
        }
        for (original, expected) in [
            (begun, CounterServiceInput::Verify(59)),
            (
                denied,
                CounterServiceInput::ProtectedConsumed {
                    correlation: 59,
                    purpose: CounterProofPurpose::Refused,
                },
            ),
            (purpose_begun, CounterServiceInput::Verify(61)),
        ] {
            let Ok(Actions {
                sends:
                    CounterRequesterSends {
                        verification,
                        reads,
                    },
                creates,
                become_,
            }) = original
            else {
                panic!("complete pure refusal and purpose-start Actions");
            };
            assert!(reads.is_empty() && creates.is_empty());
            assert!(matches!(become_, Step::Continue));
            assert_eq!(verification.len(), 1);
            match (&verification[0].message, expected) {
                (CounterServiceInput::Verify(actual), CounterServiceInput::Verify(expected)) => {
                    assert_eq!(*actual, expected);
                }
                (
                    CounterServiceInput::ProtectedConsumed {
                        correlation,
                        purpose,
                    },
                    CounterServiceInput::ProtectedConsumed {
                        correlation: expected,
                        purpose: expected_purpose,
                    },
                ) => {
                    assert_eq!(*correlation, expected);
                    assert_eq!(*purpose, expected_purpose);
                }
                _ => panic!("complete semantic Actions trace"),
            }
        }
        assert!(success.is_err() && read.is_err());
        assert!(
            matches!(refused.state, CounterRequesterState::Protected { correlation: 59, receipts, .. }
            if receipts == [CounterProtectedMeaning::Refused(CounterRefusal::Grant)])
        );
        assert!(
            matches!(crossed_purpose.state, CounterRequesterState::Protected { correlation: 61, receipts, .. } if receipts.is_empty())
        );
    }

    #[test]
    fn protected_counter_publication_failure_retains_original_handoff_and_native_receipts() {
        for profile in [
            CounterProofProfile::RecordedTest,
            CounterProofProfile::ScheduledTest,
        ] {
            protected_application_execution_case(
                CounterWorkCase::CounterBeforePublication,
                profile,
            );
        }
    }
    #[test]
    fn protected_requester_publication_failure_retains_original_handoff_and_native_receipts() {
        for profile in [
            CounterProofProfile::RecordedTest,
            CounterProofProfile::ScheduledTest,
        ] {
            protected_application_execution_case(
                CounterWorkCase::RequesterBeforePublication,
                profile,
            );
        }
    }
    #[test]
    fn protected_requester_work_failure_preserves_originals_after_exchange_wait_ends() {
        for profile in [
            CounterProofProfile::RecordedTest,
            CounterProofProfile::ScheduledTest,
        ] {
            protected_application_execution_case(
                CounterWorkCase::RequesterAfterPublication,
                profile,
            );
        }
    }
    #[test]
    fn protected_requester_completed_execution_preserves_originals_after_exchange_wait_ends() {
        for profile in [
            CounterProofProfile::RecordedTest,
            CounterProofProfile::ScheduledTest,
        ] {
            protected_application_execution_case(
                CounterWorkCase::RequesterCompletedAfterPublication,
                profile,
            );
        }
    }
    #[test]
    fn recorded_counter_retirement_preserves_admission_and_available_processing() {
        protected_application_execution_case(
            CounterWorkCase::CounterWhileProcessingPending,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_counter_retirement_preserves_admission_and_available_processing() {
        protected_application_execution_case(
            CounterWorkCase::CounterWhileProcessingPending,
            CounterProofProfile::ScheduledTest,
        );
    }

    #[test]
    fn recorded_reply_bytes_survive_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplyBytes,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_reply_bytes_survive_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplyBytes,
            CounterProofProfile::ScheduledTest,
        );
    }
    #[test]
    fn recorded_verified_reply_survives_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplyVerification,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_verified_reply_survives_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplyVerification,
            CounterProofProfile::ScheduledTest,
        );
    }
    #[test]
    fn recorded_reply_send_result_survives_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplySend,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_reply_send_result_survives_cancelled_exchange_wait() {
        protected_application_execution_case(
            CounterWorkCase::RequesterAfterReplySend,
            CounterProofProfile::ScheduledTest,
        );
    }
    fn protected_application_execution_case(
        execution_case: CounterWorkCase,
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actual owning host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[
                CounterOperation::Increment,
                CounterOperation::Read,
                CounterOperation::Stop,
            ],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("finite authorized assignment");
        let configured = configured_protected_counter(
            binding,
            authority.grant_issuer,
            authority.authority_generation,
            profile,
        );
        let original = match profile {
            CounterProofProfile::RecordedTest => host.block_on(run_protected_counter_profile(
                execution_case,
                &authority,
                binding,
                deadline,
                profile,
                &configured,
                &record_provider::verify,
                record_provider::projection,
                record_provider::meaning,
                record_provider::endpoints,
            )),
            CounterProofProfile::ScheduledTest => host.block_on(run_protected_counter_profile(
                execution_case,
                &authority,
                binding,
                deadline,
                profile,
                &configured,
                &scheduled_provider::verify,
                scheduled_provider::projection,
                scheduled_provider::meaning,
                scheduled_provider::endpoints,
            )),
        };
        original.expect("original Work, publication, native and notification custody");
    }

    #[test]
    fn recorded_authenticated_processing_fact_cannot_admit_a_counter_command() {
        non_request_admission(
            &record_provider::verify,
            record_provider::projection,
            CounterProofProfile::RecordedTest,
        );
    }
    #[test]
    fn scheduled_authenticated_processing_fact_cannot_admit_a_counter_command() {
        non_request_admission(
            &scheduled_provider::verify,
            scheduled_provider::projection,
            CounterProofProfile::ScheduledTest,
        );
    }
    fn non_request_admission<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
            CounterProofProfile,
            CounterProofPurpose,
            &str,
            &str,
            &protected::Limits,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
        profile: CounterProofProfile,
    ) {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actual host");
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 23,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(29),
        };
        let (binding, deadline) = host
            .block_on(async { authority.issue() })
            .expect("finite assignment");
        let mut configured = configured_counter_grants(binding, profile);
        configured[0] = counter_record(
            configured[0].scope,
            profile,
            CounterProtectedMeaning::IncrementProcessed,
            &[43],
            &mut [0; 8192],
        )
        .expect("independently registered non-command fact");
        let acquired = host.block_on(run_counter_profile(
            StartupMode::ExplicitTest,
            Some((&authority, binding, deadline)),
            &configured,
            async |original, records| {
                let (purpose, producer, consumer) = match records.iter().find(|record| {
                    record.protected.as_ref() == original.protected.as_ref()
                        && record.proof.as_ref() == original.proof.as_ref()
                }) {
                    Some(record) => (record.purpose, record.producer, record.intended_consumer),
                    None => return Err(VerificationError::Invalid),
                };
                provider(
                    original,
                    records,
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    profile,
                    purpose,
                    producer,
                    consumer,
                    &counter_test_json_limits(),
                )
                .await
            },
            projection,
            65_536,
            CounterWorkCase::Healthy,
            profile,
            [
                CounterStatus::Refused(CounterRefusal::Invalid),
                CounterStatus::Refused(CounterRefusal::Invalid),
                CounterStatus::ReadAdmitted,
            ],
            0,
        ));
        acquired.expect("actual native zero-effect and full original receipt custody");
    }
}

#[cfg(test)]
#[path = "retirement.rs"]
mod held_counter_retirement;

#[cfg(test)]
mod provider_receipt_custody {
    use super::{
        CounterRequest, PendingAdmission, Verification, VerificationError, acquire_verification,
    };
    use core::future::Future;
    use core::pin::{Pin, pin};
    use core::task::{Context, Poll, Waker};
    use futures_util::FutureExt;
    use std::panic::{AssertUnwindSafe, resume_unwind};
    use std::ptr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct ProviderReceipt {
        protected: Box<[u8]>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for ProviderReceipt {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ProviderVerification {
        receipt: Option<ProviderReceipt>,
        disposal: Option<Box<[u8; 2]>>,
        returned: Arc<AtomicUsize>,
    }
    impl Future for ProviderVerification {
        type Output = Result<ProviderReceipt, VerificationError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let producer = self.get_mut();
            let receipt = producer.receipt.take().expect("one original receipt");
            producer.returned.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(receipt))
        }
    }
    impl Drop for ProviderVerification {
        fn drop(&mut self) {
            if self.receipt.is_none() {
                let cause = self.disposal.take().expect("original disposal cause");
                resume_unwind(cause);
            }
        }
    }

    #[test]
    fn verification_keeps_returned_receipt_beside_original_disposal_panic() {
        let drops = Arc::new(AtomicUsize::new(0));
        let returned = Arc::new(AtomicUsize::new(0));
        let receipt = ProviderReceipt {
            protected: Box::from([7, 11]),
            drops: drops.clone(),
        };
        let receipt_allocation = receipt.protected.as_ptr();
        let cause = Box::new([13_u8, 17]);
        let cause_allocation = ptr::from_ref(cause.as_ref()).cast::<()>();
        let mut pending = PendingAdmission {
            correlation: 0,
            reservation_bytes: 0,
            original: CounterRequest {
                protected: Box::from([7, 11]),
                proof: Box::from([19]),
            },
            verification: Verification::Awaiting,
        };
        let raw_allocation = pending.original.protected.as_ptr();
        let proof_allocation = pending.original.proof.as_ptr();
        let observed_returned = returned.clone();
        let provider = move |original: &CounterRequest| {
            assert_eq!(original.protected.as_ref(), receipt.protected.as_ref());
            assert_eq!(original.proof.as_ref(), &[19]);
            ProviderVerification {
                receipt: Some(receipt),
                disposal: Some(cause),
                returned,
            }
        };
        let caught = {
            let mut operation =
                pin!(AssertUnwindSafe(acquire_verification(&mut pending, provider)).catch_unwind());
            let mut context = Context::from_waker(Waker::noop());
            operation.as_mut().poll(&mut context)
        };
        let Poll::Ready(Err(cause)) = caught else {
            panic!("actual producer disposal must raise its original cause");
        };
        assert_eq!(ptr::from_ref(cause.as_ref()).cast::<()>(), cause_allocation);
        assert_eq!(observed_returned.load(Ordering::SeqCst), 1);
        assert_eq!(pending.original.protected.as_ptr(), raw_allocation);
        assert_eq!(pending.original.proof.as_ptr(), proof_allocation);
        assert_eq!(pending.original.protected.as_ref(), &[7, 11]);
        assert_eq!(pending.original.proof.as_ref(), &[19]);
        let Verification::Acquired(Ok(receipt)) = &pending.verification else {
            panic!("provider Ready receipt was erased by producer disposal");
        };
        assert_eq!(receipt.protected.as_ptr(), receipt_allocation);
        assert_eq!(receipt.protected.as_ref(), &[7, 11]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let factory_entries = AtomicUsize::new(0);
        let acquired_again = {
            let mut verification = pin!(acquire_verification(&mut pending, |_| {
                factory_entries.fetch_add(1, Ordering::SeqCst);
                async { Err(VerificationError::Invalid) }
            }));
            let mut context = Context::from_waker(Waker::noop());
            verification.as_mut().poll(&mut context)
        };
        assert!(matches!(acquired_again, Poll::Ready(())));
        assert_eq!(factory_entries.load(Ordering::SeqCst), 0);
        assert!(
            matches!(&pending.verification, Verification::Acquired(Ok(retained)) if retained.protected.as_ptr() == receipt_allocation)
        );
        drop((pending, cause));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[cfg(all(test, feature = "protected-transport"))]
mod public_node;
