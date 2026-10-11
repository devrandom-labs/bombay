//! Counter's explicit local test profile; production identity is not established.
//! The primary service runs beside Work; received facts stay in caller-owned storage.
//! Finite byte conventions/limits are test inputs, not production defaults.
//! No network, KERI, durability, fresh global issuer or whole-heap bound.

use bombay::behavior::{
    BehaviorSettlements, ClassifySettlement, MessageProtocol, SettlementStatus,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use core::ops::{AsyncFn, AsyncFnOnce};
use futures_util::{FutureExt, future::maybe_done};
use std::collections::TryReserveError;
use std::env;
use std::error::Error;
use std::fmt;
use std::mem::{replace, size_of};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::ptr;
use std::time::{Duration, Instant};
use tokio::{
    runtime::{Builder, Handle, Id, Runtime, TryCurrentError},
    sync::oneshot,
};

#[path = "../counter.rs"]
mod counter;
use counter::{Counter, CounterMessage, CounterValue};

struct CounterRequest {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterOperation {
    Increment,
    Read,
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the healthy executable uses static grants; owning pending and Full retry tests exercise known revocation"
        )
    )]
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterWorkCase {
    Healthy,
    CounterBeforePublication,
    CounterAfterPublication,
    RequesterBeforePublication,
    RequesterAfterPublication,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the healthy executable keeps valid configuration; the owning constructor-refusal test acquires real native cleanup"
        )
    )]
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
enum VerificationError {
    Invalid,
    Stale,
    Unavailable(oneshot::error::RecvError),
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
enum CounterDelivery {
    Ready(CounterMessage),
    Full(CounterMessage),
    PossiblyAdmitted,
    Accepted,
    Closed(TrySendError<CounterMessage>),
    Refused {
        command: CounterMessage,
        refusal: CounterRefusal,
    },
}
enum Verification<R> {
    Awaiting,
    Acquired(Result<R, VerificationError>),
    Native {
        receipt: R,
        operation: CounterOperation,
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
    projection: fn(&R) -> &Scope,
    deadline: Instant,
    target: EstablishedRecipient<Counter>,
    permission: Permission,
    limits: CounterLimits,
    pending: Vec<PendingAdmission<R>>,
    retained: Vec<CounterReplayRecord>,
    reserved_bytes: usize,
    next_correlation: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterStatus {
    Accepted(CounterOperation),
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
    fn new(
        mode: StartupMode,
        assignment: Option<(Binding, Instant, EstablishedRecipient<Counter>)>,
        grant_issuer: &'static str,
        authority_generation: u64,
        permitted_operations: &'static [CounterOperation],
        projection: fn(&R) -> &Scope,
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
            deadline,
            target,
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
            Ok(receipt) => Ok((self.projection)(receipt)),
            Err(VerificationError::Invalid) => Err(CounterRefusal::Invalid),
            Err(VerificationError::Stale) => Err(CounterRefusal::Stale),
            Err(VerificationError::Unavailable(_)) => Err(CounterRefusal::Unavailable),
        };
        let (identity, operation) = match verified {
            Err(refusal) => {
                self.refuse(position, refusal);
                return;
            }
            Ok(scope) => {
                if let Some(refusal) = self.scope_refusal(scope) {
                    self.refuse(position, refusal);
                    return;
                }
                (scope.identity, scope.operation)
            }
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
            CounterOperation::Increment => CounterMessage::Increment,
            CounterOperation::Read => CounterMessage::Read(replies.clone()),
        };
        self.pending[position].verification = Verification::Native {
            receipt,
            operation,
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
    fn scope_refusal(&self, scope: &Scope) -> Option<CounterRefusal> {
        if scope.identity.binding != self.binding {
            Some(CounterRefusal::Binding)
        } else if scope.grant_issuer != self.grant_issuer
            || scope.authority_generation != self.authority_generation
            || !self.permitted_operations.contains(&scope.operation)
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
                Verification::Native { receipt, .. } => {
                    self.scope_refusal((self.projection)(receipt))
                }
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
        let prior = replace(delivery, CounterDelivery::PossiblyAdmitted);
        *delivery = match prior {
            CounterDelivery::Ready(command) | CounterDelivery::Full(command) => {
                let refusal = scope_refusal.or_else(|| match self.permission {
                    Permission::Revoked => Some(CounterRefusal::Revoked),
                    Permission::Granted if now >= self.deadline => Some(CounterRefusal::Expired),
                    Permission::Granted => None,
                });
                if let Some(refusal) = refusal {
                    CounterDelivery::Refused { command, refusal }
                } else {
                    match service.try_send(&self.target, command) {
                        Ok(()) => CounterDelivery::Accepted,
                        Err(TrySendError::Full(command)) => CounterDelivery::Full(command),
                        Err(error @ TrySendError::Closed(_)) => CounterDelivery::Closed(error),
                    }
                }
            }
            settled @ (CounterDelivery::Accepted
            | CounterDelivery::PossiblyAdmitted
            | CounterDelivery::Closed(_)
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
                operation,
                delivery: CounterDelivery::Accepted,
                ..
            } => CounterStatus::Accepted(*operation),
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
async fn acquire_verification<R>(
    pending: &mut PendingAdmission<R>,
    provider: impl AsyncFnOnce(&CounterRequest) -> Result<R, VerificationError>,
) {
    if matches!(pending.verification, Verification::Awaiting) {
        let acquired = provider(&pending.original).await;
        pending.verification = Verification::Acquired(acquired);
    }
}

// Provider-local distinct receipts; not cryptographic evidence or a wire profile.
type ProtectedRecord = (Scope, [u8; 1], [u8; 1]);
mod record_provider {
    use super::{
        AuthorityFreshness, CounterRequest, ProtectedRecord, ProviderAvailability, Scope,
        VerificationError, oneshot,
    };
    pub(super) struct Receipt {
        scope: Scope,
    }
    pub(super) fn projection(receipt: &Receipt) -> &Scope {
        &receipt.scope
    }
    pub(super) async fn verify(
        original: &CounterRequest,
        configured: &[ProtectedRecord],
        freshness: AuthorityFreshness,
        availability: ProviderAvailability,
    ) -> Result<Receipt, VerificationError> {
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
                for (scope, protected, proof) in configured {
                    if protected.as_slice() == original.protected.as_ref()
                        && proof.as_slice() == original.proof.as_ref()
                    {
                        return Ok(Receipt { scope: *scope });
                    }
                }
                Err(VerificationError::Invalid)
            }
        }
    }
}
mod scheduled_provider {
    use super::{
        AuthorityFreshness, CounterRequest, ProtectedRecord, ProviderAvailability, Scope,
        VerificationError, oneshot,
    };
    pub(super) struct Receipt {
        scope: Scope,
        #[expect(
            dead_code,
            reason = "opaque provider-owned evidence remains owned through caller receipt; heapful custody is measured without disclosing its bytes"
        )]
        evidence: Box<[u8]>,
    }
    pub(super) fn projection(receipt: &Receipt) -> &Scope {
        &receipt.scope
    }
    pub(super) async fn verify(
        original: &CounterRequest,
        configured: &[ProtectedRecord],
        freshness: AuthorityFreshness,
        availability: ProviderAvailability,
    ) -> Result<Receipt, VerificationError> {
        let (publication, receiving) = oneshot::channel();
        match availability {
            ProviderAvailability::Unavailable => drop(publication),
            ProviderAvailability::Available => {
                let verified = match freshness {
                    AuthorityFreshness::Stale => Err(VerificationError::Stale),
                    AuthorityFreshness::Current => configured
                        .iter()
                        .find(|(_, protected, proof)| {
                            protected.as_slice() == original.protected.as_ref()
                                && proof.as_slice() == original.proof.as_ref()
                        })
                        .map_or(Err(VerificationError::Invalid), |(scope, _, _)| {
                            Ok(Receipt {
                                scope: *scope,
                                evidence: vec![19].into_boxed_slice(),
                            })
                        }),
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
    ReportConsumed(u64),
    ReadObserved { correlation: u64, value: u64 },
}
type CounterRequests = MessageProtocol<MailAddr, CounterServiceInput>;
enum CounterRequesterMessage {
    Begin(u64),
    VerificationCompleted(CounterReport),
    ReadObserved { correlation: u64, value: u64 },
}
enum CounterRequesterState {
    Ready,
    Verifying(u64),
    WaitingRead(CounterReport),
    Complete {
        report: CounterReport,
        observation: Option<u64>,
    },
}
#[derive(thiserror::Error)]
enum CounterRequesterError {
    #[error("unexpected {kind}", kind = match .0 {
        CounterRequesterMessage::Begin(_) => "verification trigger",
        CounterRequesterMessage::VerificationCompleted(_) => "verification completion",
        CounterRequesterMessage::ReadObserved { .. } => "Counter snapshot",
    })]
    UnexpectedMessage(CounterRequesterMessage),
}
impl fmt::Debug for CounterRequesterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CounterRequesterError({self})")
    }
}
struct CounterRequester {
    service: EstablishedRecipient<CounterRequests>,
    counter: EstablishedRecipient<Counter>,
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
            CounterRequesterMessage::Begin(correlation) => {
                if !matches!(self.state, CounterRequesterState::Ready) {
                    return Err(CounterRequesterError::UnexpectedMessage(
                        CounterRequesterMessage::Begin(correlation),
                    ));
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
                    return Err(CounterRequesterError::UnexpectedMessage(
                        CounterRequesterMessage::VerificationCompleted(report),
                    ));
                }
                self.state = match report.status {
                    CounterStatus::Accepted(_) => CounterRequesterState::WaitingRead(report),
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
                    CounterStatus::Accepted(CounterOperation::Increment) => {
                        Ok(actions.send_reads(EstablishedDelivery::new(
                            self.counter.clone(),
                            CounterMessage::Read(self.replies.clone()),
                        )))
                    }
                    CounterStatus::Accepted(CounterOperation::Read)
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
                        Err(CounterRequesterError::UnexpectedMessage(
                            CounterRequesterMessage::ReadObserved { correlation, value },
                        ))
                    }
                }
            }
        }
    }
}
fn complete_counter(report: CounterReport) -> CounterRequesterMessage {
    CounterRequesterMessage::VerificationCompleted(report)
}
// Actual native custody, not a reduced shadow outcome. Every native field is inspected.
fn completed_actor<B>(origin: RootOrigin<B>, retirement: ActorRetirement<B, Never, ()>) -> B
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Settlements: ClassifySettlement,
{
    let ActorRetirement::Completed {
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        behavior,
        settlements,
        control,
        user,
        descendants,
        completion,
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
        panic!("actual native actor failed; no clean result is synthesized");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
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

fn assert_cancelled_actor<B>(origin: RootOrigin<B>, retirement: &ActorRetirement<B, Never, ()>)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::OwnerCancelled {
        capability_failures,
        retirement_failures,
        additional_failures,
        descendants,
        terminal_report,
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
    projection: fn(&R) -> &Scope,
    verification_maximum: usize,
    execution_case: CounterWorkCase,
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
            .execute_with::<_, _, Never, (), _, _>(|application| async move {
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
                        individual: NonZeroUsize::new(1024).expect("finite input"),
                        total: NonZeroUsize::new(match execution_case {
                            CounterWorkCase::ConstructorRefusal => 1,
                            _ => 4096,
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
                        limits,
                    ));
                    if let Some(Ok(authorization)) = service.as_mut() {
                        for protected in *b"IIR" {
                            let original = CounterRequest {
                                protected: vec![protected].into_boxed_slice(),
                                proof: vec![41].into_boxed_slice(),
                            };
                            allocations.push((
                                original.protected.as_ptr(),
                                original.proof.as_ptr(),
                                protected,
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
                                    .execute_with::<_, _, Never, (), _, _>(
                                        |application| async move {
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
                                        },
                                    )
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
                                                            async |original| {
                                                                provider(original, configured).await
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
                                                    status: CounterStatus::Accepted(_),
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
                            let product = receiving.await;
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
        let counter_product = counter_receiving.await;
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
                ApplicationOutcome::Interrupted { cleanup: Ok(()) },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = counter_product
            else {
                panic!("original failed Work coexists with native and both notifications");
            };
            assert_cancelled_actor(origin, &retirement);
            for (execution, product, handoff, _) in &requester_products {
                assert!(execution.is_ok());
                let (
                    ApplicationOutcome::Completed {
                        cleanup: Ok(()),
                        output: (publication, permission),
                    },
                    Ok((origin, retirement)),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }),
                ) = product
                else {
                    panic!("outer Work fault retains requester native and notifications");
                };
                assert!(publication.is_ok());
                assert!(permission.is_err());
                assert!(matches!(handoff, Some(Ok(()))));
                let ActorRetirement::Completed {
                    completion: Completion::Stopped,
                    capability_failures,
                    retirement_failures,
                    additional_failures,
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
                ApplicationOutcome::Interrupted { cleanup: Ok(()) },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
            assert_eq!(pending.original.protected.as_ref(), &[*byte]);
            assert_eq!(pending.original.proof.as_ref(), &[41]);
            let (
                ApplicationOutcome::Completed {
                    cleanup: Ok(()),
                    output: counter_work_receipts,
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
        CounterWorkCase::Healthy | CounterWorkCase::ConstructorRefusal => {}
    }
    // The original execution panic, if acquired, coexists with actual native
    // receiving and both notifications. No JoinError or clean product is forged.
    // These following assertions are the healthy demo oracle ONLY; owning panic
    // tests must inspect the actual Interrupted/cleanup/native products directly.
    // Every native product/full raw+R/native error is outside Work before assertions.
    assert!(counter_execution.is_ok());
    let (
        ApplicationOutcome::Completed {
            output: (publication, permission),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
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
    for (((execution, product, handoff, _), expected_report), sent_report) in requester_products
        .into_iter()
        .zip(expected_reports)
        .zip(observations.iter().map(|(_, _, _, report, ..)| *report))
    {
        assert!(matches!(handoff, Some(Ok(()))));
        assert!(execution.is_ok());
        let (
            ApplicationOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
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
            CounterStatus::Accepted(_) => Some(expected_snapshot),
            CounterStatus::Refused(_) | CounterStatus::Closed | CounterStatus::PossiblyAdmitted => {
                None
            }
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
            Some(CounterStatus::Accepted(_)) => {
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
            Some(CounterStatus::Closed | CounterStatus::PossiblyAdmitted) | None => {
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
        assert_eq!(original.protected.as_ref(), &[protected]);
        assert_eq!(original.proof.as_ref(), &[41]);
        match (expected_report, verification) {
            (
                CounterStatus::Accepted(expected_operation),
                Verification::Native {
                    receipt,
                    operation,
                    delivery: CounterDelivery::Accepted,
                },
            ) => {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.1.as_slice() == original.protected.as_ref()
                            && record.2.as_slice() == original.proof.as_ref()
                    })
                    .expect("independent configured protected authority");
                assert_eq!(projection(&receipt), &configured_receipt.0);
                assert_eq!(operation, expected_operation);
            }
            (
                CounterStatus::Accepted(CounterOperation::Increment),
                Verification::Duplicate {
                    receipt,
                    report:
                        CounterReport {
                            status: CounterStatus::Accepted(CounterOperation::Increment),
                            ..
                        },
                },
            ) => {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.1.as_slice() == original.protected.as_ref()
                            && record.2.as_slice() == original.proof.as_ref()
                    })
                    .expect("independent configured protected authority");
                assert_eq!(projection(&receipt), &configured_receipt.0);
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
            ) if matches!(expected, CounterRefusal::Binding | CounterRefusal::Grant) => {
                let configured_receipt = configured
                    .iter()
                    .find(|record| {
                        record.1.as_slice() == original.protected.as_ref()
                            && record.2.as_slice() == original.proof.as_ref()
                    })
                    .expect("independent refused Scope remains owned with actual R");
                assert_eq!(projection(&receipt), &configured_receipt.0);
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
                        CounterMessage::Increment | CounterMessage::IncrementAndReply { .. } => {
                            CounterOperation::Increment
                        }
                        CounterMessage::Read(_) => CounterOperation::Read,
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
    let expected_retention = [CounterOperation::Increment, CounterOperation::Read]
        .iter()
        .filter(|operation| expected_reports.contains(&CounterStatus::Accepted(**operation)))
        .count();
    assert_eq!(authorization.retained.len(), expected_retention);
    Ok(())
}

fn configured_counter_grants(binding: Binding) -> [ProtectedRecord; 2] {
    [
        (
            Scope {
                identity: Identity {
                    binding,
                    sequence: 0,
                },
                operation: CounterOperation::Increment,
                grant_issuer: "grant-controller",
                authority_generation: 7,
            },
            *b"I",
            [41],
        ),
        (
            Scope {
                identity: Identity {
                    binding,
                    sequence: 1,
                },
                operation: CounterOperation::Read,
                grant_issuer: "grant-controller",
                authority_generation: 7,
            },
            *b"R",
            [41],
        ),
    ]
}

fn main() -> Result<(), StartupRefusal> {
    let mode = match env::args().nth(1).as_deref() {
        Some("--explicit-test") => StartupMode::ExplicitTest,
        _ => StartupMode::ProductionRequired,
    };
    if let StartupMode::ProductionRequired = mode {
        return Err(StartupRefusal::TestProvider);
    }
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller-owned explicit host");
    let mut authority = AssignmentAuthority {
        host: &host,
        runtime_generation: 23,
        grant_issuer: "grant-controller",
        authority_generation: 7,
        permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
        next: Some(29),
    };
    let (binding, deadline) = host.block_on(async { authority.issue() })?;
    let configured = configured_counter_grants(binding);
    host.block_on(run_counter_profile(
        StartupMode::ExplicitTest,
        Some((&authority, binding, deadline)),
        &configured,
        async |original, records| {
            record_provider::verify(
                original,
                records,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
            )
            .await
        },
        record_provider::projection,
        256,
        CounterWorkCase::Healthy,
        [
            CounterStatus::Accepted(CounterOperation::Increment),
            CounterStatus::Accepted(CounterOperation::Increment),
            CounterStatus::Accepted(CounterOperation::Read),
        ],
        1,
    ))?;
    let (binding, deadline) = host.block_on(async { authority.issue() })?;
    let configured = configured_counter_grants(binding);
    host.block_on(run_counter_profile(
        StartupMode::ExplicitTest,
        Some((&authority, binding, deadline)),
        &configured,
        async |original, records| {
            scheduled_provider::verify(
                original,
                records,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
            )
            .await
        },
        scheduled_provider::projection,
        256,
        CounterWorkCase::Healthy,
        [
            CounterStatus::Accepted(CounterOperation::Increment),
            CounterStatus::Accepted(CounterOperation::Increment),
            CounterStatus::Accepted(CounterOperation::Read),
        ],
        1,
    ))?;
    Ok(())
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
        let configured = [(scope, *b"I", [41])];
        let original = CounterRequest {
            protected: vec![b'I'].into_boxed_slice(),
            proof: vec![41].into_boxed_slice(),
        };
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let recorded = host
            .block_on(record_provider::verify(
                &original,
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
            ))
            .unwrap_or_else(|_| panic!("the independently configured record verifies"));
        let scheduled = host
            .block_on(scheduled_provider::verify(
                &original,
                &configured,
                AuthorityFreshness::Current,
                ProviderAvailability::Available,
            ))
            .unwrap_or_else(|_| panic!("the independently configured scheduled evidence verifies"));
        assert_eq!(record_provider::projection(&recorded), &scope);
        assert_eq!(scheduled_provider::projection(&scheduled), &scope);
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
            let configured = configured_counter_grants(binding);
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
                    )
                    .await
                },
                record_provider::projection,
                256,
                execution_case,
                [
                    CounterStatus::Accepted(CounterOperation::Increment),
                    CounterStatus::Accepted(CounterOperation::Increment),
                    CounterStatus::Accepted(CounterOperation::Read),
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
        let configured = configured_counter_grants(binding);
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
                )
                .await
            },
            record_provider::projection,
            256,
            CounterWorkCase::ConstructorRefusal,
            [
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Read),
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
        let configured = configured_counter_grants(binding);
        // This concrete receipt is a real non-Send read lease of the configured
        // authority record. No marker, Rc-cloned receipt or extra observation.
        let authority_records = RefCell::new(configured);
        let acquired = host.block_on(run_counter_profile(
            StartupMode::ExplicitTest,
            Some((&authority, binding, deadline)),
            &configured,
            async |original, expected| {
                let records = authority_records.borrow();
                let position = records.iter().position(|record| {
                    expected.contains(record)
                        && original.protected.as_ref() == record.1
                        && original.proof.as_ref() == record.2
                });
                match position {
                    Some(position) => Ok(Ref::map(records, |records| &records[position])),
                    None => Err(VerificationError::Invalid),
                }
            },
            |record| &record.0,
            256,
            CounterWorkCase::Healthy,
            [
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Read),
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

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Preserve the real pending wait, original receipt and sequential native acquisition before revocation oracles."
    )]
    fn canceled_borrowed_verification_observes_known_revocation() {
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
            let records = [configured_counter_grants(binding)[0]];
            let mut original = Some(CounterRequest {
                protected: Box::from(*b"I"),
                proof: Box::from([41]),
            });
            let request_allocations = original
                .as_ref()
                .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
            let mut counter_authorization = None;
            let mut reservation = None;
            let mut verification_observations = None;
            let original_storage = &mut original;
            let reservation_storage = &mut reservation;
            let observations_storage = &mut verification_observations;
            let authorization_storage = &mut counter_authorization;
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
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
                        CounterLimits {
                            incoming: NonZeroUsize::new(2).expect("explicit count"),
                            retained: NonZeroUsize::new(2).expect("explicit count"),
                            individual: NonZeroUsize::new(1024).expect("explicit bytes"),
                            total: NonZeroUsize::new(4096).expect("explicit bytes"),
                            verification_maximum: 256,
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
                        let mut verification = pin!(acquire_verification(
                            &mut authorization.pending[0],
                            async |original| {
                                released.await.map_err(VerificationError::Unavailable)?;
                                record_provider::verify(
                                    original,
                                    &records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                )
                                .await
                            }
                        ));
                        let canceled_wait = verification.as_mut().now_or_never();
                        authorization.permission = Permission::Revoked;
                        let after_cancel = verification.as_mut().now_or_never();
                        let held_after_cancel = authorization.reserved_bytes;
                        let released = release.send(());
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
            let native = receiving.await;
            let (
                ApplicationOutcome::Completed {
                    output: stopped,
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
            assert!(released.is_ok());
            assert!(sent.is_ok());
            let Some(CounterAdmission {
                original,
                verification:
                    Verification::Native {
                        receipt,
                        operation: CounterOperation::Increment,
                        delivery:
                            CounterDelivery::Refused {
                                command: CounterMessage::Increment,
                                refusal: CounterRefusal::Revoked,
                            },
                    },
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
                (&b"I"[..], &[41][..])
            );
            assert_eq!(record_provider::projection(&receipt), &records[0].0);
            let Some(Ok(authorization)) = counter_authorization else {
                panic!("original setup cause");
            };
            assert!(authorization.pending.is_empty());
            assert_eq!(authorization.retained.len(), 1);
        });
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
            let records = configured_counter_grants(binding);
            let mut original = Some(CounterRequest {
                protected: Box::from(*b"I"),
                proof: Box::from([41]),
            });
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
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
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
                        CounterLimits {
                            incoming: NonZeroUsize::new(2).expect("finite input"),
                            retained: NonZeroUsize::new(2).expect("finite input"),
                            individual: NonZeroUsize::new(1024).expect("finite input"),
                            total: NonZeroUsize::new(4096).expect("finite input"),
                            verification_maximum: 256,
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
                    acquire_verification(&mut authorization.pending[0], async |original| {
                        record_provider::verify(
                            original,
                            &records,
                            AuthorityFreshness::Current,
                            ProviderAvailability::Available,
                        )
                        .await
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
            let acquired = receiving.await;
            let (
                ApplicationOutcome::Completed {
                    output: stopped,
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
                verification:
                    Verification::Native {
                        receipt,
                        operation: CounterOperation::Increment,
                        delivery,
                    },
            }) = admission
            else {
                panic!("full original/raw/R/native command remain typed");
            };
            match (policy, delivery) {
                (
                    CounterRetryPolicy::KnownRevocation,
                    CounterDelivery::Refused {
                        command: CounterMessage::Increment,
                        refusal: CounterRefusal::Revoked,
                    },
                )
                | (
                    CounterRetryPolicy::DeadlineReached,
                    CounterDelivery::Refused {
                        command: CounterMessage::Increment,
                        refusal: CounterRefusal::Expired,
                    },
                )
                | (
                    CounterRetryPolicy::GrantIssuerChanged
                    | CounterRetryPolicy::AuthorityGenerationChanged
                    | CounterRetryPolicy::OperationRemoved,
                    CounterDelivery::Refused {
                        command: CounterMessage::Increment,
                        refusal: CounterRefusal::Grant,
                    },
                )
                | (CounterRetryPolicy::Granted, CounterDelivery::Accepted) => {}
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
                (&b"I"[..], &[41][..])
            );
            assert_eq!(record_provider::projection(&receipt), &records[0].0);
            assert!(authorization.pending.is_empty());
            assert_eq!(authorization.retained.len(), 1);
            assert!(authorization.reserved_bytes < held);
        });
    }
}

#[cfg(test)]
#[global_allocator]
static COUNTER_COMPONENT_ALLOCATIONS: dhat::Alloc = dhat::Alloc;

#[cfg(test)]
mod allocation_profile {
    use super::*;
    use std::env::VarError;
    use std::pin::pin;
    use std::process::Command;
    use tokio::runtime::Runtime;

    // Provider-only calibration inside the already isolated owning test process.
    // Caller originals/configuration and the existing host precede the profiler.
    fn measure_counter_provider_refusals<R>(
        host: &Runtime,
        originals: &[CounterRequest; 4],
        configured: &[ProtectedRecord],
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
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
                host.block_on(provider(original, configured, freshness, availability))
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
                requested.1 <= 256,
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
    fn counter_request(protected: u8) -> CounterRequest {
        CounterRequest {
            protected: Box::from([protected]),
            proof: Box::from([41]),
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
        projection: fn(&R) -> &Scope,
        order: IdentityOrder,
    ) -> CounterAllocationObservations<R> {
        // Refused inputs and the release channel belong to the test caller.
        let overflow = counter_request(b'X');
        let overflow_allocations = (overflow.protected.as_ptr(), overflow.proof.as_ptr());
        let byte_overflows = [counter_request(b'Y'), counter_request(b'Y')];
        let (release, released) = oneshot::channel();
        let profiler = dhat::Profiler::builder().testing().build();
        let (authorization, header_requests) = requested_allocations(|| {
            CounterAuthorization::new(
                StartupMode::ExplicitTest,
                Some((binding, deadline, target)),
                "grant-controller",
                7,
                &[CounterOperation::Increment, CounterOperation::Read],
                projection,
                CounterLimits {
                    incoming: NonZeroUsize::new(2).expect("explicit count"),
                    retained: NonZeroUsize::new(2).expect("explicit count"),
                    individual: NonZeroUsize::new(1024).expect("explicit bytes"),
                    total: NonZeroUsize::new(4096).expect("explicit bytes"),
                    verification_maximum: 256,
                },
            )
        });
        let (originals, incoming_requests) = requested_allocations(|| match order {
            IdentityOrder::Increasing => [counter_request(b'I'), counter_request(b'R')],
            IdentityOrder::Decreasing => [counter_request(b'R'), counter_request(b'I')],
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
            acquire_verification(&mut authorization.pending[position], async |original| {
                provider(original, configured).await
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
            let mut verification = pin!(acquire_verification(
                &mut authorization.pending[0],
                async |original| {
                    released.await.map_err(VerificationError::Unavailable)?;
                    provider(original, configured).await
                }
            ));
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
        acquire_verification(&mut authorization.pending[1], async |original| {
            provider(original, configured).await
        })
        .await;
        let after = dhat::HeapStats::get();
        measured.verifier_requests[1] = (
            after.total_blocks - before.total_blocks,
            after.total_bytes - before.total_bytes,
        );
        authorization.permission = Permission::Revoked;
        for position in 0..2 {
            let ((), requested) = requested_allocations(|| {
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
        let (duplicate, _) = requested_allocations(|| counter_request(b'I'));
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
            acquire_verification(&mut authorization.pending[position], async |original| {
                provider(original, configured).await
            })
            .await;
            let ((), storage) = requested_allocations(|| {
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
                    authorization.limits.individual =
                        NonZeroUsize::new(257).expect("individual capacity plus one");
                }
                CounterByteLimit::Aggregate => {
                    authorization.limits.total = NonZeroUsize::new(
                        authorization
                            .reserved_bytes
                            .checked_add(257)
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
                acquire_verification(&mut authorization.pending[position], async |original| {
                    provider(original, configured).await
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
        drop(profiler);
        measured
    }
    fn assert_counter_storage<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> &Scope,
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
        assert_eq!(measured.incoming_requests, (4, 4));
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
        assert_eq!(measured.reserved_before_cancel, headers + 2 * 258);
        assert_eq!(
            measured.reserved_after_cancel,
            measured.reserved_before_cancel
        );
        assert!(matches!(measured.verification_release, Some(Ok(()))));
        for requested in measured.verifier_requests {
            assert!(
                requested.1 <= 256,
                "actual provider requests fit the already declared verifier budget"
            );
        }
        let comparison_requested: u64 = measured
            .comparison_requests
            .iter()
            .map(|requested| requested.1)
            .sum();
        assert_eq!(comparison_requested, 2);
        assert_eq!(
            measured
                .reserved_unread
                .checked_sub(measured.reserved_after_cancel)
                .expect("comparison-only charge increase") as u64,
            comparison_requested,
            "protected comparison allocation is independently charged"
        );
        assert_eq!(measured.reserved_unread, headers + 2 * 258 + 2);
        assert_eq!(measured.reserved_after_report, measured.reserved_unread);
        assert_eq!(measured.reserved_after_consume, headers + 2);
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
        assert!(measured.reserved_unread <= 4096);
        println!(
            "Counter component requests {finite_requested}; headers {headers}; verifier {:?}; recorded reservation {}",
            measured.verifier_requests, measured.reserved_unread
        );
    }
    fn assert_counter_refused_commands<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> &Scope,
        configured: &[ProtectedRecord],
    ) {
        for (admission, allocation) in measured.originals.iter().zip(measured.original_allocations)
        {
            let Some(CounterAdmission {
                original,
                verification:
                    Verification::Native {
                        receipt,
                        operation,
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
                    record.1.as_slice() == original.protected.as_ref()
                        && record.2.as_slice() == original.proof.as_ref()
                })
                .expect("independently configured original authority record");
            assert_eq!(projection(receipt), &expected.0);
            assert_eq!(expected.0.operation, *operation);
            let protected = match operation {
                CounterOperation::Increment => b'I',
                CounterOperation::Read => b'R',
            };
            assert_eq!(original.protected.as_ref(), &[protected]);
            assert!(matches!(
                (operation, command),
                (CounterOperation::Increment, CounterMessage::Increment)
                    | (CounterOperation::Read, CounterMessage::Read(_))
            ));
        }
    }
    fn assert_counter_duplicate_receipt<R>(
        measured: &CounterAllocationObservations<R>,
        projection: fn(&R) -> &Scope,
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
        assert_eq!(duplicate.protected.as_ref(), b"I");
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
            .find(|record| record.1 == *b"I" && record.2 == [41])
            .expect("independent duplicate authority record");
        assert_eq!(projection(receipt), &expected.0);
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
            let configured = configured_counter_grants(binding);
            let mut record = None;
            let mut scheduled = None;
            let mut snapshot = None;
            let snapshot_storage = &mut snapshot;
            let record_storage = &mut record;
            let scheduled_storage = &mut scheduled;
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
                    let target = application.root().established_recipient();
                    let interface = application.interface(());
                    let requests = interface
                        .external::<CounterRequests>()
                        .expect("real service port");
                    let mut replies = interface
                        .external::<CounterValue>()
                        .expect("real reply port");
                    *record_storage = Some([
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            &configured,
                            &async |original, records| {
                                record_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                )
                                .await
                            },
                            record_provider::projection,
                            IdentityOrder::Increasing,
                        )
                        .await,
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            &configured,
                            &async |original, records| {
                                record_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                )
                                .await
                            },
                            record_provider::projection,
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
                            &configured,
                            &async |original, records| {
                                scheduled_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                )
                                .await
                            },
                            scheduled_provider::projection,
                            IdentityOrder::Increasing,
                        )
                        .await,
                        measure_counter_custody(
                            binding,
                            deadline,
                            target.clone(),
                            &requests,
                            &replies.recipient(),
                            &configured,
                            &async |original, records| {
                                scheduled_provider::verify(
                                    original,
                                    records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                )
                                .await
                            },
                            scheduled_provider::projection,
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
            let native = receiving.await;
            let (
                ApplicationOutcome::Completed {
                    output: stopped,
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
            let Some(record) = record else {
                panic!("actual record-provider observations");
            };
            let Some(scheduled) = scheduled else {
                panic!("actual scheduled-provider observations");
            };
            for observed in &record {
                assert_counter_storage(observed, record_provider::projection, &configured);
            }
            for observed in &scheduled {
                assert_counter_storage(observed, scheduled_provider::projection, &configured);
            }
        });
        // This code runs only in AllocationRole::Isolated, after the actual
        // Application/native component controls above, with no active profiler.
        let configured = configured_counter_grants(Binding {
            deployment: "test-deployment",
            caller: "caller",
            controller: "controller",
            actor: "counter",
            host: "counter-host",
            protocol: "counter-protocol",
            runtime_generation: 23,
            actor_generation: 29,
        });
        let originals = [
            counter_request(b'X'),
            counter_request(b'I'),
            counter_request(b'I'),
            counter_request(b'I'),
        ];
        let record_refusals = measure_counter_provider_refusals(
            &host,
            &originals,
            &configured,
            &record_provider::verify,
        );
        let scheduled_refusals = measure_counter_provider_refusals(
            &host,
            &originals,
            &configured,
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
        let configured = configured_counter_grants(binding);
        let original = CounterRequest {
            protected: Box::from(*b"I"),
            proof: Box::from([41]),
        };
        let record = record_provider::verify(
            &original,
            &configured,
            AuthorityFreshness::Stale,
            ProviderAvailability::Available,
        )
        .await;
        let scheduled = scheduled_provider::verify(
            &original,
            &configured,
            AuthorityFreshness::Stale,
            ProviderAvailability::Unavailable,
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
        counter_provider_outcomes(&record_provider::verify, record_provider::projection);
    }

    #[test]
    fn scheduled_provider_outcomes_reach_the_actual_counter_requester() {
        counter_provider_outcomes(&scheduled_provider::verify, scheduled_provider::projection);
    }

    fn counter_provider_outcomes<R>(
        provider: &impl AsyncFn(
            &CounterRequest,
            &[ProtectedRecord],
            AuthorityFreshness,
            ProviderAvailability,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> &Scope,
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
            let configured = configured_counter_grants(binding);
            let (records, freshness, availability, expected_reports, expected_snapshot) = match case
            {
                CounterEvidenceCase::Accepted => (
                    configured.as_slice(),
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                    [
                        CounterStatus::Accepted(CounterOperation::Increment),
                        CounterStatus::Accepted(CounterOperation::Increment),
                        CounterStatus::Accepted(CounterOperation::Read),
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
                    provider(original, records, freshness, availability).await
                },
                projection,
                256,
                CounterWorkCase::Healthy,
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
        counter_scopes(&record_provider::verify, record_provider::projection);
    }

    #[test]
    fn scheduled_scopes_refuse_before_replay_and_native_mutation() {
        counter_scopes(&scheduled_provider::verify, scheduled_provider::projection);
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
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> &Scope,
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
            let mut configured = configured_counter_grants(binding);
            for (scope, _, _) in &mut configured {
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
            let (expected, snapshot) = match case {
                CounterScopeCase::Valid => (
                    [
                        CounterStatus::Accepted(CounterOperation::Increment),
                        CounterStatus::Accepted(CounterOperation::Increment),
                        CounterStatus::Accepted(CounterOperation::Read),
                    ],
                    1,
                ),
                CounterScopeCase::IncrementOnly => {
                    authority.permitted_operations = &[CounterOperation::Increment];
                    (
                        [
                            CounterStatus::Accepted(CounterOperation::Increment),
                            CounterStatus::Accepted(CounterOperation::Increment),
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
                            CounterStatus::Accepted(CounterOperation::Read),
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
                    )
                    .await
                },
                projection,
                256,
                CounterWorkCase::Healthy,
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
        run_counter_profile(
            mode,
            assignment,
            configured,
            async |original, records| {
                record_provider::verify(
                    original,
                    records,
                    AuthorityFreshness::Current,
                    ProviderAvailability::Available,
                )
                .await
            },
            record_provider::projection,
            256,
            CounterWorkCase::Healthy,
            [
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Increment),
                CounterStatus::Accepted(CounterOperation::Read),
            ],
            1,
        )
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
        let configured = configured_counter_grants(binding);
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
            &configured_counter_grants(first),
        ));
        authority.grant_issuer = "";
        let empty_issuer = host.block_on(async { authority.issue() });
        let invalid_consumer = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &configured_counter_grants(first),
        ));
        authority.grant_issuer = "grant-controller";
        authority.permitted_operations = &[];
        let empty_operations = host.block_on(async { authority.issue() });
        let empty_consumer = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, first, first_deadline)),
            &configured_counter_grants(first),
        ));
        let next_after_grant_refusals = authority.next;
        authority.permitted_operations = &[CounterOperation::Increment, CounterOperation::Read];
        let (replacement, replacement_deadline) = host
            .block_on(async { authority.issue() })
            .expect("replacement finite assignment");
        let replacement_result = host.block_on(counter_on_assignment(
            StartupMode::ExplicitTest,
            Some((&authority, replacement, replacement_deadline)),
            &configured_counter_grants(replacement),
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
        counter_proof_rotation(&record_provider::verify, record_provider::projection);
    }

    #[test]
    fn scheduled_proof_rotation_preserves_the_same_live_replay_owner() {
        counter_proof_rotation(&scheduled_provider::verify, scheduled_provider::projection);
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
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> &Scope,
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
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
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
                            CounterLimits {
                                incoming: NonZeroUsize::new(2).expect("configured input"),
                                retained: NonZeroUsize::new(2).expect("configured input"),
                                individual: NonZeroUsize::new(1024).expect("configured input"),
                                total: NonZeroUsize::new(4096).expect("configured input"),
                                verification_maximum: 256,
                            },
                        ));
                        if let Some(Ok(authorization)) = service.as_mut() {
                            let mut configured = configured_counter_grants(binding);
                            for proof in [
                                CounterProof::Original,
                                CounterProof::Replacement,
                                CounterProof::Retired,
                            ] {
                                if let CounterProof::Replacement = proof {
                                    // This is trusted configuration, not incoming bytes.
                                    // The same authorization owner/identity stays live.
                                    configured[0].2 = [42];
                                    configured[0].0.grant_issuer = "rotated-grant-controller";
                                    configured[0].0.authority_generation = 8;
                                    authorization.grant_issuer = "rotated-grant-controller";
                                    authorization.authority_generation = 8;
                                }
                                let proof_byte = match proof {
                                    CounterProof::Original | CounterProof::Retired => 41,
                                    CounterProof::Replacement => 42,
                                };
                                let original = CounterRequest {
                                    protected: Box::from(*b"I"),
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
                                acquire_verification(
                                    &mut authorization.pending[position],
                                    async |original| {
                                        provider(
                                            original,
                                            &configured,
                                            AuthorityFreshness::Current,
                                            ProviderAvailability::Available,
                                        )
                                        .await
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
            let counter_product = receiving.await;
            let (
                ApplicationOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
                        CounterStatus::Accepted(CounterOperation::Increment)
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
                assert_eq!(original.protected.as_ref(), &b"I"[..]);
                assert_eq!(original.proof.as_ref(), &[byte]);
                match (proof, verification) {
                    (
                        CounterProof::Original,
                        Verification::Native {
                            receipt,
                            operation: CounterOperation::Increment,
                            delivery: CounterDelivery::Accepted,
                        },
                    ) => {
                        assert_eq!(
                            projection(&receipt),
                            &configured_counter_grants(binding)[0].0
                        );
                    }
                    (CounterProof::Replacement, Verification::Duplicate { receipt, report }) => {
                        let expected_scope = Scope {
                            grant_issuer: "rotated-grant-controller",
                            authority_generation: 8,
                            ..configured_counter_grants(binding)[0].0
                        };
                        assert_eq!(projection(&receipt), &expected_scope);
                        assert_eq!(report.correlation, correlation);
                        assert_eq!(
                            report.status,
                            CounterStatus::Accepted(CounterOperation::Increment)
                        );
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
            assert_eq!(authorization.retained[0].protected.as_ref(), &b"I"[..]);
            assert!(matches!(
                authorization.retained[0].retention,
                CounterReplayRetention::Consumed(CounterReport {
                    status: CounterStatus::Accepted(CounterOperation::Increment),
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
            let configured = configured_counter_grants(binding);
            let mut original = Some(CounterRequest {
                protected: Box::from(*b"I"),
                proof: Box::from([41]),
            });
            let pointers = original
                .as_ref()
                .map(|original| (original.protected.as_ptr(), original.proof.as_ptr()));
            let (publication, ports) = oneshot::channel();
            let (permitted, permission) = oneshot::channel();
            let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
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
                            CounterLimits {
                                incoming: NonZeroUsize::new(1).expect("explicit incoming count"),
                                retained: NonZeroUsize::new(1).expect("explicit retained count"),
                                individual: NonZeroUsize::new(1024).expect("explicit bytes"),
                                total: NonZeroUsize::new(4096).expect("explicit bytes"),
                                verification_maximum: 256,
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
                                acquire_verification(
                                    &mut authorization.pending[position],
                                    async |original| {
                                        record_provider::verify(
                                            original,
                                            &configured,
                                            AuthorityFreshness::Current,
                                            ProviderAvailability::Available,
                                        )
                                        .await
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
            let native = receiving.await;
            let (
                ApplicationOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, native_retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
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
                        operation: CounterOperation::Increment,
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
            assert_eq!(recovered.protected.as_ref(), b"I");
            assert_eq!(recovered.proof.as_ref(), &[41]);
            assert_eq!(
                Some((recovered.protected.as_ptr(), recovered.proof.as_ptr())),
                pointers
            );
            assert_eq!(record_provider::projection(receipt), &configured[0].0);
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
            let error = CounterRequesterError::UnexpectedMessage(message);
            let display = format!("{error}");
            let debug = format!("{error:?}");
            let expected_debug = format!("CounterRequesterError({expected})");
            let source = Error::source(&error);
            assert_eq!(display, expected);
            assert_eq!(debug, expected_debug);
            assert!(source.is_none());
            let CounterRequesterError::UnexpectedMessage(message) = error;
            match message {
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
