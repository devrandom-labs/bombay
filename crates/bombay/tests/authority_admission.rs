//! Private local admission/replay evidence; no production identity or networking contract.
use bombay::behavior::{BehaviorBase, EstablishedDelivery, MessageProtocol, Protocol};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use core::ops::AsyncFn;
use std::collections::{TryReserveError, VecDeque};
use std::future::Future;
use std::mem::{replace, size_of, size_of_val};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::{runtime::Builder, sync::oneshot};
mod application_support;
use application_support::{RootTerminal, assert_completed};

#[global_allocator]
static ALLOCATION_PROFILE: dhat::Alloc = dhat::Alloc;
const NOTIFICATION_RESERVATION: usize = 128;
const VERIFICATION_RESERVATION: usize = 256;
const CONTROL_RESERVATION: usize = 128;
const LOCAL_STORAGE_CEILING: usize = 4096;
const INCOMING_RESERVATION: usize =
    NOTIFICATION_RESERVATION + VERIFICATION_RESERVATION + CONTROL_RESERVATION;

fn incoming_reservation(original: &ProtectedCommand) -> Option<usize> {
    original
        .payload
        .len()
        .checked_add(original.proof.len())?
        .checked_add(INCOMING_RESERVATION)
}

fn allocated_storage<T>(allocation: impl FnOnce() -> T) -> (T, (u64, u64)) {
    let before = dhat::HeapStats::get();
    let owned = allocation();
    let after = dhat::HeapStats::get();
    (
        owned,
        (
            after.total_blocks - before.total_blocks,
            after.total_bytes - before.total_bytes,
        ),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Permission {
    Granted,
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Counter,
    Arithmetic,
    DifferentTarget,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmissionCase {
    Granted,
    Revoked,
    Expired,
    Invalid,
    Stale,
    Unavailable,
    Scope,
    MissingFreshness,
}
#[derive(Clone, Copy)]
enum VerificationTiming {
    Immediate,
    Stalled,
}
enum AuthorityUpdate {
    Permission(Permission),
    Time(Instant),
}
struct AuthorityProgress {
    verification: Poll<()>,
    acknowledgement: Poll<Result<(), oneshot::error::RecvError>>,
    permission: Permission,
    now: Instant,
    deadline: Instant,
    released: Result<(), ()>,
}
#[derive(Debug)]
enum VerificationError {
    Invalid,
    Stale,
    Unavailable(Option<oneshot::error::RecvError>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    Invalid,
    Stale,
    Unavailable,
    Scope,
    Revoked,
    Expired,
    Full,
    Closed,
    Count,
    IndividualBytes,
    TotalBytes,
    ByteAccountingOverflow,
}
#[derive(Debug)]
struct ProtectedCommand {
    payload: Box<[u8]>,
    proof: Box<[u8]>,
}
fn protected_command() -> ProtectedCommand {
    ProtectedCommand {
        payload: vec![2, 7].into_boxed_slice(),
        proof: vec![41].into_boxed_slice(),
    }
}
mod record_provider {
    use super::replay::{Identity, ProtectedRecord};
    use super::{AdmissionCase, Instant, ProtectedCommand, Scope, VerificationError, oneshot};
    pub(super) struct Receipt {
        identity: Option<Identity>,
        amount: u64,
        scope: Scope,
        deadline: Option<Instant>,
    }
    pub(super) fn projection(receipt: &Receipt) -> (u64, Scope, Option<Instant>) {
        (receipt.amount, receipt.scope, receipt.deadline)
    }
    pub(super) fn identity(receipt: &Receipt) -> Option<Identity> {
        receipt.identity
    }
    pub(super) async fn verify_replay(
        original: &ProtectedCommand,
        claimed: Identity,
        configured: &[ProtectedRecord],
        deadline: Instant,
    ) -> Result<Receipt, VerificationError> {
        for (identity, protected, proof) in configured {
            if *identity == claimed
                && protected.as_slice() == original.payload.as_ref()
                && proof.as_slice() == original.proof.as_ref()
            {
                return Ok(Receipt {
                    identity: Some(*identity),
                    amount: 2,
                    scope: identity.binding.target,
                    deadline: Some(deadline),
                });
            }
        }
        Err(VerificationError::Invalid)
    }
    pub(super) async fn verify(
        original: &ProtectedCommand,
        case: AdmissionCase,
        deadline: Instant,
        release: Option<&mut oneshot::Receiver<()>>,
    ) -> Result<Receipt, VerificationError> {
        if let Some(release) = release {
            release
                .await
                .map_err(|cause| VerificationError::Unavailable(Some(cause)))?;
        }
        match case {
            AdmissionCase::Invalid => return Err(VerificationError::Invalid),
            AdmissionCase::Stale => return Err(VerificationError::Stale),
            AdmissionCase::Unavailable => return Err(VerificationError::Unavailable(None)),
            _ => {}
        }
        if original.payload.as_ref() != [2, 7] || original.proof.as_ref() != [41] {
            return Err(VerificationError::Invalid);
        }
        Ok(Receipt {
            identity: None,
            amount: 2,
            scope: match case {
                AdmissionCase::Scope => Scope::DifferentTarget,
                _ => Scope::Counter,
            },
            deadline: match case {
                AdmissionCase::MissingFreshness => None,
                _ => Some(deadline),
            },
        })
    }
}
mod scheduled_provider {
    use super::replay::{Identity, ProtectedRecord};
    use super::{AdmissionCase, Instant, ProtectedCommand, Scope, VerificationError, oneshot};
    pub(super) struct Receipt {
        identity: Option<Identity>,
        amount: u64,
        scope: Scope,
        deadline: Option<Instant>,
    }
    pub(super) fn projection(receipt: &Receipt) -> (u64, Scope, Option<Instant>) {
        (receipt.amount, receipt.scope, receipt.deadline)
    }
    pub(super) fn identity(receipt: &Receipt) -> Option<Identity> {
        receipt.identity
    }
    pub(super) async fn verify_replay(
        original: &ProtectedCommand,
        claimed: Identity,
        configured: &[ProtectedRecord],
        deadline: Instant,
    ) -> Result<Receipt, VerificationError> {
        let (publication, receiving) = oneshot::channel();
        let mut verified = Err(VerificationError::Invalid);
        for (identity, protected, proof) in configured {
            if *identity == claimed
                && protected.as_slice() == original.payload.as_ref()
                && proof.as_slice() == original.proof.as_ref()
            {
                verified = Ok(Receipt {
                    identity: Some(*identity),
                    amount: 2,
                    scope: identity.binding.target,
                    deadline: Some(deadline),
                });
                break;
            }
        }
        let sent = publication.send(verified);
        assert!(sent.is_ok());
        receiving
            .await
            .map_err(|cause| VerificationError::Unavailable(Some(cause)))?
    }
    pub(super) async fn verify(
        original: &ProtectedCommand,
        case: AdmissionCase,
        deadline: Instant,
        release: Option<&mut oneshot::Receiver<()>>,
    ) -> Result<Receipt, VerificationError> {
        if let Some(release) = release {
            release
                .await
                .map_err(|cause| VerificationError::Unavailable(Some(cause)))?;
        }
        let (publication, mut receiving) = oneshot::channel();
        if case == AdmissionCase::Unavailable {
            drop(publication);
        } else {
            let receipt = match case {
                AdmissionCase::Invalid => Err(VerificationError::Invalid),
                AdmissionCase::Stale => Err(VerificationError::Stale),
                _ if original.payload.as_ref() != [2, 7] || original.proof.as_ref() != [41] => {
                    Err(VerificationError::Invalid)
                }
                _ => Ok(Receipt {
                    identity: None,
                    amount: 2,
                    scope: match case {
                        AdmissionCase::Scope => Scope::DifferentTarget,
                        _ => Scope::Arithmetic,
                    },
                    deadline: match case {
                        AdmissionCase::MissingFreshness => None,
                        _ => Some(deadline),
                    },
                }),
            };
            let sent = publication.send(receipt);
            assert!(sent.is_ok());
        }
        (&mut receiving)
            .await
            .map_err(|cause| VerificationError::Unavailable(Some(cause)))?
    }
}
enum Admission<R> {
    AwaitingVerification(ProtectedCommand),
    VerifiedCommand(ProtectedCommand, R),
    Refused(ProtectedCommand, Result<R, VerificationError>, Refusal),
    Accepted,
    PossiblyAdmitted,
}
struct PendingAdmission<R> {
    reservation_bytes: usize,
    admission: Admission<R>,
    publication: Option<oneshot::Sender<()>>,
    notification: oneshot::Receiver<()>,
}
struct AdmissionService<R> {
    permission: Permission,
    expected_target: Scope,
    now: Instant,
    pending: VecDeque<PendingAdmission<R>>,
    count_limit: NonZeroUsize,
    individual_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
    reserved_bytes: usize,
}
impl<R> AdmissionService<R> {
    fn new(
        now: Instant,
        expected_target: Scope,
        count: NonZeroUsize,
        individual: NonZeroUsize,
        bytes: NonZeroUsize,
    ) -> Result<Self, TryReserveError> {
        let mut pending = VecDeque::new();
        pending.try_reserve_exact(count.get())?;
        let reserved_bytes = pending
            .capacity()
            .checked_mul(size_of::<PendingAdmission<R>>())
            .expect("explicit finite header allocation");
        assert!(reserved_bytes <= bytes.get());
        Ok(Self {
            permission: Permission::Granted,
            expected_target,
            now,
            pending,
            count_limit: count,
            individual_limit: individual,
            byte_limit: bytes,
            reserved_bytes,
        })
    }
    fn retention_refusal(&self, original: &ProtectedCommand) -> Option<Refusal> {
        let Some(reservation) = incoming_reservation(original) else {
            return Some(Refusal::ByteAccountingOverflow);
        };
        if self.pending.len() >= self.count_limit.get() {
            return Some(Refusal::Count);
        }
        if reservation > self.individual_limit.get() {
            return Some(Refusal::IndividualBytes);
        }
        let Some(total) = self.reserved_bytes.checked_add(reservation) else {
            return Some(Refusal::ByteAccountingOverflow);
        };
        if total > self.byte_limit.get() {
            return Some(Refusal::TotalBytes);
        }
        None
    }
    fn retain(&mut self, original: ProtectedCommand) -> Result<(), (Refusal, ProtectedCommand)> {
        if let Some(reason) = self.retention_refusal(&original) {
            return Err((reason, original));
        }
        let reservation_bytes =
            incoming_reservation(&original).expect("checked incoming reservation");
        let (publication, notification) = oneshot::channel();
        self.pending.push_back(PendingAdmission {
            reservation_bytes,
            admission: Admission::AwaitingVerification(original),
            publication: Some(publication),
            notification,
        });
        self.reserved_bytes += reservation_bytes;
        Ok(())
    }
    fn consume(&mut self) -> Admission<R> {
        let completed = self
            .pending
            .pop_front()
            .expect("one explicitly consumed disposition");
        self.reserved_bytes -= completed.reservation_bytes;
        completed.admission
    }
}
fn reject<R>(
    evidence: &Result<R, VerificationError>,
    permission: Permission,
    now: Instant,
    expected_target: Scope,
    projection: fn(&R) -> (u64, Scope, Option<Instant>),
) -> Option<Refusal> {
    match evidence {
        Err(VerificationError::Invalid) => Some(Refusal::Invalid),
        Err(VerificationError::Stale) => Some(Refusal::Stale),
        Err(VerificationError::Unavailable(_)) => Some(Refusal::Unavailable),
        Ok(receipt) => {
            let (_, scope, deadline) = projection(receipt);
            if scope != expected_target {
                return Some(Refusal::Scope);
            }
            match (permission, deadline) {
                (Permission::Revoked, _) => Some(Refusal::Revoked),
                (_, None) => Some(Refusal::Unavailable),
                (_, Some(deadline)) if now >= deadline => Some(Refusal::Expired),
                _ => None,
            }
        }
    }
}
fn attempt<R, P>(
    owner: &mut AdmissionService<R>,
    service: &ExternalActor<Replies>,
    target: &EstablishedRecipient<P>,
    projection: fn(&R) -> (u64, Scope, Option<Instant>),
    construct: fn(ProtectedCommand, R) -> P::Msg,
    recover: fn(P::Msg) -> (ProtectedCommand, R),
) -> Option<Refusal>
where
    P: Protocol<Addr = MailAddr>,
{
    let pending = owner.pending.front_mut().expect("the reserved request");
    attempt_pending(
        pending,
        owner.permission,
        owner.now,
        owner.expected_target,
        service,
        target,
        projection,
        construct,
        recover,
    )
}
#[expect(
    clippy::too_many_arguments,
    reason = "queue and replay retention share the same exact owning admission operation"
)]
fn attempt_pending<R, P>(
    pending: &mut PendingAdmission<R>,
    permission: Permission,
    now: Instant,
    expected_target: Scope,
    service: &ExternalActor<Replies>,
    target: &EstablishedRecipient<P>,
    projection: fn(&R) -> (u64, Scope, Option<Instant>),
    construct: fn(ProtectedCommand, R) -> P::Msg,
    recover: fn(P::Msg) -> (ProtectedCommand, R),
) -> Option<Refusal>
where
    P: Protocol<Addr = MailAddr>,
{
    let prior = replace(&mut pending.admission, Admission::PossiblyAdmitted);
    let Admission::VerifiedCommand(original, evidence) = prior else {
        let refusal = match &prior {
            Admission::Refused(_, _, reason) => Some(*reason),
            _ => None,
        };
        pending.admission = prior;
        return refusal;
    };
    let evidence = Ok(evidence);
    let mut observation = None;
    if let Some(reason) = reject(&evidence, permission, now, expected_target, projection) {
        pending.admission = Admission::Refused(original, evidence, reason);
        observation = Some(reason);
    } else {
        let Ok(receipt) = evidence else {
            unreachable!("ready receipt")
        };
        let command = construct(original, receipt);
        let admitted = service.try_send(target, command);
        pending.admission = match admitted {
            Ok(()) => Admission::Accepted,
            Err(TrySendError::Full(command)) => {
                let (original, receipt) = recover(command);
                observation = Some(Refusal::Full);
                Admission::VerifiedCommand(original, receipt)
            }
            Err(TrySendError::Closed(command)) => {
                let (original, receipt) = recover(command);
                observation = Some(Refusal::Closed);
                Admission::Refused(original, Ok(receipt), Refusal::Closed)
            }
        };
    }
    match pending.admission {
        Admission::Accepted | Admission::Refused(..) => {
            let sent = pending
                .publication
                .take()
                .expect("one completion publication")
                .send(());
            assert!(sent.is_ok());
        }
        _ => {}
    }
    observation
}
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    prefix: usize,
    protected: usize,
    value: u64,
}
type Replies = MessageProtocol<MailAddr, Snapshot>;
enum CounterCommand {
    Prefix(usize),
    Protected(ProtectedCommand, record_provider::Receipt),
    Snapshot(EstablishedRecipient<Replies>),
}
#[derive(Default)]
struct Counter {
    prefix: Vec<(MailAddr, usize)>,
    protected: Vec<(MailAddr, ProtectedCommand, record_provider::Receipt)>,
    value: u64,
}
#[bombay::actor(sends = { replies: Vec<EstablishedDelivery<Replies>>, })]
impl Counter {
    fn receive(&mut self, from: MailAddr, command: CounterCommand) -> BehaviorActed<Self> {
        match command {
            CounterCommand::Prefix(sequence) => {
                self.prefix.push((from, sequence));
                Ok(Actions::cont())
            }
            CounterCommand::Protected(original, receipt) => {
                self.value += record_provider::projection(&receipt).0;
                self.protected.push((from, original, receipt));
                Ok(Actions::cont())
            }
            CounterCommand::Snapshot(reply_to) => Ok(CounterActions::send_replies(
                Actions::cont(),
                EstablishedDelivery::new(
                    reply_to,
                    Snapshot {
                        prefix: self.prefix.len(),
                        protected: self.protected.len(),
                        value: self.value,
                    },
                ),
            )),
        }
    }
}
enum ArithmeticCommand {
    Prefix(usize),
    Protected(ProtectedCommand, scheduled_provider::Receipt),
    Snapshot(EstablishedRecipient<Replies>),
}
struct Arithmetic {
    prefix: Vec<(MailAddr, usize)>,
    protected: Vec<(MailAddr, ProtectedCommand, scheduled_provider::Receipt)>,
    value: u64,
}
#[bombay::actor(sends = { replies: Vec<EstablishedDelivery<Replies>>, })]
impl Arithmetic {
    fn receive(&mut self, from: MailAddr, command: ArithmeticCommand) -> BehaviorActed<Self> {
        match command {
            ArithmeticCommand::Prefix(sequence) => {
                self.prefix.push((from, sequence));
                Ok(Actions::cont())
            }
            ArithmeticCommand::Protected(original, receipt) => {
                self.value *= scheduled_provider::projection(&receipt).0;
                self.protected.push((from, original, receipt));
                Ok(Actions::cont())
            }
            ArithmeticCommand::Snapshot(reply_to) => Ok(ArithmeticActions::send_replies(
                Actions::cont(),
                EstablishedDelivery::new(
                    reply_to,
                    Snapshot {
                        prefix: self.prefix.len(),
                        protected: self.protected.len(),
                        value: self.value,
                    },
                ),
            )),
        }
    }
}
fn counter_recovery(command: CounterCommand) -> (ProtectedCommand, record_provider::Receipt) {
    let CounterCommand::Protected(original, receipt) = command else {
        panic!("exact typed protected command")
    };
    (original, receipt)
}
fn arithmetic_recovery(
    command: ArithmeticCommand,
) -> (ProtectedCommand, scheduled_provider::Receipt) {
    let ArithmeticCommand::Protected(original, receipt) = command else {
        panic!("exact typed protected command")
    };
    (original, receipt)
}
fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("explicit nonzero fixture limit")
}
fn limits<R>() -> (NonZeroUsize, NonZeroUsize, NonZeroUsize) {
    let footprint = INCOMING_RESERVATION + 3;
    (
        nonzero(2),
        nonzero(footprint),
        nonzero(2 * size_of::<PendingAdmission<R>>() + 2 * footprint),
    )
}
#[expect(
    clippy::too_many_arguments,
    reason = "ordinary concrete provider and typed message constructors remain explicit"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one trace retains pending originals, provider results and processing barriers"
)]
async fn exercise<R, P, V>(
    case: AdmissionCase,
    timing: VerificationTiming,
    expected_target: Scope,
    service: &mut ExternalActor<Replies>,
    target: &EstablishedRecipient<P>,
    verify: V,
    projection: fn(&R) -> (u64, Scope, Option<Instant>),
    prefix: fn(usize) -> P::Msg,
    construct: fn(ProtectedCommand, R) -> P::Msg,
    recover: fn(P::Msg) -> (ProtectedCommand, R),
    snapshot: fn(EstablishedRecipient<Replies>) -> P::Msg,
) -> (
    (
        Admission<R>,
        Snapshot,
        (*const u8, *const u8),
        Option<Refusal>,
        Option<Refusal>,
    ),
    Option<AuthorityProgress>,
)
where
    P: Protocol<Addr = MailAddr>,
    P::Msg: Send + 'static,
    V: AsyncFn(
        &ProtectedCommand,
        AdmissionCase,
        Instant,
        Option<&mut oneshot::Receiver<()>>,
    ) -> Result<R, VerificationError>,
{
    let now = Instant::now();
    let deadline = now
        .checked_add(Duration::from_secs(1))
        .expect("finite fixture deadline");
    let (count, individual, bytes) = limits::<R>();
    let mut owner = AdmissionService::new(now, expected_target, count, individual, bytes)
        .expect("finite pending storage reservation");
    let request = protected_command();
    let allocation = (request.payload.as_ptr(), request.proof.as_ptr());
    let retained = owner.retain(request);
    assert!(retained.is_ok());
    let (evidence, progress) = {
        let (permission, now, pending) = (&mut owner.permission, &mut owner.now, &owner.pending);
        let Admission::AwaitingVerification(original) = &pending
            .front()
            .expect("reserved before verification")
            .admission
        else {
            panic!("unverified original")
        };
        match timing {
            VerificationTiming::Immediate => (verify(original, case, deadline, None).await, None),
            VerificationTiming::Stalled => {
                let (release, mut held) = oneshot::channel();
                let (publication, mut updates) = oneshot::channel();
                let update = match case {
                    AdmissionCase::Revoked => AuthorityUpdate::Permission(Permission::Revoked),
                    AdmissionCase::Expired => AuthorityUpdate::Time(deadline),
                    _ => AuthorityUpdate::Permission(Permission::Granted),
                };
                let (acknowledgement, mut acknowledged) = oneshot::channel();
                let mut acknowledgement = Some(acknowledgement);
                let mut verifier = pin!(verify(original, case, deadline, Some(&mut held)));
                let mut context = Context::from_waker(Waker::noop());
                let (verification, mut acquired) = match verifier.as_mut().poll(&mut context) {
                    Poll::Pending => (Poll::Pending, None),
                    Poll::Ready(evidence) => (Poll::Ready(()), Some(evidence)),
                };
                let queued = publication.send(update);
                assert!(queued.is_ok());
                if acquired.is_none() {
                    let mut progress = pin!(async {
                        tokio::select! {
                            evidence = &mut verifier => Some(evidence),
                            update = &mut updates => {
                                match update.expect("independently queued authority update") {
                                    AuthorityUpdate::Permission(value) => *permission = value,
                                    AuthorityUpdate::Time(value) => *now = value,
                                }
                                let sent = acknowledgement.take()
                                    .expect("one authority acknowledgement").send(());
                                assert!(sent.is_ok());
                                None
                            }
                        }
                    });
                    if let Poll::Ready(evidence) = progress.as_mut().poll(&mut context) {
                        acquired = evidence;
                    }
                }
                drop(acknowledgement);
                let acknowledged = pin!(&mut acknowledged).as_mut().poll(&mut context);
                let observed_permission = *permission;
                let observed_time = *now;
                let released = release.send(());
                let evidence = match acquired {
                    Some(evidence) => evidence,
                    None => verifier.await,
                };
                (
                    evidence,
                    Some(AuthorityProgress {
                        verification,
                        acknowledgement: acknowledged,
                        permission: observed_permission,
                        now: observed_time,
                        deadline,
                        released,
                    }),
                )
            }
        }
    };
    let pending = owner.pending.front_mut().expect("same reservation");
    let Admission::AwaitingVerification(original) =
        replace(&mut pending.admission, Admission::PossiblyAdmitted)
    else {
        panic!("original before result")
    };
    pending.admission = match evidence {
        Ok(receipt) => Admission::VerifiedCommand(original, receipt),
        Err(verification_error) => {
            let evidence = Err(verification_error);
            let reason = reject(
                &evidence,
                owner.permission,
                owner.now,
                owner.expected_target,
                projection,
            )
            .expect("provider refusal");
            Admission::Refused(original, evidence, reason)
        }
    };
    for sequence in 0..1_024 {
        let sent = service.try_send(target, prefix(sequence));
        match sent {
            Ok(()) => {}
            Err(refused_prefix) => {
                drop(refused_prefix);
                panic!("actual native prefix fits");
            }
        }
    }
    let first_attempt = attempt(&mut owner, service, target, projection, construct, recover);
    let reservation = owner.reserved_bytes;
    if matches!(timing, VerificationTiming::Immediate)
        && matches!(
            case,
            AdmissionCase::Granted | AdmissionCase::Revoked | AdmissionCase::Expired
        )
        || matches!(case, AdmissionCase::Granted)
    {
        let Admission::VerifiedCommand(original, _) = &owner
            .pending
            .front()
            .expect("retained native Full")
            .admission
        else {
            panic!("Full original")
        };
        assert_eq!(original.payload.as_ptr(), allocation.0);
        assert_eq!(original.proof.as_ptr(), allocation.1);
        let mut notification = Box::pin(
            &mut owner
                .pending
                .front_mut()
                .expect("pending Full")
                .notification,
        );
        let mut context = Context::from_waker(Waker::noop());
        let observation = notification.as_mut().poll(&mut context);
        assert!(matches!(observation, Poll::Pending));
        drop(notification);
        if matches!(timing, VerificationTiming::Immediate) {
            match case {
                AdmissionCase::Revoked => owner.permission = Permission::Revoked,
                AdmissionCase::Expired => owner.now = deadline,
                _ => {}
            }
        }
    }
    let snapshot_sent = service.send(target, snapshot(service.recipient())).await;
    assert!(snapshot_sent.is_ok());
    let before = service
        .receive()
        .await
        .expect("same-user-lane prefix barrier");
    assert_eq!(before.message.prefix, 1_024);
    assert_eq!(before.message.protected, 0);
    let final_attempt = attempt(&mut owner, service, target, projection, construct, recover);
    assert_eq!(
        owner.reserved_bytes, reservation,
        "completed but unreceived retains its reservation"
    );
    let slot = owner
        .pending
        .front_mut()
        .expect("complete disposition retained");
    // Provider failures were already complete before the first native attempt.
    if let Some(publication) = slot.publication.take() {
        let sent = publication.send(());
        assert!(sent.is_ok());
    }
    let notified = (&mut slot.notification).await;
    assert!(notified.is_ok());
    let sent = service.send(target, snapshot(service.recipient())).await;
    assert!(sent.is_ok());
    let after = service
        .receive()
        .await
        .expect("complete processing barrier");
    let disposition = owner.consume();
    assert!(owner.pending.is_empty());
    assert_eq!(
        owner.reserved_bytes,
        owner.pending.capacity() * size_of::<PendingAdmission<R>>()
    );
    (
        (
            disposition,
            after.message,
            allocation,
            first_attempt,
            final_attempt,
        ),
        progress,
    )
}
fn refusal(case: AdmissionCase) -> Option<Refusal> {
    match case {
        AdmissionCase::Granted => None,
        AdmissionCase::Revoked => Some(Refusal::Revoked),
        AdmissionCase::Expired => Some(Refusal::Expired),
        AdmissionCase::Invalid => Some(Refusal::Invalid),
        AdmissionCase::Stale => Some(Refusal::Stale),
        AdmissionCase::Unavailable | AdmissionCase::MissingFreshness => Some(Refusal::Unavailable),
        AdmissionCase::Scope => Some(Refusal::Scope),
    }
}
fn assert_disposition<R>(
    case: AdmissionCase,
    admission: Admission<R>,
    allocation: (*const u8, *const u8),
) {
    match (refusal(case), admission) {
        (None, Admission::Accepted) => {}
        (Some(expected), Admission::Refused(original, evidence, actual)) => {
            assert_eq!(actual, expected);
            assert_eq!(original.payload.as_ptr(), allocation.0);
            assert_eq!(original.proof.as_ptr(), allocation.1);
            assert_eq!(original.payload.as_ref(), [2, 7]);
            assert_eq!(original.proof.as_ref(), [41]);
            match (case, evidence) {
                (AdmissionCase::Invalid, Err(VerificationError::Invalid))
                | (AdmissionCase::Stale, Err(VerificationError::Stale))
                | (AdmissionCase::Unavailable, Err(VerificationError::Unavailable(None)))
                | (
                    AdmissionCase::Revoked
                    | AdmissionCase::Expired
                    | AdmissionCase::Scope
                    | AdmissionCase::MissingFreshness,
                    Ok(_),
                ) => {}
                (
                    AdmissionCase::Unavailable,
                    Err(VerificationError::Unavailable(Some(receiving_error))),
                ) => {
                    assert_eq!(receiving_error.to_string(), "channel closed");
                }
                _ => panic!("original provider evidence and refusal preserved"),
            }
        }
        _ => panic!("exact expected disposition"),
    }
}
const CASES: [AdmissionCase; 8] = [
    AdmissionCase::Granted,
    AdmissionCase::Revoked,
    AdmissionCase::Expired,
    AdmissionCase::Invalid,
    AdmissionCase::Stale,
    AdmissionCase::Unavailable,
    AdmissionCase::Scope,
    AdmissionCase::MissingFreshness,
];
fn admission_cases() -> impl Iterator<Item = (AdmissionCase, VerificationTiming)> {
    CASES
        .into_iter()
        .map(|case| (case, VerificationTiming::Immediate))
        .chain(
            [
                AdmissionCase::Granted,
                AdmissionCase::Revoked,
                AdmissionCase::Expired,
            ]
            .map(|case| (case, VerificationTiming::Stalled)),
        )
}
fn assert_authority_progress(
    case: AdmissionCase,
    timing: VerificationTiming,
    progress: Option<AuthorityProgress>,
) {
    match (timing, progress) {
        (VerificationTiming::Immediate, None) => {}
        (VerificationTiming::Stalled, Some(progress)) => {
            assert!(
                matches!(progress.verification, Poll::Pending),
                "provider actually stalled"
            );
            assert!(
                matches!(progress.acknowledgement, Poll::Ready(Ok(()))),
                "authority progresses before provider release"
            );
            assert_eq!(progress.released, Ok(()));
            assert_eq!(
                progress.permission,
                match case {
                    AdmissionCase::Revoked => Permission::Revoked,
                    _ => Permission::Granted,
                }
            );
            match case {
                AdmissionCase::Expired => assert_eq!(progress.now, progress.deadline),
                _ => assert!(progress.now < progress.deadline),
            }
        }
        _ => panic!("exact authority-progress observation"),
    }
}
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the concrete Application retains Work, native state and both notification receipts"
)]
fn record_provider_consumer_uses_exclusive_admission() {
    for (case, timing) in admission_cases() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("configured host");
        let received = host
            .block_on(
                Application::new(Counter::default().stop_on_shutdown()).run_with::<RootTerminal<
                    StopOnShutdown<Counter>,
                >, _, _, _, _, _>(
                    |application| async move {
                        let mut service = application
                            .interface(())
                            .external::<Replies>()
                            .expect("typed service");
                        let target = application.root().established_recipient();
                        let result = exercise(
                            case,
                            timing,
                            Scope::Counter,
                            &mut service,
                            &target,
                            record_provider::verify,
                            record_provider::projection,
                            CounterCommand::Prefix,
                            CounterCommand::Protected,
                            counter_recovery,
                            CounterCommand::Snapshot,
                        )
                        .await;
                        service.close_admission();
                        let drained = service.receive().await;
                        assert!(drained.is_none());
                        let stopped = application.lifecycle().request_shutdown();
                        assert_eq!(stopped, Ok(()));
                        (result, service.address())
                    },
                ),
            )
            .unwrap_or_else(|original| {
                drop(original);
                panic!("successful Application startup")
            });
        drop(host);
        let (
            ApplicationOutcome::Completed {
                output: (observations, sender),
                cleanup: Ok(()),
            },
            Ok((origin, terminal)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = received
        else {
            panic!("all actual native and notification receipts")
        };
        let ((disposition, snapshot, allocation, first_attempt, final_attempt), progress) =
            observations;
        let ActorRetirement::Completed { behavior, .. } = &terminal else {
            panic!("native completed Counter")
        };
        let expected = usize::from(matches!(case, AdmissionCase::Granted));
        assert_eq!(
            behavior.base().protected.len(),
            expected,
            "fresh retry refuses protected admission"
        );
        assert_eq!(
            snapshot,
            Snapshot {
                prefix: 1_024,
                protected: expected,
                value: 2 * expected as u64
            }
        );
        assert_eq!(behavior.base().value, snapshot.value);
        for (sequence, &(from, observed)) in behavior.base().prefix.iter().enumerate() {
            assert_eq!((from, observed), (sender, sequence));
        }
        assert_eq!(behavior.base().prefix.len(), 1_024);
        for (from, original, receipt) in &behavior.base().protected {
            assert_eq!(*from, sender);
            assert_eq!(original.payload.as_ptr(), allocation.0);
            assert_eq!(original.proof.as_ptr(), allocation.1);
            assert_eq!(original.payload.as_ref(), [2, 7]);
            assert_eq!(original.proof.as_ref(), [41]);
            assert_eq!(record_provider::projection(receipt).0, 2);
        }
        assert_authority_progress(case, timing, progress);
        assert_eq!(final_attempt, refusal(case));
        if matches!(
            case,
            AdmissionCase::Granted | AdmissionCase::Revoked | AdmissionCase::Expired
        ) {
            assert_eq!(
                first_attempt,
                match timing {
                    VerificationTiming::Immediate => Some(Refusal::Full),
                    VerificationTiming::Stalled => refusal(case).or(Some(Refusal::Full)),
                }
            );
        }
        assert_disposition(case, disposition, allocation);
        assert_completed(RootTerminal::Root { origin, terminal }, None);
    }
}
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the concrete Application retains Work, native state and both notification receipts"
)]
fn scheduled_provider_consumer_uses_exclusive_admission() {
    for (case, timing) in admission_cases() {
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("configured host");
        let received = host
            .block_on(
                Application::new(
                    Arithmetic {
                        prefix: Vec::new(),
                        protected: Vec::new(),
                        value: 3,
                    }
                    .stop_on_shutdown(),
                )
                .run_with::<RootTerminal<StopOnShutdown<Arithmetic>>, _, _, _, _, _>(
                    |application| async move {
                        let mut service = application
                            .interface(())
                            .external::<Replies>()
                            .expect("typed service");
                        let target = application.root().established_recipient();
                        let result = exercise(
                            case,
                            timing,
                            Scope::Arithmetic,
                            &mut service,
                            &target,
                            scheduled_provider::verify,
                            scheduled_provider::projection,
                            ArithmeticCommand::Prefix,
                            ArithmeticCommand::Protected,
                            arithmetic_recovery,
                            ArithmeticCommand::Snapshot,
                        )
                        .await;
                        service.close_admission();
                        let drained = service.receive().await;
                        assert!(drained.is_none());
                        let stopped = application.lifecycle().request_shutdown();
                        assert_eq!(stopped, Ok(()));
                        (result, service.address())
                    },
                ),
            )
            .unwrap_or_else(|original| {
                drop(original);
                panic!("successful Application startup")
            });
        drop(host);
        let (
            ApplicationOutcome::Completed {
                output: (observations, sender),
                cleanup: Ok(()),
            },
            Ok((origin, terminal)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = received
        else {
            panic!("all actual native and notification receipts")
        };
        let ((disposition, snapshot, allocation, first_attempt, final_attempt), progress) =
            observations;
        let ActorRetirement::Completed { behavior, .. } = &terminal else {
            panic!("native completed Arithmetic")
        };
        let expected = usize::from(matches!(case, AdmissionCase::Granted));
        assert_eq!(
            behavior.base().protected.len(),
            expected,
            "fresh retry refuses protected admission"
        );
        assert_eq!(
            snapshot,
            Snapshot {
                prefix: 1_024,
                protected: expected,
                value: match case {
                    AdmissionCase::Granted => 6,
                    _ => 3,
                }
            }
        );
        assert_eq!(behavior.base().value, snapshot.value);
        for (sequence, &(from, observed)) in behavior.base().prefix.iter().enumerate() {
            assert_eq!((from, observed), (sender, sequence));
        }
        assert_eq!(behavior.base().prefix.len(), 1_024);
        for (from, original, receipt) in &behavior.base().protected {
            assert_eq!(*from, sender);
            assert_eq!(original.payload.as_ptr(), allocation.0);
            assert_eq!(original.proof.as_ptr(), allocation.1);
            assert_eq!(original.payload.as_ref(), [2, 7]);
            assert_eq!(original.proof.as_ref(), [41]);
            assert_eq!(scheduled_provider::projection(receipt).0, 2);
        }
        assert_authority_progress(case, timing, progress);
        assert_eq!(final_attempt, refusal(case));
        if matches!(
            case,
            AdmissionCase::Granted | AdmissionCase::Revoked | AdmissionCase::Expired
        ) {
            assert_eq!(
                first_attempt,
                match timing {
                    VerificationTiming::Immediate => Some(Refusal::Full),
                    VerificationTiming::Stalled => refusal(case).or(Some(Refusal::Full)),
                }
            );
        }
        assert_disposition(case, disposition, allocation);
        assert_completed(RootTerminal::Root { origin, terminal }, None);
    }
}
fn bounded_originals<R>(expected_target: Scope) {
    let now = Instant::now();
    let (count, individual, bytes) = limits::<R>();
    let mut owner = AdmissionService::<R>::new(now, expected_target, count, individual, bytes)
        .expect("finite pending storage reservation");
    for _ in 0..2 {
        let retained = owner.retain(protected_command());
        assert!(retained.is_ok());
    }
    let overflow = protected_command();
    let allocation = (overflow.payload.as_ptr(), overflow.proof.as_ptr());
    let refused = owner.retain(overflow);
    let Err((Refusal::Count, overflow)) = refused else {
        panic!("count capacity plus one")
    };
    assert_eq!(overflow.payload.as_ptr(), allocation.0);
    assert_eq!(overflow.proof.as_ptr(), allocation.1);
    let exact_reservation = owner.reserved_bytes;
    let entry = owner.pending.front_mut().expect("reserved original");
    let Admission::AwaitingVerification(retained_original) =
        replace(&mut entry.admission, Admission::PossiblyAdmitted)
    else {
        panic!("one original")
    };
    entry.admission = Admission::Refused(
        retained_original,
        Err(VerificationError::Invalid),
        Refusal::Invalid,
    );
    let sent = entry.publication.take().expect("publication").send(());
    assert!(sent.is_ok());
    assert_eq!(
        owner.reserved_bytes, exact_reservation,
        "completed unreceived refusal still occupies capacity"
    );
    let extra = protected_command();
    let allocation = (extra.payload.as_ptr(), extra.proof.as_ptr());
    let refused = owner.retain(extra);
    let Err((Refusal::Count, extra)) = refused else {
        panic!("completed unreceived count overflow")
    };
    assert_eq!(extra.payload.as_ptr(), allocation.0);
    assert_eq!(extra.proof.as_ptr(), allocation.1);
    let completed = owner.consume();
    assert!(matches!(completed, Admission::Refused(..)));
    let footprint = individual.get();
    let mut owner = AdmissionService::<R>::new(
        now,
        expected_target,
        count,
        individual,
        nonzero(2 * size_of::<PendingAdmission<R>>() + footprint),
    )
    .expect("finite pending storage reservation");
    let retained = owner.retain(protected_command());
    assert!(retained.is_ok());
    let extra = protected_command();
    let allocation = (extra.payload.as_ptr(), extra.proof.as_ptr());
    let refused = owner.retain(extra);
    let Err((Refusal::TotalBytes, extra)) = refused else {
        panic!("retained byte capacity plus one")
    };
    assert_eq!(extra.payload.as_ptr(), allocation.0);
    assert_eq!(extra.proof.as_ptr(), allocation.1);
    let mut owner =
        AdmissionService::<R>::new(now, expected_target, count, nonzero(footprint - 1), bytes)
            .expect("finite pending storage reservation");
    let extra = protected_command();
    let allocation = (extra.payload.as_ptr(), extra.proof.as_ptr());
    let refused = owner.retain(extra);
    let Err((Refusal::IndividualBytes, extra)) = refused else {
        panic!("individual byte capacity plus one")
    };
    assert_eq!(extra.payload.as_ptr(), allocation.0);
    assert_eq!(extra.proof.as_ptr(), allocation.1);
}
#[test]
fn pending_original_limits_include_completed_unreceived_dispositions() {
    bounded_originals::<record_provider::Receipt>(Scope::Counter);
    bounded_originals::<scheduled_provider::Receipt>(Scope::Arithmetic);
}

mod replay {
    use super::{
        Admission, AdmissionCase, AdmissionService, Arithmetic, ArithmeticCommand, Builder,
        Counter, CounterCommand, Instant, PendingAdmission, ProtectedCommand, Refusal, Replies,
        RootTerminal, Scope, Snapshot, VerificationError, allocated_storage, arithmetic_recovery,
        assert_completed, attempt_pending, counter_recovery, nonzero, record_provider,
        scheduled_provider, size_of_val,
    };
    use super::{
        CASES, INCOMING_RESERVATION, LOCAL_STORAGE_CEILING, Permission, protected_command, reject,
    };
    use bombay::behavior::{BehaviorBase, Protocol};
    use bombay::prelude::*;
    use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
    use core::ops::AsyncFn;
    use std::collections::TryReserveError;
    use std::env::{self, VarError};
    use std::future::{Future, poll_fn};
    use std::mem::{replace, size_of};
    use std::pin::pin;
    use std::process::Command;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    use tokio::{runtime::Runtime, sync::oneshot};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) struct Binding {
        deployment: u64,
        caller: u64,
        runtime: u64,
        actor: u64,
        pub(super) target: Scope,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) struct Identity {
        pub(super) binding: Binding,
        sequence: u64,
    }
    pub(super) type ProtectedRecord = (Identity, [u8; 2], [u8; 1]);
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ReplayRefusal {
        Unbound,
        Binding,
        Conflict,
        Capacity,
        Reservation(Refusal),
        Exhausted,
    }
    struct Retained<R> {
        protected: Box<[u8]>,
        pending: PendingAdmission<R>,
    }
    struct Replay<R> {
        binding: Binding,
        admissions: AdmissionService<R>,
        entries: Vec<(u64, Retained<R>)>,
    }
    impl<R> Replay<R> {
        fn new(
            binding: Binding,
            mut admissions: AdmissionService<R>,
        ) -> Result<Self, TryReserveError> {
            let mut entries = Vec::new();
            entries.try_reserve_exact(admissions.count_limit.get())?;
            admissions.reserved_bytes = entries
                .capacity()
                .checked_mul(size_of::<(u64, Retained<R>)>())
                .and_then(|bytes| admissions.reserved_bytes.checked_add(bytes))
                .expect("explicit finite pending and live-ledger storage reservation");
            assert!(admissions.reserved_bytes <= admissions.byte_limit.get());
            Ok(Self {
                binding,
                admissions,
                entries,
            })
        }
    }
    fn issue(
        next: &mut Option<u64>,
        binding: Binding,
        original: ProtectedCommand,
    ) -> Result<(Identity, ProtectedCommand), (ReplayRefusal, ProtectedCommand)> {
        let Some(sequence) = *next else {
            return Err((ReplayRefusal::Exhausted, original));
        };
        *next = sequence.checked_add(1);
        Ok((Identity { binding, sequence }, original))
    }
    #[expect(
        clippy::type_complexity,
        reason = "new refusals and duplicate originals preserve distinct concrete provider receipts"
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "the shared native operation receives concrete explicit constructors"
    )]
    fn submit<R, P>(
        owner: &mut Replay<R>,
        service: &ExternalActor<Replies>,
        target: &EstablishedRecipient<P>,
        original: ProtectedCommand,
        receipt: R,
        identity: fn(&R) -> Option<Identity>,
        projection: fn(&R) -> (u64, Scope, Option<Instant>),
        construct: fn(ProtectedCommand, R) -> P::Msg,
        recover: fn(P::Msg) -> (ProtectedCommand, R),
    ) -> Result<
        (Option<Refusal>, Option<(ProtectedCommand, R)>),
        (ReplayRefusal, ProtectedCommand, R),
    >
    where
        P: Protocol<Addr = MailAddr>,
    {
        let Some(confirmed) = identity(&receipt) else {
            return Err((ReplayRefusal::Unbound, original, receipt));
        };
        if confirmed.binding != owner.binding {
            return Err((ReplayRefusal::Binding, original, receipt));
        }
        let position = owner
            .entries
            .binary_search_by_key(&confirmed.sequence, |(sequence, _)| *sequence);
        if let Ok(position) = position {
            let retained = &mut owner.entries[position].1;
            if retained.protected.as_ref() != original.payload.as_ref() {
                return Err((ReplayRefusal::Conflict, original, receipt));
            }
            let attempted = attempt_pending(
                &mut retained.pending,
                owner.admissions.permission,
                owner.admissions.now,
                owner.binding.target,
                service,
                target,
                projection,
                construct,
                recover,
            );
            return Ok((attempted, Some((original, receipt))));
        }
        if owner.entries.len() >= owner.admissions.count_limit.get() {
            return Err((ReplayRefusal::Capacity, original, receipt));
        }
        let comparison_charge = original.payload.len();
        let Some(total_reservation) = owner
            .admissions
            .reserved_bytes
            .checked_add(comparison_charge)
        else {
            return Err((
                ReplayRefusal::Reservation(Refusal::ByteAccountingOverflow),
                original,
                receipt,
            ));
        };
        if total_reservation > owner.admissions.byte_limit.get() {
            return Err((
                ReplayRefusal::Reservation(Refusal::TotalBytes),
                original,
                receipt,
            ));
        }
        let protected = Box::from(original.payload.as_ref());
        owner.admissions.reserved_bytes += comparison_charge;
        let reserved = owner.admissions.retain(original);
        if let Err((reason, original)) = reserved {
            owner.admissions.reserved_bytes -= comparison_charge;
            return Err((ReplayRefusal::Reservation(reason), original, receipt));
        }
        let mut pending = owner
            .admissions
            .pending
            .pop_front()
            .expect("actual reservation transfers to replay custody");
        let Admission::AwaitingVerification(original) =
            replace(&mut pending.admission, Admission::PossiblyAdmitted)
        else {
            panic!("provider result joins the retained original");
        };
        pending.admission = Admission::VerifiedCommand(original, receipt);
        let position = position.expect_err("confirmed new identity");
        owner.entries.insert(
            position,
            (confirmed.sequence, Retained { protected, pending }),
        );
        let retained = &mut owner.entries[position].1;
        let attempted = attempt_pending(
            &mut retained.pending,
            owner.admissions.permission,
            owner.admissions.now,
            owner.binding.target,
            service,
            target,
            projection,
            construct,
            recover,
        );
        Ok((attempted, None))
    }
    #[derive(Clone, Copy, Debug)]
    enum Case {
        Live,
        Replacement,
        Exhaustion,
    }
    #[expect(
        clippy::type_complexity,
        reason = "every actual submission keeps its nonclone originals until native cleanup"
    )]
    struct Observations<R> {
        owner: Replay<R>,
        submissions: Vec<
            Result<
                (Option<Refusal>, Option<(ProtectedCommand, R)>),
                (ReplayRefusal, ProtectedCommand, R),
            >,
        >,
        snapshots: Vec<Snapshot>,
        allocations: Vec<(*const u8, *const u8)>,
        rejected_allocations: Vec<(*const u8, *const u8)>,
        exhaustion: Option<(ReplayRefusal, ProtectedCommand)>,
        pending: Poll<()>,
        senders: Vec<MailAddr>,
        closed: Option<Result<(), TrySendError<(ProtectedCommand, R)>>>,
    }
    fn binding(target: Scope, runtime: u64) -> Binding {
        Binding {
            deployment: 11,
            caller: 13,
            runtime,
            actor: 17,
            target,
        }
    }
    fn request(proof: u8, changed: u8) -> ProtectedCommand {
        ProtectedCommand {
            payload: vec![2, changed].into_boxed_slice(),
            proof: vec![proof].into_boxed_slice(),
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum SequenceOrder {
        Increasing,
        Decreasing,
    }
    fn protected_records(binding: Binding) -> [ProtectedRecord; 2] {
        [
            (
                Identity {
                    binding,
                    sequence: 0,
                },
                [2, 7],
                [41],
            ),
            (
                Identity {
                    binding,
                    sequence: 1,
                },
                [2, 7],
                [42],
            ),
        ]
    }
    fn retained_storage<R>(
        scope: Scope,
        mut originals: [ProtectedCommand; 2],
        mut receipts: [R; 2],
        order: SequenceOrder,
        identity: fn(&R) -> Option<Identity>,
    ) -> (Replay<R>, u64) {
        match order {
            SequenceOrder::Increasing => {}
            SequenceOrder::Decreasing => {
                originals.reverse();
                receipts.reverse();
            }
        }
        let now = Instant::now();
        let footprint = size_of::<PendingAdmission<R>>();
        let (mut owner, slots) = allocated_storage(|| {
            let admissions = AdmissionService::new(
                now,
                scope,
                nonzero(2),
                nonzero(INCOMING_RESERVATION + 16),
                nonzero(LOCAL_STORAGE_CEILING),
            )
            .expect("finite pending storage reservation");
            Replay::new(binding(scope, 19), admissions)
                .expect("finite live-binding storage reservation")
        });
        let entry_capacity = owner.entries.capacity();
        println!(
            "empty storage {scope:?}/{order:?}: allocation {slots:?}; pending capacity {}; ledger capacity {}; pending inline {}; ledger inline {}",
            owner.admissions.pending.capacity(),
            entry_capacity,
            footprint,
            size_of::<(u64, Retained<R>)>()
        );
        let mut notifications = (0, 0);
        for original in originals {
            let (reserved, storage) = allocated_storage(|| owner.admissions.retain(original));
            assert!(reserved.is_ok(), "existing finite reservation");
            notifications.0 += storage.0;
            notifications.1 += storage.1;
            println!(
                "pending prefix {}: cell allocation {storage:?}",
                owner.admissions.pending.len()
            );
        }
        let mut comparisons = (0, 0);
        for receipt in receipts {
            let ((), storage) = allocated_storage(|| {
                let confirmed = identity(&receipt).expect("actual provider-owned identity");
                let PendingAdmission {
                    reservation_bytes,
                    admission,
                    publication,
                    notification,
                } = owner
                    .admissions
                    .pending
                    .pop_front()
                    .expect("retained original");
                let Admission::AwaitingVerification(original) = admission else {
                    panic!("only measurement custody moves into the same owning product");
                };
                let protected: Box<[u8]> = Box::from(original.payload.as_ref());
                owner.admissions.reserved_bytes += protected.len();
                let pending = PendingAdmission {
                    reservation_bytes,
                    admission: Admission::VerifiedCommand(original, receipt),
                    publication,
                    notification,
                };
                let position = owner
                    .entries
                    .binary_search_by_key(&confirmed.sequence, |(sequence, _)| *sequence)
                    .expect_err("two independently verified identities");
                owner.entries.insert(
                    position,
                    (confirmed.sequence, Retained { protected, pending }),
                );
            });
            comparisons.0 += storage.0;
            comparisons.1 += storage.1;
            assert_eq!(
                owner.entries.capacity(),
                entry_capacity,
                "finite insertion never reallocates slots"
            );
            assert!(
                owner
                    .entries
                    .windows(2)
                    .all(|entries| entries[0].0 < entries[1].0)
            );
            println!(
                "ledger prefix {}: comparison allocation {storage:?}",
                owner.entries.len()
            );
        }
        println!(
            "retained storage {scope:?}/{order:?}: slot allocation {slots:?}; notification allocation \
             {notifications:?}; comparison allocation {comparisons:?}; conservative reservation {}",
            owner.admissions.reserved_bytes
        );
        (owner, slots.1 + notifications.1 + comparisons.1)
    }

    #[derive(Clone, Copy, Debug)]
    enum VerificationWait {
        Released,
        Cancelled,
    }
    #[expect(
        clippy::type_complexity,
        reason = "complete nonclone provider outcomes and borrowed-wait outcomes retain their actual errors"
    )]
    fn provider_storage<R>(
        host: &Runtime,
        scope: Scope,
        original: &ProtectedCommand,
        deadline: Instant,
        verify: &impl AsyncFn(
            &ProtectedCommand,
            AdmissionCase,
            Instant,
            Option<&mut oneshot::Receiver<()>>,
        ) -> Result<R, VerificationError>,
    ) -> (
        [Result<R, VerificationError>; 8],
        [Result<R, VerificationError>; 2],
    ) {
        let evidence = CASES.map(|case| {
            let (result, storage) =
                allocated_storage(|| host.block_on(verify(original, case, deadline, None)));
            println!(
                "provider outcome {scope:?}/{case:?}: allocation {storage:?}; result inline {}",
                size_of::<Result<R, VerificationError>>()
            );
            if let Err(cause) = &result {
                println!("actual provider cause {cause:?}");
            }
            result
        });
        let waiting = [VerificationWait::Released, VerificationWait::Cancelled].map(|route| {
            let ((publication, mut receiving), control_storage) =
                allocated_storage(oneshot::channel);
            let mut publication = Some(publication);
            let (initial, completed) = {
                let mut verification = pin!(verify(
                    original,
                    AdmissionCase::Granted,
                    deadline,
                    Some(&mut receiving),
                ));
                let (observed, poll_storage) = allocated_storage(|| {
                    host.block_on(poll_fn(|context| {
                        Poll::Ready(verification.as_mut().poll(context))
                    }))
                });
                let initial = match &observed {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                };
                println!(
                    "blocked provider {scope:?}/{route:?}: test control {control_storage:?}; \
                     real Tokio waker poll {poll_storage:?}; observed {initial:?}"
                );
                let completed = match (observed, route) {
                    (Poll::Ready(result), _) => Some(result),
                    (Poll::Pending, VerificationWait::Released) => {
                        let (sent, wake_storage) = allocated_storage(|| {
                            publication.take().expect("one verifier release").send(())
                        });
                        assert_eq!(sent, Ok(()));
                        let (result, completion_storage) =
                            allocated_storage(|| host.block_on(verification));
                        println!(
                            "released provider {scope:?}: wake {wake_storage:?}; \
                             completion {completion_storage:?}"
                        );
                        Some(result)
                    }
                    (Poll::Pending, VerificationWait::Cancelled) => None,
                };
                (initial, completed)
            };
            let result = if let Some(result) = completed {
                result
            } else {
                let (sent, wake_storage) = allocated_storage(|| {
                    publication
                        .take()
                        .expect("retained receiver survives borrowed cancellation")
                        .send(())
                });
                assert_eq!(sent, Ok(()));
                let (result, retried_storage) = allocated_storage(|| {
                    host.block_on(verify(
                        original,
                        AdmissionCase::Granted,
                        deadline,
                        Some(&mut receiving),
                    ))
                });
                println!(
                    "cancelled provider {scope:?}: wake {wake_storage:?}; \
                     fresh borrowed verification {retried_storage:?}"
                );
                result
            };
            assert_eq!(
                initial,
                Poll::Pending,
                "genuine provider stall precedes release or cancellation"
            );
            assert!(result.is_ok());
            result
        });
        (evidence, waiting)
    }
    #[expect(
        clippy::too_many_lines,
        reason = "one custody trace retains the original and provider result through refusal, borrowed cancellation and explicit consumption"
    )]
    fn pending_storage<R>(
        owner: &mut Replay<R>,
        host: &Runtime,
        scope: Scope,
        deadline: Instant,
        verify: &impl AsyncFn(
            &ProtectedCommand,
            AdmissionCase,
            Instant,
            Option<&mut oneshot::Receiver<()>>,
        ) -> Result<R, VerificationError>,
        projection: fn(&R) -> (u64, Scope, Option<Instant>),
    ) -> (Admission<R>, u64, usize) {
        let ((), incoming_storage) = allocated_storage(|| {
            for _ in 0..2 {
                let reserved = owner.admissions.retain(protected_command());
                assert!(reserved.is_ok());
            }
        });
        let peak_reservation = owner.admissions.reserved_bytes;
        let ((releases, mut receiving), control_storage) = allocated_storage(|| {
            let first = oneshot::channel();
            let second = oneshot::channel();
            ([first.0, second.0], [first.1, second.1])
        });
        let overflow = protected_command();
        let allocation = (overflow.payload.as_ptr(), overflow.proof.as_ptr());
        let (
            verification_polls,
            stalled_refusal,
            third_verification,
            incoming_allocations,
            verification_poll_storage,
        ) = {
            let first = owner
                .admissions
                .pending
                .front()
                .expect("first incoming reservation");
            let second = owner
                .admissions
                .pending
                .back()
                .expect("second incoming reservation");
            let Admission::AwaitingVerification(first_original) = &first.admission else {
                panic!("first verifier input");
            };
            let Admission::AwaitingVerification(second_original) = &second.admission else {
                panic!("second verifier input");
            };
            let [first_release, second_release] = &mut receiving;
            let mut first_verification = pin!(verify(
                first_original,
                AdmissionCase::Granted,
                deadline,
                Some(first_release)
            ));
            let mut second_verification = pin!(verify(
                second_original,
                AdmissionCase::Granted,
                deadline,
                Some(second_release)
            ));
            let (observed, polling_storage) = allocated_storage(|| {
                host.block_on(poll_fn(|context| {
                    Poll::Ready([
                        first_verification.as_mut().poll(context),
                        second_verification.as_mut().poll(context),
                    ])
                }))
            });
            let (refused, third_verification) =
                if let Some(reason) = owner.admissions.retention_refusal(&overflow) {
                    (Err((reason, overflow)), None)
                } else {
                    let evidence =
                        host.block_on(verify(&overflow, AdmissionCase::Granted, deadline, None));
                    (Ok(overflow), Some(evidence))
                };
            (
                observed,
                refused,
                third_verification,
                [
                    (
                        first_original.payload.as_ptr(),
                        first_original.proof.as_ptr(),
                    ),
                    (
                        second_original.payload.as_ptr(),
                        second_original.proof.as_ptr(),
                    ),
                ],
                polling_storage,
            )
        };
        let cancellation_reservation = owner.admissions.reserved_bytes;
        for release in releases {
            let sent = release.send(());
            assert_eq!(sent, Ok(()));
        }
        // Both real borrowed verifier futures have been cancelled; their slots and exact inputs remain owned.
        let (evidence, verification_storage) = allocated_storage(|| {
            let Admission::AwaitingVerification(original) = &owner
                .admissions
                .pending
                .front()
                .expect("retained verifier input")
                .admission
            else {
                panic!("actual verifier original");
            };
            host.block_on(verify(
                original,
                AdmissionCase::Granted,
                deadline,
                Some(&mut receiving[0]),
            ))
        });
        let PendingAdmission {
            reservation_bytes,
            admission,
            mut publication,
            mut notification,
        } = owner
            .admissions
            .pending
            .pop_front()
            .expect("retained original");
        let Admission::AwaitingVerification(original) = admission else {
            panic!("actual pending original");
        };
        let reason = reject(
            &evidence,
            Permission::Revoked,
            owner.admissions.now,
            scope,
            projection,
        )
        .expect("observed local revocation");
        let disposition = Admission::Refused(original, evidence, reason);
        let (observed, polling) = allocated_storage(|| {
            host.block_on(poll_fn(|context| {
                let mut waiting = pin!(&mut notification);
                Poll::Ready(waiting.as_mut().poll(context))
            }))
        });
        assert!(matches!(observed, Poll::Pending));
        owner.admissions.pending.push_front(PendingAdmission {
            reservation_bytes,
            admission: disposition,
            publication: publication.take(),
            notification,
        });
        let retained_reservation = owner.admissions.reserved_bytes;
        let (sent, publication_storage) = allocated_storage(|| {
            owner
                .admissions
                .pending
                .front_mut()
                .expect("retained refusal")
                .publication
                .take()
                .expect("one local refusal publication")
                .send(())
        });
        assert_eq!(sent, Ok(()));
        let unread = protected_command();
        let unread_allocation = (unread.payload.as_ptr(), unread.proof.as_ptr());
        let (unread_refusal, unread_storage) =
            allocated_storage(|| owner.admissions.retain(unread));
        let unread_reservation = owner.admissions.reserved_bytes;
        let notified = host.block_on(
            &mut owner
                .admissions
                .pending
                .front_mut()
                .expect("unread disposition")
                .notification,
        );
        assert_eq!(notified, Ok(()));
        let notified_reservation = owner.admissions.reserved_bytes;
        let consumed = owner.admissions.consume();
        let remaining = owner.admissions.reserved_bytes;
        let saved_byte_limit = owner.admissions.byte_limit;
        owner.admissions.byte_limit = nonzero(remaining + INCOMING_RESERVATION + 3 - 1);
        let byte_overflow = protected_command();
        let byte_allocation = (byte_overflow.payload.as_ptr(), byte_overflow.proof.as_ptr());
        let byte_refused = owner.admissions.retain(byte_overflow);
        let byte_verification = match &byte_refused {
            Ok(()) => {
                let Admission::AwaitingVerification(original) = &owner
                    .admissions
                    .pending
                    .back()
                    .expect("unexpected incoming custody")
                    .admission
                else {
                    panic!("verified overflow input");
                };
                Some(host.block_on(verify(original, AdmissionCase::Granted, deadline, None)))
            }
            Err(_) => None,
        };
        owner.admissions.byte_limit = saved_byte_limit;
        // Classify after the real borrowed producer waits and retained notification have settled.
        assert!(
            verification_polls
                .iter()
                .all(|poll| matches!(poll, Poll::Pending)),
            "two genuine simultaneous verifiers"
        );
        assert!(
            third_verification.is_none(),
            "third incoming request must not invoke verification"
        );
        let Err((Refusal::Count, overflow)) = stalled_refusal else {
            panic!("actual stalled incoming count refusal");
        };
        assert_eq!(
            (overflow.payload.as_ptr(), overflow.proof.as_ptr()),
            allocation
        );
        let Err((Refusal::Count, unread)) = unread_refusal else {
            panic!("completed unread remains reserved");
        };
        assert_eq!(
            (unread.payload.as_ptr(), unread.proof.as_ptr()),
            unread_allocation
        );
        assert_eq!(
            cancellation_reservation, peak_reservation,
            "borrowed verification cancellation keeps both reservations"
        );
        assert_eq!(
            unread_reservation, peak_reservation,
            "completed unread keeps reservation"
        );
        assert_eq!(
            notified_reservation, peak_reservation,
            "notification read is not result consumption"
        );
        assert_eq!(peak_reservation - remaining, INCOMING_RESERVATION + 3);
        assert_eq!(
            owner.entries.len(),
            2,
            "input consumption leaves the same live ledger intact"
        );
        assert!(
            byte_verification.is_none(),
            "byte-overflow request must not invoke verification"
        );
        let Err((Refusal::TotalBytes, byte_overflow)) = byte_refused else {
            panic!("actual incoming byte capacity plus one");
        };
        assert_eq!(
            (byte_overflow.payload.as_ptr(), byte_overflow.proof.as_ptr()),
            byte_allocation
        );
        println!(
            "local refusal custody {scope:?}: real waker poll {polling:?}; publication {publication_storage:?}; unread overflow {unread_storage:?}; retained reservation {retained_reservation}; after explicit consume {remaining}"
        );
        let Admission::Refused(original, Ok(_), Refusal::Revoked) = &consumed else {
            panic!("complete actual refusal product");
        };
        assert_eq!(
            (original.payload.as_ptr(), original.proof.as_ptr()),
            incoming_allocations[0]
        );
        assert_eq!(original.payload.as_ref(), [2, 7]);
        assert_eq!(original.proof.as_ref(), [41]);
        let Admission::AwaitingVerification(original) = &owner
            .admissions
            .pending
            .back()
            .expect("cancelled second original retained")
            .admission
        else {
            panic!("second original custody");
        };
        assert_eq!(
            (original.payload.as_ptr(), original.proof.as_ptr()),
            incoming_allocations[1]
        );
        drop(verification_polls);
        (
            consumed,
            incoming_storage.1
                + control_storage.1
                + verification_storage.1
                + verification_poll_storage.1,
            peak_reservation,
        )
    }
    enum StorageProfileRole {
        Parent,
        Child,
    }
    #[derive(Debug, PartialEq, Eq)]
    enum StorageProfileObservation {
        Complete,
        RoleRefused,
        NoNamedTest,
        Failed,
    }
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "both concrete provider profiles preserve original allocation identities before the independent final accounting oracle"
    )]
    fn retained_storage_includes_notification_and_comparison_allocations() {
        let role = match env::var("BOMBAY_STATIC_STORAGE_PROFILE_ROLE") {
            Err(VarError::NotPresent) => StorageProfileRole::Parent,
            Ok(value) if value == "isolated" => StorageProfileRole::Child,
            Ok(value) => panic!("unknown storage profile role {value:?}"),
            Err(cause) => panic!("invalid storage profile role {cause:?}"),
        };
        match role {
            StorageProfileRole::Parent => {
                let executable = env::current_exe().expect("actual owning test executable");
                let test =
                    "replay::retained_storage_includes_notification_and_comparison_allocations";
                for (selected_role, selected_test, expected) in [
                    ("unknown", test, StorageProfileObservation::RoleRefused),
                    ("isolated", "", StorageProfileObservation::NoNamedTest),
                    (
                        "isolated",
                        "replay::misspelled_storage_profile",
                        StorageProfileObservation::NoNamedTest,
                    ),
                    ("isolated", test, StorageProfileObservation::Complete),
                ] {
                    let output = Command::new(&executable)
                        .args([selected_test, "--exact", "--test-threads=1", "--nocapture"])
                        .env("BOMBAY_STATIC_STORAGE_PROFILE_ROLE", selected_role)
                        .output()
                        .expect("actual isolated storage observation");
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    print!("{stdout}");
                    eprint!("{stderr}");
                    let observed = if stdout.contains("unknown storage profile role")
                        || stderr.contains("unknown storage profile role")
                    {
                        assert!(!output.status.success());
                        StorageProfileObservation::RoleRefused
                    } else if stdout.matches("running 1 test").count() != 1
                        || stdout.matches("test replay::retained_storage_includes_notification_and_comparison_allocations ...").count() != 1
                    {
                        StorageProfileObservation::NoNamedTest
                    } else if output.status.success()
                        && stdout.matches("isolated storage profile completed").count() == 1
                    {
                        StorageProfileObservation::Complete
                    } else {
                        StorageProfileObservation::Failed
                    };
                    if matches!(expected, StorageProfileObservation::NoNamedTest) {
                        assert!(output.status.success());
                        assert_eq!(stdout.matches("running 0 tests").count(), 1);
                    }
                    assert_eq!(
                        observed, expected,
                        "original child output/status preserved {:?}",
                        output.status
                    );
                }
                return;
            }
            StorageProfileRole::Child => {}
        }
        let host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("configured host");
        host.block_on(async {});
        let _profiler = dhat::Profiler::builder().testing().build();
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(60))
            .expect("finite validity");
        let counter_records = protected_records(binding(Scope::Counter, 19));
        let arithmetic_records = protected_records(binding(Scope::Arithmetic, 19));
        let mut gaps = [(None, None); 2];
        for (position, order) in [SequenceOrder::Increasing, SequenceOrder::Decreasing]
            .into_iter()
            .enumerate()
        {
            let (counter_originals, counter_buffers) =
                allocated_storage(|| [request(41, 7), request(42, 7)]);
            let counter_allocations = counter_originals
                .each_ref()
                .map(|original| (original.payload.as_ptr(), original.proof.as_ptr()));
            let counter_verification = record_provider::verify_replay(
                &counter_originals[0],
                counter_records[0].0,
                &counter_records,
                deadline,
            );
            let counter_future = size_of_val(&counter_verification);
            let (counter_receipts, counter_provider) = allocated_storage(|| {
                host.block_on(async {
                    let first = counter_verification.await.expect("first configured record");
                    let second = record_provider::verify_replay(
                        &counter_originals[1],
                        counter_records[1].0,
                        &counter_records,
                        deadline,
                    )
                    .await
                    .expect("second configured record");
                    [first, second]
                })
            });
            let (mut counter, counter_storage) = retained_storage(
                Scope::Counter,
                counter_originals,
                counter_receipts,
                order,
                record_provider::identity,
            );
            let (arithmetic_originals, arithmetic_buffers) =
                allocated_storage(|| [request(41, 7), request(42, 7)]);
            let arithmetic_allocations = arithmetic_originals
                .each_ref()
                .map(|original| (original.payload.as_ptr(), original.proof.as_ptr()));
            let arithmetic_verification = scheduled_provider::verify_replay(
                &arithmetic_originals[0],
                arithmetic_records[0].0,
                &arithmetic_records,
                deadline,
            );
            let arithmetic_future = size_of_val(&arithmetic_verification);
            let (arithmetic_receipts, arithmetic_provider) = allocated_storage(|| {
                host.block_on(async {
                    let first = arithmetic_verification
                        .await
                        .expect("first configured scheduled evidence");
                    let second = scheduled_provider::verify_replay(
                        &arithmetic_originals[1],
                        arithmetic_records[1].0,
                        &arithmetic_records,
                        deadline,
                    )
                    .await
                    .expect("second configured scheduled evidence");
                    [first, second]
                })
            });
            let (mut arithmetic, arithmetic_storage) = retained_storage(
                Scope::Arithmetic,
                arithmetic_originals,
                arithmetic_receipts,
                order,
                scheduled_provider::identity,
            );
            let counter_requested = counter_storage + counter_buffers.1;
            let arithmetic_requested = arithmetic_storage + arithmetic_buffers.1;
            let counter_gap = counter_requested
                .checked_sub(counter.admissions.reserved_bytes as u64)
                .filter(|bytes| *bytes != 0);
            let arithmetic_gap = arithmetic_requested
                .checked_sub(arithmetic.admissions.reserved_bytes as u64)
                .filter(|bytes| *bytes != 0);
            println!(
                "profile Counter: buffers {counter_buffers:?}; verifier {counter_provider:?}; \
             inline future {counter_future}; inline message {}; requested retained {counter_requested}; \
             conservative reservation {}; gap {counter_gap:?}",
                size_of::<CounterCommand>(),
                counter.admissions.reserved_bytes,
            );
            println!(
                "profile Arithmetic: buffers {arithmetic_buffers:?}; verifier {arithmetic_provider:?}; \
             inline future {arithmetic_future}; inline message {}; requested retained {arithmetic_requested}; \
             conservative reservation {}; gap {arithmetic_gap:?}",
                size_of::<ArithmeticCommand>(),
                arithmetic.admissions.reserved_bytes,
            );
            for ((identity, payload, proof), allocation) in
                counter_records.iter().zip(counter_allocations)
            {
                let retained = counter
                    .entries
                    .iter()
                    .find(|(sequence, _)| *sequence == identity.sequence)
                    .map(|(_, retained)| retained)
                    .expect("verified record custody");
                let Admission::VerifiedCommand(original, receipt) = &retained.pending.admission
                else {
                    panic!("measured record originals remain retained");
                };
                assert_eq!(original.payload.as_ref(), payload);
                assert_eq!(original.proof.as_ref(), proof);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    allocation
                );
                assert_eq!(record_provider::identity(receipt), Some(*identity));
            }
            for ((identity, payload, proof), allocation) in
                arithmetic_records.iter().zip(arithmetic_allocations)
            {
                let retained = arithmetic
                    .entries
                    .iter()
                    .find(|(sequence, _)| *sequence == identity.sequence)
                    .map(|(_, retained)| retained)
                    .expect("verified scheduled custody");
                let Admission::VerifiedCommand(original, receipt) = &retained.pending.admission
                else {
                    panic!("measured scheduled originals remain retained");
                };
                assert_eq!(original.payload.as_ref(), payload);
                assert_eq!(original.proof.as_ref(), proof);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    allocation
                );
                assert_eq!(scheduled_provider::identity(receipt), Some(*identity));
            }
            if let SequenceOrder::Decreasing = order {
                let original = protected_command();
                let record_outcomes = provider_storage(
                    &host,
                    Scope::Counter,
                    &original,
                    deadline,
                    &record_provider::verify,
                );
                let scheduled_outcomes = provider_storage(
                    &host,
                    Scope::Arithmetic,
                    &original,
                    deadline,
                    &scheduled_provider::verify,
                );
                // Owned measurement receipts are surrendered after recording actual causes and allocation phases.
                drop((record_outcomes, scheduled_outcomes));
                let (record_disposition, incoming_record_storage, record_reservation) =
                    pending_storage(
                        &mut counter,
                        &host,
                        Scope::Counter,
                        deadline,
                        &record_provider::verify,
                        record_provider::projection,
                    );
                let (scheduled_disposition, incoming_scheduled_storage, scheduled_reservation) =
                    pending_storage(
                        &mut arithmetic,
                        &host,
                        Scope::Arithmetic,
                        deadline,
                        &scheduled_provider::verify,
                        scheduled_provider::projection,
                    );
                assert_eq!(
                    counter.entries.len(),
                    2,
                    "explicit operation consumption does not retire the separate live ledger"
                );
                assert_eq!(arithmetic.entries.len(), 2);
                println!(
                    "combined profile Counter: incoming requested {incoming_record_storage}; combined requested {}; reservation {record_reservation}; fixed ceiling {LOCAL_STORAGE_CEILING}",
                    counter_requested + incoming_record_storage
                );
                println!(
                    "combined profile Arithmetic: incoming requested {incoming_scheduled_storage}; combined requested {}; reservation {scheduled_reservation}; fixed ceiling {LOCAL_STORAGE_CEILING}",
                    arithmetic_requested + incoming_scheduled_storage
                );
                assert!(
                    counter_requested + incoming_record_storage <= record_reservation as u64,
                    "record reservation covers measured incoming/ledger allocation requests"
                );
                assert!(
                    arithmetic_requested + incoming_scheduled_storage
                        <= scheduled_reservation as u64,
                    "scheduled reservation covers measured incoming/ledger allocation requests"
                );
                assert!(
                    record_reservation <= LOCAL_STORAGE_CEILING
                        && scheduled_reservation <= LOCAL_STORAGE_CEILING
                );
                drop((record_disposition, scheduled_disposition));
                let impossible = AdmissionService::<record_provider::Receipt>::new(
                    Instant::now(),
                    Scope::Counter,
                    nonzero(usize::MAX),
                    nonzero(usize::MAX),
                    nonzero(usize::MAX),
                );
                let Err(cause) = impossible else {
                    panic!("actual standard capacity overflow");
                };
                println!("actual pending setup refusal {cause:?}");
            }
            gaps[position] = (counter_gap, arithmetic_gap);
        }
        assert_eq!(
            gaps,
            [(None, None); 2],
            "explicit conservative reservation covers retained allocations for BOTH providers and orders"
        );
        println!("isolated storage profile completed");
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "ordinary functions connect two concrete providers and real endpoint replacement"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "all actual submissions survive until complete native retirement and temporal assertions"
    )]
    async fn exercise<R, P, V>(
        case: Case,
        target_scope: Scope,
        service: &mut ExternalActor<Replies>,
        target: &EstablishedRecipient<P>,
        old_target: Option<&EstablishedRecipient<P>>,
        reconnect: impl FnOnce() -> ExternalActor<Replies>,
        verify: V,
        unbound: impl AsyncFn(&ProtectedCommand, Instant) -> Result<R, VerificationError>,
        identity: fn(&R) -> Option<Identity>,
        projection: fn(&R) -> (u64, Scope, Option<Instant>),
        prefix: fn(usize) -> P::Msg,
        construct: fn(ProtectedCommand, R) -> P::Msg,
        recover: fn(P::Msg) -> (ProtectedCommand, R),
        snapshot: fn(EstablishedRecipient<Replies>) -> P::Msg,
    ) -> Observations<R>
    where
        P: Protocol<Addr = MailAddr>,
        P::Msg: Send + 'static,
        V: AsyncFn(
            &ProtectedCommand,
            Identity,
            &[ProtectedRecord],
            Instant,
        ) -> Result<R, VerificationError>,
    {
        let now = Instant::now();
        let deadline = now
            .checked_add(Duration::from_secs(60))
            .expect("finite fixture validity");
        let old = match case {
            Case::Exhaustion => binding(target_scope, 29),
            _ => binding(target_scope, 19),
        };
        let current = match case {
            Case::Replacement => binding(target_scope, 23),
            _ => old,
        };
        let sequence = match case {
            Case::Exhaustion => u64::MAX,
            _ => 0,
        };
        let mut next = Some(sequence);
        let owner = Replay::new(
            current,
            AdmissionService::new(
                now,
                target_scope,
                nonzero(2),
                nonzero(INCOMING_RESERVATION + 16),
                nonzero(LOCAL_STORAGE_CEILING),
            )
            .expect("finite pending storage reservation"),
        )
        .expect("finite live-binding storage reservation");
        let mut observed = Observations {
            owner,
            submissions: Vec::new(),
            snapshots: Vec::new(),
            allocations: Vec::new(),
            rejected_allocations: Vec::new(),
            exhaustion: None,
            pending: Poll::Ready(()),
            senders: vec![service.address()],
            closed: None,
        };
        let first = Identity {
            binding: old,
            sequence,
        };
        let second = Identity {
            binding: old,
            sequence: 1,
        };
        let third = Identity {
            binding: old,
            sequence: match case {
                Case::Exhaustion => 0,
                _ => 2,
            },
        };
        let configured = [
            (first, [2, 7], [41]),
            (second, [2, 7], [42]),
            (first, [2, 8], [43]),
            (third, [2, 7], [44]),
            (first, [2, 7], [45]),
        ];
        if matches!(case, Case::Live) {
            for sequence in 0..1_024 {
                let sent = service.try_send(target, prefix(sequence));
                match sent {
                    Ok(()) => {}
                    Err(original) => {
                        drop(original);
                        panic!("native prefix fits");
                    }
                }
            }
        }
        let original = request(41, 7);
        observed
            .allocations
            .push((original.payload.as_ptr(), original.proof.as_ptr()));
        let (claimed, original) = match case {
            Case::Replacement => (first, original),
            _ => issue(&mut next, current, original).expect("first checked identity"),
        };
        let receipt = verify(&original, claimed, &configured, deadline)
            .await
            .expect("configured authenticated original");
        let submitted = submit(
            &mut observed.owner,
            service,
            target,
            original,
            receipt,
            identity,
            projection,
            construct,
            recover,
        );
        observed.submissions.push(submitted);
        if matches!(case, Case::Exhaustion) {
            let original = request(44, 7);
            observed
                .rejected_allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let exhausted = issue(&mut next, current, original);
            match exhausted {
                Ok((claimed, original)) => {
                    let receipt = verify(&original, claimed, &configured, deadline)
                        .await
                        .expect("configured overflow inverse");
                    let submitted = submit(
                        &mut observed.owner,
                        service,
                        target,
                        original,
                        receipt,
                        identity,
                        projection,
                        construct,
                        recover,
                    );
                    observed.submissions.push(submitted);
                }
                Err(original) => observed.exhaustion = Some(original),
            }
        }
        if matches!(case, Case::Live) {
            let (claimed, original) =
                issue(&mut next, current, request(42, 7)).expect("second checked identity");
            observed
                .allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let receipt = verify(&original, claimed, &configured, deadline)
                .await
                .expect("second authenticated original");
            let submitted = submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            );
            observed.submissions.push(submitted);
            {
                let retained = observed
                    .owner
                    .entries
                    .iter_mut()
                    .find(|(sequence, _)| *sequence == 0)
                    .map(|(_, retained)| retained)
                    .expect("actual Full reservation");
                let mut wait = pin!(&mut retained.pending.notification);
                let mut context = Context::from_waker(Waker::noop());
                observed.pending = wait.as_mut().poll(&mut context).map(|result| {
                    result.expect("actual admission notification");
                });
            }

            let original = request(45, 7);
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("fresh evidence same authenticated identity/body");
            let submitted = submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            );
            observed.submissions.push(submitted);
        }
        let sent = service.send(target, snapshot(service.recipient())).await;
        assert!(sent.is_ok());
        observed.snapshots.push(
            service
                .receive()
                .await
                .expect("native prefix barrier")
                .message,
        );
        if matches!(case, Case::Live) {
            for sequence in [0, 1] {
                let retained = observed
                    .owner
                    .entries
                    .iter_mut()
                    .find(|(confirmed, _)| *confirmed == sequence)
                    .map(|(_, retained)| retained)
                    .expect("same Full original");
                let attempted = attempt_pending(
                    &mut retained.pending,
                    observed.owner.admissions.permission,
                    observed.owner.admissions.now,
                    target_scope,
                    service,
                    target,
                    projection,
                    construct,
                    recover,
                );
                observed.submissions.push(Ok((attempted, None)));
            }
        }
        if !matches!(case, Case::Replacement) {
            let original = request(41, 7);
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("admitted duplicate authenticated");
            let submitted = submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            );
            observed.submissions.push(submitted);
        }
        let sent = service.send(target, snapshot(service.recipient())).await;
        assert!(sent.is_ok());
        observed.snapshots.push(
            service
                .receive()
                .await
                .expect("actual processing completion")
                .message,
        );
        if matches!(case, Case::Live) {
            let original = request(43, 8);
            observed
                .rejected_allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("changed protected bytes independently authenticate");
            let submitted = submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            );
            observed.submissions.push(submitted);
            let (claimed, original) =
                issue(&mut next, current, request(44, 7)).expect("new checked identity");
            observed
                .rejected_allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let receipt = verify(&original, claimed, &configured, deadline)
                .await
                .expect("new full-retention identity authenticates");
            observed.submissions.push(submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            ));
            for (_, retained) in &mut observed.owner.entries {
                if matches!(retained.pending.admission, Admission::Accepted) {
                    let notified = (&mut retained.pending.notification).await;
                    assert!(notified.is_ok());
                }
            }
            let original = request(41, 7);
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("after consuming actual notification");
            observed.submissions.push(submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            ));
            service.close_admission();
            let drained = service.receive().await;
            assert!(drained.is_none());
            let replaced = replace(service, reconnect());
            drop(replaced);
            observed.senders.push(service.address());
            let sent = service.send(target, prefix(1_024)).await;
            assert!(sent.is_ok());
            let original = request(45, 7);
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("same authenticated binding after local connection replacement");
            observed.submissions.push(submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            ));
            let original = request(41, 7);
            observed
                .rejected_allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let receipt = unbound(&original, deadline)
                .await
                .expect("ordinary provider original remains unbound");
            observed.submissions.push(submit(
                &mut observed.owner,
                service,
                target,
                original,
                receipt,
                identity,
                projection,
                construct,
                recover,
            ));
        }
        if let Some(old_target) = old_target {
            let original = request(41, 7);
            observed
                .rejected_allocations
                .push((original.payload.as_ptr(), original.proof.as_ptr()));
            let receipt = verify(&original, first, &configured, deadline)
                .await
                .expect("old exact recipient command");
            let command = construct(original, receipt);
            observed.closed = Some(match service.try_send(old_target, command) {
                Ok(()) => Ok(()),
                Err(TrySendError::Closed(command)) => Err(TrySendError::Closed(recover(command))),
                Err(TrySendError::Full(command)) => Err(TrySendError::Full(recover(command))),
            });
        }
        let sent = service.send(target, snapshot(service.recipient())).await;
        assert!(sent.is_ok());
        observed.snapshots.push(
            service
                .receive()
                .await
                .expect("final independent actor trace")
                .message,
        );
        observed
    }
    #[expect(
        clippy::too_many_lines,
        reason = "all three actual replay traces conserve original provider refusals and native observations"
    )]
    fn assert_observations<R>(
        case: Case,
        observed: &Observations<R>,
        identity: fn(&R) -> Option<Identity>,
        expected: usize,
        value: u64,
    ) {
        let final_snapshot = observed.snapshots.last().expect("actual final trace");
        assert_eq!(
            final_snapshot.protected, expected,
            "live binding replay cannot second-admit"
        );
        assert_eq!(final_snapshot.value, value);
        let mut refused = observed
            .submissions
            .iter()
            .filter_map(|submission| submission.as_ref().err());
        match case {
            Case::Live => {
                assert!(matches!(observed.pending, Poll::Pending));
                assert_eq!(observed.owner.entries.len(), 2);
                for (position, expected_reason) in [
                    ReplayRefusal::Conflict,
                    ReplayRefusal::Capacity,
                    ReplayRefusal::Unbound,
                ]
                .iter()
                .enumerate()
                {
                    let (reason, original, receipt) =
                        refused.next().expect("acquired actual replay refusal");
                    assert_eq!(reason, expected_reason);
                    assert_eq!(
                        (original.payload.as_ptr(), original.proof.as_ptr()),
                        observed.rejected_allocations[position]
                    );
                    match expected_reason {
                        ReplayRefusal::Unbound => assert_eq!(identity(receipt), None),
                        _ => assert_eq!(
                            identity(receipt),
                            Some(Identity {
                                binding: observed.owner.binding,
                                sequence: match expected_reason {
                                    ReplayRefusal::Capacity => 2,
                                    _ => 0,
                                }
                            })
                        ),
                    }
                }
                assert!(refused.next().is_none());
                let native_full: Vec<_> = observed.submissions[..3]
                    .iter()
                    .map(|submission| submission.as_ref().ok().map(|(refusal, _)| *refusal))
                    .collect();
                assert_eq!(native_full, [Some(Some(Refusal::Full)); 3]);
                assert_eq!(observed.snapshots[0].protected, 0);
                assert_ne!(observed.senders[0], observed.senders[1]);
            }
            Case::Replacement => {
                let (reason, original, receipt) =
                    refused.next().expect("old authenticated binding refused");
                assert_eq!(*reason, ReplayRefusal::Binding);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    observed.allocations[0]
                );
                let confirmed = identity(receipt).expect("old confirmed binding");
                assert_eq!(
                    confirmed.binding,
                    binding(observed.owner.binding.target, 19)
                );
                assert_eq!(observed.owner.binding.runtime, 23);
                assert!(refused.next().is_none());
                assert!(observed.owner.entries.is_empty());
                let Some(Err(TrySendError::Closed((original, receipt)))) = &observed.closed else {
                    panic!("actual old exact recipient must be Closed");
                };
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    observed.rejected_allocations[0]
                );
                let confirmed = identity(receipt).expect("closed original evidence");
                assert_eq!(
                    confirmed.binding,
                    binding(observed.owner.binding.target, 19)
                );
            }
            Case::Exhaustion => {
                let Some((reason, original)) = &observed.exhaustion else {
                    panic!("actual checked exhaustion refusal");
                };
                assert_eq!(*reason, ReplayRefusal::Exhausted);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    observed.rejected_allocations[0]
                );
                assert!(
                    observed
                        .owner
                        .entries
                        .iter()
                        .any(|(sequence, _)| *sequence == u64::MAX)
                );
                assert_eq!(observed.owner.entries.len(), 1);
                assert!(refused.next().is_none());
            }
        }
        for (_, retained) in &observed.owner.entries {
            assert_eq!(retained.protected.as_ref(), [2, 7]);
            assert!(matches!(retained.pending.admission, Admission::Accepted));
        }
        for submission in &observed.submissions {
            if let Ok((_, Some((original, receipt)))) = submission {
                assert_eq!(original.payload.as_ref(), [2, 7]);
                assert_eq!(
                    identity(receipt).expect("duplicate bound receipt").binding,
                    observed.owner.binding
                );
            }
        }
    }
    fn assert_prefix(prefix: &[(MailAddr, usize)], senders: &[MailAddr], case: Case) {
        for (position, (from, sequence)) in prefix.iter().enumerate() {
            assert_eq!(*sequence, position);
            assert_eq!(*from, senders[usize::from(position == 1_024)]);
        }
        assert_eq!(
            prefix.len(),
            match case {
                Case::Live => 1_025,
                _ => 0,
            }
        );
    }
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "actual old owner retirement, replacement and full Counter originals precede replay assertions"
    )]
    fn counter_retains_live_binding_replay() {
        let mut retired = None;
        for case in [Case::Live, Case::Replacement, Case::Exhaustion] {
            let host = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("finite host");
            let previous = match case {
                Case::Replacement => retired.as_ref(),
                _ => None,
            };
            let received = host
                .block_on(
                    Application::new(Counter::default().stop_on_shutdown())
                        .run_with::<RootTerminal<StopOnShutdown<Counter>>, _, _, _, _, _>(
                            |application| async move {
                                let mut service = application
                                    .interface(())
                                    .external::<Replies>()
                                    .expect("typed service");
                                let target = application.root().established_recipient();
                                let observations = exercise(
                                    case,
                                    Scope::Counter,
                                    &mut service,
                                    &target,
                                    previous,
                                    || {
                                        application
                                            .interface(())
                                            .external::<Replies>()
                                            .expect("new local service connection")
                                    },
                                    record_provider::verify_replay,
                                    async |original, deadline| {
                                        record_provider::verify(
                                            original,
                                            AdmissionCase::Granted,
                                            deadline,
                                            None,
                                        )
                                        .await
                                    },
                                    record_provider::identity,
                                    record_provider::projection,
                                    CounterCommand::Prefix,
                                    CounterCommand::Protected,
                                    counter_recovery,
                                    CounterCommand::Snapshot,
                                )
                                .await;
                                service.close_admission();
                                let drained = service.receive().await;
                                assert!(drained.is_none());
                                let stopped = application.lifecycle().request_shutdown();
                                assert_eq!(stopped, Ok(()));
                                (observations, target)
                            },
                        ),
                )
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("actual startup");
                });
            drop(host);
            let (
                ApplicationOutcome::Completed {
                    output: (observations, target),
                    cleanup: Ok(()),
                },
                Ok((origin, terminal)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = received
            else {
                panic!("complete native and notification originals");
            };
            let ActorRetirement::Completed { behavior, .. } = &terminal else {
                panic!("native Counter");
            };
            let expected = match case {
                Case::Live => 2,
                Case::Replacement => 0,
                Case::Exhaustion => 1,
            };
            assert_eq!(
                behavior.base().protected.len(),
                expected,
                "retained replay identities permit only unique native admissions"
            );
            assert_observations(
                case,
                &observations,
                record_provider::identity,
                expected,
                2 * expected as u64,
            );
            assert_eq!(behavior.base().value, 2 * expected as u64);
            for (position, (from, original, receipt)) in
                behavior.base().protected.iter().enumerate()
            {
                assert_eq!(*from, observations.senders[0]);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    observations.allocations[position]
                );
                assert_eq!(
                    record_provider::identity(receipt).expect("original authenticated identity"),
                    Identity {
                        binding: observations.owner.binding,
                        sequence: match case {
                            Case::Exhaustion => u64::MAX,
                            _ => position as u64,
                        }
                    }
                );
            }
            assert_prefix(&behavior.base().prefix, &observations.senders, case);
            assert_completed(RootTerminal::Root { origin, terminal }, None);
            drop(observations); // The volatile ledger is destroyed before the replacement Application exists.
            retired = Some(target);
        }
    }
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "actual old owner retirement, replacement and full Arithmetic originals precede replay assertions"
    )]
    fn arithmetic_retains_live_binding_replay() {
        let mut retired = None;
        for case in [Case::Live, Case::Replacement, Case::Exhaustion] {
            let host = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("finite host");
            let previous = match case {
                Case::Replacement => retired.as_ref(),
                _ => None,
            };
            let received = host
                .block_on(
                    Application::new(
                        Arithmetic {
                            prefix: Vec::new(),
                            protected: Vec::new(),
                            value: 3,
                        }
                        .stop_on_shutdown(),
                    )
                    .run_with::<RootTerminal<StopOnShutdown<Arithmetic>>, _, _, _, _, _>(
                        |application| async move {
                            let mut service = application
                                .interface(())
                                .external::<Replies>()
                                .expect("typed service");
                            let target = application.root().established_recipient();
                            let observations = exercise(
                                case,
                                Scope::Arithmetic,
                                &mut service,
                                &target,
                                previous,
                                || {
                                    application
                                        .interface(())
                                        .external::<Replies>()
                                        .expect("new local service connection")
                                },
                                scheduled_provider::verify_replay,
                                async |original, deadline| {
                                    scheduled_provider::verify(
                                        original,
                                        AdmissionCase::Granted,
                                        deadline,
                                        None,
                                    )
                                    .await
                                },
                                scheduled_provider::identity,
                                scheduled_provider::projection,
                                ArithmeticCommand::Prefix,
                                ArithmeticCommand::Protected,
                                arithmetic_recovery,
                                ArithmeticCommand::Snapshot,
                            )
                            .await;
                            service.close_admission();
                            let drained = service.receive().await;
                            assert!(drained.is_none());
                            let stopped = application.lifecycle().request_shutdown();
                            assert_eq!(stopped, Ok(()));
                            (observations, target)
                        },
                    ),
                )
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("actual startup");
                });
            drop(host);
            let (
                ApplicationOutcome::Completed {
                    output: (observations, target),
                    cleanup: Ok(()),
                },
                Ok((origin, terminal)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = received
            else {
                panic!("complete native and notification originals");
            };
            let ActorRetirement::Completed { behavior, .. } = &terminal else {
                panic!("native Arithmetic");
            };
            let expected = match case {
                Case::Live => 2,
                Case::Replacement => 0,
                Case::Exhaustion => 1,
            };
            assert_eq!(
                behavior.base().protected.len(),
                expected,
                "retained replay identities permit only unique native admissions"
            );
            let value = match case {
                Case::Live => 12,
                Case::Replacement => 3,
                Case::Exhaustion => 6,
            };
            assert_observations(
                case,
                &observations,
                scheduled_provider::identity,
                expected,
                value,
            );
            assert_eq!(behavior.base().value, value);
            for (position, (from, original, receipt)) in
                behavior.base().protected.iter().enumerate()
            {
                assert_eq!(*from, observations.senders[0]);
                assert_eq!(
                    (original.payload.as_ptr(), original.proof.as_ptr()),
                    observations.allocations[position]
                );
                assert_eq!(
                    scheduled_provider::identity(receipt).expect("original authenticated identity"),
                    Identity {
                        binding: observations.owner.binding,
                        sequence: match case {
                            Case::Exhaustion => u64::MAX,
                            _ => position as u64,
                        }
                    }
                );
            }
            assert_prefix(&behavior.base().prefix, &observations.senders, case);
            assert_completed(RootTerminal::Root { origin, terminal }, None);
            drop(observations);
            retired = Some(target);
        }
    }
}
