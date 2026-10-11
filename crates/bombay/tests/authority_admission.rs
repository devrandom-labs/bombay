//! Private local composition evidence; no production provider, replay or network protocol.
use bombay::behavior::{BehaviorBase, EstablishedDelivery, MessageProtocol, Protocol};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use core::ops::AsyncFn;
use std::collections::VecDeque;
use std::future::Future;
use std::mem::{replace, size_of};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::{runtime::Builder, sync::oneshot};
mod application_support;
use application_support::{RootTerminal, assert_completed};

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
    use super::{AdmissionCase, Instant, ProtectedCommand, Scope, VerificationError, oneshot};
    pub(super) struct Receipt {
        amount: u64,
        scope: Scope,
        deadline: Option<Instant>,
    }
    pub(super) fn projection(receipt: &Receipt) -> (u64, Scope, Option<Instant>) {
        (receipt.amount, receipt.scope, receipt.deadline)
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
    use super::{AdmissionCase, Instant, ProtectedCommand, Scope, VerificationError, oneshot};
    pub(super) struct Receipt {
        amount: u64,
        scope: Scope,
        deadline: Option<Instant>,
    }
    pub(super) fn projection(receipt: &Receipt) -> (u64, Scope, Option<Instant>) {
        (receipt.amount, receipt.scope, receipt.deadline)
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
    charged: usize,
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
    charged: usize,
}
impl<R> AdmissionService<R> {
    fn new(
        now: Instant,
        expected_target: Scope,
        count: NonZeroUsize,
        individual: NonZeroUsize,
        bytes: NonZeroUsize,
    ) -> Self {
        let pending = VecDeque::with_capacity(count.get());
        let charged = pending
            .capacity()
            .checked_mul(size_of::<PendingAdmission<R>>())
            .expect("explicit finite header allocation");
        assert!(charged <= bytes.get());
        Self {
            permission: Permission::Granted,
            expected_target,
            now,
            pending,
            count_limit: count,
            individual_limit: individual,
            byte_limit: bytes,
            charged,
        }
    }
    fn retain(&mut self, original: ProtectedCommand) -> Result<(), (Refusal, ProtectedCommand)> {
        let Some(charged) = original
            .payload
            .len()
            .checked_add(original.proof.len())
            .and_then(|bytes| bytes.checked_add(size_of::<PendingAdmission<R>>()))
        else {
            return Err((Refusal::ByteAccountingOverflow, original));
        };
        if self.pending.len() >= self.count_limit.get() {
            return Err((Refusal::Count, original));
        }
        if charged > self.individual_limit.get() {
            return Err((Refusal::IndividualBytes, original));
        }
        let Some(total) = self.charged.checked_add(charged) else {
            return Err((Refusal::ByteAccountingOverflow, original));
        };
        if total > self.byte_limit.get() {
            return Err((Refusal::TotalBytes, original));
        }
        let (publication, notification) = oneshot::channel();
        self.pending.push_back(PendingAdmission {
            charged,
            admission: Admission::AwaitingVerification(original),
            publication: Some(publication),
            notification,
        });
        self.charged = total;
        Ok(())
    }
    fn consume(&mut self) -> Admission<R> {
        let completed = self
            .pending
            .pop_front()
            .expect("one explicitly consumed disposition");
        self.charged -= completed.charged;
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
    if let Some(reason) = reject(
        &evidence,
        owner.permission,
        owner.now,
        owner.expected_target,
        projection,
    ) {
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
    let footprint = size_of::<PendingAdmission<R>>() + 3;
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
    let mut owner = AdmissionService::new(now, expected_target, count, individual, bytes);
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
    let charge = owner.charged;
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
        owner.charged, charge,
        "completed but unreceived remains charged"
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
        owner.charged,
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
    let mut owner = AdmissionService::<R>::new(now, expected_target, count, individual, bytes);
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
    let exact_charge = owner.charged;
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
        owner.charged, exact_charge,
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
    );
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
        AdmissionService::<R>::new(now, expected_target, count, nonzero(footprint - 1), bytes);
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
