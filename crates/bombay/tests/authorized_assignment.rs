//! Finite assignment/deadline evidence; raw serial equality is not caller authentication or KERI proof.
use bombay::behavior::{Behavior, BehaviorBase, EstablishedDelivery, MessageProtocol, User};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use std::time::{Duration, Instant};
use tokio::{runtime::Builder, sync::oneshot};
mod application_support;
use application_support::{RootTerminal, assert_completed};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Grant {
    Counter,
    Other,
}
#[derive(Clone, Copy)]
enum Deployment {
    ExplicitTest,
    ProductionRequired,
}
#[derive(Debug)]
enum StartupRefusal {
    MissingAuthority,
    Unavailable(oneshot::error::RecvError),
    TestAuthorityInProduction,
    Assignment(AssignmentRefusal),
}
#[derive(Debug, PartialEq, Eq)]
enum AssignmentRefusal {
    Grant,
    Exhausted,
}
struct Request {
    assignment: u64,
    original: Box<[u8]>,
}
enum Refusal {
    Assignment(Request),
    Expired(Request),
    Mailbox(TrySendError<Command>),
}
mod assignment_authority {
    use super::{AssignmentRefusal, Deployment, Grant, StartupRefusal, oneshot};

    pub(super) struct Authority {
        grant: Grant,
        next: Option<u64>,
    }
    pub(super) struct AuthorizedAssignment {
        serial: u64,
    }
    impl AuthorizedAssignment {
        pub(super) fn serial(&self) -> u64 {
            self.serial
        }
    }
    impl Authority {
        pub(super) fn configured(next: u64) -> Self {
            Self {
                grant: Grant::Counter,
                next: Some(next),
            }
        }
        pub(super) fn issue(
            &mut self,
            claim: Grant,
        ) -> Result<AuthorizedAssignment, AssignmentRefusal> {
            if claim != self.grant {
                return Err(AssignmentRefusal::Grant);
            }
            let Some(serial) = self.next else {
                return Err(AssignmentRefusal::Exhausted);
            };
            self.next = serial.checked_add(1);
            Ok(AuthorizedAssignment { serial })
        }
    }
    pub(super) fn select(
        deployment: Deployment,
        source: Option<Result<Authority, oneshot::error::RecvError>>,
    ) -> Result<Authority, StartupRefusal> {
        match (deployment, source) {
            (_, None) => Err(StartupRefusal::MissingAuthority),
            (_, Some(Err(cause))) => Err(StartupRefusal::Unavailable(cause)),
            (Deployment::ProductionRequired, Some(Ok(_))) => {
                Err(StartupRefusal::TestAuthorityInProduction)
            }
            (Deployment::ExplicitTest, Some(Ok(authority))) => Ok(authority),
        }
    }
}
use assignment_authority::{Authority, AuthorizedAssignment};

struct ConfiguredExport {
    assignment: AuthorizedAssignment,
    recipient: EstablishedRecipient<<Counter as Behavior>::Protocol>,
    deadline: Instant,
}
impl ConfiguredExport {
    fn activated(
        assignment: AuthorizedAssignment,
        recipient: EstablishedRecipient<<Counter as Behavior>::Protocol>,
        lifetime: Duration,
    ) -> Self {
        let deadline = Instant::now()
            .checked_add(lifetime)
            .expect("finite profile lifetime");
        Self {
            assignment,
            recipient,
            deadline,
        }
    }
    fn cached_deadline(&self, claim: u64) -> Option<Instant> {
        (claim == self.assignment.serial()).then_some(self.deadline)
    }
    fn attempt(&self, service: &ExternalActor<Replies>, request: Request) -> Result<(), Refusal> {
        self.attempt_at(service, request, Instant::now())
    }
    fn attempt_at(
        &self,
        service: &ExternalActor<Replies>,
        request: Request,
        now: Instant,
    ) -> Result<(), Refusal> {
        let Some(deadline) = self.cached_deadline(request.assignment) else {
            return Err(Refusal::Assignment(request));
        };
        if now >= deadline {
            return Err(Refusal::Expired(request));
        }
        service
            .try_send(&self.recipient, Command::Protected(request))
            .map_err(Refusal::Mailbox)
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    prefix: usize,
    protected: usize,
}
type Replies = MessageProtocol<MailAddr, Snapshot>;
enum Command {
    Prefix,
    Protected(Request),
    Snapshot(EstablishedRecipient<Replies>),
}
#[derive(Default)]
struct Counter {
    prefix: usize,
    originals: Vec<(MailAddr, Request)>,
}
#[bombay::actor(sends = { snapshots: Vec<EstablishedDelivery<Replies>>, })]
impl Counter {
    fn receive(&mut self, from: MailAddr, command: Command) -> BehaviorActed<Self> {
        match command {
            Command::Prefix => self.prefix += 1,
            Command::Protected(request) => self.originals.push((from, request)),
            Command::Snapshot(recipient) => {
                return Ok(CounterActions::send_snapshots(
                    Actions::cont(),
                    EstablishedDelivery::new(
                        recipient,
                        Snapshot {
                            prefix: self.prefix,
                            protected: self.originals.len(),
                        },
                    ),
                ));
            }
        }
        Ok(Actions::cont())
    }
}
fn request(assignment: u64) -> Request {
    Request {
        assignment,
        original: vec![2, 7].into_boxed_slice(),
    }
}
fn assert_original(request: &Request, allocation: usize, assignment: u64) {
    assert_eq!(request.original.as_ptr() as usize, allocation);
    assert_eq!(request.original.as_ref(), [2, 7]);
    assert_eq!(request.assignment, assignment);
}

fn assert_counter(counter: &Counter, expected: &[(MailAddr, usize, u64)]) {
    assert_eq!(
        counter.originals.len(),
        expected.len(),
        "exact configured assignment admits once"
    );
    for ((from, original), &(sender, allocation, serial)) in counter.originals.iter().zip(expected)
    {
        assert_eq!(*from, sender);
        assert_original(original, allocation, serial);
    }
}
fn assert_snapshot(
    snapshot: Option<User<MailAddr, Snapshot>>,
    from: MailAddr,
    prefix: usize,
    protected: usize,
) {
    let snapshot = snapshot.expect("actual typed processing receipt");
    assert_eq!(snapshot.from, from);
    assert_eq!(snapshot.message, Snapshot { prefix, protected });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "two actual incarnations retain complete temporal, original and native custody"
)]
fn authorized_assignments_survive_application_replacement() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("configured host");
    host.block_on(async {
        {
            let mut authority = assignment_authority::select(
                Deployment::ExplicitTest,
                Some(Ok(Authority::configured(7))),
            )
            .expect("explicit finite authority");
            let denied = authority.issue(Grant::Other);
            assert!(matches!(denied, Err(AssignmentRefusal::Grant)));
            let first = authority
                .issue(Grant::Counter)
                .expect("first authorized assignment");
            let first_serial = first.serial();
            let received = Application::new(Counter::default().stop_on_shutdown())
                .run_with::<RootTerminal<StopOnShutdown<Counter>>, _, _, _, _, _>(
                    |application| async move {
                        let mut service = application
                            .interface(())
                            .external::<Replies>()
                            .expect("service");
                        let recipient = application.root().established_recipient();
                        let configured = ConfiguredExport::activated(
                            first,
                            recipient.clone(),
                            Duration::from_secs(60),
                        );
                        for _ in 0..1_024 {
                            let prefix = service.try_send(&recipient, Command::Prefix);
                            assert!(prefix.is_ok(), "actual prefix setup");
                        }
                        let original = request(first_serial);
                        let allocation = original.original.as_ptr() as usize;
                        let full = configured.attempt(&service, original);
                        let drained_barrier = service
                            .send(&recipient, Command::Snapshot(service.recipient()))
                            .await;
                        let drained = service.receive().await;
                        let attempted = match full {
                            Err(Refusal::Mailbox(TrySendError::Full(Command::Protected(
                                original,
                            )))) => Ok(configured.attempt(&service, original)),
                            original => Err(original),
                        };
                        let barrier = service
                            .send(&recipient, Command::Snapshot(service.recipient()))
                            .await;
                        let snapshot = service.receive().await;
                        let shutdown = application.lifecycle().request_shutdown();
                        (
                            recipient,
                            allocation,
                            service.address(),
                            attempted,
                            barrier,
                            snapshot,
                            shutdown,
                            drained,
                            drained_barrier,
                        )
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("Application owns its execution"));
            let (
                ApplicationOutcome::Completed {
                    output: observed,
                    cleanup: Ok(()),
                },
                Ok((origin, terminal)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = received
            else {
                panic!("complete first Work/native/notification custody");
            };
            let (
                old,
                allocation,
                sender,
                attempted,
                barrier,
                snapshot,
                shutdown,
                drained,
                drained_barrier,
            ) = observed;
            let ActorRetirement::Completed { behavior, .. } = &terminal else {
                panic!("first native state");
            };
            assert_counter(behavior.base(), &[(sender, allocation, first_serial)]);
            let Ok(attempted) = attempted else {
                panic!("actual Full recovered before a fresh system-time retry");
            };
            assert!(attempted.is_ok());
            assert!(barrier.is_ok());
            assert!(drained_barrier.is_ok());
            assert_snapshot(drained, origin.address(), 1_024, 0);
            assert_eq!(behavior.base().prefix, 1_024);
            assert_snapshot(snapshot, origin.address(), 1_024, 1);
            assert_eq!(shutdown, Ok(()));
            assert_completed(RootTerminal::Root { origin, terminal }, None);

            // This same trusted issuer survives; all old native ownership retired before replacement.
            let next = authority
                .issue(Grant::Counter)
                .expect("replacement authorized assignment");
            let next_serial = next.serial();
            let received = Application::new(Counter::default().stop_on_shutdown())
                .run_with::<RootTerminal<StopOnShutdown<Counter>>, _, _, _, _, _>(
                    |application| async move {
                        let mut service = application
                            .interface(())
                            .external::<Replies>()
                            .expect("service");
                        let recipient = application.root().established_recipient();
                        let configured = ConfiguredExport::activated(
                            next,
                            recipient.clone(),
                            Duration::from_secs(60),
                        );
                        let stale = request(first_serial);
                        let stale_allocation = stale.original.as_ptr() as usize;
                        let refused = configured.attempt(&service, stale);
                        let fresh = request(next_serial);
                        let fresh_allocation = fresh.original.as_ptr() as usize;
                        let accepted = configured.attempt(&service, fresh);
                        let old_request = request(first_serial);
                        let old_allocation = old_request.original.as_ptr() as usize;
                        let old_attempt = service.try_send(&old, Command::Protected(old_request));
                        let barrier = service
                            .send(&recipient, Command::Snapshot(service.recipient()))
                            .await;
                        let snapshot = service.receive().await;
                        let shutdown = application.lifecycle().request_shutdown();
                        (
                            stale_allocation,
                            fresh_allocation,
                            old_allocation,
                            service.address(),
                            refused,
                            accepted,
                            old_attempt,
                            barrier,
                            snapshot,
                            shutdown,
                        )
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("Application owns its replacement"));
            let (
                ApplicationOutcome::Completed {
                    output: observed,
                    cleanup: Ok(()),
                },
                Ok((origin, terminal)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = received
            else {
                panic!("complete replacement Work/native/notification custody");
            };
            let (
                stale_allocation,
                fresh_allocation,
                old_allocation,
                sender,
                refused,
                accepted,
                old_attempt,
                barrier,
                snapshot,
                shutdown,
            ) = observed;
            let ActorRetirement::Completed { behavior, .. } = &terminal else {
                panic!("replacement native state");
            };
            assert_counter(behavior.base(), &[(sender, fresh_allocation, next_serial)]);
            assert_ne!(first_serial, next_serial);
            let Err(Refusal::Assignment(stale)) = refused else {
                panic!("old assignment refused");
            };
            assert_original(&stale, stale_allocation, first_serial);
            let Err(TrySendError::Closed(Command::Protected(old_original))) = old_attempt else {
                panic!("old exact endpoint remains closed");
            };
            assert_original(&old_original, old_allocation, first_serial);
            assert!(accepted.is_ok());
            assert!(barrier.is_ok());
            assert_snapshot(snapshot, origin.address(), 0, 1);
            assert_eq!(shutdown, Ok(()));
            assert_completed(RootTerminal::Root { origin, terminal }, None);
        }
        let cold = assignment_authority::select(Deployment::ExplicitTest, None).and_then(
            |mut authority| {
                authority
                    .issue(Grant::Counter)
                    .map_err(StartupRefusal::Assignment)
            },
        );
        let cold = export_assignment(cold).await;
        assert!(matches!(cold, Err(StartupRefusal::MissingAuthority)));
    });
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "capacity, equality and sampled expiry retain all original and native outcomes"
)]
fn cached_deadlines_and_full_retries_sample_current_time() {
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
                    let mut authority = Authority::configured(19);
                    let mut service = application
                        .interface(())
                        .external::<Replies>()
                        .expect("service");
                    let recipient = application.root().established_recipient();
                    let configured = ConfiguredExport::activated(
                        authority
                            .issue(Grant::Counter)
                            .expect("authorized assignment"),
                        recipient.clone(),
                        Duration::ZERO,
                    );
                    let serial = configured.assignment.serial();
                    let before = configured
                        .deadline
                        .checked_sub(Duration::from_secs(1))
                        .expect("finite controlled time");
                    for _ in 0..1_024 {
                        let prefix = service.try_send(&recipient, Command::Prefix);
                        assert!(prefix.is_ok(), "actual mailbox prefix setup");
                    }
                    let original = request(serial);
                    let allocation = original.original.as_ptr() as usize;
                    let full = configured.attempt_at(&service, original, before);
                    let barrier = service
                        .send(&recipient, Command::Snapshot(service.recipient()))
                        .await;
                    let drained = service.receive().await;
                    let equality = match full {
                        Err(Refusal::Mailbox(TrySendError::Full(Command::Protected(original)))) => {
                            Ok(configured.attempt_at(&service, original, configured.deadline))
                        }
                        original => Err(original),
                    };
                    let actual = request(serial);
                    let actual_allocation = actual.original.as_ptr() as usize;
                    let expired = configured.attempt(&service, actual);
                    let cached = configured.cached_deadline(serial);
                    let final_barrier = service
                        .send(&recipient, Command::Snapshot(service.recipient()))
                        .await;
                    let snapshot = service.receive().await;
                    let shutdown = application.lifecycle().request_shutdown();
                    (
                        allocation,
                        actual_allocation,
                        serial,
                        equality,
                        expired,
                        cached,
                        configured.deadline,
                        barrier,
                        drained,
                        final_barrier,
                        snapshot,
                        shutdown,
                    )
                },
            ),
        )
        .unwrap_or_else(|_| panic!("Application owns expiry execution"));
    let (
        ApplicationOutcome::Completed {
            output: observed,
            cleanup: Ok(()),
        },
        Ok((origin, terminal)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = received
    else {
        panic!("complete expiry Work/native/notification custody");
    };
    let (
        allocation,
        actual_allocation,
        serial,
        equality,
        expired,
        cached,
        deadline,
        barrier,
        drained,
        final_barrier,
        snapshot,
        shutdown,
    ) = observed;
    let ActorRetirement::Completed { behavior, .. } = &terminal else {
        panic!("expiry native state");
    };
    assert_eq!(
        behavior.base().originals.len(),
        0,
        "expired cached evidence never admits on retry"
    );
    assert_eq!(behavior.base().prefix, 1_024);
    let Ok(Err(Refusal::Expired(original))) = equality else {
        panic!("deadline equality refuses");
    };
    assert_original(&original, allocation, serial);
    let Err(Refusal::Expired(original)) = expired else {
        panic!("actual sampled Instant refuses expired evidence");
    };
    assert_original(&original, actual_allocation, serial);
    assert_eq!(
        cached,
        Some(deadline),
        "cache checks never extend eligibility"
    );
    assert!(barrier.is_ok());
    assert!(final_barrier.is_ok());
    assert_snapshot(drained, origin.address(), 1_024, 0);
    assert_snapshot(snapshot, origin.address(), 1_024, 0);
    assert_eq!(shutdown, Ok(()));
    assert_completed(RootTerminal::Root { origin, terminal }, None);
}

async fn export_assignment(
    selected: Result<AuthorizedAssignment, StartupRefusal>,
) -> Result<u64, StartupRefusal> {
    let assignment = selected?;
    let received = Application::new(Counter::default().stop_on_shutdown())
        .run_with::<RootTerminal<StopOnShutdown<Counter>>, _, _, _, _, _>(
            |application| async move {
                let mut service = application
                    .interface(())
                    .external::<Replies>()
                    .expect("service");
                let configured = ConfiguredExport::activated(
                    assignment,
                    application.root().established_recipient(),
                    Duration::from_secs(60),
                );
                let serial = configured.assignment.serial();
                let original = request(serial);
                let allocation = original.original.as_ptr() as usize;
                let admitted = configured.attempt(&service, original);
                let barrier = service
                    .send(
                        &configured.recipient,
                        Command::Snapshot(service.recipient()),
                    )
                    .await;
                let snapshot = service.receive().await;
                let shutdown = application.lifecycle().request_shutdown();
                (
                    serial,
                    allocation,
                    service.address(),
                    admitted,
                    barrier,
                    snapshot,
                    shutdown,
                )
            },
        )
        .await
        .unwrap_or_else(|_| panic!("every actual export still cleaned up"));
    let (
        ApplicationOutcome::Completed {
            output: observed,
            cleanup: Ok(()),
        },
        Ok((origin, terminal)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = received
    else {
        panic!("complete startup export native and both notifications");
    };
    let (serial, allocation, sender, admitted, barrier, snapshot, shutdown) = observed;
    let ActorRetirement::Completed { behavior, .. } = &terminal else {
        panic!("native authorized export");
    };
    assert_counter(behavior.base(), &[(sender, allocation, serial)]);
    assert!(admitted.is_ok());
    assert!(barrier.is_ok());
    assert_snapshot(snapshot, origin.address(), 0, 1);
    assert_eq!(shutdown, Ok(()));
    assert_completed(RootTerminal::Root { origin, terminal }, None);
    Ok(serial)
}

#[test]
fn missing_unavailable_production_and_exhausted_authority_refuse_export() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("configured host");
    host.block_on(async {
        let (publication, notification) = oneshot::channel::<()>();
        drop(publication);
        let cause = notification.await.expect_err("actual unavailable source closed");
        let selected = [
            assignment_authority::select(Deployment::ExplicitTest, None),
            assignment_authority::select(Deployment::ExplicitTest, Some(Err(cause))),
            assignment_authority::select(Deployment::ProductionRequired, Some(Ok(Authority::configured(31)))),
        ];
        let mut observed = Vec::new();
        for selection in selected {
            let assignment = selection.and_then(|mut authority| authority.issue(Grant::Counter).map_err(StartupRefusal::Assignment));
            observed.push(export_assignment(assignment).await);
        }
        {
            let mut authority = Authority::configured(u64::MAX);
            for _ in 0..3 {
                let assignment = authority.issue(Grant::Counter).map_err(StartupRefusal::Assignment);
                observed.push(export_assignment(assignment).await);
            }
        }
        // Evaluate cold restart only after the real MAX export's complete native acquisition.
        let cold = assignment_authority::select(Deployment::ExplicitTest, None)
            .and_then(|mut authority| authority.issue(Grant::Counter).map_err(StartupRefusal::Assignment));
        observed.push(export_assignment(cold).await);
        let export_count = observed.iter().filter(|receipt| receipt.is_ok()).count();
        assert_eq!(export_count, 1, "only the final checked assignment may export");
        let [Err(StartupRefusal::MissingAuthority), Err(StartupRefusal::Unavailable(cause)), Err(StartupRefusal::TestAuthorityInProduction),
            Ok(u64::MAX), Err(StartupRefusal::Assignment(AssignmentRefusal::Exhausted)), Err(StartupRefusal::Assignment(AssignmentRefusal::Exhausted)),
            Err(StartupRefusal::MissingAuthority)] = observed.as_slice()
        else { panic!("missing, unavailable, production, exhaustion and cold authority facts stay distinct"); };
        assert_eq!(cause.to_string(), "channel closed");
    });
}
