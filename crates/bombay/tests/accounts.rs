//! For learning native Entity installation: `Account` owns domain state,
//! `StopOnShutdown` owns lifecycle policy, Behavior owns typed actions, and the
//! advanced Bombay `Application` boundary owns hydration, stable identity, passivation,
//! command-triggered reactivation, and root-first family shutdown. Reactivation
//! is a fresh incarnation after fenced retirement; it is not Behavior Actors'
//! explicit `StableProxy` worker-replacement policy.

use bombay::ActorCustody;
use core::future::{Future, poll_fn};
use core::num::NonZeroUsize;
use core::pin::pin;
use core::task::Poll;
use std::panic::resume_unwind;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, BehaviorSettlements, ClassifySettlement, EventLayer, Never, Protocol,
    SettlementStatus,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityRetirementError, EntityRetirementFailure,
    EntityShutdown, RetirementMode,
};
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin, StopOnShutdown};
use bombay::{ActorFailureAssessment, ActorRetirementReport, Application, RetirementAssessment};
use tokio::runtime::Builder;
use tokio::sync::Semaphore;

const ACCOUNT_ID: u64 = 7;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

mod account {
    include!("../../../examples/entity/src/account.rs");
}

use account::{Account, AccountCommand};

type AccountRetirement =
    Result<ActorRetirement<StopOnShutdown<Account>, Never, Vec<Never>>, tokio::task::JoinError>;

enum RetirementDelivery {
    Stored,
    Held {
        entered: mpsc::SyncSender<()>,
        released: Mutex<mpsc::Receiver<()>>,
    },
    Panicked(Mutex<Option<Box<u64>>>),
}

struct Accounts {
    initial_balance: u64,
    delivery: RetirementDelivery,
    retired: Arc<Semaphore>,
    retirements: Arc<Mutex<Vec<(AccountRetirement, ActorRetirementReport)>>>,
    unexpected_facts: Arc<AtomicUsize>,
}

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = account_application::Topology;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = Vec<Never>;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn hydrate(&self, _: EntityId<Self::Id>) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Account {
            balance: self.initial_balance,
        }
        .stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        _: EntityId<Self::Id>,
        _: ActivationId,
        _: EntityActivationError<
            Self::HydrationError,
            Self::Behavior,
            Self::Terminal,
            Self::ChildFailures,
        >,
    ) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn admission_refused(&self, _: EntityId<Self::Id>, _: AdmissionFailure<AccountCommand>) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn forced_retirement(&self, _: &EntityId<Self::Id>, _: ActivationId, _: DrainFailure) {
        self.unexpected_facts.fetch_add(1, Ordering::Relaxed);
    }

    fn retired(
        &self,
        _: &EntityId<Self::Id>,
        _: ActivationId,
        retirement: AccountRetirement,
        retirement_report: ActorRetirementReport,
    ) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((retirement, retirement_report));
        match &self.delivery {
            RetirementDelivery::Stored => {}
            RetirementDelivery::Held { entered, released } => {
                entered
                    .send(())
                    .expect("the retirement observer remains live");
                released
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .recv()
                    .expect("the test releases the actual retirement callback");
            }
            RetirementDelivery::Panicked(cause) => {
                let cause = cause
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .expect("this test owns one actual failing callback");
                resume_unwind(cause);
            }
        }
        self.retired.add_permits(1);
    }
}

#[derive(Clone, Copy)]
struct AccountsRole;

bombay::topology! {
    account_application {
        root: Root {}
        hosts { accounts: Account; }
    }
}

#[test]
fn accounts_passivate_reactivate_and_preserve_complete_retirement() {
    let retired = Arc::new(Semaphore::new(0));
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let unexpected_facts = Arc::new(AtomicUsize::new(0));
    let definition = Accounts {
        initial_balance: 0,
        delivery: RetirementDelivery::Stored,
        retired: Arc::clone(&retired),
        retirements: Arc::clone(&retirements),
        unexpected_facts: Arc::clone(&unexpected_facts),
    };
    let spaces = account_application::Topology::new();
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two is non-zero"),
        NonZeroUsize::new(32).expect("32 is non-zero"),
    );
    let application = Application::with_topology(Root.stop_on_shutdown(), spaces).entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    );

    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous example owns one enabled application host");
    let outcome = application_host
        .block_on(
            application.run_with_entities(move |application| async move {
                let accounts = application.entities(AccountsRole);
                let account = accounts.entity(ACCOUNT_ID);
                let caller = application
                    .sender_to(account)
                    .unwrap_or_else(|_| panic!("the account sender is established"));

                caller
                    .send(AccountCommand::Deposit(40))
                    .await
                    .expect("the first incarnation accepts its command");
                let retirement = application.retire_entity(AccountsRole, &ACCOUNT_ID).await;
                assert_eq!(retirement, Ok(bombay::entity::RetirementMode::Graceful));
                caller
                    .send(AccountCommand::Deposit(2))
                    .await
                    .expect("the same stable reference activates a replacement");
                let shutdown = application.shutdown();
                assert_eq!(shutdown, Ok(()));
            }),
        )
        .unwrap_or_else(|(application, work, error)| {
            drop((application, work));
            panic!("the explicit application host must be entered: {error}");
        });
    let ((), retirement) = outcome.try_into_completed().unwrap_or_else(|_| {
        panic!("the original application work completes beside its joined cleanup");
    });
    let (cleanup, _root_report, root_receiving, notification_receiving, shutdowns) =
        retirement.into_parts();
    cleanup.expect("the original application cleanup succeeds");
    let notifications = notification_receiving
        .expect("the existing cleanup owner transferred both original notifications");
    notifications
        .termination
        .expect("ordinary root termination notification succeeded");
    notifications
        .retirement
        .expect("joined root report notification succeeded");
    let (root_origin, joined_actor) =
        root_receiving.expect("the independent original root retirement is acquired");
    let terminal = (
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    let (head_receiving, ()) = shutdowns;
    let (AccountsRole, family_retirement) =
        head_receiving.expect("the complete original account family retirement is acquired");
    let (shutdown, metrics, family_disposal_failure) = family_retirement.into_parts();

    assert!(family_disposal_failure.is_none());
    let EntityShutdown::Settled {
        represented,
        entities: returned_entities,
    } = shutdown
    else {
        panic!("application family must settle");
    };
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![EntityId::new(ACCOUNT_ID)]);
    assert_eq!(metrics.activations, 2);
    assert_eq!(metrics.residents, 0);
    assert_eq!(unexpected_facts.load(Ordering::Relaxed), 0);
    assert_application_stopped(terminal);
    assert_retirements(&retirements);
    assert_eq!(retired.available_permits(), 2);
}

fn assert_application_stopped<R>(terminal: (RootOrigin<R>, ActorRetirement<R, Never, Vec<Never>>))
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let (
        origin,
        ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    control,
                    user,
                    descendants,
                    behavior,
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
        },
    ) = terminal
    else {
        panic!("the application root must stop normally")
    };
    assert_eq!(child_failures, []);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants, []);
    assert_eq!(completion, Completion::Stopped);
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete fenced-retirement trace preserves both phase-specific custody oracles"
)]
fn assert_retirements(retirements: &Mutex<Vec<(AccountRetirement, ActorRetirementReport)>>) {
    let retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(retirements.len(), 2);
    let balances = retirements
        .iter()
        .map(|(retirement, report)| {
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            let (behavior, settlements, user, descendants, capability_failures) = match retirement {
                Ok(ActorRetirement::Completed {
                    custody:
                        ActorCustody {
                            child_failures,
                            capability_failures,
                            unread_owner_cancellation: None | Some(()),
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
                }) => {
                    assert_eq!(child_failures, &[]);
                    assert!(interpretation.is_none());
                    assert!(source.is_none());
                    assert!(additional_failures.is_empty());
                    assert!(received_interpretation.is_none());
                    assert!(received_source.is_none());
                    assert!(source_index.is_none());
                    assert!(acquired_ingress.is_none());
                    assert!(retirement_failures.is_empty());
                    assert!(terminal_report.is_none());
                    assert_eq!(*completion, Completion::Stopped);
                    assert!(control.is_empty());
                    // Natural stopping may precede acquisition of the owner request;
                    // either original unit presence remains in the retained result.
                    (
                        behavior,
                        settlements,
                        user,
                        descendants,
                        capability_failures,
                    )
                }
                Ok(ActorRetirement::OwnerCancelled {
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
                    interpretation,
                    source,
                }) => {
                    assert_eq!(child_failures, &[]);
                    assert!(interpretation.is_none());
                    assert!(source.is_none());
                    assert!(additional_failures.is_empty());
                    assert!(received_interpretation.is_none());
                    assert!(received_source.is_none());
                    assert!(source_index.is_none());
                    assert!(acquired_ingress.is_none());
                    assert!(retirement_failures.is_empty());
                    assert!(terminal_report.is_none());
                    assert!(unread_owner_cancellation.is_none());
                    assert!(matches!(control.as_slice(), [EventLayer::Owned(_)]));
                    (
                        behavior,
                        settlements,
                        user,
                        descendants,
                        capability_failures,
                    )
                }
                _ => panic!("each fenced account incarnation must return its joined state"),
            };
            assert!(capability_failures.is_empty());
            assert!(user.is_empty());
            assert_eq!(descendants.len(), 0);
            let settlement_status = settlements.settlement_status();
            assert_eq!(settlement_status, SettlementStatus::Accepted);
            behavior.base().balance
        })
        .collect::<Vec<_>>();
    assert_eq!(balances, [40, 2]);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one native trace checks the callback barrier, three observers, cancellation and an unrelated live incarnation"
)]
fn entity_retirement_waits_for_callback_and_preserves_independent_observers() {
    let (entered, entering) = mpsc::sync_channel(4);
    let (release, released) = mpsc::sync_channel(4);
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let records = Arc::clone(&retirements);
    let definition = Accounts {
        initial_balance: 0,
        delivery: RetirementDelivery::Held {
            entered,
            released: Mutex::new(released),
        },
        retired: Arc::new(Semaphore::new(0)),
        retirements,
        unexpected_facts: Arc::new(AtomicUsize::new(0)),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two"),
        NonZeroUsize::new(32).expect("32"),
    );
    let application = Application::with_topology(
        Root.stop_on_shutdown(),
        account_application::Topology::new(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    );
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the caller owns its host");
    let entering = &entering;
    let outcome = host
        .block_on(application.run_with_entities(|application| async move {
            let accounts = application.entities(AccountsRole);
            let first = application
                .sender_to(accounts.entity(ACCOUNT_ID))
                .unwrap_or_else(|_| panic!("first sender"));
            let other = application
                .sender_to(accounts.entity(8))
                .unwrap_or_else(|_| panic!("other sender"));
            first
                .send(AccountCommand::Deposit(40))
                .await
                .expect("first deposit");
            other
                .send(AccountCommand::Deposit(3))
                .await
                .expect("other deposit");
            let mut first_wait = pin!(application.retire_entity(AccountsRole, &ACCOUNT_ID));
            poll_fn(|cx| {
                let status = first_wait.as_mut().poll(cx);
                assert!(matches!(status, Poll::Pending));
                Poll::Ready(())
            })
            .await;
            entering
                .recv_timeout(Duration::from_secs(2))
                .expect("the actual retirement callback begins");
            let mut second_wait = pin!(application.retire_entity(AccountsRole, &ACCOUNT_ID));
            poll_fn(|cx| {
                let status = second_wait.as_mut().poll(cx);
                assert!(matches!(status, Poll::Pending));
                let first_status = first_wait.as_mut().poll(cx);
                assert!(matches!(first_status, Poll::Pending));
                Poll::Ready(())
            })
            .await;
            {
                let mut cancelled_wait = pin!(application.retire_entity(AccountsRole, &ACCOUNT_ID));
                poll_fn(|cx| {
                    let status = cancelled_wait.as_mut().poll(cx);
                    assert!(matches!(status, Poll::Pending));
                    Poll::Ready(())
                })
                .await;
            }
            release.send(()).expect("the callback is released");
            let first_mode = first_wait.await;
            let second_mode = second_wait.await;
            assert_eq!(first_mode, Ok(RetirementMode::Graceful));
            assert_eq!(second_mode, first_mode);
            let absent = application.retire_entity(AccountsRole, &ACCOUNT_ID).await;
            let Err(refusal) = absent else {
                panic!("no incarnation remains active");
            };
            let reason = match refusal {
                EntityRetirementError::NotActive => "not active",
                EntityRetirementError::ShuttingDown => "shutting down",
            };
            assert_eq!(reason, "not active");
            other
                .send(AccountCommand::Deposit(4))
                .await
                .expect("the unrelated incarnation remains live");
            release
                .send(())
                .expect("release the unrelated account's eventual retirement");
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
        }))
        .unwrap_or_else(|_| panic!("the application starts"));
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    assert!(retirement.cleanup().is_ok());
    let family = retirement
        .family(AccountsRole)
        .expect("the complete family is acquired");
    assert!(matches!(
        family.shutdown(),
        EntityShutdown::Settled { represented: 1, .. }
    ));
    let records = records.lock().unwrap_or_else(PoisonError::into_inner);
    let balances = records
        .iter()
        .map(|(retirement, _)| {
            retirement
                .as_ref()
                .expect("native join")
                .behavior()
                .expect("retained account")
                .base()
                .balance
        })
        .collect::<Vec<_>>();
    assert_eq!(balances, [40, 7]);
}

#[test]
fn entity_retirement_keeps_callback_failure_in_the_family_outcome() {
    let cause = Box::new(571_u64);
    let allocation = ptr::from_ref(cause.as_ref()).cast::<()>();
    let definition = Accounts {
        initial_balance: 0,
        delivery: RetirementDelivery::Panicked(Mutex::new(Some(cause))),
        retired: Arc::new(Semaphore::new(0)),
        retirements: Arc::new(Mutex::new(Vec::new())),
        unexpected_facts: Arc::new(AtomicUsize::new(0)),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one"),
        NonZeroUsize::new(2).expect("two"),
    );
    let application = Application::with_topology(
        Root.stop_on_shutdown(),
        account_application::Topology::new(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    );
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its host");
    let outcome = host
        .block_on(application.run_with_entities(|application| async move {
            let caller = application
                .sender_to(application.entities(AccountsRole).entity(ACCOUNT_ID))
                .unwrap_or_else(|_| panic!("the sender exists"));
            caller
                .send(AccountCommand::Deposit(40))
                .await
                .expect("deposit");
            let mode = application.retire_entity(AccountsRole, &ACCOUNT_ID).await;
            assert_eq!(mode, Ok(RetirementMode::Graceful));
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
        }))
        .unwrap_or_else(|_| panic!("the application starts"));
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    assert!(retirement.cleanup().is_ok());
    let family = retirement
        .family(AccountsRole)
        .expect("the complete family is acquired");
    let EntityShutdown::Unsettled {
        represented,
        entities,
        failures,
    } = family.shutdown()
    else {
        panic!("the original callback failure must remain acquired");
    };
    assert_eq!(*represented, 0);
    assert!(failures.is_empty());
    assert_eq!(entities.len(), 1);
    let (id, disposition, original_failures) = &entities[0];
    assert_eq!(id, &EntityId::new(ACCOUNT_ID));
    assert_eq!(disposition, &Ok(()));
    assert_eq!(original_failures.len(), 1);
    let (
        _,
        mode,
        EntityRetirementFailure::RetirementPanicked {
            retired,
            termination_notification,
        },
    ) = &original_failures[0]
    else {
        panic!("the exact callback cause must remain independent of completion");
    };
    assert_eq!(*mode, RetirementMode::Graceful);
    assert_eq!(ptr::from_ref(retired.as_ref()).cast::<()>(), allocation);
    assert!(termination_notification.is_ok());
}

#[test]
fn entity_retirement_preserves_the_forced_fence_reason() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let definition = Accounts {
        initial_balance: u64::MAX,
        delivery: RetirementDelivery::Stored,
        retired: Arc::new(Semaphore::new(0)),
        retirements: Arc::clone(&records),
        unexpected_facts: Arc::new(AtomicUsize::new(0)),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one"),
        NonZeroUsize::new(2).expect("two"),
    );
    let application = Application::with_topology(
        Root.stop_on_shutdown(),
        account_application::Topology::new(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    );
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the caller owns its host");
    let outcome = host
        .block_on(application.run_with_entities(|application| async move {
            let caller = application
                .sender_to(application.entities(AccountsRole).entity(ACCOUNT_ID))
                .unwrap_or_else(|_| panic!("the sender exists"));
            caller
                .send(AccountCommand::Deposit(1))
                .await
                .expect("the exact command is admitted");
            let mode = application
                .retire_entity(AccountsRole, &ACCOUNT_ID)
                .await
                .expect("the actual incarnation ends");
            let RetirementMode::Forced(reason) = mode else {
                panic!("an unacknowledged fence cannot be graceful");
            };
            assert_eq!(reason.outstanding_reservations, 0);
            assert!(matches!(
                reason.stage,
                bombay::entity::DrainStage::FenceEnqueue
                    | bombay::entity::DrainStage::FenceAcknowledgement
            ));
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
            reason
        }))
        .unwrap_or_else(|_| panic!("the application starts"));
    let (reason, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    assert!(retirement.cleanup().is_ok());
    let family = retirement
        .family(AccountsRole)
        .expect("the family remains acquired");
    assert!(matches!(
        family.shutdown(),
        EntityShutdown::Settled { represented: 0, .. }
    ));
    let records = records.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        &records[0].0,
        Ok(ActorRetirement::TransitionPanicked { .. })
    ));
    assert_eq!(
        records[0].1.failures(),
        ActorFailureAssessment::FailuresFound
    );
    assert_eq!(records[0].1.retirement(), RetirementAssessment::Established);
    assert!(matches!(
        reason.stage,
        bombay::entity::DrainStage::FenceEnqueue | bombay::entity::DrainStage::FenceAcknowledgement
    ));
}

#[test]
fn reactivated_entity_cannot_reuse_the_previous_retirement_fact() {
    let (entered, entering) = mpsc::sync_channel(2);
    let (release, released) = mpsc::sync_channel(2);
    let records = Arc::new(Mutex::new(Vec::new()));
    let definition = Accounts {
        initial_balance: 0,
        delivery: RetirementDelivery::Held {
            entered,
            released: Mutex::new(released),
        },
        retired: Arc::new(Semaphore::new(0)),
        retirements: Arc::clone(&records),
        unexpected_facts: Arc::new(AtomicUsize::new(0)),
    };
    let application = Application::with_topology(
        Root.stop_on_shutdown(),
        account_application::Topology::new(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        EntityCapacity::new(
            NonZeroUsize::new(1).expect("one"),
            NonZeroUsize::new(2).expect("two"),
        ),
    );
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the caller owns its host");
    let entering = &entering;
    let outcome = host
        .block_on(application.run_with_entities(|application| async move {
            let caller = application
                .sender_to(application.entities(AccountsRole).entity(ACCOUNT_ID))
                .unwrap_or_else(|_| panic!("the stable sender exists"));
            for amount in [40, 2] {
                caller
                    .send(AccountCommand::Deposit(amount))
                    .await
                    .expect("the actual incarnation accepts its command");
                let mut waiting = pin!(application.retire_entity(AccountsRole, &ACCOUNT_ID));
                poll_fn(|cx| {
                    let status = waiting.as_mut().poll(cx);
                    assert!(matches!(status, Poll::Pending));
                    Poll::Ready(())
                })
                .await;
                entering
                    .recv_timeout(Duration::from_secs(2))
                    .expect("this incarnation's actual callback begins");
                poll_fn(|cx| {
                    let status = waiting.as_mut().poll(cx);
                    assert!(matches!(status, Poll::Pending));
                    Poll::Ready(())
                })
                .await;
                release.send(()).expect("release this exact retirement");
                let mode = waiting.await;
                assert_eq!(mode, Ok(RetirementMode::Graceful));
            }
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
        }))
        .unwrap_or_else(|_| panic!("the application starts"));
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work completes"));
    assert!(retirement.cleanup().is_ok());
    assert!(
        matches!(retirement.family(AccountsRole).expect("the family is acquired").shutdown(),
        EntityShutdown::Settled { represented: 0, entities } if entities.is_empty())
    );
    let records = records.lock().unwrap_or_else(PoisonError::into_inner);
    let balances = records
        .iter()
        .map(|(retirement, _)| {
            retirement
                .as_ref()
                .expect("original native join")
                .behavior()
                .expect("retained account")
                .base()
                .balance
        })
        .collect::<Vec<_>>();
    assert_eq!(balances, [40, 2]);
}

mod account_reports {
    include!("../../../examples/entity/src/accounts.rs");

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{ACCOUNT_ID, AccountsRole, Root};
        use bombay::Application;
        use bombay::entity::{
            DirectoryConfig, DrainStage, EntityCapacity, EntityShutdown, Refusal, RetirementMode,
        };
        use core::num::{NonZeroU64, NonZeroUsize};

        #[test]
        fn callbacks_retain_complete_account_values_instead_of_counting_and_discarding_them() {
            let reports = Arc::new(Mutex::new(Vec::new()));
            let definition = Accounts {
                reports: Arc::clone(&reports),
            };
            let attempt =
                ActivationId::new(NonZeroU64::new(17).expect("the protocol attempt is nonzero"));
            definition.activation_failed(
                EntityId::new(73),
                attempt,
                EntityActivationError::AllocationRejected {
                    behavior: Account { balance: 431 }.stop_on_shutdown(),
                    reason: bombay::behavior::AllocationRejection::Exhausted,
                },
            );
            definition.admission_refused(
                EntityId::new(74),
                AdmissionFailure::Refused {
                    command: AccountCommand::Deposit(57),
                    reason: Refusal::Draining,
                },
            );
            let reason = DrainFailure {
                stage: DrainStage::Reservations,
                outstanding_reservations: 3,
            };
            definition.forced_retirement(&EntityId::new(75), attempt, reason);
            let reports = reports.lock().unwrap_or_else(PoisonError::into_inner);
            let [
                AccountReport::Startup {
                    id,
                    attempt: returned,
                    failure:
                        EntityActivationError::AllocationRejected {
                            behavior,
                            reason: rejection,
                        },
                },
                AccountReport::Admission {
                    id: rejected_id,
                    rejection:
                        AdmissionFailure::Refused {
                            command: AccountCommand::Deposit(amount),
                            reason: refusal,
                        },
                },
                AccountReport::Drain {
                    id: drained_id,
                    attempt: drained_attempt,
                    reason: drained,
                },
            ] = reports.as_slice()
            else {
                panic!("all complete callback inputs must remain owned in order");
            };
            assert_eq!(id, &EntityId::new(73));
            assert_eq!(*returned, attempt);
            assert_eq!(behavior.base().balance, 431);
            assert_eq!(rejection, &bombay::behavior::AllocationRejection::Exhausted);
            assert_eq!(rejected_id, &EntityId::new(74));
            assert_eq!(*amount, 57);
            assert_eq!(*refusal, Refusal::Draining);
            assert_eq!(drained_id, &EntityId::new(75));
            assert_eq!(*drained_attempt, attempt);
            assert_eq!(*drained, reason);
        }
        #[test]
        #[expect(
            clippy::too_many_lines,
            reason = "one native journey checks the actual family policy, fresh reactivation and both complete callback retirements"
        )]
        fn native_account_reports_preserve_both_retired_incarnations() {
            let reports = Arc::new(Mutex::new(Vec::new()));
            let definition = Accounts {
                reports: Arc::clone(&reports),
            };
            let capacity = EntityCapacity::new(
                NonZeroUsize::new(2).expect("two concurrent hydrations"),
                NonZeroUsize::new(32).expect("32 residents"),
            );
            let host = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the native host starts");
            let outcome = host
                .block_on(
                    Application::with_topology(
                        Root.stop_on_shutdown(),
                        account_application::Topology::new(),
                    )
                    .entity_family(
                        AccountsRole,
                        definition,
                        DirectoryConfig::default(),
                        capacity,
                    )
                    .run_with_entities(|application| async move {
                        let caller = application
                            .sender_to(application.entities(AccountsRole).entity(ACCOUNT_ID))
                            .unwrap_or_else(|_| panic!("the actual account sender starts"));
                        caller
                            .send(AccountCommand::Deposit(40))
                            .await
                            .expect("first deposit admitted");
                        let mode = application.retire_entity(AccountsRole, &ACCOUNT_ID).await;
                        assert_eq!(mode, Ok(RetirementMode::Graceful));
                        caller
                            .send(AccountCommand::Deposit(2))
                            .await
                            .expect("fresh incarnation admits deposit");
                        let shutdown = application.shutdown();
                        assert_eq!(shutdown, Ok(()));
                    }),
                )
                .unwrap_or_else(|_| panic!("the application starts"));
            let ((), retirement) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("the work completes beside original cleanup"));
            assert!(retirement.cleanup().is_ok());
            let family = retirement
                .family(AccountsRole)
                .expect("the original family is acquired");
            assert!(family.disposal_failure().is_none());
            assert_eq!(family.metrics().activations, 2);
            assert_eq!(family.metrics().residents, 0);
            let EntityShutdown::Settled {
                represented,
                entities,
            } = family.shutdown()
            else {
                panic!("the actual family must settle");
            };
            assert_eq!(*represented, 1);
            assert_eq!(entities, &vec![EntityId::new(ACCOUNT_ID)]);
            let records = reports.lock().unwrap_or_else(PoisonError::into_inner);
            let [
                AccountReport::Retirement {
                    id: first_id,
                    attempt: first_attempt,
                    result: first,
                    report: first_report,
                },
                AccountReport::Retirement {
                    id: second_id,
                    attempt: second_attempt,
                    result: second,
                    report: second_report,
                },
            ] = records.as_slice()
            else {
                panic!("both complete native retirements must reach the actual policy");
            };
            assert_eq!(first_id, &EntityId::new(ACCOUNT_ID));
            assert_eq!(second_id, first_id);
            assert_ne!(first_attempt, second_attempt);
            assert_eq!(
                first
                    .as_ref()
                    .expect("first native join succeeds")
                    .behavior()
                    .expect("first actor custody is retained")
                    .base()
                    .balance,
                40
            );
            assert_eq!(
                second
                    .as_ref()
                    .expect("second native join succeeds")
                    .behavior()
                    .expect("second actor custody is retained")
                    .base()
                    .balance,
                2
            );
            assert_eq!(
                first_report.retirement(),
                bombay::RetirementAssessment::Established
            );
            assert_eq!(
                second_report.retirement(),
                bombay::RetirementAssessment::Established
            );
        }
    }
}
