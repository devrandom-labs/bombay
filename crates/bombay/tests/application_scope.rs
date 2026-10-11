//! For learning native Entity installation: `Account` owns domain state,
//! `StopOnShutdown` owns lifecycle policy, Behavior owns typed actions, and the
//! Bombay `Application` owner owns hydration, stable identity, passivation,
//! command-triggered reactivation, and root-first family shutdown. Reactivation
//! is a fresh incarnation after fenced retirement; it is not Behavior Actors'
//! explicit `StableProxy` worker-replacement policy.

use bombay::ActorCustody;
use core::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, BehaviorSettlements, ClassifySettlement, EventLayer,
    Never, Protocol, SettlementStatus,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityShutdown, Passivation,
};
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin, StopOnShutdown};
use bombay::{
    ActorSpace, ActorSpaces, Application, ApplicationBehavior, ApplicationOutcome, ChildFailure,
    ChildOrigin, ProjectTerminal,
};
use tokio::runtime::Builder;
use tokio::sync::Semaphore;

const ACCOUNT_ID: u64 = 7;

enum RootAdmission {
    Ready,
    Refuse { payload: Box<[u8]> },
}
#[derive(Debug, PartialEq)]
enum RootRefusal {
    Initialization,
}
struct Root {
    admission: RootAdmission,
}

#[bombay::actor(message = Never, error = RootRefusal)]
impl Root {
    fn init(&mut self) -> BehaviorActed<Self> {
        match &self.admission {
            RootAdmission::Ready => Ok(Actions::cont()),
            RootAdmission::Refuse { .. } => Err(RootRefusal::Initialization),
        }
    }
}

#[derive(Debug)]
enum AccountCommand {
    Deposit(u64),
}

struct Account {
    balance: u64,
    original: Arc<Vec<u8>>,
}

#[bombay::actor]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Behavior fold returns the typed action algebra"
)]
impl Account {
    fn receive(&mut self, command: AccountCommand) -> BehaviorActed<Self> {
        let AccountCommand::Deposit(amount) = command;
        self.balance = self
            .balance
            .checked_add(amount)
            .expect("the example account balance remains bounded");
        Ok(Actions::cont())
    }
}

type AccountRetirement =
    Result<ActorRetirement<StopOnShutdown<Account>, Never, ()>, tokio::task::JoinError>;

struct Accounts {
    retired: Arc<Semaphore>,
    retirements: Arc<Mutex<Vec<AccountRetirement>>>,
    unexpected_facts: Arc<AtomicUsize>,
}

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = Spaces;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = ();

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn hydrate(&self, _: EntityId<Self::Id>) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Account {
            balance: 0,
            original: Arc::new(vec![0]),
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

    fn retired(&self, _: &EntityId<Self::Id>, _: ActivationId, retirement: AccountRetirement) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(retirement);
        self.retired.add_permits(1);
    }
}

#[derive(Clone, Copy)]
struct AccountsRole;

struct Replies;

impl Protocol for Replies {
    type Addr = MailAddr;
    type Msg = Never;
}

#[derive(ActorSpaces)]
struct Spaces {
    #[actor_space(Root)]
    root: ActorSpace<Root>,
    #[actor_space(Account)]
    accounts: ActorSpace<Account>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "retain complete typed retirements inline in this ownership comparison"
)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: RootOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<R, Self, ColdFailures>,
    },
    Cold {
        origin: ChildOrigin<StopOnShutdown<Root>, ColdAccount>,
        terminal: ActorRetirement<StopOnShutdown<Account>, Never, ()>,
    },
}

impl<R>
    From<(
        RootOrigin<StopOnShutdown<Root>>,
        ActorRetirement<R, Self, ColdFailures>,
    )> for ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn from(
        (origin, terminal): (
            RootOrigin<StopOnShutdown<Root>>,
            ActorRetirement<R, Self, ColdFailures>,
        ),
    ) -> Self {
        Self::Root { origin, terminal }
    }
}
impl<R> ProjectTerminal<ChildOrigin<StopOnShutdown<Root>, ColdAccount>, StopOnShutdown<Account>, ()>
    for ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    type Descendants = Never;
    fn project(
        origin: ChildOrigin<StopOnShutdown<Root>, ColdAccount>,
        terminal: ActorRetirement<StopOnShutdown<Account>, Never, ()>,
    ) -> Self {
        Self::Cold { origin, terminal }
    }
}

enum ColdAccount {
    Declared,
    DisposalRefused,
}
impl Drop for ColdAccount {
    fn drop(&mut self) {
        match self {
            Self::Declared => {}
            Self::DisposalRefused => panic!("original cold role disposal failure"),
        }
    }
}
type ColdFailures = (
    Vec<ChildFailure<ChildOrigin<StopOnShutdown<Root>, ColdAccount>, StopOnShutdown<Account>>>,
    (),
);
type ComposedRoot = ApplicationBehavior<
    StopOnShutdown<Root>,
    bombay::behavior::ChildCons<MailAddr, StopOnShutdown<Account>, bombay::behavior::NoChildren>,
>;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep each complete custody journey observable without hiding caller syntax in another abstraction"
)]
fn cold_members_compose_with_live_entity_families() {
    let retired = Arc::new(Semaphore::new(0));
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let unexpected_facts = Arc::new(AtomicUsize::new(0));
    let definition = Accounts {
        retired: Arc::clone(&retired),
        retirements: Arc::clone(&retirements),
        unexpected_facts: Arc::clone(&unexpected_facts),
    };
    let spaces = Spaces {
        root: ActorSpace::new(),
        accounts: ActorSpace::new(),
    };
    let capacity = EntityCapacity::new(
        NonZeroUsize::new(2).expect("two is non-zero"),
        NonZeroUsize::new(32).expect("32 is non-zero"),
    );
    let application = Application::with_topology(
        Root {
            admission: RootAdmission::Ready,
        }
        .stop_on_shutdown(),
        spaces,
    )
    .child(
        ColdAccount::Declared,
        Account {
            balance: 71,
            original: Arc::new(vec![71]),
        }
        .stop_on_shutdown(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        capacity,
    );

    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous example owns one enabled application host");
    let (outcome, root_receiving, shutdowns) = application_host
        .block_on(
            application.run_with_entities(move |application| async move {
                let accounts = application.entities(AccountsRole);
                let account = accounts.entity(ACCOUNT_ID);
                let interface = application.interface(account.clone());
                let caller = interface
                    .external::<Replies>()
                    .expect("the external caller is established");

                caller
                    .send(interface.api(), AccountCommand::Deposit(40))
                    .await
                    .expect("the first incarnation accepts its command");
                let passivation = application.passivate_entity(AccountsRole, &ACCOUNT_ID);
                assert_eq!(passivation, Passivation::Begun);
                Arc::clone(&retired)
                    .acquire_owned()
                    .await
                    .expect("the first incarnation retires")
                    .forget();
                caller
                    .send(&account, AccountCommand::Deposit(2))
                    .await
                    .expect("the same stable reference activates a replacement");
                let shutdown = application.lifecycle().request_shutdown();
                assert_eq!(shutdown, Ok(()));
            }),
        )
        .unwrap_or_else(|(application, work, error)| {
            drop((application, work));
            panic!("the explicit application host must be entered: {error}");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: (),
        cleanup: Ok(()),
    } = outcome
    else {
        panic!("the original application work completes beside its joined cleanup");
    };
    let (root_origin, joined_actor) =
        root_receiving.expect("the independent original root retirement is acquired");
    let terminal: ApplicationTerminal<_> = (
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    )
        .into();
    let (head_receiving, ()) = shutdowns;
    let (AccountsRole, (shutdown, metrics, family_disposal_failure)) =
        head_receiving.expect("the complete original account family retirement is acquired");

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
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
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
    } = terminal
    else {
        panic!("the application root must stop normally")
    };
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
    assert!(child_failures.0.is_empty());
    assert_eq!(descendants.len(), 1);
    let ApplicationTerminal::Cold {
        origin: child_origin,
        terminal: child_retirement,
    } = descendants
        .into_iter()
        .next()
        .expect("the actual cold account retires")
    else {
        panic!("the exact cold role remains distinguishable")
    };
    assert_ne!(child_origin.address(), origin.address());
    assert_eq!(
        child_origin.creation_kind(),
        bombay::behavior::CreationKind::Birth
    );
    assert_eq!(
        child_retirement
            .behavior()
            .expect("original cold account retained")
            .base()
            .balance,
        71
    );
    assert_eq!(
        child_retirement
            .behavior()
            .expect("original cold account retained")
            .base()
            .original
            .as_slice(),
        [71]
    );
    let custody = child_retirement
        .custody()
        .expect("complete cold account custody retained");
    assert_eq!(custody.descendants.len(), 0);
    assert!(custody.additional_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.terminal_report.is_none());
    assert_eq!(completion, Completion::Stopped);
}

fn assert_retirements(retirements: &Mutex<Vec<AccountRetirement>>) {
    let retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(retirements.len(), 2);
    let balances = retirements
        .iter()
        .map(|retirement| {
            let (behavior, settlements, user, descendants, capability_failures) = match retirement {
                Ok(ActorRetirement::Completed {
                    custody:
                        ActorCustody {
                            child_failures: (),
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
                            child_failures: (),
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

fn original_family() -> Accounts {
    Accounts {
        retired: Arc::new(Semaphore::new(0)),
        retirements: Arc::new(Mutex::new(Vec::new())),
        unexpected_facts: Arc::new(AtomicUsize::new(0)),
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep each complete custody journey observable without hiding caller syntax in another abstraction"
)]
fn combined_staging_rejection_returns_original_family_and_work() {
    let definition = original_family();
    let original_retirements = Arc::clone(&definition.retirements);
    let unexpected = Arc::clone(&definition.unexpected_facts);
    let invoked = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&invoked);
    let payload: Box<[u8]> = Box::from([83, 89, 97]);
    let allocation = payload.as_ptr();
    let application = Application::with_topology(
        Root {
            admission: RootAdmission::Ready,
        }
        .stop_on_shutdown(),
        Spaces {
            root: ActorSpace::new(),
            accounts: ActorSpace::new(),
        },
    )
    .child(
        ColdAccount::DisposalRefused,
        Account {
            balance: 101,
            original: Arc::new(vec![101]),
        }
        .stop_on_shutdown(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        EntityCapacity::new(
            NonZeroUsize::new(2).expect("nonzero"),
            NonZeroUsize::new(32).expect("nonzero"),
        ),
    );
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("actual caller host");
    let (outcome, root, (family, ())) = host
        .block_on(
            application.run_with_entities::<_, _, ApplicationTerminal<ComposedRoot>, _, _, _>(
                move |application| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let requested = application.lifecycle().request_shutdown();
                    assert_eq!(requested, Ok(()));
                    payload
                },
            ),
        )
        .unwrap_or_else(|rejected| {
            drop(rejected);
            panic!("entered host owns original application execution")
        });
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert_eq!(unexpected.load(Ordering::SeqCst), 0);
    assert!(
        original_retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    assert!(root.is_err());
    assert!(family.is_err());
    let ApplicationOutcome::StagingRejected {
        inputs: (root, spaces, (role, definition, directory, capacity, ()), work, failure),
    } = outcome
    else {
        panic!("exact staging rejection survives")
    };
    assert!(Arc::ptr_eq(&definition.retirements, &original_retirements));
    let bombay::ApplicationStagingError::RoleDisposalPanicked {
        id,
        actor,
        children,
        cause,
    } = failure
    else {
        panic!("actual role destructor cause owns original child")
    };
    assert_eq!(actor.base().balance, 101);
    assert_eq!(actor.base().original.as_slice(), [101]);
    drop((id, children, cause));
    let application = Application::with_topology(root, spaces)
        .child(ColdAccount::Declared, actor)
        .entity_family(role, definition, directory, capacity);
    let (outcome, root, (family, ())) = host
        .block_on(
            application.run_with_entities::<_, _, ApplicationTerminal<ComposedRoot>, _, _, _>(work),
        )
        .unwrap_or_else(|rejected| {
            drop(rejected);
            panic!("same original work runs after deliberate role replacement")
        });
    let (returned_payload, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("original recovered work completes")
    });
    assert_eq!(returned_payload.as_ptr(), allocation);
    assert_eq!(&*returned_payload, &[83, 89, 97]);
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
    assert!(cleanup.is_ok());
    let (_, retirement) = root.expect("real root retirement after rerun");
    assert_eq!(
        retirement
            .custody()
            .expect("root custody")
            .descendants
            .len(),
        1
    );
    let (_, (shutdown, metrics, disposal)) = family.expect("real family retirement after rerun");
    assert!(matches!(
        shutdown,
        EntityShutdown::Settled { represented: 0, .. }
    ));
    assert_eq!(metrics.activations, 0);
    assert!(disposal.is_none());
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep each complete custody journey observable without hiding caller syntax in another abstraction"
)]
fn unstarted_combined_execution_returns_original_application_and_work() {
    let definition = original_family();
    let retired = Arc::clone(&definition.retirements);
    let payload: Box<[u8]> = Box::from([103, 107]);
    let allocation = payload.as_ptr();
    let application = Application::with_topology(
        Root {
            admission: RootAdmission::Ready,
        }
        .stop_on_shutdown(),
        Spaces {
            root: ActorSpace::new(),
            accounts: ActorSpace::new(),
        },
    )
    .child(
        ColdAccount::Declared,
        Account {
            balance: 109,
            original: Arc::new(vec![109]),
        }
        .stop_on_shutdown(),
    )
    .entity_family(
        AccountsRole,
        definition,
        DirectoryConfig::default(),
        EntityCapacity::new(
            NonZeroUsize::new(2).expect("nonzero"),
            NonZeroUsize::new(32).expect("nonzero"),
        ),
    );
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("actual caller host");
    let entered = host.enter();
    let (execution, receiving) = application
        .execute_with_entities::<_, _, ApplicationTerminal<ComposedRoot>, _, _, _>(
            move |application| async move {
                let requested = application.lifecycle().request_shutdown();
                assert_eq!(requested, Ok(()));
                payload
            },
        )
        .unwrap_or_else(|rejected| {
            drop(rejected);
            panic!("entered host")
        });
    drop(entered);
    drop(execution);
    let (outcome, root, (family, ())) = host.block_on(receiving);
    assert!(root.is_err());
    assert!(family.is_err());
    assert!(
        retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    let ApplicationOutcome::Unstarted { application, work } = outcome else {
        panic!("complete original cold declaration survives cancellation")
    };
    let (outcome, root, (family, ())) = host
        .block_on(
            application.run_with_entities::<_, _, ApplicationTerminal<ComposedRoot>, _, _, _>(work),
        )
        .unwrap_or_else(|rejected| {
            drop(rejected);
            panic!("same complete original application reruns")
        });
    let (payload, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("original work completes")
    });
    assert_eq!(payload.as_ptr(), allocation);
    assert_eq!(&*payload, &[103, 107]);
    assert!(cleanup.is_ok());
    let (_, retirement) = root.expect("actual root retirement");
    let custody = retirement.custody().expect("actual root custody");
    let [ApplicationTerminal::Cold { terminal, .. }] = custody.descendants.as_slice() else {
        panic!("original cold actor retained once")
    };
    assert_eq!(
        terminal.behavior().expect("cold behavior").base().balance,
        109
    );
    assert_eq!(
        terminal
            .behavior()
            .expect("cold behavior")
            .base()
            .original
            .as_slice(),
        [109]
    );
    let (_, (shutdown, metrics, disposal)) = family.expect("actual family retirement");
    assert!(matches!(
        shutdown,
        EntityShutdown::Settled { represented: 0, .. }
    ));
    assert_eq!(metrics.activations, 0);
    assert!(disposal.is_none());
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep each complete custody journey observable without hiding caller syntax in another abstraction"
)]
fn root_refusal_keeps_original_cold_actor_and_family_shutdown() {
    let root_payload: Box<[u8]> = Box::from([113, 127]);
    let root_allocation = root_payload.as_ptr();
    let child_token = Arc::new(vec![131, 137]);
    let child_retained = Arc::downgrade(&child_token);
    let work_token = Arc::new(vec![139, 149]);
    let work_retained = Arc::downgrade(&work_token);
    let original_work: Box<[u8]> = Box::from([151, 157]);
    let family = original_family();
    let configured = DirectoryConfig::new(
        NonZeroUsize::new(3).expect("positive"),
        NonZeroUsize::new(17).expect("positive"),
    );
    let Err(bombay::entity::DirectoryConfigError::InvalidShardCount {
        shards,
        activation_waiters,
    }) = configured
    else {
        panic!("invalid configuration is rejected before any affine application input transfers");
    };
    assert_eq!(shards.get(), 3);
    assert_eq!(activation_waiters.get(), 17);
    assert_eq!(root_payload.as_ptr(), root_allocation);
    let retirements = Arc::clone(&family.retirements);
    let unexpected = Arc::clone(&family.unexpected_facts);
    let application = Application::with_topology(
        Root {
            admission: RootAdmission::Refuse {
                payload: root_payload,
            },
        }
        .stop_on_shutdown(),
        Spaces {
            root: ActorSpace::new(),
            accounts: ActorSpace::new(),
        },
    )
    .child(
        ColdAccount::Declared,
        Account {
            balance: 163,
            original: child_token,
        }
        .stop_on_shutdown(),
    )
    .entity_family(
        AccountsRole,
        family,
        DirectoryConfig::default(),
        EntityCapacity::new(
            NonZeroUsize::new(2).expect("nonzero"),
            NonZeroUsize::new(32).expect("nonzero"),
        ),
    );
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("actual caller-owned host");
    let (outcome, root, (family, ())) = host
        .block_on(
            application.run_with_entities::<_, _, ApplicationTerminal<ComposedRoot>, _, _, _>(
                move |_| async move {
                    drop((original_work, work_token));
                    panic!("the original refused root never invokes work")
                },
            ),
        )
        .unwrap_or_else(|rejected| {
            drop(rejected);
            panic!("entered caller host")
        });
    let ApplicationOutcome::NotInvoked {
        work,
        startup_error: Some(startup_error),
        cleanup: Ok(()),
    } = outcome
    else {
        panic!("exact uninvoked work and native startup refusal retained")
    };
    let (origin, retirement) = root.expect("actual complete refused root retirement");
    let ActorRetirement::InitializationRejected { custody, error } = retirement else {
        panic!("actual pure root error remains classified")
    };
    assert!(matches!(
        &error,
        bombay::ApplicationDefinitionError::Root(RootRefusal::Initialization)
    ));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let RootAdmission::Refuse { payload } = &custody.behavior.base().admission else {
        panic!("original refusing root state")
    };
    assert_eq!(payload.as_ptr(), root_allocation);
    assert_eq!(payload.as_ref(), vec![113, 127]);
    assert_eq!(custody.descendants.len(), 0);
    assert!(custody.child_failures.0.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert!(custody.unread_owner_cancellation.is_none());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert_eq!(child_retained.strong_count(), 1);
    assert_eq!(
        child_retained
            .upgrade()
            .expect("unstarted original cold account token")
            .as_slice(),
        &[131, 137]
    );
    assert_eq!(work_retained.strong_count(), 1);
    let (_, (shutdown, metrics, disposal)) =
        family.expect("genuine independently joined zero-incarnation family");
    assert!(
        matches!(shutdown, EntityShutdown::Settled { represented: 0, entities } if entities.is_empty())
    );
    assert_eq!(metrics.activations, 0);
    assert!(disposal.is_none());
    assert!(
        retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    assert_eq!(unexpected.load(Ordering::SeqCst), 0);
    drop((custody, error, startup_error));
    assert_eq!(child_retained.strong_count(), 0);
    drop(work);
    assert_eq!(work_retained.strong_count(), 0);
}
