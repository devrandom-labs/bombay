use core::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use behavior_actors::StopOnShutdown;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, CreateChild, CreationSequence, Creations,
    InterpreterRequests, Never, Protocol,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DirectoryError, DrainFailure, DrainStage,
    EntityActivationError, EntityAdmission, EntityCapacity, EntityDefinition, EntityId, EntityRef,
    Passivation, Refusal,
};
use bombay::{
    ActorOrigin, ActorRetirement, ActorSpace, ActorSpaces, App, MailAddr, TerminalProjection,
};
use tokio::sync::Semaphore;

mod application_support;

use application_support::{RootTerminal, assert_completed};

const FIRST_ACCOUNT: u64 = 20;
const SECOND_ACCOUNT: u64 = 21;
const CAPACITY_REFUSAL: u64 = 22;
const HYDRATION_REFUSAL: u64 = 13;
const LAUNCH_REFUSAL: u64 = 14;
const FORCED_RETIREMENT: u64 = 23;
const STOP_ACCOUNT: u64 = 67;
const PROFILE_CHILD_NONCE: u64 = 1;

enum RootCommand {
    Admit(EntityRef<Profiles>, u64),
}

struct Root;

#[bombay::actor(
    sends = { entity_admissions: InterpreterRequests<EntityAdmission<Profiles>> },
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the Behavior fold returns the typed action algebra"
)]
impl Root {
    fn receive(&mut self, command: RootCommand) -> BehaviorActed<Self> {
        let RootCommand::Admit(profile, value) = command;
        Ok(Actions::stop().send_entity_admissions(profile.request(value)))
    }
}

#[derive(Clone, Copy)]
enum AccountStart {
    Ready,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountError {
    Initialization,
}

struct Account {
    start: AccountStart,
}

#[bombay::actor(error = AccountError)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "the Behavior fold owns the command and returns the typed action algebra"
)]
impl Account {
    fn init(&mut self) -> BehaviorActed<Self> {
        match self.start {
            AccountStart::Ready => Ok(Actions::cont()),
            AccountStart::Reject => Err(AccountError::Initialization),
        }
    }

    fn receive(&mut self, command: u64) -> BehaviorActed<Self> {
        match command {
            STOP_ACCOUNT => Ok(Actions::stop()),
            _ => Ok(Actions::cont()),
        }
    }
}

struct ProfileWorker;

#[bombay::actor(message = Never)]
impl ProfileWorker {}

struct Profile {
    admissions: usize,
}

#[bombay::actor(
    births = { worker: StopOnShutdown<ProfileWorker> },
)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::unused_self,
    reason = "the Behavior fold owns the command and returns the typed action algebra"
)]
impl Profile {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let worker = creations.issue().expect("the first creation ID exists");
        let worker_nonce = worker.get();
        assert_eq!(worker_nonce, PROFILE_CHILD_NONCE);
        Ok(Actions::create(Creations::one(CreateChild::birth(
            worker,
            ProfileWorker.stop_on_shutdown(),
        ))))
    }

    fn receive(&mut self, _: u64) -> BehaviorActed<Self> {
        self.admissions += 1;
        Ok(Actions::cont())
    }
}

#[derive(TerminalProjection)]
enum ProfileTerminal {
    Worker {
        origin: ActorOrigin<Profile, ProfileChildrenWorker>,
        terminal: ActorRetirement<StopOnShutdown<ProfileWorker>, Self>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HydrationFailure {
    Unavailable,
}

type AccountActivationFailure =
    EntityActivationError<HydrationFailure, StopOnShutdown<Account>, Never>;
type AccountRetirement = ActorRetirement<StopOnShutdown<Account>, Never>;
type AccountActivationFacts =
    Arc<Mutex<Vec<(EntityId<u64>, ActivationId, AccountActivationFailure)>>>;
type ForcedRetirementFacts = Arc<Mutex<Vec<(EntityId<u64>, ActivationId, DrainFailure)>>>;
type AccountRetirementFacts = Arc<Mutex<Vec<(EntityId<u64>, ActivationId, AccountRetirement)>>>;

#[derive(Clone)]
struct Accounts {
    hydrations_started: Arc<AtomicUsize>,
    hydration_releases: Arc<Semaphore>,
    retirements_completed: Arc<Semaphore>,
    activation_failures: AccountActivationFacts,
    forced_retirements: ForcedRetirementFacts,
    retirements: AccountRetirementFacts,
}

impl Accounts {
    fn new() -> Self {
        Self {
            hydrations_started: Arc::new(AtomicUsize::new(0)),
            hydration_releases: Arc::new(Semaphore::new(0)),
            retirements_completed: Arc::new(Semaphore::new(0)),
            activation_failures: Arc::new(Mutex::new(Vec::new())),
            forced_retirements: Arc::new(Mutex::new(Vec::new())),
            retirements: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = Spaces;
    type HydrationError = HydrationFailure;
    type Terminal = Never;

    async fn hydrate(
        &self,
        id: EntityId<Self::Id>,
    ) -> Result<Self::Behavior, Self::HydrationError> {
        match *id.get() {
            FIRST_ACCOUNT | SECOND_ACCOUNT => {
                let sequence = self.hydrations_started.fetch_add(1, Ordering::Release) + 1;
                match sequence {
                    1 | 2 => {
                        Arc::clone(&self.hydration_releases)
                            .acquire_owned()
                            .await
                            .expect("the test-owned hydration semaphore remains open")
                            .forget();
                    }
                    _ => {}
                }
                Ok(Account {
                    start: AccountStart::Ready,
                }
                .stop_on_shutdown())
            }
            HYDRATION_REFUSAL => Err(HydrationFailure::Unavailable),
            LAUNCH_REFUSAL => Ok(Account {
                start: AccountStart::Reject,
            }
            .stop_on_shutdown()),
            _ => Ok(Account {
                start: AccountStart::Ready,
            }
            .stop_on_shutdown()),
        }
    }

    fn activation_failed(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        failure: AccountActivationFailure,
    ) {
        self.activation_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, activation, failure));
    }

    fn admission_refused(&self, _: EntityId<Self::Id>, _: AdmissionFailure<u64>) {}

    fn forced_retirement(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        self.forced_retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, activation, failure));
    }

    fn retired(
        &self,
        id: EntityId<Self::Id>,
        activation: ActivationId,
        retirement: AccountRetirement,
    ) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, activation, retirement));
        self.retirements_completed.add_permits(1);
    }
}

type ProfileRetirement = ActorRetirement<StopOnShutdown<Profile>, ProfileTerminal>;
type ProfileRetirementFact = Arc<Mutex<Option<ProfileRetirement>>>;

struct Profiles {
    retirement: ProfileRetirementFact,
}

impl Profiles {
    fn new() -> Self {
        Self {
            retirement: Arc::new(Mutex::new(None)),
        }
    }
}

impl EntityDefinition for Profiles {
    type Id = u64;
    type Behavior = StopOnShutdown<Profile>;
    type Hosts = Spaces;
    type HydrationError = Never;
    type Terminal = ProfileTerminal;

    async fn hydrate(&self, _: EntityId<Self::Id>) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Profile { admissions: 0 }.stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        _: EntityId<Self::Id>,
        _: ActivationId,
        _: EntityActivationError<Self::HydrationError, Self::Behavior, Self::Terminal>,
    ) {
    }

    fn admission_refused(&self, _: EntityId<Self::Id>, _: AdmissionFailure<u64>) {}

    fn forced_retirement(&self, _: EntityId<Self::Id>, _: ActivationId, _: DrainFailure) {}

    fn retired(
        &self,
        _: EntityId<Self::Id>,
        _: ActivationId,
        retirement: ActorRetirement<Self::Behavior, Self::Terminal>,
    ) {
        *self
            .retirement
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(retirement);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AccountsRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfilesRole;

struct Replies;

impl Protocol for Replies {
    type Addr = MailAddr;
    type Msg = Never;
}

#[derive(ActorSpaces)]
struct Spaces {
    root: ActorSpace<Root>,
    accounts: ActorSpace<Account>,
    profiles: ActorSpace<Profile>,
    profile_workers: ActorSpace<ProfileWorker>,
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end journey asserts the complete native Entity composition law"
)]
fn application_runs_two_native_entity_families() -> Result<(), DirectoryError<u64>> {
    let spaces = Spaces {
        root: ActorSpace::new(),
        accounts: ActorSpace::new(),
        profiles: ActorSpace::new(),
        profile_workers: ActorSpace::new(),
    };
    let account_capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one is non-zero"),
        NonZeroUsize::new(2).expect("two is non-zero"),
    );
    let profile_capacity = EntityCapacity::new(
        NonZeroUsize::new(1).expect("one is non-zero"),
        NonZeroUsize::new(1).expect("one is non-zero"),
    );
    let accounts = Accounts::new();
    let hydrations_started = Arc::clone(&accounts.hydrations_started);
    let hydration_releases = Arc::clone(&accounts.hydration_releases);
    let retirements_completed = Arc::clone(&accounts.retirements_completed);
    let activation_failures = Arc::clone(&accounts.activation_failures);
    let forced_retirements = Arc::clone(&accounts.forced_retirements);
    let retirements = Arc::clone(&accounts.retirements);
    let profiles = Profiles::new();
    let profile_retirement = Arc::clone(&profiles.retirement);
    let application = App::new(Root.stop_on_shutdown(), spaces)
        .entity_family(
            AccountsRole,
            accounts,
            DirectoryConfig::default(),
            account_capacity,
        )?
        .entity_family(
            ProfilesRole,
            profiles,
            DirectoryConfig::default(),
            profile_capacity,
        )?;
    let ((), terminal, shutdowns): (_, RootTerminal<_>, _) = application
        .run_with_entities(move |application| async move {
            let accounts = application.entities(AccountsRole);
            let profiles = application.entities(ProfilesRole);
            let first = accounts.entity(FIRST_ACCOUNT);
            let second = accounts.entity(SECOND_ACCOUNT);
            let profile = profiles.entity(9);
            let root = application.root().established_recipient();
            let interface =
                application.interface((root, first.clone(), second.clone(), profile.clone()));
            let caller = interface
                .external::<Replies>()
                .expect("the application establishes one external actor");

            let release_hydrations = async {
                while hydrations_started.load(Ordering::Acquire) < 1 {
                    tokio::task::yield_now().await;
                }
                let first_peak = hydrations_started.load(Ordering::Acquire);
                assert_eq!(first_peak, 1);
                hydration_releases.add_permits(1);
                while hydrations_started.load(Ordering::Acquire) < 2 {
                    tokio::task::yield_now().await;
                }
                hydration_releases.add_permits(1);
            };
            let (first_admission, second_admission, ()) = tokio::join!(
                caller.send(&interface.api().1, 41),
                caller.send(&interface.api().2, 43),
                release_hydrations,
            );
            require_admitted(first_admission, "first account");
            require_admitted(second_admission, "second account");
            require_admitted(caller.send(&interface.api().3, 73).await, "profile");

            assert_unavailable(
                caller.send(&accounts.entity(CAPACITY_REFUSAL), 47).await,
                47,
            );
            assert_eq!(
                application.passivate_entity(AccountsRole, &FIRST_ACCOUNT),
                Passivation::Begun
            );
            assert_eq!(
                application.passivate_entity(AccountsRole, &SECOND_ACCOUNT),
                Passivation::Begun
            );
            Arc::clone(&retirements_completed)
                .acquire_many_owned(2)
                .await
                .expect("both passivated accounts retire")
                .forget();
            assert_unavailable(
                caller.send(&accounts.entity(HYDRATION_REFUSAL), 53).await,
                53,
            );
            assert_unavailable(caller.send(&accounts.entity(LAUNCH_REFUSAL), 59).await, 59);
            require_admitted(caller.send(&first, 61).await, "reactivated account");
            let forced = accounts.entity(FORCED_RETIREMENT);
            require_admitted(caller.send(&forced, STOP_ACCOUNT).await, "stopping account");
            assert_eq!(
                application.passivate_entity(AccountsRole, &FORCED_RETIREMENT),
                Passivation::Begun
            );
            Arc::clone(&retirements_completed)
                .acquire_owned()
                .await
                .expect("the forced account retirement completes")
                .forget();
            caller
                .send(&interface.api().0, RootCommand::Admit(profile, 97))
                .await
                .expect("the actor-originated command enters the root mailbox");
        })
        .expect("the root and both native families settle");

    let (ProfilesRole, (profile_shutdown, profile_metrics), tail) = shutdowns;
    let (AccountsRole, (account_shutdown, account_metrics), ()) = tail;
    assert_eq!(profile_shutdown.represented, 1);
    assert_eq!(account_shutdown.represented, 1);
    assert_eq!(profile_metrics.activations, 1);
    assert_eq!(account_metrics.activations, 4);
    assert_eq!(account_metrics.hydration_failures, 1);
    assert_eq!(account_metrics.launch_failures, 1);
    assert_eq!(account_metrics.capacity_refusals, 1);
    assert_eq!(account_metrics.forced_retirements, 1);
    assert_eq!(account_metrics.peak_hydrations, 1);
    assert_eq!(profile_metrics.residents, 0);
    assert_eq!(account_metrics.residents, 0);
    let profile_retirement = profile_retirement
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
        .expect("the profile definition receives its exact retirement");
    let ActorRetirement::Completed {
        behavior,
        mut descendants,
        ..
    } = profile_retirement
    else {
        panic!("the profile must retire normally")
    };
    assert_eq!(behavior.base().admissions, 2);
    assert_eq!(descendants.len(), 1);
    let ProfileTerminal::Worker {
        origin,
        terminal: child_retirement,
    } = descendants
        .pop()
        .expect("the profile retains its child terminal");
    assert_eq!(origin.nonce(), Some(PROFILE_CHILD_NONCE));
    match child_retirement {
        ActorRetirement::OwnerCancelled { .. } => {}
        _ => panic!("the profile child must preserve owner-cancellation custody"),
    }

    let failures = activation_failures
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    assert_eq!(failures.len(), 3);
    assert_eq!(failures[0].0, EntityId::new(CAPACITY_REFUSAL));
    assert!(matches!(
        &failures[0].2,
        EntityActivationError::ResidentCapacity
    ));
    assert_eq!(failures[1].0, EntityId::new(HYDRATION_REFUSAL));
    assert!(matches!(
        &failures[1].2,
        EntityActivationError::Hydration(HydrationFailure::Unavailable)
    ));
    assert_eq!(failures[2].0, EntityId::new(LAUNCH_REFUSAL));
    assert!(matches!(
        &failures[2].2,
        EntityActivationError::Launch(ActorRetirement::InitializationRejected {
            error: AccountError::Initialization,
            ..
        })
    ));
    drop(failures);

    let forced = forced_retirements
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    assert_eq!(forced.len(), 1);
    assert_eq!(forced[0].0, EntityId::new(FORCED_RETIREMENT));
    assert_eq!(forced[0].2.stage, DrainStage::FenceEnqueue);
    assert_eq!(forced[0].2.outstanding_reservations, 0);
    drop(forced);

    let retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(retirements.len(), 4);
    let completed = retirements
        .iter()
        .all(|(_, _, retirement)| matches!(retirement, ActorRetirement::Completed { .. }));
    assert!(completed);
    let mut retired_ids = retirements
        .iter()
        .map(|(id, _, _)| *id.get())
        .collect::<Vec<_>>();
    retired_ids.sort_unstable();
    assert_eq!(
        retired_ids,
        [
            FIRST_ACCOUNT,
            FIRST_ACCOUNT,
            SECOND_ACCOUNT,
            FORCED_RETIREMENT,
        ]
    );
    assert_completed(terminal);
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the regression deliberately consumes the exact owned admission outcome"
)]
fn require_admitted(result: Result<(), AdmissionFailure<u64>>, target: &str) {
    match result {
        Ok(()) => {}
        Err(AdmissionFailure::Refused { command, reason }) => {
            panic!("the {target} command {command} was refused as {reason:?}")
        }
        Err(AdmissionFailure::ActivationIdsExhausted(command)) => {
            panic!("the {target} command {command} exhausted activation identities")
        }
        Err(AdmissionFailure::DispatchIdsExhausted(command)) => {
            panic!("the {target} command {command} exhausted dispatch identities")
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the regression deliberately consumes the exact rejected command"
)]
fn assert_unavailable(result: Result<(), AdmissionFailure<u64>>, expected_command: u64) {
    let Err(AdmissionFailure::Refused { command, reason }) = result else {
        panic!("the activation must return its rejected command")
    };
    assert_eq!(command, expected_command);
    assert_eq!(reason, Refusal::Unavailable);
}
