use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, Never, StopOnShutdown};
use bombay::entity::{
    ActivationId, AdmissionFailure, DrainFailure, Entities, EntityActivationError,
    EntityDefinition, EntityId,
};
use bombay::{ActorRetirement, ActorSpace, ActorSpaces, ApplicationHandle};

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

struct Account;

#[bombay::actor]
impl Account {
    fn receive(&mut self, _: u64) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct Profile;

#[bombay::actor]
impl Profile {
    fn receive(&mut self, _: u64) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[derive(ActorSpaces)]
struct Spaces {
    root: ActorSpace<Root>,
    accounts: ActorSpace<Account>,
    profiles: ActorSpace<Profile>,
}

struct Accounts;
struct Profiles;
struct AccountsRole;
struct ProfilesRole;

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = Spaces;
    type HydrationError = Never;
    type Terminal = Never;

    async fn hydrate(
        &self,
        _: EntityId<Self::Id>,
    ) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Account.stop_on_shutdown())
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
        _: ActorRetirement<Self::Behavior, Self::Terminal>,
    ) {
    }
}

impl EntityDefinition for Profiles {
    type Id = u64;
    type Behavior = StopOnShutdown<Profile>;
    type Hosts = Spaces;
    type HydrationError = Never;
    type Terminal = Never;

    async fn hydrate(
        &self,
        _: EntityId<Self::Id>,
    ) -> Result<Self::Behavior, Self::HydrationError> {
        Ok(Profile.stop_on_shutdown())
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
        _: ActorRetirement<Self::Behavior, Self::Terminal>,
    ) {
    }
}

type Families = (
    ProfilesRole,
    Entities<Profiles>,
    (AccountsRole, Entities<Accounts>, ()),
);

fn exchange(application: ApplicationHandle<Root, Families>) {
    let _: Entities<Profiles> = application.entities(AccountsRole);
}

fn main() {}
