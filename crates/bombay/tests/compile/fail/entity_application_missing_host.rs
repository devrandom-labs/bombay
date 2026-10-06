use bombay::{actors::ActorExt, prelude::StopOnShutdown};
use bombay::behavior::{Actions, BehaviorActed, Never};
use bombay::entity::{
    ActivationId, AdmissionFailure, DrainFailure, EntityActivationError, EntityDefinition,
    EntityId,
};
use bombay::{ActorRetirement, ActorSpace, ActorSpaces};

struct Account;

#[bombay::actor]
impl Account {
    fn receive(&mut self, _: u64) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

#[derive(ActorSpaces)]
struct MissingAccountSpace {
    #[actor_space(Root)]
    root: ActorSpace<Root>,
}

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

struct Accounts;

impl EntityDefinition for Accounts {
    type Id = u64;
    type Behavior = StopOnShutdown<Account>;
    type Hosts = MissingAccountSpace;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = ();

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
        _: EntityActivationError<Self::HydrationError, Self::Behavior, Self::Terminal, Self::ChildFailures>,
    ) {
    }

    fn admission_refused(&self, _: EntityId<Self::Id>, _: AdmissionFailure<u64>) {}

    fn forced_retirement(&self, _: &EntityId<Self::Id>, _: ActivationId, _: DrainFailure) {}

    fn retired(
        &self,
        _: &EntityId<Self::Id>,
        _: ActivationId,
        _: Result<ActorRetirement<Self::Behavior, Self::Terminal, Self::ChildFailures>, tokio::task::JoinError>,
    ) {
    }
}

fn main() {}
