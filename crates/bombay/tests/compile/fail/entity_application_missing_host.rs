use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, Never, StopOnShutdown};
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

fn main() {}
