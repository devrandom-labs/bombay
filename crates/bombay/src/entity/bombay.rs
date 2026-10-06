//! Native binding from Entity lifecycle effects to Bombay incarnations.

use core::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Here, Ingress, InjectEvent, Never, Protocol,
};
use behavior_actors::ShutdownRequested;
use communication::Config;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application_runtime::{ApplicationCapabilities, NoParent, StructuralOrigins};
use crate::child_bindings::{ChildBindings, RetireChildTasks, RuntimeChildBindings};
use crate::interpret::{ActionInterpreter, ActionSettlementOf};
use crate::launch::{OwnedActor, SpawnError, spawn_owned_entity_with};
use crate::local::{ActorRef, CommitActions, request_actor_shutdown};
use crate::topology::{HostedActorSpaces, Hosts};

use super::family::{EntityCapacity, EntityDefinition, EntityMetricState, EntityRetirementFailure};
use super::{
    Activated, ActivationId, EntityActivationError, EntityId, FenceFailure, LocalEntityRuntime,
    RetirementMode,
};

const USER_CAPACITY: usize = 1_024;

type NativeEntityCapabilities<B, N, Terminal> = ApplicationCapabilities<
    B,
    HostedActorSpaces<Arc<N>>,
    NoParent,
    ChildBindings<B, Terminal, StructuralOrigins<<B as BehaviorBase>::Base>>,
    StructuralOrigins<<B as BehaviorBase>::Base>,
>;
type NativeEntityInterpreter<B, N, Terminal> =
    ActionInterpreter<NativeEntityCapabilities<B, N, Terminal>>;
type NativeEntityActor<B, Terminal, ChildFailures> = OwnedActor<B, (Vec<Terminal>, ChildFailures)>;

#[diagnostic::on_unimplemented(
    message = "the application host product cannot execute Entity behavior `{B}`",
    label = "missing a required actor host or typed effect interpreter"
)]
pub(crate) trait NativeEntityHost<B, Terminal, ChildFailures>: Send + Sync + Sized
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    fn launch_entity(
        self: Arc<Self>,
        address: MailAddr,
        allocations: ApplicationAddresses,
        behavior: B,
    ) -> impl Future<
        Output = Result<
            NativeEntityActor<B, Terminal, ChildFailures>,
            ActorRetirement<B, Terminal, ChildFailures>,
        >,
    > + Send;
}

impl<B, N, Terminal, ChildFailures> NativeEntityHost<B, Terminal, ChildFailures> for N
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + BehaviorBase
        + Send
        + 'static,
    B::Error: Send + 'static,
    B::InterpretationCustody: Send + 'static,
    B::SourceCustody: Send + 'static,
    B::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    B::Sends: Send + 'static,
    BehaviorMessage<B>: Send + 'static,
    <B::Birth as BirthMode>::Child: ChildOccurrenceProduct<
            RuntimeChildBindings<Terminal, StructuralOrigins<<B as BehaviorBase>::Base>>,
        > + Send
        + 'static,
    N: Hosts<B::Protocol> + Send + Sync + 'static,
    ChildBindings<B, Terminal, StructuralOrigins<<B as BehaviorBase>::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    NativeEntityInterpreter<B, N, Terminal>:
        CommitActions<B, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
    ActionSettlementOf<B>: ClassifySettlement + Send + 'static,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
{
    async fn launch_entity(
        self: Arc<Self>,
        address: MailAddr,
        allocations: ApplicationAddresses,
        behavior: B,
    ) -> Result<
        NativeEntityActor<B, Terminal, ChildFailures>,
        ActorRetirement<B, Terminal, ChildFailures>,
    > {
        let addresses = <N as Hosts<B::Protocol>>::space(&self).clone();
        spawn_owned_entity_with(
            addresses,
            Config::new(USER_CAPACITY),
            address,
            behavior,
            move |control, terminal_reports, timers, observations| {
                ActionInterpreter::new(ApplicationCapabilities::new_with_bindings(
                    crate::application_runtime::ApplicationCapabilityInputs {
                        address,
                        actor_spaces: Arc::new(HostedActorSpaces(self)),
                        allocations,
                        control,
                        timers,
                        observations,
                        terminal_reports,
                    },
                    ChildBindings::<B, Terminal, StructuralOrigins<<B as BehaviorBase>::Base>>::default(),
                ))
            },
        )
        .await
        .map_err(crate::launch::SpawnError::into_retirement)
    }
}

struct HydrationMeasurement {
    metrics: Arc<EntityMetricState>,
}

impl HydrationMeasurement {
    fn begin(metrics: Arc<EntityMetricState>) -> Self {
        metrics.hydration_started();
        Self { metrics }
    }
}

impl Drop for HydrationMeasurement {
    fn drop(&mut self) {
        self.metrics.hydration_finished();
    }
}

pub(crate) struct NativeEntityLease<D>
where
    D: EntityDefinition,
{
    actor: NativeEntityActor<D::Behavior, D::Terminal, D::ChildFailures>,
    resident: OwnedSemaphorePermit,
}

pub(crate) struct BombayEntityRuntime<D>
where
    D: EntityDefinition,
{
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    hydrations: Arc<Semaphore>,
    residents: Arc<Semaphore>,
    metrics: Arc<EntityMetricState>,
}

impl<D> Clone for BombayEntityRuntime<D>
where
    D: EntityDefinition,
{
    fn clone(&self) -> Self {
        Self {
            definition: Arc::clone(&self.definition),
            actors: Arc::clone(&self.actors),
            allocations: self.allocations.clone(),
            hydrations: Arc::clone(&self.hydrations),
            residents: Arc::clone(&self.residents),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

pub(crate) fn bombay_entity_runtime<D>(
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    capacity: EntityCapacity,
    metrics: Arc<EntityMetricState>,
) -> BombayEntityRuntime<D>
where
    D: EntityDefinition,
{
    BombayEntityRuntime {
        definition,
        actors,
        allocations,
        hydrations: Arc::new(Semaphore::new(capacity.concurrent_hydrations().get())),
        residents: Arc::new(Semaphore::new(capacity.residents().get())),
        metrics,
    }
}

impl<D> LocalEntityRuntime<D::Id, BehaviorMessage<D::Behavior>> for BombayEntityRuntime<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    type Origin = MailAddr;
    type Endpoint = ActorRef<<D::Behavior as Behavior>::Protocol>;
    type Lease = NativeEntityLease<D>;
    type ActivationError =
        EntityActivationError<D::HydrationError, D::Behavior, D::Terminal, D::ChildFailures>;
    type Task = tokio::task::JoinHandle<()>;
    type TaskFailure = tokio::task::JoinError;
    type RetirementFailure = EntityRetirementFailure;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        tokio::spawn(task)
    }

    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        task.await
    }

    async fn activate(
        &self,
        entity_id: EntityId<D::Id>,
        _: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        let resident = Arc::clone(&self.residents)
            .try_acquire_owned()
            .map_err(|_| {
                self.metrics.capacity_refused();
                EntityActivationError::ResidentCapacity
            })?;
        let hydration = Arc::clone(&self.hydrations)
            .acquire_owned()
            .await
            .expect("the application-owned hydration semaphore remains open");
        let measurement = HydrationMeasurement::begin(Arc::clone(&self.metrics));
        let behavior = self.definition.hydrate(entity_id).await.map_err(|error| {
            self.metrics.hydration_failed();
            EntityActivationError::Hydration(error)
        })?;
        drop((measurement, hydration));
        let address = match self.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                self.metrics.launch_failed();
                let failure = SpawnError::<D::Behavior, (Vec<D::Terminal>, D::ChildFailures)>::AllocationRejected {
                    behavior,
                    reason,
                };
                return Err(EntityActivationError::Launch(failure.into_retirement()));
            }
        };
        let mut actor = Arc::clone(&self.actors)
            .launch_entity(address, self.allocations.clone(), behavior)
            .await
            .map_err(|retirement| {
                self.metrics.launch_failed();
                EntityActivationError::Launch(retirement)
            })?;
        actor.acknowledge_binding();
        self.metrics.activation_succeeded();
        Ok(Activated {
            endpoint: actor.actor.clone(),
            lease: NativeEntityLease { actor, resident },
        })
    }

    fn activation_failed(
        &self,
        entity_id: EntityId<D::Id>,
        activation_id: ActivationId,
        error: Self::ActivationError,
    ) {
        self.definition
            .activation_failed(entity_id, activation_id, error);
    }

    async fn deliver(
        &self,
        endpoint: Self::Endpoint,
        origin: Self::Origin,
        command: BehaviorMessage<D::Behavior>,
    ) -> Result<(), BehaviorMessage<D::Behavior>> {
        endpoint
            .send_from(origin, command)
            .await
            .map_err(crate::SendError::into_message)
    }

    async fn fence(&self, endpoint: Self::Endpoint) -> Result<(), FenceFailure> {
        endpoint.fence().await
    }

    async fn retire(
        &self,
        entity_id: &EntityId<D::Id>,
        activation_id: ActivationId,
        lease: Self::Lease,
        retirement: RetirementMode,
    ) -> Result<(), Self::RetirementFailure> {
        // Preserve the existing forced notification before shutdown/join. The
        // original lease and identity remain owned outside this user call.
        let forced = match retirement {
            RetirementMode::Graceful => None,
            RetirementMode::Forced(failure) => {
                self.metrics.forced_retirement();
                catch_unwind(AssertUnwindSafe(|| {
                    self.definition
                        .forced_retirement(entity_id, activation_id, failure);
                }))
                .err()
            }
        };
        let actor = lease.actor.actor.clone();
        let requested = catch_unwind(AssertUnwindSafe(|| {
            request_actor_shutdown(&actor, &lease.actor.control, Ingress::new())
        }));
        let shutdown_request = requested.err();
        // Graceful names the successful fence, not guaranteed actor stopping.
        // The same lease owns cancellation and join even when the actor ignores
        // ShutdownRequested. Its factual cause and queued inputs are returned.
        let joined = lease
            .actor
            .task
            .retire()
            .await
            .map(ActorRetirement::from_local);
        // Only the final notification consumes the joined actor result. The
        // original key and resident permit remain owned outside that user call.

        let failure = match joined {
            Ok(joined) => {
                let retired = catch_unwind(AssertUnwindSafe(|| {
                    self.definition
                        .retired(entity_id, activation_id, Ok(joined));
                }))
                .err();
                match (shutdown_request, forced, retired) {
                    (None, None, None) => Ok(()),
                    (Some(shutdown_request), forced, retired) => {
                        Err(EntityRetirementFailure::ShutdownRequestPanicked {
                            shutdown_request,
                            forced,
                            retired,
                        })
                    }
                    (None, Some(forced), retired) => {
                        Err(EntityRetirementFailure::ForcedRetirementPanicked { forced, retired })
                    }
                    (None, None, Some(retired)) => {
                        Err(EntityRetirementFailure::RetirementPanicked { retired })
                    }
                }
            }
            Err(failure) => {
                let retired = catch_unwind(AssertUnwindSafe(|| {
                    self.definition
                        .retired(entity_id, activation_id, Err(failure));
                }))
                .err();
                Err(EntityRetirementFailure::ActorRetirementUnavailable {
                    shutdown_request,
                    forced,
                    retired,
                })
            }
        };
        // Preserve the original successful final-notification -> metrics ->
        // resident-release ordering. Caught failures also complete this release.
        self.metrics.retired();
        drop(lease.resident);
        failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::Termination;
    use behavior::{ActiveTurn, BehaviorActed, NoBirths, NoSends, User, UserEvent};
    use behavior_actors::StopOnShutdown;
    use core::num::{NonZeroU64, NonZeroUsize};
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::Mutex;

    use crate::actors::ActorExt;
    use crate::entity::{AdmissionFailure, DrainFailure, DrainStage};
    use crate::launch::ActorSpace;

    struct ShutdownConversionActor {
        entries: Arc<Vec<u64>>,
    }

    struct ShutdownConversionEvent(User<MailAddr, Never>);

    impl UserEvent for ShutdownConversionEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(from: MailAddr, message: Never) -> Self {
            Self(User::new(from, message))
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Ok(self.0)
        }
    }

    impl InjectEvent<ShutdownRequested, Here> for ShutdownConversionEvent {
        fn inject_at(_: ShutdownRequested) -> Self {
            panic!("original application shutdown conversion panic");
        }
    }

    impl Protocol for ShutdownConversionActor {
        type Addr = MailAddr;
        type Msg = Never;
    }

    impl Behavior for ShutdownConversionActor {
        type Protocol = Self;
        type Event = ShutdownConversionEvent;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.0.message {}
        }
    }

    impl BehaviorBase for ShutdownConversionActor {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    struct ShutdownConversionDefinition {
        entries: Arc<Vec<u64>>,
        endpoint: Mutex<Option<ActorRef<ShutdownConversionActor>>>,
        #[expect(
            clippy::type_complexity,
            reason = "The forced notification retains its original identity, endpoint, reason, and live termination observation."
        )]
        forced: Mutex<
            Vec<(
                EntityId<u64>,
                ActivationId,
                DrainFailure,
                ActorRef<ShutdownConversionActor>,
                Poll<Termination<MailAddr>>,
            )>,
        >,
        #[expect(
            clippy::type_complexity,
            reason = "The retired notification owns the original identity and complete actor retirement or raw task failure."
        )]
        retirement: Mutex<
            Option<(
                EntityId<u64>,
                ActivationId,
                Result<ActorRetirement<ShutdownConversionActor, Never, ()>, tokio::task::JoinError>,
            )>,
        >,
    }

    impl EntityDefinition for ShutdownConversionDefinition {
        type Id = u64;
        type Behavior = ShutdownConversionActor;
        type Hosts = ActorSpace<ShutdownConversionActor>;
        type HydrationError = Never;
        type Terminal = Never;
        type ChildFailures = ();
        fn hydrate(
            &self,
            _: EntityId<u64>,
        ) -> impl Future<Output = Result<Self::Behavior, Never>> + Send {
            let entries = Arc::clone(&self.entries);
            async move { Ok(ShutdownConversionActor { entries }) }
        }
        fn activation_failed(
            &self,
            _: EntityId<u64>,
            _: ActivationId,
            _: EntityActivationError<Never, Self::Behavior, Never, ()>,
        ) {
            panic!("the real native activation must succeed");
        }
        fn admission_refused(&self, _: EntityId<u64>, _: AdmissionFailure<Never>) {
            panic!("the conversion witness emits no user message");
        }
        fn forced_retirement(
            &self,
            id: &EntityId<u64>,
            activation: ActivationId,
            reason: DrainFailure,
        ) {
            let endpoint = self
                .endpoint
                .lock()
                .expect("outside callback input owner")
                .take()
                .expect("original actual activated endpoint");
            let observation = {
                let mut termination = pin!(endpoint.termination());
                let mut context = Context::from_waker(Waker::noop());
                termination.as_mut().poll(&mut context)
            };
            self.forced
                .lock()
                .expect("outside callback observation owner")
                .push((*id, activation, reason, endpoint, observation));
        }
        fn retired(
            &self,
            id: &EntityId<u64>,
            activation: ActivationId,
            retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, tokio::task::JoinError>,
        ) {
            let prior = self
                .retirement
                .lock()
                .expect("outside application callback owner")
                .replace((*id, activation, retirement));
            assert!(prior.is_none());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "The complete original and native conversion traces join their actor owners before the custody oracles."
    )]
    async fn shutdown_conversion_fault_keeps_the_same_actor_owner_until_join() {
        let inputs = Arc::new(vec![17_u64, 43]);
        let allocation = inputs.as_ptr();
        let addresses = ApplicationAddresses::new();
        let address = addresses.allocate().expect("real original actor address");
        let spaces = Arc::new(ActorSpace::<ShutdownConversionActor>::new());
        let original = Arc::clone(&spaces)
            .launch_entity(
                address,
                addresses.clone(),
                ShutdownConversionActor {
                    entries: Arc::clone(&inputs),
                },
            )
            .await;
        let Ok(mut original) = original else {
            panic!("real original actor is privately committed")
        };
        original.acknowledge_binding();
        // The original production conversion, while its exact actor owner remains
        // outside this narrowly caught application call. No fabricated JoinError.
        let rejected_conversion = catch_unwind(AssertUnwindSafe(|| {
            request_actor_shutdown(&original.actor, &original.control, Ingress::new())
        }));
        let original = original
            .task
            .retire()
            .await
            .map(ActorRetirement::<ShutdownConversionActor, Never, ()>::from_local);
        let Ok(ActorRetirement::OwnerCancelled {
            behavior,
            settlements,
            control,
            user,
            descendants,
            child_failures,
            capability_failures,
            unread_owner_cancellation,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
        }) = original
        else {
            panic!("the surviving original affine owner joins its exact actor")
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
        assert!(rejected_conversion.is_err());
        assert_eq!(behavior.entries.as_ptr(), allocation);
        assert_eq!(behavior.entries.as_slice(), [17, 43]);
        assert!(
            settlements.is_empty()
                && control.is_empty()
                && user.is_empty()
                && descendants.is_empty()
        );
        assert_eq!(child_failures, ());
        assert!(capability_failures.is_empty() && unread_owner_cancellation.is_none());

        let definition = Arc::new(ShutdownConversionDefinition {
            entries: inputs,
            endpoint: Mutex::new(None),
            forced: Mutex::new(Vec::new()),
            retirement: Mutex::new(None),
        });
        let capacity = EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN);
        let runtime = bombay_entity_runtime(
            Arc::clone(&definition),
            spaces,
            addresses,
            capacity,
            Arc::new(EntityMetricState::default()),
        );
        let id = EntityId::new(59);
        let activation = ActivationId::new(NonZeroU64::MIN);
        let activated = runtime.activate(id, activation).await;
        let Ok(activated) = activated else {
            panic!("actual native lease is acquired")
        };
        let address = activated.endpoint.address();
        let prior = definition
            .endpoint
            .lock()
            .expect("actual external callback input")
            .replace(activated.endpoint);
        assert!(prior.is_none());
        let reason = DrainFailure {
            stage: DrainStage::Retirement,
            outstanding_reservations: 0,
        };
        let retired = runtime
            .retire(
                &id,
                activation,
                activated.lease,
                RetirementMode::Forced(reason),
            )
            .await;
        let acquired = definition
            .retirement
            .lock()
            .expect("callback follows exact actor join")
            .take();
        // Every lease/actor operation is settled before the final failure oracle.
        let Some((
            returned_id,
            returned_activation,
            Ok(ActorRetirement::OwnerCancelled {
                behavior,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                interpretation,
                source,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            }),
        )) = acquired
        else {
            panic!("actual native callback receives its complete result")
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
        assert_eq!(returned_id, id);
        assert_eq!(returned_activation, activation);
        assert_eq!(behavior.entries.as_ptr(), allocation);
        assert_eq!(behavior.entries.as_slice(), [17, 43]);
        assert!(
            settlements.is_empty()
                && control.is_empty()
                && user.is_empty()
                && descendants.is_empty()
        );
        assert_eq!(child_failures, ());
        assert!(capability_failures.is_empty() && unread_owner_cancellation.is_none());
        assert_eq!(runtime.residents.available_permits(), 1);
        assert_eq!(runtime.hydrations.available_permits(), 1);
        let Err(EntityRetirementFailure::ShutdownRequestPanicked {
            shutdown_request: _,
            forced: None,
            retired: None,
        }) = retired
        else {
            panic!("the original conversion panic is independently retained")
        };
        let forced = definition
            .forced
            .lock()
            .expect("callback and join are complete");
        let [(forced_id, forced_activation, forced_reason, endpoint, Poll::Pending)] =
            forced.as_slice()
        else {
            panic!("the forced notification observes the actual live actor before shutdown/join");
        };
        assert_eq!(forced_id, &id);
        assert_eq!(*forced_activation, activation);
        assert_eq!(*forced_reason, reason);
        assert_eq!(endpoint.address(), address);
    }

    struct JoinedActor;

    #[crate::actor(message = Never)]
    impl JoinedActor {}

    struct JoinDefinition;

    impl EntityDefinition for JoinDefinition {
        type Id = u64;
        type Behavior = StopOnShutdown<JoinedActor>;
        type Hosts = ActorSpace<JoinedActor>;
        type HydrationError = Never;
        type Terminal = Never;
        type ChildFailures = ();

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn hydrate(
            &self,
            _: EntityId<Self::Id>,
        ) -> Result<Self::Behavior, Self::HydrationError> {
            Ok(JoinedActor.stop_on_shutdown())
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
            unreachable!("the join regression never activates an entity")
        }

        fn admission_refused(
            &self,
            _: EntityId<Self::Id>,
            _: AdmissionFailure<BehaviorMessage<Self::Behavior>>,
        ) {
            unreachable!("the join regression never admits a command")
        }

        fn forced_retirement(&self, _: &EntityId<Self::Id>, _: ActivationId, _: DrainFailure) {
            unreachable!("the join regression never retires an entity")
        }

        fn retired(
            &self,
            _: &EntityId<Self::Id>,
            _: ActivationId,
            _: Result<
                ActorRetirement<Self::Behavior, Self::Terminal, Self::ChildFailures>,
                tokio::task::JoinError,
            >,
        ) {
            unreachable!("the join regression never retires an entity")
        }
    }

    #[tokio::test]
    async fn native_join_preserves_a_panicked_task_failure() {
        let mut task = tokio::spawn(async {
            panic!("the native lifecycle task fails");
        });
        let outcome =
            <BombayEntityRuntime<JoinDefinition> as LocalEntityRuntime<u64, Never>>::join(
                &mut task,
            )
            .await;

        let Err(failure) = outcome else {
            panic!("the native join must preserve the failed task")
        };
        assert!(failure.is_panic());
    }
}
