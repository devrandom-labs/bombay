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
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ActorRetirement;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::launch::{OwnedActor, SpawnError, spawn_owned_entity_with};
use crate::local::children::StructuralOrigins;
use crate::local::children::{ChildBindings, RetireChildTasks, RuntimeChildBindings};
use crate::local::effects::CommitActions;
use crate::local::effects::{ActionInterpreter, ActionSettlementOf};
use crate::local::effects::{ApplicationCapabilities, NoParent};
use crate::local::endpoint::{ActorRef, request_actor_shutdown};
use crate::topology::Hosts;

use super::family::{EntityCapacity, EntityDefinition, EntityMetricState, EntityRetirementFailure};
use super::{
    Activated, ActivationId, EntityActivationError, EntityId, FenceFailure, LocalEntityRuntime,
    RetirementMode,
};

const USER_CAPACITY: usize = 1_024;

type NativeEntityCapabilities<B, N, Terminal> = ApplicationCapabilities<
    B,
    Arc<N>,
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
            SpawnError<B, (Vec<Terminal>, ChildFailures)>,
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
        SpawnError<B, (Vec<Terminal>, ChildFailures)>,
    > {
        let addresses = <N as Hosts<B::Protocol>>::space(&self).clone();
        spawn_owned_entity_with(
            addresses,
            Config::new(USER_CAPACITY),
            address,
            behavior,
            move |control, terminal_reports, timers, observations| {
                ActionInterpreter::new(ApplicationCapabilities::new_with_bindings(
                    crate::local::effects::ApplicationCapabilityInputs {
                        address,
                        actor_spaces: Arc::new(self),
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
    executor: Handle,
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
            executor: self.executor.clone(),
        }
    }
}

pub(crate) fn bombay_entity_runtime<D>(
    definition: Arc<D>,
    actors: Arc<D::Hosts>,
    allocations: ApplicationAddresses,
    capacity: EntityCapacity,
    metrics: Arc<EntityMetricState>,
    executor: Handle,
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
        executor,
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
        self.executor.spawn(task)
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
                return Err(EntityActivationError::from_launch(
                    SpawnError::AllocationRejected { behavior, reason },
                ));
            }
        };
        let mut actor = Arc::clone(&self.actors)
            .launch_entity(address, self.allocations.clone(), behavior)
            .await
            .map_err(|failure| {
                self.metrics.launch_failed();
                EntityActivationError::from_launch(failure)
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
        let (joined, termination_notification) = lease.actor.task.retire().await;
        let joined = joined.map(ActorRetirement::from_local);
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
                    (None, None, None) => termination_notification.map_err(|error| {
                        EntityRetirementFailure::TerminationNotificationFailed { error }
                    }),
                    (Some(shutdown_request), forced, retired) => {
                        Err(EntityRetirementFailure::ShutdownRequestPanicked {
                            termination_notification,
                            shutdown_request,
                            forced,
                            retired,
                        })
                    }
                    (None, Some(forced), retired) => {
                        Err(EntityRetirementFailure::ForcedRetirementPanicked {
                            termination_notification,
                            forced,
                            retired,
                        })
                    }
                    (None, None, Some(retired)) => {
                        Err(EntityRetirementFailure::RetirementPanicked {
                            termination_notification,
                            retired,
                        })
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
                    termination_notification,
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
    use crate::RetirementNotificationError;
    use crate::termination::Termination;
    use behavior::{
        Actions, ActiveTurn, AllocationRejection, BehaviorActed, NoBirths, NoSends, Step, User,
        UserEvent,
    };
    use behavior_actors::{Crash, Exit, StopOnShutdown};
    use bombay_engine::Completion;
    use core::num::{NonZeroU64, NonZeroUsize};
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::any::Any;
    use std::panic::resume_unwind;
    use std::ptr;
    use std::sync::Mutex;
    use std::task::Wake;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::JoinError;
    use tokio::task::spawn_blocking;

    use crate::actors::ActorExt;
    use crate::entity::family::assert_activation_metrics;
    use crate::entity::{AdmissionFailure, DrainFailure, DrainStage, EntityMetrics};
    use crate::launch::ActorSpace;

    #[derive(Default)]
    struct DeliveryLedger {
        received: Vec<User<MailAddr, Vec<u64>>>,
    }

    #[crate::actor]
    impl DeliveryLedger {
        fn receive(&mut self, from: MailAddr, values: Vec<u64>) -> BehaviorActed<Self> {
            self.received.push(User::new(from, values));
            Ok(Actions::cont())
        }
    }

    struct LedgerDefinition {
        #[expect(
            clippy::type_complexity,
            reason = "The runtime owner retains one whole native actor retirement without an extra wrapper."
        )]
        retired: Mutex<
            Option<
                oneshot::Sender<
                    Result<ActorRetirement<StopOnShutdown<DeliveryLedger>, Never, ()>, JoinError>,
                >,
            >,
        >,
    }

    impl EntityDefinition for LedgerDefinition {
        type Id = u64;
        type Behavior = StopOnShutdown<DeliveryLedger>;
        type Hosts = ActorSpace<DeliveryLedger>;
        type HydrationError = Never;
        type Terminal = Never;
        type ChildFailures = ();

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Hydration retains its cold future boundary."
        )]
        async fn hydrate(&self, id: EntityId<u64>) -> Result<Self::Behavior, Never> {
            assert_eq!(id.into_inner(), 73);
            Ok(DeliveryLedger::default().stop_on_shutdown())
        }

        fn activation_failed(
            &self,
            _: EntityId<u64>,
            _: ActivationId,
            _: EntityActivationError<Never, Self::Behavior, Never, ()>,
        ) {
            panic!("the native ledger activation must succeed")
        }

        fn admission_refused(&self, _: EntityId<u64>, _: AdmissionFailure<Vec<u64>>) {
            panic!("the native ledger commands must be admitted")
        }

        fn forced_retirement(&self, _: &EntityId<u64>, _: ActivationId, _: DrainFailure) {
            panic!("the native ledger must drain gracefully")
        }

        fn retired(
            &self,
            id: &EntityId<u64>,
            activation: ActivationId,
            retirement: Result<ActorRetirement<Self::Behavior, Never, ()>, JoinError>,
        ) {
            assert_eq!((*id).into_inner(), 73);
            assert_eq!(activation.get().get(), 83);
            let sender = self
                .retired
                .lock()
                .expect("retirement custody")
                .take()
                .expect("one native retirement");
            let published = sender.send(retirement);
            assert!(published.is_ok());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both original delivery allocations, actual stop, and complete native joined retirement in one trace."
    )]
    async fn native_entity_wrapper_and_direct_hosts_retain_exact_actor_retirement() {
        let hosts = Arc::new(ActorSpace::<DeliveryLedger>::new());
        let native_hosts = Arc::clone(&hosts);
        let (retired_sender, retired_receiver) = oneshot::channel();
        let definition = Arc::new(LedgerDefinition {
            retired: Mutex::new(Some(retired_sender)),
        });
        let metrics = Arc::new(EntityMetricState::default());
        let runtime = bombay_entity_runtime(
            Arc::clone(&definition),
            Arc::clone(&hosts),
            ApplicationAddresses::new(),
            EntityCapacity::new(
                NonZeroUsize::new(1).expect("hydration capacity"),
                NonZeroUsize::new(1).expect("resident capacity"),
            ),
            Arc::clone(&metrics),
            Handle::current(),
        );
        let activation = ActivationId::new(NonZeroU64::new(83).expect("activation"));
        let activated = runtime.activate(EntityId::new(73), activation).await;
        let Ok(Activated { endpoint, lease }) = activated else {
            panic!("real native activation must establish an actor")
        };
        let committed = runtime.fence(endpoint.clone()).await;
        assert!(committed.is_ok());
        let address = endpoint.address();
        let native_endpoint = native_hosts
            .space()
            .resolve(&address)
            .map(|actor| actor.as_ref().clone())
            .expect("native endpoint");
        let direct_endpoint = hosts
            .space()
            .resolve(&address)
            .expect("direct native endpoint")
            .as_ref()
            .clone();
        let from = MailAddr(97);
        let native_values = vec![101, 103];
        let native_allocation = native_values.as_ptr();
        let native_delivery = runtime
            .deliver(native_endpoint.clone(), from, native_values)
            .await;
        assert!(native_delivery.is_ok());
        let direct_values = vec![107, 109];
        let direct_allocation = direct_values.as_ptr();
        let direct_delivery = direct_endpoint.send_from(from, direct_values).await;
        assert!(direct_delivery.is_ok());
        let fenced = runtime.fence(endpoint.clone()).await;
        assert!(fenced.is_ok());
        // The current native port owns cancellation as well as shutdown. Keep
        // the original Completed oracle by observing real stop before retiring its lease.
        let requested = request_actor_shutdown(&endpoint, &lease.actor.control, Ingress::new());
        assert_eq!(requested, Ok(()));
        let actual_stopped = endpoint.termination().await;
        assert_eq!(actual_stopped, Ok(Exit::Normal));
        let retired = runtime
            .retire(
                &EntityId::new(73),
                activation,
                lease,
                RetirementMode::Graceful,
            )
            .await;
        assert!(retired.is_ok());
        let retirement = retired_receiver.await.expect("whole native retirement");
        let Ok(ActorRetirement::Completed {
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            additional_failures,
            child_failures,
            capability_failures,
            unread_owner_cancellation,
        }) = retirement
        else {
            panic!("native shutdown must preserve completed state")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert_eq!(child_failures, ());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(control.len(), 0);
        assert_eq!(user, []);
        assert_eq!(descendants, []);
        assert_eq!(settlements.len(), 1);
        for settlement in settlements {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            assert!(matches!(settlement.become_, Step::Stop(_)));
        }
        let stopped = endpoint.termination().await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let ledger = behavior.base();
        assert_eq!(ledger.received.len(), 2);
        assert_eq!(ledger.received[0], User::new(from, vec![101, 103]));
        assert_eq!(ledger.received[0].message.as_ptr(), native_allocation);
        assert_eq!(ledger.received[1], User::new(from, vec![107, 109]));
        assert_eq!(ledger.received[1].message.as_ptr(), direct_allocation);
        let absent_native = native_hosts.space().resolve(&address);
        let absent_direct = hosts.space().resolve(&address);
        assert!(absent_native.is_none());
        assert!(absent_direct.is_none());
        for captured in [native_endpoint, direct_endpoint, endpoint] {
            let values = vec![113, 127];
            let allocation = values.as_ptr();
            let rejected = captured.send_from(from, values).await;
            let original = rejected
                .expect_err("retired native endpoint stays closed")
                .into_message();
            assert_eq!(original, [113, 127]);
            assert_eq!(original.as_ptr(), allocation);
        }
    }

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

    struct TerminationWaiter {
        cause: Mutex<Option<Box<dyn Any + Send>>>,
    }

    impl Wake for TerminationWaiter {
        fn wake(self: Arc<Self>) {
            let cause = self.cause.lock().expect("one actual waiter cause").take();
            if let Some(cause) = cause {
                resume_unwind(cause);
            }
        }
    }

    struct ShutdownConversionDefinition {
        retirement_fault: Mutex<Option<Box<dyn Any + Send>>>,
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
            let fault = self
                .retirement_fault
                .lock()
                .expect("one original callback fault")
                .take();
            if let Some(fault) = fault {
                resume_unwind(fault);
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "The complete original and native conversion traces join their actor owners before the custody oracles."
    )]
    async fn shutdown_conversion_fault_keeps_the_same_actor_owner_until_join() {
        let executor = Handle::current();
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
        let (original, termination_notification) = original.task.retire().await;
        let original =
            original.map(ActorRetirement::<ShutdownConversionActor, Never, ()>::from_local);
        assert!(termination_notification.is_ok());
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

        let notification_values = Arc::new(vec![67_u64, 71]);
        let notification_owner = Arc::downgrade(&notification_values);
        let notification_fault: Box<dyn Any + Send> = Box::new(notification_values);
        let notification_allocation = ptr::from_ref(notification_fault.as_ref()).cast::<()>();
        let retirement_fault: Box<dyn Any + Send> = Box::new(Arc::new(vec![73_u64, 79]));
        let retirement_allocation = ptr::from_ref(retirement_fault.as_ref()).cast::<()>();
        let waiter = Waker::from(Arc::new(TerminationWaiter {
            cause: Mutex::new(Some(notification_fault)),
        }));
        let definition = Arc::new(ShutdownConversionDefinition {
            retirement_fault: Mutex::new(Some(retirement_fault)),
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
            executor,
        );
        let id = EntityId::new(59);
        let activation = ActivationId::new(NonZeroU64::MIN);
        let activated = runtime.activate(id, activation).await;
        let Ok(activated) = activated else {
            panic!("actual native lease is acquired")
        };
        let address = activated.endpoint.address();
        let mut termination = pin!(activated.endpoint.termination());
        let before_retirement = termination.as_mut().poll(&mut Context::from_waker(&waiter));
        assert!(before_retirement.is_pending());
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
        let observed = termination.await;
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
        assert_eq!(
            notification_owner.strong_count(),
            1,
            "the runtime retains the first original after the consuming callback panic"
        );
        let Err(EntityRetirementFailure::ShutdownRequestPanicked {
            termination_notification: Err(RetirementNotificationError::Panicked { payload }),
            shutdown_request,
            forced: None,
            retired: Some(retired_fault),
        }) = retired
        else {
            panic!("conversion, termination notification and consuming callback faults coexist")
        };
        assert_eq!(observed, Err(Crash::Cancelled));
        let shutdown_cause = shutdown_request
            .downcast::<&str>()
            .expect("original shutdown conversion cause");
        assert_eq!(
            *shutdown_cause,
            "original application shutdown conversion panic"
        );
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            notification_allocation
        );
        assert_eq!(
            ptr::from_ref(retired_fault.as_ref()).cast::<()>(),
            retirement_allocation
        );
        let first_cause = payload
            .downcast::<Arc<Vec<u64>>>()
            .expect("original waiter payload");
        let later_cause = retired_fault
            .downcast::<Arc<Vec<u64>>>()
            .expect("original callback payload");
        assert_eq!(first_cause.as_slice(), [67, 71]);
        assert_eq!(later_cause.as_slice(), [73, 79]);
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

    #[tokio::test]
    async fn exhausted_entity_allocation_retains_unstarted_state_and_releases_permits() {
        let entries = Arc::new(vec![83_u64, 89]);
        let definition = Arc::new(ShutdownConversionDefinition {
            retirement_fault: Mutex::new(None),
            entries: Arc::clone(&entries),
            endpoint: Mutex::new(None),
            forced: Mutex::new(Vec::new()),
            retirement: Mutex::new(None),
        });
        let spaces = Arc::new(ActorSpace::<ShutdownConversionActor>::new());
        let allocations = ApplicationAddresses::from_next(u64::MAX);
        let metrics = Arc::new(EntityMetricState::default());
        let runtime = bombay_entity_runtime(
            definition,
            Arc::clone(&spaces),
            allocations.clone(),
            EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
            Arc::clone(&metrics),
            Handle::current(),
        );
        for generation in [1, 2] {
            let activation =
                ActivationId::new(NonZeroU64::new(generation).expect("actual activation"));
            let refused = runtime.activate(EntityId::new(97), activation).await;
            let Err(EntityActivationError::AllocationRejected { behavior, reason }) = refused
            else {
                panic!(
                    "exhaustion refuses before actor launch, including after permits are returned"
                );
            };
            assert!(Arc::ptr_eq(&entries, &behavior.entries));
            assert_eq!(behavior.entries.as_slice(), [83, 89]);
            assert_eq!(reason, AllocationRejection::Exhausted);
            assert_eq!(runtime.residents.available_permits(), 1);
            assert_eq!(runtime.hydrations.available_permits(), 1);
        }
        assert!(spaces.space().resolve(&MailAddr(u64::MAX)).is_none());
        let still_exhausted = allocations.allocate();
        assert_eq!(still_exhausted, Err(AllocationRejection::Exhausted));
        assert_activation_metrics(
            &metrics,
            EntityMetrics {
                activations: 0,
                hydration_failures: 0,
                launch_failures: 2,
                capacity_refusals: 0,
                forced_retirements: 0,
                peak_hydrations: 1,
                residents: 0,
            },
        );
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

    #[tokio::test]
    async fn native_entity_port_spawns_on_its_selected_host_from_distinct_caller() {
        let caller = Handle::current().id();
        let selected_runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("real independent selected Entity host");
        let executor = selected_runtime.handle().clone();
        let selected = executor.id();
        let native_runtime = bombay_entity_runtime(
            Arc::new(JoinDefinition),
            Arc::new(ActorSpace::<JoinedActor>::new()),
            ApplicationAddresses::new(),
            EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
            Arc::new(EntityMetricState::default()),
            executor,
        );
        let (task_host, received_host) = oneshot::channel();
        let mut task = native_runtime.spawn(async move {
            let actual_host = Handle::current().id();
            task_host
                .send(actual_host)
                .expect("actual task observation receiver remains owned");
        });
        let actual_host = received_host.await;
        let joined = <BombayEntityRuntime<JoinDefinition> as LocalEntityRuntime<u64, Never>>::join(
            &mut task,
        )
        .await;
        drop(task);
        drop(native_runtime);
        let released_host = spawn_blocking(move || drop(selected_runtime)).await;

        let Ok(()) = released_host else {
            panic!("the selected host is disposed after its native task joins")
        };
        let Ok(()) = joined else {
            panic!("the actual owning native Entity port joins its task")
        };
        let Ok(actual_host) = actual_host else {
            panic!("the actual task returns its observed runtime identity")
        };
        assert_ne!(selected, caller);
        assert_eq!(
            actual_host, selected,
            "native Entity lifecycle task must use its selected host"
        );
    }
}
