use crate::ActorExecutionOutcome;
use crate::address::MailAddr;
use crate::launch::{ProjectedTask, SpawnError, spawn_owned_with};
use crate::local::children::{
    ChildBindingAt, ChildOriginAt, CreationBinding, NestedBindings, NestedChildBindings,
    RetireChildTasks, StructuralOrigins,
};
use crate::local::effects::reports::LocalParentReports;
use crate::local::effects::{
    ActionInterpreter, ApplicationCapabilities, ApplicationCapabilityInputs, BindingTerminal,
    CommitActions, NoParent,
};
use crate::local::endpoint::InstalledActor;
use crate::local::environment::{LocalActivationRejection, LocalResidual};
use crate::local::ingress::DEFAULT_USER_CAPACITY;
use crate::terminal::{ActorRetirement, ProjectTerminal};
use crate::worker_preparation::{WorkerPreparationSource, settle_worker_preparation};
use behavior::{
    ActionItem, Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildCreationOutcome, ChildNamespaceExhausted, ClassifySettlement, CommittedChild, CreateChild,
    CreationId, CreationKind, CreationRejection, Creations, EstablishChild, EstablishedActor,
    EventIngress, InjectEvent, InterpretItem, InterpretationProgress, ItemSettlement, Never,
    Protocol, RoutedCreation, SourceAdmission,
};
use behavior_actors::atomic::{
    ActivationPlan, BeginActivation, PrepareWorkers, WorkerActivation, WorkerPreparation,
    WorkerSource,
};
use bombay_address::ClaimError;
use bombay_engine::DriverError;
use communication::ControlClosed;
use core::future::Future;

impl<C, N, P, Bindings, Origins, New, RootEvent, Path>
    InterpretItem<Creations<CreateChild<MailAddr, New>>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    New: Send,
    Bindings: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <Creations<CreateChild<MailAddr, New>> as ActionItem>::Input<'a>,
        received: &'a mut Option<<Creations<CreateChild<MailAddr, New>> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        Creations<CreateChild<MailAddr, New>>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(creations) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let Ok(count) = u64::try_from(creations.len()) else {
                    break 'settlement ItemSettlement::Rejected {
                        item: creations,
                        reason: ChildNamespaceExhausted,
                    };
                };
                let Some(next) = self.next_child_route.checked_add(count) else {
                    break 'settlement ItemSettlement::Rejected {
                        item: creations,
                        reason: ChildNamespaceExhausted,
                    };
                };
                let mut route = self.next_child_route;
                self.next_child_route = next;
                ItemSettlement::Accepted(creations.map(|creation| {
                    let routed = RoutedCreation::new(creation, route);
                    route += 1;
                    routed
                }))
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Source, Input> SourceAdmission<C::Event, Source, Input>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: EventIngress<Source, Input>,
    Input: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn admit_source(
        &mut self,
        input: &mut Option<Input>,
        received: &mut Option<Result<(), Input>>,
    ) -> impl Future<Output = ()> + Send {
        async move {
            if received.is_some() {
                return;
            }
            let Some(input) = input.take() else {
                return;
            };
            let event = C::Event::ingress(input);
            match self.control.send(event) {
                Ok(()) => *received = Some(Ok(())),
                Err(ControlClosed(_)) => {
                    unreachable!("the actor control lane remains live while its effects commit")
                }
            }
        }
    }
}

impl<C, N, P, Bindings, Origins, Position, Child> EstablishChild<Position, Child>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + BehaviorBase,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    N: Send + Sync + 'static,
    P: Send,
    Position: 'static,
    Bindings: ChildBindingAt<Position, Child = Child, Root = BindingTerminal<Bindings>, Origins = Origins>
        + NestedChildBindings<Child>
        + RetireChildTasks
        + Send,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + BehaviorBase
        + Send
        + 'static,
    <Child as BehaviorSettlements>::Settlements: ClassifySettlement + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    Child::InterpretationCustody: Send + 'static,
    Child::SourceCustody: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    BindingTerminal<Bindings>: Send + 'static,
    NestedBindings<Bindings, Child>:
        Default + RetireChildTasks<Root = BindingTerminal<Bindings>> + Send + 'static,
    ActionInterpreter<
        ApplicationCapabilities<
            Child,
            N,
            LocalParentReports<C::Event, Child, C::Birth>,
            NestedBindings<Bindings, Child>,
            StructuralOrigins<Child::Base>,
        >,
    >: CommitActions<
            Child,
            Retired = (
                Vec<BindingTerminal<Bindings>>,
                <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    Origins: ChildOriginAt<Position, Child>,
    BindingTerminal<Bindings>: ProjectTerminal<
            Origins::Origin,
            ActorRetirement<
                Child,
                BindingTerminal<Bindings>,
                <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures,
            >,
        >,
    <NestedBindings<Bindings, Child> as RetireChildTasks>::Failures: Send + 'static,
{
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive child-establishment transition owns allocation, launch, binding, and every exact settlement"
    )]
    async fn establish_child(
        &mut self,
        creation: RoutedCreation<MailAddr, Child>,
    ) -> ItemSettlement<
        RoutedCreation<MailAddr, Child>,
        ChildCreationOutcome<Child, Position>,
        CreationRejection,
        Never,
    > {
        let id = creation.id();
        let kind = creation.kind();
        let route = creation.route();
        let (request, _) = creation.into_parts();
        let (_, child, _) = request.into_parts();
        let address = match self.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(id, CreationBinding::Rejected);
                return ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, child),
                    reason: CreationRejection::Allocation(reason),
                };
            }
        };
        let actors = self
            .child_bindings
            .as_ref()
            .expect("live child bindings remain installed")
            .child_actors();
        let actor_spaces = self.actor_spaces.clone();
        let allocations = self.allocations.clone();
        let parent = self.control.clone();
        let installed = spawn_owned_with(
            actors,
            communication::Config::new(DEFAULT_USER_CAPACITY),
            address,
            child,
            move |control, terminal_reports, timers, observations| {
                ActionInterpreter::new(
                    ApplicationCapabilities::<
                        Child,
                        N,
                        NoParent,
                        NestedBindings<Bindings, Child>,
                        StructuralOrigins<Child::Base>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        NestedBindings::<Bindings, Child>::default(),
                    )
                    .with_parent(
                        LocalParentReports::<C::Event, Child, C::Birth>::new(id, parent),
                    ),
                )
            },
        )
        .await;
        match installed {
            Ok(mut installed) => {
                let endpoint = installed.actor.clone();
                let control = installed.control.clone();
                let binding = installed
                    .binding
                    .take()
                    .expect("fresh child installation awaits one binding acknowledgement");
                let task = ProjectedTask::project(installed.task, Origins::origin(address, route));
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::Established {
                            kind,
                            route,
                            endpoint: endpoint.clone(),
                            control: installed.control,
                            task: Some(task),
                            joined: None,
                        },
                    );
                let acknowledgement = binding.send(());
                assert!(
                    acknowledgement.is_ok(),
                    "the privately committed child awaits its recorded binding"
                );
                let actor =
                    EstablishedActor::<Child>::issued(InstalledActor::new(endpoint, control));
                ItemSettlement::Accepted(ChildCreationOutcome::Established(CommittedChild::new(
                    id, kind, actor,
                )))
            }
            Err(SpawnError::AllocationRejected { behavior, reason }) => {
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(id, CreationBinding::Rejected);
                ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, behavior),
                    reason: CreationRejection::Allocation(reason),
                }
            }
            Err(SpawnError::InitializationRejected {
                behavior,
                error,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: None,
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    error,
                })
            }
            Err(SpawnError::InitializationPanicked {
                behavior,
                payload,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::InitializationPanicked(payload)),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::InitializationPanicked {
                    creation: routed_creation(id, kind, route, behavior),
                })
            }
            Err(SpawnError::HostRejected {
                behavior,
                initialization: InterpretationProgress::Original(initialization),
                error,
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                let reason = match &error {
                    ClaimError::AddressInUse(_) => CreationRejection::Allocation(
                        behavior::AllocationRejection::AddressAlreadyClaimed,
                    ),
                    ClaimError::RegistrationIdsExhausted(_) => CreationRejection::EnvironmentFailed,
                };
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::Activation(
                                LocalActivationRejection::Address(error),
                            )),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason,
                })
            }
            Err(SpawnError::BindingAbandoned {
                behavior,
                initialization: InterpretationProgress::Original(initialization),
                control,
                user,
                descendants: (descendants, child_failures),
                capability_failures,
                additional_failures,
                terminal_report,
                retirement_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                unread_owner_cancellation,
            }) => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(control.is_empty());
                assert!(user.is_empty());
                assert!(descendants.is_empty());
                // This standard child has not crossed its binding ACK, so its freshly
                // constructed nested bindings have interpreted no initialization effects.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::Activation(
                                LocalActivationRejection::BindingAbandoned,
                            )),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(SpawnError::Unpublished(ActorExecutionOutcome::ActivationPanicked {
                behavior,
                residual:
                    LocalResidual::Uncommitted {
                        initialization: InterpretationProgress::Original(initialization),
                        ingress,
                        activation_tasks,
                        descendants: (descendants, child_failures),
                        capability_failures,
                        terminal_report,
                        retirement_failures,
                        received_interpretation: None,
                        received_source: None,
                        source_index: None,
                        acquired_ingress: None,
                        unread_owner_cancellation,
                    },
                additional_failures,
                payload,
            })) => {
                // This branch is reached only after the real private committed
                // receiver closed. No ACK or initialization interpreter ran.
                assert!(activation_tasks.is_empty());
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert!(ingress.control.is_empty());
                assert!(ingress.user.is_empty());
                assert!(descendants.is_empty());
                drop(activation_tasks);
                // The exact freshly defaulted nested product has no creation
                // occurrence; its failure lanes have no admitted child rows.
                drop(child_failures);
                self.child_bindings
                    .as_mut()
                    .expect("live child bindings remain installed")
                    .record_creation(
                        id,
                        CreationBinding::StartupRejected {
                            kind,
                            address,
                            route,
                            primary_failure: Some(DriverError::ActivationPanicked(payload)),
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        },
                    );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(unexpected_startup) => {
                // The concrete private commitment producer cannot reach an
                // active phase without returning its endpoint and ACK sender.
                // Only the enumerated Prepared/Original startup phases above
                // can be returned before that successful transfer. Changing
                // this standard producer requires revisiting this invariant.
                panic!(
                    "the standard private child returned an invalid startup phase: {unexpected_startup:?}"
                );
            }
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, Path>
    InterpretItem<BeginActivation<Worker, Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerActivation<Worker, Plan>, Path> + Send + 'static,
    Worker: Behavior<Protocol: Protocol<Addr = MailAddr>> + 'static,
    BehaviorMessage<Worker>: Send,
    Plan: ActivationPlan + 'static,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <BeginActivation<Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<BeginActivation<Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        BeginActivation<Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.as_ref() else {
                return;
            };
            self.inject_control_event::<_, Path>(request.started());
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let control = self.control.clone();
                self.activation_tasks
                    .as_mut()
                    .expect("live activation tasks remain installed")
                    .spawn(async move {
                        let event = C::Event::inject_at(request.activate().await);
                        control.send(event).map_err(|ControlClosed(event)| event)
                    });
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Source, Role, Worker, Plan, RootEvent, Path>
    InterpretItem<PrepareWorkers<Source, Role, Worker, Plan>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<WorkerPreparation<Source, Role, Worker, Plan>, Path> + Send + 'static,
    Source:
        WorkerSource<Role, Worker, Plan> + WorkerPreparationSource<Role, Worker, Plan> + 'static,
    Role: Send + Sync + 'static,
    Worker: Behavior + Send + 'static,
    Plan: ActivationPlan + 'static,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <PrepareWorkers<Source, Role, Worker, Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<PrepareWorkers<Source, Role, Worker, Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        PrepareWorkers<Source, Role, Worker, Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let (receipt, starting) = request.start();
                let control = self.control.clone();
                self.activation_tasks
                    .as_mut()
                    .expect("live activation tasks remain installed")
                    .spawn(async move {
                        let event = C::Event::inject_at(settle_worker_preparation(starting).await);
                        control.send(event).map_err(|ControlClosed(event)| event)
                    });
                ItemSettlement::Accepted(receipt)
            };
            *received = Some(settlement);
        }
    }
}

fn routed_creation<Child>(
    id: CreationId,
    kind: CreationKind,
    route: u64,
    child: Child,
) -> RoutedCreation<MailAddr, Child> {
    let creation = match kind {
        CreationKind::Birth => CreateChild::birth(id, child),
        CreationKind::Replacement { previous } => CreateChild::replacement(id, previous, child),
    };
    RoutedCreation::new(creation, route)
}

#[cfg(test)]
mod child_projection_panic {
    use std::any::Any;
    use std::future::Future;
    use std::panic::resume_unwind;
    use std::pin::pin;
    use std::ptr;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll, Waker};

    use behavior::{
        ActionSettlement, Actions, BehaviorActed, BehaviorBase, ChildCreationOutcome, ChildHead,
        CreateChild, CreationId, CreationKind, CreationSequence, CreationSettlement, Creations,
        EventLayer, ItemSettlement, Never, NoSends, SendLayer, SettledItem, Step, Stopped,
    };
    use behavior_actors::{ShutdownRequested, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::Config;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::{Id, id};

    use crate::actor;
    use crate::actors::ActorExt;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::launch::{OwnedTask, spawn_root_with};
    use crate::local::children::ChildBindings;
    use crate::local::children::StructuralOrigins;
    use crate::local::effects::ActionInterpreter;
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs, NoParent};
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::terminal::{ActorRetirement, ChildFailure, ChildOrigin, ProjectTerminal};
    use crate::{ActorSpace, ActorSpaces};

    type ProjectionCustody = (oneshot::Sender<(Id, ChildTerminal)>, Box<dyn Any + Send>);
    static PROJECTION_CUSTODY: Mutex<Option<ProjectionCustody>> = Mutex::new(None);

    enum ChildDisposition {
        Stop,
        Continue,
    }

    struct ProjectionChild {
        disposition: ChildDisposition,
        entries: Arc<Vec<u64>>,
    }

    #[actor(message = Never)]
    impl ProjectionChild {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "the generated fold retains its exact controlled-error boundary"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            match self.disposition {
                ChildDisposition::Stop => Ok(Actions::stop()),
                ChildDisposition::Continue => Ok(Actions::cont()),
            }
        }
    }

    struct ProjectionParent {
        first: CreationId,
        later: CreationId,
        first_entries: Option<Arc<Vec<u64>>>,
        later_entries: Option<Arc<Vec<u64>>>,
    }

    #[actor(
        message = Never,
        births = { child: StopOnShutdown<ProjectionChild> },
        creation_settlements = retain_for_retirement,
    )]
    impl ProjectionParent {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "the generated fold retains its exact controlled-error boundary"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            let first = ProjectionChild {
                disposition: ChildDisposition::Stop,
                entries: self
                    .first_entries
                    .take()
                    .expect("one original first child input"),
            };
            let later = ProjectionChild {
                disposition: ChildDisposition::Continue,
                entries: self
                    .later_entries
                    .take()
                    .expect("one original later child input"),
            };
            Ok(Actions::new(
                NoSends,
                Creations::one(CreateChild::birth(self.first, first.stop_on_shutdown()))
                    .and(CreateChild::birth(self.later, later.stop_on_shutdown())),
                Step::Continue,
            ))
        }
    }

    #[derive(ActorSpaces)]
    struct ProjectionSpaces {
        parents: ActorSpace<ProjectionParent>,
    }

    struct ChildTerminal {
        origin: ChildOrigin<ProjectionParent, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
    }

    impl
        ProjectTerminal<
            ChildOrigin<ProjectionParent, ChildHead>,
            ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
        > for ChildTerminal
    {
        fn project(
            origin: ChildOrigin<ProjectionParent, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Self, ()>,
        ) -> Self {
            match retirement {
                retirement @ ActorRetirement::Completed { .. } => {
                    let (publication, payload) = PROJECTION_CUSTODY
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        .expect("the actual eager projector owns its publication and native cause");
                    let transferred = publication.send((id(), Self { origin, retirement }));
                    match transferred {
                        Ok(()) => {}
                        Err(original) => {
                            drop(original);
                            panic!("the complete original child report receiver remains live");
                        }
                    }
                    // Runtime projection is outside every Behavior fold. The
                    // original opaque Box becomes this actual task's native cause.
                    resume_unwind(payload)
                }
                retirement => Self { origin, retirement },
            }
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one finite native projector controller joins owners before complete custody oracles"
    )]
    #[expect(
        clippy::default_trait_access,
        reason = "the concrete constructor infers the binding type; the non-injective ChildBindings alias cannot name its Default implementation"
    )]
    fn actual_projection_panic_keeps_origin_native_cause_and_later_sibling() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual local actor executor");
        let first_entries = Arc::new(vec![31, 37, 41]);
        let later_entries = Arc::new(vec![43, 47, 53]);
        let original_cause = Arc::new(vec![59_u64, 61, 67]);
        let first_allocation = first_entries.as_ptr();
        let later_allocation = later_entries.as_ptr();
        let first_owner = Arc::downgrade(&first_entries);
        let later_owner = Arc::downgrade(&later_entries);
        let cause_owner = Arc::downgrade(&original_cause);
        let mut creations = CreationSequence::new();
        let first = creations
            .issue()
            .expect("one actual first creation identity");
        let later = creations
            .issue()
            .expect("one actual later creation identity");
        let (publication, projected) = oneshot::channel();
        let original_payload: Box<dyn Any + Send> = Box::new(original_cause);
        let payload_allocation = ptr::from_ref(original_payload.as_ref()).cast::<()>();
        let prior_custody = PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace((publication, original_payload));
        drop(prior_custody);
        let spaces = ProjectionSpaces {
            parents: ActorSpace::new(),
        };
        let roots = spaces.parents.clone();
        let parent = ProjectionParent {
            first,
            later,
            first_entries: Some(first_entries),
            later_entries: Some(later_entries),
        }
        .stop_on_shutdown();
        let (reported, parent_poll, shutdown, received, remaining_owner) =
            runtime.block_on(async move {
                let actor_spaces = Arc::new(spaces);
                let root = spawn_root_with(
                    roots.clone(),
                    Config::new(2),
                    MailAddr::APPLICATION_ROOT,
                    parent,
                    move |control, terminal_reports, timers, observations| {
                        ActionInterpreter::new(ApplicationCapabilities::<
                            StopOnShutdown<ProjectionParent>,
                            ProjectionSpaces,
                            NoParent,
                            ChildBindings<
                                StopOnShutdown<ProjectionParent>,
                                ChildTerminal,
                                StructuralOrigins<ProjectionParent>,
                            >,
                            StructuralOrigins<ProjectionParent>,
                        >::new_with_bindings(
                            ApplicationCapabilityInputs {
                                address: MailAddr::APPLICATION_ROOT,
                                actor_spaces,
                                allocations: ApplicationAddresses::new(),
                                control,
                                timers,
                                observations,
                                terminal_reports,
                            },
                            Default::default(),
                        ))
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("the actual parent publishes both committed births"));
                let mut owned = Some(root.task);
                let mut received = None;
                let reported = projected.await;
                // The eager synchronous callback transfers then unwinds in the same
                // task poll. No await separates publication from its native panic.
                let parent_poll = {
                    let mut waiting = pin!(OwnedTask::receive_finish(&mut owned, &mut received));
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                };
                let shutdown = match root.shutdown_control.upgrade() {
                    Some(control) => control.send(EventLayer::Owned(ShutdownRequested)),
                    None => panic!("the parent remains live until its explicit shutdown"),
                };
                OwnedTask::receive_finish(&mut owned, &mut received).await;
                drop(root.actor);
                (reported, parent_poll, shutdown, received, owned)
            });
        drop(runtime);
        let stale_custody = PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(stale_custody);
        // Native root join and executor cleanup precede every custody oracle.
        assert!(remaining_owner.is_none());
        assert!(matches!(parent_poll, Poll::Pending));
        assert!(shutdown.is_ok());
        let (projection_task, first_terminal) =
            reported.expect("the actual first child transferred its complete retirement");
        let outcome = received
            .expect("the actual root join was acquired")
            .expect("the projector panic must not substitute for the parent task result");
        let retirement = ActorRetirement::from_local(outcome);
        let ActorRetirement::Completed {
            behavior,
            interpretation,
            source,
            settlements,
            control,
            user,
            descendants,
            child_failures: (mut failures, ()),
            capability_failures,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
            completion,
        } = retirement
        else {
            panic!("explicit shutdown completes the original live parent");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        let parent = behavior.into_inner();
        assert_eq!(parent.first, first);
        assert_eq!(parent.later, later);
        assert!(parent.first_entries.is_none());
        assert!(parent.later_entries.is_none());
        let [stopped, settlement] = settlements.try_into().unwrap_or_else(|_| {
            panic!("the parent retains its ordered shutdown Stop and initialization Continue settlements");
        });
        assert_eq!(stopped.sends.owned, NoSends);
        assert_eq!(stopped.sends.inner, NoSends);
        assert_eq!(stopped.become_, Step::Stop(Stopped));
        let CreationSettlement::Settled(stopped_creations) = stopped.creations.into_settlement()
        else {
            panic!("the actual shutdown Stop retains its complete empty creation lane");
        };
        assert!(stopped_creations.is_empty());
        assert_eq!(settlement.sends.owned, NoSends);
        assert_eq!(settlement.sends.inner, NoSends);
        assert_eq!(settlement.become_, Step::Continue);
        let CreationSettlement::Settled(reports) = settlement.creations.into_settlement() else {
            panic!("both original births retain their complete routed settlement lane");
        };
        let reports: Vec<_> = reports.into_iter().collect();
        let [first_report, later_report] = reports.try_into().unwrap_or_else(|_| {
            panic!("the actual initialized product owns both committed creations");
        });
        let mut committed_addresses = Vec::new();
        for (report, expected) in [(first_report, first), (later_report, later)] {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::Established(report),
            )) = report
            else {
                panic!("both children committed before parent shutdown");
            };
            assert_eq!(report.id(), expected);
            assert_eq!(report.kind(), CreationKind::Birth);
            let endpoint = report
                .actor()
                .into_recipient()
                .interpret(&mut ExtractLocalEndpoint);
            committed_addresses.push(endpoint.address());
        }
        assert_eq!(failures.len(), 1);
        let failure = failures.remove(0);
        let ChildFailure::ProjectionTaskFailed {
            id,
            kind,
            origin,
            actor,
            error,
        } = failure
        else {
            panic!("the actual native cause belongs to the projection task");
        };
        assert_eq!(id, first);
        assert_eq!(kind, CreationKind::Birth);
        assert_eq!(origin, first_terminal.origin);
        assert_eq!(origin.address(), committed_addresses[0]);
        let endpoint = actor.into_recipient().interpret(&mut ExtractLocalEndpoint);
        assert_eq!(endpoint.address(), origin.address());
        assert_eq!(error.id(), projection_task);
        assert!(error.is_panic());
        assert!(!error.is_cancelled());
        let payload = error.into_panic();
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            payload_allocation
        );
        let [later_terminal] = descendants.as_slice() else {
            panic!("the later sibling is joined despite the earlier native projector failure");
        };
        assert_eq!(later_terminal.origin.address(), committed_addresses[1]);
        assert_ne!(
            first_terminal.origin.address(),
            later_terminal.origin.address()
        );
        assert_ne!(first_terminal.origin.nonce(), later_terminal.origin.nonce());
        let mut terminals = vec![first_terminal];
        terminals.extend(descendants);
        let stopped_child_settlements = vec![ActionSettlement {
            creations: Creations::empty(),
            sends: SendLayer::new(NoSends, NoSends),
            become_: Step::Stop(Stopped),
        }];
        for (terminal, allocation, entries) in [
            (&terminals[0], first_allocation, &[31, 37, 41][..]),
            (&terminals[1], later_allocation, &[43, 47, 53][..]),
        ] {
            let (
                behavior,
                interpretation,
                source,
                control,
                user,
                descendants,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            ) = match &terminal.retirement {
                ActorRetirement::Completed {
                    behavior,
                    interpretation,
                    source,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    completion,
                } => {
                    assert_eq!(*completion, Completion::Stopped);
                    assert_eq!(settlements, &stopped_child_settlements);
                    assert_eq!(terminal.origin, origin);
                    (
                        behavior,
                        interpretation,
                        source,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
                        terminal_report,
                        unread_owner_cancellation,
                    )
                }
                ActorRetirement::OwnerCancelled {
                    behavior,
                    interpretation,
                    source,
                    settlements,
                    control,
                    user,
                    descendants,
                    child_failures: (),
                    capability_failures,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                    unread_owner_cancellation,
                } => {
                    assert_eq!(settlements.len(), 0);
                    assert_ne!(terminal.origin, origin);
                    (
                        behavior,
                        interpretation,
                        source,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        additional_failures,
                        received_interpretation,
                        received_source,
                        source_index,
                        acquired_ingress,
                        retirement_failures,
                        terminal_report,
                        unread_owner_cancellation,
                    )
                }
                _ => panic!("the first child stopped and the later sibling was owner-cancelled"),
            };
            match &behavior.base().disposition {
                ChildDisposition::Stop => assert_eq!(terminal.origin, origin),
                ChildDisposition::Continue => assert_ne!(terminal.origin, origin),
            }
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(behavior.base().entries.as_slice(), entries);
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert!(descendants.is_empty());
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(retirement_failures.is_empty());
            assert!(terminal_report.is_none());
            assert!(unread_owner_cancellation.is_none());
        }
        assert_eq!(first_owner.strong_count(), 1);
        assert_eq!(later_owner.strong_count(), 1);
        assert_eq!(cause_owner.strong_count(), 1);
        drop((terminals, payload));
        assert_eq!(first_owner.strong_count(), 0);
        assert_eq!(later_owner.strong_count(), 0);
        assert_eq!(cause_owner.strong_count(), 0);
    }
}
