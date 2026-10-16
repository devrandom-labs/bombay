use super::services::{NativeCapabilities, NativeConstruction};
use crate::ActorExecutionOutcome;
use crate::NativeHost;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::MailboxCapacity;
use crate::launch::{
    ActorSpace, OwnedActor, OwnedTask, ProjectedTask, SpawnError, spawn_owned_execution,
};
use crate::local::children::{
    ChildBindingsIn, ChildCreationAt, ChildOriginAt, CreationBinding, NestedBindings,
    RetireChildTasks, StartupRetirementCustody, StructuralOrigins, record_child_creation,
};
use crate::local::effects::reports::LocalParentReports;
use crate::local::effects::{
    ActionInterpreter, ApplicationCapabilities, ApplicationCapabilityInputs, BindingTerminal,
    CommitActions,
};
use crate::local::endpoint::InstalledActor;
use crate::local::environment::{LocalActivationRejection, LocalResidual};
use crate::observe;
use crate::terminal::{ActorRetirementReport, ProjectTerminal, RetirementSchema};
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
use communication::{ControlClosed, ControlSender};
use core::future::Future;
use core::marker::PhantomData;
use std::collections::HashMap;
use std::sync::Arc;

impl<C, N, P, Bindings, Origins, New, RootEvent, Path, Services>
    InterpretItem<Creations<CreateChild<MailAddr, New>>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
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

impl<C, N, P, Bindings, Origins, Source, Input, Services> SourceAdmission<C::Event, Source, Input>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
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

/// Scope-owned native realization of one declared child occurrence.
///
/// `Terminal` selects the parent's complete descendant report family; the
/// namespace loan already owns that exact family. `Descendants` selects the
/// child's own custody. Neither selection changes during establishment.
/// Custom terminal presentations require their corresponding native operation.
#[allow(private_bounds)]
pub trait NativeChild<C, Position, Child, Terminal, Descendants>:
    Sized + Send + Sync + 'static
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    /// Select the physical registration family for this declared child class.
    /// Hosted classes share their explicit logical family; exact-only classes
    /// retain an independent occurrence family.
    fn registration(&self) -> ActorSpace<Child::Protocol>;

    fn establish<Origin, OriginFn>(
        pending: PendingChild<'_, C, Self, Position, Child, Terminal, Descendants, OriginFn>,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        Terminal: ProjectTerminal<Origin, Child, Descendants = Descendants> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send;
}

impl<C, N, Position, Child, Terminal, Descendants>
    NativeChild<C, Position, Child, Terminal, Descendants> for Arc<N>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    N: NativeChild<C, Position, Child, Terminal, Descendants>,
{
    fn registration(&self) -> ActorSpace<Child::Protocol> {
        <N as NativeChild<C, Position, Child, Terminal, Descendants>>::registration(self.as_ref())
    }

    fn establish<Origin, OriginFn>(
        pending: PendingChild<'_, C, Self, Position, Child, Terminal, Descendants, OriginFn>,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        Terminal: ProjectTerminal<Origin, Child, Descendants = Descendants> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        let PendingChild {
            creations,
            creation_order,
            mailbox_capacity,
            actors,
            actor_spaces,
            allocations,
            control,
            creation,
            origin,
            position,
        } = pending;
        N::establish::<Origin, OriginFn>(PendingChild {
            creations,
            creation_order,
            mailbox_capacity,
            actors,
            actor_spaces: Arc::clone(actor_spaces.as_ref()),
            allocations,
            control,
            creation,
            origin,
            position,
        })
    }
}

/// Affine loan of the selected creator-local namespace and original request.
#[allow(private_bounds)]
pub struct PendingChild<'a, C, N, Position, Child, Terminal, Descendants, OriginFn>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    #[expect(
        clippy::type_complexity,
        reason = "the consuming operation borrows the original typed registry without another storage owner"
    )]
    creations: &'a mut HashMap<
        CreationId,
        CreationBinding<
            Child,
            Terminal,
            Descendants,
            Vec<<Descendants as RetirementSchema>::Failure>,
        >,
    >,
    creation_order: &'a mut Vec<CreationId>,
    actors: ActorSpace<Child::Protocol>,
    actor_spaces: Arc<N>,
    allocations: ApplicationAddresses,
    mailbox_capacity: MailboxCapacity,
    control: ControlSender<C::Event>,
    creation: RoutedCreation<MailAddr, Child>,
    origin: OriginFn,
    position: PhantomData<fn() -> Position>,
}

trait NativeChildStartup<C, Child, Descendants>: Sized + Send + Sync + 'static
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn establish_native_child<Position, Terminal, Origin, OriginFn>(
        pending: PendingChild<'_, C, Self, Position, Child, Terminal, Descendants, OriginFn>,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        Position: 'static,
        Terminal: ProjectTerminal<Origin, Child, Descendants = Descendants> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send;
}

/// Consuming native realization of a selected pending child request.
///
/// This interpreter port preserves the complete request on rejection and the
/// exact child execution/source custody on acceptance. This port is sealed;
/// only the runtime supplies standard-kernel execution authority.
///
/// A recorded creator-local correlation ID is rejected before allocation with
/// [`CreationRejection::CorrelationAlreadyRecorded`], returning the original
/// request without changing the existing child binding.
///
/// # Panics
/// Panics if internal startup-row custody is inconsistent.
#[allow(private_bounds)]
pub trait EstablishNativeChild<C, Child, Descendants>:
    NativeChildStartup<C, Child, Descendants>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    fn establish_native_child<Position, Terminal, Origin, OriginFn>(
        pending: PendingChild<'_, C, Self, Position, Child, Terminal, Descendants, OriginFn>,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        Position: 'static,
        Terminal: ProjectTerminal<Origin, Child, Descendants = Descendants> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as NativeChildStartup<C, Child, Descendants>>::establish_native_child::<
            Position,
            Terminal,
            Origin,
            OriginFn,
        >(pending)
    }
}

#[allow(private_bounds)]
impl<C, N, Child, Descendants> EstablishNativeChild<C, Child, Descendants> for N
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Descendants: RetirementSchema,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    N: NativeChildStartup<C, Child, Descendants>,
{
}

#[allow(private_bounds)]
impl<C, N, Child, Descendants> NativeChildStartup<C, Child, Descendants> for N
where
    C: BehaviorBase + Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    N: NativeHost + Send + Sync + 'static,
    N::Runtime: NativeConstruction,
    <Child::Birth as BirthMode>::Child: behavior::ChildOccurrenceProduct<
            crate::local::children::RuntimeChildBindings<
                Descendants,
                StructuralOrigins<Child::Base>,
            >,
        >,
    Descendants::Failure: Send + 'static,
    Child: BehaviorBase
        + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + Send
        + 'static,
    Child::Settlements: ClassifySettlement + Send + 'static,
    Child::Event: Send + 'static,
    BehaviorMessage<Child>: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Error: Send + 'static,
    Child::InterpretationCustody: Send + 'static,
    Child::SourceCustody: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    NestedBindings<Child, Descendants>: ChildBindingsIn<Child, N>
        + RetireChildTasks<
            Root = Descendants,
            Failures = Vec<<Descendants as RetirementSchema>::Failure>,
        > + Send
        + 'static,
    ActionInterpreter<
        NativeCapabilities<
            Child,
            N,
            LocalParentReports<C::Event, Child, C::Birth>,
            NestedBindings<Child, Descendants>,
            StructuralOrigins<Child::Base>,
        >,
    >: CommitActions<
            Child,
            Retired = (
                Vec<Descendants>,
                <NestedBindings<Child, Descendants> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    Descendants: RetirementSchema + Send + 'static,
    <NestedBindings<Child, Descendants> as RetireChildTasks>::Failures: Send + 'static,
{
    #[allow(clippy::too_many_lines)]
    async fn establish_native_child<Position, Terminal, Origin, OriginFn>(
        pending: PendingChild<'_, C, Self, Position, Child, Terminal, Descendants, OriginFn>,
    ) -> ItemSettlement<
        RoutedCreation<MailAddr, Child>,
        ChildCreationOutcome<Child, Position>,
        CreationRejection,
        Never,
    >
    where
        Position: 'static,
        Terminal: ProjectTerminal<Origin, Child, Descendants = Descendants> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        let creation = pending.creation;
        let id = creation.id();
        if pending.creations.contains_key(&id) {
            return ItemSettlement::Rejected {
                item: creation,
                reason: CreationRejection::CorrelationAlreadyRecorded,
            };
        }
        let kind = creation.kind();
        let route = creation.route();
        let (request, _) = creation.into_parts();
        let (_, child, _) = request.into_parts();
        let address = match pending.allocations.allocate() {
            Ok(address) => address,
            Err(reason) => {
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::Rejected,
                );
                return ItemSettlement::Rejected {
                    item: routed_creation(id, kind, route, child),
                    reason: CreationRejection::Allocation(reason),
                };
            }
        };
        let mailbox_capacity = pending.mailbox_capacity;
        let actors = pending.actors;
        let actor_spaces = pending.actor_spaces.clone();
        let allocations = pending.allocations.clone();
        let parent = pending.control.clone();
        let (startup, control, task) = spawn_owned_execution(
            actors,
            communication::Config::new(mailbox_capacity.get()),
            address,
            child,
            #[cfg(feature = "remote")]
            Some(allocations.downgrade()),
            move |control, terminal_reports, timers, observations| {
                let bindings = NestedBindings::<Child, Descendants>::new_in(actor_spaces.as_ref());
                ActionInterpreter::new(
                    <N::Runtime as NativeConstruction>::capabilities::<
                        Child,
                        N,
                        NestedBindings<Child, Descendants>,
                        StructuralOrigins<Child::Base>,
                    >(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces,
                            allocations,
                            mailbox_capacity,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        bindings,
                    )
                    .with_parent(
                        LocalParentReports::<C::Event, Child, C::Birth>::new(id, parent),
                    ),
                )
            },
        );
        record_child_creation(
            pending.creations,
            pending.creation_order,
            id,
            CreationBinding::Starting {
                kind,
                address,
                route,
                startup,
                control,
                task: Some(task),
            },
        );
        let started = {
            let binding = pending
                .creations
                .get_mut(&id)
                .expect("the creator owns the exact startup row");
            let CreationBinding::Starting { startup, .. } = binding else {
                unreachable!("the original owned kernel selects its startup row once");
            };
            startup.await
        };
        let installed = if let Ok((actor, binding)) = started {
            let row = pending
                .creations
                .remove(&id)
                .expect("the creator owns this exact startup row");
            let CreationBinding::Starting {
                control,
                task: Some(task),
                ..
            } = row
            else {
                unreachable!("successful startup retains original raw task custody");
            };
            Ok(OwnedActor {
                actor,
                control,
                task,
                binding: Some(binding),
            })
        } else {
            let row = pending
                .creations
                .remove(&id)
                .expect("the creator owns this exact startup row");
            let CreationBinding::Starting { task, .. } = row else {
                unreachable!("failed startup retains original raw task custody");
            };
            record_child_creation(
                pending.creations,
                pending.creation_order,
                id,
                CreationBinding::RetiringStartup {
                    kind,
                    address,
                    route,
                    custody: StartupRetirementCustody::Raw {
                        task,
                        received: None,
                        termination_notification: None,
                    },
                },
            );
            let row = pending
                .creations
                .get_mut(&id)
                .expect("raw startup retirement remains outside its waiter");
            let CreationBinding::RetiringStartup {
                custody:
                    StartupRetirementCustody::Raw {
                        task,
                        received,
                        termination_notification,
                    },
                ..
            } = row
            else {
                unreachable!("the creator receives original raw startup retirement");
            };
            OwnedTask::receive_finish(task, received, termination_notification).await;
            if matches!(received, Some(Err(_))) {
                return ItemSettlement::Accepted(ChildCreationOutcome::Unavailable { id, kind });
            }
            let retirement_report = ActorRetirementReport::from_joined(
                received
                    .as_ref()
                    .expect("raw startup retained its original joined result"),
                termination_notification
                    .as_ref()
                    .expect("raw startup retained its original notification result"),
            );
            let row = pending
                .creations
                .remove(&id)
                .expect("the creator owns this exact startup row");
            let CreationBinding::RetiringStartup {
                custody:
                    StartupRetirementCustody::Raw {
                        received: Some(Ok(outcome)),
                        termination_notification: Some(termination_notification),
                        ..
                    },
                ..
            } = row
            else {
                unreachable!("complete raw startup rejection is outside its producer");
            };
            Err(SpawnError::from_local(
                outcome,
                termination_notification,
                retirement_report,
            ))
        };
        match installed {
            Ok(mut installed) => {
                let endpoint = installed.actor.clone();
                let control = installed.control.clone();
                let binding = installed
                    .binding
                    .take()
                    .expect("fresh child installation awaits one binding acknowledgement");
                let (retirement_publisher, retirement) = observe::pair();
                let task = ProjectedTask::project(
                    installed.task,
                    (pending.origin)(address, route, id, kind),
                    retirement_publisher,
                );
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::Established {
                        kind,
                        route,
                        endpoint: endpoint.clone(),
                        control: installed.control,
                        task: Some(task),
                        joined: None,
                        notifications: None,
                        retirement: retirement.clone(),
                    },
                );
                let acknowledgement = binding.send(());
                assert!(
                    acknowledgement.is_ok(),
                    "the privately committed child awaits its recorded binding"
                );
                let actor = EstablishedActor::<Child>::issued(InstalledActor::new(
                    endpoint, control, retirement,
                ));
                ItemSettlement::Accepted(ChildCreationOutcome::Established(CommittedChild::new(
                    id, kind, actor,
                )))
            }
            Err(SpawnError::AllocationRejected { behavior, reason }) => {
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::Rejected,
                );
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
                termination_notification,
                retirement_report,
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
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::StartupRejected {
                        kind,
                        address,
                        route,
                        primary_failure: None,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        termination_notification,
                        retirement_report,
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
                termination_notification,
                retirement_report,
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
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::StartupRejected {
                        kind,
                        address,
                        route,
                        primary_failure: Some(DriverError::InitializationPanicked(payload)),
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        termination_notification,
                        retirement_report,
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
                termination_notification,
                retirement_report,
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
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
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
                        termination_notification,
                        retirement_report,
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
                termination_notification,
                retirement_report,
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
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
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
                        termination_notification,
                        retirement_report,
                    },
                );
                ItemSettlement::Accepted(ChildCreationOutcome::HostRejected {
                    creation: routed_creation(id, kind, route, behavior),
                    initialization,
                    reason: CreationRejection::EnvironmentFailed,
                })
            }
            Err(SpawnError::Unpublished {
                termination_notification,
                retirement_report,
                outcome:
                    ActorExecutionOutcome::ActivationPanicked {
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
                                ..
                            },
                        additional_failures,
                        payload,
                    },
            }) => {
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
                record_child_creation(
                    pending.creations,
                    pending.creation_order,
                    id,
                    CreationBinding::StartupRejected {
                        kind,
                        address,
                        route,
                        primary_failure: Some(DriverError::ActivationPanicked(payload)),
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        termination_notification,
                        retirement_report,
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

impl<C, N, P, Bindings, Origins, Position, Child, Descendants, Services>
    EstablishChild<Position, Child>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + BehaviorBase,
    Child:
        BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase + Send,
    Descendants: RetirementSchema,
    Bindings: ChildCreationAt<
            Position,
            Child = Child,
            Root = BindingTerminal<Bindings>,
            Origins = Origins,
            Descendants = Descendants,
        > + RetireChildTasks
        + Send,
    Origins: ChildOriginAt<Position, Child>,
    BindingTerminal<Bindings>:
        ProjectTerminal<Origins::Origin, Child, Descendants = Descendants> + Send + 'static,
    N: NativeChild<C, Position, Child, BindingTerminal<Bindings>, Descendants>,
    Self: Send,
{
    async fn establish_child(
        &mut self,
        creation: RoutedCreation<MailAddr, Child>,
    ) -> ItemSettlement<
        RoutedCreation<MailAddr, Child>,
        ChildCreationOutcome<Child, Position>,
        CreationRejection,
        Never,
    > {
        let (creations, creation_order, actors) = self
            .child_bindings
            .as_mut()
            .expect("live child bindings remain installed")
            .creation_namespace();
        N::establish(PendingChild {
            creations,
            creation_order,
            actors,
            actor_spaces: self.actor_spaces.clone(),
            mailbox_capacity: self.mailbox_capacity,
            allocations: self.allocations.clone(),
            control: self.control.clone(),
            creation,
            origin: Origins::origin,
            position: PhantomData,
        })
        .await
    }
}

impl<C, N, Parent, Bindings, Origins, Worker, Plan, Path, Services>
    InterpretItem<BeginActivation<Worker, Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins, Services>
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

impl<C, N, P, Bindings, Origins, Source, Role, Worker, Plan, RootEvent, Path, Services>
    InterpretItem<PrepareWorkers<Source, Role, Worker, Plan>, RootEvent, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Services>
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
    use crate::ActorCustody;
    use crate::MailboxCapacity;

    use std::any::Any;
    use std::future::Future;
    use std::panic::resume_unwind;
    use std::pin::pin;
    use std::ptr;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll, Wake, Waker};

    use behavior::{
        ActionSettlement, Actions, BehaviorActed, BehaviorBase, Births, ChildCreationOutcome,
        ChildDelivery, ChildDeliveryReason, ChildHead, CreateChild, CreationId,
        CreationInterpretationCustody, CreationKind, CreationSequence, CreationSettlement,
        Creations, EventLayer, Here, InterpretCreations, InterpretItem, InterpretationProgress,
        ItemSettlement, Never, NoSends, RoutedCreation, SendLayer, SettledItem, Step, Stopped,
        User,
    };
    use behavior_actors::{ShutdownRequested, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::{Config, mailbox_channel};
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::{Id, id};

    use crate::actor;
    use crate::actors::ActorExt;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::launch::{OwnedTask, spawn_root_with};
    use crate::local::children::StructuralOrigins;
    use crate::local::children::{
        ChildBinding, ChildBindings, ChildBindingsIn, ChildCreationAt, CreationBinding,
        StartupRetirementCustody,
    };
    use crate::local::effects::ActionInterpreter;
    use crate::local::effects::observation::TerminationObservations;
    use crate::local::effects::reports::LocalTerminalReports;
    use crate::local::effects::timers::LocalTimers;
    use crate::local::effects::{
        ApplicationCapabilities, ApplicationCapabilityInputs, NoParent, RetireCapabilities,
    };
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::terminal::{
        ActorRetirement, ChildFailure, ChildOrigin, ProjectTerminal, RetirementNotificationError,
        RetirementSchema,
    };
    use crate::{
        ActorFailureAssessment, ActorNotificationReceipts, ActorRetirementReport, ActorSpace,
        ActorSpaces, LedgerProtocol, NativeHost, RetirementAssessment, Runtime, ShutdownLedger,
    };

    type ProjectionCustody = (oneshot::Sender<(Id, ChildTerminal)>, Box<dyn Any + Send>);
    static PROJECTION_CUSTODY: Mutex<Option<ProjectionCustody>> = Mutex::new(None);

    struct StartupTerminationWake {
        panic: Mutex<Option<Box<dyn Any + Send>>>,
    }

    impl Wake for StartupTerminationWake {
        fn wake(self: Arc<Self>) {
            let payload = self
                .panic
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("the actual termination publication owns one original cause");
            resume_unwind(payload);
        }
    }

    enum ChildDisposition {
        Stop,
        Continue,
        RejectInitialization(Option<Vec<u64>>),
    }

    struct ProjectionChild {
        disposition: ChildDisposition,
        entries: Arc<Vec<u64>>,
    }

    #[actor(message = Never, error = Vec<u64>)]
    impl ProjectionChild {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "the generated fold retains its exact controlled-error boundary"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            match &mut self.disposition {
                ChildDisposition::RejectInitialization(error) => {
                    Err(error.take().expect("one original initialization rejection"))
                }
                ChildDisposition::Stop => Ok(Actions::stop()),
                ChildDisposition::Continue => Ok(Actions::cont()),
            }
        }
    }

    struct ProjectionParent {
        first: CreationId,
        first_disposition: Option<ChildDisposition>,
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
                disposition: self
                    .first_disposition
                    .take()
                    .expect("one original first child disposition"),
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

    impl NativeHost for ProjectionSpaces {
        type Runtime = Runtime;
        fn runtime(&self) -> &Self::Runtime {
            const RUNTIME: Runtime = Runtime::new();
            &RUNTIME
        }
    }

    impl RetirementSchema for ChildTerminal {
        type Failure =
            ChildFailure<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>;
    }
    impl
        super::NativeChild<
            StopOnShutdown<ProjectionParent>,
            ChildHead,
            StopOnShutdown<ProjectionChild>,
            ChildTerminal,
            Never,
        > for ProjectionSpaces
    {
        fn registration(
            &self,
        ) -> ActorSpace<<StopOnShutdown<ProjectionChild> as behavior::Behavior>::Protocol> {
            ActorSpace::new()
        }
        fn establish<Origin, OriginFn>(
            pending: super::PendingChild<
                '_,
                StopOnShutdown<ProjectionParent>,
                Self,
                ChildHead,
                StopOnShutdown<ProjectionChild>,
                ChildTerminal,
                Never,
                OriginFn,
            >,
        ) -> impl Future<
            Output = ItemSettlement<
                RoutedCreation<MailAddr, StopOnShutdown<ProjectionChild>>,
                ChildCreationOutcome<StopOnShutdown<ProjectionChild>, ChildHead>,
                behavior::CreationRejection,
                Never,
            >,
        > + Send
        where
            ChildTerminal: ProjectTerminal<Origin, StopOnShutdown<ProjectionChild>, Descendants = Never>
                + Send
                + 'static,
            Origin: Send + 'static,
            OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
        {
            <Self as super::EstablishNativeChild<
                StopOnShutdown<ProjectionParent>,
                StopOnShutdown<ProjectionChild>,
                Never,
            >>::establish_native_child::<ChildHead, ChildTerminal, Origin, OriginFn>(
                pending
            )
        }
    }

    struct ChildTerminal {
        origin: ChildOrigin<ProjectionParent, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
    }

    impl ProjectTerminal<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>
        for ChildTerminal
    {
        type Descendants = Never;
        fn project(
            origin: ChildOrigin<ProjectionParent, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
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

    impl RetirementSchema for StartupChildRetirement {
        type Failure =
            ChildFailure<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>;
    }
    impl
        super::NativeChild<
            StopOnShutdown<ProjectionParent>,
            ChildHead,
            StopOnShutdown<ProjectionChild>,
            StartupChildRetirement,
            Never,
        > for ProjectionSpaces
    {
        fn registration(
            &self,
        ) -> ActorSpace<<StopOnShutdown<ProjectionChild> as behavior::Behavior>::Protocol> {
            ActorSpace::new()
        }
        fn establish<Origin, OriginFn>(
            pending: super::PendingChild<
                '_,
                StopOnShutdown<ProjectionParent>,
                Self,
                ChildHead,
                StopOnShutdown<ProjectionChild>,
                StartupChildRetirement,
                Never,
                OriginFn,
            >,
        ) -> impl Future<
            Output = ItemSettlement<
                RoutedCreation<MailAddr, StopOnShutdown<ProjectionChild>>,
                ChildCreationOutcome<StopOnShutdown<ProjectionChild>, ChildHead>,
                behavior::CreationRejection,
                Never,
            >,
        > + Send
        where
            StartupChildRetirement: ProjectTerminal<Origin, StopOnShutdown<ProjectionChild>, Descendants = Never>
                + Send
                + 'static,
            Origin: Send + 'static,
            OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
        {
            <Self as super::EstablishNativeChild<
                StopOnShutdown<ProjectionParent>,
                StopOnShutdown<ProjectionChild>,
                Never,
            >>::establish_native_child::<ChildHead, StartupChildRetirement, Origin, OriginFn>(
                pending,
            )
        }
    }

    struct StartupChildRetirement {
        origin: ChildOrigin<ProjectionParent, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
    }

    impl ProjectTerminal<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>
        for StartupChildRetirement
    {
        type Descendants = Never;
        fn project(
            origin: ChildOrigin<ProjectionParent, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
        ) -> Self {
            Self { origin, retirement }
        }
    }

    struct LedgerCreator {
        pending: CreationId,
        committed: CreationId,
        ledgers: Vec<StopOnShutdown<ShutdownLedger>>,
    }

    #[actor(message = Never, births = { ledger: StopOnShutdown<ShutdownLedger> }, creation_settlements = retain_for_retirement)]
    impl LedgerCreator {
        #[expect(
            clippy::unnecessary_wraps,
            reason = "the owning actor initialization contract returns BehaviorActed even when this creator's error is uninhabited"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            let mut ledgers = self.ledgers.drain(..);
            Ok(Actions::new(
                NoSends,
                Creations::one(CreateChild::birth(
                    self.pending,
                    ledgers.next().expect("pending ledger definition"),
                ))
                .and(CreateChild::birth(
                    self.committed,
                    ledgers.next().expect("committed ledger definition"),
                )),
                Step::Continue,
            ))
        }
    }

    #[derive(ActorSpaces)]
    #[expect(
        dead_code,
        reason = "the derived source namespace names the creator protocol; this child kernel test does not publish a root actor"
    )]
    struct LedgerSpaces {
        creators: ActorSpace<LedgerCreator>,
    }

    impl NativeHost for LedgerSpaces {
        type Runtime = Runtime;
        fn runtime(&self) -> &Self::Runtime {
            const RUNTIME: Runtime = Runtime::new();
            &RUNTIME
        }
    }

    impl RetirementSchema for LedgerChildRetirement {
        type Failure =
            ChildFailure<ChildOrigin<LedgerCreator, ChildHead>, StopOnShutdown<ShutdownLedger>>;
    }
    impl
        super::NativeChild<
            LedgerCreator,
            ChildHead,
            StopOnShutdown<ShutdownLedger>,
            LedgerChildRetirement,
            Never,
        > for LedgerSpaces
    {
        fn registration(
            &self,
        ) -> ActorSpace<<StopOnShutdown<ShutdownLedger> as behavior::Behavior>::Protocol> {
            ActorSpace::new()
        }
        fn establish<Origin, OriginFn>(
            pending: super::PendingChild<
                '_,
                LedgerCreator,
                Self,
                ChildHead,
                StopOnShutdown<ShutdownLedger>,
                LedgerChildRetirement,
                Never,
                OriginFn,
            >,
        ) -> impl Future<
            Output = ItemSettlement<
                RoutedCreation<MailAddr, StopOnShutdown<ShutdownLedger>>,
                ChildCreationOutcome<StopOnShutdown<ShutdownLedger>, ChildHead>,
                behavior::CreationRejection,
                Never,
            >,
        > + Send
        where
            LedgerChildRetirement: ProjectTerminal<Origin, StopOnShutdown<ShutdownLedger>, Descendants = Never>
                + Send
                + 'static,
            Origin: Send + 'static,
            OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
        {
            <Self as super::EstablishNativeChild<
                LedgerCreator,
                StopOnShutdown<ShutdownLedger>,
                Never,
            >>::establish_native_child::<ChildHead, LedgerChildRetirement, Origin, OriginFn>(
                pending,
            )
        }
    }

    struct LedgerChildRetirement {
        origin: ChildOrigin<LedgerCreator, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>,
    }

    impl ProjectTerminal<ChildOrigin<LedgerCreator, ChildHead>, StopOnShutdown<ShutdownLedger>>
        for LedgerChildRetirement
    {
        type Descendants = Never;
        fn project(
            origin: ChildOrigin<LedgerCreator, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>,
        ) -> Self {
            Self { origin, retirement }
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep creation authority and every original accepted/rejected message in one end-to-end trace"
    )]
    fn dependent_ledger_delivery_preserves_original_messages_across_creation_authority() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("actual local ledger executor");
        runtime.block_on(async {
            let mut sequence = CreationSequence::new();
            let pending = sequence.issue().expect("pending ledger ID");
            let committed = sequence.issue().expect("committed ledger ID");
            let missing = sequence.issue().expect("known but unbound ledger ID");
            let mut creator = LedgerCreator {
                pending, committed,
                ledgers: vec![ShutdownLedger { entries: vec![2, 3], received: Vec::new() }.stop_on_shutdown(), ShutdownLedger { entries: vec![5, 7], received: Vec::new() }.stop_on_shutdown()],
            };
            let actions = behavior::initialize(&mut creator).unwrap_or_else(|_| panic!("pure creator emits original ledger definitions"));
            assert!(creator.ledgers.is_empty());
            let mut batch = Some(actions.creates);
            let (control, source_owner, source_mailbox, source_receiver) = mailbox_channel::<<LedgerCreator as behavior::Behavior>::Event, User<MailAddr, Never>>(Config::new(1));
            let (terminal_sender, terminal_receiver) = oneshot::channel();
            let spaces = Arc::new(LedgerSpaces { creators: ActorSpace::new() });
            let mut capabilities = ApplicationCapabilities::<LedgerCreator, LedgerSpaces, NoParent, ChildBindings<LedgerCreator, LedgerChildRetirement, StructuralOrigins<LedgerCreator>>, StructuralOrigins<LedgerCreator>>::new_with_bindings(ApplicationCapabilityInputs {
                address: MailAddr::APPLICATION_ROOT,
                actor_spaces: spaces.clone(),
                allocations: ApplicationAddresses::new(), control,
mailbox_capacity: MailboxCapacity::default(),
                timers: LocalTimers::new(), observations: TerminationObservations::new(), terminal_reports: LocalTerminalReports::new(terminal_sender),
            }, ChildBinding::new_in(spaces.as_ref()));
            let mut routed = None;
            <_ as InterpretItem<Creations<CreateChild<MailAddr, StopOnShutdown<ShutdownLedger>>>, (), Here>>::interpret_item(&mut capabilities, &mut batch, &mut routed).await;
            let Some(ItemSettlement::Accepted(routed)) = routed else { panic!("exact ledger requests were routed"); };
            let mut requests = routed.into_iter();
            let pending_request = requests.next().expect("pending request");
            let committed_request = requests.next().expect("committed request");
            assert!(requests.next().is_none());
            let pending_route = pending_request.route();
            let committed_route = committed_request.route();
            let startup_poll = {
                let mut establishing = pin!(<_ as behavior::EstablishChild<ChildHead, StopOnShutdown<ShutdownLedger>>>::establish_child(&mut capabilities, pending_request));
                establishing.as_mut().poll(&mut Context::from_waker(Waker::noop()))
            };
            assert!(matches!(startup_poll, Poll::Pending));
            let blocked_message = vec![11, 13, 17];
            let blocked_allocation = blocked_message.as_ptr();
            let mut blocked = Some(ChildDelivery::<LedgerProtocol, ChildHead>::after(pending, blocked_message));
            let mut blocked_reply = None;
            <_ as InterpretItem<ChildDelivery<LedgerProtocol, ChildHead>, (), Here>>::interpret_item(&mut capabilities, &mut blocked, &mut blocked_reply).await;
            let Some(ItemSettlement::Blocked { item, prerequisite }) = blocked_reply else { panic!("uncommitted ledger grants no delivery authority"); };
            assert_eq!(prerequisite.id(), pending);
            assert_eq!(item.creation, pending);
            assert_eq!(item.message.as_ptr(), blocked_allocation);
            assert_eq!(item.message, [11, 13, 17]);
            assert!(blocked.is_none());
            let missing_message = vec![19, 23, 29];
            let missing_allocation = missing_message.as_ptr();
            let mut absent = Some(ChildDelivery::<LedgerProtocol, ChildHead>::after(missing, missing_message));
            let mut absent_reply = None;
            <_ as InterpretItem<ChildDelivery<LedgerProtocol, ChildHead>, (), Here>>::interpret_item(&mut capabilities, &mut absent, &mut absent_reply).await;
            let Some(ItemSettlement::Rejected { item, reason: ChildDeliveryReason::MissingBinding }) = absent_reply else { panic!("unbound ID returns original request"); };
            assert_eq!(item.creation, missing);
            assert_eq!(item.message.as_ptr(), missing_allocation);
            assert_eq!(item.message, [19, 23, 29]);
            let reply = <_ as behavior::EstablishChild<ChildHead, StopOnShutdown<ShutdownLedger>>>::establish_child(&mut capabilities, committed_request).await;
            let ItemSettlement::Accepted(ChildCreationOutcome::Established(receipt)) = reply else { panic!("actual fresh ledger commits"); };
            assert_eq!(receipt.id(), committed);
            let endpoint = receipt.actor().recipient().interpret(&mut ExtractLocalEndpoint);
            let committed_address = endpoint.address();
            let delivered_message = vec![31, 37, 41];
            let delivered_allocation = delivered_message.as_ptr();
            let mut delivery = Some(ChildDelivery::<LedgerProtocol, ChildHead>::after(committed, delivered_message));
            let mut delivery_reply = None;
            <_ as InterpretItem<ChildDelivery<LedgerProtocol, ChildHead>, (), Here>>::interpret_item(&mut capabilities, &mut delivery, &mut delivery_reply).await;
            assert!(matches!(delivery_reply, Some(ItemSettlement::Accepted(()))));
            for _ in 0..8 { tokio::task::yield_now().await; }
            let authority = <_ as crate::local::children::ChildBindingAt<ChildHead>>::creation(capabilities.child_bindings.as_ref().expect("same creator row"), committed);
            let Some(crate::local::children::ChildAuthority::Established { control, .. }) = authority else { panic!("only genuine commit grants shutdown capability"); };
            control.send(EventLayer::Owned(ShutdownRequested)).unwrap_or_else(|_| panic!("live ledger admits its explicit shutdown"));
            let termination = endpoint.termination().await;
            assert!(termination.is_ok());
            let closed_message = vec![43, 47, 53];
            let closed_allocation = closed_message.as_ptr();
            let mut closed = Some(ChildDelivery::<LedgerProtocol, ChildHead>::after(committed, closed_message));
            let mut closed_reply = None;
            <_ as InterpretItem<ChildDelivery<LedgerProtocol, ChildHead>, (), Here>>::interpret_item(&mut capabilities, &mut closed, &mut closed_reply).await;
            let Some(ItemSettlement::Rejected { item, reason: ChildDeliveryReason::ClosedRecipient }) = closed_reply else { panic!("stopped ledger returns original payload"); };
            assert_eq!(item.creation, committed);
            assert_eq!(item.message.as_ptr(), closed_allocation);
            assert_eq!(item.message, [43, 47, 53]);
            let retirement = capabilities.retire().await;
            let (children, failures) = retirement.descendants;
            assert!(failures.is_empty());
            let [pending_child, committed_child] = children.as_slice() else { panic!("both original child definitions remain owned"); };
            assert_eq!(pending_child.origin.nonce(), pending_route);
            assert_eq!(pending_child.retirement.behavior().expect("pending actor returned").base().entries, [2, 3]);
            assert_eq!(committed_child.origin.nonce(), committed_route);
            assert_eq!(committed_child.origin.address(), committed_address);
            let ledger = committed_child.retirement.behavior().expect("committed ledger returned").base();
            assert_eq!(ledger.entries, [5, 7]);
            let [received] = ledger.received.as_slice() else { panic!("one original message was processed once"); };
            assert_eq!(received.from, MailAddr::APPLICATION_ROOT);
            assert_eq!(received.message.as_ptr(), delivered_allocation);
            assert_eq!(received.message, [31, 37, 41]);
            drop((source_owner, source_mailbox, source_receiver, terminal_receiver));
        });
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the actual pending startup, parent retirement and complete original custody oracle together"
    )]
    fn pending_creation_cancellation_retains_child_custody() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual local actor executor");
        let entries = Arc::new(vec![71, 73, 79]);
        let original_allocation = entries.as_ptr();
        let original_owner = Arc::downgrade(&entries);
        let notification_cause = Arc::new(vec![109_u64, 113]);
        let notification_owner = Arc::downgrade(&notification_cause);
        let notification_payload: Box<dyn Any + Send> = Box::new(notification_cause);
        let notification_identity = ptr::from_ref(notification_payload.as_ref()).cast::<()>();
        let notification_waker = Waker::from(Arc::new(StartupTerminationWake {
            panic: Mutex::new(Some(notification_payload)),
        }));
        let mut creations = CreationSequence::new();
        let previous = creations
            .issue()
            .expect("one explicit previous incarnation identity");
        let creation_id = creations
            .issue()
            .expect("one later actual creation identity");
        let mut batch = Some(Creations::one(CreateChild::replacement(
            creation_id,
            previous,
            ProjectionChild {
                disposition: ChildDisposition::Continue,
                entries,
            }
            .stop_on_shutdown(),
        )));
        let (pending, retirement, progress, route, allocated_address, registered, report_before_projection, terminated) = runtime.block_on(async {
            let spaces = Arc::new(ProjectionSpaces {
                parents: ActorSpace::new(),
            });
            let (control, source_owner, source_mailbox, source_receiver) =
                mailbox_channel::<
                    <StopOnShutdown<ProjectionParent> as behavior::Behavior>::Event,
                    User<MailAddr, Never>,
                >(Config::new(1));
            let (terminal_sender, terminal_receiver) = oneshot::channel();
            let mut capabilities = ApplicationCapabilities::<
                StopOnShutdown<ProjectionParent>,
                ProjectionSpaces,
                NoParent,
                ChildBindings<
                    StopOnShutdown<ProjectionParent>,
                    StartupChildRetirement,
                    StructuralOrigins<ProjectionParent>,
                >,
                StructuralOrigins<ProjectionParent>,
            >::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr::APPLICATION_ROOT,
                    actor_spaces: spaces.clone(),
                    allocations: ApplicationAddresses::new(),
mailbox_capacity: MailboxCapacity::default(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(terminal_sender),
                },
                ChildBinding::new_in(spaces.as_ref()),
            );
            let mut routed = None;
            <_ as InterpretItem<
                Creations<CreateChild<MailAddr, StopOnShutdown<ProjectionChild>>>,
                (),
                Here,
            >>::interpret_item(&mut capabilities, &mut batch, &mut routed)
            .await;
            let Some(ItemSettlement::Accepted(routed)) = routed else {
                panic!("the real batch reservation returns its original routed child");
            };
            let request = routed
                .into_one()
                .unwrap_or_else(|_| panic!("one original child keeps exactly one routed request"));
            let route = request.route();
            let mut progress = Some(InterpretationProgress::Interpreting(
                CreationInterpretationCustody::Children(vec![(Some(request), None)]),
            ));
            let pending = {
                let mut attempt = pin!(
                    <Births<StopOnShutdown<ProjectionChild>> as InterpretCreations<
                        MailAddr,
                        _,
                        (),
                        Here,
                    >>::interpret_creations(&mut progress, &mut capabilities)
                );
                attempt
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let authority = <_ as crate::local::children::ChildBindingAt<ChildHead>>::creation(
                capabilities.child_bindings.as_ref().expect("original binding owner remains installed"), creation_id);
            assert!(matches!(authority, Some(crate::local::children::ChildAuthority::Unavailable(id)) if id == creation_id));
            let binding = <_ as crate::local::children::ChildCreationAt<ChildHead>>::creation_namespace(capabilities.child_bindings.as_mut().expect("actual pending binding owner remains")).0.get_mut(&creation_id)
                .expect("the actual pending child has its reserved row");
            let crate::local::children::CreationBinding::Starting { address, startup, .. } = binding else {
                panic!("the pending child retains its actual raw startup allocation");
            };
            let allocated_address = *address;
            let (endpoint, acknowledgement) = startup.await.expect("actual private startup receipt");
            let mut termination = pin!(endpoint.termination());
            let registered = termination.as_mut().poll(&mut Context::from_waker(&notification_waker));
            // Refuse this original private ACK after registering the observer;
            // the endpoint remains unavailable as an established child.
            drop(acknowledgement);
            assert_ne!(creation_id.get(), route, "creator identity and actual reserved route are distinct facts");
            let mut capabilities = Some(capabilities);
            let mut retirement = None;
            let mut report_before_projection = None;
            loop {
                let polled = {
                    let mut receiving = pin!(<_ as RetireCapabilities>::receive_retirement(
                        &mut capabilities, &mut retirement));
                    receiving.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                };
                if let Some(owner) = capabilities.as_mut() {
                    let namespace = <_ as ChildCreationAt<ChildHead>>::creation_namespace(
                        owner.child_bindings.as_mut().expect("same original child owner"));
                    if let Some(CreationBinding::RetiringStartup {
                        custody: StartupRetirementCustody::Projection {
                            retirement_report, ..
                        }, ..
                    }) = namespace.0.get(&creation_id) {
                        report_before_projection.get_or_insert(*retirement_report);
                    }
                }
                if matches!(polled, Poll::Ready(())) { break; }
                tokio::task::yield_now().await;
            }
            let retirement = retirement.expect("actual native startup and parent capability cleanup");
            let terminated = termination.await;
            drop(endpoint);
            drop((
                source_owner,
                source_mailbox,
                source_receiver,
                terminal_receiver,
            ));
            (pending, retirement, progress, route, allocated_address, registered, report_before_projection, terminated)
        });
        drop(runtime);
        assert!(matches!(registered, Poll::Pending));
        assert!(terminated.is_err());
        let report_before_projection =
            report_before_projection.expect("joined report precedes raw startup projection");
        assert_eq!(
            report_before_projection.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            report_before_projection.failures(),
            ActorFailureAssessment::FailuresFound,
            "raw startup report lost its known joined failure assessment"
        );
        let [ChildFailure::StartupTerminationNotificationFailed { origin, error }] =
            retirement.descendants.1.as_slice()
        else {
            panic!("raw startup projection lost its original termination notification cause");
        };
        assert_eq!(origin.creation_id(), creation_id);
        assert_eq!(origin.address(), allocated_address);
        let RetirementNotificationError::Panicked { payload } = error else {
            panic!("actual notification panic must not become a disappeared-producer cause");
        };
        assert!(ptr::eq(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            notification_identity
        ));
        assert_eq!(notification_owner.strong_count(), 1);
        assert!(matches!(pending, Poll::Pending));
        let Some(InterpretationProgress::Interpreting(CreationInterpretationCustody::Children(
            mut rows,
        ))) = progress
        else {
            panic!("one pending original creation retains its routed custody row");
        };
        assert_eq!(rows.len(), 1);
        let (request, received) = rows.pop().expect("one original routed row");
        eprintln!(
            "original pending creation: request={}, reply={}, joined={}, child failures={}, surviving definitions={}",
            request.is_some(),
            received.is_some(),
            retirement.descendants.0.len(),
            retirement.descendants.1.len(),
            original_owner.strong_count()
        );
        let returned = match (request, received) {
            (Some(request), None) => Some(request),
            (
                None,
                Some(ItemSettlement::Rejected { item, .. } | ItemSettlement::Corrupt { item, .. }),
            ) => Some(item),
            (
                None,
                Some(ItemSettlement::Accepted(
                    ChildCreationOutcome::InitializationRejected { creation, .. }
                    | ChildCreationOutcome::InitializationPanicked { creation }
                    | ChildCreationOutcome::HostRejected { creation, .. },
                )),
            ) => Some(creation),
            (None, Some(ItemSettlement::Accepted(ChildCreationOutcome::Established(child)))) => {
                assert_eq!(child.id(), creation_id);
                assert_eq!(child.kind(), CreationKind::replacement(previous));
                None
            }
            (None, Some(ItemSettlement::Accepted(ChildCreationOutcome::Unavailable { .. }))) => {
                panic!("ordinary pending cancellation must retain its original child definition");
            }
            (None, Some(ItemSettlement::Blocked { prerequisite, .. })) => match prerequisite {},
            (None, None) => None,
            (Some(_), Some(_)) => panic!("one creation cannot own both request and reply"),
        };
        let returned_entries = returned.map(|request| {
            assert_eq!(request.id(), creation_id);
            assert_eq!(request.kind(), CreationKind::replacement(previous));
            assert_eq!(request.route(), route);
            let (request, _) = request.into_parts();
            let (_, child, _) = request.into_parts();
            child.into_inner().entries
        });
        assert!(
            returned_entries.is_none(),
            "parent-owned pending startup completes into joined original child custody"
        );
        let entries = match (&returned_entries, retirement.descendants.0.as_slice()) {
            (Some(entries), []) => entries,
            (None, [terminal]) => {
                assert_eq!(terminal.origin.nonce(), route);
                assert_eq!(terminal.origin.address(), allocated_address);
                assert_eq!(terminal.origin.creation_id(), creation_id);
                assert_eq!(
                    terminal.origin.creation_kind(),
                    CreationKind::replacement(previous)
                );
                let declared = terminal
                    .origin
                    .into_declared_child::<ProjectionParentChildrenChild>();
                assert_eq!(declared.address(), allocated_address);
                assert_eq!(declared.nonce(), route);
                assert_eq!(declared.creation_id(), creation_id);
                assert_eq!(
                    declared.creation_kind(),
                    CreationKind::replacement(previous)
                );
                let child = terminal
                    .retirement
                    .behavior()
                    .expect("joined child retirement retains its original definition");
                &child.base().entries
            }
            (None, []) => panic!(
                "disposing a pending original creation must retain its original definition in progress, exact reply, or joined child retirement"
            ),
            _ => panic!("one original child definition has exactly one retained owner"),
        };
        assert_eq!(entries.as_ptr(), original_allocation);
        assert_eq!(entries.as_slice(), [71, 73, 79]);
        let [terminal] = retirement.descendants.0.as_slice() else {
            panic!("one complete original raw child survives");
        };
        let ActorRetirement::BindingAbandoned {
            custody,
            initialization,
        } = &terminal.retirement
        else {
            panic!("the real unacknowledged private startup retains its full native disposition");
        };
        assert_eq!(
            custody.behavior.base().entries.as_ptr(),
            original_allocation
        );
        let InterpretationProgress::Original(actions) = initialization else {
            panic!("uncommitted startup retains the original complete initialization Actions");
        };
        assert_eq!(actions.sends, SendLayer::new(NoSends, NoSends));
        assert!(actions.creates.is_empty());
        assert_eq!(actions.become_, Step::Continue);
        drop(retirement);
        assert_eq!(original_owner.strong_count(), 0);
        assert_eq!(notification_owner.strong_count(), 0);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one finite native projector controller joins owners before complete custody oracles"
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
            first_disposition: Some(ChildDisposition::Stop),
            later,
            first_entries: Some(first_entries),
            later_entries: Some(later_entries),
        }
        .stop_on_shutdown();
        let (reported, parent_poll, shutdown, received, remaining_owner, termination_notification) =
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
                                actor_spaces: actor_spaces.clone(),
                                allocations: ApplicationAddresses::new(),
                                mailbox_capacity: MailboxCapacity::default(),
                                control,
                                timers,
                                observations,
                                terminal_reports,
                            },
                            ChildBinding::new_in(actor_spaces.as_ref()),
                        ))
                    },
                )
                .await
                .unwrap_or_else(|_| panic!("the actual parent publishes both committed births"));
                let mut owned = Some(root.task);
                let mut received = None;
                let mut termination_notification = None;
                let reported = projected.await;
                // The eager synchronous callback transfers then unwinds in the same
                // task poll. No await separates publication from its native panic.
                let parent_poll = {
                    let mut waiting = pin!(OwnedTask::receive_finish(
                        &mut owned,
                        &mut received,
                        &mut termination_notification
                    ));
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                };
                let shutdown = match root.shutdown_control.upgrade() {
                    Some(control) => control.send(EventLayer::Owned(ShutdownRequested)),
                    None => panic!("the parent remains live until its explicit shutdown"),
                };
                OwnedTask::receive_finish(&mut owned, &mut received, &mut termination_notification)
                    .await;
                drop(root.actor);
                (
                    reported,
                    parent_poll,
                    shutdown,
                    received,
                    owned,
                    termination_notification,
                )
            });
        drop(runtime);
        let stale_custody = PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(stale_custody);
        // Native root join and executor cleanup precede every custody oracle.
        assert!(remaining_owner.is_none());
        assert!(matches!(termination_notification, Some(Ok(()))));
        assert!(matches!(parent_poll, Poll::Pending));
        assert!(shutdown.is_ok());
        let (projection_task, first_terminal) =
            reported.expect("the actual first child transferred its complete retirement");
        let parent_report = ActorRetirementReport::from_joined(
            received.as_ref().expect("actual parent join"),
            termination_notification
                .as_ref()
                .expect("actual first notification"),
        );
        assert_eq!(
            parent_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            parent_report.failures(),
            ActorFailureAssessment::FailuresFound
        );
        let outcome = received
            .expect("the actual root join was acquired")
            .expect("the projector panic must not substitute for the parent task result");
        let retirement = ActorRetirement::from_local(outcome);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures: mut failures,
                    capability_failures,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                    unread_owner_cancellation,
                },
            interpretation,
            source,
            settlements,
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
            notifications:
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            origin,
            actor,
            error,
        } = failure
        else {
            panic!("the actual native cause belongs to the projection task");
        };
        assert_eq!(origin.creation_id(), first);
        assert_eq!(origin.creation_kind(), CreationKind::Birth);
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
        assert_eq!(later_terminal.origin.creation_id(), later);
        assert_eq!(later_terminal.origin.creation_kind(), CreationKind::Birth);
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
                    custody:
                        ActorCustody {
                            behavior,
                            control,
                            user,
                            descendants,
                            child_failures,
                            capability_failures,
                            additional_failures,
                            received_interpretation,
                            received_source,
                            source_index,
                            acquired_ingress,
                            retirement_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        },
                    interpretation,
                    source,
                    settlements,
                    completion,
                } => {
                    assert_eq!(child_failures.as_slice(), []);
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
                    custody:
                        ActorCustody {
                            behavior,
                            control,
                            user,
                            descendants,
                            child_failures,
                            capability_failures,
                            additional_failures,
                            received_interpretation,
                            received_source,
                            source_index,
                            acquired_ingress,
                            retirement_failures,
                            terminal_report,
                            unread_owner_cancellation,
                        },
                    interpretation,
                    source,
                    settlements,
                } => {
                    assert_eq!(child_failures.as_slice(), []);
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
                ChildDisposition::RejectInitialization(_) => {
                    panic!("this control initializes both child actors successfully");
                }
            }
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(behavior.base().entries.as_slice(), entries);
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert_eq!(control.len(), 0);
            assert_eq!(user.len(), 0);
            assert_eq!(descendants.as_slice(), []);
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
    type StartupProjectionCustody = (
        oneshot::Sender<(Id, StartupProjectionRetirement)>,
        Box<dyn Any + Send>,
    );
    static STARTUP_PROJECTION_CUSTODY: Mutex<Option<StartupProjectionCustody>> = Mutex::new(None);

    impl RetirementSchema for StartupProjectionRetirement {
        type Failure =
            ChildFailure<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>;
    }
    impl
        super::NativeChild<
            StopOnShutdown<ProjectionParent>,
            ChildHead,
            StopOnShutdown<ProjectionChild>,
            StartupProjectionRetirement,
            Never,
        > for ProjectionSpaces
    {
        fn registration(
            &self,
        ) -> ActorSpace<<StopOnShutdown<ProjectionChild> as behavior::Behavior>::Protocol> {
            ActorSpace::new()
        }
        fn establish<Origin, OriginFn>(
            pending: super::PendingChild<
                '_,
                StopOnShutdown<ProjectionParent>,
                Self,
                ChildHead,
                StopOnShutdown<ProjectionChild>,
                StartupProjectionRetirement,
                Never,
                OriginFn,
            >,
        ) -> impl Future<
            Output = ItemSettlement<
                RoutedCreation<MailAddr, StopOnShutdown<ProjectionChild>>,
                ChildCreationOutcome<StopOnShutdown<ProjectionChild>, ChildHead>,
                behavior::CreationRejection,
                Never,
            >,
        > + Send
        where
            StartupProjectionRetirement: ProjectTerminal<Origin, StopOnShutdown<ProjectionChild>, Descendants = Never>
                + Send
                + 'static,
            Origin: Send + 'static,
            OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
        {
            <Self as super::EstablishNativeChild<
                StopOnShutdown<ProjectionParent>,
                StopOnShutdown<ProjectionChild>,
                Never,
            >>::establish_native_child::<ChildHead, StartupProjectionRetirement, Origin, OriginFn>(
                pending,
            )
        }
    }

    struct StartupProjectionRetirement {
        origin: ChildOrigin<ProjectionParent, ChildHead>,
        retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
    }
    impl ProjectTerminal<ChildOrigin<ProjectionParent, ChildHead>, StopOnShutdown<ProjectionChild>>
        for StartupProjectionRetirement
    {
        type Descendants = Never;
        fn project(
            origin: ChildOrigin<ProjectionParent, ChildHead>,
            retirement: ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
        ) -> Self {
            match retirement.behavior().map(|child| &child.base().disposition) {
                Some(ChildDisposition::Continue) => {
                    let (publication, payload) = STARTUP_PROJECTION_CUSTODY.lock().unwrap_or_else(PoisonError::into_inner).take().expect("one actual startup projection owns its complete report and native cause");
                    let transferred = publication.send((id(), Self { origin, retirement }));
                    transferred.unwrap_or_else(|original| {
                        drop(original);
                        panic!("original startup report receiver remains live");
                    });
                    resume_unwind(payload)
                }
                _ => Self { origin, retirement },
            }
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one actual quiet startup preserves complete settlement lanes and joined sibling custody"
    )]
    fn quiet_startup_keeps_failure_assessment_without_a_native_failure_row() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("local executor");
        let first_entries = Arc::new(vec![71, 73]);
        let later_entries = Arc::new(vec![79, 83]);
        let first_allocation = first_entries.as_ptr();
        let later_allocation = later_entries.as_ptr();
        let first_owner = Arc::downgrade(&first_entries);
        let later_owner = Arc::downgrade(&later_entries);
        let original_error = vec![89, 97];
        let error_allocation = original_error.as_ptr();
        let mut creations = CreationSequence::new();
        let first = creations.issue().expect("first issued creation");
        let later = creations.issue().expect("later issued creation");
        let spaces = ProjectionSpaces {
            parents: ActorSpace::new(),
        };
        let roots = spaces.parents.clone();
        let parent = ProjectionParent {
            first,
            first_disposition: Some(ChildDisposition::RejectInitialization(Some(original_error))),
            later,
            first_entries: Some(first_entries),
            later_entries: Some(later_entries),
        }
        .stop_on_shutdown();
        let (joined, notification) = runtime.block_on(async move {
            let actor_spaces = Arc::new(spaces);
            let root = spawn_root_with(
                roots,
                Config::new(2),
                MailAddr::APPLICATION_ROOT,
                parent,
                move |control, terminal_reports, timers, observations| {
                    let bindings = ChildBinding::new_in(actor_spaces.as_ref());
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
                            mailbox_capacity: MailboxCapacity::default(),
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        bindings,
                    ))
                },
            )
            .await
            .unwrap_or_else(|_| panic!("actual parent admission"));
            let shutdown = root
                .shutdown_control
                .upgrade()
                .expect("live parent control")
                .send(EventLayer::Owned(ShutdownRequested));
            let retirement = root.task.finish().await;
            assert!(shutdown.is_ok());
            drop(root.actor);
            retirement
        });
        drop(runtime);
        let report = ActorRetirementReport::from_joined(&joined, &notification);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
        assert!(notification.is_ok());
        let native = ActorRetirement::from_local(joined.expect("actual original actor join"));
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    behavior,
                    descendants,
                    child_failures: failures,
                    capability_failures,
                    additional_failures,
                    retirement_failures,
                    ..
                },
            settlements,
            completion,
            ..
        } = native
        else {
            panic!("the original parent completes its explicit stop");
        };
        assert_eq!(completion, Completion::Stopped);
        assert!(failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(retirement_failures.is_empty());
        let parent = behavior.into_inner();
        assert_eq!((parent.first, parent.later), (first, later));
        assert!(parent.first_disposition.is_none());
        assert!(parent.first_entries.is_none());
        assert!(parent.later_entries.is_none());
        let [stopped, initialized] = settlements
            .try_into()
            .unwrap_or_else(|_| panic!("ordered stop and initialization settlements"));
        assert_eq!(stopped.sends, SendLayer::new(NoSends, NoSends));
        assert_eq!(stopped.become_, Step::Stop(Stopped));
        let CreationSettlement::Settled(stopped_creations) = stopped.creations.into_settlement()
        else {
            panic!("complete stop creation lane");
        };
        assert!(stopped_creations.is_empty());
        assert_eq!(initialized.sends, SendLayer::new(NoSends, NoSends));
        assert_eq!(initialized.become_, Step::Continue);
        let CreationSettlement::Settled(reports) = initialized.creations.into_settlement() else {
            panic!("complete initialization creation lane");
        };
        let reports: Vec<_> = reports.into_iter().collect();
        let [rejected, established] = reports
            .try_into()
            .unwrap_or_else(|_| panic!("both original creation settlements"));
        let SettledItem::Attempted(ItemSettlement::Accepted(
            ChildCreationOutcome::InitializationRejected { creation, error },
        )) = rejected
        else {
            panic!("Core retains the original quiet initialization rejection");
        };
        assert_eq!(creation.id(), first);
        assert_eq!(creation.kind(), CreationKind::Birth);
        let (creation, _) = creation.into_parts();
        let (_, rejected_child, _) = creation.into_parts();
        assert_eq!(rejected_child.base().entries.as_ptr(), first_allocation);
        assert_eq!(rejected_child.base().entries.as_slice(), &[71, 73]);
        assert!(matches!(
            rejected_child.base().disposition,
            ChildDisposition::RejectInitialization(None)
        ));
        assert_eq!(error.as_ptr(), error_allocation);
        assert_eq!(error, vec![89, 97]);
        let SettledItem::Attempted(ItemSettlement::Accepted(ChildCreationOutcome::Established(
            established,
        ))) = established
        else {
            panic!("the independent later sibling commits");
        };
        assert_eq!(established.id(), later);
        assert_eq!(established.kind(), CreationKind::Birth);
        let endpoint = established
            .actor()
            .into_recipient()
            .interpret(&mut ExtractLocalEndpoint);
        let [later_terminal] = descendants.as_slice() else {
            panic!("one actual joined later sibling");
        };
        assert_eq!(later_terminal.origin.address(), endpoint.address());
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    behavior,
                    child_failures,
                    descendants,
                    capability_failures,
                    additional_failures,
                    retirement_failures,
                    ..
                },
            settlements,
            ..
        } = &later_terminal.retirement
        else {
            panic!("parent retirement joins the independent sibling");
        };
        assert_eq!(behavior.base().entries.as_ptr(), later_allocation);
        assert_eq!(behavior.base().entries.as_slice(), &[79, 83]);
        assert!(matches!(
            behavior.base().disposition,
            ChildDisposition::Continue
        ));
        assert_eq!(child_failures.as_slice(), []);
        assert_eq!(settlements.len(), 0);
        assert_eq!(descendants.as_slice(), []);
        assert!(capability_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert!(retirement_failures.is_empty());
        assert_eq!(first_owner.strong_count(), 1);
        assert_eq!(later_owner.strong_count(), 1);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one native trace proves both interrupted receiving loans and later-child retirement preserve the original affine values"
    )]
    fn pending_startup_projection_failure_preserves_raw_retry_projection_retry_and_later_child() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual local actor executor");
        let first_entries = Arc::new(vec![83, 89]);
        let later_entries = Arc::new(vec![97, 101]);
        let first_allocation = first_entries.as_ptr();
        let later_allocation = later_entries.as_ptr();
        let payload = Box::new(String::from("original pending startup projector cause"));
        let payload_allocation = ptr::from_ref(payload.as_ref()).addr();
        let notification_cause = Arc::new(vec![127_u64, 131]);
        let notification_owner = Arc::downgrade(&notification_cause);
        let notification_payload: Box<dyn Any + Send> = Box::new(notification_cause);
        let notification_identity = ptr::from_ref(notification_payload.as_ref()).cast::<()>();
        let notification_waker = Waker::from(Arc::new(StartupTerminationWake {
            panic: Mutex::new(Some(notification_payload)),
        }));
        let (publication, projected) = oneshot::channel();
        let previous_custody = STARTUP_PROJECTION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace((publication, payload));
        drop(previous_custody);
        let mut sequence = CreationSequence::new();
        let first = sequence.issue().expect("two actual creations fit");
        let later = sequence.issue().expect("two actual creations fit");
        let previous = sequence.issue().expect("explicit predecessor fits");
        let (
            retirement,
            reported,
            first_route,
            later_route,
            registered,
            report_before_projection,
            terminated,
        ) = runtime.block_on(async move {
            let spaces = Arc::new(ProjectionSpaces {
                parents: ActorSpace::new(),
            });
            let (control, source_owner, source_mailbox, source_receiver) =
                mailbox_channel::<
                    <StopOnShutdown<ProjectionParent> as behavior::Behavior>::Event,
                    User<MailAddr, Never>,
                >(Config::new(1));
            let (terminal_sender, terminal_receiver) = oneshot::channel();
            let mut capabilities = ApplicationCapabilities::<
                StopOnShutdown<ProjectionParent>,
                ProjectionSpaces,
                NoParent,
                ChildBindings<
                    StopOnShutdown<ProjectionParent>,
                    StartupProjectionRetirement,
                    StructuralOrigins<ProjectionParent>,
                >,
                StructuralOrigins<ProjectionParent>,
            >::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr::APPLICATION_ROOT,
                    actor_spaces: spaces.clone(),
                    allocations: ApplicationAddresses::new(),
                    mailbox_capacity: MailboxCapacity::default(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(terminal_sender),
                },
                ChildBinding::new_in(spaces.as_ref()),
            );
            let mut batch = Some(Creations::from_iter([
                CreateChild::replacement(
                    first,
                    previous,
                    ProjectionChild {
                        disposition: ChildDisposition::Continue,
                        entries: first_entries,
                    }
                    .stop_on_shutdown(),
                ),
                CreateChild::birth(
                    later,
                    ProjectionChild {
                        disposition: ChildDisposition::Stop,
                        entries: later_entries,
                    }
                    .stop_on_shutdown(),
                ),
            ]));
            let mut routed = None;
            <_ as InterpretItem<
                Creations<CreateChild<MailAddr, StopOnShutdown<ProjectionChild>>>,
                (),
                Here,
            >>::interpret_item(&mut capabilities, &mut batch, &mut routed)
            .await;
            let Some(ItemSettlement::Accepted(routed)) = routed else {
                panic!("the real reservation returns both original requests");
            };
            let mut routed = routed.into_iter();
            let first_request = routed.next().expect("first original routed request");
            let later_request = routed.next().expect("later original routed request");
            assert!(routed.next().is_none());
            let first_route = first_request.route();
            let later_route = later_request.route();
            let mut first_progress = Some(InterpretationProgress::Interpreting(
                CreationInterpretationCustody::Children(vec![(Some(first_request), None)]),
            ));
            let startup_poll = {
                let mut startup = pin!(
                    <Births<StopOnShutdown<ProjectionChild>> as InterpretCreations<
                        MailAddr,
                        _,
                        (),
                        Here,
                    >>::interpret_creations(
                        &mut first_progress, &mut capabilities
                    )
                );
                startup
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            assert!(matches!(startup_poll, Poll::Pending));
            let (first_endpoint, acknowledgement) = {
                let namespace = <_ as ChildCreationAt<ChildHead>>::creation_namespace(
                    capabilities
                        .child_bindings
                        .as_mut()
                        .expect("same pending child namespace"),
                );
                let Some(CreationBinding::Starting { startup, .. }) = namespace.0.get_mut(&first)
                else {
                    panic!("the real unpublished child retains its original private receiver");
                };
                startup
                    .await
                    .expect("the actual first private startup receipt")
            };
            let mut termination = pin!(first_endpoint.termination());
            let registered = termination
                .as_mut()
                .poll(&mut Context::from_waker(&notification_waker));
            drop(acknowledgement);
            let later_reply = <_ as behavior::EstablishChild<
                ChildHead,
                StopOnShutdown<ProjectionChild>,
            >>::establish_child(&mut capabilities, later_request)
            .await;
            let ItemSettlement::Accepted(ChildCreationOutcome::Established(later_receipt)) =
                later_reply
            else {
                panic!("later child was actually committed independently");
            };
            assert_eq!(later_receipt.id(), later);
            let expected_endpoint = later_receipt
                .actor()
                .recipient()
                .interpret(&mut ExtractLocalEndpoint);
            let authority = <_ as crate::local::children::ChildBindingAt<ChildHead>>::creation(
                capabilities
                    .child_bindings
                    .as_ref()
                    .expect("same registry retains later committed authority"),
                later,
            );
            let Some(crate::local::children::ChildAuthority::Established {
                endpoint, kind, ..
            }) = authority
            else {
                panic!("only actual established row grants endpoint and control authority");
            };
            assert_eq!(endpoint.address(), expected_endpoint.address());
            assert_eq!(kind, later_receipt.kind());
            let mut capabilities = Some(capabilities);
            let mut retirement = None;
            let raw_poll = {
                let mut receiving = pin!(<_ as RetireCapabilities>::receive_retirement(
                    &mut capabilities,
                    &mut retirement
                ));
                receiving
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            assert!(matches!(raw_poll, Poll::Pending));
            let mut projection_polls = 0;
            let report_before_projection;
            let mut polled = raw_poll;
            loop {
                let owner = capabilities
                    .as_mut()
                    .expect("the original parent survives receiving disposal");
                let row =
                    <_ as crate::local::children::ChildCreationAt<ChildHead>>::creation_namespace(
                        owner
                            .child_bindings
                            .as_mut()
                            .expect("exact bindings remain outside receiving"),
                    )
                    .0
                    .get_mut(&first)
                    .expect("original pending row remains owned");
                match row {
                    crate::local::children::CreationBinding::RetiringStartup {
                        custody:
                            crate::local::children::StartupRetirementCustody::Projection {
                                task: Some(_),
                                received: None,
                                retirement_report,
                                ..
                            },
                        ..
                    } => {
                        assert!(matches!(polled, Poll::Pending));
                        report_before_projection = *retirement_report;
                        break;
                    }
                    crate::local::children::CreationBinding::RetiringStartup {
                        custody: crate::local::children::StartupRetirementCustody::Raw { .. },
                        ..
                    } => {
                        assert!(matches!(polled, Poll::Pending));
                        projection_polls += 1;
                        assert!(projection_polls < 16);
                    }
                    _ => panic!(
                        "startup retirement retains exactly its current original receiving value"
                    ),
                }
                tokio::task::yield_now().await;
                polled = {
                    let mut receiving = pin!(<_ as RetireCapabilities>::receive_retirement(
                        &mut capabilities,
                        &mut retirement
                    ));
                    receiving
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                };
            }
            <_ as RetireCapabilities>::receive_retirement(&mut capabilities, &mut retirement).await;
            assert!(capabilities.is_none());
            let reported = projected
                .await
                .expect("one original projected startup retirement");
            let terminated = termination.await;
            drop(first_endpoint);
            drop((
                source_owner,
                source_mailbox,
                source_receiver,
                terminal_receiver,
            ));
            (
                retirement.expect("all original child custody retired"),
                reported,
                first_route,
                later_route,
                registered,
                report_before_projection,
                terminated,
            )
        });
        drop(runtime);
        assert!(matches!(registered, Poll::Pending));
        assert!(terminated.is_err());
        assert_eq!(
            report_before_projection.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            report_before_projection.failures(),
            ActorFailureAssessment::FailuresFound,
            "raw startup report must exist before the actual projector can panic"
        );
        let (projection_task, first_terminal) = reported;
        assert_eq!(first_terminal.origin.nonce(), first_route);
        assert_eq!(first_terminal.origin.creation_id(), first);
        assert_eq!(
            first_terminal.origin.creation_kind(),
            CreationKind::replacement(previous)
        );
        let first_child = first_terminal
            .retirement
            .behavior()
            .expect("original uncommitted child survives total projection");
        assert_eq!(first_child.base().entries.as_ptr(), first_allocation);
        let ActorRetirement::BindingAbandoned {
            custody,
            initialization,
        } = &first_terminal.retirement
        else {
            panic!("projector panic must retain the complete original raw child disposition");
        };
        assert_eq!(custody.behavior.base().entries.as_slice(), [83, 89]);
        let InterpretationProgress::Original(actions) = initialization else {
            panic!("projector panic must retain original uncommitted initialization Actions");
        };
        assert_eq!(actions.sends, SendLayer::new(NoSends, NoSends));
        assert!(actions.creates.is_empty());
        assert_eq!(actions.become_, Step::Continue);
        let mut descendants = retirement.descendants.0;
        let later_terminal = descendants
            .pop()
            .expect("later child retires beside the native projection error");
        assert!(descendants.is_empty());
        assert_eq!(later_terminal.origin.nonce(), later_route);
        assert_eq!(later_terminal.origin.creation_id(), later);
        assert_eq!(later_terminal.origin.creation_kind(), CreationKind::Birth);
        assert_eq!(
            later_terminal
                .retirement
                .behavior()
                .expect("later original actor survives")
                .base()
                .entries
                .as_ptr(),
            later_allocation
        );
        let mut failures = retirement.descendants.1;
        let failure = failures
            .pop()
            .expect("exact startup projector cause survives");
        assert!(failures.is_empty());
        let ChildFailure::StartupProjectionTaskFailed {
            origin,
            error,
            termination_notification,
        } = failure
        else {
            panic!("startup failure grants no invented endpoint authority");
        };
        assert_eq!(origin.creation_id(), first);
        assert_eq!(origin.creation_kind(), CreationKind::replacement(previous));
        assert_eq!(origin.nonce(), first_route);
        assert_eq!(error.id(), projection_task);
        let Err(RetirementNotificationError::Panicked { payload }) = &termination_notification
        else {
            panic!("raw projector panic erased the original termination notification cause");
        };
        assert!(ptr::eq(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            notification_identity
        ));
        assert_eq!(notification_owner.strong_count(), 1);
        let payload = error.into_panic();
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>().addr(),
            payload_allocation
        );
        drop(termination_notification);
        assert_eq!(notification_owner.strong_count(), 0);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one executor-shutdown trace retains the surviving creator future and exact native failure through receipt and retirement"
    )]
    fn normal_native_startup_failure_retains_original_join_error_without_a_definition() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual child host");
        let entries = Arc::new(vec![103, 107]);
        let original_owner = Arc::downgrade(&entries);
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one actual child");
        let spaces = Arc::new(ProjectionSpaces {
            parents: ActorSpace::new(),
        });
        let (control, source_owner, source_mailbox, source_receiver) = mailbox_channel::<
            <StopOnShutdown<ProjectionParent> as behavior::Behavior>::Event,
            User<MailAddr, Never>,
        >(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let mut capabilities = ApplicationCapabilities::<
            StopOnShutdown<ProjectionParent>,
            ProjectionSpaces,
            NoParent,
            ChildBindings<
                StopOnShutdown<ProjectionParent>,
                StartupChildRetirement,
                StructuralOrigins<ProjectionParent>,
            >,
            StructuralOrigins<ProjectionParent>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr::APPLICATION_ROOT,
                actor_spaces: spaces.clone(),
                allocations: ApplicationAddresses::new(),
                mailbox_capacity: MailboxCapacity::default(),
                control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(terminal_sender),
            },
            ChildBinding::new_in(spaces.as_ref()),
        );
        let request = RoutedCreation::new(
            CreateChild::birth(
                id,
                ProjectionChild {
                    disposition: ChildDisposition::Continue,
                    entries,
                }
                .stop_on_shutdown(),
            ),
            113,
        );
        let after = {
            let mut establishing = pin!(<_ as behavior::EstablishChild<
                ChildHead,
                StopOnShutdown<ProjectionChild>,
            >>::establish_child(&mut capabilities, request));
            let first_poll = {
                let entered = runtime.enter();
                let first_poll = establishing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                drop(entered);
                first_poll
            };
            assert!(matches!(first_poll, Poll::Pending));
            // The ordinary creator future and its original parent loan survive
            // actual child-host shutdown; only that host's native actor task dies.
            drop(runtime);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                establishing
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }))
        };
        assert_eq!(original_owner.strong_count(), 0);
        let row = <_ as crate::local::children::ChildCreationAt<ChildHead>>::creation_namespace(
            capabilities
                .child_bindings
                .as_mut()
                .expect("original parent binding scope remains owned"),
        )
        .0
        .get_mut(&id);
        let Some(crate::local::children::CreationBinding::RetiringStartup {
            address,
            custody:
                crate::local::children::StartupRetirementCustody::Raw {
                    task: None,
                    received: Some(Err(error)),
                    termination_notification: Some(_),
                },
            ..
        }) = row
        else {
            panic!(
                "native startup failure must preserve its original acquired JoinError in the outside-owned raw row"
            );
        };
        assert!(error.is_cancelled());
        let Ok(Poll::Ready(ItemSettlement::Accepted(outcome))) = after else {
            panic!("native startup failure settles through its exact unavailable receipt");
        };
        let Err(ChildCreationOutcome::Unavailable {
            id: received_id,
            kind,
        }) = outcome.into_committed()
        else {
            panic!("unavailable receipt grants no committed child");
        };
        assert_eq!(received_id, id);
        assert_eq!(kind, CreationKind::Birth);
        let native_task = error.id();
        let allocated_address = *address;
        let mut capabilities = Some(capabilities);
        let mut retirement = None;
        let retired = {
            let mut receiving = pin!(<_ as RetireCapabilities>::receive_retirement(
                &mut capabilities,
                &mut retirement
            ));
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(matches!(retired, Poll::Ready(())));
        assert!(capabilities.is_none());
        let retirement = retirement.expect("parent acquires complete native startup failure");
        assert!(retirement.descendants.0.is_empty());
        let mut failures = retirement.descendants.1;
        let ChildFailure::StartupActorTaskFailed {
            origin,
            error,
            termination_notification,
        } = failures.pop().expect("exact native child task error")
        else {
            panic!("native startup failure claims no established actor");
        };
        assert!(failures.is_empty());
        assert_eq!(origin.creation_id(), id);
        assert_eq!(origin.creation_kind(), CreationKind::Birth);
        assert_eq!(origin.nonce(), 113);
        assert_eq!(origin.address(), allocated_address);
        assert_eq!(error.id(), native_task);
        assert!(error.is_cancelled());
        drop((
            source_owner,
            source_mailbox,
            source_receiver,
            terminal_receiver,
        ));
        assert!(
            matches!(
                termination_notification,
                Err(RetirementNotificationError::ReceiptClosed { .. })
            ),
            "native raw JoinError erased its independent original termination receipt"
        );
    }

    #[test]
    fn child_origin_layout() {
        eprintln!(
            "LAYOUT origin {} {} root {} {} failure {} {} report {} {} binding {} {} retirement {} {}",
            core::mem::size_of::<ChildOrigin<ProjectionParent, ChildHead>>(),
            core::mem::align_of::<ChildOrigin<ProjectionParent, ChildHead>>(),
            core::mem::size_of::<crate::terminal::RootOrigin<ProjectionParent>>(),
            core::mem::align_of::<crate::terminal::RootOrigin<ProjectionParent>>(),
            core::mem::size_of::<
                ChildFailure<
                    ChildOrigin<ProjectionParent, ChildHead>,
                    StopOnShutdown<ProjectionChild>,
                >,
            >(),
            core::mem::align_of::<
                ChildFailure<
                    ChildOrigin<ProjectionParent, ChildHead>,
                    StopOnShutdown<ProjectionChild>,
                >,
            >(),
            core::mem::size_of::<StartupChildRetirement>(),
            core::mem::align_of::<StartupChildRetirement>(),
            core::mem::size_of::<
                crate::local::children::CreationBinding<
                    StopOnShutdown<ProjectionChild>,
                    StartupChildRetirement,
                    Never,
                    Vec<Never>,
                >,
            >(),
            core::mem::align_of::<
                crate::local::children::CreationBinding<
                    StopOnShutdown<ProjectionChild>,
                    StartupChildRetirement,
                    Never,
                    Vec<Never>,
                >,
            >(),
            core::mem::size_of::<ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>>(
            ),
            core::mem::align_of::<
                ActorRetirement<StopOnShutdown<ProjectionChild>, Never, Vec<Never>>,
            >()
        );
    }
}

#[cfg(test)]
mod child_rejection_custody {
    use crate::actors::ActorExt;
    use crate::behavior::{
        Actions, BehaviorActed, BehaviorBase, CreateChild, CreationKind, CreationSequence,
        Creations, Never,
    };
    use crate::lifecycle::StopOnShutdown;
    use crate::{ActorCustody, ActorRetirement, Application};

    struct RefusedWorker {
        payload: Box<[u8]>,
    }
    #[derive(Debug, PartialEq)]
    enum WorkerError {
        InitializationRefused,
    }
    #[crate::actor(message = Never, error = WorkerError)]
    impl RefusedWorker {
        #[expect(
            clippy::unused_self,
            reason = "the actor initialization contract borrows the original definition even when its policy always refuses"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            Err(WorkerError::InitializationRefused)
        }
    }
    struct Parent {
        child: Option<CreateChild<crate::MailAddr, StopOnShutdown<RefusedWorker>>>,
    }
    #[derive(Debug)]
    enum ParentError {
        ChildAlreadyTransferred,
    }
    #[crate::actor(message = Never, error = ParentError, births = { worker: StopOnShutdown<RefusedWorker> }, creation_settlements = retain_for_retirement)]
    impl Parent {
        fn init(&mut self) -> BehaviorActed<Self> {
            let child = self
                .child
                .take()
                .ok_or(ParentError::ChildAlreadyTransferred)?;
            Ok(Actions::create(Creations::one(child)))
        }
    }

    crate::topology! { worker_application {
        root: Parent { Worker(ParentChildrenWorker) => worker; }
        worker: StopOnShutdown<RefusedWorker> {}
    } }

    #[tokio::test(flavor = "current_thread")]
    async fn refused_initialization_preserves_original_child_work_identity_and_provenance() {
        let payload: Box<[u8]> = Box::from([7, 13]);
        let allocation = payload.as_ptr();
        let mut sequence = CreationSequence::new();
        let previous = sequence
            .issue()
            .expect("prior incarnation correlation fits");
        let id = sequence
            .issue()
            .expect("one child fits the creator namespace");
        let work = Box::new(String::from("caller work survived"));
        let work_allocation = work.as_ptr();
        let root = Parent {
            child: Some(CreateChild::replacement(
                id,
                previous,
                RefusedWorker { payload }.stop_on_shutdown(),
            )),
        }
        .stop_on_shutdown();
        let (outcome, native, notifications, ()) =
            Application::with_topology(root, worker_application::Topology::new())
                .run_with(move |application| async move {
                    application
                        .lifecycle()
                        .request_shutdown()
                        .expect("shutdown admitted");
                    work
                })
                .await
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("entered host");
                })
                .into_parts();
        let (work, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
            drop(original);
            panic!("work returned independently of cleanup");
        });
        assert_eq!(work.as_ptr(), work_allocation);
        assert_eq!(&*work, "caller work survived");
        cleanup.expect("parent cleanup completed");
        let (_, retirement) = native.expect("parent retirement joined");
        let notifications = notifications.expect("parent original notifications transferred");
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    descendants,
                    child_failures: failures,
                    ..
                },
            settlements,
            ..
        } = retirement
        else {
            panic!("parent completed");
        };
        assert!(descendants.is_empty());
        let mut returned = Vec::new();
        for action in settlements {
            let crate::behavior::CreationSettlement::Settled(creations) =
                action.creations.into_settlement()
            else {
                panic!("routed creation batch");
            };
            for creation in creations {
                let crate::behavior::SettledItem::Attempted(
                    crate::behavior::ItemSettlement::Accepted(
                        crate::behavior::ChildCreationOutcome::InitializationRejected {
                            creation,
                            error,
                        },
                    ),
                ) = creation
                else {
                    panic!("exact child initialization refusal");
                };
                assert_eq!(error, WorkerError::InitializationRefused);
                let (creation, route) = creation.into_parts();
                let (returned_id, child, kind) = creation.into_parts();
                assert_eq!(returned_id, id);
                assert_eq!(kind, CreationKind::Replacement { previous });
                assert_eq!(child.base().payload.as_ptr(), allocation);
                assert_eq!(&*child.base().payload, &[7, 13]);
                returned.push((returned_id, kind, route));
            }
        }
        assert_eq!(returned.len(), 1);
        assert!(
            failures.is_empty(),
            "no independent startup failure exists after the complete refusal returned"
        );
    }
}
