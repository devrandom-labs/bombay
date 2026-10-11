use crate::address::MailAddr;
use crate::launch::{ActorSpace, OwnedTask, ProjectedTask};
use crate::local::endpoint::{ActorRef, InstalledActor};
use crate::local::environment::LocalActivationRejection;
use crate::terminal::{ActorRetirement, ChildFailure, ChildOrigin, LocalOutcome, ProjectTerminal};
use crate::termination::Termination;
use behavior::{
    Behavior, BehaviorBase, BehaviorSettlements, BirthMode, ChildHead, ChildOccurrenceMember,
    ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences, ChildTail, CreationId,
    CreationKind, EstablishedActor, Never, Protocol,
};
use bombay_engine::DriverError;
use communication::ControlSender;
use core::future::poll_fn;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;
use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

pub(crate) trait ChildOriginAt<Position, Child: Behavior> {
    type Origin: Send + 'static;

    fn origin(address: MailAddr, nonce: u64, id: CreationId, kind: CreationKind) -> Self::Origin;
}

impl<Owner: 'static, Position: 'static, Child: Behavior> ChildOriginAt<Position, Child>
    for StructuralOrigins<Owner>
{
    type Origin = ChildOrigin<Owner, Position>;

    fn origin(address: MailAddr, nonce: u64, id: CreationId, kind: CreationKind) -> Self::Origin {
        ChildOrigin::new(address, nonce, id, kind)
    }
}

pub(crate) struct DirectChildFailures<Owner>(PhantomData<fn() -> Owner>);
impl<Owner> ChildOccurrenceShape for DirectChildFailures<Owner> {
    type Empty = ();
}
impl<Owner, Position, Child, Tail> ChildOccurrenceMember<Position, Child, Tail>
    for DirectChildFailures<Owner>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    type Member = (Vec<ChildFailure<ChildOrigin<Owner, Position>, Child>>, Tail);
}
pub(crate) type ChildStartupFailures<Child> =
    ChildOccurrences<ChildNode<Child>, DirectChildFailures<<Child as BehaviorBase>::Base>>;

impl<Root, Origins> ChildOccurrenceShape for RuntimeChildBindings<Root, Origins> {
    type Empty = NoChildBindings<Root>;
}
impl<Root, Origins, Position, Child, Tail> ChildOccurrenceMember<Position, Child, Tail>
    for RuntimeChildBindings<Root, Origins>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    type Member = ChildBinding<Position, Child, Root, Origins, Tail>;
}

impl<Root> Default for NoChildBindings<Root> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<Position, Child, Root, Origins, Tail> Default
    for ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
    Tail: Default,
{
    fn default() -> Self {
        Self {
            creations: HashMap::new(),
            creation_order: Vec::new(),
            actors: ActorSpace::new(),
            tail: Some(Tail::default()),
            position: PhantomData,
        }
    }
}

pub(crate) enum ChildAuthority<'a, Child>
where
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    Established {
        endpoint: &'a ActorRef<Child::Protocol>,
        control: &'a ControlSender<Child::Event>,
        kind: CreationKind,
    },
    Unavailable(CreationId),
}

/// Borrowed authority of one structural child occurrence.
pub(crate) trait ChildBindingAt<Position> {
    type Child: Behavior<Protocol: Protocol<Addr = MailAddr>>;
    fn creation(&self, id: CreationId) -> Option<ChildAuthority<'_, Self::Child>>;
}

impl<Bindings, Position> ChildBindingAt<Position> for Bindings
where
    Bindings: ChildBindingAtCursor<Position, Position>,
{
    type Child = Bindings::Child;
    fn creation(&self, id: CreationId) -> Option<ChildAuthority<'_, Self::Child>> {
        self.creation_at(id)
    }
}

pub(crate) trait ChildBindingAtCursor<Target, Cursor> {
    type Child: Behavior<Protocol: Protocol<Addr = MailAddr>>;
    fn creation_at(&self, id: CreationId) -> Option<ChildAuthority<'_, Self::Child>>;
}

impl<Target, Child, Root, Origins, Tail> ChildBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Origins, Tail>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    type Child = Child;
    fn creation_at(&self, id: CreationId) -> Option<ChildAuthority<'_, Child>> {
        self.creations.get(&id).map(|binding| match binding {
            CreationBinding::Established {
                kind,
                endpoint,
                control,
                ..
            } => ChildAuthority::Established {
                endpoint,
                control,
                kind: *kind,
            },
            CreationBinding::Starting { .. }
            | CreationBinding::RetiringStartup { .. }
            | CreationBinding::Rejected
            | CreationBinding::StartupRejected { .. } => ChildAuthority::Unavailable(id),
        })
    }
}

impl<Target, Cursor, Position, Head, Root, Origins, Tail>
    ChildBindingAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Origins, Tail>
where
    Head: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Head>: ChildOccurrenceProduct<DirectChildFailures<Head::Base>>,
    Tail: ChildBindingAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;
    fn creation_at(&self, id: CreationId) -> Option<ChildAuthority<'_, Self::Child>> {
        self.tail
            .as_ref()
            .expect("live child occurrence retains its tail")
            .creation_at(id)
    }
}

/// Exclusive startup custody of one structural child occurrence.
pub(crate) trait ChildCreationAt<Position> {
    type Child: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth: BirthMode<
                Child: ChildOccurrenceProduct<
                    DirectChildFailures<<Self::Child as BehaviorBase>::Base>,
                >,
            >,
        > + BehaviorBase;
    type Root;
    type Origins;

    fn creation_mut(
        &mut self,
        id: CreationId,
    ) -> Option<&mut CreationBinding<Self::Child, Self::Root>>;
    fn take_creation(&mut self, id: CreationId) -> CreationBinding<Self::Child, Self::Root>;
    fn record_creation(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    );
    fn child_actors(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol>;
}

impl<Bindings, Position> ChildCreationAt<Position> for Bindings
where
    Bindings: ChildCreationAtCursor<Position, Position>,
{
    type Child = Bindings::Child;
    type Root = Bindings::Root;
    type Origins = Bindings::Origins;

    fn creation_mut(
        &mut self,
        id: CreationId,
    ) -> Option<&mut CreationBinding<Self::Child, Self::Root>> {
        self.creation_mut_at(id)
    }
    fn take_creation(&mut self, id: CreationId) -> CreationBinding<Self::Child, Self::Root> {
        self.take_creation_at(id)
    }
    fn record_creation(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    ) {
        self.record_creation_at(id, binding);
    }

    fn child_actors(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol> {
        self.child_actors_at()
    }
}

pub(crate) trait ChildCreationAtCursor<Target, Cursor> {
    type Child: BehaviorSettlements<
            Protocol: Protocol<Addr = MailAddr>,
            Ph = Never,
            Birth: BirthMode<
                Child: ChildOccurrenceProduct<
                    DirectChildFailures<<Self::Child as BehaviorBase>::Base>,
                >,
            >,
        > + BehaviorBase;
    type Root;
    type Origins;

    fn creation_mut_at(
        &mut self,
        id: CreationId,
    ) -> Option<&mut CreationBinding<Self::Child, Self::Root>>;
    fn take_creation_at(&mut self, id: CreationId) -> CreationBinding<Self::Child, Self::Root>;
    fn record_creation_at(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    );
    fn child_actors_at(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol>;
}

impl<Target, Child, Root, Origins, Tail> ChildCreationAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Origins, Tail>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    type Child = Child;
    type Root = Root;
    type Origins = Origins;

    fn creation_mut_at(
        &mut self,
        id: CreationId,
    ) -> Option<&mut CreationBinding<Self::Child, Self::Root>> {
        self.creations.get_mut(&id)
    }
    fn take_creation_at(&mut self, id: CreationId) -> CreationBinding<Self::Child, Self::Root> {
        self.creations
            .remove(&id)
            .expect("the creator owns this exact startup row")
    }
    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding<Child, Root>) {
        match self.creations.entry(id) {
            Entry::Occupied(_) => {
                panic!("one child creation identity settles once per occurrence")
            }
            Entry::Vacant(entry) => {
                if match &binding {
                    CreationBinding::Starting { .. }
                    | CreationBinding::RetiringStartup { .. }
                    | CreationBinding::Established { .. } => true,
                    CreationBinding::StartupRejected {
                        primary_failure,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                        ..
                    } => {
                        primary_failure.is_some()
                            || !additional_failures.is_empty()
                            || terminal_report.is_some()
                            || !retirement_failures.is_empty()
                    }
                    CreationBinding::Rejected => false,
                } {
                    if !self.creation_order.contains(&id) {
                        self.creation_order.push(id);
                    }
                } else {
                    self.creation_order.retain(|owned| *owned != id);
                }
                entry.insert(binding);
            }
        }
    }

    fn child_actors_at(&self) -> ActorSpace<Child::Protocol> {
        self.actors.clone()
    }
}

impl<Target, Cursor, Position, Head, Root, Origins, Tail>
    ChildCreationAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Origins, Tail>
where
    Head: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Head>: ChildOccurrenceProduct<DirectChildFailures<Head::Base>>,
    Tail: ChildCreationAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;
    type Root = Tail::Root;
    type Origins = Tail::Origins;

    fn creation_mut_at(
        &mut self,
        id: CreationId,
    ) -> Option<&mut CreationBinding<Self::Child, Self::Root>> {
        self.tail
            .as_mut()
            .expect("live occurrence retains its tail")
            .creation_mut_at(id)
    }
    fn take_creation_at(&mut self, id: CreationId) -> CreationBinding<Self::Child, Self::Root> {
        self.tail
            .as_mut()
            .expect("live occurrence retains its tail")
            .take_creation_at(id)
    }
    fn record_creation_at(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    ) {
        self.tail
            .as_mut()
            .expect("live child occurrence retains its tail")
            .record_creation_at(id, binding);
    }

    fn child_actors_at(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol> {
        self.tail
            .as_ref()
            .expect("live child occurrence retains its tail")
            .child_actors_at()
    }
}

/// Derive the same exact binding representation for a newly installed child.
pub(crate) trait NestedChildBindings<Child>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    type Bindings: Default + RetireChildTasks<Failures = ChildStartupFailures<Child>>;
}

impl<Child, Bindings> NestedChildBindings<Child> for Bindings
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
    Bindings: RetireChildTasks,
    ChildNode<Child>: ChildOccurrenceProduct<
        RuntimeChildBindings<Bindings::Root, StructuralOrigins<Child::Base>>,
    >,
    ChildBindings<Child, Bindings::Root, StructuralOrigins<Child::Base>>:
        Default + RetireChildTasks<Failures = ChildStartupFailures<Child>>,
{
    type Bindings = ChildBindings<Child, Bindings::Root, StructuralOrigins<Child::Base>>;
}

pub(crate) trait RetireChildTasks {
    type Root;
    type Failures: Send;

    fn request_retirement(&mut self);

    /// Construct the exact initially empty ordered failure product. This is
    /// partial receiving custody, not evidence that children retired.
    fn retirement_failures() -> Self::Failures;

    /// Loan all original task/creation custody while acquiring each joined
    /// row in the caller's ordered receiving lanes. Remaining input is kept.
    fn receive_retirement(
        bindings: &mut Option<Self>,
        retired: &mut Vec<Self::Root>,
        failures: &mut Self::Failures,
    ) -> impl core::future::Future<Output = ()> + Send
    where
        Self: Sized;

    #[cfg(test)]
    fn retire_child_tasks(
        self,
    ) -> impl core::future::Future<Output = (Vec<Self::Root>, Self::Failures)> + Send;
}

impl<Root> RetireChildTasks for NoChildBindings<Root> {
    type Root = Root;
    type Failures = ();

    fn request_retirement(&mut self) {}

    fn retirement_failures() {}

    #[expect(
        clippy::manual_async_fn,
        reason = "this empty binding future captures only bindings; an async function would capture unused root destinations and require Root: Send"
    )]
    fn receive_retirement(
        bindings: &mut Option<Self>,
        _retired: &mut Vec<Self::Root>,
        _failures: &mut Self::Failures,
    ) -> impl core::future::Future<Output = ()> + Send {
        async move {
            *bindings = None;
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    #[cfg(test)]
    async fn retire_child_tasks(self) -> (Vec<Self::Root>, ()) {
        (Vec::new(), ())
    }
}

impl<Position, Child, Root, Origins, Tail> RetireChildTasks
    for ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
    ChildStartupFailures<Child>: Send + 'static,
    Child::InterpretationCustody: Send + 'static,
    Child::SourceCustody: Send + 'static,
    Child::Sends: Send + 'static,
    Child::Settlements: Send + 'static,
    <Child::Birth as BirthMode>::Child: Send + 'static,
    Child: Send + 'static,
    Child::Protocol: Send + Sync,
    <Child::Protocol as behavior::Protocol>::Msg: Send,
    Child::Event: Send + 'static,
    Child::Error: Send + 'static,
    Root: ProjectTerminal<Origins::Origin, ActorRetirement<Child, Root, ChildStartupFailures<Child>>>
        + Send
        + 'static,
    Origins: ChildOriginAt<Position, Child>,
    Tail: RetireChildTasks<Root = Root> + Send,
{
    type Root = Root;
    type Failures = (Vec<ChildFailure<Origins::Origin, Child>>, Tail::Failures);

    fn request_retirement(&mut self) {
        for id in &self.creation_order {
            let binding = self
                .creations
                .get_mut(id)
                .expect("ordered child creation retains its owned binding");
            match binding {
                CreationBinding::Starting { task, .. }
                | CreationBinding::RetiringStartup {
                    custody: StartupRetirementCustody::Raw { task, .. },
                    ..
                } => {
                    if let Some(task) = task {
                        task.request_retirement();
                    }
                }
                CreationBinding::RetiringStartup {
                    custody: StartupRetirementCustody::Projection { .. },
                    ..
                }
                | CreationBinding::Rejected
                | CreationBinding::StartupRejected { .. } => {}
                CreationBinding::Established { task, .. } => {
                    if let Some(task) = task {
                        task.request_retirement();
                    }
                }
            }
        }
        if let Some(tail) = &mut self.tail {
            tail.request_retirement();
        }
    }

    fn retirement_failures() -> Self::Failures {
        (Vec::new(), Tail::retirement_failures())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one ordered concrete child retirement traversal conserves original child state, projection failures, native causes and tail custody across every await"
    )]
    async fn receive_retirement(
        bindings: &mut Option<Self>,
        retired: &mut Vec<Self::Root>,
        failures: &mut Self::Failures,
    ) {
        let Some(owner) = bindings.as_mut() else {
            return;
        };
        for id in &owner.creation_order {
            if let Some(
                CreationBinding::Established {
                    task: Some(_),
                    joined: Some(_),
                    ..
                }
                | CreationBinding::Established {
                    task: None,
                    joined: None,
                    ..
                },
            ) = owner.creations.get(id)
            {
                return;
            }
        }
        owner.request_retirement();
        while let Some(id) = owner.creation_order.first().copied() {
            if matches!(
                owner.creations.get(&id),
                Some(CreationBinding::Starting { .. })
            ) {
                let starting = owner
                    .creations
                    .remove(&id)
                    .expect("the original startup row remains owned");
                let CreationBinding::Starting {
                    kind,
                    address,
                    route,
                    task,
                    ..
                } = starting
                else {
                    unreachable!("the selected current value is startup custody");
                };
                owner.creations.insert(
                    id,
                    CreationBinding::RetiringStartup {
                        kind,
                        address,
                        route,
                        custody: StartupRetirementCustody::Raw {
                            task,
                            received: None,
                        },
                    },
                );
            }
            let binding = owner
                .creations
                .get_mut(&id)
                .expect("ordered child creation retains its owned binding");
            let origin = match binding {
                CreationBinding::Established {
                    kind,
                    route,
                    endpoint,
                    task,
                    joined,
                    ..
                } => {
                    ProjectedTask::receive_retirement(task, joined).await;
                    if task.is_some() || joined.is_none() {
                        return;
                    }
                    // Any origin construction borrows the original binding;
                    // no whole parent is taken across this policy call.
                    Origins::origin(endpoint.address(), *route, id, *kind)
                }
                CreationBinding::RetiringStartup {
                    kind,
                    address,
                    route,
                    custody,
                    ..
                } => {
                    let origin = Origins::origin(*address, *route, id, *kind);
                    if let StartupRetirementCustody::Raw { task, received } = custody {
                        OwnedTask::receive_retirement(task, received).await;
                        if task.is_some() || received.is_none() {
                            return;
                        }
                        if matches!(received, Some(Ok(_))) {
                            let Some(Ok(outcome)) = received.take() else {
                                unreachable!("the original native outcome was acquired");
                            };
                            let task = tokio::spawn(async move {
                                Root::project(origin, ActorRetirement::from_local(outcome))
                            });
                            *custody = StartupRetirementCustody::Projection {
                                task: Some(task),
                                received: None,
                            };
                        }
                    }
                    if let StartupRetirementCustody::Projection { task, received } = custody {
                        if received.is_none() {
                            poll_fn(|context| {
                                let Some(projection) = task.as_mut() else {
                                    return Poll::Ready(());
                                };
                                match Pin::new(projection).poll(context) {
                                    Poll::Pending => Poll::Pending,
                                    Poll::Ready(joined) => {
                                        *received = Some(joined);
                                        drop(task.take());
                                        Poll::Ready(())
                                    }
                                }
                            })
                            .await;
                        }
                        if task.is_some() || received.is_none() {
                            return;
                        }
                    }
                    Origins::origin(*address, *route, id, *kind)
                }
                CreationBinding::Starting { .. } => unreachable!(
                    "the creator transferred startup to raw retirement before borrowing"
                ),
                CreationBinding::StartupRejected {
                    kind,
                    address,
                    route,
                    ..
                } => Origins::origin(*address, *route, id, *kind),
                CreationBinding::Rejected => {
                    panic!(
                        "only established tasks and owned startup retirement facts enter creation order"
                    );
                }
            };
            // Origin policy completed. All following transfers are structural,
            // and each genuine row enters the outside destination before the
            // next child/tail operation is constructed or polled.
            let binding = owner
                .creations
                .remove(&id)
                .expect("the borrowed creation row remains installed");
            match binding {
                CreationBinding::Established {
                    endpoint,
                    control,
                    joined: Some(joined),
                    ..
                } => match joined {
                    Ok(Ok(terminal)) => retired.push(terminal),
                    Ok(Err(error)) => failures.0.push(ChildFailure::ActorTaskFailed {
                        origin,
                        actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                        error,
                    }),
                    Err(error) => failures.0.push(ChildFailure::ProjectionTaskFailed {
                        origin,
                        actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                        error,
                    }),
                },
                CreationBinding::RetiringStartup { custody, .. } => match custody {
                    StartupRetirementCustody::Raw {
                        received: Some(Err(error)),
                        ..
                    } => failures
                        .0
                        .push(ChildFailure::StartupActorTaskFailed { origin, error }),
                    StartupRetirementCustody::Projection {
                        received: Some(Ok(terminal)),
                        ..
                    } => retired.push(terminal),
                    StartupRetirementCustody::Projection {
                        received: Some(Err(error)),
                        ..
                    } => failures
                        .0
                        .push(ChildFailure::StartupProjectionTaskFailed { origin, error }),
                    _ => unreachable!(
                        "only complete outside-owned startup retirement crosses extraction"
                    ),
                },
                CreationBinding::Starting { .. } => {
                    unreachable!("starting input was transferred before receiving")
                }
                CreationBinding::StartupRejected {
                    primary_failure,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    ..
                } => match primary_failure {
                    Some(DriverError::InitializationPanicked(payload)) => {
                        failures.0.push(ChildFailure::InitializationPanicked {
                            origin,
                            payload,
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        });
                    }
                    primary_failure => failures.0.push(ChildFailure::StartupRetirementFailed {
                        origin,
                        primary_failure,
                        additional_failures,
                        terminal_report,
                        retirement_failures,
                    }),
                },
                CreationBinding::Established { joined: None, .. } | CreationBinding::Rejected => {
                    unreachable!("only an observed complete row crosses structural extraction");
                }
            }
            owner.creation_order.remove(0);
        }
        Tail::receive_retirement(&mut owner.tail, retired, &mut failures.1).await;
        if owner.tail.is_none() {
            drop(bindings.take());
        }
    }

    #[cfg(test)]
    async fn retire_child_tasks(self) -> (Vec<Self::Root>, Self::Failures) {
        // Historical consuming adapter only. New affine callers keep these
        // original inputs and destinations outside their operation future.
        let mut bindings = Some(self);
        let mut retired = Vec::new();
        let mut failures = Self::retirement_failures();
        Self::receive_retirement(&mut bindings, &mut retired, &mut failures).await;
        assert!(
            bindings.is_none(),
            "the concrete child product completes retirement"
        );
        (retired, failures)
    }
}

pub(crate) struct StructuralOrigins<Owner>(PhantomData<fn() -> Owner>);

type ChildNode<Owner> = <<Owner as Behavior>::Birth as BirthMode>::Child;

/// Bombay's runtime binding representation for one direct-child birth node.
pub(crate) struct RuntimeChildBindings<Root, Origins>(PhantomData<fn() -> (Root, Origins)>);

/// Exact binding product for the direct children of `Owner`.
pub(crate) type ChildBindings<Owner, Root, Origins> =
    ChildOccurrences<ChildNode<Owner>, RuntimeChildBindings<Root, Origins>>;

/// End of one creator's direct-child binding product.
pub(crate) struct NoChildBindings<Root = behavior::Never>(PhantomData<fn() -> Root>);

/// Creation outcomes, hosting, and ordered task custody for one occurrence.
pub(crate) struct ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    creations: HashMap<CreationId, CreationBinding<Child, Root>>,
    creation_order: Vec<CreationId>,
    actors: ActorSpace<Child::Protocol>,
    tail: Option<Tail>,
    position: PhantomData<fn() -> (Position, Origins)>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "raw and projected retirement retain distinct affine payloads; ownership does not require indirection"
)]
pub(crate) enum StartupRetirementCustody<Child, Root>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    Raw {
        #[expect(
            clippy::type_complexity,
            reason = "the native task retains its exact actor and direct child retirement product"
        )]
        task: Option<OwnedTask<Child, (Vec<Root>, ChildStartupFailures<Child>)>>,
        #[expect(
            clippy::type_complexity,
            reason = "the outside destination acquires the original raw actor outcome before native task disposal"
        )]
        received: Option<
            Result<LocalOutcome<Child, (Vec<Root>, ChildStartupFailures<Child>)>, JoinError>,
        >,
    },
    Projection {
        task: Option<JoinHandle<Root>>,
        received: Option<Result<Root, JoinError>>,
    },
}

#[expect(
    clippy::large_enum_variant,
    reason = "one existing registry row owns each current custody phase; no allocation law requires added indirection"
)]
pub(crate) enum CreationBinding<Child, Root>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    ChildNode<Child>: ChildOccurrenceProduct<DirectChildFailures<Child::Base>>,
{
    Starting {
        kind: CreationKind,
        address: MailAddr,
        route: u64,
        startup: oneshot::Receiver<(ActorRef<Child::Protocol>, oneshot::Sender<()>)>,
        control: ControlSender<Child::Event>,
        #[expect(
            clippy::type_complexity,
            reason = "the native task retains its exact actor and direct child retirement product"
        )]
        task: Option<OwnedTask<Child, (Vec<Root>, ChildStartupFailures<Child>)>>,
    },
    RetiringStartup {
        kind: CreationKind,
        address: MailAddr,
        route: u64,
        custody: StartupRetirementCustody<Child, Root>,
    },
    Established {
        kind: CreationKind,
        route: u64,
        endpoint: ActorRef<Child::Protocol>,
        control: ControlSender<Child::Event>,
        task: Option<ProjectedTask<Child, Root>>,
        joined: Option<Result<Result<Root, JoinError>, JoinError>>,
    },
    Rejected,
    StartupRejected {
        kind: CreationKind,
        address: MailAddr,
        route: u64,
        primary_failure: Option<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        additional_failures: Vec<DriverError<Child::Error, LocalActivationRejection<MailAddr>>>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
    },
}

pub(crate) type NestedBindings<Bindings, Child> =
    <Bindings as NestedChildBindings<Child>>::Bindings;

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use behavior::{ChildHead, ChildTail, CreationSequence, Never};

    use crate::actor;
    use crate::local::children::StructuralOrigins;
    use crate::local::children::{
        ChildAuthority, ChildBinding, ChildBindingAt, ChildCreationAt, CreationBinding,
        NoChildBindings,
    };

    struct Worker;

    #[actor(message = Never)]
    impl Worker {}

    type LaterOccurrence = ChildBinding<
        ChildTail<ChildHead>,
        Worker,
        (),
        StructuralOrigins<Worker>,
        NoChildBindings<()>,
    >;
    type FirstOccurrence =
        ChildBinding<ChildHead, Worker, (), StructuralOrigins<Worker>, LaterOccurrence>;

    fn creation_authority<Child, Bindings>(
        bindings: &Bindings,
        id: behavior::CreationId,
    ) -> Option<ChildAuthority<'_, Child>>
    where
        Child: behavior::Behavior<Protocol: behavior::Protocol<Addr = crate::address::MailAddr>>,
        Bindings: ChildBindingAt<ChildHead, Child = Child>,
    {
        bindings.creation(id)
    }

    #[test]
    fn creation_identity_is_local_to_its_child_occurrence() {
        let mut bindings = FirstOccurrence::default();
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        <FirstOccurrence as ChildCreationAt<ChildHead>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        assert!(matches!(
            creation_authority(&bindings, creation),
            Some(ChildAuthority::Unavailable(id)) if id == creation
        ));
        assert!(
            <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::creation(
                &bindings, creation
            )
            .is_none()
        );
    }

    #[test]
    fn child_actor_space_retains_one_scope_per_occurrence() {
        let bindings = FirstOccurrence::default();
        let first = <FirstOccurrence as ChildCreationAt<ChildHead>>::child_actors(&bindings);
        let first_again = <FirstOccurrence as ChildCreationAt<ChildHead>>::child_actors(&bindings);
        let later =
            <FirstOccurrence as ChildCreationAt<ChildTail<ChildHead>>>::child_actors(&bindings);
        let later_again =
            <FirstOccurrence as ChildCreationAt<ChildTail<ChildHead>>>::child_actors(&bindings);

        assert_eq!(
            first.registration_scope_id(),
            first_again.registration_scope_id()
        );
        assert_ne!(first.registration_scope_id(), later.registration_scope_id());
        assert_eq!(
            later.registration_scope_id(),
            later_again.registration_scope_id()
        );
    }

    #[test]
    fn later_child_occurrence_resolves_its_exact_creation() {
        let mut bindings = FirstOccurrence::default();
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        <FirstOccurrence as ChildCreationAt<ChildTail<ChildHead>>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        assert!(matches!(
            <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::creation(
                &bindings, creation
            ),
            Some(ChildAuthority::Unavailable(id)) if id == creation
        ));
        assert!(
            <FirstOccurrence as ChildBindingAt<ChildHead>>::creation(&bindings, creation).is_none()
        );
    }

    #[test]
    fn duplicate_creation_keeps_the_first_exact_settlement() {
        let mut bindings = FirstOccurrence::default();
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        <FirstOccurrence as ChildCreationAt<ChildHead>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        let duplicate = catch_unwind(AssertUnwindSafe(|| {
            <FirstOccurrence as ChildCreationAt<ChildHead>>::record_creation(
                &mut bindings,
                creation,
                CreationBinding::Rejected,
            );
        }));
        assert!(duplicate.is_err());
        assert!(matches!(
            <FirstOccurrence as ChildBindingAt<ChildHead>>::creation(&bindings, creation),
            Some(ChildAuthority::Unavailable(id)) if id == creation
        ));
    }
}
