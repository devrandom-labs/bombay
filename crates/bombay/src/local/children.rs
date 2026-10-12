use crate::address::MailAddr;
use crate::launch::{ActorSpace, OwnedTask, ProjectedTask};
use crate::local::effects::NativeChild;
use crate::local::endpoint::{ActorRef, InstalledActor};
use crate::local::environment::LocalActivationRejection;
use crate::terminal::{
    ActorRetirement, ChildFailure, ChildOrigin, LocalOutcome, ProjectTerminal, RetirementSchema,
};
use crate::termination::Termination;
use behavior::{
    Behavior, BehaviorBase, BehaviorSettlements, BirthMode, ChildHead, ChildOccurrenceMember,
    ChildOccurrenceShape, ChildOccurrences, ChildTail, CreationId, CreationKind, EstablishedActor,
    Never, Protocol,
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

impl<Root: RetirementSchema, Origins> ChildOccurrenceShape for RuntimeChildBindings<Root, Origins> {
    type Empty = NoChildBindings<Root>;
}
impl<Root: RetirementSchema, Origins, Position, Child, Tail>
    ChildOccurrenceMember<Position, Child, Tail> for RuntimeChildBindings<Root, Origins>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Origins: ChildOriginAt<Position, Child>,
    Root: ProjectTerminal<Origins::Origin, Child>,
{
    type Member = ChildBinding<
        Position,
        Child,
        Root,
        Origins,
        Tail,
        <Root as ProjectTerminal<Origins::Origin, Child>>::Descendants,
    >;
}

impl<Root> Default for NoChildBindings<Root> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

pub(crate) trait ChildBindingsIn<Creator, Scope> {
    fn new_in(spaces: &Scope) -> Self;
}

impl<Creator, Scope, Root> ChildBindingsIn<Creator, Scope> for NoChildBindings<Root> {
    fn new_in(_: &Scope) -> Self {
        Self(PhantomData)
    }
}

impl<Creator, Scope, Position, Child, Root, Origins, Tail, Descendants>
    ChildBindingsIn<Creator, Scope>
    for ChildBinding<Position, Child, Root, Origins, Tail, Descendants>
where
    Creator: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
    Scope: NativeChild<Creator, Position, Child, Root, Descendants>,
    Tail: ChildBindingsIn<Creator, Scope>,
{
    fn new_in(spaces: &Scope) -> Self {
        Self {
            creations: HashMap::new(),
            creation_order: Vec::new(),
            actors: Scope::registration(spaces),
            tail: Some(Tail::new_in(spaces)),
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

impl<Target, Child, Root, Origins, Tail, Descendants> ChildBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Origins, Tail, Descendants>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
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

impl<Target, Cursor, Position, Head, Root, Origins, Tail, Descendants>
    ChildBindingAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Origins, Tail, Descendants>
where
    Head: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
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

pub(crate) fn record_child_creation<Child, Root, Descendants, Failures>(
    creations: &mut HashMap<CreationId, CreationBinding<Child, Root, Descendants, Failures>>,
    creation_order: &mut Vec<CreationId>,
    id: CreationId,
    binding: CreationBinding<Child, Root, Descendants, Failures>,
) where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    match creations.entry(id) {
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
                if !creation_order.contains(&id) {
                    creation_order.push(id);
                }
            } else {
                creation_order.retain(|owned| *owned != id);
            }
            entry.insert(binding);
        }
    }
}

/// Exclusive startup custody of one structural child occurrence.
pub(crate) trait ChildCreationAt<Position> {
    type Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase;
    type Root;
    type Descendants: RetirementSchema;
    type Origins;

    #[expect(
        clippy::type_complexity,
        reason = "the selected affine namespace loan preserves its exact typed map, retirement order and actor registration space"
    )]
    fn creation_namespace(
        &mut self,
    ) -> (
        &mut HashMap<
            CreationId,
            CreationBinding<
                Self::Child,
                Self::Root,
                Self::Descendants,
                Vec<<Self::Descendants as RetirementSchema>::Failure>,
            >,
        >,
        &mut Vec<CreationId>,
        ActorSpace<<Self::Child as Behavior>::Protocol>,
    );
}

impl<Bindings, Position> ChildCreationAt<Position> for Bindings
where
    Bindings: ChildCreationAtCursor<Position, Position>,
{
    type Child = Bindings::Child;
    type Root = Bindings::Root;
    type Descendants = Bindings::Descendants;
    type Origins = Bindings::Origins;

    fn creation_namespace(
        &mut self,
    ) -> (
        &mut HashMap<
            CreationId,
            CreationBinding<
                Self::Child,
                Self::Root,
                Self::Descendants,
                Vec<<Self::Descendants as RetirementSchema>::Failure>,
            >,
        >,
        &mut Vec<CreationId>,
        ActorSpace<<Self::Child as Behavior>::Protocol>,
    ) {
        self.creation_namespace_at()
    }
}

pub(crate) trait ChildCreationAtCursor<Target, Cursor> {
    type Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase;
    type Root;
    type Descendants: RetirementSchema;
    type Origins;

    #[expect(
        clippy::type_complexity,
        reason = "the selected affine namespace loan preserves its exact typed map, retirement order and actor registration space"
    )]
    fn creation_namespace_at(
        &mut self,
    ) -> (
        &mut HashMap<
            CreationId,
            CreationBinding<
                Self::Child,
                Self::Root,
                Self::Descendants,
                Vec<<Self::Descendants as RetirementSchema>::Failure>,
            >,
        >,
        &mut Vec<CreationId>,
        ActorSpace<<Self::Child as Behavior>::Protocol>,
    );
}

impl<Target, Child, Root, Origins, Tail, Descendants> ChildCreationAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Origins, Tail, Descendants>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
{
    type Child = Child;
    type Root = Root;
    type Descendants = Descendants;
    type Origins = Origins;

    fn creation_namespace_at(
        &mut self,
    ) -> (
        &mut HashMap<
            CreationId,
            CreationBinding<
                Self::Child,
                Self::Root,
                Self::Descendants,
                Vec<<Self::Descendants as RetirementSchema>::Failure>,
            >,
        >,
        &mut Vec<CreationId>,
        ActorSpace<<Self::Child as Behavior>::Protocol>,
    ) {
        (
            &mut self.creations,
            &mut self.creation_order,
            self.actors.clone(),
        )
    }
}

impl<Target, Cursor, Position, Head, Root, Origins, Tail, Descendants>
    ChildCreationAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Origins, Tail, Descendants>
where
    Head: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
    Tail: ChildCreationAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;
    type Root = Tail::Root;
    type Descendants = Tail::Descendants;
    type Origins = Tail::Origins;

    fn creation_namespace_at(
        &mut self,
    ) -> (
        &mut HashMap<
            CreationId,
            CreationBinding<
                Self::Child,
                Self::Root,
                Self::Descendants,
                Vec<<Self::Descendants as RetirementSchema>::Failure>,
            >,
        >,
        &mut Vec<CreationId>,
        ActorSpace<<Self::Child as Behavior>::Protocol>,
    ) {
        self.tail
            .as_mut()
            .expect("live occurrence retains its tail")
            .creation_namespace_at()
    }
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

impl<Root: RetirementSchema> RetireChildTasks for NoChildBindings<Root>
where
    Root::Failure: Send,
{
    type Root = Root;
    type Failures = Vec<Root::Failure>;

    fn request_retirement(&mut self) {}

    fn retirement_failures() -> Self::Failures {
        Vec::new()
    }

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
    async fn retire_child_tasks(self) -> (Vec<Self::Root>, Self::Failures) {
        (Vec::new(), Vec::new())
    }
}

impl<Position, Child, Root, Origins, Tail, Descendants> RetireChildTasks
    for ChildBinding<Position, Child, Root, Origins, Tail, Descendants>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
    Vec<<Descendants as RetirementSchema>::Failure>: Send + 'static,
    Descendants: Send + 'static,
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
    Root: RetirementSchema
        + ProjectTerminal<Origins::Origin, Child, Descendants = Descendants>
        + Send
        + 'static,
    Origins: ChildOriginAt<Position, Child>,
    Root::Failure: From<ChildFailure<Origins::Origin, Child>> + Send,
    Tail: RetireChildTasks<Root = Root, Failures = Vec<Root::Failure>> + Send,
{
    type Root = Root;
    type Failures = Vec<Root::Failure>;

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
        Vec::new()
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
                    Ok(Err(error)) => failures.push(
                        (ChildFailure::ActorTaskFailed {
                            origin,
                            actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                            error,
                        })
                        .into(),
                    ),
                    Err(error) => failures.push(
                        (ChildFailure::ProjectionTaskFailed {
                            origin,
                            actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                            error,
                        })
                        .into(),
                    ),
                },
                CreationBinding::RetiringStartup { custody, .. } => match custody {
                    StartupRetirementCustody::Raw {
                        received: Some(Err(error)),
                        ..
                    } => {
                        failures
                            .push(ChildFailure::StartupActorTaskFailed { origin, error }.into());
                    }
                    StartupRetirementCustody::Projection {
                        received: Some(Ok(terminal)),
                        ..
                    } => retired.push(terminal),
                    StartupRetirementCustody::Projection {
                        received: Some(Err(error)),
                        ..
                    } => failures
                        .push(ChildFailure::StartupProjectionTaskFailed { origin, error }.into()),
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
                        failures.push(
                            (ChildFailure::InitializationPanicked {
                                origin,
                                payload,
                                additional_failures,
                                terminal_report,
                                retirement_failures,
                            })
                            .into(),
                        );
                    }
                    primary_failure => failures.push(
                        (ChildFailure::StartupRetirementFailed {
                            origin,
                            primary_failure,
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        })
                        .into(),
                    ),
                },
                CreationBinding::Established { joined: None, .. } | CreationBinding::Rejected => {
                    unreachable!("only an observed complete row crosses structural extraction");
                }
            }
            owner.creation_order.remove(0);
        }
        Tail::receive_retirement(&mut owner.tail, retired, failures).await;
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
pub(crate) struct ChildBinding<Position, Child, Root, Origins, Tail, Descendants>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Descendants: RetirementSchema,
{
    #[expect(
        clippy::type_complexity,
        reason = "one registry retains each original child and its selected descendant/failure custody"
    )]
    creations: HashMap<
        CreationId,
        CreationBinding<Child, Root, Descendants, Vec<<Descendants as RetirementSchema>::Failure>>,
    >,
    creation_order: Vec<CreationId>,
    actors: ActorSpace<Child::Protocol>,
    tail: Option<Tail>,
    position: PhantomData<fn() -> (Position, Origins)>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "raw and projected retirement retain distinct affine payloads; ownership does not require indirection"
)]
pub(crate) enum StartupRetirementCustody<Child, Root, Descendants, Failures>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Raw {
        task: Option<OwnedTask<Child, (Vec<Descendants>, Failures)>>,
        #[expect(
            clippy::type_complexity,
            reason = "the outside destination acquires the original raw actor outcome before native task disposal"
        )]
        received: Option<Result<LocalOutcome<Child, (Vec<Descendants>, Failures)>, JoinError>>,
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
pub(crate) enum CreationBinding<Child, Root, Descendants, Failures>
where
    Child: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Starting {
        kind: CreationKind,
        address: MailAddr,
        route: u64,
        startup: oneshot::Receiver<(ActorRef<Child::Protocol>, oneshot::Sender<()>)>,
        control: ControlSender<Child::Event>,
        task: Option<OwnedTask<Child, (Vec<Descendants>, Failures)>>,
    },
    RetiringStartup {
        kind: CreationKind,
        address: MailAddr,
        route: u64,
        custody: StartupRetirementCustody<Child, Root, Descendants, Failures>,
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

pub(crate) type NestedBindings<Child, Descendants> =
    ChildBindings<Child, Descendants, StructuralOrigins<<Child as BehaviorBase>::Base>>;

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use behavior::{ChildHead, ChildTail, CreationSequence, Never};

    use crate::actor;
    use crate::local::children::StructuralOrigins;
    use crate::local::children::{
        ChildAuthority, ChildBinding, ChildBindingAt, ChildCreationAt, CreationBinding,
        NoChildBindings, record_child_creation,
    };

    struct Worker;

    #[actor(message = Never)]
    impl Worker {}

    type LaterOccurrence = ChildBinding<
        ChildTail<ChildHead>,
        Worker,
        Never,
        StructuralOrigins<Worker>,
        NoChildBindings<Never>,
        Never,
    >;
    type FirstOccurrence =
        ChildBinding<ChildHead, Worker, Never, StructuralOrigins<Worker>, LaterOccurrence, Never>;

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
        let mut bindings = FirstOccurrence {
            creations: std::collections::HashMap::new(),
            creation_order: Vec::new(),
            actors: crate::ActorSpace::new(),
            tail: Some(LaterOccurrence {
                creations: std::collections::HashMap::new(),
                creation_order: Vec::new(),
                actors: crate::ActorSpace::new(),
                tail: Some(NoChildBindings::default()),
                position: core::marker::PhantomData,
            }),
            position: core::marker::PhantomData,
        };
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        {
            let (creations, creation_order, _) =
                <FirstOccurrence as ChildCreationAt<ChildHead>>::creation_namespace(&mut bindings);
            record_child_creation(
                creations,
                creation_order,
                creation,
                CreationBinding::Rejected,
            );
        }

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
        let mut bindings = FirstOccurrence {
            creations: std::collections::HashMap::new(),
            creation_order: Vec::new(),
            actors: crate::ActorSpace::new(),
            tail: Some(LaterOccurrence {
                creations: std::collections::HashMap::new(),
                creation_order: Vec::new(),
                actors: crate::ActorSpace::new(),
                tail: Some(NoChildBindings::default()),
                position: core::marker::PhantomData,
            }),
            position: core::marker::PhantomData,
        };
        let first =
            <FirstOccurrence as ChildCreationAt<ChildHead>>::creation_namespace(&mut bindings).2;
        let first_again =
            <FirstOccurrence as ChildCreationAt<ChildHead>>::creation_namespace(&mut bindings).2;
        let later = <FirstOccurrence as ChildCreationAt<ChildTail<ChildHead>>>::creation_namespace(
            &mut bindings,
        )
        .2;
        let later_again =
            <FirstOccurrence as ChildCreationAt<ChildTail<ChildHead>>>::creation_namespace(
                &mut bindings,
            )
            .2;

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
        let mut bindings = FirstOccurrence {
            creations: std::collections::HashMap::new(),
            creation_order: Vec::new(),
            actors: crate::ActorSpace::new(),
            tail: Some(LaterOccurrence {
                creations: std::collections::HashMap::new(),
                creation_order: Vec::new(),
                actors: crate::ActorSpace::new(),
                tail: Some(NoChildBindings::default()),
                position: core::marker::PhantomData,
            }),
            position: core::marker::PhantomData,
        };
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        {
            let (creations, creation_order, _) = <FirstOccurrence as ChildCreationAt<
                ChildTail<ChildHead>,
            >>::creation_namespace(&mut bindings);
            record_child_creation(
                creations,
                creation_order,
                creation,
                CreationBinding::Rejected,
            );
        }

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
        let mut bindings = FirstOccurrence {
            creations: std::collections::HashMap::new(),
            creation_order: Vec::new(),
            actors: crate::ActorSpace::new(),
            tail: Some(LaterOccurrence {
                creations: std::collections::HashMap::new(),
                creation_order: Vec::new(),
                actors: crate::ActorSpace::new(),
                tail: Some(NoChildBindings::default()),
                position: core::marker::PhantomData,
            }),
            position: core::marker::PhantomData,
        };
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        {
            let (creations, creation_order, _) =
                <FirstOccurrence as ChildCreationAt<ChildHead>>::creation_namespace(&mut bindings);
            record_child_creation(
                creations,
                creation_order,
                creation,
                CreationBinding::Rejected,
            );
        }

        let duplicate = catch_unwind(AssertUnwindSafe(|| {
            let (creations, creation_order, _) =
                <FirstOccurrence as ChildCreationAt<ChildHead>>::creation_namespace(&mut bindings);
            record_child_creation(
                creations,
                creation_order,
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
