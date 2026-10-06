//! Exact creator-local bindings derived from one behavior's closed births.

use core::marker::PhantomData;
use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use behavior::{
    Behavior, BirthMode, ChildHead, ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences,
    ChildTail, CreationId, CreationKind, EstablishedActor,
};
use bombay_engine::DriverError;
use communication::ControlSender;
use tokio::task::JoinError;

use crate::address::MailAddr;
use crate::application_runtime::{ChildOriginAt, StructuralOrigins};

use crate::ActorSpace;
use crate::launch::ProjectedTask;
use crate::local::{ActorRef, InstalledActor, LocalActivationRejection, Termination};
use crate::terminal::ChildFailure;

type ChildNode<Owner> = <<Owner as Behavior>::Birth as BirthMode>::Child;

/// Bombay's runtime binding representation for one direct-child birth node.
pub(crate) struct RuntimeChildBindings<Root, Origins>(PhantomData<fn() -> (Root, Origins)>);

impl<Root, Origins> ChildOccurrenceShape for RuntimeChildBindings<Root, Origins> {
    type Empty = NoChildBindings<Root>;
    type Member<Position, Child: Behavior, Tail> =
        ChildBinding<Position, Child, Root, Origins, Tail>;
}

/// Exact binding product for the direct children of `Owner`.
pub(crate) type ChildBindings<Owner, Root, Origins> =
    ChildOccurrences<ChildNode<Owner>, RuntimeChildBindings<Root, Origins>>;

/// End of one creator's direct-child binding product.
pub(crate) struct NoChildBindings<Root = behavior::Never>(PhantomData<fn() -> Root>);

impl<Root> Default for NoChildBindings<Root> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Creation outcomes, hosting, and ordered task custody for one occurrence.
pub(crate) struct ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: Behavior,
{
    creations: HashMap<CreationId, CreationBinding<Child, Root>>,
    creation_order: Vec<CreationId>,
    actors: ActorSpace<Child::Protocol>,
    tail: Option<Tail>,
    position: PhantomData<fn() -> (Position, Origins)>,
}

impl<Position, Child, Root, Origins, Tail> Default
    for ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: Behavior,
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

pub(crate) enum CreationBinding<Child: Behavior, Root> {
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

/// Static selection and custody of one structural child occurrence.
pub(crate) trait ChildBindingAt<Position> {
    type Child: Behavior;
    type Root;
    type Origins;

    fn creation(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>>;
    fn record_creation(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    );
    fn child_actors(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol>;
}

impl<Bindings, Position> ChildBindingAt<Position> for Bindings
where
    Bindings: ChildBindingAtCursor<Position, Position>,
{
    type Child = Bindings::Child;
    type Root = Bindings::Root;
    type Origins = Bindings::Origins;

    fn creation(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>> {
        self.creation_at(id)
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

pub(crate) trait ChildBindingAtCursor<Target, Cursor> {
    type Child: Behavior;
    type Root;
    type Origins;

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>>;
    fn record_creation_at(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    );
    fn child_actors_at(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol>;
}

impl<Target, Child, Root, Origins, Tail> ChildBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Origins, Tail>
where
    Child: Behavior,
{
    type Child = Child;
    type Root = Root;
    type Origins = Origins;

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Child, Root>> {
        self.creations.get(&id)
    }

    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding<Child, Root>) {
        match self.creations.entry(id) {
            Entry::Occupied(_) => {
                panic!("one child creation identity settles once per occurrence")
            }
            Entry::Vacant(entry) => {
                if match &binding {
                    CreationBinding::Established { .. } => true,
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
                    self.creation_order.push(id);
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
    ChildBindingAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Origins, Tail>
where
    Head: Behavior,
    Tail: ChildBindingAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;
    type Root = Tail::Root;
    type Origins = Tail::Origins;

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>> {
        self.tail
            .as_ref()
            .expect("live child occurrence retains its tail")
            .creation_at(id)
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
    Child: Behavior,
{
    type Bindings: Default;
}

impl<Child, Bindings> NestedChildBindings<Child> for Bindings
where
    Child: Behavior + behavior::BehaviorBase,
    Bindings: RetireChildTasks,
    ChildNode<Child>: ChildOccurrenceProduct<
        RuntimeChildBindings<Bindings::Root, StructuralOrigins<Child::Base>>,
    >,
    ChildBindings<Child, Bindings::Root, StructuralOrigins<Child::Base>>: Default,
{
    type Bindings = ChildBindings<Child, Bindings::Root, StructuralOrigins<Child::Base>>;
}

pub(crate) type NestedBindings<Bindings, Child> =
    <Bindings as NestedChildBindings<Child>>::Bindings;

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

    fn retire_child_tasks(
        self,
    ) -> impl core::future::Future<Output = (Vec<Self::Root>, Self::Failures)> + Send;
}

impl<Root> RetireChildTasks for NoChildBindings<Root> {
    type Root = Root;
    type Failures = ();

    fn request_retirement(&mut self) {}

    fn retirement_failures() {}

    fn receive_retirement(
        bindings: &mut Option<Self>,
        _retired: &mut Vec<Self::Root>,
        _failures: &mut Self::Failures,
    ) -> impl core::future::Future<Output = ()> + Send {
        async move {
            drop(bindings.take());
        }
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn retire_child_tasks(self) -> (Vec<Self::Root>, ()) {
        (Vec::new(), ())
    }
}

impl<Position, Child, Root, Origins, Tail> RetireChildTasks
    for ChildBinding<Position, Child, Root, Origins, Tail>
where
    Child: Behavior<Protocol: behavior::Protocol<Addr = MailAddr>>,
    Child::Protocol: Send + Sync,
    <Child::Protocol as behavior::Protocol>::Msg: Send,
    Child::Event: Send,
    Child::Error: Send,
    Root: Send,
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
                CreationBinding::Established { task, .. } => {
                    if let Some(task) = task {
                        task.request_retirement();
                    }
                }
                CreationBinding::Rejected | CreationBinding::StartupRejected { .. } => {}
            }
        }
        if let Some(tail) = &mut self.tail {
            tail.request_retirement();
        }
    }

    fn retirement_failures() -> Self::Failures {
        (Vec::new(), Tail::retirement_failures())
    }

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
            let binding = owner
                .creations
                .get_mut(&id)
                .expect("ordered child creation retains its owned binding");
            let origin = match binding {
                CreationBinding::Established {
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
                    Origins::origin(endpoint.address(), *route)
                }
                CreationBinding::StartupRejected { address, route, .. } => {
                    Origins::origin(*address, *route)
                }
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
                    kind,
                    endpoint,
                    control,
                    joined: Some(joined),
                    ..
                } => match joined {
                    Ok(Ok(terminal)) => retired.push(terminal),
                    Ok(Err(error)) => failures.0.push(ChildFailure::ActorTaskFailed {
                        id,
                        kind,
                        origin,
                        actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                        error,
                    }),
                    Err(error) => failures.0.push(ChildFailure::ProjectionTaskFailed {
                        id,
                        kind,
                        origin,
                        actor: EstablishedActor::issued(InstalledActor::new(endpoint, control)),
                        error,
                    }),
                },
                CreationBinding::StartupRejected {
                    kind,
                    primary_failure,
                    additional_failures,
                    terminal_report,
                    retirement_failures,
                    ..
                } => match primary_failure {
                    Some(DriverError::InitializationPanicked(payload)) => {
                        failures.0.push(ChildFailure::InitializationPanicked {
                            id,
                            kind,
                            origin,
                            payload,
                            additional_failures,
                            terminal_report,
                            retirement_failures,
                        })
                    }
                    primary_failure => failures.0.push(ChildFailure::StartupRetirementFailed {
                        id,
                        kind,
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

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use behavior::{ChildHead, ChildTail, CreationSequence, Never};

    use super::{ChildBinding, ChildBindingAt, CreationBinding, NoChildBindings};
    use crate::actor;
    use crate::application_runtime::StructuralOrigins;

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

    #[test]
    fn creation_identity_is_local_to_its_child_occurrence() {
        let mut bindings = FirstOccurrence::default();
        let mut sequence = CreationSequence::new();
        let creation = sequence.issue().expect("creation identity available");
        <FirstOccurrence as ChildBindingAt<ChildHead>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        assert!(matches!(
            <FirstOccurrence as ChildBindingAt<ChildHead>>::creation(&bindings, creation),
            Some(CreationBinding::Rejected)
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
        let first = <FirstOccurrence as ChildBindingAt<ChildHead>>::child_actors(&bindings);
        let first_again = <FirstOccurrence as ChildBindingAt<ChildHead>>::child_actors(&bindings);
        let later =
            <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::child_actors(&bindings);
        let later_again =
            <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::child_actors(&bindings);

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
        <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        assert!(matches!(
            <FirstOccurrence as ChildBindingAt<ChildTail<ChildHead>>>::creation(
                &bindings, creation
            ),
            Some(CreationBinding::Rejected)
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
        <FirstOccurrence as ChildBindingAt<ChildHead>>::record_creation(
            &mut bindings,
            creation,
            CreationBinding::Rejected,
        );

        let duplicate = catch_unwind(AssertUnwindSafe(|| {
            <FirstOccurrence as ChildBindingAt<ChildHead>>::record_creation(
                &mut bindings,
                creation,
                CreationBinding::Rejected,
            );
        }));
        assert!(duplicate.is_err());
        assert!(matches!(
            <FirstOccurrence as ChildBindingAt<ChildHead>>::creation(&bindings, creation),
            Some(CreationBinding::Rejected)
        ));
    }
}
