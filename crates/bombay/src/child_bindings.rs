//! Exact creator-local bindings derived from one behavior's closed births.

use core::marker::PhantomData;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use behavior::{
    Behavior, BirthMode, ChildHead, ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences,
    ChildTail, CreationId, CreationKind,
};
use communication::ControlSender;

use crate::ActorSpace;
use crate::launch::ProjectedTask;
use crate::local::ActorRef;

type ChildNode<Owner> = <<Owner as Behavior>::Birth as BirthMode>::Child;

/// Bombay's runtime binding representation for one direct-child birth node.
pub(crate) struct RuntimeChildBindings<Root>(PhantomData<fn() -> Root>);

impl<Root> ChildOccurrenceShape for RuntimeChildBindings<Root> {
    type Empty = NoChildBindings<Root>;
    type Member<Position, Child: Behavior, Tail> = ChildBinding<Position, Child, Root, Tail>;
}

/// Exact binding product for the direct children of `Owner`.
pub(crate) type ChildBindings<Owner, Root> =
    ChildOccurrences<ChildNode<Owner>, RuntimeChildBindings<Root>>;

/// End of one creator's direct-child binding product.
pub(crate) struct NoChildBindings<Root = behavior::Never>(PhantomData<fn() -> Root>);

impl<Root> Default for NoChildBindings<Root> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Creation outcomes, hosting, and ordered task custody for one occurrence.
pub(crate) struct ChildBinding<Position, Child, Root, Tail>
where
    Child: Behavior,
{
    creations: HashMap<CreationId, CreationBinding<Child, Root>>,
    creation_order: Vec<CreationId>,
    actors: ActorSpace<Child::Protocol>,
    tail: Tail,
    position: PhantomData<fn() -> Position>,
}

impl<Position, Child, Root, Tail> Default for ChildBinding<Position, Child, Root, Tail>
where
    Child: Behavior,
    Tail: Default,
{
    fn default() -> Self {
        Self {
            creations: HashMap::new(),
            creation_order: Vec::new(),
            actors: ActorSpace::new(),
            tail: Tail::default(),
            position: PhantomData,
        }
    }
}

pub(crate) enum CreationBinding<Child: Behavior, Root> {
    Established {
        kind: CreationKind,
        endpoint: ActorRef<Child::Protocol>,
        control: ControlSender<Child::Event>,
        task: ProjectedTask<Child, Root>,
    },
    Rejected,
}

/// Static selection and custody of one structural child occurrence.
pub(crate) trait ChildBindingAt<Position> {
    type Child: Behavior;
    type Root;

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

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>>;
    fn record_creation_at(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    );
    fn child_actors_at(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol>;
}

impl<Target, Child, Root, Tail> ChildBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Tail>
where
    Child: Behavior,
{
    type Child = Child;
    type Root = Root;

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Child, Root>> {
        self.creations.get(&id)
    }

    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding<Child, Root>) {
        match self.creations.entry(id) {
            Entry::Occupied(_) => {
                panic!("one child creation identity settles once per occurrence")
            }
            Entry::Vacant(entry) => {
                if matches!(&binding, CreationBinding::Established { .. }) {
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

impl<Target, Cursor, Position, Head, Root, Tail> ChildBindingAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Tail>
where
    Head: Behavior,
    Tail: ChildBindingAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;
    type Root = Tail::Root;

    fn creation_at(&self, id: CreationId) -> Option<&CreationBinding<Self::Child, Self::Root>> {
        self.tail.creation_at(id)
    }

    fn record_creation_at(
        &mut self,
        id: CreationId,
        binding: CreationBinding<Self::Child, Self::Root>,
    ) {
        self.tail.record_creation_at(id, binding);
    }

    fn child_actors_at(&self) -> ActorSpace<<Self::Child as Behavior>::Protocol> {
        self.tail.child_actors_at()
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
    Child: Behavior,
    Bindings: RetireChildTasks,
    ChildNode<Child>: ChildOccurrenceProduct<RuntimeChildBindings<Bindings::Root>>,
    ChildBindings<Child, Bindings::Root>: Default,
{
    type Bindings = ChildBindings<Child, Bindings::Root>;
}

pub(crate) type NestedBindings<Bindings, Child> =
    <Bindings as NestedChildBindings<Child>>::Bindings;

pub(crate) trait RetireChildTasks {
    type Root;

    fn retire_child_tasks(self) -> impl core::future::Future<Output = Vec<Self::Root>> + Send;
}

impl<Root> RetireChildTasks for NoChildBindings<Root> {
    type Root = Root;

    async fn retire_child_tasks(self) -> Vec<Self::Root> {
        Vec::new()
    }
}

impl<Position, Child, Root, Tail> RetireChildTasks for ChildBinding<Position, Child, Root, Tail>
where
    Child: Behavior,
    Child::Protocol: Send + Sync,
    <Child::Protocol as behavior::Protocol>::Msg: Send,
    <Child::Protocol as behavior::Protocol>::Addr: Send + Sync,
    Child::Event: Send,
    Root: Send,
    Tail: RetireChildTasks<Root = Root> + Send,
{
    type Root = Root;

    async fn retire_child_tasks(self) -> Vec<Self::Root> {
        let Self {
            mut creations,
            creation_order,
            actors,
            tail,
            position,
        } = self;
        let mut tasks = Vec::with_capacity(creation_order.len());
        for id in creation_order {
            let Some(CreationBinding::Established { task, .. }) = creations.remove(&id) else {
                panic!("ordered child creation lost its owned task");
            };
            tasks.push(task);
        }
        drop((creations, actors, position));
        let mut retired = Vec::with_capacity(tasks.len());
        for task in tasks {
            retired.push(task.retire().await);
        }
        retired.extend(tail.retire_child_tasks().await);
        retired
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use behavior::{ChildHead, ChildTail, CreationSequence, Never};

    use super::{ChildBinding, ChildBindingAt, CreationBinding, NoChildBindings};
    use crate::actor;

    struct Worker;

    #[actor(message = Never)]
    impl Worker {}

    type LaterOccurrence = ChildBinding<ChildTail<ChildHead>, Worker, (), NoChildBindings>;
    type FirstOccurrence = ChildBinding<ChildHead, Worker, (), LaterOccurrence>;

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
