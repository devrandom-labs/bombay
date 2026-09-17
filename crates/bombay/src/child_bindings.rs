//! Exact creator-local bindings derived from one behavior's closed births.

use core::marker::PhantomData;
use std::collections::HashMap;

use behavior::{
    Behavior, BirthMode, ChildHead, ChildOccurrenceProduct, ChildOccurrenceShape, ChildOccurrences,
    ChildTail, CreationId, CreationKind,
};
use communication::ControlSender;

use crate::launch::LocalAddresses;
use crate::launch::ProjectedTask;
use crate::local::ActorRef;

type ChildNode<Owner> = <<Owner as Behavior>::Birth as BirthMode>::Child;

struct BoundChild<Child: Behavior> {
    endpoint: ActorRef<Child::Protocol>,
    control: ControlSender<Child::Event>,
}

impl<Child: Behavior> Clone for BoundChild<Child> {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            control: self.control.clone(),
        }
    }
}

/// Bombay's runtime binding representation for one direct-child birth node.
pub(crate) struct RuntimeChildBindings<Root>(PhantomData<fn() -> Root>);

impl<Root> ChildOccurrenceShape for RuntimeChildBindings<Root> {
    type Empty = NoChildBindings<Root>;
    type Member<Position, Child: Behavior, Tail> = ChildBinding<Position, Child, Root, Tail>;
}

/// Bombay's protocol-space representation for one direct-child birth node.
pub(crate) struct RuntimeChildSpaces;

impl ChildOccurrenceShape for RuntimeChildSpaces {
    type Empty = NoChildSpaces;
    type Member<Position, Child: Behavior, Tail> = ChildSpace<Position, Child, Tail>;
}

/// Exact binding product for the direct children of `Owner`.
pub(crate) type ChildBindings<Owner, Root> =
    ChildOccurrences<ChildNode<Owner>, RuntimeChildBindings<Root>>;

/// Occurrence-local protocol spaces for one behavior's direct children.
pub(crate) type ChildSpaces<Owner> = ChildOccurrences<ChildNode<Owner>, RuntimeChildSpaces>;

/// End of one creator's direct-child binding product.
pub(crate) struct NoChildBindings<Root = behavior::Never>(PhantomData<fn() -> Root>);

impl<Root> Default for NoChildBindings<Root> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// End of one occurrence-local child-space product.
#[derive(Clone, Default)]
pub(crate) struct NoChildSpaces;

/// One concrete typed address space at one structural child occurrence.
pub(crate) struct ChildSpace<Position, Child, Tail>
where
    Child: Behavior,
{
    actors: LocalAddresses<Child::Protocol>,
    tail: Tail,
    position: PhantomData<fn() -> Position>,
}

impl<Position, Child, Tail> Clone for ChildSpace<Position, Child, Tail>
where
    Child: Behavior,
    Tail: Clone,
{
    fn clone(&self) -> Self {
        Self {
            actors: self.actors.clone(),
            tail: self.tail.clone(),
            position: PhantomData,
        }
    }
}

impl<Position, Child, Tail> Default for ChildSpace<Position, Child, Tail>
where
    Child: Behavior,
    Tail: Default,
{
    fn default() -> Self {
        Self {
            actors: LocalAddresses::new(),
            tail: Tail::default(),
            position: PhantomData,
        }
    }
}

/// Exact occurrence-local hosting and bindings for one actor.
#[derive(Default)]
pub(crate) struct OccurrenceChildBindings<Bindings, Spaces> {
    bindings: Bindings,
    spaces: Spaces,
}

pub(crate) type OccurrenceBindings<Owner, Root> =
    OccurrenceChildBindings<ChildBindings<Owner, Root>, ChildSpaces<Owner>>;

/// Endpoints, rejections, and owned tasks for one structural occurrence.
pub(crate) struct ChildBinding<Position, Child, Root, Tail>
where
    Child: Behavior,
{
    creations: HashMap<CreationId, CreationBinding>,
    endpoints: HashMap<u64, BoundChild<Child>>,
    tasks: Vec<ProjectedTask<Child, Root>>,
    retired: Vec<Root>,
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
            endpoints: HashMap::new(),
            tasks: Vec::new(),
            retired: Vec::new(),
            tail: Tail::default(),
            position: PhantomData,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CreationBinding {
    Established { route: u64, kind: CreationKind },
    Rejected,
}

pub(crate) trait CreationBindingAt<Position> {
    fn creation(&self, id: CreationId) -> Option<CreationBinding>;
    fn record_creation(&mut self, id: CreationId, binding: CreationBinding);
}

impl<Bindings, Position> CreationBindingAt<Position> for Bindings
where
    Bindings: CreationBindingAtCursor<Position, Position>,
{
    fn creation(&self, id: CreationId) -> Option<CreationBinding> {
        self.creation_at(id)
    }

    fn record_creation(&mut self, id: CreationId, binding: CreationBinding) {
        self.record_creation_at(id, binding);
    }
}

pub(crate) trait CreationBindingAtCursor<Target, Cursor> {
    fn creation_at(&self, id: CreationId) -> Option<CreationBinding>;
    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding);
}

impl<Target, Child, Root, Tail> CreationBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Tail>
where
    Child: Behavior,
{
    fn creation_at(&self, id: CreationId) -> Option<CreationBinding> {
        self.creations.get(&id).copied()
    }

    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding) {
        self.creations.insert(id, binding);
    }
}

impl<Target, Cursor, Position, Head, Root, Tail> CreationBindingAtCursor<Target, ChildTail<Cursor>>
    for ChildBinding<Position, Head, Root, Tail>
where
    Head: Behavior,
    Tail: CreationBindingAtCursor<Target, Cursor>,
{
    fn creation_at(&self, id: CreationId) -> Option<CreationBinding> {
        self.tail.creation_at(id)
    }

    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding) {
        self.tail.record_creation_at(id, binding);
    }
}

impl<Target, Cursor, Bindings, Spaces> CreationBindingAtCursor<Target, Cursor>
    for OccurrenceChildBindings<Bindings, Spaces>
where
    Bindings: CreationBindingAtCursor<Target, Cursor>,
{
    fn creation_at(&self, id: CreationId) -> Option<CreationBinding> {
        self.bindings.creation_at(id)
    }

    fn record_creation_at(&mut self, id: CreationId, binding: CreationBinding) {
        self.bindings.record_creation_at(id, binding);
    }
}

/// Static selection of one structural child occurrence.
pub(crate) trait ChildBindingAt<Position> {
    type Child: Behavior;
    type Root;

    fn endpoint(&self, nonce: u64) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>>;
    fn control(&self, nonce: u64) -> Option<ControlSender<<Self::Child as Behavior>::Event>>;
    fn bind(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<<Self::Child as Behavior>::Protocol>,
        control: ControlSender<<Self::Child as Behavior>::Event>,
    ) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>>;
    fn retain_task(&mut self, task: ProjectedTask<Self::Child, Self::Root>);
}

impl<Bindings, Position> ChildBindingAt<Position> for Bindings
where
    Bindings: ChildBindingAtCursor<Position, Position>,
{
    type Child = Bindings::Child;
    type Root = Bindings::Root;

    fn endpoint(&self, nonce: u64) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.endpoint_at(nonce)
    }

    fn control(&self, nonce: u64) -> Option<ControlSender<<Self::Child as Behavior>::Event>> {
        self.control_at(nonce)
    }

    fn bind(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<<Self::Child as Behavior>::Protocol>,
        control: ControlSender<<Self::Child as Behavior>::Event>,
    ) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.bind_at(nonce, endpoint, control)
    }

    fn retain_task(&mut self, task: ProjectedTask<Self::Child, Self::Root>) {
        self.retain_task_at(task);
    }
}

pub(crate) trait ChildBindingAtCursor<Target, Cursor> {
    type Child: Behavior;
    type Root;

    fn endpoint_at(&self, nonce: u64) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>>;
    fn control_at(&self, nonce: u64) -> Option<ControlSender<<Self::Child as Behavior>::Event>>;
    fn bind_at(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<<Self::Child as Behavior>::Protocol>,
        control: ControlSender<<Self::Child as Behavior>::Event>,
    ) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>>;
    fn retain_task_at(&mut self, task: ProjectedTask<Self::Child, Self::Root>);
}

impl<Target, Child, Root, Tail> ChildBindingAtCursor<Target, ChildHead>
    for ChildBinding<Target, Child, Root, Tail>
where
    Child: Behavior,
{
    type Child = Child;
    type Root = Root;

    fn endpoint_at(&self, nonce: u64) -> Option<ActorRef<Child::Protocol>> {
        self.endpoints
            .get(&nonce)
            .map(|bound| bound.endpoint.clone())
    }

    fn control_at(&self, nonce: u64) -> Option<ControlSender<Child::Event>> {
        self.endpoints
            .get(&nonce)
            .map(|bound| bound.control.clone())
    }

    fn bind_at(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<Child::Protocol>,
        control: ControlSender<Child::Event>,
    ) -> Option<ActorRef<Child::Protocol>> {
        self.endpoints
            .insert(nonce, BoundChild { endpoint, control })
            .map(|bound| bound.endpoint)
    }

    fn retain_task_at(&mut self, task: ProjectedTask<Child, Root>) {
        self.tasks.push(task);
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

    fn endpoint_at(&self, nonce: u64) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.tail.endpoint_at(nonce)
    }

    fn control_at(&self, nonce: u64) -> Option<ControlSender<<Self::Child as Behavior>::Event>> {
        self.tail.control_at(nonce)
    }

    fn bind_at(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<<Self::Child as Behavior>::Protocol>,
        control: ControlSender<<Self::Child as Behavior>::Event>,
    ) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.tail.bind_at(nonce, endpoint, control)
    }

    fn retain_task_at(&mut self, task: ProjectedTask<Self::Child, Self::Root>) {
        self.tail.retain_task_at(task);
    }
}

impl<Target, Cursor, Bindings, Spaces> ChildBindingAtCursor<Target, Cursor>
    for OccurrenceChildBindings<Bindings, Spaces>
where
    Bindings: ChildBindingAtCursor<Target, Cursor>,
{
    type Child = Bindings::Child;
    type Root = Bindings::Root;

    fn endpoint_at(&self, nonce: u64) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.bindings.endpoint_at(nonce)
    }

    fn control_at(&self, nonce: u64) -> Option<ControlSender<<Self::Child as Behavior>::Event>> {
        self.bindings.control_at(nonce)
    }

    fn bind_at(
        &mut self,
        nonce: u64,
        endpoint: ActorRef<<Self::Child as Behavior>::Protocol>,
        control: ControlSender<<Self::Child as Behavior>::Event>,
    ) -> Option<ActorRef<<Self::Child as Behavior>::Protocol>> {
        self.bindings.bind_at(nonce, endpoint, control)
    }

    fn retain_task_at(&mut self, task: ProjectedTask<Self::Child, Self::Root>) {
        self.bindings.retain_task_at(task);
    }
}

trait ChildSpaceAt<Position> {
    type Child: Behavior;

    fn actors(&self) -> &LocalAddresses<<Self::Child as Behavior>::Protocol>;
}

impl<Spaces, Position> ChildSpaceAt<Position> for Spaces
where
    Spaces: ChildSpaceAtCursor<Position, Position>,
{
    type Child = Spaces::Child;

    fn actors(&self) -> &LocalAddresses<<Self::Child as Behavior>::Protocol> {
        self.actors_at()
    }
}

trait ChildSpaceAtCursor<Target, Cursor> {
    type Child: Behavior;

    fn actors_at(&self) -> &LocalAddresses<<Self::Child as Behavior>::Protocol>;
}

impl<Target, Child, Tail> ChildSpaceAtCursor<Target, ChildHead> for ChildSpace<Target, Child, Tail>
where
    Child: Behavior,
{
    type Child = Child;

    fn actors_at(&self) -> &LocalAddresses<Child::Protocol> {
        &self.actors
    }
}

impl<Target, Cursor, Position, Head, Tail> ChildSpaceAtCursor<Target, ChildTail<Cursor>>
    for ChildSpace<Position, Head, Tail>
where
    Head: Behavior,
    Tail: ChildSpaceAtCursor<Target, Cursor>,
{
    type Child = Tail::Child;

    fn actors_at(&self) -> &LocalAddresses<<Self::Child as Behavior>::Protocol> {
        self.tail.actors_at()
    }
}

pub(crate) trait HostChildAt<Position, Child>
where
    Child: Behavior,
{
    fn child_actors(&self) -> LocalAddresses<Child::Protocol>;
}

impl<Position, Child, Bindings, Spaces> HostChildAt<Position, Child>
    for OccurrenceChildBindings<Bindings, Spaces>
where
    Child: Behavior,
    Spaces: ChildSpaceAt<Position, Child = Child>,
{
    fn child_actors(&self) -> LocalAddresses<Child::Protocol> {
        self.spaces.actors().clone()
    }
}

/// Derive the same exact binding representation for a newly installed child.
pub(crate) trait NestedChildBindings<Child>
where
    Child: Behavior,
{
    type Bindings: Default;
}

impl<Child, Bindings, Spaces> NestedChildBindings<Child>
    for OccurrenceChildBindings<Bindings, Spaces>
where
    Child: Behavior,
    Bindings: RetireChildTasks,
    ChildNode<Child>: ChildOccurrenceProduct<RuntimeChildBindings<Bindings::Root>>
        + ChildOccurrenceProduct<RuntimeChildSpaces>,
    ChildBindings<Child, Bindings::Root>: Default,
    ChildSpaces<Child>: Default,
{
    type Bindings = OccurrenceBindings<Child, Bindings::Root>;
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
            creations: _,
            endpoints,
            tasks,
            mut retired,
            tail,
            position,
        } = self;
        drop((endpoints, position));
        retired.reserve(tasks.len());
        for task in tasks {
            retired.push(task.retire().await);
        }
        retired.extend(tail.retire_child_tasks().await);
        retired
    }
}

impl<Bindings, Spaces> RetireChildTasks for OccurrenceChildBindings<Bindings, Spaces>
where
    Bindings: RetireChildTasks + Send,
    Spaces: Send,
{
    type Root = Bindings::Root;

    async fn retire_child_tasks(self) -> Vec<Self::Root> {
        let Self { bindings, spaces } = self;
        drop(spaces);
        bindings.retire_child_tasks().await
    }
}
