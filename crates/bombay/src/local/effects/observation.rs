use crate::address::MailAddr;
use crate::launch::ActorSpace;
use crate::local::children::{ChildBindingAt, CreationBinding};
use crate::local::effects::ApplicationCapabilities;
use crate::local::endpoint::{ActorRef, InstalledActor, request_actor_shutdown};
use crate::observe::{Observation, ObservationFuture};
use crate::termination::Termination;
use crate::topology::Hosts;
use behavior::{
    ActionItem, Address, Behavior, BehaviorMessage, CommittedChild, CreationCorrelation,
    CreationId, EstablishedActor, EstablishedCreation, Here, InjectEvent, InterpretItem,
    InterpreterFault, ItemSettlement, Protocol, ResolveChildOccurrence, ResolvedChild,
    ResolvedChildPosition,
};
use behavior_actors::{
    CancelObservation, ChildShutdownRejection, ChildStopped, CreationResolved,
    EstablishedObservation, EstablishedShutdownResolved, InterpretEstablishedObservation,
    InterpretEstablishedShutdown, ObservationAuthority, ObservationRejection, ObserveChild,
    ObserveCreation, ObserveEstablished, ObserveEstablishedCreation, ObservePeer,
    PeerObservationRejection, PeerStopped, ShutdownChild, ShutdownEstablished, ShutdownId,
    ShutdownRejection, ShutdownRequested,
};
use communication::ControlClosed;
use core::future::{Future, poll_fn};
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Poll;
use std::hash::Hash;
use std::sync::{Arc, PoisonError};
use std::time::Instant;
use tokio::sync::oneshot;

impl<C, N, P, Bindings, Origins, ChildProtocol, Occurrence, Path>
    InterpretItem<ObserveCreation<ChildProtocol, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    ChildProtocol: Protocol<Addr = MailAddr>,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = ChildProtocol>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    C::Event: InjectEvent<CreationResolved<MailAddr>, Path>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveCreation<ChildProtocol, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveCreation<ChildProtocol, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveCreation<ChildProtocol, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.creation)
                {
                    Some(CreationBinding::Established { endpoint, kind, .. }) => {
                        let address = endpoint.address();
                        let kind = *kind;
                        self.inject_control_event::<_, Path>(CreationResolved::installed(
                            request.creation,
                            kind,
                            address,
                        ));
                        ItemSettlement::Accepted(())
                    }
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.creation),
                            item: request,
                        }
                    }
                    None => ItemSettlement::Corrupt {
                        item: request,
                        fault: InterpreterFault::MissingCapability,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Child, Occurrence, Path>
    InterpretItem<ObserveEstablishedCreation<Child, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child>,
    C::Event: InjectEvent<EstablishedCreation<Child, Occurrence>, Path>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveEstablishedCreation<Child, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<
            <ObserveEstablishedCreation<Child, Occurrence> as ActionItem>::Reply,
        >,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveEstablishedCreation<Child, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.creation)
                {
                    Some(CreationBinding::Established {
                        endpoint,
                        control,
                        kind,
                        ..
                    }) => {
                        let actor = EstablishedActor::<Child>::issued(InstalledActor::new(
                            endpoint.clone(),
                            control.clone(),
                        ));
                        let child = CommittedChild::new(request.creation, *kind, actor);
                        self.inject_control_event::<_, Path>(EstablishedCreation::installed(child));
                        ItemSettlement::Accepted(())
                    }
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.creation),
                            item: request,
                        }
                    }
                    None => ItemSettlement::Corrupt {
                        item: request,
                        fault: InterpreterFault::MissingCapability,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Path> InterpretItem<ObservePeer<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<PeerStopped<MailAddr>, Path> + Send + 'static,
    BehaviorMessage<C>: Send,
    N: Hosts<C::Protocol>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObservePeer<MailAddr> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObservePeer<MailAddr> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObservePeer<MailAddr>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                let result = observe_peer::<C::Protocol, C::Event, Path>(
                    self.actor_spaces.space(),
                    request,
                    &mut self.observations,
                );
                match result {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(_) => ItemSettlement::Rejected {
                        item: request,
                        reason: PeerObservationRejection::UnknownAddress,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, ChildProtocol, Occurrence, Path>
    InterpretItem<ObserveChild<ChildProtocol, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>> + ResolveChildOccurrence<Occurrence>,
    ChildProtocol: Protocol<Addr = MailAddr>,
    C::Event: InjectEvent<ChildStopped<MailAddr>, Path> + Send + 'static,
    ResolvedChild<C, Occurrence>: Behavior<Protocol = ChildProtocol>,
    Bindings:
        ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = ResolvedChild<C, Occurrence>>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveChild<ChildProtocol, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveChild<ChildProtocol, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveChild<ChildProtocol, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let child = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.child)
                {
                    Some(CreationBinding::Established { endpoint, .. }) => endpoint.clone(),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        break 'settlement ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.child),
                            item: request,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Corrupt {
                            item: request,
                            fault: InterpreterFault::MissingCapability,
                        };
                    }
                };
                self.observations
                    .insert_child::<Path>(request.child, child.termination_observation());
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Child, Occurrence, Path>
    InterpretItem<ShutdownChild<Child, Occurrence>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>
        + ResolveChildOccurrence<Occurrence, Child = Child>,
    C::Event: InjectEvent<ChildStopped<MailAddr>, Path> + Send + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, Here>,
    Bindings: ChildBindingAt<ResolvedChildPosition<C, Occurrence>, Child = Child>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ShutdownChild<Child, Occurrence> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ShutdownChild<Child, Occurrence> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ShutdownChild<Child, Occurrence>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = 'settlement: {
                let (child, control) = match self
                    .child_bindings
                    .as_ref()
                    .expect("live child bindings remain installed")
                    .creation(request.child)
                {
                    Some(CreationBinding::Established {
                        endpoint, control, ..
                    }) => (endpoint.clone(), control.clone()),
                    Some(CreationBinding::Rejected | CreationBinding::StartupRejected { .. }) => {
                        break 'settlement ItemSettlement::Blocked {
                            prerequisite: CreationCorrelation::new(request.child),
                            item: request,
                        };
                    }
                    None => {
                        break 'settlement ItemSettlement::Corrupt {
                            item: request,
                            fault: InterpreterFault::MissingCapability,
                        };
                    }
                };
                match request_actor_shutdown(&child, &control, request.ingress) {
                    Ok(()) => {
                        self.observations
                            .insert_child::<Path>(request.child, child.termination_observation());
                        ItemSettlement::Accepted(())
                    }
                    Err(ShutdownRejection::AlreadyStopping) => ItemSettlement::Rejected {
                        item: request,
                        reason: ChildShutdownRejection::AlreadyStopping,
                    },
                    Err(ShutdownRejection::AlreadyStopped) => ItemSettlement::Rejected {
                        item: request,
                        reason: ChildShutdownRejection::NotEstablished,
                    },
                }
            };
            *received = Some(settlement);
        }
    }
}

impl<'a, Capabilities, Path> EstablishedObservationInterpreter<'a, Capabilities, Path> {
    pub(in crate::local::effects) fn new(capabilities: &'a mut Capabilities) -> Self {
        Self {
            capabilities,
            path: PhantomData,
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Target, Path> InterpretEstablishedObservation<Target>
    for EstablishedObservationInterpreter<
        '_,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>,
        Path,
    >
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    type Output = ();

    fn observe(&mut self, request: ObserveEstablished<Target>, endpoint: ActorRef<Target>) {
        let id = request.id();
        let mut observation_admission = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(observations) = observation_admission.as_mut() else {
            unreachable!("exclusive live capability interpretation precedes capability retirement");
        };
        if observations.contains_key(&id) {
            drop(observation_admission);
            self.capabilities.inject_control_event::<_, Path>(
                EstablishedObservation::observe_rejected(
                    request,
                    ObservationRejection::IdAlreadyBound,
                ),
            );
            return;
        }
        let authority = ObservationAuthority::issued(request);
        let relationship = authority.relationship().clone();
        let (revocation, revoked) = oneshot::channel();
        observations.insert(id, (relationship.identity().clone(), revocation));
        drop(observation_admission);
        // Synchronous exclusive interpretation publishes the committed grant
        // before spawning its observation. No completion may precede Started.
        self.capabilities
            .inject_control_event::<_, Path>(EstablishedObservation::started(authority));
        let notifications = self.capabilities.exact_observations.clone();
        let control = self.capabilities.control.clone();
        let termination = endpoint.termination();
        self.capabilities
            .activation_tasks
            .as_mut()
            .expect("live activation tasks remain installed")
            .spawn(async move {
                tokio::pin!(termination);
                let completed = {
                    let completion = poll_fn(|context| {
                        let mut observation_admission =
                            notifications.lock().unwrap_or_else(PoisonError::into_inner);
                        let Some(observations) = observation_admission.as_mut() else {
                            return Poll::Ready(None);
                        };
                        let registered = observations.get(&id).is_some_and(|(registered, _)| {
                            Arc::ptr_eq(registered, relationship.identity())
                        });
                        if !registered {
                            return Poll::Ready(None);
                        }
                        match termination.as_mut().poll(context) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(outcome) => {
                                // The irrevocable completion decision removes its
                                // exact member under cancellation's same guard.
                                observations.remove(&id);
                                Poll::Ready(Some((outcome, Instant::now())))
                            }
                        }
                    });
                    tokio::select! {
                        _ = revoked => None,
                        completed = completion => completed,
                    }
                };
                let Some((outcome, at)) = completed else {
                    return Ok(());
                };
                // The scoped poll closure has released its relationship borrow.
                // Application conversion occurs outside the owner guard. A
                // consuming panic preserves only available outside values and
                // the original task failure, not destroyed application inputs.
                #[cfg(test)]
                super::atomic_interpretation_contract::hold_acquired_completion(id, at);
                let event = C::Event::inject_at(EstablishedObservation::<Target>::stopped(
                    relationship,
                    outcome,
                    at,
                ));
                let observation_admission =
                    notifications.lock().unwrap_or_else(PoisonError::into_inner);
                if observation_admission.is_none() {
                    return Err(event);
                }
                // Retirement takes this owner before draining control. Admission
                // completes while the same owner still possesses notification.
                control.send(event).map_err(|ControlClosed(event)| event)
            });
    }

    fn cancel(&mut self, request: CancelObservation<Target>) {
        let id = request.id();
        let mut observation_admission = self
            .capabilities
            .exact_observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(observations) = observation_admission.as_mut() else {
            unreachable!("exclusive live capability interpretation precedes capability retirement");
        };
        let matching = observations
            .get(&id)
            .is_some_and(|(identity, _)| Arc::ptr_eq(identity, request.relationship().identity()));
        let cancellation = if matching {
            observations.remove(&id)
        } else {
            None
        };
        drop(observation_admission);
        match cancellation {
            Some((_, cancellation)) => {
                match cancellation.send(()) {
                    Ok(()) | Err(()) => {}
                }
                self.capabilities.inject_control_event::<_, Path>(
                    EstablishedObservation::<Target>::cancelled(request),
                );
            }
            None => self.capabilities.inject_control_event::<_, Path>(
                EstablishedObservation::cancel_rejected(request, ObservationRejection::NotObserved),
            ),
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretItem<ObserveEstablished<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ObserveEstablished<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ObserveEstablished<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ObserveEstablished<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Target, Path>
    InterpretItem<CancelObservation<Target>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Target: Protocol<Addr = MailAddr> + Send + 'static,
    Target::Msg: Send,
    C::Event: InjectEvent<EstablishedObservation<Target>, Path> + Send + 'static,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <CancelObservation<Target> as ActionItem>::Input<'a>,
        received: &'a mut Option<<CancelObservation<Target> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        CancelObservation<Target>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                request.interpret(&mut EstablishedObservationInterpreter::<_, Path>::new(self));
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, Parent, Bindings, Origins, Child, TargetPath>
    InterpretEstablishedShutdown<Child, TargetPath>
    for ApplicationCapabilities<C, N, Parent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, TargetPath>,
{
    fn shutdown(
        &mut self,
        _id: ShutdownId,
        installed: InstalledActor<Child>,
        ingress: behavior::Ingress<ShutdownRequested, TargetPath>,
    ) -> Result<(), ShutdownRejection> {
        installed.request_shutdown(ingress)
    }
}

impl<C, N, P, Bindings, Origins, Child, Path, TargetPath>
    InterpretItem<ShutdownEstablished<Child, TargetPath>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: InjectEvent<EstablishedShutdownResolved<Child::Protocol>, Path> + Send + 'static,
    Child: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Child::Event: InjectEvent<ShutdownRequested, TargetPath> + Send,
    BehaviorMessage<Child>: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ShutdownEstablished<Child, TargetPath> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ShutdownEstablished<Child, TargetPath> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ShutdownEstablished<Child, TargetPath>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let shutdown = request.id;
            *received = Some(request.settle(self));
            let resolution = match received.as_ref() {
                Some(ItemSettlement::Accepted(_)) => {
                    EstablishedShutdownResolved::accepted(shutdown)
                }
                Some(ItemSettlement::Rejected { reason, .. }) => {
                    EstablishedShutdownResolved::rejected(shutdown, *reason)
                }
                Some(ItemSettlement::Blocked { prerequisite, .. }) => match *prerequisite {},
                Some(ItemSettlement::Corrupt { .. }) => {
                    unreachable!("exact shutdown settlement cannot create interpreter corruption")
                }
                None => return,
            };
            self.inject_control_event::<_, Path>(resolution);
        }
    }
}

impl<A, E> TerminationObservations<A, E>
where
    A: Address,
{
    pub(crate) fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    pub(crate) fn insert_peer<Path>(&mut self, address: A, observation: Observation<Termination<A>>)
    where
        E: InjectEvent<PeerStopped<A>, Path>,
    {
        self.insert(
            observation,
            TerminationSource::Peer(address),
            inject_peer::<A, E, Path>,
        );
    }

    pub(crate) fn insert_child<Path>(
        &mut self,
        creation: CreationId,
        observation: Observation<Termination<A>>,
    ) where
        E: InjectEvent<ChildStopped<A>, Path>,
    {
        self.insert(
            observation,
            TerminationSource::Child(creation),
            inject_child::<A, E, Path>,
        );
    }

    fn insert(
        &mut self,
        observation: Observation<Termination<A>>,
        source: TerminationSource<A>,
        inject: InjectTermination<A, E>,
    ) {
        self.pending.push(PendingTermination {
            future: observation.into_future(),
            source,
            inject,
        });
    }

    pub(crate) async fn next(&mut self) -> E {
        poll_fn(|context| {
            for index in 0..self.pending.len() {
                if let Poll::Ready(outcome) =
                    Pin::new(&mut self.pending[index].future).poll(context)
                {
                    let pending = self.pending.remove(index);
                    return Poll::Ready((pending.inject)(pending.source, outcome));
                }
            }
            Poll::Pending
        })
        .await
    }
}

pub(in crate::local::effects) struct EstablishedObservationInterpreter<'a, Capabilities, Path> {
    capabilities: &'a mut Capabilities,
    path: PhantomData<fn() -> Path>,
}

#[derive(Clone, Copy)]
enum TerminationSource<A: Address> {
    Peer(A),
    Child(CreationId),
}

type InjectTermination<A, E> = fn(TerminationSource<A>, Termination<A>) -> E;

struct PendingTermination<A: Address, E> {
    future: ObservationFuture<Termination<A>>,
    source: TerminationSource<A>,
    inject: InjectTermination<A, E>,
}

/// Pending observations owned by one actor's capability interpreter.
/// Among observations ready at one poll, the earliest registration is delivered first.
pub(crate) struct TerminationObservations<A: Address, E> {
    pending: Vec<PendingTermination<A, E>>,
}

fn inject_peer<A, E, Path>(source: TerminationSource<A>, outcome: Termination<A>) -> E
where
    A: Address,
    E: InjectEvent<PeerStopped<A>, Path>,
{
    let TerminationSource::Peer(address) = source else {
        unreachable!("peer fact mapper receives only peer sources");
    };
    E::inject_at(PeerStopped::new(address, outcome))
}

fn inject_child<A, E, Path>(source: TerminationSource<A>, outcome: Termination<A>) -> E
where
    A: Address,
    E: InjectEvent<ChildStopped<A>, Path>,
{
    let TerminationSource::Child(creation) = source else {
        unreachable!("child fact mapper receives only child sources");
    };
    E::inject_at(ChildStopped::new(creation, outcome, Instant::now()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ObservationError<A> {
    #[error("no live actor generation exists at the requested address")]
    Unknown(A),
}

/// Register an exact peer observation in the actor-owned fact queue.
pub(crate) fn observe_peer<P, E, Path>(
    peers: &ActorSpace<P>,
    request: ObservePeer<P::Addr>,
    observations: &mut TerminationObservations<P::Addr, E>,
) -> Result<(), ObservationError<P::Addr>>
where
    P::Addr: Hash,
    P: Protocol,
    P::Addr: Send + Sync + 'static,
    P::Msg: Send,
    E: InjectEvent<PeerStopped<P::Addr>, Path> + Send + 'static,
{
    let peer = peers
        .resolve(&request.peer)
        .ok_or(ObservationError::Unknown(request.peer))?;
    let observation = peer.termination_observation();
    observations.insert_peer::<Path>(request.peer, observation);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::task::{Context, Waker};

    use behavior::{Actions, Behavior, BehaviorActed, InitializationTurn, Never, NoBirths, User};
    use behavior_actors::{Exit, StopOnShutdown};
    use communication::Config;

    use super::*;
    use crate::MailAddr;

    struct Peer;

    impl Behavior for Peer {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::stop())
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum ObserverEvent {
        Stopped(PeerStopped<MailAddr>),
    }

    impl InjectEvent<PeerStopped<MailAddr>, behavior::Here> for ObserverEvent {
        fn inject_at(event: PeerStopped<MailAddr>) -> Self {
            Self::Stopped(event)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum LayeredObserverEvent {
        Outer(PeerStopped<MailAddr>),
        Inner(PeerStopped<MailAddr>),
    }

    impl InjectEvent<PeerStopped<MailAddr>, behavior::Here> for LayeredObserverEvent {
        fn inject_at(event: PeerStopped<MailAddr>) -> Self {
            Self::Outer(event)
        }
    }

    impl InjectEvent<PeerStopped<MailAddr>, behavior::Inside<behavior::Here>> for LayeredObserverEvent {
        fn inject_at(event: PeerStopped<MailAddr>) -> Self {
            Self::Inner(event)
        }
    }

    #[tokio::test]
    async fn observation_captures_the_exact_resolved_generation() {
        let peers = ActorSpace::<<Peer as Behavior>::Protocol>::new();
        let peer = crate::launch::launch_inert(
            peers.clone(),
            Config::new(2),
            MailAddr(2),
            StopOnShutdown::new(Peer),
            |_| {},
        )
        .await
        .unwrap();
        let mut observations = TerminationObservations::<MailAddr, ObserverEvent>::new();
        observe_peer::<_, _, behavior::Here>(
            &peers,
            ObservePeer::new(MailAddr(2)),
            &mut observations,
        )
        .unwrap();
        peer.actor.send_from(MailAddr(1), ()).await.unwrap();

        let stopped = observations.next().await;
        assert_eq!(
            stopped,
            ObserverEvent::Stopped(PeerStopped::new(MailAddr(2), Ok(Exit::Normal),))
        );
    }

    #[tokio::test]
    async fn unknown_peer_is_an_error() {
        let peers = ActorSpace::<<Peer as Behavior>::Protocol>::new();
        let mut observations = TerminationObservations::new();

        assert_eq!(
            observe_peer::<_, ObserverEvent, behavior::Here>(
                &peers,
                ObservePeer::new(MailAddr(9)),
                &mut observations
            ),
            Err(ObservationError::Unknown(MailAddr(9)))
        );
    }

    #[tokio::test]
    async fn structural_observations_of_one_peer_are_multiplicity_preserving() {
        let peers = ActorSpace::<<Peer as Behavior>::Protocol>::new();
        let peer = crate::launch::launch_inert(
            peers.clone(),
            Config::new(2),
            MailAddr(2),
            StopOnShutdown::new(Peer),
            |_| {},
        )
        .await
        .unwrap();
        let mut observations = TerminationObservations::<MailAddr, LayeredObserverEvent>::new();
        observe_peer::<_, _, behavior::Here>(
            &peers,
            ObservePeer::new(MailAddr(2)),
            &mut observations,
        )
        .unwrap();
        observe_peer::<_, _, behavior::Inside<behavior::Here>>(
            &peers,
            ObservePeer::new(MailAddr(2)),
            &mut observations,
        )
        .unwrap();
        peer.actor.send_from(MailAddr(1), ()).await.unwrap();

        let first = observations.next().await;
        let second = observations.next().await;
        assert_eq!(
            first,
            LayeredObserverEvent::Outer(PeerStopped::new(MailAddr(2), Ok(Exit::Normal)))
        );
        assert_eq!(
            second,
            LayeredObserverEvent::Inner(PeerStopped::new(MailAddr(2), Ok(Exit::Normal)))
        );
    }

    #[tokio::test]
    async fn simultaneously_ready_facts_follow_registration_after_wait_cancellation() {
        let mut observations = TerminationObservations::<MailAddr, ObserverEvent>::new();
        let (first_publisher, first_observation) = crate::observe::pair();
        let (second_publisher, second_observation) = crate::observe::pair();
        let (third_publisher, third_observation) = crate::observe::pair();

        observations.insert_peer::<behavior::Here>(MailAddr(1), first_observation);
        observations.insert_peer::<behavior::Here>(MailAddr(2), second_observation);
        observations.insert_peer::<behavior::Here>(MailAddr(3), third_observation);

        let mut waiting = Box::pin(observations.next());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let pending = waiting.as_mut().poll(&mut context);
        assert!(pending.is_pending());
        drop(waiting);

        first_publisher.complete(Ok(Exit::Normal));
        second_publisher.complete(Ok(Exit::Normal));
        third_publisher.complete(Ok(Exit::Normal));

        let first = observations.next().await;
        let second = observations.next().await;
        let third = observations.next().await;
        assert_eq!(
            [first, second, third],
            [
                ObserverEvent::Stopped(PeerStopped::new(MailAddr(1), Ok(Exit::Normal))),
                ObserverEvent::Stopped(PeerStopped::new(MailAddr(2), Ok(Exit::Normal))),
                ObserverEvent::Stopped(PeerStopped::new(MailAddr(3), Ok(Exit::Normal))),
            ]
        );
    }

    #[test]
    fn inserting_a_fact_with_spare_queue_capacity_does_not_allocate() {
        let first = crate::observe::pair::<Termination<MailAddr>>().1;
        let second = crate::observe::pair::<Termination<MailAddr>>().1;
        let mut observations = TerminationObservations::<MailAddr, ObserverEvent>::new();

        observations.insert_peer::<behavior::Here>(MailAddr(1), first);
        let allocations = crate::actor_execution::tests::allocations_during(|| {
            observations.insert_peer::<behavior::Here>(MailAddr(2), second);
        });

        assert_eq!(allocations, 0, "one allocation remains per pending fact");
    }
}

#[cfg(test)]
mod installed_shutdown_contract {

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, ChildCreationOutcome,
        ChildHead, CreateChild, CreationKind, CreationSequence, EstablishChild, EventLayer, Here,
        Ingress, Inside, InterpretItem, ItemSettlement, MessageProtocol, Never, NoBirths, NoSends,
        RoutedCreation, Step, User, delegate_transition,
    };
    use behavior_actors::{
        EstablishedShutdownResolved, Exit, ShutdownEstablished, ShutdownId, ShutdownRejection,
        ShutdownRequested, StopOnShutdown,
    };
    use bombay_engine::Completion;
    use communication::{Config, mailbox_channel};

    use std::sync::Arc;
    use tokio::sync::oneshot;

    use crate::address::{ApplicationAddresses, MailAddr};

    use crate::local::children::StructuralOrigins;
    use crate::local::children::{ChildBinding, NoChildBindings, RetireChildTasks};
    use crate::local::effects::observation::TerminationObservations;
    use crate::local::effects::reports::LocalTerminalReports;
    use crate::local::effects::timers::LocalTimers;
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs, NoParent};
    use crate::local::endpoint::ExtractLocalEndpoint;

    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};

    use crate::{LedgerProtocol, ShutdownLedger};
    type DirectLedger = StopOnShutdown<ShutdownLedger>;
    type NestedLedger = StopOnShutdown<DirectLedger>;
    #[derive(Default)]
    struct ShutdownObserver {
        resolutions: Vec<(ShutdownId, Result<(), ShutdownRejection>)>,
    }
    impl Behavior for ShutdownObserver {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = EventLayer<EstablishedShutdownResolved<LedgerProtocol>, User<MailAddr, Never>>;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                EventLayer::Owned(EstablishedShutdownResolved::Accepted { id, .. }) => {
                    self.resolutions.push((id, Ok(())));
                }
                EventLayer::Owned(EstablishedShutdownResolved::Rejected { id, reason, .. }) => {
                    self.resolutions.push((id, Err(reason)));
                }
                EventLayer::Inner(user) => match user.message {},
            }
            Ok(Actions::cont())
        }
    }
    impl BehaviorBase for ShutdownObserver {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    enum ShutdownChildren {
        Direct {
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<DirectLedger, Self, ()>,
        },
        Nested {
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<NestedLedger, Self, ()>,
        },
    }
    impl
        ProjectTerminal<
            ChildOrigin<ShutdownObserver, ChildHead>,
            ActorRetirement<DirectLedger, Self, ()>,
        > for ShutdownChildren
    {
        fn project(
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<DirectLedger, Self, ()>,
        ) -> Self {
            Self::Direct { origin, retirement }
        }
    }
    impl
        ProjectTerminal<
            ChildOrigin<ShutdownObserver, ChildHead>,
            ActorRetirement<NestedLedger, Self, ()>,
        > for ShutdownChildren
    {
        fn project(
            origin: ChildOrigin<ShutdownObserver, ChildHead>,
            retirement: ActorRetirement<NestedLedger, Self, ()>,
        ) -> Self {
            Self::Nested { origin, retirement }
        }
    }
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep complete commitment, typed transfer, and both original terminal products in one trace."
    )]
    async fn committed_distinct_event_targets_transfer_to_unrelated_shutdown_interpreter() {
        let allocations = ApplicationAddresses::new();
        let (direct_control, direct_owner, direct_mailbox, direct_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (direct_terminal, direct_report) = oneshot::channel();
        let mut direct_creator = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            ChildBinding<
                ChildHead,
                DirectLedger,
                ShutdownChildren,
                StructuralOrigins<ShutdownObserver>,
                NoChildBindings<ShutdownChildren>,
            >,
            StructuralOrigins<ShutdownObserver>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(501),
                actor_spaces: Arc::new(()),
                allocations: allocations.clone(),
                control: direct_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(direct_terminal),
            },
            ChildBinding::default(),
        );
        let (nested_control, nested_owner, nested_mailbox, nested_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (nested_terminal, nested_report) = oneshot::channel();
        let mut nested_creator = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            ChildBinding<
                ChildHead,
                NestedLedger,
                ShutdownChildren,
                StructuralOrigins<ShutdownObserver>,
                NoChildBindings<ShutdownChildren>,
            >,
            StructuralOrigins<ShutdownObserver>,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(503),
                actor_spaces: Arc::new(()),
                allocations,
                control: nested_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(nested_terminal),
            },
            ChildBinding::default(),
        );
        let mut creations = CreationSequence::new();
        let direct_id = creations.issue().expect("direct child identity");
        let nested_id = creations.issue().expect("nested child identity");
        let direct_entries = vec![11, 13];
        let direct_allocation = direct_entries.as_ptr();
        let direct = EstablishChild::<ChildHead, DirectLedger>::establish_child(
            &mut direct_creator,
            RoutedCreation::new(
                CreateChild::birth(
                    direct_id,
                    StopOnShutdown::new(ShutdownLedger {
                        entries: direct_entries,
                        received: Vec::new(),
                    }),
                ),
                0,
            ),
        )
        .await;
        let ItemSettlement::Accepted(direct) = direct else {
            panic!("actual direct child commitment")
        };
        let ChildCreationOutcome::Established(committed) = &direct else {
            panic!("actual committed direct child")
        };
        assert_eq!(committed.id(), direct_id);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let direct = direct
            .into_actor()
            .unwrap_or_else(|_| panic!("original direct installed authority"));
        let nested_entries = vec![17, 19];
        let nested_allocation = nested_entries.as_ptr();
        let nested = EstablishChild::<ChildHead, NestedLedger>::establish_child(
            &mut nested_creator,
            RoutedCreation::new(
                CreateChild::birth(
                    nested_id,
                    StopOnShutdown::new(StopOnShutdown::new(ShutdownLedger {
                        entries: nested_entries,
                        received: Vec::new(),
                    })),
                ),
                1,
            ),
        )
        .await;
        let ItemSettlement::Accepted(nested) = nested else {
            panic!("actual nested child commitment")
        };
        let ChildCreationOutcome::Established(committed) = &nested else {
            panic!("actual committed nested child")
        };
        assert_eq!(committed.id(), nested_id);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let nested = nested
            .into_actor()
            .unwrap_or_else(|_| panic!("original nested installed authority"));
        let direct_endpoint = direct.recipient().interpret(&mut ExtractLocalEndpoint);
        let nested_endpoint = nested.recipient().interpret(&mut ExtractLocalEndpoint);
        let (observer_control, observer_owner, observer_mailbox, mut observer_receiver) =
            mailbox_channel::<<ShutdownObserver as Behavior>::Event, User<MailAddr, Never>>(
                Config::new(1),
            );
        let (observer_terminal, observer_report) = oneshot::channel();
        let mut observer = ApplicationCapabilities::<
            ShutdownObserver,
            (),
            NoParent,
            NoChildBindings,
        >::new_with_bindings(
            ApplicationCapabilityInputs {
                address: MailAddr(509),
                actor_spaces: Arc::new(()),
                allocations: ApplicationAddresses::new(),
                control: observer_control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(observer_terminal),
            },
            NoChildBindings::default(),
        );
        let direct_request = ShutdownEstablished::new(
            ShutdownId(23),
            direct.clone(),
            Ingress::<ShutdownRequested, Here>::new(),
        );
        let mut shutdown_request = Some(direct_request);
        let mut accepted = None;
        InterpretItem::<
            ShutdownEstablished<DirectLedger, Here>,
            <ShutdownObserver as Behavior>::Event,
            Here,
        >::interpret_item(&mut observer, &mut shutdown_request, &mut accepted)
        .await;
        let Some(accepted) = accepted else {
            panic!("actual native shutdown admission reply")
        };
        assert!(shutdown_request.is_none());
        assert!(matches!(accepted, ItemSettlement::Accepted(ShutdownId(23))));
        let nested_request = ShutdownEstablished::new(
            ShutdownId(29),
            nested.clone(),
            Ingress::<ShutdownRequested, Inside<Here>>::new(),
        );
        let mut shutdown_request = Some(nested_request);
        let mut accepted = None;
        InterpretItem::<
            ShutdownEstablished<NestedLedger, Inside<Here>>,
            <ShutdownObserver as Behavior>::Event,
            Here,
        >::interpret_item(&mut observer, &mut shutdown_request, &mut accepted)
        .await;
        let Some(accepted) = accepted else {
            panic!("actual native shutdown admission reply")
        };
        assert!(shutdown_request.is_none());
        assert!(matches!(accepted, ItemSettlement::Accepted(ShutdownId(29))));
        let direct_stopped = direct_endpoint.termination().await;
        let nested_stopped = nested_endpoint.termination().await;
        assert_eq!(direct_stopped, Ok(Exit::Normal));
        assert_eq!(nested_stopped, Ok(Exit::Normal));
        for id in [31, 37] {
            let request = ShutdownEstablished::new(
                ShutdownId(id),
                nested.clone(),
                Ingress::<ShutdownRequested, Inside<Here>>::new(),
            );
            let mut shutdown_request = Some(request);
            let mut rejected = None;
            InterpretItem::<
                ShutdownEstablished<NestedLedger, Inside<Here>>,
                <ShutdownObserver as Behavior>::Event,
                Here,
            >::interpret_item(&mut observer, &mut shutdown_request, &mut rejected)
            .await;
            let Some(rejected) = rejected else {
                panic!("actual native shutdown admission reply")
            };
            assert!(shutdown_request.is_none());
            let ItemSettlement::Rejected { item, reason } = rejected else {
                panic!("exact stopped nested request")
            };
            assert_eq!(item.id, ShutdownId(id));
            let rejected_endpoint = item
                .actor()
                .recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(rejected_endpoint.address(), nested_endpoint.address());
            let rejected_terminal = rejected_endpoint.termination().await;
            assert_eq!(rejected_terminal, Ok(Exit::Normal));
            assert_eq!(reason, ShutdownRejection::AlreadyStopped);
            let mut shutdown_request = Some(item);
            let mut retry = None;
            InterpretItem::<
                ShutdownEstablished<NestedLedger, Inside<Here>>,
                <ShutdownObserver as Behavior>::Event,
                Here,
            >::interpret_item(&mut observer, &mut shutdown_request, &mut retry)
            .await;
            let Some(retry) = retry else {
                panic!("actual native shutdown admission reply")
            };
            assert!(shutdown_request.is_none());
            let ItemSettlement::Rejected { item, reason } = retry else {
                panic!("replayed original stopped request")
            };
            assert_eq!(item.id, ShutdownId(id));
            assert_eq!(reason, ShutdownRejection::AlreadyStopped);
            let replayed_endpoint = item
                .actor()
                .recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(replayed_endpoint.address(), nested_endpoint.address());
            let replayed_terminal = replayed_endpoint.termination().await;
            assert_eq!(replayed_terminal, Ok(Exit::Normal));
        }
        let mut resolution_ledger = ShutdownObserver::default();
        for _ in 0..6 {
            let event = observer_receiver
                .recv_control()
                .await
                .expect("one complete immediate resolution");
            let actions = delegate_transition(&mut resolution_ledger, event).unwrap();
            assert!(actions.creates.is_empty());
            assert!(matches!(actions.sends, NoSends));
            assert!(matches!(actions.become_, Step::Continue));
        }
        assert_eq!(
            resolution_ledger.resolutions,
            [
                (ShutdownId(23), Ok(())),
                (ShutdownId(29), Ok(())),
                (ShutdownId(31), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(31), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(37), Err(ShutdownRejection::AlreadyStopped)),
                (ShutdownId(37), Err(ShutdownRejection::AlreadyStopped))
            ]
        );
        let (direct_retired, (direct_failures, ())) = direct_creator
            .child_bindings
            .take()
            .expect("original child bindings remain installed")
            .retire_child_tasks()
            .await;
        let (nested_retired, (nested_failures, ())) = nested_creator
            .child_bindings
            .take()
            .expect("original child bindings remain installed")
            .retire_child_tasks()
            .await;
        assert!(direct_failures.is_empty());
        assert!(nested_failures.is_empty());
        let [ShutdownChildren::Direct { origin, retirement }] = direct_retired.as_slice() else {
            panic!("one original direct result")
        };
        assert_eq!(origin.address(), direct_endpoint.address());
        assert_eq!(origin.nonce(), 0);
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
            capability_failures,
            unread_owner_cancellation,
        } = retirement
        else {
            panic!("direct completed result")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert_eq!(behavior.base().entries, [11, 13]);
        assert_eq!(behavior.base().entries.as_ptr(), direct_allocation);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(*completion, Completion::Stopped);
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        let [ShutdownChildren::Nested { origin, retirement }] = nested_retired.as_slice() else {
            panic!("one original nested result")
        };
        assert_eq!(origin.address(), nested_endpoint.address());
        assert_eq!(origin.nonce(), 1);
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            completion,
            capability_failures,
            unread_owner_cancellation,
        } = retirement
        else {
            panic!("nested completed result")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert_eq!(behavior.base().entries, [17, 19]);
        assert_eq!(behavior.base().entries.as_ptr(), nested_allocation);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(*completion, Completion::Stopped);
        assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        drop((
            direct_owner,
            direct_mailbox,
            direct_receiver,
            direct_report,
            nested_owner,
            nested_mailbox,
            nested_receiver,
            nested_report,
            observer_owner,
            observer_mailbox,
            observer_report,
        ));
    }
}
