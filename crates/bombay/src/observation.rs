//! Actor-local delivery of peer and child termination observations.

use std::hash::Hash;
use std::time::Instant;

use behavior::{Address, CreationId, InjectEvent, Protocol};
use behavior_actors::{ChildStopped, ObservePeer, PeerStopped};
use core::future::{Future, IntoFuture, poll_fn};
use core::pin::Pin;
use core::task::Poll;

use crate::launch::ActorSpace;
use crate::local::Termination;
use crate::observe::{Observation, ObservationFuture};

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
