//! Actor-local delivery of peer and child termination facts.

use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use behavior::{Address, CreationId, InjectEvent, Protocol};
use behavior_actors::{ChildStopped, ObservePeer, PeerStopped};
use core::future::{Future, IntoFuture, poll_fn};
use core::pin::Pin;
use core::task::Poll;

use crate::launch::LocalAddresses;
use crate::local::Termination;
use crate::observe::{Observation, ObservationFuture};

#[derive(Clone, Copy)]
enum FactSource<A: Address> {
    Peer(A),
    Child(CreationId),
}

type InjectFact<A, E> = fn(FactSource<A>, Termination<A>) -> E;

struct PendingFact<A: Address, E> {
    future: ObservationFuture<Termination<A>>,
    source: FactSource<A>,
    inject: InjectFact<A, E>,
}

struct FactState<A: Address, E> {
    pending: Vec<PendingFact<A, E>>,
}

/// Pending capability facts polled by the actor's single Environment spine.
pub(crate) struct FactQueue<A: Address, E> {
    state: Arc<Mutex<FactState<A, E>>>,
}

impl<A: Address, E> Clone for FactQueue<A, E> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<A, E> FactQueue<A, E>
where
    A: Address,
{
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(FactState {
                pending: Vec::new(),
            })),
        }
    }

    pub(crate) fn insert_peer<Path>(&self, address: A, observation: Observation<Termination<A>>)
    where
        E: InjectEvent<PeerStopped<A>, Path>,
    {
        self.insert(
            observation,
            FactSource::Peer(address),
            inject_peer::<A, E, Path>,
        );
    }

    pub(crate) fn insert_child<Path>(
        &self,
        creation: CreationId,
        observation: Observation<Termination<A>>,
    ) where
        E: InjectEvent<ChildStopped<A>, Path>,
    {
        self.insert(
            observation,
            FactSource::Child(creation),
            inject_child::<A, E, Path>,
        );
    }

    fn insert(
        &self,
        observation: Observation<Termination<A>>,
        source: FactSource<A>,
        inject: InjectFact<A, E>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.pending.push(PendingFact {
            future: observation.into_future(),
            source,
            inject,
        });
    }

    pub(crate) async fn next(&self) -> E {
        poll_fn(|context| {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            for index in 0..state.pending.len() {
                if let Poll::Ready(outcome) =
                    Pin::new(&mut state.pending[index].future).poll(context)
                {
                    let pending = state.pending.swap_remove(index);
                    return Poll::Ready((pending.inject)(pending.source, outcome));
                }
            }
            Poll::Pending
        })
        .await
    }
}

fn inject_peer<A, E, Path>(source: FactSource<A>, outcome: Termination<A>) -> E
where
    A: Address,
    E: InjectEvent<PeerStopped<A>, Path>,
{
    let FactSource::Peer(address) = source else {
        unreachable!("peer fact mapper receives only peer sources");
    };
    E::inject_at(PeerStopped::new(address, outcome))
}

fn inject_child<A, E, Path>(source: FactSource<A>, outcome: Termination<A>) -> E
where
    A: Address,
    E: InjectEvent<ChildStopped<A>, Path>,
{
    let FactSource::Child(creation) = source else {
        unreachable!("child fact mapper receives only child sources");
    };
    E::inject_at(ChildStopped::new(creation, outcome, Instant::now()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ObservationError<A> {
    #[error("no live actor generation exists at the requested address")]
    Unknown(A),
}

/// Observations installed by one actor for one concrete peer protocol.
pub(crate) struct LocalPeerObservations<P: Protocol, E>
where
    P::Addr: Hash,
{
    peers: LocalAddresses<P>,
    facts: FactQueue<P::Addr, E>,
}

impl<P, E> LocalPeerObservations<P, E>
where
    P: Protocol,
    P::Addr: Hash,
{
    pub(crate) fn new(peers: LocalAddresses<P>, facts: FactQueue<P::Addr, E>) -> Self {
        Self { peers, facts }
    }

    pub(crate) fn observe<Path>(
        &mut self,
        request: ObservePeer<P::Addr>,
    ) -> Result<(), ObservationError<P::Addr>>
    where
        P::Addr: Hash + Send + Sync + 'static,
        P::Msg: Send,
        E: InjectEvent<PeerStopped<P::Addr>, Path> + Send + 'static,
    {
        let peer = self
            .peers
            .resolve(&request.peer)
            .ok_or(ObservationError::Unknown(request.peer))?;
        let observation = peer.termination_observation();
        let address = request.peer;
        self.facts.insert_peer::<Path>(address, observation);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

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
        let peers = crate::launch::LocalAddresses::<<Peer as Behavior>::Protocol>::new();
        let peer = crate::launch::launch_inert(
            peers.clone(),
            Config::new(2),
            MailAddr(2),
            StopOnShutdown::new(Peer),
            |_| {},
        )
        .await
        .unwrap();
        let facts = FactQueue::<MailAddr, ObserverEvent>::new();
        let mut observations = LocalPeerObservations::new(peers, facts.clone());

        observations
            .observe::<behavior::Here>(ObservePeer::new(MailAddr(2)))
            .unwrap();
        peer.send_from(MailAddr(1), ()).await.unwrap();

        assert_eq!(
            facts.next().await,
            ObserverEvent::Stopped(PeerStopped::new(MailAddr(2), Ok(Exit::Normal),))
        );
        drop(observations);
    }

    #[tokio::test]
    async fn unknown_peer_is_an_error() {
        let peers = crate::launch::LocalAddresses::<<Peer as Behavior>::Protocol>::new();
        let mut observations =
            LocalPeerObservations::new(peers, FactQueue::<MailAddr, ObserverEvent>::new());

        assert_eq!(
            observations.observe::<behavior::Here>(ObservePeer::new(MailAddr(9))),
            Err(ObservationError::Unknown(MailAddr(9)))
        );
    }

    #[tokio::test]
    async fn structural_observations_of_one_peer_are_multiplicity_preserving() {
        let peers = crate::launch::LocalAddresses::<<Peer as Behavior>::Protocol>::new();
        let peer = crate::launch::launch_inert(
            peers.clone(),
            Config::new(2),
            MailAddr(2),
            StopOnShutdown::new(Peer),
            |_| {},
        )
        .await
        .unwrap();
        let facts = FactQueue::<MailAddr, LayeredObserverEvent>::new();
        let mut observations = LocalPeerObservations::new(peers, facts.clone());

        observations
            .observe::<behavior::Here>(ObservePeer::new(MailAddr(2)))
            .unwrap();
        observations
            .observe::<behavior::Inside<behavior::Here>>(ObservePeer::new(MailAddr(2)))
            .unwrap();
        peer.send_from(MailAddr(1), ()).await.unwrap();

        let first = facts.next().await;
        let second = facts.next().await;
        assert!(matches!(
            (&first, &second),
            (
                LayeredObserverEvent::Outer(PeerStopped {
                    outcome: Ok(Exit::Normal),
                    ..
                }),
                LayeredObserverEvent::Inner(PeerStopped {
                    outcome: Ok(Exit::Normal),
                    ..
                })
            ) | (
                LayeredObserverEvent::Inner(PeerStopped {
                    outcome: Ok(Exit::Normal),
                    ..
                }),
                LayeredObserverEvent::Outer(PeerStopped {
                    outcome: Ok(Exit::Normal),
                    ..
                })
            )
        ));
    }

    #[test]
    fn inserting_a_fact_with_spare_queue_capacity_does_not_allocate() {
        let first = crate::observe::pair::<Termination<MailAddr>>().1;
        let second = crate::observe::pair::<Termination<MailAddr>>().1;
        let facts = FactQueue::<MailAddr, ObserverEvent>::new();

        facts.insert_peer::<behavior::Here>(MailAddr(1), first);
        let allocations = crate::incarnation::tests::allocations_during(|| {
            facts.insert_peer::<behavior::Here>(MailAddr(2), second);
        });

        assert_eq!(allocations, 0, "one allocation remains per pending fact");
    }
}
