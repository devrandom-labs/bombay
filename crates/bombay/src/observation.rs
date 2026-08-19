//! Actor-local interpretation of exact-generation observation requests.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::Poll;

use behavior::{InjectEvent, ObservePeer, PeerStopped, Protocol, UnwatchPeer};

use crate::interpret::RetireCapabilities;
use crate::launch::LocalAddresses;

type FactFuture<E> = Pin<Box<dyn Future<Output = E> + Send + 'static>>;

struct FactState<E> {
    next: u64,
    pending: Vec<(u64, FactFuture<E>)>,
}

/// Pending capability facts polled by the actor's single Environment spine.
pub(crate) struct FactQueue<E> {
    state: std::sync::Arc<std::sync::Mutex<FactState<E>>>,
}

impl<E> Clone for FactQueue<E> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<E> FactQueue<E> {
    pub(crate) fn new() -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(FactState {
                next: 0,
                pending: Vec::new(),
            })),
        }
    }

    pub(crate) fn insert(&self, future: impl Future<Output = E> + Send + 'static) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = state.next;
        state.next = state.next.wrapping_add(1);
        state.pending.push((id, Box::pin(future)));
        id
    }

    pub(crate) fn remove(&self, id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = state.pending.iter().position(|(pending, _)| *pending == id) {
            drop(state.pending.swap_remove(index));
        }
    }

    pub(crate) async fn next(&self) -> E {
        std::future::poll_fn(|context| {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for index in 0..state.pending.len() {
                if let Poll::Ready(event) = state.pending[index].1.as_mut().poll(context) {
                    drop(state.pending.swap_remove(index));
                    return Poll::Ready(event);
                }
            }
            Poll::Pending
        })
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ObservationError<A> {
    #[error("no live actor generation exists at the requested address")]
    Unknown(A),
}

impl<P, E> RetireCapabilities for LocalPeerObservations<P, E>
where
    P: Protocol,
    P::Addr: Hash + Send,
    P::Msg: Send,
    LocalPeerObservations<P, E>: Send,
{
    async fn retire(self) {}
}

/// Observations installed by one actor for one concrete peer protocol.
pub(crate) struct LocalPeerObservations<P: Protocol, E>
where
    P::Addr: Hash,
{
    peers: LocalAddresses<P>,
    facts: FactQueue<E>,
    watches: HashMap<P::Addr, u64>,
}

impl<P, E> LocalPeerObservations<P, E>
where
    P: Protocol,
    P::Addr: Hash,
{
    pub(crate) fn new(peers: LocalAddresses<P>, facts: FactQueue<E>) -> Self {
        Self {
            peers,
            facts,
            watches: HashMap::new(),
        }
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
        let observation = peer.termination();
        let address = request.peer;
        let watch = self
            .facts
            .insert(async move { E::inject_at(PeerStopped::new(address, observation.await)) });
        if let Some(previous) = self.watches.insert(address, watch) {
            self.facts.remove(previous);
        }
        Ok(())
    }

    pub(crate) fn unwatch(&mut self, request: UnwatchPeer<P::Addr>) {
        if let Some(watch) = self.watches.remove(&request.peer) {
            self.facts.remove(watch);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::ready;

    use behavior::{
        Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths, User,
    };
    use communication::Config;

    use super::*;

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

    #[tokio::test]
    async fn observation_captures_the_exact_resolved_generation() {
        let peers = crate::launch::LocalAddresses::<<Peer as Behavior>::Protocol>::new();
        let peer = crate::launch::spawn(
            peers.clone(),
            Config::new(2),
            MailAddr(2),
            behavior::StopOnShutdown::new(Peer),
            |_| ready(Ok::<_, Infallible>(())),
        )
        .await
        .unwrap();
        let facts = FactQueue::<ObserverEvent>::new();
        let mut observations = LocalPeerObservations::new(peers, facts.clone());

        observations
            .observe::<behavior::Here>(ObservePeer::new(MailAddr(2)))
            .unwrap();
        peer.send(MailAddr(1), ()).await.unwrap();

        assert_eq!(
            facts.next().await,
            ObserverEvent::Stopped(PeerStopped::new(MailAddr(2), Ok(behavior::Exit::Normal),))
        );
        observations.retire().await;
    }

    #[tokio::test]
    async fn unknown_peer_is_an_error_and_unwatch_is_idempotent() {
        let peers = crate::launch::LocalAddresses::<<Peer as Behavior>::Protocol>::new();
        let mut observations = LocalPeerObservations::new(peers, FactQueue::<ObserverEvent>::new());

        assert_eq!(
            observations.observe::<behavior::Here>(ObservePeer::new(MailAddr(9))),
            Err(ObservationError::Unknown(MailAddr(9)))
        );
        observations.unwatch(UnwatchPeer::new(MailAddr(9)));
    }
}
