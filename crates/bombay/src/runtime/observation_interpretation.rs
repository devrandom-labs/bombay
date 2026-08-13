//! Child and peer observation interpretation for one actor incarnation.

use core::convert::Infallible;

use behavior::{
    Address, ChildEvent, ChildStopped, ObserveChild, ObservePeer, PeerEvent, PeerStopped,
    ServiceSends, UnwatchPeer,
};
use tokio::time::Instant;

use super::{Completion, IncarnationEffects, IntoPeerOutcome, classify_task};
use crate::{ChildLease, EventSender, PeerObservationError, PeerObserver, RouteSends};

impl<A, R, Edge, T, S, P>
    RouteSends<A, IncarnationEffects<R, A::Nonce, ChildLease<Edge, T>, S, P, A>>
    for ServiceSends<ObserveChild<A::Nonce>>
where
    A: Address + Send + Sync + 'static,
    A::Nonce: Send + 'static,
    R: Send,
    Edge: Send + 'static,
    T: IntoPeerOutcome<A> + Send + Sync + 'static,
    S: EventSender + Clone + Send + Sync + 'static,
    S::Event: ChildEvent<Addr = A> + Send + 'static,
    P: Send,
{
    type Error = super::child_scope::ChildObservationError<A::Nonce>;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, A::Nonce, ChildLease<Edge, T>, S, P, A>,
    ) -> Result<(), Self::Error> {
        for request in self {
            if !services.children.contains(request.nonce)
                && services.creation_was_rejected(request.nonce)
            {
                // The paired creation was rejected; the recoverable
                // result travels through the creation lane instead.
                continue;
            }
            let child = services.children.get_mut(request.nonce)?;
            let completion: Completion<T> = child
                .take_completion()
                .expect("a selectable child must own its completion seat");
            let response = services.response.clone();
            services.monitor(async move {
                let completion = completion.wait().await;
                let outcome = classify_task::<A, _>(&completion);
                if let Some(event) = <S::Event as ChildEvent>::child_stopped(ChildStopped {
                    nonce: request.nonce,
                    outcome,
                    at: Instant::now(),
                }) {
                    let _ = response.send(event).await;
                }
            });
        }
        Ok(())
    }
}

impl<A, R, N, L, S, P> RouteSends<A, IncarnationEffects<R, N, L, S, P, A>>
    for ServiceSends<ObservePeer<A>>
where
    A: Address + Send + Sync + 'static,
    R: PeerObserver<A> + Send,
    N: Send,
    L: Send,
    S: EventSender + Clone + Send + Sync + 'static,
    S::Event: PeerEvent<Addr = A> + Send + 'static,
    P: Send,
{
    type Error = PeerObservationError<A>;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, P, A>,
    ) -> Result<(), Self::Error> {
        for request in self {
            let completion = services.router.observe_peer(request.peer)?;
            let response = services.response.clone();
            services.monitor_peer(request.peer, async move {
                let outcome = completion.await;
                if let Some(event) = <S::Event as PeerEvent>::peer_stopped(PeerStopped {
                    peer: request.peer,
                    outcome,
                }) {
                    let _ = response.send(event).await;
                }
            });
        }
        Ok(())
    }
}

impl<A, R, N, L, S, P> RouteSends<A, IncarnationEffects<R, N, L, S, P, A>>
    for ServiceSends<UnwatchPeer<A>>
where
    A: Address + Send,
    R: Send,
    N: Send,
    L: Send,
    S: Send,
    P: Send,
{
    type Error = Infallible;

    async fn route(
        self,
        _from: A,
        services: &mut IncarnationEffects<R, N, L, S, P, A>,
    ) -> Result<(), Self::Error> {
        for request in self {
            services.unwatch_peer(&request.peer);
        }
        Ok(())
    }
}
