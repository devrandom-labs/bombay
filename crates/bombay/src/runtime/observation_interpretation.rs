//! Child and peer observation interpretation for one actor incarnation.

use core::convert::Infallible;

use behavior::{
    Address, ChildStopped, EventInput, ObserveChild, ObservePeer, PeerStopped, ServiceSends,
    UnwatchPeer,
};
use std::time::Instant;

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
    S::Event: EventInput<ChildStopped<A>> + Send + 'static,
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
                let event = S::Event::inject(ChildStopped {
                    nonce: request.nonce,
                    outcome,
                    at: Instant::now(),
                });
                let _ = response.send(event).await;
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
    S::Event: EventInput<PeerStopped<A>> + Send + 'static,
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
                let event = S::Event::inject(PeerStopped {
                    peer: request.peer,
                    outcome,
                });
                let _ = response.send(event).await;
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
