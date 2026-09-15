//! Static local-actor composition for one actor system.

use core::hash::Hash;
use std::sync::Arc;

use behavior::Protocol;

use crate::address::MailAddr;
use crate::launch::ActorSpace;
use crate::local::ActorRef;

/// Static proof that a local actor system hosts protocol `P`.
#[diagnostic::on_unimplemented(
    message = "the actor system does not host actor protocol `{P}` locally",
    label = "missing local actor protocol",
    note = "add an `ActorSpace<{P}>` and implement `Hosts<{P}>` for the actor-space product"
)]
pub trait Hosts<P>
where
    P: Protocol,
    P::Addr: Hash,
{
    fn space(&self) -> &ActorSpace<P>;
}

impl<P> Hosts<P> for ActorSpace<P>
where
    P: Protocol,
    P::Addr: Hash,
{
    fn space(&self) -> &ActorSpace<P> {
        self
    }
}

impl<P, N> Hosts<P> for Arc<N>
where
    P: Protocol,
    P::Addr: Hash,
    N: Hosts<P>,
{
    fn space(&self) -> &ActorSpace<P> {
        self.as_ref().space()
    }
}

/// Preserve the existing advanced host product behind an explicit logical
/// delivery policy.
pub(crate) struct HostedActorSpaces<N>(pub(crate) N);

impl<P, N> Hosts<P> for HostedActorSpaces<N>
where
    P: Protocol,
    P::Addr: Hash,
    N: Hosts<P>,
{
    fn space(&self) -> &ActorSpace<P> {
        self.0.space()
    }
}

pub(crate) trait ResolveLogical<Target>
where
    Target: Protocol<Addr = MailAddr>,
{
    fn resolve_logical(&self, address: MailAddr) -> Option<ActorRef<Target>>;
}

impl<Target, N> ResolveLogical<Target> for HostedActorSpaces<N>
where
    Target: Protocol<Addr = MailAddr>,
    N: Hosts<Target>,
{
    fn resolve_logical(&self, address: MailAddr) -> Option<ActorRef<Target>> {
        self.space()
            .resolve(&address)
            .map(|actor| actor.as_ref().clone())
    }
}
