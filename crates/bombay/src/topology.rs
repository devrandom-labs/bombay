//! Static local-actor composition for one actor system.

use core::hash::Hash;

use behavior::Protocol;

use crate::launch::ActorSpace;

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
