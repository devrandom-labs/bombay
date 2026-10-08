//! Static local-actor composition for one actor system.

use core::hash::Hash;
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use super::*;
    use behavior::User;
    use communication::{Config, mailbox_channel};

    use crate::address::MailAddr;
    use crate::local::endpoint::ActorRef;
    use crate::local::ingress::Admission;
    use crate::observe;

    struct RootProtocol;

    impl Protocol for RootProtocol {
        type Addr = MailAddr;
        type Msg = ();
    }

    #[test]
    fn shared_host_retains_the_owned_registration_scope() {
        let owned_space = Arc::new(ActorSpace::<RootProtocol>::new());
        let shared_space = Arc::clone(&owned_space);
        let owned_scope = owned_space.registration_scope_id();

        assert_eq!(
            Hosts::<RootProtocol>::space(&owned_space).registration_scope_id(),
            owned_scope
        );
        assert_eq!(
            Hosts::<RootProtocol>::space(&shared_space).registration_scope_id(),
            owned_scope
        );
    }

    #[test]
    fn hosted_space_retains_the_owned_registration_scope() {
        let owned_space = ActorSpace::<RootProtocol>::new();
        let owned_scope = owned_space.registration_scope_id();
        let hosted_space = owned_space;

        assert_eq!(
            Hosts::<RootProtocol>::space(&hosted_space).registration_scope_id(),
            owned_scope
        );
    }

    #[test]
    fn logical_host_resolves_an_exact_claimed_endpoint() {
        let address = MailAddr(41);
        let (control, mailbox_owner, mailbox, receiver) =
            mailbox_channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(1));
        let admission = Arc::new(Admission::new(mailbox_owner));
        let (publisher, observation) = observe::pair();
        let actor = ActorRef::<RootProtocol>::external(
            address,
            mailbox,
            Arc::downgrade(&admission),
            observation,
        );
        let owned_space = ActorSpace::<RootProtocol>::new();
        let claim = owned_space
            .try_claim(address, actor)
            .expect("the address is vacant");
        let hosted_space = owned_space;

        let resolved = hosted_space
            .space()
            .resolve(&address)
            .map(|actor| actor.as_ref().clone())
            .expect("the live claim is resolvable");
        assert_eq!(resolved.address(), address);
        assert!(hosted_space.space().resolve(&MailAddr(42)).is_none());
        drop((claim, control, publisher, admission, receiver));
    }
}
