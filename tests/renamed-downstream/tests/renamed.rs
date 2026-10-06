//! Downstream expansion evidence for Bombay's renamed public dependency.

use core::marker::PhantomData;

use actor_runtime::behavior::{Actions, BehaviorActed, Never, NoBirths, NoSends};
use actor_runtime::prelude::{ActorRetirement, Protocol, RootOrigin};
use actor_runtime::{Hosts, ProjectTerminal};

// These caller-owned names must not capture paths emitted by the macros.
mod bombay {
    pub struct ActorSpace;
}

struct Collision;

type NamedSpace<P> = actor_runtime::ActorSpace<P>;

struct NamedActor<T> {
    value: u64,
    marker: PhantomData<T>,
}

#[actor_runtime::actor]
impl<T> NamedActor<T>
where
    T: Send + Sync + 'static,
{
    fn receive(&mut self, amount: u8) -> BehaviorActed<Self> {
        self.value += u64::from(amount);
        Ok(Actions::cont())
    }
}

#[derive(actor_runtime::ActorSpaces)]
struct NamedSpaces<P>
where
    P: Protocol<Addr = actor_runtime::MailAddr>,
{
    #[actor_space(P)]
    actors: NamedSpace<P>,
    collision: Collision,
}

#[allow(
    dead_code,
    reason = "the fixture proves the generated projection contract by type"
)]
#[derive(actor_runtime::TerminalProjection)]
enum NamedTerminal {
    Root {
        origin: RootOrigin<NamedActor<u8>>,
        terminal: ActorRetirement<NamedActor<u8>, Self, ()>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_runtime::behavior::{User, delegate_transition, initialize};

    fn accepts_projection<
        T: ProjectTerminal<
                RootOrigin<NamedActor<u8>>,
                ActorRetirement<NamedActor<u8>, NamedTerminal, ()>,
            >,
    >() {
    }

    #[test]
    fn renamed_macros_keep_their_owner_paths_and_typed_products() {
        let mut actor = NamedActor::<u8> {
            value: 2,
            marker: PhantomData,
        };
        let initialization = initialize(&mut actor).expect("initialization succeeds");
        let expected: Actions<actor_runtime::MailAddr, Never, NoSends, NoBirths> = Actions::cont();
        assert_eq!(initialization, expected);

        let transition = delegate_transition(&mut actor, User::new(actor_runtime::MailAddr(3), 4))
            .expect("transition succeeds");
        assert_eq!(transition, expected);
        assert_eq!(actor.value, 6);

        let spaces = NamedSpaces::<NamedActor<u8>> {
            actors: actor_runtime::ActorSpace::new(),
            collision: Collision,
        };
        let hosted = <NamedSpaces<NamedActor<u8>> as Hosts<NamedActor<u8>>>::space(&spaces);
        assert!(core::ptr::eq(hosted, core::ptr::from_ref(&spaces.actors)));
        let _: Option<bombay::ActorSpace> = None;
        assert!(matches!(spaces.collision, Collision));

        accepts_projection::<NamedTerminal>();
    }
}
