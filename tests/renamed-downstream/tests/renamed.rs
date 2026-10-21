//! Downstream expansion evidence for Bombay's renamed public dependency.

use core::marker::PhantomData;

use actor_runtime::actors::ActorExt;
use actor_runtime::behavior::{
    Actions, BehaviorActed, BehaviorBase, CreateChild, CreationSequence, Creations,
    EstablishedDelivery, EstablishedRecipient, Never, NoBirths, NoSends, Step, Stopped,
};
use actor_runtime::prelude::{ActorRetirement, Protocol, RootOrigin};
use actor_runtime::{ActorNotificationReceipts, Hosts};

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

struct Emitter;

struct TallyReplies;

impl Protocol for TallyReplies {
    type Addr = actor_runtime::MailAddr;
    type Msg = u64;
}

enum TallyCommand {
    Record(u8),
    Read(EstablishedRecipient<TallyReplies>),
}

struct Tally {
    value: u64,
}

#[actor_runtime::actor(sends = { receipts: Vec<EstablishedDelivery<TallyReplies>> })]
impl Tally {
    fn receive(&mut self, command: TallyCommand) -> BehaviorActed<Self> {
        match command {
            TallyCommand::Record(amount) => {
                self.value += u64::from(amount);
                Ok(Actions::cont())
            }
            TallyCommand::Read(reply_to) => {
                Ok(Actions::stop().send_receipts(EstablishedDelivery::new(reply_to, self.value)))
            }
        }
    }
}

#[actor_runtime::actor(
    message = Never,
    births = { worker: actor_runtime::prelude::StopOnShutdown<NamedActor<u8>> },
    creation_settlements = retain_for_retirement,
)]
impl Emitter {
    #[expect(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the actor initialization contract requires self and its typed success/error carrier"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let creation = sequence
            .issue()
            .expect("the first child correlation exists");
        Ok(Actions::new(
            NoSends,
            Creations::one(CreateChild::birth(
                creation,
                NamedActor::<u8> {
                    value: 19,
                    marker: PhantomData,
                }
                .stop_on_shutdown(),
            )),
            Step::Stop(Stopped),
        ))
    }
}

actor_runtime::topology! {
    named_tree {
        root { Worker => worker; }
        worker {}
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
        T: From<(
            RootOrigin<NamedActor<u8>>,
            ActorRetirement<NamedActor<u8>, NamedTerminal, ()>,
        )>,
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

    #[tokio::test(flavor = "current_thread")]
    async fn renamed_topology_preserves_native_child_custody() {
        let outcome =
            actor_runtime::Application::with_topology(Emitter, named_tree::Topology::new())
                .run()
                .await
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("the renamed application must retain complete retirement");
                })
                .into_parts();
        let (
            actor_runtime::WorkOutcome::NotInvoked {
                work: None,
                cleanup: Ok(()),
                ..
            },
            _root_report,
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = outcome
        else {
            panic!("initialization stops before startup grant and returns complete child custody");
        };
        assert_eq!(origin.address(), actor_runtime::MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody,
            completion,
            ..
        } = retirement
        else {
            panic!("the emitter stops after creating its child");
        };
        assert_eq!(completion, actor_runtime::prelude::Completion::Stopped);
        assert!(custody.child_failures.is_empty());
        assert_eq!(custody.descendants.len(), 1);
        let child = custody
            .descendants
            .into_iter()
            .next()
            .expect("one child remains owned");
        assert!(matches!(
            child.origin.creation_kind(),
            actor_runtime::behavior::CreationKind::Birth
        ));
        let ActorRetirement::OwnerCancelled { custody, .. } = *child.retirement else {
            panic!("the child is retired by its actual parent");
        };
        assert_eq!(custody.behavior.base().value, 19);
        assert_eq!(custody.descendants.len(), 0);
        assert_eq!(custody.child_failures.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn renamed_explicit_clients_keep_inferred_replies_and_real_sender_authority() {
        let outcome = actor_runtime::Application::new(Tally { value: 5 })
            .run_with(|application| async move {
                let interface = application.interface(application.root().established_recipient());
                let sender = application
                    .sender_to(interface.api().clone())
                    .unwrap_or_else(|_| panic!("the selected real sender is established"));
                let mut client = interface
                    .client_to(interface.api().clone())
                    .unwrap_or_else(|_| panic!("the selected real reply endpoint is established"));
                assert_ne!(client.address(), sender.address());
                sender
                    .send(TallyCommand::Record(8))
                    .await
                    .expect("the selected tally admits its actual command");
                client
                    .send(TallyCommand::Read(client.recipient()))
                    .await
                    .expect("the selected tally admits its inferred reply capability");
                let reply = client
                    .receive()
                    .await
                    .expect("the tally replies before stopping");
                assert_eq!(reply.message, 13);
                assert_eq!(reply.from, application.root().address());
                let termination = application.termination().await;
                assert_eq!(termination, Ok(actor_runtime::prelude::Exit::Normal));
            })
            .await
            .unwrap_or_else(|_| panic!("the actual caller owns the entered host"))
            .into_parts();
        let (outcome, _root_report, native, notifications, ()) = outcome;
        let ((), cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the caller completes independently of cleanup"));
        cleanup.expect("the application's cleanup completes");
        let Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }) = notifications
        else {
            panic!("both original tally notifications succeed");
        };
        let (_, retirement) = native.expect("the exact original tally is acquired");
        assert_eq!(
            retirement.completion(),
            Some(&actor_runtime::prelude::Completion::Stopped)
        );
        let custody = retirement
            .custody()
            .expect("all original actor custody remains");
        assert_eq!(custody.behavior.value, 13);
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
    }
}
