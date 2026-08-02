//! The stage-1 seam: [`Shell`] adapts a capability actor onto the shipped
//! runtime loop, [`Handle`] names its ref type, and [`SelectRunner`] plus
//! the loop-shape markers pick the loop at compile time (spike-280).

use core::{future::Future, ops::ControlFlow};

use tokio::time::Instant;

use crate::{
    actor::{ActorRef, Flow, LinkReact, SupervisedReact, WeakActorRef, sealed},
    error::{ActorStopReason, PanicError},
    mailbox::{ActorId, Mailboxed},
    restart::SupervisionStrategy,
};

use super::{
    Actor, Admission, Admitted, CapSet, Ctx, DeadlineHook, HasSupervising, HasWatching, Replay,
    Strategy, WatchPolicy,
};

/// Names the run-loop shape for a cap set (ADR-0026 stage 3 — the
/// compile-time loop selection, spike-280).
///
/// Derive-emitted alongside [`Replay`]: a `Supervising` field selects
/// [`SupervisedRun`], else a `Watching` field selects [`LinkedRun`], else
/// [`PlainRun`]; `()` selects [`PlainRun`] via the core impl below.
///
/// The associated type is deliberately UNBOUNDED here: the "this shape is
/// actually runnable for this actor" obligation
/// (`Runner: RunKind<A>`) sits on the ONE [`spawn`](super::spawn), discharging at the
/// concrete actor — which is what lets the derive-emitted impl stay
/// generic over `A`.
pub trait SelectRunner<A: Actor> {
    /// The selected loop-shape marker.
    type Runner;
}

impl<A: Actor> SelectRunner<A> for () {
    type Runner = PlainRun;
}

/// Loop-shape marker: the one-arm plain message loop.
pub struct PlainRun;

/// Loop-shape marker: the two-arm linked loop (mailbox + link channel).
pub struct LinkedRun;

/// Loop-shape marker: the three-arm supervised loop (mailbox + link
/// channel + restart-backoff queue).
pub struct SupervisedRun;

/// The stage-1 seam: a `capability` actor's runtime handle is an
/// [`ActorRef`] over its [`Shell`]. The alias collapses when the loop
/// drives [`Actor`] natively in a later ADR-0026 stage.
pub type Handle<A> = ActorRef<Shell<A>>;

/// Internal adapter wrapping a [`capability::Actor`](Actor) into the shipped
/// runtime trait.
///
/// Holds user state + the built capability set; inherits the loop's
/// poisoning, drain-window, and stop semantics unchanged. Public only
/// because [`Handle`] names it; its fields are not.
pub struct Shell<A: Actor> {
    user: A,
    caps: A::Caps,
}

impl<A: Actor> Mailboxed for Shell<A> {
    type Msg = A::Msg;
}

impl<A: Actor> crate::actor::Actor for Shell<A> {
    type Args = A::Args;
    type Error = A::Error;

    fn name() -> &'static str {
        A::name()
    }

    async fn on_start(args: A::Args, actor_ref: ActorRef<Self>) -> Result<Self, A::Error> {
        let mut caps = A::Caps::build(&args);
        let user = A::init(
            args,
            Ctx {
                caps: &mut caps,
                self_ref: &actor_ref,
            },
        )
        .await?;
        // An init-time `Phased::goto` commits here — before any message —
        // rather than dangling until the first handled step.
        caps.commit();
        Ok(Self { user, caps })
    }

    /// One delivered message, then in-step replay (ADR-0022): run the
    /// `step` (admission → user handler → transition
    /// commit), then drain the cap set's [`Replay`] queue in the same step
    /// — ahead of the whole mailbox backlog, in stash-arrival order, under
    /// this step's strong `actor_ref` (no upgrade, no drain-window
    /// hazard). Replayed messages re-enter admission, so a [`Phased`](super::Phased) gate
    /// re-classifies them in the CURRENT phase (a re-deferred message goes
    /// to `held`, never back into the draining batch — the snapshot
    /// bound). A replayed handler's `Err`/panic/`Flow::Stop` routes
    /// exactly as a delivered message's would; `Flow::Stop` abandons the
    /// rest of the batch. For a plain actor (`Caps = ()`) admission is a
    /// pass-through and the drain loop never enters.
    async fn handle(&mut self, msg: A::Msg, actor_ref: ActorRef<Self>) -> Result<Flow, A::Error> {
        if self.step(msg, &actor_ref).await? == Flow::Stop {
            return Ok(Flow::Stop);
        }
        while let Some(m) = self.caps.next_replay() {
            if self.step(m, &actor_ref).await? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    /// The loop's deadline arm reads the cap set's declarative slot — the
    /// runtime bridge of the ADR-0025 plane onto the [`Deadlined`](super::Deadlined)
    /// capability (stage 4). A plain set (`Caps = ()`) reports `None` and
    /// the arm stays disabled.
    fn next_deadline(&self) -> Option<Instant> {
        self.caps.next_deadline(&self.user)
    }

    /// Expiry rides the cap set's hook, then the same in-step replay drain
    /// as [`handle`](Actor::handle) — a phase timeout may release a stash
    /// batch that must replay ahead of the backlog, re-gated. The drain
    /// needs a strong ref for the replayed handlers' `Ctx`; a deadline
    /// fire has no message to mint one from, so in the drain window
    /// (`upgrade` fails) the released batch waits for the next delivered
    /// step — and dies with the incarnation if none comes (ADR-0022 D6).
    async fn on_deadline(&mut self, actor_ref: WeakActorRef<Self>) -> Result<Flow, A::Error> {
        if self
            .caps
            .on_deadline(&mut self.user, actor_ref.clone())
            .await?
            == Flow::Stop
        {
            return Ok(Flow::Stop);
        }
        let Some(strong) = actor_ref.upgrade() else {
            return Ok(Flow::Continue);
        };
        while let Some(m) = self.caps.next_replay() {
            if self.step(m, &strong).await? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    async fn on_panic(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> ActorStopReason {
        A::on_panic(&mut self.user, actor_ref, err).await
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), A::Error> {
        A::on_stop(&mut self.user, actor_ref, reason).await
    }
}

impl<A: Actor> Shell<A> {
    /// One handler step under admission (stage 4): the cap set classifies
    /// the message first — a [`Phased`] gate defers/ignores/sheds without
    /// the handler ever seeing it — then a delivered message runs the user
    /// handler, and a pending [`Phased::goto`] commits ONLY after its `Ok`
    /// (D3: an `Err`/panic never observes a half-switched phase).
    async fn step(&mut self, msg: A::Msg, actor_ref: &ActorRef<Self>) -> Result<Flow, A::Error> {
        match self.caps.admit(&mut self.user, msg).await? {
            Admitted::Absorbed(flow) => Ok(flow),
            Admitted::Deliver(m) => {
                let flow = A::handle(
                    &mut self.user,
                    m,
                    Ctx {
                        caps: &mut self.caps,
                        self_ref: actor_ref,
                    },
                )
                .await?;
                self.caps.commit();
                Ok(flow)
            }
        }
    }
}

impl<A: Actor> sealed::Sealed for Shell<A> {}

/// The stage-3 analogue of stage 2's derive-emitted [`Replay`]: the
/// `Shell` is link-reactive exactly when the cap set declares
/// [`Watching`](super::Watching), and the reaction IS the declared policy, reached through
/// the [`HasWatching::Policy`] associated type with `&mut user` access —
/// no free policy parameter anywhere (spike-280, O2).
impl<A: Actor> LinkReact for Shell<A>
where
    A::Caps: HasWatching<A>,
{
    fn on_link_died(
        &mut self,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, A::Error>> + Send {
        <<A::Caps as HasWatching<A>>::Policy as WatchPolicy<A>>::on_link_died(
            &mut self.user,
            id,
            reason,
            linked,
        )
    }
}

impl<A: Actor> SupervisedReact for Shell<A>
where
    A::Caps: HasSupervising<A>,
{
    fn strategy() -> SupervisionStrategy {
        <A::Caps as HasSupervising<A>>::Strat::STRATEGY
    }
}

#[cfg(test)]
mod tests {
    use core::any::type_name;
    use core::convert::Infallible;
    use core::ops::ControlFlow;
    use core::time::Duration;

    use futures::stream::AbortHandle;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use super::super::fixtures::{M, MCaps, MMsg, Nameless, Ph, RecMsg, Sup, dead_weak};
    use super::super::{
        Actor, Admission, Admitted, ByState, CapSet, Ctx, DeadlineHook, DeadlinePolicy, Deadlined,
        HasWatching, LinkedRun, Never, PlainRun, Provide, Replay, SelectRunner, Shell, Step,
        WatchPolicy, Watching,
    };
    use crate::{
        actor::{Actor as RuntimeActor, ActorRef, Flow, LinkReact, SupervisedReact, WeakActorRef},
        error::ActorStopReason,
        mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
        message::Msg,
        restart::SupervisionStrategy,
    };

    /// A recording watcher: its policy pushes every notice into the
    /// actor's own state — the shape the ported equivalence suites'
    /// recording hooks need (`&mut A` access from a policy).
    struct Rec {
        seen: Vec<(ActorId, bool)>,
    }

    impl Mailboxed for Rec {
        type Msg = RecMsg;
    }

    struct RecPolicy;

    impl WatchPolicy<Rec> for RecPolicy {
        async fn on_link_died(
            actor: &mut Rec,
            id: ActorId,
            _reason: ActorStopReason,
            linked: bool,
        ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
            actor.seen.push((id, linked));
            Ok(ControlFlow::Continue(()))
        }
    }

    /// Hand-written cap set (what the derive emits, spelled out — the
    /// derive's own emission is unit-tested in `bombay_macros`).
    struct RecCaps {
        watching: Watching<RecPolicy>,
    }

    impl CapSet<Rec> for RecCaps {
        fn build((): &()) -> Self {
            Self {
                watching: Watching::new(),
            }
        }
    }
    impl Provide<Watching<RecPolicy>> for RecCaps {
        fn provide(&mut self) -> &mut Watching<RecPolicy> {
            &mut self.watching
        }
    }
    impl<M> Replay<M> for RecCaps {
        fn next_replay(&mut self) -> Option<M> {
            None
        }
    }
    impl<A: Actor> Admission<A> for RecCaps {
        async fn admit(&mut self, _: &mut A, msg: A::Msg) -> Result<Admitted<A::Msg>, A::Error> {
            Ok(Admitted::Deliver(msg))
        }
        fn commit(&mut self) {}
    }
    impl<A: Actor> DeadlineHook<A> for RecCaps {
        fn next_deadline(&self, _: &A) -> Option<tokio::time::Instant> {
            None
        }
        async fn on_deadline(
            &mut self,
            _: &mut A,
            _: crate::actor::WeakActorRef<Shell<A>>,
        ) -> Result<Flow, A::Error> {
            Ok(Flow::Continue)
        }
    }
    impl<A: Actor> HasWatching<A> for RecCaps
    where
        RecPolicy: WatchPolicy<A>,
    {
        type Policy = RecPolicy;
    }
    impl<A: Actor> SelectRunner<A> for RecCaps {
        type Runner = LinkedRun;
    }

    impl Actor for Rec {
        type Msg = RecMsg;
        type Args = ();
        type Error = Infallible;
        type Caps = RecCaps;

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
            Ok(Self { seen: Vec::new() })
        }

        async fn handle(&mut self, _: RecMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
            Ok(Flow::Continue)
        }
    }

    /// An actor whose deadline is a pure function of its own state —
    /// the quinn `poll_timeout` shape the policy reads.
    struct Idler {
        due: Option<Instant>,
        fires: u32,
    }

    #[derive(Debug)]
    struct IdleMsg;
    impl Msg for IdleMsg {}
    impl Mailboxed for Idler {
        type Msg = IdleMsg;
    }

    struct IdlePolicy;

    impl DeadlinePolicy<ByState<Idler>> for IdlePolicy {
        fn build((): &()) -> Self {
            Self
        }
        fn next_deadline(&self, actor: &Idler, (): ()) -> Option<Instant> {
            actor.due
        }
        async fn on_deadline(
            &self,
            actor: &mut Idler,
            (): (),
            _: WeakActorRef<Shell<Idler>>,
        ) -> Result<Step<Never>, Infallible> {
            actor.fires = actor.fires.saturating_add(1);
            actor.due = None;
            Ok(Step::Stop)
        }
    }

    /// Hand-written cap set (what the derive emits, spelled out).
    struct IdleCaps {
        deadlined: Deadlined<IdlePolicy>,
    }

    impl CapSet<Idler> for IdleCaps {
        fn build(args: &()) -> Self {
            Self {
                deadlined: Deadlined::build(args),
            }
        }
    }
    impl Provide<Deadlined<IdlePolicy>> for IdleCaps {
        fn provide(&mut self) -> &mut Deadlined<IdlePolicy> {
            &mut self.deadlined
        }
    }
    impl<M> Replay<M> for IdleCaps {
        fn next_replay(&mut self) -> Option<M> {
            None
        }
    }
    impl<A: Actor> Admission<A> for IdleCaps {
        async fn admit(&mut self, _: &mut A, msg: A::Msg) -> Result<Admitted<A::Msg>, A::Error> {
            Ok(Admitted::Deliver(msg))
        }
        fn commit(&mut self) {}
    }
    impl<A: Actor> DeadlineHook<A> for IdleCaps
    where
        IdlePolicy: DeadlinePolicy<ByState<A>>,
    {
        fn next_deadline(&self, actor: &A) -> Option<Instant> {
            DeadlineHook::next_deadline(&self.deadlined, actor)
        }
        async fn on_deadline(
            &mut self,
            actor: &mut A,
            actor_ref: WeakActorRef<Shell<A>>,
        ) -> Result<Flow, A::Error> {
            DeadlineHook::on_deadline(&mut self.deadlined, actor, actor_ref).await
        }
    }
    impl<A: Actor> SelectRunner<A> for IdleCaps {
        type Runner = PlainRun;
    }

    impl Actor for Idler {
        type Msg = IdleMsg;
        type Args = ();
        type Error = Infallible;
        type Caps = IdleCaps;

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
            Ok(Self {
                due: None,
                fires: 0,
            })
        }

        async fn handle(&mut self, _: IdleMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
            Ok(Flow::Continue)
        }
    }

    /// A strong ref plus the receiver that keeps its channel open for
    /// the test's life (the `actor_ref.rs` `build_ref_with_rx` shape).
    fn strong_ref() -> (
        ActorRef<Shell<M>>,
        crate::mailbox::MailboxReceiver<Shell<M>>,
    ) {
        let cap = Capacity::try_from(1usize).expect("valid test capacity");
        let (tx, rx) = Mailbox::<Shell<M>>::bounded(cap, ActorId::from_raw_for_test(11));
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(
            ActorId::from_raw_for_test(11),
            tx,
            CancellationToken::new(),
            abort,
            None,
        );
        (actor_ref, rx)
    }

    /// Kills the `Shell::name` mutants: the wrapper reports the USER
    /// type's name, not its own.
    #[test]
    fn shell_forwards_the_user_type_name() {
        assert_eq!(
            <Shell<Nameless> as crate::actor::Actor>::name(),
            type_name::<Nameless>()
        );
    }

    /// `Shell`'s sealed `LinkReact` impl dispatches the loop's death
    /// notice INTO the cap set's declared policy with `&mut user`
    /// access — the stage-3 analogue of the stage-2 replay-drain test.
    #[tokio::test]
    async fn shell_dispatches_on_link_died_to_the_declared_policy() {
        let mut shell = Shell {
            user: Rec { seen: Vec::new() },
            caps: RecCaps::build(&()),
        };
        let id = ActorId::from_raw_for_test(7);

        let out =
            <Shell<Rec> as LinkReact>::on_link_died(&mut shell, id, ActorStopReason::Killed, true)
                .await
                .expect("recording policy is infallible");

        assert!(
            matches!(out, ControlFlow::Continue(())),
            "the recording policy continues, got {out:?}",
        );
        assert_eq!(
            shell.user.seen,
            vec![(id, true)],
            "the policy observed the notice through &mut user state",
        );
    }

    /// `Shell`'s sealed `SupervisedReact` impl reads the strategy off
    /// the `Supervising` cap's TYPE — the re-pointed `kind.rs`
    /// strategy read.
    #[test]
    fn shell_strategy_reads_the_supervising_caps_type() {
        assert_eq!(
            <Shell<Sup> as SupervisedReact>::strategy(),
            SupervisionStrategy::OneForAll,
            "the strategy is the cap set's named type, not a default",
        );
    }

    /// The plain-actor floor: `()` selects the one-arm loop shape.
    #[test]
    fn unit_capset_selects_the_plain_runner() {
        assert_eq!(
            type_name::<<() as SelectRunner<Nameless>>::Runner>(),
            type_name::<PlainRun>(),
            "a capability-less set runs the plain message loop",
        );
    }

    /// `Shell`'s runtime `next_deadline` forwards to the cap set's
    /// declared policy — a pure read of user state through the bridge.
    #[tokio::test]
    async fn shell_forwards_next_deadline_to_the_declared_policy() {
        let due = Instant::now() + Duration::from_secs(5);
        let mut shell = Shell {
            user: Idler {
                due: Some(due),
                fires: 0,
            },
            caps: IdleCaps::build(&()),
        };
        assert_eq!(
            RuntimeActor::next_deadline(&shell),
            Some(due),
            "the bridge reads the policy's pure function of user state",
        );
        shell.user.due = None;
        assert_eq!(
            RuntimeActor::next_deadline(&shell),
            None,
            "no declared deadline disables the arm",
        );
    }

    /// `Shell`'s runtime `on_deadline` dispatches expiry INTO the
    /// declared policy with `&mut user` access, and the policy's
    /// `Flow` decision rides back out — the stage-4 analogue of the
    /// stage-3 link-dispatch test.
    #[tokio::test]
    async fn shell_dispatches_on_deadline_to_the_declared_policy() {
        let mut shell = Shell {
            user: Idler {
                due: Some(Instant::now()),
                fires: 0,
            },
            caps: IdleCaps::build(&()),
        };
        let flow = RuntimeActor::on_deadline(&mut shell, dead_weak())
            .await
            .expect("the recording policy is infallible");
        assert_eq!(flow, Flow::Stop, "the policy's Flow decision rides out");
        assert_eq!(
            shell.user.fires, 1,
            "the policy observed expiry through &mut user state",
        );
    }

    /// D3 commit-after-Ok, on the `Shell::step` code order itself: a
    /// handler that requests a transition and then FAILS never
    /// commits it (an unwind takes the same post-await path), while
    /// the same request followed by `Ok` does.
    #[tokio::test]
    async fn a_failing_handler_never_commits_its_pending_goto() {
        let mut shell = Shell {
            user: M,
            caps: MCaps::build(&()),
        };
        let (actor_ref, _rx) = strong_ref();

        let out =
            crate::actor::Actor::handle(&mut shell, MMsg::GotoBThenFail, actor_ref.clone()).await;
        assert_eq!(out, Err("bang"), "the controlled crash rides out");
        assert_eq!(
            shell.caps.phased.phase(),
            Ph::A,
            "a failed handler's goto is NEVER committed (D3)",
        );

        let out = crate::actor::Actor::handle(&mut shell, MMsg::GotoBThenOk, actor_ref).await;
        assert_eq!(out, Ok(Flow::Continue));
        assert_eq!(
            shell.caps.phased.phase(),
            Ph::B,
            "the same goto commits after Ok",
        );
    }
}
