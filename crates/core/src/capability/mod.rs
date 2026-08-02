//! The distilled actor surface (ADR-0026, stage 1 — card #278): ONE
//! [`Actor`] trait, capabilities as plugged types on [`Actor::Caps`],
//! access through the one typed window [`Ctx`].
//!
//! Stage-1 shape: the new surface runs ON the existing runtime through the
//! internal [`Shell`] adapter (which implements the shipped
//! [`actor::Actor`](crate::actor::Actor) and drives the untouched run
//! loop). The [`Handle`] alias names the seam; it collapses when a later
//! stage teaches the loop to drive this trait natively.
//!
//! Compile-time laws (ADR-0026 constraints):
//! - capability access is gated by [`Provide`] bounds — a capability the
//!   set does not declare is a **compile error**, and no runtime-checked
//!   accessor exists (constraint 1);
//! - the seam is **open**: any crate defines a capability type and a
//!   cap-set struct providing it (constraint 2; encoding per the
//!   ADR-0026 Addendum — `#[derive(bombay_macros::Provide)]` emits the
//!   per-field impls, and duplicate capability fields are rejected by
//!   coherence, E0119).
//!
//! Split one file per unit (card #292): the surface below is the module's
//! one door — every unit is a private `mod` re-exported here.

mod actor;
mod ctx;
mod deadline;
mod phased;
mod shell;
mod spawn;
mod stashing;
mod supervising;
mod verdict;
mod watching;

pub use actor::{Actor, CapSet, Provide};
pub use ctx::Ctx;
pub use deadline::{
    ByPhase, ByState, DeadlineCx, DeadlineHook, DeadlinePolicy, Deadlined, NoTimeout, PhaseView,
};
pub use phased::{
    Admission, Admitted, Bounded, DeferOutcome, DeferRouted, DeferSeat, DeferVerdict, NoDefer,
    NoStash, PhaseBuffer, PhasePolicy, Phased, StashOf, TokenOf,
};
pub use shell::{Handle, LinkedRun, PlainRun, SelectRunner, Shell, SupervisedRun};
pub use spawn::{RunKind, spawn, spawn_with};
pub use stashing::{Replay, StashPolicy, Stashing};
pub use supervising::{HasSupervising, OneForAll, OneForOne, RestForOne, Strategy, Supervising};
pub use verdict::{Deferred, Disposition, Flow, Never, Normal, Overflow, Step};
pub use watching::{HasWatching, OtpPropagation, WatchPolicy, Watching};

/// The typed overflow handback of the [`Stashing`] capability — re-exported
/// so a stashing actor imports its whole surface from [`capability`](crate::capability).
pub use crate::stash::StashFull;

/// The deadline plane's instant type (tokio's, paused-clock testable) —
/// re-exported so a [`DeadlinePolicy`] and the derive-emitted
/// [`DeadlineHook`] impls name it without a direct tokio dependency.
pub use tokio::time::Instant as DeadlineInstant;

#[cfg(test)]
pub(crate) mod fixtures {
    //! Shared hand-written cap sets (what the derive emits, spelled out —
    //! the derive's own emission is unit-tested in `bombay_macros`), used
    //! by several units' tests.

    use core::convert::Infallible;

    use futures::stream::AbortHandle;
    use tokio_util::sync::CancellationToken;

    use super::{
        Actor, Admission, Admitted, CapSet, Ctx, DeadlineHook, Disposition, HasSupervising,
        HasWatching, NoDefer, NoTimeout, OneForAll, OtpPropagation, PhasePolicy, Phased, PlainRun,
        Provide, Replay, SelectRunner, Shell, SupervisedRun, Supervising, WatchPolicy, Watching,
    };
    use crate::{
        actor::{ActorRef, Flow, WeakActorRef},
        mailbox::{ActorId, Capacity, Mailbox, Mailboxed},
        message::Msg,
    };

    /// The capability-less floor actor: `Caps = ()`, an uninhabited menu.
    pub(crate) struct Nameless;

    #[derive(Debug)]
    pub(crate) enum NoMsg {}
    impl Msg for NoMsg {}

    impl Mailboxed for Nameless {
        type Msg = NoMsg;
    }

    impl Actor for Nameless {
        type Msg = NoMsg;
        type Args = ();
        type Error = core::convert::Infallible;
        type Caps = ();

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }

        async fn handle(&mut self, msg: NoMsg, _: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
            match msg {}
        }
    }

    /// A drain-window weak ref (its strong parent is dropped at
    /// return): exactly the ref shape a deadline hook must tolerate.
    pub(crate) fn dead_weak<A: crate::actor::Actor>() -> WeakActorRef<A> {
        let cap = Capacity::try_from(1usize).expect("valid test capacity");
        let (tx, _rx) = Mailbox::<A>::bounded(cap, ActorId::from_raw_for_test(9));
        let (abort, _reg) = AbortHandle::new_pair();
        let strong = ActorRef::new(
            ActorId::from_raw_for_test(9),
            tx,
            CancellationToken::new(),
            abort,
            None,
        );
        strong.downgrade()
    }

    /// The shared no-payload test message (the `Rec`/`Sup` menu).
    #[derive(Debug)]
    pub(crate) struct RecMsg;
    impl Msg for RecMsg {}

    /// A supervising actor whose cap set names `OneForAll` — the
    /// strategy-read probe.
    pub(crate) struct Sup;

    impl Mailboxed for Sup {
        type Msg = RecMsg;
    }

    pub(crate) struct SupCaps {
        watching: Watching<OtpPropagation>,
        supervising: Supervising<OneForAll>,
    }

    impl CapSet<Sup> for SupCaps {
        fn build((): &()) -> Self {
            Self {
                watching: Watching::new(),
                supervising: Supervising::new(),
            }
        }
    }
    impl Provide<Watching<OtpPropagation>> for SupCaps {
        fn provide(&mut self) -> &mut Watching<OtpPropagation> {
            &mut self.watching
        }
    }
    impl Provide<Supervising<OneForAll>> for SupCaps {
        fn provide(&mut self) -> &mut Supervising<OneForAll> {
            &mut self.supervising
        }
    }
    impl<M> Replay<M> for SupCaps {
        fn next_replay(&mut self) -> Option<M> {
            None
        }
    }
    impl<A: Actor> Admission<A> for SupCaps {
        async fn admit(&mut self, _: &mut A, msg: A::Msg) -> Result<Admitted<A::Msg>, A::Error> {
            Ok(Admitted::Deliver(msg))
        }
        fn commit(&mut self) {}
    }
    impl<A: Actor> DeadlineHook<A> for SupCaps {
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
    impl<A: Actor> HasWatching<A> for SupCaps
    where
        OtpPropagation: WatchPolicy<A>,
    {
        type Policy = OtpPropagation;
    }
    impl<A: Actor> HasSupervising<A> for SupCaps
    where
        OtpPropagation: WatchPolicy<A>,
    {
        type Strat = OneForAll;
    }
    impl<A: Actor> SelectRunner<A> for SupCaps {
        type Runner = SupervisedRun;
    }

    impl Actor for Sup {
        type Msg = RecMsg;
        type Args = ();
        type Error = Infallible;
        type Caps = SupCaps;

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
            Ok(Self)
        }

        async fn handle(&mut self, _: RecMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
            Ok(Flow::Continue)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Ph {
        A,
        B,
    }

    #[derive(Debug)]
    pub(crate) enum MMsg {
        GotoBThenFail,
        GotoBThenOk,
    }
    impl Msg for MMsg {}

    pub(crate) struct M;
    impl Mailboxed for M {
        type Msg = MMsg;
    }

    pub(crate) struct MPolicy;
    impl PhasePolicy for MPolicy {
        type Actor = M;
        type Phase = Ph;
        type Deferral = NoDefer;
        type Timeout = NoTimeout;
        fn initial((): &()) -> Ph {
            Ph::A
        }
        fn gate(_: Ph, _: &MMsg) -> Disposition {
            Disposition::Deliver
        }
    }

    pub(crate) struct MCaps {
        pub(crate) phased: Phased<MPolicy>,
    }
    impl CapSet<M> for MCaps {
        fn build(args: &()) -> Self {
            Self {
                phased: Phased::build(args),
            }
        }
    }
    impl Provide<Phased<MPolicy>> for MCaps {
        fn provide(&mut self) -> &mut Phased<MPolicy> {
            &mut self.phased
        }
    }
    impl Replay<MMsg> for MCaps {
        fn next_replay(&mut self) -> Option<MMsg> {
            Replay::next_replay(&mut self.phased)
        }
    }
    impl DeadlineHook<M> for MCaps {
        fn next_deadline(&self, actor: &M) -> Option<tokio::time::Instant> {
            DeadlineHook::next_deadline(&self.phased, actor)
        }
        async fn on_deadline(
            &mut self,
            actor: &mut M,
            actor_ref: WeakActorRef<Shell<M>>,
        ) -> Result<Flow, &'static str> {
            DeadlineHook::on_deadline(&mut self.phased, actor, actor_ref).await
        }
    }
    impl Admission<M> for MCaps {
        async fn admit(
            &mut self,
            actor: &mut M,
            msg: MMsg,
        ) -> Result<Admitted<MMsg>, &'static str> {
            Admission::admit(&mut self.phased, actor, msg).await
        }
        fn commit(&mut self) {
            Admission::commit(&mut self.phased);
        }
    }
    impl<A: Actor> SelectRunner<A> for MCaps {
        type Runner = PlainRun;
    }

    impl Actor for M {
        type Msg = MMsg;
        type Args = ();
        type Error = &'static str;
        type Caps = MCaps;

        async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, &'static str> {
            Ok(Self)
        }

        async fn handle(&mut self, msg: MMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, &'static str> {
            cx.cap::<Phased<MPolicy>>().goto(Ph::B);
            match msg {
                MMsg::GotoBThenFail => Err("bang"),
                MMsg::GotoBThenOk => Ok(Flow::Continue),
            }
        }
    }
}
