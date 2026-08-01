//! Card #290 — declared seats on `PhasePolicy` and the ONE
//! context-generic `DeadlinePolicy<Cx>` (ADR-0028).
//!
//! Pins the new surface: a `NoDefer`/`NoTimeout` machine writes no stash
//! bound and no timeout reaction (and its stash TYPE is zero-sized — no
//! buffer exists); a `ByPhase` seat is a plugged strategy carrying the
//! deadline pair; a `ByState` seat is the same trait serving `Deadlined`,
//! speaking `Step<Never> ≅ Flow`. The machine laws themselves (gate trio,
//! D3/D4, overflow) stay pinned by `caps_phased.rs`; the plane by
//! `deadline_plane.rs`.

use core::{convert::Infallible, mem::size_of, time::Duration};

use tokio::time::{Instant, sleep, timeout};

use bombay::{
    actor::{Flow, WeakActorRef},
    caps::{
        Actor, ByPhase, ByState, CapSet, Ctx, DeadlinePolicy, Deadlined, Disposition, Never,
        NoDefer, NoTimeout, PhasePolicy, PhaseView, Phased, Shell, StashOf, Step, spawn,
    },
    error::ActorStopReason,
    test_support::terminate_bound,
};

async fn bounded<F: core::future::IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("await must resolve within the bound")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Served(u32),
    Refused(u32),
    TimedOutAt(Instant),
    Stopped(bool),
}

async fn collect_until_stopped(rx: &flume::Receiver<Ev>) -> Vec<Ev> {
    bounded(async {
        let mut got = Vec::new();
        loop {
            let ev = rx
                .recv_async()
                .await
                .expect("probe channel must not close before Stopped");
            let done = matches!(ev, Ev::Stopped(_));
            got.push(ev);
            if done {
                return got;
            }
        }
    })
    .await
}

// ------------------------------------------- NoDefer + ByPhase seat ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WPhase {
    Serving,
    Draining,
}

#[derive(Debug, Clone, bombay_macros::Msg)]
enum WMsg {
    Job(u32),
    /// Transition to Draining (arms its grace deadline).
    Drain,
    /// Declared noise while Draining — dropped before the handler.
    Gossip,
}

/// The machine's core declaration: phases + gate. Never defers (the gate
/// cannot even SPELL `Defer` — its verdict type's token is uninhabited),
/// and the deadline pair lives on the plugged [`Grace`] seat, not here.
struct WPolicy;

impl PhasePolicy for WPolicy {
    type Actor = W;
    type Phase = WPhase;
    type Deferral = NoDefer;
    type Timeout = Grace;

    fn initial(_: &WArgs) -> WPhase {
        WPhase::Serving
    }

    fn gate(phase: WPhase, msg: &WMsg) -> Disposition {
        match (phase, msg) {
            (WPhase::Draining, WMsg::Gossip) => Disposition::Ignore,
            _ => Disposition::Deliver,
        }
    }
}

/// The deadline seat as a PLUGGED STRATEGY (tower `retry::Policy` shape):
/// the pair — slot + reaction — in one trait, magnitudes built from Args.
struct Grace {
    grace: Duration,
}

impl DeadlinePolicy<ByPhase<WPolicy>> for Grace {
    fn build(args: &WArgs) -> Self {
        Self { grace: args.grace }
    }

    fn next_deadline(&self, _: &W, view: PhaseView<WPolicy>) -> Option<Instant> {
        match view.phase {
            WPhase::Serving => None,
            WPhase::Draining => view.entered_at.checked_add(self.grace),
        }
    }

    async fn on_deadline(
        &self,
        actor: &mut W,
        _: PhaseView<WPolicy>,
        _: WeakActorRef<Shell<W>>,
    ) -> Result<Step<WPhase>, Infallible> {
        let _ = actor.probe.send(Ev::TimedOutAt(Instant::now()));
        Ok(Step::Stop)
    }
}

struct WArgs {
    grace: Duration,
    probe: flume::Sender<Ev>,
}

struct W {
    probe: flume::Sender<Ev>,
}

#[derive(bombay_macros::Provide)]
struct WCaps {
    phased: Phased<WPolicy>,
}

impl CapSet<W> for WCaps {
    fn build(args: &WArgs) -> Self {
        Self {
            phased: Phased::build(args),
        }
    }
}

impl Actor for W {
    type Msg = WMsg;
    type Args = WArgs;
    type Error = Infallible;
    type Caps = WCaps;

    async fn init(args: WArgs, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { probe: args.probe })
    }

    async fn handle(&mut self, msg: WMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let phase = cx.cap::<Phased<WPolicy>>().phase();
        match (phase, msg) {
            (WPhase::Serving, WMsg::Job(n)) => {
                let _ = self.probe.send(Ev::Served(n));
            }
            // A draining worker refuses loudly — delivered by declaration,
            // never deferred (there is no stash to defer into).
            (WPhase::Draining, WMsg::Job(n)) => {
                let _ = self.probe.send(Ev::Refused(n));
            }
            (WPhase::Serving, WMsg::Drain) => {
                cx.cap::<Phased<WPolicy>>().goto(WPhase::Draining);
            }
            _ => {}
        }
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Infallible> {
        let _ = self.probe.send(Ev::Stopped(reason.is_normal()));
        Ok(())
    }
}

/// A `NoDefer` machine carries NO buffer: the stash type the seat
/// declares is zero-sized. (The old design carved a live `Stashing` into
/// every machine — with a mandatory ceremonial bound.)
#[test]
fn a_no_defer_machines_stash_type_is_zero_sized() {
    assert_eq!(
        size_of::<StashOf<WPolicy>>(),
        0,
        "NoDefer declares a ZST stash — no buffer exists to allocate",
    );
}

/// The full seated machine, end to end on the paused clock: all-Deliver
/// gating in Serving, a declared Ignore in Draining, and the plugged
/// seat's grace deadline firing exactly at `entered(Draining) + grace`
/// with a `Step::Stop` reaction.
#[tokio::test(start_paused = true)]
async fn by_phase_seat_arms_on_entry_and_fires_its_pair(){
    let (tx, rx) = flume::unbounded();
    let h = spawn::<W>(WArgs {
        grace: Duration::from_millis(25),
        probe: tx,
    });
    bounded(h.tell(WMsg::Job(1))).await.expect("queued");
    sleep(Duration::from_millis(10)).await;
    let entered = Instant::now(); // Drain lands (paused clock: exact)
    bounded(h.tell(WMsg::Drain)).await.expect("queued");
    bounded(h.tell(WMsg::Job(2))).await.expect("queued");
    bounded(h.tell(WMsg::Gossip)).await.expect("queued");

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::Served(1),
            Ev::Refused(2),
            Ev::TimedOutAt(entered + Duration::from_millis(25)),
            Ev::Stopped(true),
        ],
        "Serving delivers; Draining refuses loudly and drops declared \
         noise; the seat's deadline anchors at phase entry and stops",
    );
}

// ------------------------------------------------- ByState seat --------

#[derive(Debug, Clone, bombay_macros::Msg)]
struct Poke;

struct Idle {
    /// The declarative slot's source: refreshed on every message.
    last_activity: Instant,
    window: Duration,
    probe: flume::Sender<Ev>,
}

/// The SAME `DeadlinePolicy` trait, `ByState` context: the slot reads
/// actor state; the reaction speaks `Step<Never>` — `Goto` is
/// unconstructible for a phase-less actor (`Never` is uninhabited).
struct IdleDl;

impl DeadlinePolicy<ByState<Idle>> for IdleDl {
    fn build(_: &(Duration, flume::Sender<Ev>)) -> Self {
        Self
    }

    fn next_deadline(&self, actor: &Idle, (): ()) -> Option<Instant> {
        actor.last_activity.checked_add(actor.window)
    }

    async fn on_deadline(
        &self,
        actor: &mut Idle,
        (): (),
        _: WeakActorRef<Shell<Idle>>,
    ) -> Result<Step<Never>, Infallible> {
        let _ = actor.probe.send(Ev::TimedOutAt(Instant::now()));
        Ok(Step::Stop)
    }
}

#[derive(bombay_macros::Provide)]
struct IdleCaps {
    deadlined: Deadlined<IdleDl>,
}

impl CapSet<Idle> for IdleCaps {
    fn build(args: &(Duration, flume::Sender<Ev>)) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Idle {
    type Msg = Poke;
    type Args = (Duration, flume::Sender<Ev>);
    type Error = Infallible;
    type Caps = IdleCaps;

    async fn init((window, probe): Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            last_activity: Instant::now(),
            window,
            probe,
        })
    }

    async fn handle(&mut self, Poke: Poke, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        self.last_activity = Instant::now();
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Infallible> {
        let _ = self.probe.send(Ev::Stopped(reason.is_normal()));
        Ok(())
    }
}

/// `ByState` through the unified trait: a poke at t=10 slides the idle
/// deadline to t=10+30; the fire lands exactly there and `Step::Stop`
/// (≅ `Flow::Stop`) stops the actor normally.
#[tokio::test(start_paused = true)]
async fn by_state_seat_slides_with_actor_state_and_stops(){
    let start = Instant::now();
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Idle>((Duration::from_millis(30), tx));
    sleep(Duration::from_millis(10)).await;
    bounded(h.tell(Poke)).await.expect("queued");

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::TimedOutAt(start + Duration::from_millis(40)),
            Ev::Stopped(true),
        ],
        "the slot re-reads actor state (slid by the poke); Step<Never> \
         adapts to a normal stop",
    );
}

// ------------------------------------------------- NoTimeout floor -----

/// A phases-only machine: gate, no deferral, no deadlines — the whole
/// policy is the three core items plus two one-line seat picks.
struct QuietPolicy;

impl PhasePolicy for QuietPolicy {
    type Actor = Quiet;
    type Phase = WPhase;
    type Deferral = NoDefer;
    type Timeout = NoTimeout;

    fn initial(_: &flume::Sender<Ev>) -> WPhase {
        WPhase::Serving
    }

    fn gate(_: WPhase, _: &WMsg) -> Disposition {
        Disposition::Deliver
    }
}

struct Quiet {
    probe: flume::Sender<Ev>,
}

#[derive(bombay_macros::Provide)]
struct QuietCaps {
    phased: Phased<QuietPolicy>,
}

impl CapSet<Quiet> for QuietCaps {
    fn build(args: &flume::Sender<Ev>) -> Self {
        Self {
            phased: Phased::build(args),
        }
    }
}

impl Actor for Quiet {
    type Msg = WMsg;
    type Args = flume::Sender<Ev>;
    type Error = Infallible;
    type Caps = QuietCaps;

    async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { probe })
    }

    async fn handle(&mut self, msg: WMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        match msg {
            WMsg::Job(n) => {
                let _ = self.probe.send(Ev::Served(n));
                Ok(Flow::Continue)
            }
            _ => Ok(Flow::Stop),
        }
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Infallible> {
        let _ = self.probe.send(Ev::Stopped(reason.is_normal()));
        Ok(())
    }
}

/// `NoTimeout`: the slot is constantly disarmed — the clock runs far past
/// any plausible deadline and NOTHING fires; the machine just serves.
#[tokio::test(start_paused = true)]
async fn no_timeout_never_arms_the_deadline_arm() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Quiet>(tx);
    bounded(h.tell(WMsg::Job(7))).await.expect("queued");
    sleep(Duration::from_secs(3600)).await; // a paused-clock eternity
    bounded(h.tell(WMsg::Job(8))).await.expect("queued");
    bounded(h.tell(WMsg::Drain)).await.expect("queued"); // Quiet: any non-Job stops

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![Ev::Served(7), Ev::Served(8), Ev::Stopped(true)],
        "no TimedOutAt ever: NoTimeout is structurally disarmed",
    );
}
