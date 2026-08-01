//! Card #281 — the ADR-0025 deadline plane, integration-tested on the caps
//! surface (`Deadlined<DP>` is the user seat; the loop arm is the SUT).
//!
//! Ports the executable model's properties (spike-274-loop, the durable
//! list in the #274 design spec): P1a prompt-under-saturation, P2
//! disabled-arm, P3 fires-once-per-value, P4 sliding deadline, P5
//! turn-boundary delivery — plus the drain-window fire, the
//! `PanicReason::OnDeadline` crash domain as a watcher observes it, the
//! cancel-delay bound, and the new arm's ordering pins on all three loop
//! shapes.
//!
//! P1b — the counter-model (an arm BELOW the mailbox starves until the
//! backlog drains) — is deliberately NOT a shipped test: it justifies the
//! arm's placement and lives as the doc-comment on the arm in `kind.rs`.
//!
//! All timing runs under `start_paused` virtual clocks; every terminal
//! await is bounded by `terminate_bound()`, which outlasts the longest
//! in-test timer (all are ≤ 50 ms virtual).

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};

use tokio::time::{Instant, sleep, timeout};

use bombay::{
    actor::{Flow, SpawnConfig, WeakActorRef},
    caps::{
        self, Actor, ByState, CapSet, Ctx, DeadlinePolicy, Deadlined, Handle, Never, Shell, Step,
        spawn, spawn_with,
    },
    error::{ActorStopReason, PanicReason},
    mailbox::Capacity,
    reply::ReplySender,
    test_support::terminate_bound,
};

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

fn config(capacity: usize) -> SpawnConfig {
    SpawnConfig {
        capacity: cap(capacity),
        ..SpawnConfig::default()
    }
}

/// Bounds a terminal await (#148 discipline). `terminate_bound()` outlasts
/// every timer in this file.
async fn bounded<F: core::future::IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("await must resolve within the bound")
}

/// The mode-blind probe vocabulary every scenario asserts over.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Handled(u32),
    HandlerDone(u32),
    Fired,
    FiredAt(Instant),
    /// The hook's `WeakActorRef` failed to upgrade — the drain window.
    UpgradeFailed(bool),
    /// A watched peer died; `true` iff the reason was
    /// `Panicked(PanicReason::OnDeadline)`.
    PeerDied {
        on_deadline_panic: bool,
    },
    /// `on_stop` ran; carries `reason.is_normal()`.
    Stopped(bool),
}

/// Collects probe events until the actor's `Stopped` lands (the sender
/// side lives in the actor, so the channel closing without `Stopped`
/// fails loudly).
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

fn count_fired(evs: &[Ev]) -> usize {
    evs.iter().filter(|e| matches!(e, Ev::Fired)).count()
}

fn handled_before_fired(evs: &[Ev]) -> usize {
    evs.iter()
        .take_while(|e| !matches!(e, Ev::Fired))
        .filter(|e| matches!(e, Ev::Handled(_)))
        .count()
}

// ---------------------------------------------------------------- Busy ----

/// The fixed-deadline workhorse: per-message virtual work, a deadline
/// declared as a pure function of its own state, and per-scenario knobs
/// for what the hook does (clear the slot / stop).
struct Busy {
    due: Option<Instant>,
    work: Duration,
    clear_on_fire: bool,
    stop_on_fire: bool,
    probe: flume::Sender<Ev>,
}

struct BusyArgs {
    due_in: Option<Duration>,
    work: Duration,
    clear_on_fire: bool,
    stop_on_fire: bool,
    probe: flume::Sender<Ev>,
}

#[derive(Debug, bombay_macros::Msg)]
enum BusyMsg {
    Item(u32),
    /// In-band FIFO stop: everything queued ahead is served first.
    Quit,
}

struct BusyDl;
impl DeadlinePolicy<ByState<Busy>> for BusyDl {
    fn build(_: &<Busy as Actor>::Args) -> Self {
        Self
    }
    fn next_deadline(&self, actor: &Busy, (): ()) -> Option<Instant> {
        actor.due
    }
    async fn on_deadline(
        &self,
        actor: &mut Busy,
        (): (),
        _: WeakActorRef<Shell<Busy>>,
    ) -> Result<Step<Never>, Infallible> {
        let _ = actor.probe.send(Ev::Fired);
        if actor.clear_on_fire {
            actor.due = None;
        }
        Ok(if actor.stop_on_fire {
            Step::Stop
        } else {
            Step::Stay
        })
    }
}

#[derive(bombay_macros::Provide)]
struct BusyCaps {
    deadlined: Deadlined<BusyDl>,
}

impl CapSet<Busy> for BusyCaps {
    fn build(args: &BusyArgs) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Busy {
    type Msg = BusyMsg;
    type Args = BusyArgs;
    type Error = Infallible;
    type Caps = BusyCaps;

    async fn init(args: BusyArgs, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            due: args.due_in.map(|d| Instant::now() + d),
            work: args.work,
            clear_on_fire: args.clear_on_fire,
            stop_on_fire: args.stop_on_fire,
            probe: args.probe,
        })
    }

    async fn handle(&mut self, msg: BusyMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let BusyMsg::Item(n) = msg else {
            return Ok(Flow::Stop);
        };
        let _ = self.probe.send(Ev::Handled(n));
        if self.work > Duration::ZERO {
            sleep(self.work).await;
        }
        let _ = self.probe.send(Ev::HandlerDone(n));
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

/// P1a — prompt under saturation: deadline at +10 ms, 1 ms of work per
/// message, 50 messages queued up front. The arm sits ABOVE the mailbox
/// arm, so the fire lands when due (~10 handled), never after the backlog.
#[tokio::test(start_paused = true)]
async fn p1a_due_deadline_fires_promptly_under_saturation() {
    let (tx, rx) = flume::unbounded();
    let h = spawn_with::<Busy>(
        config(64),
        BusyArgs {
            due_in: Some(Duration::from_millis(10)),
            work: Duration::from_millis(1),
            clear_on_fire: true,
            stop_on_fire: false,
            probe: tx,
        },
    );
    for i in 0..50 {
        bounded(h.tell(BusyMsg::Item(i))).await.expect("queued");
    }
    bounded(h.tell(BusyMsg::Quit)).await.expect("queued");
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(count_fired(&evs), 1, "exactly one fire: {evs:?}");
    let before = handled_before_fired(&evs);
    assert!(
        (9..=11).contains(&before),
        "the fire lands when due (~10 messages in), not after the backlog; \
         fired after {before}: {evs:?}",
    );
}

/// P2 — disabled arm: no deadline declared, five messages, a graceful
/// stop: zero fires (and no spin — the loop drains and exits).
#[tokio::test(start_paused = true)]
async fn p2_no_deadline_never_fires() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Busy>(BusyArgs {
        due_in: None,
        work: Duration::ZERO,
        clear_on_fire: false,
        stop_on_fire: false,
        probe: tx,
    });
    for i in 0..5 {
        bounded(h.tell(BusyMsg::Item(i))).await.expect("queued");
    }
    bounded(h.tell(BusyMsg::Quit)).await.expect("queued");
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(count_fired(&evs), 0, "disabled arm never fires: {evs:?}");
    assert_eq!(
        evs.iter().filter(|e| matches!(e, Ev::Handled(_))).count(),
        5,
        "all five messages were served: {evs:?}",
    );
    assert_eq!(evs.last(), Some(&Ev::Stopped(true)), "normal stop");
}

/// P3 — fires-once-per-value: the hook leaves its already-due deadline
/// unchanged. Exactly one fire, and the actor still serves a subsequent
/// tell (without the guard this is a busy loop starving the mailbox).
#[tokio::test(start_paused = true)]
async fn p3_unchanged_deadline_fires_exactly_once() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Busy>(BusyArgs {
        due_in: Some(Duration::ZERO),
        work: Duration::ZERO,
        clear_on_fire: false, // the pathological hook: slot left as-is
        stop_on_fire: false,
        probe: tx,
    });
    bounded(h.tell(BusyMsg::Item(7))).await.expect("queued");
    bounded(h.tell(BusyMsg::Quit)).await.expect("queued");
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(
        count_fired(&evs),
        1,
        "fires-once guard: one fire per deadline value: {evs:?}",
    );
    assert!(
        evs.contains(&Ev::Handled(7)),
        "the loop still serves messages after the guarded fire: {evs:?}",
    );
    assert_eq!(evs.last(), Some(&Ev::Stopped(true)), "normal stop");
}

/// P5 — turn-boundary delivery: the deadline comes due MID-handler (30 ms
/// of work, due at +10 ms) and is observed only after the handler returns:
/// Handled → HandlerDone → Fired, never interleaved.
#[tokio::test(start_paused = true)]
async fn p5_deadline_due_mid_handler_fires_at_the_step_boundary() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Busy>(BusyArgs {
        due_in: Some(Duration::from_millis(10)),
        work: Duration::from_millis(30),
        clear_on_fire: true,
        stop_on_fire: true,
        probe: tx,
    });
    bounded(h.tell(BusyMsg::Item(1))).await.expect("queued");
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(
        evs,
        vec![
            Ev::Handled(1),
            Ev::HandlerDone(1),
            Ev::Fired,
            Ev::Stopped(true),
        ],
        "expiry mid-handler is delivered only at the step boundary",
    );
}

/// Cancel-delay bound (ADR-0025 Decision 2's recorded consequence): a
/// token-stop issued while a deadline is due is observed after AT MOST one
/// hook turn — the biased arm order serves the due fire first, the
/// fires-once guard disarms it, and the very next poll sees the cancel.
#[tokio::test(start_paused = true)]
async fn p3b_token_stop_with_a_due_deadline_stops_after_at_most_one_hook_turn() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<Busy>(BusyArgs {
        due_in: Some(Duration::ZERO),
        work: Duration::ZERO,
        clear_on_fire: false, // sticky: without fires-once this would spin
        stop_on_fire: false,
        probe: tx,
    });
    h.stop();
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(
        evs,
        vec![Ev::Fired, Ev::Stopped(true)],
        "exactly one hook turn, then the cancel is observed as Normal",
    );
}

// -------------------------------------------------------------- Slider ----

/// P4 — the sliding deadline (#241's reset-on-message shape): the slot is
/// `last_activity + T`, re-read by the loop after every state-touching
/// step, so each handled message defers the fire with no set/cancel verbs.
struct Slider {
    last_activity: Instant,
    idle: Duration,
    probe: flume::Sender<Ev>,
}

#[derive(Debug, bombay_macros::Msg)]
enum TouchMsg {
    Touch(u32),
}

struct SliderDl;
impl DeadlinePolicy<ByState<Slider>> for SliderDl {
    fn build(_: &<Slider as Actor>::Args) -> Self {
        Self
    }
    fn next_deadline(&self, actor: &Slider, (): ()) -> Option<Instant> {
        Some(actor.last_activity + actor.idle)
    }
    async fn on_deadline(
        &self,
        actor: &mut Slider,
        (): (),
        _: WeakActorRef<Shell<Slider>>,
    ) -> Result<Step<Never>, Infallible> {
        let _ = actor.probe.send(Ev::FiredAt(Instant::now()));
        Ok(Step::Stop)
    }
}

#[derive(bombay_macros::Provide)]
struct SliderCaps {
    deadlined: Deadlined<SliderDl>,
}

impl CapSet<Slider> for SliderCaps {
    fn build(args: &(Duration, flume::Sender<Ev>)) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Slider {
    type Msg = TouchMsg;
    type Args = (Duration, flume::Sender<Ev>);
    type Error = Infallible;
    type Caps = SliderCaps;

    async fn init((idle, probe): Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            last_activity: Instant::now(),
            idle,
            probe,
        })
    }

    async fn handle(&mut self, msg: TouchMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let TouchMsg::Touch(n) = msg;
        self.last_activity = Instant::now();
        let _ = self.probe.send(Ev::Handled(n));
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

/// P4 — touches at t=15 and t=30 under a 20 ms idle window: the fire lands
/// at exactly t=50 (virtual), i.e. `last_activity + T`, never earlier.
#[tokio::test(start_paused = true)]
async fn p4_sliding_deadline_resets_on_activity() {
    let (tx, rx) = flume::unbounded();
    let start = Instant::now();
    let h = spawn::<Slider>((Duration::from_millis(20), tx));

    sleep(Duration::from_millis(15)).await;
    bounded(h.tell(TouchMsg::Touch(1))).await.expect("queued"); // defers to 35
    sleep(Duration::from_millis(15)).await;
    bounded(h.tell(TouchMsg::Touch(2))).await.expect("queued"); // defers to 50

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::Handled(1),
            Ev::Handled(2),
            Ev::FiredAt(start + Duration::from_millis(50)),
            Ev::Stopped(true),
        ],
        "the fire lands at last_activity + T exactly (virtual clock)",
    );
}

// ------------------------------------------------------------- Drainer ----

/// The drain-window scenario: deadlines keep firing after every external
/// strong ref is gone (the arm sits above the mailbox arm), the hook's
/// `WeakActorRef` fails to upgrade there, and its `Flow::Stop` ends the
/// actor `Normal` BEFORE the backlog is exhausted.
struct Drainer {
    due: Option<Instant>,
    probe: flume::Sender<Ev>,
}

struct DrainerDl;
impl DeadlinePolicy<ByState<Drainer>> for DrainerDl {
    fn build(_: &<Drainer as Actor>::Args) -> Self {
        Self
    }
    fn next_deadline(&self, actor: &Drainer, (): ()) -> Option<Instant> {
        actor.due
    }
    async fn on_deadline(
        &self,
        actor: &mut Drainer,
        (): (),
        actor_ref: WeakActorRef<Shell<Drainer>>,
    ) -> Result<Step<Never>, Infallible> {
        let _ = actor
            .probe
            .send(Ev::UpgradeFailed(actor_ref.upgrade().is_none()));
        Ok(Step::Stop)
    }
}

#[derive(bombay_macros::Provide)]
struct DrainerCaps {
    deadlined: Deadlined<DrainerDl>,
}

impl CapSet<Drainer> for DrainerCaps {
    fn build(args: &flume::Sender<Ev>) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Drainer {
    type Msg = BusyMsg;
    type Args = flume::Sender<Ev>;
    type Error = Infallible;
    type Caps = DrainerCaps;

    async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            due: Some(Instant::now() + Duration::from_millis(12)),
            probe,
        })
    }

    async fn handle(&mut self, msg: BusyMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let BusyMsg::Item(n) = msg else {
            return Ok(Flow::Stop);
        };
        let _ = self.probe.send(Ev::Handled(n));
        sleep(Duration::from_millis(5)).await;
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

/// Drain window: 10 messages queued, every external strong ref dropped,
/// deadline due at +12 ms (mid-drain, ~2–3 messages in at 5 ms each). The
/// hook fires, its weak ref fails upgrade, and `Flow::Stop` stops the
/// actor `Normal` before the backlog finishes.
#[tokio::test(start_paused = true)]
async fn drain_window_deadline_fires_with_a_dead_weak_ref_and_stops() {
    let (tx, rx) = flume::unbounded();
    let h = spawn_with::<Drainer>(config(64), tx);
    for i in 0..10 {
        bounded(h.tell(BusyMsg::Item(i))).await.expect("queued");
    }
    drop(h); // the drain window: no external strong ref survives

    let evs = collect_until_stopped(&rx).await;
    let handled = evs.iter().filter(|e| matches!(e, Ev::Handled(_))).count();
    assert!(
        (1..10).contains(&handled),
        "the fire preempts the backlog: {handled} of 10 handled: {evs:?}",
    );
    assert!(
        evs.contains(&Ev::UpgradeFailed(true)),
        "the hook's WeakActorRef must fail upgrade in the drain window: {evs:?}",
    );
    assert_eq!(
        evs.last(),
        Some(&Ev::Stopped(true)),
        "the hook's Flow::Stop is a Normal stop, not Collected: {evs:?}",
    );
}

// ------------------------------------------------------ Bomb + Watcher ----

/// A hook that panics: the crash domain must be `PanicReason::OnDeadline`,
/// observable by a watcher on the death notice.
struct Bomb {
    due: Option<Instant>,
}

struct BombDl;
impl DeadlinePolicy<ByState<Bomb>> for BombDl {
    fn build(_: &<Bomb as Actor>::Args) -> Self {
        Self
    }
    fn next_deadline(&self, actor: &Bomb, (): ()) -> Option<Instant> {
        actor.due
    }
    async fn on_deadline(
        &self,
        _: &mut Bomb,
        (): (),
        _: WeakActorRef<Shell<Bomb>>,
    ) -> Result<Step<Never>, Infallible> {
        panic!("deadline bomb");
    }
}

#[derive(bombay_macros::Provide)]
struct BombCaps {
    deadlined: Deadlined<BombDl>,
}

impl CapSet<Bomb> for BombCaps {
    fn build(args: &()) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Bomb {
    type Msg = BusyMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = BombCaps;

    async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            due: Some(Instant::now() + Duration::from_millis(5)),
        })
    }

    async fn handle(&mut self, _: BusyMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}

/// Records whether a watched death carried the `OnDeadline` crash domain.
struct TapeDeath;
impl caps::WatchPolicy<Watcher> for TapeDeath {
    async fn on_link_died(
        actor: &mut Watcher,
        _: bombay::ActorId,
        reason: ActorStopReason,
        _: bool,
    ) -> Result<core::ops::ControlFlow<ActorStopReason>, Infallible> {
        let on_deadline_panic = matches!(
            &reason,
            ActorStopReason::Panicked(e) if e.reason() == PanicReason::OnDeadline
        );
        let _ = actor.probe.send(Ev::PeerDied { on_deadline_panic });
        Ok(core::ops::ControlFlow::Continue(()))
    }
}

#[derive(Debug, bombay_macros::Msg)]
enum WatchMsg {
    #[msg(budget = 64)]
    Watch {
        target: Handle<Bomb>,
        reply: ReplySender<Result<(), ()>>,
    },
}

#[derive(bombay_macros::Provide)]
struct WatcherCaps {
    watching: caps::Watching<TapeDeath>,
}

impl CapSet<Watcher> for WatcherCaps {
    fn build(_: &flume::Sender<Ev>) -> Self {
        Self {
            watching: caps::Watching::new(),
        }
    }
}

struct Watcher {
    probe: flume::Sender<Ev>,
}

impl Actor for Watcher {
    type Msg = WatchMsg;
    type Args = flume::Sender<Ev>;
    type Error = Infallible;
    type Caps = WatcherCaps;

    async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { probe })
    }

    async fn handle(&mut self, msg: WatchMsg, cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let WatchMsg::Watch { target, reply } = msg;
        let outcome = bounded(cx.self_ref().watch(&target)).await.map_err(|_| ());
        let _ = reply.send(outcome);
        Ok(Flow::Continue)
    }
}

/// Panic-in-hook: the actor stops and its watcher sees `Panicked` tagged
/// `PanicReason::OnDeadline` — the handler-like crash domain, end to end.
#[tokio::test(start_paused = true)]
async fn a_panicking_hook_dies_in_the_on_deadline_crash_domain() {
    let (tx, rx) = flume::unbounded();
    let bomb = spawn::<Bomb>(());
    let watcher = spawn::<Watcher>(tx);

    let watched = bounded(watcher.ask(|reply| WatchMsg::Watch {
        target: bomb.clone(),
        reply,
    }))
    .await
    .expect("watch replied");
    assert_eq!(watched, Ok(()), "the edge installed before the fire");

    let ev = bounded(rx.recv_async()).await.expect("death observed");
    assert_eq!(
        ev,
        Ev::PeerDied {
            on_deadline_panic: true
        },
        "the death notice carries Panicked(PanicReason::OnDeadline)",
    );
    drop(bomb);
    drop(watcher);
}

// ---------------------------------------------- ordering pins (3 loops) ----

#[derive(Debug, bombay_macros::Msg)]
enum ArmMsg {
    /// Sets the deadline slot to an ALREADY-DUE instant and returns.
    Arm,
    /// The message queued behind the due deadline.
    Next,
    /// In-band FIFO stop.
    Quit,
}

/// Stamps one fixed-shape "armer" actor per loop flavor: `Arm` makes the
/// deadline due at the very next step boundary with `Next` already queued
/// — the biased order (deadline above mailbox) must serve the fire first.
macro_rules! armer {
    ($actor:ident, $dl:ident, $caps:ident { $($field:ident : $fty:ty = $finit:expr),* $(,)? }) => {
        struct $actor {
            due: Option<Instant>,
            probe: flume::Sender<Ev>,
        }

        struct $dl;
        impl DeadlinePolicy<ByState<$actor>> for $dl {
            fn build(_: &<$actor as Actor>::Args) -> Self {
                Self
            }
            fn next_deadline(&self, actor: &$actor, (): ()) -> Option<Instant> {
                actor.due
            }
            async fn on_deadline(
                &self,
                actor: &mut $actor,
                (): (),
                _: WeakActorRef<Shell<$actor>>,
            ) -> Result<Step<Never>, Infallible> {
                let _ = actor.probe.send(Ev::Fired);
                actor.due = None;
                Ok(Step::Stay)
            }
        }

        #[derive(bombay_macros::Provide)]
        struct $caps {
            deadlined: Deadlined<$dl>,
            $($field: $fty),*
        }

        impl CapSet<$actor> for $caps {
            fn build(args: &flume::Sender<Ev>) -> Self {
                Self {
                    deadlined: Deadlined::build(args),
                    $($field: $finit),*
                }
            }
        }

        impl Actor for $actor {
            type Msg = ArmMsg;
            type Args = flume::Sender<Ev>;
            type Error = Infallible;
            type Caps = $caps;

            async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
                Ok(Self { due: None, probe })
            }

            async fn handle(&mut self, msg: ArmMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
                match msg {
                    ArmMsg::Arm => {
                        let _ = self.probe.send(Ev::Handled(100));
                        self.due = Some(Instant::now());
                    }
                    ArmMsg::Next => {
                        let _ = self.probe.send(Ev::Handled(200));
                    }
                    ArmMsg::Quit => return Ok(Flow::Stop),
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
    };
}

armer!(ArmerPlain, ArmerPlainDl, ArmerPlainCaps {});
armer!(ArmerLinked, ArmerLinkedDl, ArmerLinkedCaps {
    watching: caps::Watching<caps::OtpPropagation> = caps::Watching::new(),
});
armer!(ArmerSup, ArmerSupDl, ArmerSupCaps {
    watching: caps::Watching<caps::OtpPropagation> = caps::Watching::new(),
    supervising: caps::Supervising<caps::OneForOne> = caps::Supervising::new(),
});

/// Drives one armer through the pin: `Arm` and `Next` are both queued
/// before the boundary where the deadline comes due; the fire must land
/// between them.
async fn drive_armer<A>(h: Handle<A>, rx: flume::Receiver<Ev>) -> Vec<Ev>
where
    A: Actor<Msg = ArmMsg>,
{
    bounded(h.tell(ArmMsg::Arm)).await.expect("queued");
    bounded(h.tell(ArmMsg::Next)).await.expect("queued");
    bounded(h.tell(ArmMsg::Quit)).await.expect("queued");
    collect_until_stopped(&rx).await
}

const DEADLINE_BEFORE_MESSAGE: [Ev; 4] = [
    Ev::Handled(100),
    Ev::Fired,
    Ev::Handled(200),
    Ev::Stopped(true),
];

/// Ordering pin, plain loop: with a due deadline and a queued message at
/// the same boundary, the hook runs first (the arm sits above the mailbox).
#[tokio::test(start_paused = true)]
async fn plain_loop_serves_a_due_deadline_before_a_queued_message() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<ArmerPlain>(tx);
    assert_eq!(drive_armer(h, rx).await, DEADLINE_BEFORE_MESSAGE);
}

/// Ordering pin, linked loop: same relation with the link arm present.
#[tokio::test(start_paused = true)]
async fn linked_loop_serves_a_due_deadline_before_a_queued_message() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<ArmerLinked>(tx);
    assert_eq!(drive_armer(h, rx).await, DEADLINE_BEFORE_MESSAGE);
}

/// Ordering pin, supervised loop: same relation with all three
/// housekeeping arms present.
#[tokio::test(start_paused = true)]
async fn supervised_loop_serves_a_due_deadline_before_a_queued_message() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<ArmerSup>(tx);
    assert_eq!(drive_armer(h, rx).await, DEADLINE_BEFORE_MESSAGE);
}

// ------------------------------------- death-before-deadline (2 loops) ----

/// The victim a mourner watches and kills.
struct Victim;

#[derive(Debug, bombay_macros::Msg)]
enum VictimMsg {
    Poke,
}

impl Actor for Victim {
    type Msg = VictimMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = ();
    async fn init((): (), _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self)
    }
    async fn handle(&mut self, _: VictimMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}

#[derive(Debug, bombay_macros::Msg)]
enum MournMsg {
    #[msg(budget = 64)]
    Watch {
        target: Handle<Victim>,
        reply: ReplySender<Result<(), ()>>,
    },
    /// Kills the watched victim, waits out its death (virtual), then arms
    /// an already-due deadline — so the notice and the fire are BOTH ready
    /// at the same step boundary.
    #[msg(budget = 64)]
    ArmAndKill { target: Handle<Victim> },
    /// In-band FIFO stop.
    Quit,
}

/// Stamps one "mourner" per link-reactive loop flavor: the ready death
/// notice must beat the due deadline (link arm above the deadline arm).
macro_rules! mourner {
    ($actor:ident, $dl:ident, $wp:ident, $caps:ident { $($field:ident : $fty:ty = $finit:expr),* $(,)? }) => {
        struct $actor {
            due: Option<Instant>,
            probe: flume::Sender<Ev>,
        }

        struct $dl;
        impl DeadlinePolicy<ByState<$actor>> for $dl {
            fn build(_: &<$actor as Actor>::Args) -> Self {
                Self
            }
            fn next_deadline(&self, actor: &$actor, (): ()) -> Option<Instant> {
                actor.due
            }
            async fn on_deadline(
                &self,
                actor: &mut $actor,
                (): (),
                _: WeakActorRef<Shell<$actor>>,
            ) -> Result<Step<Never>, Infallible> {
                let _ = actor.probe.send(Ev::Fired);
                actor.due = None;
                Ok(Step::Stay)
            }
        }

        struct $wp;
        impl caps::WatchPolicy<$actor> for $wp {
            async fn on_link_died(
                actor: &mut $actor,
                _: bombay::ActorId,
                _: ActorStopReason,
                _: bool,
            ) -> Result<core::ops::ControlFlow<ActorStopReason>, Infallible> {
                let _ = actor.probe.send(Ev::PeerDied {
                    on_deadline_panic: false,
                });
                Ok(core::ops::ControlFlow::Continue(()))
            }
        }

        #[derive(bombay_macros::Provide)]
        struct $caps {
            deadlined: Deadlined<$dl>,
            watching: caps::Watching<$wp>,
            $($field: $fty),*
        }

        impl CapSet<$actor> for $caps {
            fn build(args: &flume::Sender<Ev>) -> Self {
                Self {
                    deadlined: Deadlined::build(args),
                    watching: caps::Watching::new(),
                    $($field: $finit),*
                }
            }
        }

        impl Actor for $actor {
            type Msg = MournMsg;
            type Args = flume::Sender<Ev>;
            type Error = Infallible;
            type Caps = $caps;

            async fn init(probe: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
                Ok(Self { due: None, probe })
            }

            async fn handle(
                &mut self,
                msg: MournMsg,
                cx: Ctx<'_, Self>,
            ) -> Result<Flow, Infallible> {
                match msg {
                    MournMsg::Watch { target, reply } => {
                        let outcome =
                            bounded(cx.self_ref().watch(&target)).await.map_err(|_| ());
                        let _ = reply.send(outcome);
                        drop(target);
                    }
                    MournMsg::ArmAndKill { target } => {
                        target.kill();
                        drop(target);
                        // Let the victim's task die and its notice land on
                        // our link channel while this turn is still running.
                        sleep(Duration::from_millis(10)).await;
                        self.due = Some(Instant::now());
                    }
                    MournMsg::Quit => return Ok(Flow::Stop),
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
    };
}

mourner!(
    MournerLinked,
    MournerLinkedDl,
    MournerLinkedWp,
    MournerLinkedCaps {}
);
mourner!(MournerSup, MournerSupDl, MournerSupWp, MournerSupCaps {
    supervising: caps::Supervising<caps::OneForOne> = caps::Supervising::new(),
});

async fn drive_mourner<A>(h: Handle<A>, rx: flume::Receiver<Ev>) -> Vec<Ev>
where
    A: Actor<Msg = MournMsg>,
{
    let victim = spawn::<Victim>(());
    let watched = bounded(h.ask(|reply| MournMsg::Watch {
        target: victim.clone(),
        reply,
    }))
    .await
    .expect("watch replied");
    assert_eq!(watched, Ok(()), "the edge installed before the kill");

    bounded(h.tell(MournMsg::ArmAndKill { target: victim }))
        .await
        .expect("queued");
    bounded(h.tell(MournMsg::Quit)).await.expect("queued");
    collect_until_stopped(&rx).await
}

const DEATH_BEFORE_DEADLINE: [Ev; 3] = [
    Ev::PeerDied {
        on_deadline_panic: false,
    },
    Ev::Fired,
    Ev::Stopped(true),
];

/// Ordering pin, linked loop: a ready death notice and a due deadline at
/// the same boundary — `on_link_died` runs first (link arm above the
/// deadline arm; no existing inter-arm relation changed).
#[tokio::test(start_paused = true)]
async fn linked_loop_serves_a_ready_death_before_a_due_deadline() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<MournerLinked>(tx);
    assert_eq!(drive_mourner(h, rx).await, DEATH_BEFORE_DEADLINE);
}

/// Ordering pin, supervised loop: the same relation for a non-child peer
/// death routed through the watch policy.
#[tokio::test(start_paused = true)]
async fn supervised_loop_serves_a_ready_death_before_a_due_deadline() {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<MournerSup>(tx);
    assert_eq!(drive_mourner(h, rx).await, DEATH_BEFORE_DEADLINE);
}
