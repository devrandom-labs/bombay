//! Head-to-head: the supervision surface (#120 design; #195 death-watch + links;
//! #196 restart; #199 `OneForAll`/`RestForOne` set-strategies) vs upstream
//! kameo's links + `SupervisedActorBuilder` (crates.io, re-pointed off the
//! deleted vendored fork by card #213).
//!
//! This is the honest replacement for `watcher_fanout.rs`, which measured a
//! *synthetic* fan-out (one event cloned into N bare mailboxes) as a proxy for a
//! graph that did not exist when it was written. The graph exists now, so these
//! arms drive the **real** watch/link/restart path on both sides, exactly as
//! `request_vs_kameo.rs` and `registry_vs_kameo.rs` do for their surfaces: same
//! mailbox / 2-worker runtime, criterion, `harness = false`, the crates.io
//! `kameo` dev-dep on the kameo side, production send/handle path only — never a
//! reimplementation (CLAUDE rule 0: measure, don't assume).
//!
//! # The semantics are NOT symmetric, and that is priced, not faked
//!
//! Where bombay's model does not map 1:1 onto kameo's, the asymmetry is stated
//! here (mirroring how `registry_vs_kameo.rs` prices weak-vs-strong handles)
//! rather than hidden behind a matched-looking number:
//!
//! * **No notify-only watch on the kameo side.** bombay's `watch` is a
//!   one-directional, notify-only edge that rides the *target's* unbounded
//!   control lane (a `ControlSignal::Watch`, ADR-0021) and pins nothing
//!   (ADR-0003 weak children). kameo has
//!   only bidirectional `link`, guarded by a per-actor `Mutex<Links>` locked on
//!   *both* peers (id-ordered) at registration. So the fan-out arm compares
//!   bombay's `watch` against kameo's `link` — the closest primitive — and the
//!   kameo side pays the extra bidirectional edge + mutex on every registration.
//!
//! * **Rebuild reuses nothing on the bombay side.** bombay's supervisor holds no
//!   strong ref to a child (ADR-0003) and rebuilds by running the user factory,
//!   which spawns a **brand-new actor** — new `ActorId`, new mailbox — every
//!   incarnation; the caller anchors it. kameo restarts **in place**: the same
//!   `mailbox_tx` (and the `ActorRef` the caller holds) is reused across
//!   incarnations, only the `ActorId` and the receiver half are fresh. So the
//!   restart arm's bombay number includes a full mailbox allocation per cycle
//!   that kameo's does not — a design cost, recorded, not smoothed over.
//!
//! * **Backoff.** bombay routes every rebuild through a `DelayQueue` (#196); the
//!   restart/set arms pin `min_backoff = max_backoff = 0` and `jitter = 0` so the
//!   number is the death → policy → respawn *machinery*, not a sleep. kameo
//!   exposes no backoff knob on its builder (immediate restart). The give-up
//!   budgets are maxed on both sides (`reset_after = 0` + `max_total = u32::MAX`
//!   on bombay; `restart_limit(u32::MAX, 1 day)` on kameo) so every arm measures
//!   steady-state cycle latency, never escalation.
//!
//! * **Set strategies exist on both sides.** kameo's `SupervisionStrategy` also
//!   offers `OneForAll`/`RestForOne`, so the coalesce arm is a recorded delta,
//!   not the documented-asymmetry fallback the card allowed for. Both restart the
//!   whole set when the eldest child crashes; bombay computes the set as a
//!   widen-coalesced suffix (#199, ADR-0014), kameo as its strategy table.
//!
//! # Method per arm
//!
//! * `supervise_watch_fanout` (swept over width) — one target's death delivered
//!   to N observers over the real notify path. `iter_batched` keeps the fleet +
//!   registrations out of the timed region; the timed routine is `kill` +
//!   collect N acknowledgements (each observer's `on_link_died` acks). bombay
//!   uses `watch` (notify-only); kameo uses `link` (see asymmetry above).
//! * `supervise_link_teardown` — a linked peer's abnormal death propagating
//!   across the link: `kill` one side, the other's default hook `Break`s and it
//!   stops; timed until its `on_stop` acks. `iter_batched` (a stopped peer can't
//!   be reused).
//! * `supervise_restart_cycle` — one child-death → policy → (zero) backoff →
//!   respawn round trip. A long-lived supervisor + child is reused; the timed
//!   routine crashes the current incarnation and waits for the next `on_start`.
//! * `supervise_set_strategy_coalesce` (swept over set size) — crash the eldest
//!   child under `OneForAll` / `RestForOne`; the whole set rebuilds. Timed until
//!   all N fresh incarnations have started.
//!
//! Measured 2026-07-26 (M-series laptop, criterion defaults; medians).
//! NOTE: the kameo column below was measured against the vendored 0.21 fork;
//! #213 re-pointed the arm at crates.io kameo 0.22 — re-measure before citing:
//!
//! | group                       | param | bombay | kameo    | delta        |
//! |-----------------------------|-------|-------------|----------|--------------|
//! | watch_fanout                | 8     | 12.3 µs     | 16.2 µs  | bombay 1.32× |
//! | watch_fanout                | 64    | 51.5 µs     | 73.0 µs  | bombay 1.42× |
//! | watch_fanout                | 512   | 269 µs      | 502 µs   | bombay 1.86× |
//! | link_teardown               | —     | 7.98 µs     | 8.43 µs  | bombay 1.06× |
//! | restart_cycle               | —     | 1.95 ms     | 18.4 µs  | kameo 106×   |
//! | set_strategy_coalesce (OFA) | 2     | 1.86 ms     | 21.4 µs  | kameo 87×    |
//! | set_strategy_coalesce (OFA) | 8     | 1.94 ms     | 32.4 µs  | kameo 60×    |
//! | set_strategy_coalesce (OFA) | 32    | 1.91 ms     | 68.5 µs  | kameo 28×    |
//! | set_strategy_coalesce (RFO) | 2     | 1.82 ms     | 21.4 µs  | kameo 85×    |
//! | set_strategy_coalesce (RFO) | 8     | 1.80 ms     | 32.6 µs  | kameo 55×    |
//! | set_strategy_coalesce (RFO) | 32    | 1.95 ms     | 68.5 µs  | kameo 29×    |
//!
//! # Reading
//!
//! * **watch_fanout — bombay wins, and the gap widens with width (1.32× → 1.86×).**
//!   This is the mailbox-Signal notify path vs kameo's `Mutex<Links>`: bombay
//!   registers and fans out over the target's single-writer mailbox with no lock,
//!   while kameo takes an async mutex on both peers per registration and walks a
//!   mutex-guarded map to deliver. More watchers ⇒ more the lock-free path pulls
//!   ahead — the many-watchers regime this surface is designed for.
//!
//! * **link_teardown — parity (1.06×).** A single link propagation is dominated by
//!   one cross-task wakeup (~µs); at width 1 the mutex-vs-mailbox difference is in
//!   the noise. It only separates under fan-out (the arm above).
//!
//! * **restart_cycle / set_coalesce — bombay's number is a TIMER FLOOR, not
//!   compute, and this is by design.** Every bombay rebuild routes through the
//!   restart `DelayQueue` (#196); a `min_backoff = 0` entry still fires on the next
//!   tokio timer tick (~1 ms granularity), so the ~1.9 ms is ≈ one tick — which is
//!   why it is *flat* across set size (rebuilding 2 vs 32 children is noise under
//!   the tick). kameo restarts synchronously, no timer, so it scales with the work
//!   (21 → 68 µs as N grows). Two things this measurement is NOT saying:
//!     1. **It is not a production cost.** Zero backoff is a crash-loop config no
//!        one runs; under any real backoff (bombay's default `min_backoff` is
//!        100 ms) the tick is free — the timer you are already waiting on.
//!     2. **bombay's flat curve is an asset at scale, not only a floor.** bombay
//!        coalesces the *whole* restart set into one timer-gated pass (#199,
//!        widen-coalesce, ADR-0014) — O(1) in timer waits — while kameo issues N
//!        synchronous restarts, O(N). Extrapolating the two curves, bombay's flat
//!        ~1.9 ms crosses *under* kameo's linear per-child cost at ~10³ children.
//!
//! * **What the restart delta really reflects: a richer identity model, not
//!   slowness.** bombay rebuilds a genuinely fresh incarnation (new mailbox/state)
//!   and lifts durable addressing to a separate cryptographic layer (KERI `ActorId`
//!   #121, Zenoh key-expr addressing M3) — the virtual-actor lineage (Orleans:
//!   logical identity decoupled from activation, Bernstein/Bykov et al., MSR 2014)
//!   extended with self-certifying identity. Callers address the stable principal;
//!   the incarnation behind it is disposable. kameo's hybrid — new `ActorId` but a
//!   *reused* mailbox — is neither clean-slate nor stable-identity. "Faster
//!   in-place restart" is kameo's local optimum; bombay's number buys a strictly
//!   more expressive addressing model. Do not read "slower ⇒ worse".
//!
//! A zero-backoff fast-path (respawn inline instead of through the `DelayQueue`
//! when `min_backoff == 0`) would erase the tick, but zero-backoff restart is not a
//! production scenario, so it is a candidate optimization, not a filed regression.

use std::{hint::black_box, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// Fan-out widths: how many observers one death is delivered to. Two orders of
/// magnitude so the per-observer scaling is visible. Smaller than
/// `watcher_fanout`'s `[16, 128, 1024]` because each observer here is a **live
/// spawned actor with a real link edge**, not a bare mailbox.
const WIDTHS: [usize; 3] = [8, 64, 512];

/// Set sizes for the coalesce arm: crashing one child rebuilds this many actors.
const SET_SIZES: [usize; 3] = [2, 8, 32];

/// The same 2-worker multi-thread runtime the sibling head-to-heads use, with
/// the TIME driver enabled — teardown bounds `on_stop` with a timer, and the
/// restart arm's `DelayQueue` backoff needs one too.
fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("bench runtime")
}

/// Drains exactly `n` acks from a birth/death channel, panicking (rather than
/// hanging silently) if the channel closes early.
async fn drain(rx: &mut UnboundedReceiver<()>, n: usize) {
    for _ in 0..n {
        rx.recv().await.expect("every expected ack must arrive");
    }
}

/// The restart/set arms crash their children with a `panic!("bench crash")`; the
/// actor loop catches it (panic = unwind, #169), so the process survives, but the
/// default panic hook still writes a message to stderr for *every* crash — which
/// at criterion's iteration counts is gigabytes of spam. This installs a hook
/// that swallows exactly that payload and defers every other panic to the
/// original hook, so a genuine bug still surfaces.
fn silence_bench_crashes() {
    static SILENCE: std::sync::Once = std::sync::Once::new();
    SILENCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let is_bench_crash = info
                .payload()
                .downcast_ref::<&str>()
                .is_some_and(|s| *s == "bench crash");
            if !is_bench_crash {
                default(info);
            }
        }));
    });
}

mod core_side {
    use bombay::actor::{Actor, ActorRef, Flow, Spawn as _, Supervisor, Watch, WeakActorRef};
    use bombay::error::{ActorStopReason, Infallible};
    use bombay::mailbox::{ActorId, MailboxSender, Mailboxed};
    use bombay::message::Msg;
    use bombay::restart::{Jitter, RestartConfig, RestartPolicy, SupervisionStrategy};
    use core::ops::ControlFlow;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::UnboundedSender;

    /// A cold marker message no arm ever sends — the watched/linked actors here
    /// exist to die, not to handle work.
    #[derive(Debug)]
    pub struct Idle;
    impl Msg for Idle {}

    /// A passive target: watched/linked, then killed. Needs no `Watch`.
    pub struct Target;
    impl Mailboxed for Target {
        type Msg = Idle;
    }
    impl Actor for Target {
        type Args = ();
        type Error = Infallible;
        async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(&mut self, _: Idle, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
    }

    /// An observer that acks each death it sees through its real `on_link_died`
    /// and keeps running (`Continue`), so one target death yields exactly one ack.
    pub struct Observer {
        ack: UnboundedSender<()>,
    }
    impl Mailboxed for Observer {
        type Msg = Idle;
    }
    impl Actor for Observer {
        type Args = UnboundedSender<()>;
        type Error = Infallible;
        async fn on_start(ack: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { ack })
        }
        async fn handle(&mut self, _: Idle, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
    }
    impl Watch for Observer {
        async fn on_link_died(
            &mut self,
            _: ActorId,
            _: ActorStopReason,
            _: bool,
        ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
            let _ = self.ack.send(());
            Ok(ControlFlow::Continue(()))
        }
    }

    /// A linked peer. Only the surviving side carries `stopped`; it acks from its
    /// `on_stop`, so the timed region ends when the link propagation has stopped it.
    pub struct Peer {
        stopped: Option<UnboundedSender<()>>,
    }
    impl Mailboxed for Peer {
        type Msg = Idle;
    }
    impl Actor for Peer {
        type Args = Option<UnboundedSender<()>>;
        type Error = Infallible;
        async fn on_start(stopped: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { stopped })
        }
        async fn handle(&mut self, _: Idle, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            if let Some(tx) = &self.stopped {
                let _ = tx.send(());
            }
            Ok(())
        }
    }
    // Default `on_link_died`: a linked abnormal death (the killed peer) `Break`s,
    // so the surviving peer stops — the propagation this arm measures.
    impl Watch for Peer {}

    /// A supervised worker: it acks its birth from `on_start` (so a rebuild is
    /// observable) and crashes on command.
    pub struct Worker {
        _tick: UnboundedSender<()>,
    }
    #[derive(Debug)]
    pub struct Crash;
    impl Msg for Crash {}
    impl Mailboxed for Worker {
        type Msg = Crash;
    }
    impl Actor for Worker {
        type Args = UnboundedSender<()>;
        type Error = Infallible;
        async fn on_start(tick: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            let _ = tick.send(());
            Ok(Self { _tick: tick })
        }
        async fn handle(&mut self, _: Crash, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            panic!("bench crash");
        }
    }

    macro_rules! supervisor {
        ($name:ident, $strategy:expr) => {
            pub struct $name;
            impl Mailboxed for $name {
                type Msg = Idle;
            }
            impl Actor for $name {
                type Args = ();
                type Error = Infallible;
                async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                    Ok(Self)
                }
                async fn handle(
                    &mut self,
                    _: Idle,
                    _: ActorRef<Self>,
                ) -> Result<Flow, Self::Error> {
                    Ok(Flow::Continue)
                }
            }
            impl Watch for $name {}
            impl Supervisor for $name {
                fn supervision_strategy() -> SupervisionStrategy {
                    $strategy
                }
            }
        };
    }
    supervisor!(SupOneForOne, SupervisionStrategy::OneForOne);
    supervisor!(SupOneForAll, SupervisionStrategy::OneForAll);
    supervisor!(SupRestForOne, SupervisionStrategy::RestForOne);

    /// Give-up budgets maxed and backoff pinned to zero: the restart/set arms
    /// measure the death → policy → respawn machinery, not a backoff sleep or an
    /// escalation. `reset_after = 0` makes every incarnation instantly healthy so
    /// the consecutive trip never fires; `max_total = u32::MAX` covers the
    /// lifetime budget across thousands of criterion iterations.
    pub fn fast_restart() -> RestartConfig {
        RestartConfig::new(RestartPolicy::Permanent)
            .with_min_backoff(Duration::ZERO)
            .with_max_backoff(Duration::ZERO)
            .with_jitter(Jitter::percent(0))
            .with_reset_after(Duration::ZERO)
            .with_max_restarts(u32::MAX)
            .with_max_total(u32::MAX)
    }

    use core::time::Duration;

    /// A per-child anchor: the supervisor pins nothing (ADR-0003), so the factory
    /// stashes each incarnation's sender here to keep it from ref-count-stopping,
    /// and the driver reads it to reach the live incarnation.
    pub type Slot = Arc<Mutex<Option<MailboxSender<Worker>>>>;

    /// The user factory `supervise` wraps: spawns a fresh `Worker`, anchors its
    /// sender in `slot`, and returns the (dropped-by-the-supervisor) ref. Captures
    /// no strong `ActorRef` of the supervisor.
    pub fn worker_factory(
        tick: UnboundedSender<()>,
        slot: Slot,
    ) -> impl FnMut() -> ActorRef<Worker> + Send + 'static {
        move || {
            let child = Worker::spawn(tick.clone());
            *slot.lock().expect("slot") = Some(child.mailbox_sender().clone());
            child
        }
    }
}

mod kameo_side {
    use core::ops::ControlFlow;
    use kameo::actor::{ActorId, ActorRef, WeakActorRef};
    use kameo::error::{ActorStopReason, Infallible};
    use kameo::prelude::*;
    use kameo::supervision::SupervisionStrategy;
    use tokio::sync::mpsc::UnboundedSender;

    /// Observer: kameo has only bidirectional `link`, so this is `link`ed to the
    /// target (see the module header's asymmetry note). Acks each death, continues.
    #[derive(Clone)]
    pub struct Observer {
        pub ack: UnboundedSender<()>,
    }
    impl Actor for Observer {
        type Args = Self;
        type Error = Infallible;
        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
        async fn on_link_died(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorId,
            _: ActorStopReason,
        ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
            let _ = self.ack.send(());
            Ok(ControlFlow::Continue(()))
        }
    }

    /// The passive target, killed to fan its death out to the linked observers.
    #[derive(Actor, Default)]
    pub struct Target;

    /// A linked peer; the survivor acks from `on_stop`.
    #[derive(Clone)]
    pub struct Peer {
        pub stopped: Option<UnboundedSender<()>>,
    }
    impl Actor for Peer {
        type Args = Self;
        type Error = Infallible;
        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(state)
        }
        async fn on_stop(
            &mut self,
            _: WeakActorRef<Self>,
            _: ActorStopReason,
        ) -> Result<(), Self::Error> {
            if let Some(tx) = &self.stopped {
                let _ = tx.send(());
            }
            Ok(())
        }
        // Default `on_link_died`: `Break` on the killed peer's abnormal death.
    }

    /// A supervised worker: acks its birth from `on_start`, crashes on command.
    #[derive(Clone)]
    pub struct Worker {
        pub tick: UnboundedSender<()>,
    }
    pub struct Crash;
    impl Actor for Worker {
        type Args = Self;
        type Error = Infallible;
        async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            let _ = state.tick.send(());
            Ok(state)
        }
    }
    impl Message<Crash> for Worker {
        type Reply = ();
        async fn handle(&mut self, _: Crash, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
            panic!("bench crash");
        }
    }

    macro_rules! supervisor {
        ($name:ident, $strategy:expr) => {
            #[derive(Clone)]
            pub struct $name;
            impl Actor for $name {
                type Args = Self;
                type Error = Infallible;
                async fn on_start(
                    state: Self::Args,
                    _: ActorRef<Self>,
                ) -> Result<Self, Self::Error> {
                    Ok(state)
                }
                fn supervision_strategy() -> SupervisionStrategy {
                    $strategy
                }
            }
        };
    }
    supervisor!(SupOneForOne, SupervisionStrategy::OneForOne);
    supervisor!(SupOneForAll, SupervisionStrategy::OneForAll);
    supervisor!(SupRestForOne, SupervisionStrategy::RestForOne);
}

/// One target's death delivered to N observers, over the real notify path.
fn watch_fanout(c: &mut Criterion) {
    use bombay::actor::{Spawn as _, SpawnLinked as _};
    use kameo::actor::Spawn as _;

    let rt = runtime();
    let mut group = c.benchmark_group("supervise_watch_fanout");
    for &n in &WIDTHS {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("bombay", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (tx, rx) = unbounded_channel::<()>();
                        let target = core_side::Target::spawn(());
                        let observers: Vec<_> = (0..n)
                            .map(|_| core_side::Observer::spawn_linked(tx.clone()))
                            .collect();
                        for obs in &observers {
                            obs.watch(&target).await.expect("linked observer can watch");
                        }
                        (target, observers, rx)
                    })
                },
                |(target, observers, mut rx)| {
                    rt.block_on(async {
                        target.kill();
                        drain(&mut rx, n).await;
                        black_box(&observers);
                    });
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("kameo", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (tx, rx) = unbounded_channel::<()>();
                        let target = kameo_side::Target::spawn(kameo_side::Target);
                        let observers: Vec<_> = (0..n)
                            .map(|_| {
                                kameo_side::Observer::spawn(kameo_side::Observer {
                                    ack: tx.clone(),
                                })
                            })
                            .collect();
                        for obs in &observers {
                            obs.link(&target).await;
                        }
                        (target, observers, rx)
                    })
                },
                |(target, observers, mut rx)| {
                    rt.block_on(async {
                        target.kill();
                        drain(&mut rx, n).await;
                        black_box(&observers);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// A linked peer's abnormal death propagating across the link and stopping the
/// survivor.
fn link_teardown(c: &mut Criterion) {
    use bombay::actor::SpawnLinked as _;
    use kameo::actor::Spawn as _;

    let rt = runtime();
    let mut group = c.benchmark_group("supervise_link_teardown");
    group.throughput(Throughput::Elements(1));

    group.bench_function("bombay", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (tx, rx) = unbounded_channel::<()>();
                    let survivor = core_side::Peer::spawn_linked(Some(tx));
                    let victim = core_side::Peer::spawn_linked(None);
                    survivor.link(&victim).await.expect("both linked-spawned");
                    (survivor, victim, rx)
                })
            },
            |(survivor, victim, mut rx)| {
                rt.block_on(async {
                    victim.kill();
                    drain(&mut rx, 1).await;
                    black_box(&survivor);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("kameo", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (tx, rx) = unbounded_channel::<()>();
                    let survivor = kameo_side::Peer::spawn(kameo_side::Peer { stopped: Some(tx) });
                    let victim = kameo_side::Peer::spawn(kameo_side::Peer { stopped: None });
                    survivor.link(&victim).await;
                    (survivor, victim, rx)
                })
            },
            |(survivor, victim, mut rx)| {
                rt.block_on(async {
                    victim.kill();
                    drain(&mut rx, 1).await;
                    black_box(&survivor);
                });
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// One child-death → policy → (zero) backoff → respawn round trip.
fn restart_cycle(c: &mut Criterion) {
    use bombay::actor::SpawnSupervised as _;
    use kameo::actor::Spawn as _;

    silence_bench_crashes();
    let rt = runtime();
    let mut group = c.benchmark_group("supervise_restart_cycle");
    group.throughput(Throughput::Elements(1));

    group.bench_function("bombay", |b| {
        let (tick_tx, mut tick_rx) = unbounded_channel::<()>();
        let slot: core_side::Slot = std::sync::Arc::new(std::sync::Mutex::new(None));
        // Every spawn happens INSIDE the runtime context: `spawn_supervised`
        // calls `tokio::spawn`, which panics outside `block_on`/`enter`.
        let sup = rt.block_on(async {
            let sup = core_side::SupOneForOne::spawn_supervised(());
            sup.supervise(
                core_side::fast_restart(),
                core_side::worker_factory(tick_tx.clone(), slot.clone()),
            )
            .await
            .expect("supervisor alive");
            tick_rx.recv().await.expect("first incarnation starts");
            sup
        });

        b.iter(|| {
            rt.block_on(async {
                let sender = slot.lock().expect("slot").clone().expect("an incarnation");
                sender
                    .try_send_message(core_side::Crash)
                    .expect("current incarnation alive");
                tick_rx
                    .recv()
                    .await
                    .expect("the rebuilt incarnation starts");
            });
        });
        drop(sup);
    });

    group.bench_function("kameo", |b| {
        use kameo_side::Worker;
        let (tick_tx, mut tick_rx) = unbounded_channel::<()>();
        let (sup, child) = rt.block_on(async {
            let sup = kameo_side::SupOneForOne::spawn(kameo_side::SupOneForOne);
            let child = Worker::supervise(
                &sup,
                Worker {
                    tick: tick_tx.clone(),
                },
            )
            .restart_policy(kameo::supervision::RestartPolicy::Permanent)
            .restart_limit(u32::MAX, Duration::from_secs(86_400))
            .spawn()
            .await;
            tick_rx.recv().await.expect("first incarnation starts");
            (sup, child)
        });

        b.iter(|| {
            rt.block_on(async {
                let _ = child.tell(kameo_side::Crash).await;
                tick_rx
                    .recv()
                    .await
                    .expect("the restarted incarnation starts");
            });
        });
        drop(child);
        drop(sup);
    });

    group.finish();
}

/// Crash the eldest child under a set strategy; the whole set rebuilds.
fn set_strategy_coalesce(c: &mut Criterion) {
    use bombay::actor::SpawnSupervised as _;
    use kameo::actor::Spawn as _;

    silence_bench_crashes();
    let rt = runtime();
    let mut group = c.benchmark_group("supervise_set_strategy_coalesce");

    for &n in &SET_SIZES {
        group.throughput(Throughput::Elements(n as u64));

        // bombay: one supervisor, N anchored children; crashing slot[0] cycles the
        // whole set (OneForAll: all; RestForOne: the eldest + all younger = all).
        macro_rules! bombay_arm {
            ($id:literal, $sup:ident) => {
                group.bench_with_input(BenchmarkId::new($id, n), &n, |b, &n| {
                    let (tick_tx, mut tick_rx) = unbounded_channel::<()>();
                    let slots: Vec<core_side::Slot> = (0..n)
                        .map(|_| std::sync::Arc::new(std::sync::Mutex::new(None)))
                        .collect();
                    let sup = rt.block_on(async {
                        let sup = core_side::$sup::spawn_supervised(());
                        for slot in &slots {
                            sup.supervise(
                                core_side::fast_restart(),
                                core_side::worker_factory(tick_tx.clone(), slot.clone()),
                            )
                            .await
                            .expect("supervisor alive");
                        }
                        drain(&mut tick_rx, n).await;
                        sup
                    });

                    b.iter(|| {
                        rt.block_on(async {
                            let sender = slots[0]
                                .lock()
                                .expect("slot")
                                .clone()
                                .expect("eldest alive");
                            sender
                                .try_send_message(core_side::Crash)
                                .expect("eldest incarnation alive");
                            drain(&mut tick_rx, n).await;
                        });
                    });
                    drop(sup);
                });
            };
        }
        bombay_arm!("bombay_one_for_all", SupOneForAll);
        bombay_arm!("bombay_rest_for_one", SupRestForOne);

        macro_rules! kameo_arm {
            ($id:literal, $sup:ident) => {
                group.bench_with_input(BenchmarkId::new($id, n), &n, |b, &n| {
                    use kameo_side::Worker;
                    let (tick_tx, mut tick_rx) = unbounded_channel::<()>();
                    let (sup, children) = rt.block_on(async {
                        let sup = kameo_side::$sup::spawn(kameo_side::$sup);
                        let mut children = Vec::with_capacity(n);
                        for _ in 0..n {
                            children.push(
                                Worker::supervise(
                                    &sup,
                                    Worker {
                                        tick: tick_tx.clone(),
                                    },
                                )
                                .restart_policy(kameo::supervision::RestartPolicy::Permanent)
                                .restart_limit(u32::MAX, Duration::from_secs(86_400))
                                .spawn()
                                .await,
                            );
                        }
                        drain(&mut tick_rx, n).await;
                        (sup, children)
                    });

                    b.iter(|| {
                        rt.block_on(async {
                            let _ = children[0].tell(kameo_side::Crash).await;
                            drain(&mut tick_rx, n).await;
                        });
                    });
                    drop(children);
                    drop(sup);
                });
            };
        }
        kameo_arm!("kameo_one_for_all", SupOneForAll);
        kameo_arm!("kameo_rest_for_one", SupRestForOne);
    }
    group.finish();
}

criterion_group!(
    benches,
    watch_fanout,
    link_teardown,
    restart_cycle,
    set_strategy_coalesce
);
criterion_main!(benches);
