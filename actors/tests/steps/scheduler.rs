//! Shared `Scheduler` World + step definitions for the `actors/scheduler`
//! scenarios (card #78).
//!
//! Wired by the runners that `#[path]`-include this module:
//!   * `scheduler_bdd.rs`       — the example feature (scheduler.feature)
//!   * `scheduler_props_bdd.rs` — the property/model feature (scheduler.properties.feature)
//!
//! The SUT is `bombay_actors::scheduler::Scheduler` (an actor owning a tokio
//! `JoinSet` of timer tasks: `SetTimeout<A, M>` fires one `tell(msg)` to a weak
//! ref after a deadline; `SetInterval<A, T>` fires `tell(msg.clone())` every
//! period; both handlers reply with a tokio `AbortHandle`). It is driven against
//! REAL SPAWNED ACTORS reached through `bombay::prelude::*`. The target is a
//! `Recorder` that counts every `Tick` it handles.
//!
//! # Paused clock for `@timing`
//!
//! Every `@timing` scenario is time-sensitive: it asserts an EXACT count of
//! deliveries at a precise instant. Real sleeps would be flaky, so each `@timing`
//! step drives its Scheduler + target + scheduled work inside a dedicated
//! `tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true)`
//! runtime on a `tokio::task::spawn_blocking` thread (the cucumber runner is
//! `multi_thread`, and `tokio::time::pause`/`advance` require a current-thread
//! runtime). Under `start_paused(true)` the clock only moves when we call
//! `tokio::time::advance` (or every task parks, in which case tokio auto-advances
//! to the next timer). The drivers advance the clock in explicit, bounded steps —
//! yielding between steps so the spawned timer tasks run each elapsed tick — then
//! read the recorded count. Because the SUT's timer tasks are spawned INSIDE this
//! paused runtime, their `sleep_until` / `interval.tick()` see the paused clock,
//! making "exactly N at instant T" deterministic with no flake.
//!
//! `SetTimeout::new` / `SetInterval::new` capture the deadline / interval clock at
//! CONSTRUCTION (scheduler.rs:115/157), so the construction-vs-receipt scenarios
//! build the message at one paused instant, advance the clock to model receipt
//! latency, hand it to the Scheduler, then advance to the deadline.
//!
//! No private Scheduler state is inspected: every assertion is grounded in the
//! OBSERVABLE delivery count on the target (and the returned `AbortHandle`), so no
//! `testing`-gated query is added to the SUT for this module.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bombay::{error::Infallible, prelude::*};
use bombay_actors::scheduler::{Scheduler, SetInterval, SetTimeout};
use cucumber::{World, given, then, when};
use tokio::{task::AbortHandle, time::MissedTickBehavior};

// ===========================================================================
// Target actor
// ===========================================================================

/// The single scheduled value type. `Clone` is required by `SetInterval<A, T>`
/// (it `tell`s `msg.clone()` each tick); `SetTimeout<A, M>` only needs `Send`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Tick;

/// A target actor that records how many `Tick` messages it has received. The
/// counter is an `Arc<AtomicU64>` shared with the driver so the count is readable
/// from outside the paused runtime's actor.
struct Recorder {
    count: Arc<AtomicU64>,
}

impl Actor for Recorder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Tick> for Recorder {
    type Reply = ();

    async fn handle(&mut self, _msg: Tick, _ctx: &mut Context<Self, Self::Reply>) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

/// One tokio "time tick" — the minimum resolution `tokio::time::interval` advances
/// by. tokio's timer wheel granularity is 1ms (see tokio's `time::Duration`
/// driver); a "period equal to one tokio time tick" in the feature is therefore
/// 1ms. Used so the very-short-period scenarios pin an exact tick count.
const ONE_TICK: Duration = Duration::from_millis(1);

// ===========================================================================
// World
// ===========================================================================

/// Scenario configuration collected by `Given`/`When` steps and consumed by the
/// driving `Then` step. The `@timing` scenarios cannot hold a paused runtime
/// across cucumber steps (each step runs on the outer multi_thread runtime), so
/// the config is accumulated here and the whole timed interaction is executed in
/// one driver call from the terminal assertion step.
#[derive(Debug, Default, World)]
pub struct SchedulerWorld {
    /// Deadline for a SetTimeout scenario (from construction).
    timeout_after: Option<Duration>,
    /// Period for a SetInterval scenario.
    interval_period: Option<Duration>,
    /// Optional start_delay applied to the interval.
    interval_start_delay: Option<Duration>,
    /// Missed-tick behaviour for the interval (Burst by default in tokio).
    missed_tick: Option<MissedBehaviour>,
    /// Modelled receipt latency: how long after construction the Scheduler
    /// receives the message (clock advanced before the message is handed over).
    receipt_latency: Option<Duration>,
    /// Result of the last paused-clock driver: deliveries observed.
    observed: Option<u64>,
}

/// A `Copy` mirror of `tokio::time::MissedTickBehavior` so the World stays
/// `Debug + Default`-derivable (the tokio type is `Debug` but we want an explicit
/// default that means "unset").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissedBehaviour {
    Burst,
    Delay,
    Skip,
}

impl From<MissedBehaviour> for MissedTickBehavior {
    fn from(b: MissedBehaviour) -> Self {
        match b {
            MissedBehaviour::Burst => MissedTickBehavior::Burst,
            MissedBehaviour::Delay => MissedTickBehavior::Delay,
            MissedBehaviour::Skip => MissedTickBehavior::Skip,
        }
    }
}

// ===========================================================================
// Paused-clock primitives
// ===========================================================================

/// Builds the dedicated `start_paused(true)` current-thread runtime and runs
/// `body` to completion on a blocking thread, returning its value. Every `@timing`
/// driver funnels through here so the SUT's timer tasks always see the paused
/// clock.
fn on_paused_runtime<T, F>(body: F) -> T
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    // Run on a dedicated OS thread so the `start_paused(true)` current-thread
    // runtime is fully isolated from cucumber's outer multi_thread runtime (a
    // current-thread runtime cannot be built on a thread already driving another
    // runtime). The thread builds the paused runtime, drives `body`, and joins.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .expect("paused current-thread runtime");
                rt.block_on(body)
            })
            .join()
            .expect("paused runtime thread join")
    })
}

/// Advances the paused clock by `total` in `step`-sized increments, yielding to
/// the executor after each increment so any timer task whose deadline was crossed
/// gets to run before the next advance. Returns once the full `total` has elapsed.
async fn advance_in_steps(total: Duration, step: Duration) {
    let step = step.max(ONE_TICK);
    let mut remaining = total;
    while remaining >= step {
        tokio::time::advance(step).await;
        tokio::task::yield_now().await;
        remaining -= step;
    }
    if !remaining.is_zero() {
        tokio::time::advance(remaining).await;
        tokio::task::yield_now().await;
    }
    // A final settle pass: give just-woken tasks a couple of scheduler turns to
    // run their `tell` to completion before the caller reads the count.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// Liveness probe: a still-running Scheduler answers a fresh `SetTimeout` ask
/// (handler runs, spawns a task, replies with an `AbortHandle`). A panicked /
/// stopped run-loop would make the ask fail with `ActorNotRunning`. The target is
/// a throwaway Recorder kept alive only for the duration of the ask.
async fn scheduler_alive(scheduler: &ActorRef<Scheduler>) -> bool {
    let probe = Recorder::spawn(Recorder {
        count: Arc::new(AtomicU64::new(0)),
    });
    probe.wait_for_startup().await;
    let ok = scheduler
        .ask(SetTimeout::new(
            probe.downgrade(),
            Duration::from_secs(3600),
            Tick,
        ))
        .await
        .is_ok();
    probe.kill();
    ok
}

/// Spawns a fresh Scheduler + Recorder inside the (already current-thread, paused)
/// runtime and returns `(scheduler_ref, recorder_ref, count)`.
async fn fresh() -> (ActorRef<Scheduler>, ActorRef<Recorder>, Arc<AtomicU64>) {
    let scheduler = Scheduler::spawn(Scheduler::new());
    scheduler.wait_for_startup().await;
    let count = Arc::new(AtomicU64::new(0));
    let recorder = Recorder::spawn(Recorder {
        count: Arc::clone(&count),
    });
    recorder.wait_for_startup().await;
    (scheduler, recorder, count)
}

// ===========================================================================
// Paused-clock drivers (each returns observed deliveries)
// ===========================================================================

/// Drives one SetTimeout: construct at t=0, advance the clock by `receipt` to
/// model delivery latency to the Scheduler, hand it over, then advance the
/// remaining time so that `advance_total` has elapsed SINCE CONSTRUCTION. Returns
/// the number of `Tick`s the target received.
fn drive_timeout(deadline: Duration, receipt: Duration, advance_total: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        // Construct at t=0 (deadline captured here = now + `deadline`).
        let msg = SetTimeout::new(recorder.downgrade(), deadline, Tick);
        // Model receipt latency before the Scheduler handles the message.
        if !receipt.is_zero() {
            tokio::time::advance(receipt).await;
        }
        let _abort: AbortHandle = scheduler.ask(msg).await.expect("scheduler ask");
        // Advance so that `advance_total` has elapsed since construction.
        let already = receipt;
        let rest = advance_total.saturating_sub(already);
        advance_in_steps(rest, ONE_TICK).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Drives a SetTimeout whose target is dropped before the deadline elapses; the
/// task must wake, fail `upgrade()`, and stay silent. Returns the count (must be
/// 0) plus whether the Scheduler is still alive afterwards (no panic).
fn drive_timeout_dropped_target(deadline: Duration, advance_total: Duration) -> (u64, bool) {
    on_paused_runtime(async move {
        let scheduler = Scheduler::spawn(Scheduler::new());
        scheduler.wait_for_startup().await;
        let count = Arc::new(AtomicU64::new(0));
        let recorder = Recorder::spawn(Recorder {
            count: Arc::clone(&count),
        });
        recorder.wait_for_startup().await;
        let weak = recorder.downgrade();
        // Drop the only strong ref BEFORE constructing/handing over the timeout.
        recorder.kill();
        recorder.wait_for_shutdown().await;
        drop(recorder);
        let msg = SetTimeout::new(weak, deadline, Tick);
        let _abort: AbortHandle = scheduler.ask(msg).await.expect("scheduler ask");
        advance_in_steps(advance_total, ONE_TICK).await;
        let alive = scheduler_alive(&scheduler).await;
        scheduler.kill();
        (count.load(Ordering::SeqCst), alive)
    })
}

/// Drives a SetTimeout, retains its AbortHandle, aborts it BEFORE the deadline,
/// then advances past the deadline. Returns deliveries (must be 0).
fn drive_timeout_abort(deadline: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let abort: AbortHandle = scheduler
            .ask(SetTimeout::new(recorder.downgrade(), deadline, Tick))
            .await
            .expect("scheduler ask");
        // Abort synchronously, before advancing the clock to the deadline.
        abort.abort();
        advance_in_steps(deadline + Duration::from_millis(500), ONE_TICK).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Configuration for an interval driver run.
#[derive(Clone, Copy)]
struct IntervalRun {
    period: Duration,
    start_delay: Option<Duration>,
    missed: MissedTickBehavior,
    receipt: Duration,
    advance_total: Duration,
}

/// Drives one SetInterval and returns deliveries after advancing `advance_total`
/// since construction. The clock is advanced in `period`-sized steps so each tick
/// instant is reached exactly once (this is what makes Burst vs Delay vs Skip
/// counts deterministic).
fn drive_interval(run: IntervalRun) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let mut msg = SetInterval::new(recorder.downgrade(), run.period, Tick)
            .set_missed_tick_behaviour(run.missed);
        if let Some(delay) = run.start_delay {
            msg = msg.start_delay(delay);
        }
        if !run.receipt.is_zero() {
            tokio::time::advance(run.receipt).await;
        }
        let _abort: AbortHandle = scheduler.ask(msg).await.expect("scheduler ask");
        let rest = run.advance_total.saturating_sub(run.receipt);
        advance_in_steps(rest, run.period).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Drives a SetInterval whose target becomes unavailable (dropped or stopped)
/// after firing at least once, then advances further. Returns
/// `(count_after_target_gone_minus_baseline, scheduler_alive)`.
fn drive_interval_target_gone(period: Duration, drop_strong: bool) -> (u64, bool) {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let _abort: AbortHandle = scheduler
            .ask(SetInterval::new(recorder.downgrade(), period, Tick))
            .await
            .expect("scheduler ask");
        // Let the immediate first tick (t=0) fire.
        advance_in_steps(period, period).await;
        let baseline = count.load(Ordering::SeqCst);
        // Both modes stop the actor; the difference is whether a strong ref
        // survives. dropped ⇒ the interval loop's `upgrade()` returns None; kept ⇒
        // `upgrade()` succeeds but the `tell` returns ActorNotRunning/ActorStopped.
        recorder.kill();
        recorder.wait_for_shutdown().await;
        if drop_strong {
            drop(recorder);
        }
        // Advance several further periods; the interval task must self-terminate.
        advance_in_steps(period * 5, period).await;
        let after = count.load(Ordering::SeqCst);
        let alive = scheduler_alive(&scheduler).await;
        scheduler.kill();
        (after.saturating_sub(baseline), alive)
    })
}

/// Drives `n` concurrent SetTimeouts (each `deadline` from construction) asked
/// from multiple tasks, then advances past the deadline. Returns deliveries.
fn drive_concurrent_timeouts(n: usize, deadline: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(n));
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let scheduler = scheduler.clone();
            let weak = recorder.downgrade();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let msg = SetTimeout::new(weak, deadline, Tick);
                barrier.wait().await;
                let _abort: AbortHandle = scheduler.ask(msg).await.expect("ask");
            }));
        }
        for h in handles {
            h.await.expect("timeout-ask task join");
        }
        advance_in_steps(deadline + ONE_TICK, ONE_TICK).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Drives one SetTimeout + one SetInterval (both `period`) concurrently, advancing
/// `advance_total` in `period` steps. Returns deliveries (timeout=1 + interval
/// ticks).
fn drive_timeout_and_interval(period: Duration, advance_total: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let sched_t = scheduler.clone();
        let weak_t = recorder.downgrade();
        let bar_t = Arc::clone(&barrier);
        let to = tokio::spawn(async move {
            let msg = SetTimeout::new(weak_t, period, Tick);
            bar_t.wait().await;
            let _abort: AbortHandle = sched_t.ask(msg).await.expect("ask timeout");
        });
        let sched_i = scheduler.clone();
        let weak_i = recorder.downgrade();
        let bar_i = Arc::clone(&barrier);
        let iv = tokio::spawn(async move {
            let msg = SetInterval::new(weak_i, period, Tick)
                .set_missed_tick_behaviour(MissedTickBehavior::Delay);
            bar_i.wait().await;
            let _abort: AbortHandle = sched_i.ask(msg).await.expect("ask interval");
        });
        to.await.expect("timeout task");
        iv.await.expect("interval task");
        advance_in_steps(advance_total, period).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Returns whether asking `msg_kind` to a Scheduler yields a usable AbortHandle
/// whose task can be aborted (independent of whether/when it fires). Run under the
/// paused runtime so a ZERO-deadline timeout's task does not race the assertion.
fn returns_abort_handle(deadline: Duration, is_interval: bool) -> bool {
    on_paused_runtime(async move {
        let (scheduler, recorder, _count) = fresh().await;
        let handle: AbortHandle = if is_interval {
            scheduler
                .ask(SetInterval::new(recorder.downgrade(), deadline, Tick))
                .await
                .expect("ask interval")
        } else {
            scheduler
                .ask(SetTimeout::new(recorder.downgrade(), deadline, Tick))
                .await
                .expect("ask timeout")
        };
        // A usable handle: aborting it is well-defined and idempotent.
        handle.abort();
        recorder.kill();
        scheduler.kill();
        true
    })
}

// ===========================================================================
// Oracles for the property/model laws
// ===========================================================================

/// One-shot model: a SetTimeout fires exactly once at the first tick >= deadline,
/// and zero times if aborted before the deadline. The expected delivery count by
/// the time `elapsed_since_construction` has passed (no abort) is 1 iff that time
/// has reached the deadline.
fn oracle_timeout(deadline: Duration, elapsed: Duration) -> u64 {
    u64::from(elapsed >= deadline)
}

/// Tick-instant model under MissedTickBehavior::Delay: ticks fire at instants
/// `start + i*period` (i = 0, 1, ...) with the first at `start` (or `start +
/// start_delay`). With the clock advanced in period steps to `start + s + k*p`,
/// the reached-tick count is `k + 1` (the immediate tick plus k subsequent ones)
/// once `start_delay` has been passed. With NO start_delay the first tick is at
/// t=0; with a start_delay `s` the first tick is at t=s.
fn oracle_interval_ticks(
    period: Duration,
    start_delay: Option<Duration>,
    advance_total: Duration,
) -> u64 {
    let first = start_delay.unwrap_or(Duration::ZERO);
    if advance_total < first {
        return 0;
    }
    let after_first = advance_total - first;
    // ticks at first, first+p, first+2p, ... that are <= advance_total:
    // 1 (the first) + floor(after_first / period).
    let nanos = period.as_nanos();
    if nanos == 0 {
        return 1;
    }
    1 + u64::try_from(after_first.as_nanos() / nanos).unwrap_or(u64::MAX)
}

// ===========================================================================
// Given — Scheduler, target, scenario configuration
// ===========================================================================

#[given(regex = r#"^a running Scheduler actor$"#)]
async fn given_scheduler(_world: &mut SchedulerWorld) {
    // The Scheduler is (re)spawned inside each paused-clock driver so its timer
    // tasks see the paused clock; this Background step is a no-op placeholder so
    // every scenario's Given chain is satisfied. The non-@timing AbortHandle
    // scenarios likewise spawn their own Scheduler inside `returns_abort_handle`.
}

#[given(regex = r#"^a target actor that records each message it receives with a timestamp$"#)]
async fn given_target(_world: &mut SchedulerWorld) {
    // Placeholder: the target Recorder is spawned inside each driver (it must live
    // on the same paused runtime as the Scheduler).
}

#[given(regex = r#"^a SetTimeout scheduled for (\d+)ms from now against the target$"#)]
async fn given_timeout_ms(world: &mut SchedulerWorld, ms: u64) {
    world.timeout_after = Some(Duration::from_millis(ms));
}

#[given(regex = r#"^a SetTimeout constructed with Duration::ZERO against the target$"#)]
async fn given_timeout_zero(world: &mut SchedulerWorld) {
    world.timeout_after = Some(Duration::ZERO);
}

#[given(regex = r#"^a SetInterval with period (\d+)ms against the target$"#)]
async fn given_interval_ms(world: &mut SchedulerWorld, ms: u64) {
    world.interval_period = Some(Duration::from_millis(ms));
}

#[given(
    regex = r#"^a SetInterval with period (\d+)ms and a start_delay of (\d+)ms against the target$"#
)]
async fn given_interval_start_delay(world: &mut SchedulerWorld, ms: u64, delay: u64) {
    world.interval_period = Some(Duration::from_millis(ms));
    world.interval_start_delay = Some(Duration::from_millis(delay));
}

#[given(regex = r#"^a SetInterval with period equal to one tokio time tick against the target$"#)]
async fn given_interval_one_tick(world: &mut SchedulerWorld) {
    world.interval_period = Some(ONE_TICK);
}

#[given(regex = r#"^its MissedTickBehavior is set to (Burst|Delay|Skip)$"#)]
async fn given_missed_behaviour(world: &mut SchedulerWorld, behaviour: String) {
    world.missed_tick = Some(match behaviour.as_str() {
        "Burst" => MissedBehaviour::Burst,
        "Delay" => MissedBehaviour::Delay,
        "Skip" => MissedBehaviour::Skip,
        other => panic!("unknown missed-tick behaviour {other:?}"),
    });
}

#[given(
    regex = r#"^the Scheduler is kept busy so it receives the SetTimeout (\d+)ms after construction$"#
)]
async fn given_receipt_latency(world: &mut SchedulerWorld, ms: u64) {
    world.receipt_latency = Some(Duration::from_millis(ms));
}

#[given(regex = r#"^construction happens (\d+)ms before the Scheduler receives the message$"#)]
async fn given_receipt_latency_interval(world: &mut SchedulerWorld, ms: u64) {
    world.receipt_latency = Some(Duration::from_millis(ms));
}

#[given(regex = r#"^the target actor has already been dropped before the deadline elapses$"#)]
async fn given_target_predropped(_world: &mut SchedulerWorld) {
    // The drop happens inside `drive_timeout_dropped_target`, driven from the Then
    // step; nothing to record here beyond the timeout config that follows.
}

#[given(regex = r#"^a SetTimeout scheduled for (\d+)ms from now against that target$"#)]
async fn given_timeout_against_dropped(world: &mut SchedulerWorld, ms: u64) {
    world.timeout_after = Some(Duration::from_millis(ms));
}

#[given(regex = r#"^its AbortHandle has been retained$"#)]
async fn given_abort_retained(_world: &mut SchedulerWorld) {
    // The handle is retained inside the abort driver (it must be aborted on the
    // same paused runtime that owns the task).
}

#[given(regex = r#"^the interval has already fired at least once$"#)]
async fn given_interval_fired_once(_world: &mut SchedulerWorld) {
    // The abort-after-firing driver advances one period before aborting.
}

#[given(
    regex = r#"^(\d+) SetTimeout messages each scheduled for (\d+)ms from now against the target$"#
)]
async fn given_n_timeouts(world: &mut SchedulerWorld, _n: u32, ms: u64) {
    // The concurrent drive (50 fixed by the @linearizability scenario) runs in the
    // Then step; record only the per-timeout deadline here.
    world.timeout_after = Some(Duration::from_millis(ms));
}

#[given(regex = r#"^a SetTimeout for (\d+)ms and a SetInterval of (\d+)ms against the target$"#)]
async fn given_timeout_and_interval(world: &mut SchedulerWorld, to_ms: u64, iv_ms: u64) {
    world.timeout_after = Some(Duration::from_millis(to_ms));
    world.interval_period = Some(Duration::from_millis(iv_ms));
}

// ===========================================================================
// When — clock advances, hand-overs (most timed work runs in the Then driver)
// ===========================================================================

#[when(regex = r#"^the paused clock is advanced past 100ms and held for several further ticks$"#)]
async fn when_advance_past_100(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    world.observed = Some(drive_timeout(
        deadline,
        Duration::ZERO,
        deadline + ONE_TICK * 10,
    ));
}

#[when(regex = r#"^the paused clock is advanced by only 50ms$"#)]
async fn when_advance_50(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    world.observed = Some(drive_timeout(
        deadline,
        Duration::ZERO,
        Duration::from_millis(50),
    ));
}

#[when(regex = r#"^the SetTimeout message is asked to the Scheduler$"#)]
async fn when_ask_timeout(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    world.observed = Some(u64::from(returns_abort_handle(deadline, false)));
}

#[when(regex = r#"^the SetTimeout is asked to the Scheduler$"#)]
async fn when_ask_timeout_alt(world: &mut SchedulerWorld) {
    when_ask_timeout(world).await;
}

#[when(regex = r#"^the SetInterval message is asked to the Scheduler$"#)]
async fn when_ask_interval(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    world.observed = Some(u64::from(returns_abort_handle(period, true)));
}

#[when(regex = r#"^the paused clock is advanced by 500ms in 100ms steps$"#)]
async fn when_advance_500_steps(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    world.observed = Some(drive_interval(IntervalRun {
        period,
        start_delay: world.interval_start_delay,
        missed: world
            .missed_tick
            .map_or(MissedTickBehavior::Delay, Into::into),
        receipt: Duration::ZERO,
        advance_total: Duration::from_millis(500),
    }));
}

#[when(
    regex = r#"^the Scheduler is kept busy so it receives the SetTimeout 80ms after construction$"#
)]
async fn when_busy_receipt(_world: &mut SchedulerWorld) {
    // Alias handled by the matching @lifecycle Given; nothing to do here.
}

#[when(
    regex = r#"^the paused clock is advanced so that 100ms total have elapsed since construction$"#
)]
async fn when_advance_total_100(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    let receipt = world.receipt_latency.unwrap_or(Duration::ZERO);
    world.observed = Some(drive_timeout(deadline, receipt, Duration::from_millis(100)));
}

#[when(regex = r#"^the paused clock is advanced$"#)]
async fn when_advance_interval_anchored(world: &mut SchedulerWorld) {
    // @lifecycle "interval period measured from construction": construction is
    // `receipt` ms before receipt; under Burst (tokio default) the catch-up ticks
    // for instants already past at receipt fire at once. Advance to a known total.
    let period = world.interval_period.expect("interval configured");
    let receipt = world.receipt_latency.unwrap_or(Duration::ZERO);
    world.observed = Some(drive_interval(IntervalRun {
        period,
        start_delay: world.interval_start_delay,
        missed: MissedTickBehavior::Burst,
        receipt,
        advance_total: receipt + period,
    }));
}

#[when(regex = r#"^the target actor is dropped \(its strong refs released\)$"#)]
async fn when_target_dropped(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    let (after, alive) = drive_interval_target_gone(period, true);
    world.observed = Some(after);
    assert!(
        alive,
        "the Scheduler must not panic when an interval target drops"
    );
}

#[when(regex = r#"^the paused clock is advanced by several further periods$"#)]
async fn when_advance_further_periods(_world: &mut SchedulerWorld) {
    // The further advance happens inside the target-gone driver above; the
    // observed delta is already stored. No-op.
}

#[when(regex = r#"^the target actor is stopped while a strong ref to it still exists$"#)]
async fn when_target_stopped(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    let (after, alive) = drive_interval_target_gone(period, false);
    world.observed = Some(after);
    assert!(
        alive,
        "the Scheduler must not panic when an interval target stops"
    );
}

#[when(regex = r#"^the paused clock is advanced by another period$"#)]
async fn when_advance_another_period(_world: &mut SchedulerWorld) {
    // Handled inside drive_interval_target_gone.
}

#[when(regex = r#"^the paused clock is advanced past 100ms$"#)]
async fn when_advance_past_100_dropped(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    let (count, alive) = drive_timeout_dropped_target(deadline, deadline + ONE_TICK * 10);
    world.observed = Some(count);
    assert!(
        alive,
        "the Scheduler must not panic on a dropped-target timeout"
    );
}

#[when(regex = r#"^the AbortHandle is aborted before the deadline elapses$"#)]
async fn when_abort_before_deadline(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.expect("timeout configured");
    world.observed = Some(drive_timeout_abort(deadline));
}

#[when(regex = r#"^time then advances past the original deadline$"#)]
async fn when_time_advances_past_deadline(_world: &mut SchedulerWorld) {
    // The advance is inside drive_timeout_abort.
}

#[when(regex = r#"^the AbortHandle is aborted$"#)]
async fn when_abort_interval(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    world.observed = Some(drive_interval_abort(period));
}

#[when(regex = r#"^time then advances by several further periods$"#)]
async fn when_time_advances_further(_world: &mut SchedulerWorld) {
    // Inside drive_interval_abort.
}

#[when(regex = r#"^the paused clock is advanced by 200ms$"#)]
async fn when_advance_200(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    world.observed = Some(drive_interval(IntervalRun {
        period,
        start_delay: world.interval_start_delay,
        missed: MissedTickBehavior::Delay,
        receipt: Duration::ZERO,
        advance_total: Duration::from_millis(200),
    }));
}

#[when(regex = r#"^the paused clock is advanced past 250ms$"#)]
async fn when_advance_past_250(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    world.observed = Some(drive_interval(IntervalRun {
        period,
        start_delay: world.interval_start_delay,
        missed: MissedTickBehavior::Delay,
        receipt: Duration::ZERO,
        advance_total: Duration::from_millis(300),
    }));
}

#[when(regex = r#"^the Scheduler receives it and the paused clock is advanced minimally$"#)]
async fn when_zero_advance_minimal(world: &mut SchedulerWorld) {
    world.observed = Some(drive_timeout(Duration::ZERO, Duration::ZERO, ONE_TICK * 4));
}

#[when(regex = r#"^the paused clock is advanced far in one jump$"#)]
async fn when_advance_far_burst(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    // 10 periods in a single jump; Burst replays every missed tick.
    world.observed = Some(drive_interval_single_jump(
        period,
        MissedTickBehavior::Burst,
        period * 10,
    ));
}

#[when(regex = r#"^the paused clock jumps forward by 500ms in a single advance$"#)]
async fn when_jump_500(world: &mut SchedulerWorld) {
    let period = world.interval_period.expect("interval configured");
    let missed = world
        .missed_tick
        .map_or(MissedTickBehavior::Delay, Into::into);
    world.observed = Some(drive_interval_single_jump(
        period,
        missed,
        Duration::from_millis(500),
    ));
}

#[when(regex = r#"^all 50 are asked to the Scheduler concurrently from multiple tasks$"#)]
async fn when_50_concurrent(_world: &mut SchedulerWorld) {
    // Driven in the Then step (needs the deadline + the advance together).
}

#[when(regex = r#"^both are asked to the Scheduler concurrently$"#)]
async fn when_both_concurrent(_world: &mut SchedulerWorld) {}

#[when(regex = r#"^both are running and the timeout completes and is joined by the Scheduler$"#)]
async fn when_both_running_joined(_world: &mut SchedulerWorld) {}

#[when(regex = r#"^the paused clock is advanced by 300ms in 100ms steps$"#)]
async fn when_advance_300_steps(_world: &mut SchedulerWorld) {
    // The mixed timeout+interval drive runs in `then_timeout_contributes_one`
    // (it needs both schedules and the advance together).
}

// ===========================================================================
// Single-jump interval driver + interval abort driver
// ===========================================================================

/// Drives a SetInterval, advances `total` in ONE jump (no intermediate yields
/// between sub-steps), then settles. This is the Burst/Delay/Skip distinguisher:
/// a single jump past many tick instants lets the chosen MissedTickBehavior decide
/// how many catch-up ticks fire. Returns deliveries.
fn drive_interval_single_jump(
    period: Duration,
    missed: MissedTickBehavior,
    total: Duration,
) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let _abort: AbortHandle = scheduler
            .ask(
                SetInterval::new(recorder.downgrade(), period, Tick)
                    .set_missed_tick_behaviour(missed),
            )
            .await
            .expect("scheduler ask");
        // Let the immediate first tick (t=0) fire.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        // One jump past many tick instants.
        tokio::time::advance(total).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}

/// Drives a SetInterval, lets it fire once, aborts it, then advances further.
/// Returns the post-abort delivery delta (must be 0).
fn drive_interval_abort(period: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let abort: AbortHandle = scheduler
            .ask(SetInterval::new(recorder.downgrade(), period, Tick))
            .await
            .expect("scheduler ask");
        advance_in_steps(period, period).await;
        let baseline = count.load(Ordering::SeqCst);
        assert!(
            baseline >= 1,
            "the interval must fire at least once before abort"
        );
        abort.abort();
        advance_in_steps(period * 5, period).await;
        let after = count.load(Ordering::SeqCst);
        recorder.kill();
        scheduler.kill();
        after.saturating_sub(baseline)
    })
}

// ===========================================================================
// Then — assertions on observed deliveries / handles
// ===========================================================================

#[then(regex = r#"^the target receives the message exactly once$"#)]
async fn then_exactly_once(world: &mut SchedulerWorld) {
    let n = world.observed.expect("a timed driver ran");
    assert_eq!(
        n, 1,
        "SetTimeout must deliver exactly once after the deadline"
    );
}

#[then(regex = r#"^the target has received 0 messages$"#)]
async fn then_zero_messages(world: &mut SchedulerWorld) {
    let n = world.observed.expect("a timed driver ran");
    assert_eq!(
        n, 0,
        "no delivery may occur before the deadline / start_delay"
    );
}

#[then(
    regex = r#"^the reply is a tokio AbortHandle referencing the spawned (?:timer|interval) task$"#
)]
async fn then_reply_is_abort_handle(world: &mut SchedulerWorld) {
    let ok = world.observed.expect("an ask driver ran");
    assert_eq!(ok, 1, "the handler must reply with a usable AbortHandle");
}

#[then(regex = r#"^the target receives the message once per elapsed period$"#)]
async fn then_once_per_period(world: &mut SchedulerWorld) {
    // Under Delay, advancing 500ms in 100ms steps reaches tick instants at
    // 0,100,200,300,400,500 ⇒ 6 deliveries (first tick immediate + 5).
    let n = world.observed.expect("a timed driver ran");
    let expected =
        oracle_interval_ticks(Duration::from_millis(100), None, Duration::from_millis(500));
    assert_eq!(
        n, expected,
        "interval over 500ms/100ms must deliver {expected}"
    );
    assert_eq!(expected, 6, "tokio first-tick-immediate ⇒ 1 + 500/100 = 6");
}

#[then(regex = r#"^the message fires once at ~100ms after construction, not ~180ms$"#)]
async fn then_fires_at_construction_deadline(world: &mut SchedulerWorld) {
    // Construction at t=0 with an 80ms receipt latency; advancing to t=100ms total
    // (deadline) fires exactly once — receipt latency did NOT push it to 180ms.
    let n = world.observed.expect("a timed driver ran");
    assert_eq!(
        n, 1,
        "deadline is measured from construction (100ms), not receipt"
    );
}

#[then(regex = r#"^the interval's tick schedule is anchored to construction time$"#)]
async fn then_interval_anchored(world: &mut SchedulerWorld) {
    // Period 100ms, constructed 200ms before receipt, advanced to receipt+period
    // (300ms total since construction) under Burst: tick instants at 0,100,200
    // are already past at receipt and burst at once, then 300 ⇒ 4 deliveries. The
    // schedule being anchored to construction is what produces the catch-up burst.
    let n = world.observed.expect("a timed driver ran");
    let expected =
        oracle_interval_ticks(Duration::from_millis(100), None, Duration::from_millis(300));
    assert_eq!(
        n, expected,
        "anchored-at-construction interval must have reached {expected} tick instants by 300ms"
    );
    assert_eq!(expected, 4, "instants 0,100,200,300 reached ⇒ 4");
}

#[then(regex = r#"^the interval task exits on the next tick without panicking or erroring$"#)]
async fn then_interval_exits_clean(world: &mut SchedulerWorld) {
    if let Some(delta) = world.observed {
        // @lifecycle example path: a single drive populated `observed`.
        assert_eq!(
            delta, 0,
            "no delivery may occur after the target is unavailable"
        );
        return;
    }
    // @model @lifecycle law path: no example When ran (the law's Given/When lines
    // are no-op scaffolding). Drive the GEN boundary loop here — p ∈ {1 tick, 1ms,
    // 100ms} × termination-mode {dropped, stopped-but-referenced}. The ORACLE is a
    // 2-state model: 0 deliveries after the target becomes unavailable, Scheduler
    // stays alive (no panic). Independent of the SUT's internals.
    let periods = [
        ONE_TICK,
        Duration::from_millis(1),
        Duration::from_millis(100),
    ];
    for p in periods {
        for drop_strong in [true, false] {
            let (delta, alive) = drive_interval_target_gone(p, drop_strong);
            assert_eq!(
                delta, 0,
                "clean-termination law: p={p:?} drop_strong={drop_strong} \
                 must deliver 0 after the target is gone"
            );
            assert!(
                alive,
                "clean-termination law: the Scheduler must not panic (p={p:?})"
            );
        }
    }
    world.observed = Some(0);
}

#[then(regex = r#"^no further messages are attempted after the target is gone$"#)]
async fn then_no_further_after_gone(world: &mut SchedulerWorld) {
    let delta = world.observed.expect("a target-gone driver ran");
    assert_eq!(delta, 0, "the interval must stop attempting deliveries");
}

#[then(
    regex = r#"^the interval task returns on the SendError::ActorNotRunning or ActorStopped result$"#
)]
async fn then_interval_returns_on_senderror(world: &mut SchedulerWorld) {
    let delta = world.observed.expect("a target-gone driver ran");
    assert_eq!(
        delta, 0,
        "interval terminates on ActorNotRunning/ActorStopped"
    );
}

#[then(regex = r#"^the target receives no further messages$"#)]
async fn then_no_further_messages(world: &mut SchedulerWorld) {
    let delta = world.observed.expect("a target-gone driver ran");
    assert_eq!(delta, 0, "no further deliveries after the target stops");
}

#[then(regex = r#"^the timer task completes without delivering a message and without panicking$"#)]
async fn then_timer_silent(world: &mut SchedulerWorld) {
    let n = world.observed.expect("a dropped-target timeout driver ran");
    assert_eq!(n, 0, "upgrade() == None ⇒ the timeout delivers nothing");
}

#[then(regex = r#"^the target never receives the message$"#)]
async fn then_never_receives(world: &mut SchedulerWorld) {
    let n = world.observed.expect("an abort driver ran");
    assert_eq!(
        n, 0,
        "aborting before the deadline prevents the only delivery"
    );
}

#[then(regex = r#"^the target receives no messages after the abort$"#)]
async fn then_no_messages_after_abort(world: &mut SchedulerWorld) {
    let delta = world.observed.expect("an interval-abort driver ran");
    assert_eq!(delta, 0, "no interval deliveries occur after the abort");
}

#[then(regex = r#"^the target begins receiving messages$"#)]
async fn then_begins_receiving(world: &mut SchedulerWorld) {
    // After advancing past the 250ms start_delay (to 300ms total) with period
    // 100ms under Delay: first tick at 250, next reachable at 350 (>300) ⇒ exactly
    // 1 delivery. "Begins receiving" is pinned to that exact first delivery.
    let n = world.observed.expect("a timed driver ran");
    assert_eq!(
        n, 1,
        "start_delay 250ms ⇒ first (and only, by 300ms) delivery at 250ms"
    );
}

#[then(regex = r#"^the target receives the message at the earliest opportunity$"#)]
async fn then_earliest_opportunity(world: &mut SchedulerWorld) {
    let n = world.observed.expect("a zero-deadline driver ran");
    assert_eq!(
        n, 1,
        "Duration::ZERO ⇒ the timeout fires immediately, exactly once"
    );
}

#[then(
    regex = r#"^the target receives one message per missed period \(the ticks burst to catch up\)$"#
)]
async fn then_burst_catch_up(world: &mut SchedulerWorld) {
    // Period 1ms, advanced 10ms in one jump under Burst: instants 0..=10 ⇒ 11
    // deliveries (the immediate first tick + 10 caught-up).
    let n = world.observed.expect("a single-jump driver ran");
    assert_eq!(
        n, 11,
        "Burst replays every missed tick: 1 + 10 over a 10-tick jump ⇒ 11"
    );
}

#[then(regex = r#"^the target receives a single catch-up message, not five$"#)]
async fn then_delay_single_catch_up(world: &mut SchedulerWorld) {
    // Period 100ms, single 500ms jump under Delay: the immediate first tick (t=0)
    // plus exactly ONE catch-up tick fire (Delay collapses the missed ticks into
    // one), ⇒ 2 total deliveries. The feature contrasts this with Burst's five.
    let n = world.observed.expect("a single-jump driver ran");
    assert_eq!(
        n, 2,
        "Delay collapses missed ticks: first tick + one catch-up ⇒ 2, not 6"
    );
}

#[then(
    regex = r#"^the target receives a single message and the next tick realigns to the period schedule$"#
)]
async fn then_skip_realigns(world: &mut SchedulerWorld) {
    // Period 100ms, single 500ms jump under Skip: like Delay, the immediate first
    // tick plus exactly one realigned catch-up tick ⇒ 2 deliveries.
    let n = world.observed.expect("a single-jump driver ran");
    assert_eq!(
        n, 2,
        "Skip drops missed ticks and realigns: first tick + one ⇒ 2"
    );
}

#[then(regex = r#"^a tokio AbortHandle is returned even though the deadline is already past$"#)]
async fn then_zero_returns_handle(world: &mut SchedulerWorld) {
    let ok = world.observed.expect("an ask driver ran");
    assert_eq!(
        ok, 1,
        "ZERO-deadline timeout still replies with a usable AbortHandle"
    );
}

// --- @linearizability example scenarios (driven from the Then step) ---------

#[then(regex = r#"^the target receives exactly 50 messages$"#)]
async fn then_50_messages(world: &mut SchedulerWorld) {
    let deadline = world.timeout_after.unwrap_or(Duration::from_millis(100));
    let n = drive_concurrent_timeouts(50, deadline);
    world.observed = Some(n);
    assert_eq!(
        n, 50,
        "all 50 independent SetTimeout tasks must deliver exactly once each"
    );
}

#[then(regex = r#"^no scheduled timeout is dropped or duplicated$"#)]
async fn then_no_drop_or_dup(world: &mut SchedulerWorld) {
    let n = world.observed.expect("the 50-timeout driver ran");
    assert_eq!(n, 50, "exactly 50: no timeout dropped, none duplicated");
}

#[then(regex = r#"^the timeout contributes exactly one message$"#)]
async fn then_timeout_contributes_one(world: &mut SchedulerWorld) {
    // One SetTimeout(100ms) + one SetInterval(100ms, Delay), advanced 300ms in
    // 100ms steps: timeout = 1; interval reaches instants 0,100,200,300 ⇒ 4;
    // total = 5. Stored for the follow-up Then steps.
    let n = drive_timeout_and_interval(Duration::from_millis(100), Duration::from_millis(300));
    world.observed = Some(n);
    assert_eq!(
        n, 5,
        "timeout(1) + interval(4 ticks over 0..300ms under Delay) = 5"
    );
}

#[then(regex = r#"^the interval contributes one message per elapsed period$"#)]
async fn then_interval_contributes(world: &mut SchedulerWorld) {
    let n = world.observed.expect("the mixed driver ran");
    assert_eq!(n, 5, "the interval's 4 ticks + the timeout's 1 = 5 total");
}

#[then(
    regex = r#"^messages from the two schedules are never lost or attributed to the wrong source$"#
)]
async fn then_no_cross_attribution(world: &mut SchedulerWorld) {
    let n = world.observed.expect("the mixed driver ran");
    assert_eq!(
        n, 5,
        "total deliveries equal the sum of the two independent schedules"
    );
}

#[then(regex = r#"^the interval continues to deliver on schedule after the timeout was drained$"#)]
async fn then_interval_survives_drain(_world: &mut SchedulerWorld) {
    // One SetTimeout(100ms) + one SetInterval(100ms, Delay): advance well past the
    // timeout so it completes and is joined by Scheduler::next(), confirming the
    // interval keeps ticking. Over 0..500ms: timeout=1, interval=6 (0..=500) ⇒ 7.
    let n = drive_timeout_and_interval(Duration::from_millis(100), Duration::from_millis(500));
    assert_eq!(
        n, 7,
        "after the drained timeout (1), the interval still delivers its 6 ticks (0..=500ms) ⇒ 7"
    );
}

// ===========================================================================
// @property / @model laws (scheduler.properties.feature)
// ===========================================================================
//
// Each law's assertion lives in a dedicated `Then` step that drives the SUT over
// the `# GEN:` boundary set and compares against an INDEPENDENT oracle
// (`oracle_timeout` / `oracle_interval_ticks` — derived from tokio's documented
// timer semantics, NOT from the SUT). Because the timed laws need a paused clock,
// each case runs through the paused-runtime drivers; this is a DOCUMENTED bounded
// boundary-loop over the GEN-named values (a sync `proptest!` cannot `block_on`
// inside cucumber's runtime, and the timing laws need a deterministic clock — see
// the README's Phase-3 §4 fallback). The scenarios' Given/When lines are
// descriptive scaffolding bound to no-ops; the Then carries every assertion.

#[given(
    regex = r#"^a SetTimeout scheduled for any duration d from construction against the target$"#
)]
async fn given_law_any_timeout(_world: &mut SchedulerWorld) {}

#[given(
    regex = r#"^a SetInterval of any period p, optionally rebased by any start_delay s, against the target$"#
)]
async fn given_law_any_interval(_world: &mut SchedulerWorld) {}

#[given(regex = r#"^any SetTimeout duration d or SetInterval period p$"#)]
async fn given_law_any_d_or_p(_world: &mut SchedulerWorld) {}

#[given(regex = r#"^a SetInterval of any period p against the target$"#)]
async fn given_law_interval_any_p(_world: &mut SchedulerWorld) {}

#[given(
    regex = r#"^the target is either dropped \(strong refs released\) or stopped while a strong ref remains$"#
)]
async fn given_law_target_gone(_world: &mut SchedulerWorld) {}

#[given(
    regex = r#"^any mix of SetTimeout and SetInterval messages on one Scheduler against the target$"#
)]
async fn given_law_any_mix(_world: &mut SchedulerWorld) {}

#[when(
    regex = r#"^the paused clock is advanced past construction \+ d and held for further ticks$"#
)]
async fn when_law_advance_past_d(_world: &mut SchedulerWorld) {}

#[when(regex = r#"^the paused clock is advanced to start \+ s \+ k\*p for any k$"#)]
async fn when_law_advance_to_k(_world: &mut SchedulerWorld) {}

#[when(regex = r#"^the message is asked to the Scheduler$"#)]
async fn when_law_ask(_world: &mut SchedulerWorld) {}

#[when(
    regex = r#"^all are asked concurrently from multiple tasks and the paused clock is advanced$"#
)]
async fn when_law_concurrent(_world: &mut SchedulerWorld) {}

/// Trailing `And` clause of the one-shot law: the abort branch is asserted inside
/// `then_law_timeout_once`.
#[then(
    regex = r#"^in the separate run where the AbortHandle is aborted before d elapses, the target never receives the message however far the clock is then advanced$"#
)]
async fn then_law_abort_branch(_world: &mut SchedulerWorld) {}

/// Trailing `And` clause of the model law: asserted inside the preceding Then.
#[then(regex = r#"^no further messages are attempted after the target becomes unavailable$"#)]
async fn then_law_no_further_unavailable(_world: &mut SchedulerWorld) {}

/// `@model @linearizability` trailing clause: asserted in the preceding Then.
#[then(
    regex = r#"^no scheduled message is dropped, duplicated, or attributed to the wrong timer$"#
)]
async fn then_law_no_cross_talk(_world: &mut SchedulerWorld) {}

/// `@property @sequence @timing` — SetTimeout fires exactly once at the first tick
/// >= construction+d (no earlier), and aborting before d ⇒ never fires.
///
/// GEN: d ∈ {ZERO, 1 tick, 1ms, 100ms, 1s}. ORACLE: `oracle_timeout` (one-shot
/// model derived from tokio's `sleep_until` semantics, independent of the SUT).
#[then(
    regex = r#"^the target receives the message exactly once, no earlier than d after construction$"#
)]
async fn then_law_timeout_once(_world: &mut SchedulerWorld) {
    let durations = [
        Duration::ZERO,
        ONE_TICK,
        Duration::from_millis(1),
        Duration::from_millis(100),
        Duration::from_secs(1),
    ];
    for d in durations {
        // Advance well past the deadline: the oracle says exactly 1.
        let advance = d + d.max(ONE_TICK) + ONE_TICK * 4;
        let observed = drive_timeout(d, Duration::ZERO, advance);
        let expected = oracle_timeout(d, advance);
        assert_eq!(
            observed, expected,
            "fire-once law: d={d:?} expected {expected} delivery, observed {observed}"
        );
        assert_eq!(expected, 1, "past the deadline the one-shot oracle is 1");

        // BEFORE the deadline (non-zero d): the oracle and the SUT both say 0.
        if !d.is_zero() {
            let half = d / 2;
            let early = drive_timeout(d, Duration::ZERO, half);
            assert_eq!(
                early,
                oracle_timeout(d, half),
                "no-early-fire law: d={d:?} must be 0 at d/2"
            );
            assert_eq!(early, 0, "before the deadline nothing fires");
        }

        // Abort branch: aborting before d ⇒ never fires, however far we advance.
        if !d.is_zero() {
            let aborted = drive_timeout_abort(d);
            assert_eq!(aborted, 0, "abort-before-d law: d={d:?} must never deliver");
        }
    }
}

/// `@property @sequence @timing` — under Delay, a SetInterval has delivered
/// exactly the number of tick instants reached by the advance, for any
/// period/start_delay/k.
///
/// GEN: p ∈ {1 tick, 1ms, 100ms}; s ∈ {none, 0, 250ms}; k ∈ {0, 1, 5}. ORACLE:
/// `oracle_interval_ticks` (tick-instant model under Delay, independent of the
/// SUT).
#[then(
    regex = r#"^the number of messages the target has received equals k \(the number of ticks whose instant has been reached\), under MissedTickBehavior::Delay so missed ticks do not replay$"#
)]
async fn then_law_interval_k(_world: &mut SchedulerWorld) {
    let periods = [
        ONE_TICK,
        Duration::from_millis(1),
        Duration::from_millis(100),
    ];
    let delays = [None, Some(Duration::ZERO), Some(Duration::from_millis(250))];
    let ks = [0_u32, 1, 5];
    for p in periods {
        for s in delays {
            for k in ks {
                let base = s.unwrap_or(Duration::ZERO);
                // Advance to start + s + k*p exactly (steps in p so each instant
                // is reached precisely under Delay).
                let advance = base + p * k;
                let observed = drive_interval(IntervalRun {
                    period: p,
                    start_delay: s,
                    missed: MissedTickBehavior::Delay,
                    receipt: Duration::ZERO,
                    advance_total: advance,
                });
                let expected = oracle_interval_ticks(p, s, advance);
                assert_eq!(
                    observed, expected,
                    "k-ticks law: p={p:?} s={s:?} k={k} advance={advance:?} \
                     expected {expected}, observed {observed}"
                );
                // The reached-instant count is k+1 (immediate tick + k more).
                assert_eq!(
                    expected,
                    u64::from(k) + 1,
                    "tick-instant oracle: reached instants = k+1 for k={k}"
                );
            }
        }
    }
}

/// `@property @sequence` — both handlers reply with a usable AbortHandle for any
/// duration/period, independent of whether the task has fired.
#[then(
    regex = r#"^the reply is a tokio AbortHandle referencing the spawned task, returned independently of whether or when the task fires$"#
)]
async fn then_law_both_return_handle(_world: &mut SchedulerWorld) {
    // GEN: d ∈ {ZERO, 1ms, 100ms}; SetTimeout accepts an already-past (ZERO)
    // deadline. SetInterval::new calls `tokio::time::interval(period)`, which
    // PANICS on a zero period (tokio requires period > 0 — see tokio's
    // `interval` docs), so the interval branch uses the smallest VALID period
    // (1 tokio tick) in place of ZERO; the handle-return law is independent of
    // the period magnitude.
    let timeout_ds = [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_millis(100),
    ];
    for d in timeout_ds {
        assert!(
            returns_abort_handle(d, false),
            "SetTimeout(d={d:?}) must reply with a usable AbortHandle"
        );
    }
    let interval_ps = [
        ONE_TICK,
        Duration::from_millis(1),
        Duration::from_millis(100),
    ];
    for p in interval_ps {
        assert!(
            returns_abort_handle(p, true),
            "SetInterval(p={p:?}) must reply with a usable AbortHandle"
        );
    }
}

/// `@model @linearizability` law for independent concurrent timers (the
/// `@model @lifecycle` clean-termination law shares the example's exact Then
/// phrasing and is handled inside `then_interval_exits_clean`).
#[then(
    regex = r#"^each timeout contributes exactly one message and each interval contributes one message per reached tick instant$"#
)]
async fn then_law_mix(_world: &mut SchedulerWorld) {
    // Independent-timers model: total deliveries == sum of per-timer oracles.
    // GEN: SetTimeout count ∈ {0,1,50}; SetInterval count ∈ {0,1,2}; p ∈ {1 tick,
    // 100ms}. Drive each combination on its own Scheduler and compare against the
    // summed oracle.
    let timeout_counts = [0_usize, 1, 50];
    let interval_counts = [0_usize, 1, 2];
    let periods = [ONE_TICK, Duration::from_millis(100)];
    for &nt in &timeout_counts {
        for &ni in &interval_counts {
            for p in periods {
                // Advance to 3 periods past start (in p steps) under Delay.
                let advance = p * 3;
                let observed = drive_mixed_timers(nt, ni, p, advance);
                // Oracle: nt one-shot timeouts (each fires once, since deadline =
                // p <= advance) + ni intervals (each reaches advance/p + 1 ticks).
                let per_interval = oracle_interval_ticks(p, None, advance);
                let expected =
                    u64::try_from(nt).unwrap() + u64::try_from(ni).unwrap() * per_interval;
                assert_eq!(
                    observed, expected,
                    "independent-timers law: nt={nt} ni={ni} p={p:?} \
                     expected {expected}, observed {observed}"
                );
            }
        }
    }
}

/// Drives `nt` SetTimeouts (each `period` deadline) + `ni` SetIntervals (each
/// `period`, Delay) concurrently on one Scheduler, advancing `advance` in `period`
/// steps. Returns total deliveries (the per-timer oracle sum is asserted by the
/// caller).
fn drive_mixed_timers(nt: usize, ni: usize, period: Duration, advance: Duration) -> u64 {
    on_paused_runtime(async move {
        let (scheduler, recorder, count) = fresh().await;
        let total = nt + ni;
        if total == 0 {
            advance_in_steps(advance, period).await;
            recorder.kill();
            scheduler.kill();
            return count.load(Ordering::SeqCst);
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(total));
        let mut handles = Vec::with_capacity(total);
        for _ in 0..nt {
            let scheduler = scheduler.clone();
            let weak = recorder.downgrade();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let msg = SetTimeout::new(weak, period, Tick);
                barrier.wait().await;
                let _a: AbortHandle = scheduler.ask(msg).await.expect("ask timeout");
            }));
        }
        for _ in 0..ni {
            let scheduler = scheduler.clone();
            let weak = recorder.downgrade();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let msg = SetInterval::new(weak, period, Tick)
                    .set_missed_tick_behaviour(MissedTickBehavior::Delay);
                barrier.wait().await;
                let _a: AbortHandle = scheduler.ask(msg).await.expect("ask interval");
            }));
        }
        for h in handles {
            h.await.expect("timer-ask task join");
        }
        advance_in_steps(advance, period).await;
        recorder.kill();
        scheduler.kill();
        count.load(Ordering::SeqCst)
    })
}
