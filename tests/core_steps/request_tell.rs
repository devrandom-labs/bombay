//! Shared `TellRequest` World + step definitions for the core `request_tell`
//! scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_request_tell_bdd.rs`        — the example feature (request_tell.feature)
//!   * `core_request_tell_props_bdd.rs`  — the property/model laws
//!     (request_tell.properties.feature)
//!
//! The SUT is `src/request/tell.rs` (the `TellRequest` builder: `tell(M)` →
//! optional `mailbox_timeout` → `send`/`try_send`/`blocking_send`/`send_after`/
//! `IntoFuture`), driven against REAL SPAWNED ACTORS reached through
//! `kameo::prelude::*`. `tell` is fire-and-forget: no reply channel, no forward
//! variants, and every send returns `Result<(), SendError<M>>`. Because the
//! dead-actor / full-mailbox failure boxes the BARE message, the
//! `From<SendError<Signal>>` conversion downcasts `<M>` (not a tuple) and yields
//! a graceful `ActorNotRunning(M)` / `MailboxFull(M)` — no panic (unlike `ask`'s
//! forward variants), so there are no `@bug` scenarios here.
//!
//! ## @timing (pause/advance via a paused current-thread runtime)
//!
//! `tokio::time::pause()`/`advance()` REQUIRE a current-thread runtime, but the
//! cucumber runner is `#[tokio::test(flavor = "multi_thread")]` (a
//! non-negotiable harness fact). So every @timing step drives its actor + tell
//! inside a DEDICATED current-thread `start_paused(true)` runtime, created on a
//! blocking thread via `tokio::task::spawn_blocking`. A paused current-thread
//! runtime AUTO-ADVANCES its clock to the next pending timer whenever it has no
//! other work, so a `send_after(d)`'s sleep or a `mailbox_timeout(t)` resolves
//! deterministically with ~zero wall-clock time and no flake. The actor MUST be
//! spawned inside that same paused runtime so any handler `sleep` and the
//! mailbox-capacity wait observe the same paused clock. For the
//! abort-before-delay case the spawned `send_after` task is aborted SYNCHRONOUSLY
//! (before any `.await` yields to the scheduler), so the paused clock never
//! advances to the send and the message is provably never delivered.
//!
//! ## blocking_send (gotcha 3)
//!
//! `blocking_send` calls tokio's blocking mailbox primitive, which PANICS
//! ("Cannot block the current thread from within a runtime") if called directly
//! on the async cucumber worker. It is wrapped in `tokio::task::spawn_blocking`
//! with a cloned `ActorRef` moved into the closure.
//!
//! ## full bounded mailbox (gotcha 4)
//!
//! Full-mailbox scenarios use a handler parked on a `watch` release gate so the
//! mailbox stays full; the test asserts the full-mailbox observable, then
//! releases the gate (or `kill()`s) — it never awaits clean completion against a
//! permanently-full mailbox.
//!
//! All bounded waits are condition-based `settle()` polling (panics loudly on
//! non-settle); no `wait_for_shutdown()` is used as the settle barrier.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use cucumber::{World, given, then, when};
use kameo::{error::Infallible, mailbox, prelude::*};
use tokio::{
    sync::{Barrier, watch},
    task::JoinHandle,
};
use tracing::subscriber::DefaultGuard;

// ===========================================================================
// Test actors and messages
// ===========================================================================

/// The actor under test. It records every integer it handles into a shared log
/// so a fire-and-forget tell's delivery is observable, and can be parked on a
/// release gate to hold a bounded mailbox full.
#[derive(Clone)]
struct Told {
    /// Every integer the actor has handled, in handling order.
    log: Arc<Mutex<Vec<u64>>>,
}

impl Told {
    fn new(log: Arc<Mutex<Vec<u64>>>) -> Self {
        Told { log }
    }
}

impl Actor for Told {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A plain marker message; the handler records the sentinel `0`. `Msg` is
/// `Clone + Copy + PartialEq + Debug` so `SendError<Msg>` equality can be
/// asserted against the exact returned message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Msg;

impl Message<Msg> for Told {
    type Reply = ();

    async fn handle(&mut self, _msg: Msg, _ctx: &mut Context<Self, Self::Reply>) {
        self.log.lock().unwrap().push(0);
    }
}

/// A numbered message: the handler records the integer it carries (linearizability
/// / model oracles read this log).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Num(u64);

impl Message<Num> for Told {
    type Reply = ();

    async fn handle(&mut self, Num(n): Num, _ctx: &mut Context<Self, Self::Reply>) {
        self.log.lock().unwrap().push(n);
    }
}

/// A numbered message whose handler records the integer after a short delay
/// (so a bounded mailbox stays under backpressure while callers queue).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct SlowNum(u64);

impl Message<SlowNum> for Told {
    type Reply = ();

    async fn handle(&mut self, SlowNum(n): SlowNum, _ctx: &mut Context<Self, Self::Reply>) {
        tokio::time::sleep(Duration::from_millis(1)).await;
        self.log.lock().unwrap().push(n);
    }
}

/// A message whose handler sleeps `dur` then records the sentinel `0`. Drives the
/// mailbox_timeout scenarios under the paused clock (a busy handler holds the slot).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Sleep(Duration);

impl Message<Sleep> for Told {
    type Reply = ();

    async fn handle(&mut self, Sleep(dur): Sleep, _ctx: &mut Context<Self, Self::Reply>) {
        tokio::time::sleep(dur).await;
        self.log.lock().unwrap().push(0);
    }
}

/// A message whose handler parks on a `watch` release gate until it flips to
/// `true`, holding the mailbox slot so a bounded mailbox stays full.
struct Hold(watch::Receiver<bool>);

impl Message<Hold> for Told {
    type Reply = ();

    async fn handle(&mut self, msg: Hold, _ctx: &mut Context<Self, Self::Reply>) {
        let mut rx = msg.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

// --- The self-tell actor for the deadlock-warning scenario ------------------

/// An actor that, on `SelfTell`, sends a bounded `tell(Msg)` to ITSELF from
/// within its own handler (so `is_current()` is true and `capacity().is_some()`),
/// then records the `send().await` outcome into a shared slot.
#[derive(Clone)]
struct SelfTeller {
    /// Records the `Ok`/`Err` result of the in-handler self-tell.
    result: Arc<Mutex<Option<bool>>>,
}

impl Actor for SelfTeller {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Marker handled by the self-tell — it just records `0` (so the self-`tell(Msg)`
/// has a real handler to run after capacity is available).
#[derive(Clone, Copy)]
struct SelfMsg;

impl Message<SelfMsg> for SelfTeller {
    type Reply = ();

    async fn handle(&mut self, _msg: SelfMsg, _ctx: &mut Context<Self, Self::Reply>) {}
}

/// Triggers the in-handler bounded self-tell. The handler uses `send().await`
/// (the path that emits warn_deadlock when `capacity().is_some() && is_current()`,
/// tell.rs:96-102/611-622 — `try_send` does NOT warn). The scenario stipulates
/// "with spare capacity available", so this bounded self-`send` returns Ok(())
/// immediately without parking (no real deadlock), while still emitting the
/// advisory warning.
struct SelfTell;

impl Message<SelfTell> for SelfTeller {
    type Reply = ();

    async fn handle(&mut self, _msg: SelfTell, ctx: &mut Context<Self, Self::Reply>) {
        let me = ctx.actor_ref();
        let r = me.tell(SelfMsg).send().await;
        *self.result.lock().unwrap() = Some(r.is_ok());
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Spawns a `Told` actor with a bounded mailbox of `cap`, awaiting startup, and
/// returns it with its shared log.
async fn spawn_bounded(cap: usize) -> (ActorRef<Told>, Arc<Mutex<Vec<u64>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let actor = Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(cap));
    actor.wait_for_startup().await;
    (actor, log)
}

/// Spawns a `Told` actor with an unbounded mailbox, awaiting startup.
async fn spawn_unbounded() -> (ActorRef<Told>, Arc<Mutex<Vec<u64>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let actor = Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::unbounded());
    actor.wait_for_startup().await;
    (actor, log)
}

/// Fills a bounded(1) `Told` mailbox so it has NO spare capacity, using parked
/// `Hold` handlers gated on `release`. The first send is dequeued into the parked
/// handler (freeing the slot), the second occupies the single buffer slot —
/// matching the in-file `bounded_tell_requests_mailbox_full` arithmetic.
async fn fill_bounded1(actor: &ActorRef<Told>, release: watch::Receiver<bool>) {
    actor
        .tell(Hold(release.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued");
    tokio::time::sleep(Duration::from_millis(20)).await;
    actor
        .tell(Hold(release))
        .try_send()
        .expect("second hold fills the single slot");
}

/// Asserts (with bounded polling) that `actor`'s bounded mailbox is observably
/// FULL — a `try_send` returns `MailboxFull` and hands the message back, so this
/// probe never enqueues anything. Panics loudly if the mailbox never fills.
async fn assert_full(actor: &ActorRef<Told>) {
    for _ in 0..200 {
        if matches!(actor.tell(Msg).try_send(), Err(SendError::MailboxFull(Msg))) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the bounded mailbox never became observably full");
}

/// Polls (bounded) until `log` contains exactly the integers in `expected` (as a
/// set), panicking loudly otherwise.
async fn settle_log_set(log: &Arc<Mutex<Vec<u64>>>, expected: &HashSet<u64>) {
    for _ in 0..400 {
        {
            let got: HashSet<u64> = log.lock().unwrap().iter().copied().collect();
            if &got == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let got = log.lock().unwrap().clone();
    panic!("log never settled to the expected set: got {got:?}, expected {expected:?}");
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct TellWorld {
    /// The actor under test (kept alive across steps).
    actor: Option<ActorRef<Told>>,
    /// The actor's shared handling log.
    log: Option<Arc<Mutex<Vec<u64>>>>,
    /// A release gate for parked `Hold` handlers (full/busy-mailbox scenarios).
    release: Option<watch::Sender<bool>>,

    // --- captured outcomes -------------------------------------------------
    /// The Ok(()) results from the send/try_send/blocking_send trio.
    trio_ok: Vec<bool>,
    /// The three `ActorNotRunning` observations on a stopped actor.
    stopped_trio: Vec<bool>,
    /// Whether the captured outcome was `SendError::Timeout(Some(Sleep(100ms)))`.
    timeout_some_sleep: Option<bool>,
    /// Whether the captured outcome was `SendError::Timeout(Some(Msg))`.
    timeout_some_msg: Option<bool>,
    /// Whether the captured outcome was `SendError::MailboxFull(Msg)`.
    mailbox_full: Option<bool>,
    /// Whether a plain (no-timeout) bounded send returned Ok(()).
    send_ok: Option<bool>,
    /// Whether the bounded-with-capacity send observably waited before Ok.
    send_waited_then_ok: Option<bool>,

    // --- send_after --------------------------------------------------------
    /// Whether the send_after JoinHandle resolved to Ok(()).
    send_after_ok: Option<bool>,
    /// Whether the message was delivered after the send_after fired.
    send_after_delivered: Option<bool>,
    /// Whether the aborted send_after reported cancellation on await.
    send_after_cancelled: Option<bool>,
    /// Whether the aborted send_after's message was NEVER delivered.
    send_after_not_delivered: Option<bool>,
    /// Whether a send_after to a stopped actor resolved ActorNotRunning(Msg).
    send_after_not_running: Option<bool>,

    // --- on_start buffering ------------------------------------------------
    /// Whether a tell issued during on_start was handled after startup.
    buffered_then_handled: Option<bool>,

    // --- self-tell deadlock warning ----------------------------------------
    /// Whether the deadlock warning was emitted (captured via a tracing layer).
    deadlock_warning: Option<bool>,
    /// Whether the self-tell still returned Ok(()).
    self_tell_ok: Option<bool>,

    // --- linearizability ---------------------------------------------------
    /// The integers whose try_send returned Ok(()).
    try_ok_ints: Vec<u64>,
    /// The integers whose try_send returned MailboxFull.
    try_full_ints: Vec<u64>,
    /// The distinct integers all concurrent bounded sends carried.
    sent_ints: Vec<u64>,

    // --- scenario-routing flags --------------------------------------------
    /// Routes the shared send/try_send Whens to the stopped-actor case.
    stopped: bool,
    /// Routes the shared try_send When to the full-mailbox case.
    full: bool,

    /// The on_start-blocking actor (lifecycle buffering scenario).
    blocker: Option<ActorRef<Blocker>>,
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^a running actor whose handler can be made to sleep for a given duration$")]
async fn given_running_actor(_world: &mut TellWorld) {
    // The concrete actor + mailbox is created by each scenario's capacity Given
    // (different scenarios need different capacities / paused clocks / unbounded),
    // so the Background is a no-op marker matching the shared feature phrasing.
}

#[given(regex = r"^a running actor whose handler records every integer it receives$")]
async fn given_running_recorder(_world: &mut TellWorld) {
    // Properties Background — same no-op marker; the laws build their own actors.
}

// ===========================================================================
// @sequence — builder protocol and the capacity contract
// ===========================================================================

#[given(regex = r"^the actor has a bounded mailbox of capacity 100 and is idle$")]
async fn given_bounded_100_idle(world: &mut TellWorld) {
    let (actor, log) = spawn_bounded(100).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the actor has a bounded mailbox of capacity 100$")]
async fn given_bounded_100(world: &mut TellWorld) {
    let (actor, log) = spawn_bounded(100).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the actor has a bounded mailbox with spare capacity$")]
async fn given_bounded_spare(world: &mut TellWorld) {
    let (actor, log) = spawn_bounded(100).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the actor has an unbounded mailbox$")]
async fn given_unbounded(world: &mut TellWorld) {
    let (actor, log) = spawn_unbounded().await;
    world.actor = Some(actor);
    world.log = Some(log);
}

// SHARED When: the idle trio (expects Ok) AND the stopped-actor scenario (expects
// ActorNotRunning) both use this text. Route by the `stopped` flag.
#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send\(\)"$"#)]
async fn when_invoke_send(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    if world.stopped {
        let r = actor.tell(Msg).send().await;
        assert_eq!(
            r,
            Err(SendError::ActorNotRunning(Msg)),
            "send to a stopped actor must return ActorNotRunning(Msg)"
        );
        world.stopped_trio.push(true);
        return;
    }
    actor.tell(Msg).send().await.expect("send delivers");
    world.trio_ok.push(true);
}

// SHARED When: the idle trio (expects Ok) AND the full-mailbox scenario (expects
// MailboxFull) use this text. Route by the `full` flag.
#[when(regex = r#"^the caller invokes "tell\(Msg\)\.try_send\(\)"$"#)]
async fn when_invoke_try_send(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    if world.full {
        let r = actor.tell(Msg).try_send();
        assert_eq!(
            r,
            Err(SendError::MailboxFull(Msg)),
            "try_send into a full bounded mailbox must return MailboxFull(Msg)"
        );
        world.mailbox_full = Some(true);
        if let Some(tx) = world.release.take() {
            let _ = tx.send(true);
        }
        actor.kill();
        return;
    }
    actor.tell(Msg).try_send().expect("try_send delivers");
    world.trio_ok.push(true);
}

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.blocking_send\(\)" on a blocking thread$"#)]
async fn when_invoke_blocking_send(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    tokio::task::spawn_blocking(move || actor.tell(Msg).blocking_send())
        .await
        .expect("blocking thread join")
        .expect("blocking_send delivers");
    world.trio_ok.push(true);
}

#[then(regex = r"^each call returns Ok\(\(\)\)$")]
async fn then_each_ok(world: &mut TellWorld) {
    assert_eq!(
        world.trio_ok,
        vec![true, true, true],
        "send, try_send and blocking_send must each return Ok(())"
    );
}

#[then(regex = r"^the actor eventually handles all three messages$")]
async fn then_handles_all_three(world: &mut TellWorld) {
    let log = world.log.as_ref().expect("log").clone();
    // All three are `Msg`, each recording the sentinel 0: expect exactly three 0s.
    for _ in 0..400 {
        if log.lock().unwrap().len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let got = log.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![0, 0, 0],
        "the idle actor must handle all three tells, got {got:?}"
    );
    world.actor.as_ref().expect("actor").kill();
}

// --- try_send never waits ----------------------------------------------------

#[then(regex = r"^the call returns Ok\(\(\)\) immediately without awaiting capacity$")]
async fn then_try_send_ok_immediate(world: &mut TellWorld) {
    assert_eq!(
        world.trio_ok,
        vec![true],
        "try_send into a mailbox with spare capacity must return Ok(()) without waiting"
    );
    world.actor.as_ref().expect("actor").kill();
}

// --- unbounded send ignores mailbox_timeout ----------------------------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.mailbox_timeout\(1ms\)\.send\(\)"$"#)]
async fn when_unbounded_mailbox_timeout(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let r = actor
        .tell(Msg)
        .mailbox_timeout(Duration::from_millis(1))
        .send()
        .await;
    world.send_ok = Some(r.is_ok());
}

#[then(regex = r"^the call returns Ok\(\(\)\) and the mailbox_timeout never fires$")]
async fn then_unbounded_ok_no_timeout(world: &mut TellWorld) {
    assert_eq!(
        world.send_ok,
        Some(true),
        "an unbounded send never waits on capacity, so the mailbox_timeout cannot fire → Ok(())"
    );
    world.actor.as_ref().expect("actor").kill();
}

// ===========================================================================
// @sequence @timing — a bounded send waits for capacity then succeeds
// ===========================================================================

#[given(regex = r"^the single slot is currently occupied by an in-flight message$")]
async fn given_single_slot_occupied(world: &mut TellWorld) {
    // Capacity Given already spawned a bounded(1) actor; park it full.
    let actor = world.actor.as_ref().expect("bounded(1) actor").clone();
    let (tx, rx) = watch::channel(false);
    fill_bounded1(&actor, rx).await;
    assert_full(&actor).await;
    world.release = Some(tx);
}

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send\(\)" with no mailbox_timeout$"#)]
async fn when_send_no_timeout_waits(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    // Spawn the send so it parks on the full bounded mailbox.
    let handle: JoinHandle<Result<(), SendError<Msg>>> =
        tokio::spawn(async move { actor.tell(Msg).send().await });
    // Observe it is genuinely still pending (no spurious early return).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handle.is_finished(),
        "a bounded send with no spare capacity must park, not return early"
    );
    // Free a slot by releasing the parked Hold handlers; the send then unblocks.
    if let Some(tx) = world.release.take() {
        let _ = tx.send(true);
    }
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the parked send must complete once a slot frees, not hang")
        .expect("send task join");
    world.send_waited_then_ok = Some(outcome.is_ok());
}

#[then(regex = r"^the send does not return until the actor frees a slot$")]
async fn then_send_waits(world: &mut TellWorld) {
    // The When already asserted the send was pending while full and only resolved
    // after a slot was freed; record that it resolved.
    assert_eq!(
        world.send_waited_then_ok,
        Some(true),
        "the parked send must have resolved only after a slot was freed"
    );
}

#[then(regex = r"^the send then returns Ok\(\(\)\)$")]
async fn then_send_then_ok(world: &mut TellWorld) {
    assert_eq!(
        world.send_waited_then_ok,
        Some(true),
        "the bounded send must return Ok(()) once capacity is available"
    );
    world.actor.as_ref().expect("actor").kill();
}

// ===========================================================================
// @boundary — bounded(1) capacity, full, stopped, mailbox_timeout, self-tell
// ===========================================================================

#[given(regex = r"^the actor has a bounded mailbox of capacity 1$")]
async fn given_bounded_1(world: &mut TellWorld) {
    let (actor, log) = spawn_bounded(1).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the mailbox is occupied so it has no spare capacity$")]
async fn given_mailbox_occupied(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("bounded(1) actor").clone();
    let (tx, rx) = watch::channel(false);
    fill_bounded1(&actor, rx).await;
    assert_full(&actor).await;
    world.release = Some(tx);
}

#[given(regex = r"^the mailbox is filled to capacity while the actor is busy in its handler$")]
async fn given_filled_while_busy(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("bounded(1) actor").clone();
    let (tx, rx) = watch::channel(false);
    fill_bounded1(&actor, rx).await;
    assert_full(&actor).await;
    world.release = Some(tx);
    world.full = true;
}

#[then(regex = r"^the caller receives SendError::MailboxFull\(Msg\)$")]
async fn then_mailbox_full(world: &mut TellWorld) {
    assert_eq!(
        world.mailbox_full,
        Some(true),
        "try_send into a full bounded mailbox must have returned MailboxFull(Msg)"
    );
}

#[given(regex = r"^the actor has been stopped gracefully and shutdown has completed$")]
async fn given_stopped_completed(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    actor.stop_gracefully().await.expect("graceful stop");
    actor.wait_for_shutdown().await;
    world.stopped = true;
}

#[then(regex = r"^the caller receives SendError::ActorNotRunning\(Msg\)$")]
async fn then_send_not_running(world: &mut TellWorld) {
    assert_eq!(
        world.stopped_trio,
        vec![true],
        "send to a stopped actor must have returned ActorNotRunning(Msg)"
    );
}

#[then(regex = r#"^"tell\(Msg\)\.try_send\(\)" also returns SendError::ActorNotRunning\(Msg\)$"#)]
async fn then_try_send_not_running(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    assert_eq!(
        actor.tell(Msg).try_send(),
        Err(SendError::ActorNotRunning(Msg)),
        "try_send to a stopped actor must return ActorNotRunning(Msg)"
    );
    world.stopped_trio.push(true);
}

#[then(
    regex = r#"^"tell\(Msg\)\.blocking_send\(\)" also returns SendError::ActorNotRunning\(Msg\)$"#
)]
async fn then_blocking_send_not_running(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let r = tokio::task::spawn_blocking(move || actor.tell(Msg).blocking_send())
        .await
        .expect("blocking thread join");
    assert_eq!(
        r,
        Err(SendError::ActorNotRunning(Msg)),
        "blocking_send to a stopped actor must return ActorNotRunning(Msg)"
    );
    world.stopped_trio.push(true);
}

// --- mailbox_timeout expiring → Timeout(Some(Sleep(100ms))) -----------------

#[when(
    regex = r#"^the caller invokes "tell\(Sleep\(100ms\)\)\.mailbox_timeout\(50ms\)\.send\(\)"$"#
)]
async fn when_mailbox_timeout_sleep(world: &mut TellWorld) {
    let outcome = with_paused_full1(|actor| {
        Box::pin(async move {
            actor
                .tell(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await
        })
    })
    .await;
    world.timeout_some_sleep = Some(matches!(
        outcome,
        Err(SendError::Timeout(Some(Sleep(d)))) if d == Duration::from_millis(100)
    ));
}

#[then(regex = r"^the caller receives SendError::Timeout\(Some\(Sleep\(100ms\)\)\)$")]
async fn then_timeout_some_sleep(world: &mut TellWorld) {
    assert_eq!(
        world.timeout_some_sleep,
        Some(true),
        "an expired mailbox_timeout on a full bounded send must return Timeout(Some(Sleep(100ms))) — message handed back"
    );
}

// --- mailbox_timeout(0) on a full mailbox → Timeout(Some(Msg)) --------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.mailbox_timeout\(0ms\)\.send\(\)"$"#)]
async fn when_mailbox_timeout_zero(world: &mut TellWorld) {
    let outcome = with_paused_full1(|actor| {
        Box::pin(async move { actor.tell(Msg).mailbox_timeout(Duration::ZERO).send().await })
    })
    .await;
    world.timeout_some_msg = Some(matches!(outcome, Err(SendError::Timeout(Some(Msg)))));
}

#[then(regex = r"^the caller receives SendError::Timeout\(Some\(Msg\)\)$")]
async fn then_timeout_some_msg(world: &mut TellWorld) {
    assert_eq!(
        world.timeout_some_msg,
        Some(true),
        "a zero mailbox_timeout on a full bounded send must fail immediately with Timeout(Some(Msg))"
    );
}

// --- self-tell deadlock warning (@review-semantics) -------------------------

#[given(regex = r"^a tracing-enabled debug build$")]
async fn given_tracing_debug(_world: &mut TellWorld) {
    // `warn_deadlock` is gated `#[cfg(all(debug_assertions, feature = "tracing"))]`
    // (src/request/tell.rs:31), so it is compiled IN only for debug+tracing builds
    // and OUT of release builds. The default `tracing` feature is always on; the
    // build profile decides debug_assertions. The Then asserts the build-correct
    // outcome (fired iff debug_assertions). The actual warning is captured by the
    // When via a per-thread tracing subscriber.
}

#[given(regex = r#"^an actor sending a bounded "tell" to itself from within its own handler$"#)]
async fn given_self_teller(_world: &mut TellWorld) {
    // The SelfTeller actor is spawned in the When (it needs the captured-warning
    // subscriber installed first so the in-handler warning is recorded).
}

#[when(regex = r"^the self-tell is dispatched with spare capacity available$")]
async fn when_self_tell_dispatched(world: &mut TellWorld) {
    let (warned, self_ok) = capture_self_tell().await;
    world.deadlock_warning = Some(warned);
    world.self_tell_ok = Some(self_ok);
}

#[then(regex = r"^a deadlock warning is emitted naming the call site$")]
async fn then_deadlock_warning(world: &mut TellWorld) {
    // `warn_deadlock` is `#[cfg(all(debug_assertions, feature = "tracing"))]`
    // (src/request/tell.rs:31), so the warning fires in a debug build and is
    // compiled OUT of a release build. The gate (`nix flake check` → crane
    // nextest) builds tests in release, where the warning legitimately does not
    // fire; a plain debug `cargo test` build does fire it. Assert the
    // build-mode-correct outcome — both arms are falsifiable (a regression that
    // emitted the warning in release, or suppressed it in debug, breaks this).
    let expect_warning = cfg!(debug_assertions);
    assert_eq!(
        world.deadlock_warning,
        Some(expect_warning),
        "the self-tell deadlock warning must fire iff debug_assertions is on \
         (debug+tracing compiles warn_deadlock in; release compiles it out); \
         debug_assertions={expect_warning}"
    );
}

#[then(regex = r"^the send still returns Ok\(\(\)\) — the warning does not alter the result$")]
async fn then_self_tell_ok(world: &mut TellWorld) {
    assert_eq!(
        world.self_tell_ok,
        Some(true),
        "the warning is advisory: the self-tell still returns Ok(())"
    );
}

// ===========================================================================
// @lifecycle — on_start buffering, send_after scheduling/abort
// ===========================================================================

#[given(regex = r"^an actor whose on_start blocks until released$")]
async fn given_on_start_blocks(world: &mut TellWorld) {
    // Wired entirely in the When (it needs the release gate + the buffered tell
    // issued BEFORE on_start completes); store the gate here.
    let (tx, _rx) = watch::channel(false);
    world.release = Some(tx);
}

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send\(\)" before on_start has completed$"#)]
async fn when_tell_before_on_start(world: &mut TellWorld) {
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    world.log = Some(Arc::clone(&log));
    let gate = world.release.as_ref().expect("on_start gate").subscribe();
    // Spawn an actor whose on_start parks on the gate. An unbounded mailbox so the
    // pre-start tell is buffered without blocking on capacity while on_start waits.
    let actor = Blocker::spawn_with_mailbox(
        Blocker {
            log: Arc::clone(&log),
            gate,
        },
        mailbox::unbounded(),
    );
    // Do NOT wait_for_startup (on_start is blocked). Issue the tell now — it must
    // be buffered by the starting mailbox, not rejected.
    actor
        .tell(BlockerMsg)
        .send()
        .await
        .expect("a tell during on_start is buffered, not rejected");
    world.blocker = Some(actor);
}

#[when(regex = r"^on_start is then released$")]
async fn when_on_start_released(world: &mut TellWorld) {
    if let Some(tx) = world.release.take() {
        let _ = tx.send(true);
    }
}

#[then(regex = r"^the message is handled after startup rather than rejected$")]
async fn then_handled_after_startup(world: &mut TellWorld) {
    let log = world.log.as_ref().expect("log").clone();
    for _ in 0..400 {
        if log.lock().unwrap().contains(&7) {
            world.buffered_then_handled = Some(true);
            if let Some(a) = world.blocker.take() {
                a.kill();
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the buffered pre-start tell was never handled after startup");
}

// --- send_after delivers after the delay ------------------------------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send_after\(50ms\)"$"#)]
async fn when_send_after_50(_world: &mut TellWorld) {
    // The whole scheduled-send lifecycle is driven under a paused clock by the
    // awaiting Then (the JoinHandle must be created in the same runtime that
    // advances the clock), so this is a phrasing marker.
}

#[when(regex = r"^the caller awaits the returned JoinHandle$")]
async fn when_await_join_handle(_world: &mut TellWorld) {
    // Marker — see the awaiting Then.
}

#[then(
    regex = r"^the handle resolves to Ok\(\(\)\) and the message was delivered after the delay$"
)]
async fn then_send_after_delivered_after_delay(world: &mut TellWorld) {
    let (handle_ok, delivered) = run_send_after(Duration::from_millis(50)).await;
    world.send_after_ok = Some(handle_ok);
    world.send_after_delivered = Some(delivered);
    assert_eq!(
        world.send_after_ok,
        Some(true),
        "the send_after JoinHandle must resolve to Ok(())"
    );
    assert_eq!(
        world.send_after_delivered,
        Some(true),
        "the message must be delivered once the delay elapses"
    );
}

// --- abort send_after before the delay --------------------------------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send_after\(1s\)"$"#)]
async fn when_send_after_1s(_world: &mut TellWorld) {
    // Marker — the abort lifecycle runs under a paused clock in the await Then.
}

#[when(regex = r"^the caller aborts the returned JoinHandle before the delay elapses$")]
async fn when_abort_before_delay(world: &mut TellWorld) {
    let (delivered, cancelled) = run_send_after_abort(Duration::from_secs(1)).await;
    world.send_after_not_delivered = Some(!delivered);
    world.send_after_cancelled = Some(cancelled);
}

#[then(regex = r"^the message is never delivered to the actor$")]
async fn then_never_delivered(world: &mut TellWorld) {
    assert_eq!(
        world.send_after_not_delivered,
        Some(true),
        "aborting send_after before the delay must prevent delivery"
    );
}

#[then(regex = r"^awaiting the aborted handle reports cancellation$")]
async fn then_reports_cancellation(world: &mut TellWorld) {
    assert_eq!(
        world.send_after_cancelled,
        Some(true),
        "awaiting an aborted JoinHandle must report cancellation (JoinError::is_cancelled)"
    );
}

// --- send_after(0) delivers on the next tick --------------------------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send_after\(0ms\)"$"#)]
async fn when_send_after_0(_world: &mut TellWorld) {
    // Marker — driven under a paused clock by the await Then.
}

#[then(regex = r"^the handle resolves to Ok\(\(\)\) and the message is delivered$")]
async fn then_send_after_zero_delivered(world: &mut TellWorld) {
    let (handle_ok, delivered) = run_send_after(Duration::ZERO).await;
    world.send_after_ok = Some(handle_ok);
    world.send_after_delivered = Some(delivered);
    assert_eq!(
        world.send_after_ok,
        Some(true),
        "send_after(0) handle resolves Ok(())"
    );
    assert_eq!(
        world.send_after_delivered,
        Some(true),
        "send_after(0) delivers on the next scheduler tick"
    );
}

// --- send_after to an actor that stops before the delay ---------------------

#[when(regex = r#"^the caller invokes "tell\(Msg\)\.send_after\(200ms\)"$"#)]
async fn when_send_after_200(_world: &mut TellWorld) {
    // Marker — driven under a paused clock by the await Then.
}

#[when(regex = r"^the actor is stopped and shutdown completes before the delay elapses$")]
async fn when_actor_stopped_before_delay(_world: &mut TellWorld) {
    // Marker — the stop is sequenced inside the paused-clock driver in the Then,
    // so the deferred send provably hits a closed mailbox.
}

#[then(regex = r"^the handle resolves to Err\(SendError::ActorNotRunning\(Msg\)\)$")]
async fn then_send_after_not_running(world: &mut TellWorld) {
    let not_running = run_send_after_stopped(Duration::from_millis(200)).await;
    world.send_after_not_running = Some(not_running);
    assert_eq!(
        world.send_after_not_running,
        Some(true),
        "a send_after whose deferred send hits a closed mailbox resolves Err(ActorNotRunning(Msg))"
    );
}

// ===========================================================================
// @linearizability — concurrent tells with real overlap
// ===========================================================================

#[given(regex = r"^the actor has a bounded mailbox of capacity 8$")]
async fn given_bounded_8(world: &mut TellWorld) {
    // Parked so the slots stay deterministically occupied during the try_send
    // race; the handler records each integer when (eventually) released.
    let (actor, log) = spawn_bounded(8).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the handler records each integer it receives$")]
async fn given_records_each_int(_world: &mut TellWorld) {
    // The `Num` handler records each integer; the actor is the bounded-8 actor.
}

#[when(
    regex = r#"^100 callers concurrently invoke "tell\(n\)\.try_send\(\)" with distinct integers under a barrier$"#
)]
async fn when_100_concurrent_try_send(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    // Park every slot so the slot count is deterministic during the race, then
    // record which try_sends were accepted vs refused.
    let (release_tx, release_rx) = watch::channel(false);
    // Fill: spawn 8 parked Holds (capacity 8) so the mailbox is observably full.
    for _ in 0..8u64 {
        let _ = actor.tell(Hold(release_rx.clone())).try_send();
    }
    // Wait until the actor has dequeued one (freeing a slot) and the rest fill it.
    // The exact occupied count races the actor; the law only needs each try_send's
    // own return value to decide recording, so proceed once at least observably
    // near-full, then run the barriered race.
    tokio::time::sleep(Duration::from_millis(30)).await;

    let n = 100u64;
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                match actor.tell(Num(i)).try_send() {
                    Ok(()) => (i, true),
                    Err(SendError::MailboxFull(Num(_))) => (i, false),
                    Err(other) => panic!("unexpected try_send error: {other:?}"),
                }
            })
        })
        .collect();
    for h in handles {
        let (i, ok) = h.await.expect("try_send task join");
        if ok {
            world.try_ok_ints.push(i);
        } else {
            world.try_full_ints.push(i);
        }
    }
    // Release the parked Holds so the accepted Nums are actually handled+recorded.
    let _ = release_tx.send(true);
    // Settle: every accepted integer must be recorded exactly once.
    let expected: HashSet<u64> = world.try_ok_ints.iter().copied().collect();
    let log = world.log.as_ref().expect("log").clone();
    settle_log_set(&log, &expected).await;
    actor.kill();
}

#[then(regex = r"^every call that returned Ok\(\(\)\) had its integer recorded exactly once$")]
async fn then_ok_recorded_once(world: &mut TellWorld) {
    let log = world.log.as_ref().expect("log").lock().unwrap().clone();
    for i in &world.try_ok_ints {
        let count = log.iter().filter(|&&x| x == *i).count();
        assert_eq!(
            count, 1,
            "Ok integer {i} must be recorded exactly once, got {count}"
        );
    }
}

#[then(regex = r"^every call that returned MailboxFull had its integer NOT recorded$")]
async fn then_full_not_recorded(world: &mut TellWorld) {
    let log: HashSet<u64> = world
        .log
        .as_ref()
        .expect("log")
        .lock()
        .unwrap()
        .iter()
        .copied()
        .collect();
    for i in &world.try_full_ints {
        assert!(
            !log.contains(i),
            "MailboxFull integer {i} must NOT have been recorded"
        );
    }
    // Sanity: the race must have produced BOTH outcomes (capacity 8 < 100 callers,
    // with all slots parked), so the assertions above are non-vacuous.
    assert!(
        !world.try_full_ints.is_empty(),
        "with capacity 8 parked and 100 callers, some try_sends must hit MailboxFull"
    );
}

// --- concurrent bounded sends all eventually deliver ------------------------

#[given(regex = r"^the actor has a bounded mailbox of capacity 2$")]
async fn given_bounded_2(world: &mut TellWorld) {
    let (actor, log) = spawn_bounded(2).await;
    world.actor = Some(actor);
    world.log = Some(log);
}

#[given(regex = r"^the handler records each integer after a short delay$")]
async fn given_records_after_delay(_world: &mut TellWorld) {
    // The `SlowNum` handler records each integer after a 1ms delay.
}

#[when(
    regex = r#"^20 callers concurrently invoke "tell\(n\)\.send\(\)" with distinct integers under a barrier$"#
)]
async fn when_20_concurrent_send(world: &mut TellWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let n = 20u64;
    world.sent_ints = (0..n).collect();
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor
                    .tell(SlowNum(i))
                    .send()
                    .await
                    .expect("bounded send delivers");
            })
        })
        .collect();
    for h in handles {
        h.await.expect("send task join");
    }
}

#[then(regex = r"^every integer is recorded exactly once once all sends complete$")]
async fn then_every_int_recorded_once(world: &mut TellWorld) {
    let expected: HashSet<u64> = world.sent_ints.iter().copied().collect();
    let log = world.log.as_ref().expect("log").clone();
    settle_log_set(&log, &expected).await;
    let recorded = log.lock().unwrap().clone();
    for i in &world.sent_ints {
        let count = recorded.iter().filter(|&&x| x == *i).count();
        assert_eq!(
            count, 1,
            "integer {i} must be recorded exactly once, got {count}"
        );
    }
    assert_eq!(
        recorded.len(),
        world.sent_ints.len(),
        "exactly the 20 distinct integers must be recorded, none lost or duplicated"
    );
    world.actor.as_ref().expect("actor").kill();
}

// ===========================================================================
// Paused-clock drivers (@timing)
// ===========================================================================

/// Runs `body` against a freshly-spawned, permanently-full bounded(1) `Told`
/// actor inside a dedicated current-thread `start_paused(true)` runtime on a
/// blocking thread, returning the tell outcome. The two parked `Hold` handlers
/// keep the single slot busy so the body's `send` must wait on capacity and its
/// `mailbox_timeout` fires deterministically under the paused clock.
async fn with_paused_full1<M, F>(body: F) -> Result<(), SendError<M>>
where
    M: Send + 'static,
    F: FnOnce(
            ActorRef<Told>,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError<M>>>>>
        + Send
        + 'static,
{
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused current-thread runtime");
        rt.block_on(async move {
            let log = Arc::new(Mutex::new(Vec::new()));
            let actor = Told::spawn_with_mailbox(Told::new(log), mailbox::bounded(1));
            actor.wait_for_startup().await;
            // Occupy the single slot permanently: a parked Hold is dequeued into the
            // handler (freeing the slot), a second occupies the one buffer slot.
            let (_tx, rx) = watch::channel(false);
            actor
                .tell(Hold(rx.clone()))
                .send()
                .await
                .expect("first hold");
            tokio::time::sleep(Duration::from_millis(1)).await;
            actor
                .tell(Hold(rx))
                .try_send()
                .expect("second hold fills slot");
            let outcome = body(actor.clone()).await;
            actor.kill();
            outcome
        })
    })
    .await
    .expect("paused runtime thread join")
}

/// Drives one `send_after(delay)` lifecycle inside a dedicated paused
/// current-thread runtime, returning `(handle_resolved_ok, delivered)`. With the
/// paused clock auto-advancing over the delay, awaiting the handle resolves the
/// scheduled send deterministically; delivery is confirmed by the recorded log.
async fn run_send_after(delay: Duration) -> (bool, bool) {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async move {
            let log = Arc::new(Mutex::new(Vec::new()));
            let actor =
                Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(100));
            actor.wait_for_startup().await;
            let handle = actor.tell(Msg).send_after(delay);
            // Awaiting the handle advances the paused clock past `delay` and runs
            // the deferred send; it resolves to the send's Result<(), SendError>.
            let handle_ok = handle.await.expect("send_after task join").is_ok();
            // Settle: the sentinel 0 must be recorded once the scheduled send lands.
            let mut delivered = false;
            for _ in 0..400 {
                if log.lock().unwrap().contains(&0) {
                    delivered = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            actor.kill();
            (handle_ok, delivered)
        })
    })
    .await
    .expect("paused runtime thread join")
}

/// Drives a `send_after(delay)` that is ABORTED before the delay elapses,
/// returning `(delivered, await_reported_cancellation)`. The abort is issued
/// SYNCHRONOUSLY (before any `.await` yields), so the paused clock never advances
/// to the scheduled send and the message is provably never delivered.
async fn run_send_after_abort(delay: Duration) -> (bool, bool) {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async move {
            let log = Arc::new(Mutex::new(Vec::new()));
            let actor =
                Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(100));
            actor.wait_for_startup().await;
            let handle = actor.tell(Msg).send_after(delay);
            // Abort BEFORE the scheduled task ever gets to run: the spawned task is
            // parked on sleep(delay), and we cancel it synchronously here.
            handle.abort();
            let cancelled = match handle.await {
                Ok(_) => false,
                Err(join_err) => join_err.is_cancelled(),
            };
            // Give the (cancelled) task ample paused-clock budget to NOT deliver:
            // advance well past the delay; nothing should ever be recorded.
            tokio::time::sleep(delay + Duration::from_secs(1)).await;
            let delivered = log.lock().unwrap().contains(&0);
            actor.kill();
            (delivered, cancelled)
        })
    })
    .await
    .expect("paused runtime thread join")
}

/// Drives a `send_after(delay)` whose target actor is stopped (and shutdown
/// completes) BEFORE the delay elapses, returning whether the handle resolved to
/// `Err(SendError::ActorNotRunning(Msg))`.
async fn run_send_after_stopped(delay: Duration) -> bool {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async move {
            let log = Arc::new(Mutex::new(Vec::new()));
            let actor =
                Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(100));
            actor.wait_for_startup().await;
            let handle = actor.tell(Msg).send_after(delay);
            // Stop + fully shut down before the delay elapses (the deferred send
            // will then hit a closed mailbox). Done before advancing the clock.
            actor.stop_gracefully().await.expect("graceful stop");
            actor.wait_for_shutdown().await;
            // Now awaiting advances past the delay; the deferred send runs against
            // the closed mailbox and returns ActorNotRunning.
            let outcome = handle.await.expect("send_after task join");
            matches!(outcome, Err(SendError::ActorNotRunning(Msg)))
        })
    })
    .await
    .expect("paused runtime thread join")
}

// ===========================================================================
// Self-tell deadlock-warning capture
// ===========================================================================

/// Spawns a `SelfTeller`, installs a per-thread tracing subscriber that records
/// whether a WARN-level event mentioning the deadlock message and the call site
/// fires, drives the in-handler bounded self-tell, and returns
/// `(warning_emitted, self_tell_returned_ok)`.
///
/// The warning is gated on `debug_assertions` + the `tracing` feature (both on
/// for the test build). It is emitted via `tracing::warn!("At {called_at}, {msg}")`
/// inside the actor's run-loop task; a `DefaultGuard` per-thread subscriber only
/// captures events on the SAME thread, so the whole thing runs inside one
/// dedicated current-thread runtime on a blocking thread with the guard held for
/// the actor's lifetime.
async fn capture_self_tell() -> (bool, bool) {
    use std::sync::atomic::{AtomicBool, Ordering};

    tokio::task::spawn_blocking(|| {
        let warned = Arc::new(AtomicBool::new(false));
        let layer = CaptureLayer {
            warned: Arc::clone(&warned),
        };
        let subscriber = tracing_subscriber::registry::Registry::default();
        use tracing_subscriber::layer::SubscriberExt;
        let subscriber = subscriber.with(layer);
        let _guard: DefaultGuard = tracing::subscriber::set_default(subscriber);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let self_ok = rt.block_on(async {
            let result = Arc::new(Mutex::new(None));
            let actor = SelfTeller::spawn_with_mailbox(
                SelfTeller {
                    result: Arc::clone(&result),
                },
                mailbox::bounded(8),
            );
            actor.wait_for_startup().await;
            actor
                .tell(SelfTell)
                .send()
                .await
                .expect("trigger self-tell");
            // Settle until the in-handler self-tell recorded its outcome.
            let mut ok = None;
            for _ in 0..400 {
                if let Some(v) = *result.lock().unwrap() {
                    ok = Some(v);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            actor.kill();
            ok.expect("the self-tell handler recorded an outcome")
        });
        (warned.load(Ordering::SeqCst), self_ok)
    })
    .await
    .expect("self-tell capture thread join")
}

/// A minimal tracing layer that flips `warned` when a WARN event whose formatted
/// message mentions the bounded self-tell deadlock guidance fires.
struct CaptureLayer {
    warned: Arc<std::sync::atomic::AtomicBool>,
}

impl<S> tracing_subscriber::Layer<S> for CaptureLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains("sending a `tell` request to itself")
            || visitor.0.contains("may lead to a deadlock")
        {
            self.warned.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// Collects the formatted `message` field of a tracing event into a String.
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

// ===========================================================================
// on_start blocker actor (lifecycle buffering)
// ===========================================================================

/// An actor whose `on_start` parks on a `watch` gate until released, so a tell
/// issued before startup is buffered by the mailbox rather than rejected.
struct Blocker {
    log: Arc<Mutex<Vec<u64>>>,
    gate: watch::Receiver<bool>,
}

impl Actor for Blocker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(mut state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        while !*state.gate.borrow() {
            if state.gate.changed().await.is_err() {
                break;
            }
        }
        Ok(state)
    }
}

struct BlockerMsg;

impl Message<BlockerMsg> for Blocker {
    type Reply = ();

    async fn handle(&mut self, _msg: BlockerMsg, _ctx: &mut Context<Self, Self::Reply>) {
        self.log.lock().unwrap().push(7);
    }
}

// ===========================================================================
// @property / @model laws (request_tell.properties.feature)
// ===========================================================================

// -- @property @boundary: try_send Ok iff k < c, MailboxFull iff k == c ------

#[given(regex = r"^a bounded mailbox of any capacity c whose actor is parked so it never drains$")]
async fn law_given_parked_any_c(_world: &mut TellWorld) {}

#[given(regex = r"^the mailbox already holds any k buffered messages with k in \[0, c\]$")]
async fn law_given_k_buffered(_world: &mut TellWorld) {}

#[when(regex = r#"^one more message is offered with "tell\(n\)\.try_send\(\)"$"#)]
async fn law_when_offer_one_more(_world: &mut TellWorld) {}

#[then(
    regex = r"^it returns Ok\(\(\)\) iff k < c and SendError::MailboxFull\(n\) iff k == c, with no waiting$"
)]
async fn law_try_send_predicate(_world: &mut TellWorld) {
    // GEN: c ∈ boundary-biased {1, 2, 64, 1024}; k ∈ {0, 1, c-1, c}. The actor is
    // held in a never-returning parked Hold handler so the buffered count is
    // deterministic. ORACLE: the predicate k < c. With one Hold dequeued into the
    // parked handler, the buffer holds (k) of c slots; offering one more must be
    // Ok iff there is a free buffer slot (k < c), else MailboxFull(n) — and never
    // wait (tx.try_send, tell.rs:169-181).
    for c in [1usize, 2, 64, 1024] {
        for k in distinct_sorted([0usize, 1, c.saturating_sub(1), c]) {
            assert_try_send_at_fill(c, k).await;
        }
    }
}

/// Drives one (capacity c, buffered k) case: parks the actor, fills the buffer to
/// exactly `k` of `c` slots, then asserts `tell(n).try_send()` is Ok iff k < c and
/// `MailboxFull(n)` iff k == c. The first send is dequeued into the parked handler
/// (it does not consume a buffer slot), so the buffer count is controlled exactly.
async fn assert_try_send_at_fill(c: usize, k: usize) {
    let (actor, _log) = spawn_bounded(c).await;
    let (release_tx, release_rx) = watch::channel(false);
    // Dequeue one Hold into the (parked) handler, freeing it from the buffer.
    actor
        .tell(Hold(release_rx.clone()))
        .send()
        .await
        .expect("first hold dequeued into handler");
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Now fill exactly k buffer slots with parked Holds.
    for _ in 0..k {
        actor
            .tell(Hold(release_rx.clone()))
            .try_send()
            .expect("buffering a Hold up to k must succeed");
    }
    // Settle so the buffer count is observable before the probe.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let n = 999u64;
    let outcome = actor.tell(Num(n)).try_send();
    if k < c {
        assert_eq!(
            outcome,
            Ok(()),
            "c={c} k={k}: k < c must accept the offer with Ok(())"
        );
    } else {
        assert_eq!(
            outcome,
            Err(SendError::MailboxFull(Num(n))),
            "c={c} k={k}: k == c must refuse with MailboxFull(n)"
        );
    }
    let _ = release_tx.send(true);
    actor.kill();
}

/// Returns the distinct values of `xs`, sorted, dropping the noisy `c-1`
/// duplicate when `c <= 1`.
fn distinct_sorted<const N: usize>(xs: [usize; N]) -> Vec<usize> {
    let mut v: Vec<usize> = xs.to_vec();
    v.sort_unstable();
    v.dedup();
    v
}

// -- @model @linearizability: Ok try_send recorded once; MailboxFull never ---

#[given(
    regex = r"^a bounded mailbox of any capacity c whose handler records each integer it receives$"
)]
async fn law_given_any_c_records(_world: &mut TellWorld) {}

#[when(
    regex = r#"^N callers concurrently invoke "tell\(n\)\.try_send\(\)" with distinct integers under a barrier$"#
)]
async fn law_when_n_try_send(_world: &mut TellWorld) {}

#[then(regex = r"^every call that returned Ok\(\(\)\) has its integer recorded exactly once$")]
async fn law_model_ok_recorded_once(_world: &mut TellWorld) {
    // GEN: N ∈ {2, 16, 128} (include N > c); c ∈ {1, 8, 64}. Real overlap via
    // tokio::spawn + Barrier. ORACLE: partition offers by their own return value;
    // the recorded set equals exactly the Ok set (each once). The accepted-then-
    // lost and accept-twice bugs both fail this.
    for (n, c) in [(2u64, 1usize), (16, 8), (128, 64)] {
        run_try_send_model(n, c).await;
    }
}

#[then(regex = r"^every call that returned MailboxFull has its integer never recorded$")]
async fn law_model_full_never_recorded(_world: &mut TellWorld) {
    // The And-line of the same law: re-run a representative (N > c) case so this
    // Then is a real assertion. run_try_send_model asserts BOTH directions.
    run_try_send_model(128, 8).await;
}

#[then(regex = r"^the recorded set equals exactly the set of Ok integers — no loss, no duplicate$")]
async fn law_model_recorded_set_equals_ok(_world: &mut TellWorld) {
    run_try_send_model(64, 8).await;
}

/// One (N callers, capacity c) concurrent-try_send case with REAL overlap. Each
/// caller's OWN return value classifies its integer; after releasing the parked
/// fill, the recorded set must equal exactly the Ok set (no loss, no duplicate)
/// and no MailboxFull integer is recorded.
async fn run_try_send_model(n: u64, c: usize) {
    let (actor, log) = spawn_bounded(c).await;
    // Park every buffer slot so the slot count is deterministic during the race.
    let (release_tx, release_rx) = watch::channel(false);
    actor
        .tell(Hold(release_rx.clone()))
        .send()
        .await
        .expect("first hold dequeued");
    tokio::time::sleep(Duration::from_millis(10)).await;
    for _ in 0..c {
        let _ = actor.tell(Hold(release_rx.clone())).try_send();
    }
    tokio::time::sleep(Duration::from_millis(10)).await;

    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                match actor.tell(Num(i)).try_send() {
                    Ok(()) => (i, true),
                    Err(SendError::MailboxFull(Num(_))) => (i, false),
                    Err(other) => panic!("unexpected try_send error: {other:?}"),
                }
            })
        })
        .collect();
    let mut ok_set: HashSet<u64> = HashSet::new();
    let mut full_set: HashSet<u64> = HashSet::new();
    for h in handles {
        let (i, ok) = h.await.expect("try_send model task join");
        if ok {
            ok_set.insert(i);
        } else {
            full_set.insert(i);
        }
    }
    // Release the parked Holds so accepted Nums get handled+recorded.
    let _ = release_tx.send(true);
    settle_log_set(&log, &ok_set).await;
    let recorded: Vec<u64> = log.lock().unwrap().clone();
    // Each Ok integer recorded exactly once.
    for i in &ok_set {
        let count = recorded.iter().filter(|&&x| x == *i).count();
        assert_eq!(
            count, 1,
            "N={n} c={c}: Ok integer {i} recorded {count} times"
        );
    }
    // No MailboxFull integer recorded.
    let recorded_set: HashSet<u64> = recorded.iter().copied().collect();
    for i in &full_set {
        assert!(
            !recorded_set.contains(i),
            "N={n} c={c}: MailboxFull integer {i} must never be recorded"
        );
    }
    // Recorded set == Ok set exactly.
    assert_eq!(
        recorded_set, ok_set,
        "N={n} c={c}: recorded set must equal exactly the Ok set"
    );
    actor.kill();
}

// -- @model @sequence: bounded send under backpressure delivers once ---------

#[given(
    regex = r"^a bounded mailbox of any capacity c whose handler records each integer after a short delay$"
)]
async fn law_given_any_c_records_slow(_world: &mut TellWorld) {}

#[when(
    regex = r#"^N callers concurrently invoke "tell\(n\)\.send\(\)" with distinct integers under a barrier$"#
)]
async fn law_when_n_send(_world: &mut TellWorld) {}

#[then(
    regex = r"^once all sends complete, every integer is recorded exactly once with none lost or duplicated$"
)]
async fn law_model_send_exactly_once(_world: &mut TellWorld) {
    // GEN: N ∈ {1, 8, 64} (include N >> c); c ∈ {1, 2, 8}. send() backpressures on
    // a full bounded mailbox (tx.send) rather than dropping, so total delivery is
    // preserved. ORACLE: the multiset of N distinct integers; the recorded
    // multiset must equal it exactly.
    for (n, c) in [(1u64, 1usize), (8, 2), (64, 8)] {
        run_send_backpressure_model(n, c).await;
    }
}

/// One (N callers, capacity c) concurrent bounded-send case with REAL overlap.
/// Bounded send backpressures rather than dropping; after all sends complete every
/// distinct integer must be recorded exactly once.
async fn run_send_backpressure_model(n: u64, c: usize) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let actor = Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(c));
    actor.wait_for_startup().await;
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor
                    .tell(SlowNum(i))
                    .send()
                    .await
                    .expect("bounded send delivers");
            })
        })
        .collect();
    for h in handles {
        h.await.expect("send model task join");
    }
    let expected: HashSet<u64> = (0..n).collect();
    settle_log_set(&log, &expected).await;
    let recorded = log.lock().unwrap().clone();
    for i in 0..n {
        let count = recorded.iter().filter(|&&x| x == i).count();
        assert_eq!(count, 1, "N={n} c={c}: integer {i} recorded {count} times");
    }
    assert_eq!(
        recorded.len() as u64,
        n,
        "N={n} c={c}: exactly N integers recorded, none lost or duplicated"
    );
    actor.kill();
}

// -- @model @lifecycle @timing: send_after delivers once or aborted never ----

#[given(regex = r"^a bounded mailbox of capacity 100 and a handler that records each integer$")]
async fn law_given_cap100_records(_world: &mut TellWorld) {}

#[when(
    regex = r#"^the caller invokes "tell\(n\)\.send_after\(d\)" and then either awaits or aborts the JoinHandle at any point$"#
)]
async fn law_when_send_after_await_or_abort(_world: &mut TellWorld) {}

#[then(
    regex = r"^if the handle is allowed to fire, n is delivered exactly once and the handle resolves Ok\(\(\)\)$"
)]
async fn law_model_send_after_fires(_world: &mut TellWorld) {
    // GEN: d ∈ boundary-biased {ZERO, 1ms, 50ms, 1s}; for the fire case, never
    // abort. ORACLE: a one-shot model — fired ⇒ delivered-count == 1 and the
    // handle resolves Ok(()). Paused clock makes the delay deterministic.
    for d in [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_millis(50),
        Duration::from_secs(1),
    ] {
        let (handle_ok, delivered_count) = run_send_after_model_fire(d).await;
        assert!(
            handle_ok,
            "d={d:?}: a fired send_after handle must resolve Ok(())"
        );
        assert_eq!(
            delivered_count, 1,
            "d={d:?}: a fired send_after delivers exactly once"
        );
    }
}

#[then(
    regex = r"^if the handle is aborted before the delay elapses, n is never delivered and the await reports cancellation$"
)]
async fn law_model_send_after_aborted(_world: &mut TellWorld) {
    // For each boundary d: abort BEFORE the delay elapses (synchronously, before
    // the clock advances). ORACLE: delivered-count == 0 and the await reports
    // cancellation. ZERO is excluded — a zero-delay send fires on the immediately-
    // next tick, so "before the delay elapses" is not a reachable abort window.
    for d in [
        Duration::from_millis(1),
        Duration::from_millis(50),
        Duration::from_secs(1),
    ] {
        let (delivered, cancelled) = run_send_after_abort(d).await;
        assert!(
            !delivered,
            "d={d:?}: an aborted send_after must never deliver"
        );
        assert!(
            cancelled,
            "d={d:?}: awaiting an aborted handle must report cancellation"
        );
    }
}

/// Drives one fired `send_after(d)` under a paused clock, returning
/// `(handle_resolved_ok, delivered_count)` for the model's one-shot oracle.
async fn run_send_after_model_fire(d: Duration) -> (bool, usize) {
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async move {
            let log = Arc::new(Mutex::new(Vec::new()));
            let actor =
                Told::spawn_with_mailbox(Told::new(Arc::clone(&log)), mailbox::bounded(100));
            actor.wait_for_startup().await;
            let handle = actor.tell(Num(5)).send_after(d);
            let handle_ok = handle.await.expect("send_after task join").is_ok();
            let mut count = 0usize;
            for _ in 0..400 {
                count = log.lock().unwrap().iter().filter(|&&x| x == 5).count();
                if count >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            actor.kill();
            (handle_ok, count)
        })
    })
    .await
    .expect("paused runtime thread join")
}
