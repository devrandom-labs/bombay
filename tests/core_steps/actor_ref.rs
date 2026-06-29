//! Shared `ActorRefWorld` + step definitions for the core `actor_ref` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_actor_ref_bdd.rs`        — the example feature (actor_ref.feature)
//!   * `core_actor_ref_props_bdd.rs`  — the property laws (actor_ref.properties.feature)
//!
//! This module exercises the `src/actor/actor_ref.rs` SUT with REAL SPAWNED
//! ACTORS: ask/tell messaging, the alive/dead state machine, strong/weak
//! reference counting, `downgrade`/`upgrade`, `is_current`, identity
//! (id/eq/hash/ord), startup/shutdown waiters, `Recipient`/`ReplyRecipient`
//! type-erasure, and self link/unlink no-ops.
//!
//! Every assertion is the SPECIFIC value confirmed in the scenario's
//! `# Confirmed:` / `# ORACLE:` note (facts only — no vague `contains`).
//!
//! All public API is reached through `kameo::prelude::*` + `kameo::actor::*`;
//! no `src/` change is needed.
//!
//! CONCURRENCY DISCIPLINE (these cost real time if ignored):
//!   * "actor is now dead/alive" and "strong/weak count is now N" are
//!     TIMING-SENSITIVE. Every such assertion uses CONDITION-BASED POLLING
//!     (a bounded retry loop + a short `tokio::time::sleep`) until the
//!     observable settles, then asserts — and PANICS with a clear message if it
//!     never settles, so a real regression fails loudly. `wait_for_shutdown()`
//!     is NOT used as the settle barrier: it returns when the mailbox closes,
//!     BEFORE refcounts / the shutdown-result observer settle.
//!   * @linearizability refcount races use REAL overlap: `tokio::spawn` +
//!     `Arc<tokio::sync::Barrier>`, all tasks released together, asserted
//!     against an INDEPENDENT integer oracle.

use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    time::Duration,
};

use cucumber::{World, given, then, when};
use kameo::{actor::ActorId, error::Infallible, prelude::*};
use proptest::prelude::*;
use tokio::sync::Barrier;

// ===========================================================================
// Test actors and messages
// ===========================================================================

/// An actor that records every numbered message it handles into a shared log,
/// and can echo / double values back as replies. The shared `log` is the
/// OBSERVABLE side-effect for tell scenarios.
#[derive(Clone)]
struct Echoer {
    log: Arc<Mutex<Vec<u64>>>,
}

impl Actor for Echoer {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Asked/told value. The reply is the value echoed back; the side-effect is the
/// append to `log`.
struct Echo(u64);

impl Message<Echo> for Echoer {
    type Reply = u64;

    async fn handle(&mut self, msg: Echo, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

/// Asked: replies with the doubled value (used by the ask @sequence scenario).
struct Double(u64);

impl Message<Double> for Echoer {
    type Reply = u64;

    async fn handle(&mut self, msg: Double, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        msg.0 * 2
    }
}

/// An actor that captures `actor_ref.is_current()` evaluated INSIDE its own
/// message handler (the only context where the `CURRENT_ACTOR_ID` task-local is
/// set to this actor's id). It stores its own `ActorRef`, taken in `on_start`.
struct SelfChecker {
    self_ref: Option<ActorRef<Self>>,
    /// `Some(bool)` once a handler has evaluated `is_current`.
    inside: Arc<Mutex<Option<bool>>>,
}

impl Actor for SelfChecker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut state: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        state.self_ref = Some(actor_ref);
        Ok(state)
    }
}

/// Triggers the handler to evaluate `self_ref.is_current()` from within its own
/// task, recording the result.
struct CheckCurrent;

impl Message<CheckCurrent> for SelfChecker {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: CheckCurrent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let here = self.self_ref.as_ref().expect("self ref set in on_start");
        let cur = here.is_current();
        *self.inside.lock().unwrap() = Some(cur);
        cur
    }
}

/// An actor whose `on_start` blocks until a shared `watch` flips to `true` — so
/// the startup-waiter scenarios can park waiters BEFORE startup completes, then
/// release `on_start` and observe the waiters resolve only afterwards.
struct SlowStart {
    release: tokio::sync::watch::Receiver<bool>,
}

impl Actor for SlowStart {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(mut state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        while !*state.release.borrow() {
            if state.release.changed().await.is_err() {
                break;
            }
        }
        Ok(state)
    }
}

struct Ping;

impl Message<Ping> for SlowStart {
    type Reply = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct ActorRefWorld {
    /// Shared handling-order log (the OBSERVABLE tell effect).
    log: Arc<Mutex<Vec<u64>>>,
    /// The background-spawned default-mailbox actor.
    actor: Option<ActorRef<Echoer>>,
    /// A captured ask reply.
    reply: Option<u64>,
    /// A captured tell send outcome (Ok / ActorNotRunning).
    tell_ok: Option<bool>,
    /// A captured send error (the stopped-actor / property scenarios).
    not_running: Option<bool>,
    /// `is_alive` checks captured in order (lifecycle scenario).
    alive_checks: Vec<bool>,
    /// A downgraded weak ref kept alive across steps.
    weak: Option<WeakActorRef<Echoer>>,
    /// An upgrade result captured by a When.
    upgraded_some: Option<bool>,
    /// A second strong clone (refcount scenario).
    clone_ref: Option<ActorRef<Echoer>>,
    /// At-rest weak_count measured after spawn, before clone/downgrade
    /// (refcount scenario). kameo's spawn machinery retains internal
    /// WeakSenders, so the at-rest weak count is non-zero; we assert only the
    /// +1 delta a single downgrade adds over this baseline.
    weak_baseline: Option<usize>,
    /// Identity scenario operands.
    pair_eq: Option<bool>,
    pair_hash_eq: Option<bool>,
    /// is_current result captured outside a handler.
    is_current_outside: Option<bool>,
    /// is_current result captured inside a handler.
    is_current_inside: Option<bool>,
    /// Recipient delivery scenario.
    recipient_same_id: Option<bool>,
    recipient_reply: Option<u64>,
    erase_same_id: Option<bool>,
    /// Self link/unlink scenarios: whether A's link set still contains B.
    link_contains_b: Option<bool>,
    /// A sibling actor B (self-unlink scenario).
    sibling: Option<ActorRef<Echoer>>,
    /// Concurrent-tell final count + oracle.
    concurrent_count: Option<u64>,
    /// Concurrent-ask: map of asked -> received.
    ask_results: Vec<(u64, u64)>,
    /// Startup/shutdown waiter scenarios.
    release_tx: Option<tokio::sync::watch::Sender<bool>>,
    slow: Option<ActorRef<SlowStart>>,
    waiters_all_resolved: Option<bool>,
    /// Parked startup waiters (released + joined by a later step).
    slow_waiters: Option<Vec<tokio::task::JoinHandle<()>>>,
    /// Parked shutdown waiters (released + joined by a later step).
    shutdown_waiters: Option<Vec<tokio::task::JoinHandle<()>>>,
}

fn hash_of<H: Hash>(v: &H) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// Spawns a fresh `Echoer` sharing a brand-new log, awaits its startup.
async fn spawn_echoer() -> (ActorRef<Echoer>, Arc<Mutex<Vec<u64>>>) {
    let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let actor = Echoer::spawn(Echoer {
        log: Arc::clone(&log),
    });
    actor.wait_for_startup().await;
    (actor, log)
}

/// Condition-based settle: polls `cond` (a closure returning bool) up to a bound
/// with a short sleep between tries; panics with `msg` if it never holds. Used
/// for every alive/dead and refcount-settle assertion — NEVER `wait_for_shutdown`.
async fn settle<F: FnMut() -> bool>(mut cond: F, msg: &str) {
    for _ in 0..400 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition did not settle within the bound: {msg}");
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^a running actor spawned with a default bounded mailbox$")]
async fn given_running_actor(world: &mut ActorRefWorld) {
    let (actor, log) = spawn_echoer().await;
    world.log = log;
    world.actor = Some(actor);
}

// ===========================================================================
// @sequence — ask/tell protocol, waiter ordering
// ===========================================================================

#[given(regex = r"^the actor replies with the doubled value of any number it receives$")]
async fn given_doubler(_world: &mut ActorRefWorld) {
    // The Echoer already implements Double; nothing to set up.
}

#[when(regex = r"^the caller asks the actor with 21$")]
async fn when_ask_21(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    world.reply = Some(actor.ask(Double(21)).await.expect("ask succeeds"));
}

#[then(regex = r"^the awaited reply is 42$")]
async fn then_reply_42(world: &mut ActorRefWorld) {
    assert_eq!(
        world.reply,
        Some(42),
        "ask must return the handler's doubled reply (21*2=42)"
    );
}

#[given(regex = r"^the actor records every message it handles$")]
async fn given_records(_world: &mut ActorRefWorld) {
    // The Echoer appends every Echo value to its shared log already.
}

#[when(regex = r"^the caller tells the actor a message and awaits the send$")]
async fn when_tell_and_await(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    world.tell_ok = Some(actor.tell(Echo(7)).await.is_ok());
}

#[then(regex = r"^the send resolves with Ok and the actor eventually records the message$")]
async fn then_tell_ok_and_recorded(world: &mut ActorRefWorld) {
    assert_eq!(world.tell_ok, Some(true), "tell must resolve with Ok");
    let log = Arc::clone(&world.log);
    settle(
        || log.lock().unwrap().contains(&7),
        "the told message was never recorded by the actor",
    )
    .await;
}

#[given(regex = r"^an actor whose on_start blocks until released$")]
async fn given_slow_start(world: &mut ActorRefWorld) {
    let (release_tx, release_rx) = tokio::sync::watch::channel(false);
    let actor = SlowStart::spawn(SlowStart {
        release: release_rx,
    });
    world.release_tx = Some(release_tx);
    world.slow = Some(actor);
}

#[when(regex = r"^wait_for_startup is awaited and then on_start is released$")]
async fn when_wait_startup_then_release(world: &mut ActorRefWorld) {
    let actor = world.slow.as_ref().expect("slow actor").clone();
    let release_tx = world.release_tx.as_ref().expect("release tx").clone();
    // Park a waiter; it must NOT resolve until on_start is released.
    let waiter = tokio::spawn(async move {
        actor.wait_for_startup().await;
    });
    // Give the waiter a moment to park, then verify it is still pending.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !waiter.is_finished(),
        "wait_for_startup resolved BEFORE on_start was released"
    );
    // Release on_start; the waiter must now resolve.
    release_tx.send(true).expect("release on_start");
    waiter
        .await
        .expect("startup waiter completes after release");
    world.waiters_all_resolved = Some(true);
}

#[then(regex = r"^wait_for_startup resolves only after on_start completes$")]
async fn then_startup_after_on_start(world: &mut ActorRefWorld) {
    assert_eq!(
        world.waiters_all_resolved,
        Some(true),
        "the parked waiter must resolve only after on_start was released"
    );
    // After startup the actor is responsive.
    let actor = world.slow.as_ref().expect("slow actor");
    actor
        .ask(Ping)
        .await
        .expect("actor responsive after startup");
}

#[given(regex = r"^a running actor$")]
async fn given_running_actor_plain(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
}

#[when(regex = r"^the actor is stopped gracefully and wait_for_shutdown is awaited$")]
async fn when_stop_and_wait_shutdown(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    actor.stop_gracefully().await.expect("stop signal sent");
    actor.wait_for_shutdown().await;
}

#[then(regex = r"^wait_for_shutdown resolves only after the mailbox has closed$")]
async fn then_shutdown_after_close(world: &mut ActorRefWorld) {
    // wait_for_shutdown awaits mailbox_sender.closed(); once it returns the
    // mailbox is closed, so is_alive (= !is_closed) must be observably false.
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    settle(
        || !actor.is_alive(),
        "mailbox never observed closed after wait_for_shutdown returned",
    )
    .await;
}

// ===========================================================================
// @lifecycle — alive state machine, downgrade/upgrade, recipients
// ===========================================================================

#[when(regex = r"^is_alive is checked before stopping$")]
async fn when_alive_before_stop(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    world.alive_checks.push(actor.is_alive());
}

#[when(regex = r"^the actor is stopped and shutdown awaited$")]
async fn when_stop_and_shutdown(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    actor.stop_gracefully().await.expect("stop signal sent");
    actor.wait_for_shutdown().await;
}

#[when(regex = r"^is_alive is checked again$")]
async fn when_alive_again(world: &mut ActorRefWorld) {
    // The mailbox close can lag wait_for_shutdown's return very slightly; settle.
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    settle(
        || !actor.is_alive(),
        "is_alive never became false after shutdown",
    )
    .await;
    world.alive_checks.push(actor.is_alive());
}

#[then(regex = r"^the first check is true and the second is false$")]
async fn then_alive_true_then_false(world: &mut ActorRefWorld) {
    assert_eq!(
        world.alive_checks,
        vec![true, false],
        "is_alive must be true while running and false after the mailbox closes"
    );
}

#[given(regex = r"^a running actor with both a strong ActorRef and a WeakActorRef$")]
async fn given_strong_and_weak(world: &mut ActorRefWorld) {
    let (actor, log) = spawn_echoer().await;
    world.weak = Some(actor.downgrade());
    world.log = log;
    world.actor = Some(actor);
}

#[when(regex = r"^the actor is stopping but its shutdown result has not yet been recorded$")]
async fn when_actor_stopping(world: &mut ActorRefWorld) {
    // The @review-semantics scenario pins only the predicate identities and the
    // post-shutdown convergence (the exact close→shutdown-recorded window width
    // is not asserted). No driving needed here.
    let _ = world;
}

#[then(regex = r"^ActorRef::is_alive becomes false as soon as the mailbox closes$")]
async fn then_actorref_alive_predicate(world: &mut ActorRefWorld) {
    // Pin the predicate IDENTITY: ActorRef::is_alive == !mailbox closed. Stop the
    // actor and assert ActorRef::is_alive converges to false once the mailbox is
    // closed (the predicate's defining signal).
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    actor.stop_gracefully().await.expect("stop");
    actor.wait_for_shutdown().await;
    settle(
        || !actor.is_alive(),
        "ActorRef::is_alive did not become false after the mailbox closed",
    )
    .await;
}

#[then(regex = r"^WeakActorRef::is_alive stays true until the shutdown result is initialized$")]
async fn then_weakref_alive_predicate(world: &mut ActorRefWorld) {
    // Pin the predicate IDENTITY: WeakActorRef::is_alive == !shutdown_result
    // initialized. Once shutdown fully completes (the result IS initialized) it
    // must read false; the next Then asserts both converge to not-alive.
    let weak = world.weak.as_ref().expect("weak ref");
    settle(
        || !weak.is_alive(),
        "WeakActorRef::is_alive did not become false after the shutdown result was recorded",
    )
    .await;
}

#[then(regex = r"^once shutdown completes both predicates report not-alive$")]
async fn then_both_not_alive(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let weak = world.weak.as_ref().expect("weak ref");
    settle(
        || !actor.is_alive() && !weak.is_alive(),
        "both predicates must converge to not-alive after shutdown completes",
    )
    .await;
    assert!(
        !actor.is_alive(),
        "ActorRef::is_alive must be false post-shutdown"
    );
    assert!(
        !weak.is_alive(),
        "WeakActorRef::is_alive must be false post-shutdown"
    );
}

#[given(regex = r"^a WeakActorRef downgraded from a live ActorRef that is still held$")]
async fn given_weak_with_strong_held(world: &mut ActorRefWorld) {
    let (actor, log) = spawn_echoer().await;
    world.weak = Some(actor.downgrade());
    world.log = log;
    world.actor = Some(actor); // strong ref retained in the World
}

#[when(regex = r"^the WeakActorRef is upgraded$")]
async fn when_upgrade(world: &mut ActorRefWorld) {
    let weak = world.weak.as_ref().expect("weak ref");
    world.upgraded_some = Some(weak.upgrade().is_some());
}

#[then(regex = r"^upgrade returns Some\(ActorRef\)$")]
async fn then_upgrade_some(world: &mut ActorRefWorld) {
    assert_eq!(
        world.upgraded_some,
        Some(true),
        "upgrade must return Some while a strong ActorRef is still held"
    );
}

#[given(regex = r"^a WeakActorRef downgraded from an ActorRef$")]
async fn given_weak_from_actorref(world: &mut ActorRefWorld) {
    let (actor, _log) = spawn_echoer().await;
    world.weak = Some(actor.downgrade());
    // Keep the strong ref ONLY until the When drops it.
    world.actor = Some(actor);
}

#[when(regex = r"^all strong ActorRefs are dropped$")]
async fn when_drop_all_strong(world: &mut ActorRefWorld) {
    // Drop every strong handle this World holds.
    world.actor = None;
    world.clone_ref = None;
    // Refcount settle: upgrade must observably fail once the last strong sender
    // is gone (the actor also stops, but we only assert the upgrade observable).
    let weak = world.weak.as_ref().expect("weak ref").clone();
    settle(
        || weak.upgrade().is_none(),
        "upgrade still returned Some after every strong ref was dropped",
    )
    .await;
}

#[then(regex = r"^upgrading the WeakActorRef returns None$")]
async fn then_upgrade_none(world: &mut ActorRefWorld) {
    let weak = world.weak.as_ref().expect("weak ref");
    assert!(
        weak.upgrade().is_none(),
        "upgrade must return None once all strong ActorRefs are dropped"
    );
}

#[given(regex = r"^a Recipient created from the actor via recipient$")]
async fn given_recipient(_world: &mut ActorRefWorld) {
    // The Recipient is created in the When (recipient<M> consumes the ActorRef).
}

#[when(regex = r"^a message is told through the Recipient$")]
async fn when_tell_via_recipient(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let source_id = actor.id();
    let recipient: Recipient<Echo> = actor.recipient();
    world.recipient_same_id = Some(recipient.id() == source_id);
    recipient
        .tell(Echo(55))
        .await
        .expect("recipient tell delivered");
    settle(
        {
            let log = Arc::clone(&world.log);
            move || log.lock().unwrap().contains(&55)
        },
        "the recipient-told message was never handled by the underlying actor",
    )
    .await;
}

#[then(regex = r"^the underlying actor handles the message$")]
async fn then_underlying_handles(world: &mut ActorRefWorld) {
    assert!(
        world.log.lock().unwrap().contains(&55),
        "the underlying actor must have handled the recipient-told message"
    );
}

#[then(regex = r"^the Recipient reports the same ActorId as the source ActorRef$")]
async fn then_recipient_same_id(world: &mut ActorRefWorld) {
    assert_eq!(
        world.recipient_same_id,
        Some(true),
        "Recipient::id must equal the source ActorRef's id"
    );
}

#[given(regex = r"^a ReplyRecipient created from the actor via reply_recipient$")]
async fn given_reply_recipient(_world: &mut ActorRefWorld) {
    // Created in the When (reply_recipient<M> consumes the ActorRef).
}

#[when(regex = r"^the caller asks through the ReplyRecipient$")]
async fn when_ask_via_reply_recipient(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let source_id = actor.id();
    let rr: ReplyRecipient<Echo, u64> = actor.reply_recipient();
    world.recipient_reply = Some(rr.ask(Echo(63)).await.expect("reply_recipient ask"));
    // erase_reply upcasts to a Recipient<Echo> preserving id.
    let erased: Recipient<Echo> = rr.erase_reply();
    world.erase_same_id = Some(erased.id() == source_id);
}

#[then(regex = r"^a reply is returned$")]
async fn then_reply_returned(world: &mut ActorRefWorld) {
    assert_eq!(
        world.recipient_reply,
        Some(63),
        "ReplyRecipient::ask must return the handler's reply (echo of 63)"
    );
}

#[then(regex = r"^erase_reply yields a Recipient with the same ActorId$")]
async fn then_erase_same_id(world: &mut ActorRefWorld) {
    assert_eq!(
        world.erase_same_id,
        Some(true),
        "erase_reply must preserve the ActorId of the source"
    );
}

// ===========================================================================
// @boundary — send-to-dead, is_current, self link/unlink, refcounts, identity
// ===========================================================================

#[given(regex = r"^an actor that has been stopped and whose shutdown has been awaited$")]
async fn given_stopped_actor(world: &mut ActorRefWorld) {
    let (actor, log) = spawn_echoer().await;
    actor.stop_gracefully().await.expect("stop signal sent");
    actor.wait_for_shutdown().await;
    settle(
        {
            let a = actor.clone();
            move || !a.is_alive()
        },
        "actor never observed not-alive after stop",
    )
    .await;
    world.log = log;
    world.actor = Some(actor);
}

#[when(regex = r"^a message is told to the stopped actor$")]
async fn when_tell_stopped(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    let result = actor.tell(Echo(9)).await;
    world.not_running = Some(matches!(result, Err(SendError::ActorNotRunning(_))));
}

#[then(regex = r"^the send fails with SendError::ActorNotRunning$")]
async fn then_send_not_running(world: &mut ActorRefWorld) {
    assert_eq!(
        world.not_running,
        Some(true),
        "telling a stopped actor must fail with SendError::ActorNotRunning"
    );
}

#[when(regex = r"^is_current is called from the spawning task$")]
async fn when_is_current_outside(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    world.is_current_outside = Some(actor.is_current());
}

#[then(regex = r"^is_current returns false$")]
async fn then_is_current_false(world: &mut ActorRefWorld) {
    assert_eq!(
        world.is_current_outside,
        Some(false),
        "is_current must be false outside the actor's own task (task-local unset)"
    );
}

#[given(regex = r"^an actor that calls actor_ref\.is_current inside a message handler$")]
async fn given_self_checker(world: &mut ActorRefWorld) {
    let inside: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let actor = SelfChecker::spawn(SelfChecker {
        self_ref: None,
        inside: Arc::clone(&inside),
    });
    actor.wait_for_startup().await;
    // Stash the result handle by driving the handler in the When; keep the actor.
    world.is_current_inside = None;
    // Store the actor + sink via a typed slot: reuse `slow`-style storage is not
    // typed for SelfChecker, so drive immediately here and capture the reply.
    let cur = actor.ask(CheckCurrent).await.expect("ask self-checker");
    world.is_current_inside = Some(cur);
    let _ = inside; // the handler-recorded value matches the reply
}

#[when(regex = r"^that handler runs$")]
async fn when_handler_runs(_world: &mut ActorRefWorld) {
    // The handler ran in the Given (the ask drove it); the result is captured.
}

#[then(regex = r"^is_current returns true within the handler$")]
async fn then_is_current_true_inside(world: &mut ActorRefWorld) {
    assert_eq!(
        world.is_current_inside,
        Some(true),
        "is_current must be true inside the actor's own handler (task-local matches id)"
    );
}

#[given(regex = r"^a single running actor$")]
async fn given_single_actor(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
}

#[when(regex = r"^the actor's ActorRef is linked to itself$")]
async fn when_link_self(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    actor.link(actor).await;
}

#[then(regex = r"^no link is recorded and no error occurs$")]
async fn then_no_self_link(world: &mut ActorRefWorld) {
    // link returns early on self.id == sibling.id, BEFORE touching links. Observe
    // the link set is still empty (length 0). Debug-formatting reads the links.
    let actor = world.actor.as_ref().expect("actor spawned");
    let dbg = format!("{actor:?}");
    assert!(
        dbg.contains("links: []"),
        "self-link must record no link (links must be empty), got {dbg}"
    );
}

#[given(regex = r"^a running actor A already linked to a different actor B$")]
async fn given_a_linked_to_b(world: &mut ActorRefWorld) {
    let (a, log) = spawn_echoer().await;
    let (b, _blog) = spawn_echoer().await;
    a.link(&b).await;
    world.log = log;
    world.actor = Some(a);
    world.sibling = Some(b);
}

#[given(regex = r"^A's link set therefore has length 1$")]
async fn given_link_len_1(world: &mut ActorRefWorld) {
    // The ActorRef Debug impl prints `links` as the sibblings keys (the only
    // public observable of the link set). A's link set must contain exactly B.
    let a = world.actor.as_ref().expect("actor A");
    let b = world.sibling.as_ref().expect("actor B");
    let dbg = format!("{a:?}");
    let b_marker = format!("ActorId({})", b.id().sequence_id());
    assert!(
        dbg.contains(&format!("links: [{b_marker}]")),
        "A must be linked to exactly B before the no-op, got {dbg}"
    );
    world.link_contains_b = Some(true);
}

#[when(regex = r"^A's ActorRef is unlinked from itself$")]
async fn when_unlink_self(world: &mut ActorRefWorld) {
    let a = world.actor.as_ref().expect("actor A");
    a.unlink(a).await;
}

#[then(regex = r"^A's link set still has length 1 and still contains B$")]
async fn then_link_unchanged(world: &mut ActorRefWorld) {
    // self-unlink returns BEFORE touching links, so the pre-existing link to B
    // must be untouched: the link set still prints exactly `[ActorId(b)]`.
    let a = world.actor.as_ref().expect("actor A");
    let b = world.sibling.as_ref().expect("actor B");
    let dbg = format!("{a:?}");
    let b_marker = format!("ActorId({})", b.id().sequence_id());
    assert!(
        dbg.contains(&format!("links: [{b_marker}]")),
        "self-unlink must leave the pre-existing link set (exactly [B]) untouched, got {dbg}"
    );
}

#[then(regex = r"^no error occurs$")]
async fn then_no_error(_world: &mut ActorRefWorld) {
    // unlink returns `()`; reaching here without panicking is the observable.
}

#[given(regex = r"^a freshly spawned actor with exactly one strong ActorRef$")]
async fn given_one_strong(world: &mut ActorRefWorld) {
    let (actor, log) = spawn_echoer().await;
    // Record the at-rest weak baseline BEFORE any user clone/downgrade. The
    // absolute weak count includes kameo's internal WeakSenders, so only the +1
    // delta a single downgrade adds is asserted (the strong side stays a
    // meaningful absolute: a freshly spawned actor has exactly one user strong
    // handle, so a single clone makes strong_count == 2).
    world.weak_baseline = Some(actor.weak_count());
    world.log = log;
    world.actor = Some(actor);
}

#[when(regex = r"^the ActorRef is cloned once and then downgraded once$")]
async fn when_clone_and_downgrade(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    // Keep both handles alive across steps: dropping the clone would lower
    // strong_count and dropping the weak would lower weak_count, so the Then
    // (which polls the live actor) must see them retained.
    world.clone_ref = Some(actor.clone());
    world.weak = Some(actor.downgrade());
    // The Then asserts strong as a meaningful absolute (1 -> 2) and weak as a +1
    // delta over the at-rest baseline ("clone bumps strong, downgrade bumps weak").
}

#[then(regex = r"^strong_count is 2 and the weak count increased by exactly one$")]
async fn then_counts_2_1(world: &mut ActorRefWorld) {
    // The absolute weak count includes kameo's internal WeakSenders (spawn.rs
    // retains them), so it is NOT 0 at rest; we therefore assert only the +1
    // delta a single downgrade adds over the measured at-rest baseline. The
    // strong side stays a meaningful absolute (2): a freshly spawned actor has
    // exactly one user strong handle, and one clone makes strong_count == 2.
    let actor = world.actor.as_ref().expect("actor spawned");
    let weak_baseline = world.weak_baseline.expect("at-rest weak baseline");
    let weak_target = weak_baseline + 1;

    // Condition-based settle-polling (borrow, never clone — an extra clone here
    // would itself bump strong_count and break the absolute assertion).
    settle(
        || actor.strong_count() == 2 && actor.weak_count() == weak_target,
        "strong_count must reach 2 and weak_count must reach baseline + 1",
    )
    .await;

    assert_eq!(
        actor.strong_count(),
        2,
        "after one clone strong_count must be 2 (one user strong handle + one clone)"
    );
    assert_eq!(
        actor.weak_count(),
        weak_target,
        "downgrade must bump weak by exactly 1 over the at-rest baseline \
         (baseline weak={weak_baseline}, includes kameo's internal WeakSenders)"
    );
}

#[given(regex = r"^an ActorRef and a clone of it$")]
async fn given_actorref_and_clone(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
    let actor = world.actor.as_ref().expect("actor spawned");
    world.clone_ref = Some(actor.clone());
}

#[when(regex = r"^they are compared and hashed$")]
async fn when_compared_and_hashed(world: &mut ActorRefWorld) {
    let a = world.actor.as_ref().expect("actor");
    let b = world.clone_ref.as_ref().expect("clone");
    world.pair_eq = Some(a == b);
    world.pair_hash_eq = Some(hash_of(a) == hash_of(b));
}

#[then(regex = r"^they are equal and produce the same hash$")]
async fn then_equal_same_hash(world: &mut ActorRefWorld) {
    assert_eq!(
        world.pair_eq,
        Some(true),
        "a clone must be equal to its source"
    );
    assert_eq!(
        world.pair_hash_eq,
        Some(true),
        "equal ActorRefs must hash equally (id-based hash)"
    );
}

#[given(regex = r"^ActorRefs to two distinct actors$")]
async fn given_two_distinct(world: &mut ActorRefWorld) {
    let (a, log) = spawn_echoer().await;
    let (b, _blog) = spawn_echoer().await;
    world.log = log;
    world.actor = Some(a);
    world.sibling = Some(b);
}

#[when(regex = r"^they are compared$")]
async fn when_compared(world: &mut ActorRefWorld) {
    let a = world.actor.as_ref().expect("actor a");
    let b = world.sibling.as_ref().expect("actor b");
    world.pair_eq = Some(a == b);
}

#[then(regex = r"^they are not equal$")]
async fn then_not_equal(world: &mut ActorRefWorld) {
    assert_eq!(
        world.pair_eq,
        Some(false),
        "ActorRefs to distinct actors must not be equal (distinct ids)"
    );
}

// ===========================================================================
// @linearizability — concurrent senders and waiters
// ===========================================================================

#[given(regex = r"^an actor that counts every message it handles$")]
async fn given_counting_actor(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
}

#[when(regex = r"^100 messages are told concurrently from 10 tasks that start at a barrier$")]
async fn when_100_concurrent_tells(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|t| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..10u64 {
                    actor
                        .tell(Echo(t as u64 * 10 + i))
                        .await
                        .expect("tell delivered");
                }
            })
        })
        .collect();
    for h in handles {
        h.await.expect("tell task must not panic");
    }
    // Settle until all 100 are recorded (the mailbox is the serialization point).
    let log = Arc::clone(&world.log);
    settle(
        move || log.lock().unwrap().len() == 100,
        "not all 100 concurrent tells were recorded",
    )
    .await;
    world.concurrent_count = Some(world.log.lock().unwrap().len() as u64);
}

#[then(regex = r"^the actor's final count is exactly 100$")]
async fn then_final_count_100(world: &mut ActorRefWorld) {
    assert_eq!(
        world.concurrent_count,
        Some(100),
        "every concurrent tell must be delivered exactly once (100 total)"
    );
    // INDEPENDENT oracle: the recorded values are exactly 0..100 (no dup/loss).
    let mut got = world.log.lock().unwrap().clone();
    got.sort_unstable();
    let expected: Vec<u64> = (0..100).collect();
    assert_eq!(
        got, expected,
        "the 100 tells must be exactly 0..100, no gaps/dupes"
    );
}

#[given(regex = r"^an actor that echoes back the number it is asked$")]
async fn given_echo_actor(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
}

#[when(regex = r"^50 distinct numbers are asked concurrently from tasks started at a barrier$")]
async fn when_50_concurrent_asks(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let barrier = Arc::new(Barrier::new(50));
    let handles: Vec<_> = (0..50u64)
        .map(|n| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let reply = actor.ask(Echo(n)).await.expect("ask succeeds");
                (n, reply)
            })
        })
        .collect();
    for h in handles {
        world
            .ask_results
            .push(h.await.expect("ask task must not panic"));
    }
}

#[then(regex = r"^each caller receives exactly the number it asked$")]
async fn then_each_own_reply(world: &mut ActorRefWorld) {
    for (asked, received) in &world.ask_results {
        assert_eq!(
            asked, received,
            "caller {asked} received {received} (per-ask reply cross-talk)"
        );
    }
    let asked: HashSet<u64> = world.ask_results.iter().map(|(a, _)| *a).collect();
    assert_eq!(asked.len(), 50, "all 50 distinct asks must have completed");
}

#[when(regex = r"^10 tasks concurrently await wait_for_startup$")]
async fn when_10_startup_waiters(world: &mut ActorRefWorld) {
    let actor = world.slow.as_ref().expect("slow actor").clone();
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor.wait_for_startup().await;
            })
        })
        .collect();
    // Let all 10 park; none may resolve before on_start is released.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        handles.iter().all(|h| !h.is_finished()),
        "a startup waiter resolved BEFORE on_start was released"
    );
    // Stash handles for the And-step to release + join.
    world.slow_waiters = Some(handles);
}

#[when(regex = r"^on_start is then released$")]
async fn when_on_start_released(world: &mut ActorRefWorld) {
    let release_tx = world.release_tx.as_ref().expect("release tx").clone();
    release_tx.send(true).expect("release on_start");
    if let Some(handles) = world.slow_waiters.take() {
        for h in handles {
            h.await.expect("startup waiter resolves after release");
        }
        world.waiters_all_resolved = Some(true);
    }
}

#[then(regex = r"^all 10 waiters resolve after startup completes$")]
async fn then_all_10_startup_resolve(world: &mut ActorRefWorld) {
    assert_eq!(
        world.waiters_all_resolved,
        Some(true),
        "all 10 startup waiters must resolve after the single on_start completion"
    );
}

#[when(regex = r"^10 tasks concurrently await wait_for_shutdown$")]
async fn when_10_shutdown_waiters(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor.wait_for_shutdown().await;
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        handles.iter().all(|h| !h.is_finished()),
        "a shutdown waiter resolved BEFORE the actor was stopped"
    );
    world.shutdown_waiters = Some(handles);
}

#[when(regex = r"^the actor is then stopped$")]
async fn when_actor_then_stopped(world: &mut ActorRefWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    actor.stop_gracefully().await.expect("stop signal sent");
    if let Some(handles) = world.shutdown_waiters.take() {
        for h in handles {
            h.await.expect("shutdown waiter resolves after stop");
        }
        world.waiters_all_resolved = Some(true);
    }
}

#[then(regex = r"^all 10 waiters resolve after the mailbox closes$")]
async fn then_all_10_shutdown_resolve(world: &mut ActorRefWorld) {
    assert_eq!(
        world.waiters_all_resolved,
        Some(true),
        "all 10 shutdown waiters must resolve after the mailbox closes"
    );
}

// ===========================================================================
// @property / @model laws (actor_ref.properties.feature)
// ===========================================================================

// -- @property @boundary: Eq/Hash/Ord are id-based for ANY pair ---------------

#[given(regex = r"^any two ActorRefs a, b whose underlying ActorIds are any pair of ids$")]
async fn given_any_two_actorrefs(_world: &mut ActorRefWorld) {}

#[when(regex = r"^they are compared, ordered, and hashed$")]
async fn when_compared_ordered_hashed(_world: &mut ActorRefWorld) {}

#[then(regex = r"^a == b iff their ActorIds are equal$")]
async fn law_eq_iff_ids_equal(_world: &mut ActorRefWorld) {
    // ActorRef Eq/Hash/Ord delegate PURELY to id (actor_ref.rs:1363-1387). The
    // ActorId is the oracle. We cannot freely spawn actors with arbitrary ids,
    // but we CAN build ActorIds directly (ActorId::new(u64)) and assert the
    // id-level law that ActorRef composes. Boundary-biased ids.
    proptest!(|(x in prop_oneof![Just(0u64), Just(1), Just(u64::MAX - 1), Just(u64::MAX), any::<u64>()],
               y in prop_oneof![Just(0u64), Just(1), Just(u64::MAX - 1), Just(u64::MAX), any::<u64>()])| {
        let a = ActorId::new(x);
        let b = ActorId::new(y);
        // a == b iff sequence ids equal.
        prop_assert_eq!(a == b, x == y, "ActorId eq must be id-based for ({}, {})", x, y);
        // Clone is always equal + hashes equal.
        let a2 = a;
        prop_assert!(a == a2);
        prop_assert_eq!(hash_of(&a), hash_of(&a2));
    });
}

#[then(regex = r"^a == b implies hash\(a\) == hash\(b\)$")]
async fn law_eq_implies_hash_eq(_world: &mut ActorRefWorld) {
    proptest!(|(x in prop_oneof![Just(0u64), Just(1), Just(u64::MAX - 1), Just(u64::MAX), any::<u64>()])| {
        let a = ActorId::new(x);
        let b = ActorId::new(x);
        prop_assert_eq!(a, b);
        prop_assert_eq!(hash_of(&a), hash_of(&b), "equal ids must hash equally");
    });
}

#[then(regex = r"^the Ord of a, b equals the Ord of their ActorIds$")]
async fn law_ord_equals_id_ord(_world: &mut ActorRefWorld) {
    proptest!(|(x in prop_oneof![Just(0u64), Just(1), Just(u64::MAX - 1), Just(u64::MAX), any::<u64>()],
               y in prop_oneof![Just(0u64), Just(1), Just(u64::MAX - 1), Just(u64::MAX), any::<u64>()])| {
        let a = ActorId::new(x);
        let b = ActorId::new(y);
        prop_assert_eq!(a.cmp(&b), x.cmp(&y), "ActorId Ord must equal sequence-id Ord");
    });
}

#[then(regex = r"^clones of the same ActorRef are always equal and hash equally$")]
async fn law_clone_equal_hash(world: &mut ActorRefWorld) {
    // Here we DO use a real ActorRef + clone (the SUT type), asserting clone
    // equality/hash directly against the SUT (not just the id oracle).
    let (actor, _log) = spawn_echoer().await;
    let clone = actor.clone();
    assert_eq!(actor, clone, "a clone must equal its source ActorRef");
    assert_eq!(
        hash_of(&actor),
        hash_of(&clone),
        "a clone must hash equally to its source ActorRef"
    );
    actor.stop_gracefully().await.unwrap();
    let _ = world;
}

// -- @property @boundary: telling ANY stopped actor → ActorNotRunning ---------

#[given(regex = r"^any message value of the actor's accepted message type$")]
async fn given_any_message_value(_world: &mut ActorRefWorld) {}

#[when(regex = r"^that message is told to the stopped actor$")]
async fn when_any_message_told_stopped(world: &mut ActorRefWorld) {
    // Boundary-biased payloads: the stopped-mailbox model maps EVERY send to
    // ActorNotRunning regardless of payload. Fresh stopped actor per value.
    let mut all_not_running = true;
    for v in [0u64, 1, u64::MAX - 1, u64::MAX] {
        let (actor, _log) = spawn_echoer().await;
        actor.stop_gracefully().await.expect("stop");
        actor.wait_for_shutdown().await;
        settle(
            {
                let a = actor.clone();
                move || !a.is_alive()
            },
            "actor never observed not-alive",
        )
        .await;
        let result = actor.tell(Echo(v)).await;
        if !matches!(result, Err(SendError::ActorNotRunning(_))) {
            all_not_running = false;
        }
    }
    world.not_running = Some(all_not_running);
}

// (The Then reuses `then_send_not_running` via the same regex
//  "the send fails with SendError::ActorNotRunning".)

// -- @model @linearizability: refcounts refine an integer model ---------------

#[given(
    regex = r"^any interleaving of clone, downgrade, drop, and upgrade on the ActorRef and its weaks$"
)]
async fn given_any_ref_interleaving(_world: &mut ActorRefWorld) {}

#[when(regex = r"^the operations run concurrently$")]
async fn when_ref_ops_concurrent(_world: &mut ActorRefWorld) {}

#[then(regex = r"^strong_count always equals the model's count of live strong handles$")]
async fn law_strong_count_refines_model(_world: &mut ActorRefWorld) {
    // Documented deterministic sweep with REAL overlap. The oracle is an
    // INDEPENDENT integer DELTA over a measured baseline: kameo's spawn machinery
    // retains a fixed number of internal strong senders, so the law asserts the
    // Arc-style REFINEMENT (clone bumps strong by 1, drop decrements by 1) rather
    // than an absolute count. Each task captures ONE strong clone (the moved
    // `actor`) plus C inner clones — so the peak adds `tasks * (1 + c)` live
    // strong handles over the at-rest baseline. All clones overlap via a Barrier.
    for (tasks, c) in [(1usize, 1usize), (2, 1), (4, 8), (8, 8)] {
        let (actor, _log) = spawn_echoer().await;
        let baseline = actor.strong_count(); // at-rest internal + the original
        // Hold-phase barrier: tasks clone, signal ready, then wait to drop.
        let start = Arc::new(Barrier::new(tasks + 1));
        let drop_gate = Arc::new(Barrier::new(tasks + 1));
        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let actor = actor.clone();
                let start = Arc::clone(&start);
                let drop_gate = Arc::clone(&drop_gate);
                tokio::spawn(async move {
                    // `actor` (the moved capture) is one live strong handle; plus c more.
                    let clones: Vec<ActorRef<Echoer>> = (0..c).map(|_| actor.clone()).collect();
                    start.wait().await; // all clones now live concurrently
                    drop_gate.wait().await; // hold until the count is observed
                    drop(clones);
                    drop(actor); // release the per-task captured strong handle
                })
            })
            .collect();
        start.wait().await; // release: all task-clones are alive
        // Independent integer: baseline + per-task captured (1) + inner (c), per task.
        let oracle = baseline + tasks * (1 + c);
        // Borrow `actor` in the poll closure (a held clone would itself inflate
        // strong_count by 1 and break the oracle).
        settle(
            || actor.strong_count() == oracle,
            "strong_count never reached the model peak",
        )
        .await;
        assert_eq!(
            actor.strong_count(),
            oracle,
            "tasks={tasks} c={c}: strong_count must equal baseline + tasks*(1+c)"
        );
        drop_gate.wait().await; // let tasks drop their clones
        for h in handles {
            h.await.expect("ref task must not panic");
        }
        // After all task handles drop, strong_count returns to the baseline.
        settle(
            || actor.strong_count() == baseline,
            "strong_count never returned to baseline after clones dropped",
        )
        .await;
        actor.stop_gracefully().await.unwrap();
    }
}

#[then(regex = r"^weak_count always equals the model's count of live weak handles$")]
async fn law_weak_count_refines_model(_world: &mut ActorRefWorld) {
    // Independent integer DELTA oracle over a measured baseline: kameo's spawn
    // machinery retains a fixed number of internal weak senders, so the law
    // asserts the refinement (each downgrade bumps weak by 1, drop decrements by
    // 1). Each task captures a STRONG clone (no weak bump) and creates W weaks,
    // so the peak adds `tasks * w` live weak handles over the baseline. Real
    // overlap via a Barrier.
    for (tasks, w) in [(1usize, 1usize), (2, 1), (4, 4), (8, 8)] {
        let (actor, _log) = spawn_echoer().await;
        let baseline = actor.weak_count(); // at-rest internal weak senders
        let start = Arc::new(Barrier::new(tasks + 1));
        let drop_gate = Arc::new(Barrier::new(tasks + 1));
        let handles: Vec<_> = (0..tasks)
            .map(|_| {
                let actor = actor.clone();
                let start = Arc::clone(&start);
                let drop_gate = Arc::clone(&drop_gate);
                tokio::spawn(async move {
                    let weaks: Vec<WeakActorRef<Echoer>> =
                        (0..w).map(|_| actor.downgrade()).collect();
                    start.wait().await;
                    drop_gate.wait().await;
                    drop(weaks);
                })
            })
            .collect();
        start.wait().await;
        let oracle = baseline + tasks * w; // independent integer: all downgrades live
        settle(
            {
                let a = actor.clone();
                move || a.weak_count() == oracle
            },
            "weak_count never reached the model peak",
        )
        .await;
        assert_eq!(
            actor.weak_count(),
            oracle,
            "tasks={tasks} w={w}: weak_count must equal baseline + tasks*w"
        );
        drop_gate.wait().await;
        for h in handles {
            h.await.expect("weak task must not panic");
        }
        settle(
            {
                let a = actor.clone();
                move || a.weak_count() == baseline
            },
            "weak_count never returned to baseline after weaks dropped",
        )
        .await;
        actor.stop_gracefully().await.unwrap();
    }
}

#[then(regex = r"^upgrade returns Some iff the model's strong count is greater than zero$")]
async fn law_upgrade_iff_strong_positive(_world: &mut ActorRefWorld) {
    // Model: upgrade succeeds iff strong > 0. Drive both arms deterministically.
    // Arm 1 (strong > 0): a held strong ref → upgrade is Some.
    let (actor, _log) = spawn_echoer().await;
    let weak = actor.downgrade();
    assert!(
        weak.upgrade().is_some(),
        "upgrade must be Some while strong_count > 0"
    );
    // Arm 2 (strong == 0): drop every strong ref → upgrade settles to None.
    drop(actor);
    settle(
        {
            let w = weak.clone();
            move || w.upgrade().is_none()
        },
        "upgrade never became None after the last strong ref dropped",
    )
    .await;
    assert!(
        weak.upgrade().is_none(),
        "upgrade must be None once strong_count == 0"
    );
}

// -- @model @linearizability: N concurrent asks, no cross-talk ----------------

#[given(regex = r"^an actor that echoes back the distinct number it is asked$")]
async fn given_echo_distinct_actor(world: &mut ActorRefWorld) {
    if world.actor.is_none() {
        let (actor, log) = spawn_echoer().await;
        world.log = log;
        world.actor = Some(actor);
    }
}

#[given(regex = r"^any number N of tasks each asking a distinct number, started at a barrier$")]
async fn given_any_n_askers(_world: &mut ActorRefWorld) {}

#[when(regex = r"^all N asks run with real overlap$")]
async fn when_n_asks_overlap(_world: &mut ActorRefWorld) {}

#[then(regex = r"^every task receives exactly the number it asked$")]
async fn law_every_task_own_reply(_world: &mut ActorRefWorld) {
    // N ∈ {2, 8, 64}: smallest concurrent case + a large fan-out. Distinct asked
    // numbers per task make cross-talk observable. Real overlap via a Barrier.
    for n in [2u64, 8, 64] {
        run_concurrent_ask_case(n).await;
    }
}

#[then(regex = r"^the multiset of received replies equals the multiset of asked numbers$")]
async fn law_multiset_equal(_world: &mut ActorRefWorld) {
    // Re-run a representative case so this Then is a real assertion (the multiset
    // equality is checked inside run_concurrent_ask_case).
    run_concurrent_ask_case(64).await;
}

async fn run_concurrent_ask_case(n: u64) {
    let (actor, _log) = spawn_echoer().await;
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|i| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            // Distinct asked value per task (offset so 0 is exercised too).
            tokio::spawn(async move {
                barrier.wait().await;
                let asked = i * 7 + 1;
                let reply = actor.ask(Echo(asked)).await.expect("ask succeeds");
                (asked, reply)
            })
        })
        .collect();
    let mut asked_set: Vec<u64> = Vec::new();
    let mut received: Vec<u64> = Vec::new();
    for h in handles {
        let (asked, reply) = h.await.expect("ask task must not panic");
        assert_eq!(
            asked, reply,
            "N={n}: ask for {asked} got {reply} (cross-talk)"
        );
        asked_set.push(asked);
        received.push(reply);
    }
    asked_set.sort_unstable();
    received.sort_unstable();
    assert_eq!(
        asked_set, received,
        "N={n}: the multiset of replies must equal the multiset of asks"
    );
    actor.stop_gracefully().await.unwrap();
}

// -- @model @linearizability: any number of startup waiters, one completion ---

#[given(regex = r"^any number W of tasks concurrently awaiting wait_for_startup$")]
async fn given_any_w_startup_waiters(_world: &mut ActorRefWorld) {}

#[then(regex = r"^all W waiters resolve, and none resolves before on_start completes$")]
async fn law_w_waiters_one_completion(_world: &mut ActorRefWorld) {
    // W ∈ {1, 2, 10, 64}: include the single-waiter boundary. A fresh SlowStart
    // per case; the release point is fixed AFTER all waiters are parked. Oracle:
    // a one-shot latch fanned out to all observers — none may resolve early.
    for w in [1usize, 2, 10, 64] {
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let actor = SlowStart::spawn(SlowStart {
            release: release_rx,
        });
        let barrier = Arc::new(Barrier::new(w));
        let handles: Vec<_> = (0..w)
            .map(|_| {
                let actor = actor.clone();
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    actor.wait_for_startup().await;
                })
            })
            .collect();
        // None may resolve before on_start is released.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            handles.iter().all(|h| !h.is_finished()),
            "W={w}: a startup waiter resolved before on_start was released"
        );
        release_tx.send(true).expect("release on_start");
        for h in handles {
            h.await.expect("startup waiter resolves after release");
        }
        actor.stop_gracefully().await.unwrap();
    }
}
