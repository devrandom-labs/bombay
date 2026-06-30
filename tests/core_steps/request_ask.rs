//! Shared `AskRequest` World + step definitions for the core `request_ask`
//! scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_request_ask_bdd.rs`        — the example feature (request_ask.feature)
//!   * `core_request_ask_props_bdd.rs`  — the property/model laws
//!     (request_ask.properties.feature)
//!
//! The SUT is `src/request/ask.rs` (the `AskRequest` builder: `ask(M)` →
//! optional `mailbox_timeout`/`reply_timeout` → `send`/`try_send`/
//! `blocking_send`/`enqueue`/`try_enqueue`/`blocking_enqueue`/`forward`/
//! `try_forward`/`blocking_forward`/`IntoFuture`), driven against REAL SPAWNED
//! ACTORS reached through `bombay::prelude::*` + `bombay::request::*`.
//!
//! ## @timing (the 13 timeout scenarios)
//!
//! `tokio::time::pause()`/`advance()` REQUIRE a current-thread runtime, but the
//! cucumber runner is `#[tokio::test(flavor = "multi_thread")]` (a
//! non-negotiable harness fact). So every @timing step drives its actor + ask
//! inside a DEDICATED current-thread `start_paused(true)` runtime, created on a
//! blocking thread via `tokio::task::spawn_blocking`. A paused current-thread
//! runtime AUTO-ADVANCES its clock to the next pending timer whenever it has no
//! other work, so a handler `sleep(d)` raced against a `reply_timeout(t)`
//! resolves deterministically with ~zero wall-clock time and no flake (verified:
//! the d<t / d>=t outcomes resolve in ~100µs). The actor MUST be spawned inside
//! that same paused runtime so its `sleep` observes the same paused clock.
//!
//! ## blocking_* (gotcha 3)
//!
//! `blocking_send`/`blocking_enqueue`/`blocking_forward` call tokio's blocking
//! mailbox primitives, which PANIC ("Cannot block the current thread from within
//! a runtime") if called directly on the async cucumber worker. Each is wrapped
//! in `tokio::task::spawn_blocking` with a cloned `ActorRef` moved into the
//! closure.
//!
//! ## full bounded mailbox (gotcha 4)
//!
//! Full-mailbox scenarios use a handler parked on a `watch` release gate so the
//! mailbox stays full; the test asserts the full-mailbox observable, then
//! releases the gate (or `kill()`s) — it never awaits clean completion against a
//! permanently-full mailbox.
//!
//! All bounded waits are condition-based `settle()` polling (panics loudly on
//! non-settle); no `wait_for_shutdown()` is used as a settle barrier.

use std::{any::Any, sync::Arc, time::Duration};

use bombay::{
    error::{BoxSendError, Infallible, SendError},
    mailbox,
    message::BoxReply,
    prelude::*,
    reply::{ReplySender, testing::reply_channel},
};
use cucumber::{World, given, then, when};
use tokio::{
    sync::{Barrier, oneshot, watch},
    task::JoinHandle,
};

// ===========================================================================
// Test actors and messages
// ===========================================================================

/// The actor under test. Its handlers cover every shape the scenarios need:
/// a prompt reply, a configurable sleep, a typed handler error, a panic, and an
/// echo of an integer payload (for the per-caller no-cross-talk oracle).
#[derive(Clone)]
struct Asked;

impl Actor for Asked {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A plain message; the handler replies `Ok(true)` promptly. `Reply = bool`
/// matches the in-file SUT tests (`bounded_ask_requests`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Msg;

impl Message<Msg> for Asked {
    type Reply = bool;

    async fn handle(&mut self, _msg: Msg, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        true
    }
}

/// A message whose handler sleeps `dur` before replying `true` — drives the
/// reply_timeout scenarios under the paused clock.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Sleep(Duration);

impl Message<Sleep> for Asked {
    type Reply = bool;

    async fn handle(
        &mut self,
        Sleep(dur): Sleep,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tokio::time::sleep(dur).await;
        true
    }
}

/// A typed handler error carried by `Result` replies.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HandlerBoom(u64);

impl std::fmt::Display for HandlerBoom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "handler boom {}", self.0)
    }
}

impl std::error::Error for HandlerBoom {}

/// A message whose handler returns `Err(HandlerBoom(tag))`.
struct Failing(u64);

impl Message<Failing> for Asked {
    type Reply = Result<u64, HandlerBoom>;

    async fn handle(&mut self, msg: Failing, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        Err(HandlerBoom(msg.0))
    }
}

/// A message whose handler panics — drives the "panic must fail the ask, not
/// hang" lifecycle scenario.
struct PanicMsg;

impl Message<PanicMsg> for Asked {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: PanicMsg,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        panic!("handler panic on PanicMsg");
    }
}

/// A message whose handler parks on a `watch` release gate until it flips to
/// `true`, holding the mailbox slot so a bounded mailbox stays full.
struct Hold(watch::Receiver<bool>);

impl Message<Hold> for Asked {
    type Reply = bool;

    async fn handle(&mut self, msg: Hold, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let mut rx = msg.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
        true
    }
}

/// An echo message: replies with the integer it carries (per-caller oracle).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Echo(u64);

impl Message<Echo> for Asked {
    type Reply = u64;

    async fn handle(&mut self, msg: Echo, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        msg.0
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Spawns an `Asked` actor with a bounded mailbox of `cap`, awaiting startup.
async fn spawn_bounded(cap: usize) -> ActorRef<Asked> {
    let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(cap));
    actor.wait_for_startup().await;
    actor
}

/// Reads the `Ok` value delivered to a forward reply channel, downcasting the
/// boxed reply to `bool`. Returns `None` if the channel is closed or empty.
async fn forward_ok_bool(rx: oneshot::Receiver<Result<BoxReply, BoxSendError>>) -> Option<bool> {
    match rx.await {
        Ok(Ok(boxed)) => {
            let any: Box<dyn Any> = boxed;
            any.downcast::<bool>().ok().map(|b| *b)
        }
        _ => None,
    }
}

/// A `ReplySender<bool>` paired with its receiver, for the forward scenarios.
fn bool_reply_channel() -> (
    ReplySender<bool>,
    oneshot::Receiver<Result<BoxReply, BoxSendError>>,
) {
    reply_channel::<bool>()
}

/// Fills a bounded(1) `Asked` mailbox so it has NO spare capacity, using parked
/// `Hold` handlers gated on `release`. Returns the release sender so the caller
/// can drain on cleanup. The first send is dequeued into the parked handler
/// (freeing the slot), the second occupies the single buffer slot — matching the
/// in-file `bounded_ask_requests_mailbox_full` arithmetic.
async fn fill_bounded1(
    actor: &ActorRef<Asked>,
    release: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    actor
        .tell(Hold(release.clone()))
        .send()
        .await
        .map_err(|e| format!("first hold enqueue: {e:?}"))?;
    // Let the actor dequeue the first Hold into the (parked) handler, freeing the
    // single slot, before the second send occupies it.
    tokio::time::sleep(Duration::from_millis(20)).await;
    actor
        .tell(Hold(release))
        .try_send()
        .map_err(|e| format!("second hold fills slot: {e:?}"))?;
    Ok(())
}

/// Asserts (with bounded polling) that `actor`'s bounded mailbox is observably
/// FULL — a `try_send` returns `MailboxFull` and hands the message back, so this
/// probe never enqueues anything. Panics loudly if the mailbox never fills.
async fn assert_full(actor: &ActorRef<Asked>) {
    for _ in 0..200 {
        if matches!(
            actor.ask(Msg).try_send().await,
            Err(SendError::MailboxFull(_))
        ) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the bounded mailbox never became observably full");
}

/// Holds an enqueued `PendingReply` across two When steps. `PendingReply` does
/// not implement `Debug`, so this wrapper supplies a manual one to keep the
/// derived `World: Debug`.
struct HeldPending(bombay::request::PendingReply<Msg, bool>);

impl std::fmt::Debug for HeldPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldPending").finish_non_exhaustive()
    }
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct AskWorld {
    /// The actor under test (kept alive across steps).
    actor: Option<ActorRef<Asked>>,
    /// A release gate for parked `Hold` handlers (full-mailbox scenarios).
    release: Option<watch::Sender<bool>>,

    // --- captured single-reply outcomes -----------------------------------
    /// A captured `Ok(bool)` reply (plain ask / send / try_send / blocking).
    ok_reply: Option<bool>,
    /// Whether the captured outcome was `SendError::Timeout(Some(..))`.
    timeout_some: Option<bool>,
    /// Whether the captured outcome was `SendError::Timeout(None)`.
    timeout_none: Option<bool>,
    /// Whether the captured outcome was `SendError::MailboxFull`.
    mailbox_full: Option<bool>,
    /// A captured typed `HandlerError` payload.
    handler_err: Option<HandlerBoom>,
    /// Whether an in-flight ask resolved to *some* `SendError` (no hang).
    resolved_send_error: Option<bool>,

    // --- send/try_send/blocking_send trio ---------------------------------
    /// The three replies from the send/try_send/blocking_send sequence scenario.
    trio_replies: Vec<bool>,
    /// The three `ActorNotRunning` observations on a stopped actor.
    stopped_trio: Vec<bool>,

    // --- enqueue / forward ------------------------------------------------
    /// A pending reply held between two When steps (enqueue scenario).
    /// `PendingReply` has no `Debug`, so it is wrapped to keep the World `Debug`.
    pending: Option<HeldPending>,
    /// The value delivered to a forward reply channel.
    forward_value: Option<bool>,
    /// Whether the forward call itself returned `Ok(())`.
    forward_ok: Option<bool>,

    // --- concurrency results ----------------------------------------------
    /// The (caller_n -> reply) map for the no-cross-talk linearizability case.
    concurrent_echo: Vec<(u64, u64)>,
    /// The short-timeout caller's outcome (Timeout(None)) flag.
    short_timeout_none: Option<bool>,
    /// The other callers' replies in the concurrent-timeout scenario.
    other_replies: Vec<bool>,
    /// Replies from concurrent blocking_send worker threads (n -> reply).
    blocking_echo: Vec<(u64, u64)>,

    // --- scenario-routing flags (set by Givens, read by shared Whens) ------
    /// Routes the shared `send()`/`try_send()` Whens to the stopped-actor case.
    stopped: bool,
    /// Routes the shared `try_send()` When to the full-mailbox case.
    full: bool,
    /// Routes the shared `ask(Msg) and awaits it` When to the panic case.
    panic_scenario: bool,
    /// Marks the kill-mid-flight scenario (handler sleeps long, actor killed
    /// while the reply is still pending). Set by its Given; the outstanding-ask
    /// When/Then carry the real wiring.
    kill_mid_flight: bool,
    /// Marks the @timing no-spare-capacity case (the single bounded slot is
    /// permanently busy). The @timing Whens reconstruct their own paused-clock
    /// actor, so this is only a scenario marker.
    no_spare_capacity: bool,
    /// The configured handler sleep, in ms, captured by the @timing Givens
    /// ("sleeps Nms" / "sleeps before replying" / "replies promptly"). The
    /// @timing Whens encode the concrete sleep inline, so this records the
    /// declared delay for the scenario phrasing.
    handler_sleep_ms: Option<u64>,
    /// Marks the @timing spare-capacity case (bounded(100), mailbox wait
    /// instant). The @timing Whens reconstruct their own paused-clock actor, so
    /// this is only a scenario marker.
    spare_capacity: bool,
    /// The outstanding ask handle for the kill-mid-flight scenario.
    outstanding: Option<JoinHandle<Result<bool, SendError<Sleep, Infallible>>>>,
    /// The in-flight blocking_forward thread + its reply receiver.
    blocking_forward: Option<(
        JoinHandle<Result<(), SendError<(Msg, ReplySender<bool>), Infallible>>>,
        oneshot::Receiver<Result<BoxReply, BoxSendError>>,
    )>,
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^a running actor whose handler can be made to sleep for a given duration$")]
async fn given_running_actor(_world: &mut AskWorld) {
    // The concrete actor + mailbox is created by each scenario's capacity Given
    // (different scenarios need different capacities / paused clocks), so the
    // Background is a no-op marker matching the shared feature phrasing.
}

// ===========================================================================
// @sequence — the builder protocol
// ===========================================================================

#[given(regex = r"^the actor has a bounded mailbox of capacity 100$")]
async fn given_bounded_100(world: &mut AskWorld) {
    world.actor = Some(spawn_bounded(100).await);
}

#[given(regex = r"^the actor has a bounded mailbox of capacity 100 and is idle$")]
async fn given_bounded_100_idle(world: &mut AskWorld) {
    world.actor = Some(spawn_bounded(100).await);
}

#[when(regex = r#"^the caller sends "ask\(Msg\)" and awaits it directly$"#)]
async fn when_ask_await_directly(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    let reply = actor.ask(Msg).await.expect("plain ask awaits the reply");
    world.ok_reply = Some(reply);
}

#[then(regex = r"^the caller receives the handler's Ok reply$")]
async fn then_receives_ok(world: &mut AskWorld) {
    assert_eq!(
        world.ok_reply,
        Some(true),
        "a plain ask must return the handler's Ok(true) reply"
    );
}

// SHARED When: the idle trio (line 53, expects Ok) AND the stopped-actor
// scenario (line 182, expects ActorNotRunning) both use this text. Route by the
// `stopped` flag set by the stopped-actor Given.
#[when(regex = r#"^the caller invokes "ask\(Msg\)\.send\(\)"$"#)]
async fn when_invoke_send(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    if world.stopped {
        let r = actor.ask(Msg).send().await;
        assert_eq!(
            r,
            Err(SendError::ActorNotRunning(Msg)),
            "send to a stopped actor must return ActorNotRunning(Msg)"
        );
        world.stopped_trio.push(true);
        return;
    }
    let reply = actor.ask(Msg).send().await.expect("send delivers");
    world.trio_replies.push(reply);
}

// SHARED When: the idle trio (line 54, expects Ok) AND the full-mailbox scenario
// (line 192, expects MailboxFull) use this text. Route by the `full` flag.
#[when(regex = r#"^the caller invokes "ask\(Msg\)\.try_send\(\)"$"#)]
async fn when_invoke_try_send(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    if world.full {
        let r = actor.ask(Msg).try_send().await;
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
    let reply = actor.ask(Msg).try_send().await.expect("try_send delivers");
    world.trio_replies.push(reply);
}

#[when(regex = r#"^the caller invokes "ask\(Msg\)\.blocking_send\(\)" on a blocking thread$"#)]
async fn when_invoke_blocking_send(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let reply = tokio::task::spawn_blocking(move || actor.ask(Msg).blocking_send())
        .await
        .expect("blocking thread join")
        .expect("blocking_send delivers");
    world.trio_replies.push(reply);
}

#[then(regex = r"^each call returns the handler's Ok reply$")]
async fn then_each_returns_ok(world: &mut AskWorld) {
    assert_eq!(
        world.trio_replies,
        vec![true, true, true],
        "send, try_send and blocking_send must each return Ok(true)"
    );
}

#[when(
    regex = r#"^the caller invokes "ask\(Msg\)\.enqueue\(\)" and holds the returned pending reply$"#
)]
async fn when_enqueue_hold(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned");
    let pending = actor.ask(Msg).enqueue().await.expect("enqueue succeeds");
    world.pending = Some(HeldPending(pending));
}

#[when(regex = r"^the caller later awaits the pending reply$")]
async fn when_await_pending(world: &mut AskWorld) {
    let pending = world.pending.take().expect("a held pending reply");
    let reply = pending.0.await.expect("pending reply resolves");
    world.ok_reply = Some(reply);
}

#[then(regex = r"^the awaited pending reply yields the handler's Ok reply$")]
async fn then_pending_yields_ok(world: &mut AskWorld) {
    assert_eq!(
        world.ok_reply,
        Some(true),
        "an enqueued ask's pending reply must resolve to Ok(true)"
    );
}

#[given(regex = r"^a reply channel is created$")]
async fn given_reply_channel(_world: &mut AskWorld) {
    // The channel is created inside the forward When (it must be moved into the
    // forward call); nothing to store here.
}

// The green `forward(sender)` scenario (live actor). The forward-to-STOPPED twin
// is @bug:error.rs:293 (filtered out + pinned by a red-on-fix probe in the
// runner), so this step is only ever reached for the live-actor case.
#[when(regex = r#"^the caller invokes "ask\(Msg\)\.forward\(sender\)"$"#)]
async fn when_forward(world: &mut AskWorld) {
    let (sender, rx) = bool_reply_channel();
    let actor = world.actor.as_ref().expect("actor spawned");
    // forward only exists on the no-reply-timeout builder; a plain ask().forward.
    actor
        .ask(Msg)
        .forward(sender)
        .await
        .expect("forward to a live actor returns Ok(())");
    world.forward_ok = Some(true);
    world.forward_value = forward_ok_bool(rx).await;
}

#[then(regex = r"^the reply is delivered to the channel, not returned to the caller$")]
async fn then_forward_delivers_to_channel(world: &mut AskWorld) {
    assert_eq!(
        world.forward_value,
        Some(true),
        "forward must deliver the handler's reply to the supplied channel"
    );
}

#[then(regex = r"^the forward call itself returns Ok\(\(\)\)$")]
async fn then_forward_ok(world: &mut AskWorld) {
    assert_eq!(
        world.forward_ok,
        Some(true),
        "a successful forward returns Ok(())"
    );
}

#[given(regex = r"^the actor has a bounded mailbox of capacity 1 that is momentarily full$")]
async fn given_bounded1_momentarily_full(world: &mut AskWorld) {
    let actor = spawn_bounded(1).await;
    let (tx, rx) = watch::channel(false);
    fill_bounded1(&actor, rx).await.expect("fill bounded(1)");
    assert_full(&actor).await;
    world.release = Some(tx);
    world.actor = Some(actor);
}

#[when(
    regex = r#"^the caller invokes "ask\(Msg\)\.blocking_forward\(sender\)" on a blocking thread$"#
)]
async fn when_blocking_forward(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let (sender, rx) = bool_reply_channel();
    // blocking_forward parks on tokio's blocking_send until a slot frees; run it
    // on a real blocking thread (never on the async worker, which would panic).
    let handle = tokio::task::spawn_blocking(move || actor.ask(Msg).blocking_forward(sender));
    // Give the blocking thread a moment to start parking on the full mailbox.
    tokio::time::sleep(Duration::from_millis(30)).await;
    world.blocking_forward = Some((handle, rx));
}

#[when(regex = r"^the actor frees one mailbox slot$")]
async fn when_actor_frees_slot(world: &mut AskWorld) {
    // Release the parked Hold handlers so the actor drains and a slot frees,
    // unblocking the parked blocking_forward send.
    if let Some(tx) = world.release.take() {
        let _ = tx.send(true);
    }
}

#[then(regex = r"^the call returns Ok\(\(\)\) once capacity is available$")]
async fn then_blocking_forward_ok(world: &mut AskWorld) {
    let (handle, rx) = world.blocking_forward.take().expect("a blocking_forward");
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("blocking_forward must complete once a slot frees, not hang")
        .expect("blocking_forward thread join");
    outcome.expect("blocking_forward returns Ok(()) once capacity is available");
    world.forward_ok = Some(true);
    // Stash the receiver for the reply-delivery Then.
    world.forward_value = forward_ok_bool(rx).await;
}

#[then(regex = r"^the reply is delivered to the channel$")]
async fn then_blocking_forward_reply(world: &mut AskWorld) {
    assert_eq!(
        world.forward_ok,
        Some(true),
        "blocking_forward must have returned Ok(())"
    );
    assert_eq!(
        world.forward_value,
        Some(true),
        "blocking_forward must deliver the handler's reply to the channel"
    );
    if let Some(actor) = world.actor.as_ref() {
        actor.kill();
    }
}

#[given(regex = r"^the actor's handler returns a typed Err for this message$")]
async fn given_handler_returns_err(world: &mut AskWorld) {
    world.actor = Some(spawn_bounded(100).await);
}

#[when(regex = r#"^the caller sends "ask\(Msg\)" and awaits it$"#)]
async fn when_ask_await_failing(world: &mut AskWorld) {
    // This phrasing is shared by the HandlerError sequence scenario AND the
    // handler-panic lifecycle scenario; route by which Given prepared the world.
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    if world.panic_scenario {
        let result = actor.ask(PanicMsg).await;
        world.resolved_send_error = Some(result.is_err());
        return;
    }
    let result = actor.ask(Failing(81)).await;
    match result.expect_err("a typed handler Err must surface") {
        SendError::HandlerError(boom) => world.handler_err = Some(boom),
        other => panic!("expected HandlerError, got {other:?}"),
    }
}

#[then(regex = r"^the caller receives SendError::HandlerError carrying the handler's typed error$")]
async fn then_handler_error(world: &mut AskWorld) {
    assert_eq!(
        world.handler_err.as_ref(),
        Some(&HandlerBoom(81)),
        "a handler Err must surface as HandlerError carrying exactly that error"
    );
}

// ===========================================================================
// @boundary — closed/full mailbox (non-timing)
//
// NOTE: the two forward-FAILURE scenarios — `try_forward` to a full mailbox
// (@bug:error.rs:305) and `forward` to a stopped actor (@bug:error.rs:293) —
// document desired-but-ABSENT behaviour: both PANIC the caller via
// `downcast_message::<(M, ReplySender)>().unwrap()` returning `None` on the
// `From<{Try}SendError<Signal>>` conversion (the signal stores the bare message,
// not the (M, ReplySender) tuple). They are @bug-tagged in the feature (dropped
// by the standard tag filter) and pinned by the `bug_*` red-on-fix probes in
// `core_request_ask_bdd.rs`, so their green-path steps are intentionally absent.
// ===========================================================================

#[given(regex = r"^the actor has been stopped gracefully and shutdown has completed$")]
async fn given_stopped_completed(world: &mut AskWorld) {
    let actor = match world.actor.take() {
        Some(a) => a,
        None => spawn_bounded(100).await,
    };
    actor.stop_gracefully().await.expect("graceful stop");
    actor.wait_for_shutdown().await;
    world.stopped = true;
    world.actor = Some(actor);
}

#[then(regex = r"^the caller receives SendError::ActorNotRunning\(Msg\)$")]
async fn then_send_not_running(world: &mut AskWorld) {
    // The When (send()) already drove + asserted ActorNotRunning under the
    // `stopped` flag; this Then confirms that observation was recorded.
    assert_eq!(
        world.stopped_trio,
        vec![true],
        "send to a stopped actor must have returned ActorNotRunning(Msg)"
    );
}

#[then(regex = r#"^"ask\(Msg\)\.try_send\(\)" also returns SendError::ActorNotRunning\(Msg\)$"#)]
async fn then_try_send_not_running(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let r = actor.ask(Msg).try_send().await;
    assert_eq!(
        r,
        Err(SendError::ActorNotRunning(Msg)),
        "try_send to a stopped actor must return ActorNotRunning(Msg)"
    );
    world.stopped_trio.push(true);
}

#[then(
    regex = r#"^"ask\(Msg\)\.blocking_send\(\)" also returns SendError::ActorNotRunning\(Msg\)$"#
)]
async fn then_blocking_send_not_running(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let r = tokio::task::spawn_blocking(move || actor.ask(Msg).blocking_send())
        .await
        .expect("blocking thread join");
    assert_eq!(
        r,
        Err(SendError::ActorNotRunning(Msg)),
        "blocking_send to a stopped actor must return ActorNotRunning(Msg)"
    );
    world.stopped_trio.push(true);
}

#[given(regex = r"^the actor has a bounded mailbox of capacity 1$")]
async fn given_bounded_1(world: &mut AskWorld) {
    world.actor = Some(spawn_bounded(1).await);
}

#[given(regex = r"^the mailbox is filled to capacity while the actor is busy in its handler$")]
async fn given_filled_while_busy(world: &mut AskWorld) {
    let actor = world
        .actor
        .as_ref()
        .expect("actor spawned (bounded 1)")
        .clone();
    let (tx, rx) = watch::channel(false);
    fill_bounded1(&actor, rx).await.expect("fill bounded(1)");
    assert_full(&actor).await;
    world.release = Some(tx);
    world.full = true;
}

#[then(regex = r"^the caller receives SendError::MailboxFull\(Msg\)$")]
async fn then_mailbox_full(world: &mut AskWorld) {
    // The When (try_send()) drove + asserted MailboxFull under the `full` flag.
    assert_eq!(
        world.mailbox_full,
        Some(true),
        "try_send into a full bounded mailbox must have returned MailboxFull(Msg)"
    );
}

// ===========================================================================
// @lifecycle — actor death mid-flight; bounded(1) full-once-both-slots
// ===========================================================================

#[given(regex = r"^the handler panics when handling this message$")]
async fn given_handler_panics(world: &mut AskWorld) {
    world.panic_scenario = true;
}

#[then(regex = r"^the caller receives a SendError rather than hanging forever$")]
async fn then_panic_fails_not_hangs(world: &mut AskWorld) {
    assert_eq!(
        world.resolved_send_error,
        Some(true),
        "a handler panic must fail the in-flight ask with a SendError, never hang"
    );
}

#[given(regex = r"^the handler sleeps long enough that the reply is still pending$")]
async fn given_handler_sleeps_pending(world: &mut AskWorld) {
    // The bounded-100 actor is already spawned; the kill scenario drives a long
    // sleep then kills mid-flight (real wall clock; the kill races the sleep).
    world.kill_mid_flight = true;
}

#[when(regex = r#"^the caller has an outstanding "ask\(Sleep\)" awaiting the reply$"#)]
async fn when_outstanding_sleep(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    // Spawn the ask so it is genuinely outstanding while we kill the actor. A
    // 10s sleep guarantees the reply is still pending when the kill lands.
    let handle = tokio::spawn(async move { actor.ask(Sleep(Duration::from_secs(10))).await });
    world.outstanding = Some(handle);
    // Give the actor a moment to dequeue and enter the sleeping handler.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[when(regex = r"^the actor is killed$")]
async fn when_actor_killed(world: &mut AskWorld) {
    world.actor.as_ref().expect("actor spawned").kill();
}

#[then(regex = r"^the outstanding ask resolves to a SendError rather than hanging$")]
async fn then_outstanding_resolves(world: &mut AskWorld) {
    let handle = world.outstanding.take().expect("an outstanding ask");
    // Bound the await: if killing failed to drop the reply sender this would hang.
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the outstanding ask must resolve, not hang")
        .expect("ask task join");
    assert!(
        outcome.is_err(),
        "killing the actor mid-ask must resolve the caller with a SendError, got {outcome:?}"
    );
}

#[given(
    regex = r"^its handler blocks long enough to stay in-flight \(the actor does not progress\)$"
)]
async fn given_handler_blocks_inflight(world: &mut AskWorld) {
    // Bounded(1) actor with a release-gated Hold handler.
    let (tx, _rx) = watch::channel(false);
    world.release = Some(tx);
}

#[when(
    regex = r"^a first message is sent and the actor dequeues it, freeing the single slot as it enters the blocked handler$"
)]
async fn when_first_dequeued(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned (bounded 1)");
    let rx = world.release.as_ref().expect("release gate").subscribe();
    actor
        .tell(Hold(rx))
        .send()
        .await
        .expect("first hold enqueued");
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[when(regex = r"^a second message is sent and now occupies the one bounded\(1\) slot$")]
async fn when_second_occupies(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned (bounded 1)");
    let rx = world.release.as_ref().expect("release gate").subscribe();
    actor
        .tell(Hold(rx))
        .try_send()
        .expect("second hold fills the single slot");
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[when(regex = r#"^the caller then calls "ask\(Msg\)\.try_send\(\)" for a third message$"#)]
async fn when_third_try_send(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let r = actor.ask(Msg).try_send().await;
    world.mailbox_full = Some(matches!(r, Err(SendError::MailboxFull(Msg))));
    if let Some(tx) = world.release.take() {
        let _ = tx.send(true);
    }
    actor.kill();
}

#[then(regex = r"^the third send fails with exactly Err\(SendError::MailboxFull\(Msg\)\)$")]
async fn then_third_mailbox_full(world: &mut AskWorld) {
    assert_eq!(
        world.mailbox_full,
        Some(true),
        "the third try_send (both slots taken) must fail with MailboxFull(Msg)"
    );
}

// ===========================================================================
// @linearizability (non-timing) — concurrent asks, per-caller isolation
// ===========================================================================

#[given(regex = r"^the actor has a bounded mailbox of capacity 4$")]
async fn given_bounded_4(world: &mut AskWorld) {
    world.actor = Some(spawn_bounded(4).await);
}

#[given(regex = r"^the handler echoes back the integer it received$")]
async fn given_echo_handler(_world: &mut AskWorld) {
    // The `Echo` handler is used; the actor is the bounded-4 actor above.
}

#[when(regex = r#"^50 callers concurrently send "ask\(n\)" each with a distinct integer n$"#)]
async fn when_50_distinct_asks(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let barrier = Arc::new(Barrier::new(50));
    let handles: Vec<_> = (0..50u64)
        .map(|n| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let reply = actor.ask(Echo(n)).await.expect("echo ask succeeds");
                (n, reply)
            })
        })
        .collect();
    for h in handles {
        world.concurrent_echo.push(h.await.expect("echo task join"));
    }
}

#[then(regex = r"^each caller receives the Ok reply equal to the n it sent$")]
async fn then_each_echo_matches(world: &mut AskWorld) {
    for (n, reply) in &world.concurrent_echo {
        assert_eq!(
            n, reply,
            "caller {n} received {reply} — a reply crossed callers"
        );
    }
}

#[then(regex = r"^no reply is delivered to the wrong caller and none is lost$")]
async fn then_no_crosstalk_none_lost(world: &mut AskWorld) {
    let mut got: Vec<u64> = world.concurrent_echo.iter().map(|(n, _)| *n).collect();
    got.sort_unstable();
    let expected: Vec<u64> = (0..50).collect();
    assert_eq!(
        got, expected,
        "exactly the 50 distinct callers must be answered, none lost or duplicated"
    );
}

#[when(
    regex = r#"^8 OS threads each invoke "ask\(n\)\.blocking_send\(\)" with a distinct n under a barrier$"#
)]
async fn when_8_blocking_threads(world: &mut AskWorld) {
    let actor = world.actor.as_ref().expect("actor spawned").clone();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let handles: Vec<_> = (0..8u64)
        .map(|n| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            // Real OS threads (std::thread) blocking on blocking_send. Each
            // thread is its own blocking context, so blocking_send is safe.
            tokio::task::spawn_blocking(move || {
                barrier.wait();
                let reply = actor.ask(Echo(n)).blocking_send().expect("blocking echo");
                (n, reply)
            })
        })
        .collect();
    for h in handles {
        world
            .blocking_echo
            .push(h.await.expect("blocking echo join"));
    }
}

#[then(regex = r"^each thread receives the Ok reply equal to its own n$")]
async fn then_each_blocking_matches(world: &mut AskWorld) {
    for (n, reply) in &world.blocking_echo {
        assert_eq!(
            n, reply,
            "blocking thread {n} received {reply} (cross-talk)"
        );
    }
    let mut got: Vec<u64> = world.blocking_echo.iter().map(|(n, _)| *n).collect();
    got.sort_unstable();
    assert_eq!(
        got,
        (0..8).collect::<Vec<_>>(),
        "exactly 8 distinct replies"
    );
}

// ===========================================================================
// @timing — driven inside a dedicated paused current-thread runtime
// ===========================================================================

/// Runs `body` inside a fresh current-thread `start_paused(true)` runtime on a
/// blocking thread, so tokio's paused clock auto-advances over the actor's
/// sleeps and the ask's timeouts with no real wall-clock wait. The actor MUST be
/// spawned inside `body` (same runtime) for its sleep to share the paused clock.
async fn with_paused<F>(body: F) -> AskOutcome
where
    F: FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = AskOutcome>>>
        + Send
        + 'static,
{
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused current-thread runtime");
        rt.block_on(body())
    })
    .await
    .expect("paused runtime thread join")
}

/// The classified outcome of a single timed ask.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AskOutcome {
    Ok,
    TimeoutSome,
    TimeoutNone,
}

fn classify(result: Result<bool, SendError<Sleep, Infallible>>) -> AskOutcome {
    match result {
        Ok(true) => AskOutcome::Ok,
        Ok(false) => panic!("handler unexpectedly replied false"),
        Err(SendError::Timeout(Some(_))) => AskOutcome::TimeoutSome,
        Err(SendError::Timeout(None)) => AskOutcome::TimeoutNone,
        other => panic!("expected Ok or Timeout, got {other:?}"),
    }
}

#[given(regex = r"^the mailbox is occupied so it has no spare capacity$")]
async fn given_mailbox_occupied(world: &mut AskWorld) {
    // Marker: the @timing scenarios reconstruct their own paused-clock actor with
    // a permanently-busy bounded(1) mailbox, so this only flags the no-spare case.
    world.no_spare_capacity = true;
}

#[given(regex = r"^the handler will sleep (\d+)ms before replying$")]
async fn given_handler_sleeps_ms(world: &mut AskWorld, ms: u64) {
    world.handler_sleep_ms = Some(ms);
}

#[given(regex = r"^the handler sleeps before replying$")]
async fn given_handler_sleeps(world: &mut AskWorld) {
    world.handler_sleep_ms = Some(100);
}

#[given(regex = r"^the handler replies promptly$")]
async fn given_handler_prompt(world: &mut AskWorld) {
    world.handler_sleep_ms = Some(0);
}

#[given(regex = r"^the actor has a bounded mailbox of capacity 100 with spare capacity$")]
async fn given_bounded_100_spare(world: &mut AskWorld) {
    world.spare_capacity = true;
}

// --- mailbox_timeout expiring → Timeout(Some) -------------------------------

#[when(regex = r#"^the caller sends "ask\(Msg\)" with a mailbox_timeout of 50ms$"#)]
async fn when_mailbox_timeout_50(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(1));
            actor.wait_for_startup().await;
            // Occupy the single slot permanently: park a Hold handler, then fill
            // the buffer slot, so a third ask never acquires capacity.
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
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.timeout_some = Some(outcome == AskOutcome::TimeoutSome);
}

#[then(regex = r"^the caller receives SendError::Timeout\(Some\(Msg\)\)$")]
async fn then_timeout_some_msg(world: &mut AskWorld) {
    assert_eq!(
        world.timeout_some,
        Some(true),
        "an expired mailbox_timeout must return Timeout(Some(msg)) — message handed back"
    );
}

// --- reply_timeout expiring → Timeout(None) ---------------------------------

#[when(regex = r#"^the caller sends "ask\(Sleep\(100ms\)\)" with a reply_timeout of 90ms$"#)]
async fn when_reply_timeout_90(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(90))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.timeout_none = Some(outcome == AskOutcome::TimeoutNone);
}

#[then(regex = r"^the caller receives SendError::Timeout\(None\)$")]
async fn then_timeout_none(world: &mut AskWorld) {
    assert_eq!(
        world.timeout_none,
        Some(true),
        "an expired reply_timeout (message already enqueued) must return Timeout(None)"
    );
}

// --- reply just inside reply_timeout → Ok -----------------------------------

#[when(regex = r#"^the caller sends "ask\(Sleep\(100ms\)\)" with a reply_timeout of 120ms$"#)]
async fn when_reply_timeout_120(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(120))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.ok_reply = Some(outcome == AskOutcome::Ok);
}

// --- both timeouts: mailbox first → Timeout(Some(Sleep(100ms))) -------------

#[when(
    regex = r#"^the caller sends "ask\(Sleep\(100ms\)\)" with a mailbox_timeout of 50ms and a reply_timeout of 1s$"#
)]
async fn when_both_mailbox_first(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(1));
            actor.wait_for_startup().await;
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
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .reply_timeout(Duration::from_secs(1))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.timeout_some = Some(outcome == AskOutcome::TimeoutSome);
}

#[then(regex = r"^the caller receives SendError::Timeout\(Some\(Sleep\(100ms\)\)\)$")]
async fn then_timeout_some_sleep(world: &mut AskWorld) {
    assert_eq!(
        world.timeout_some,
        Some(true),
        "with no capacity, the mailbox_timeout fires first → Timeout(Some(Sleep(100ms)))"
    );
}

// --- both timeouts: enqueued then slow reply → Timeout(None) ----------------

#[when(
    regex = r#"^the caller sends "ask\(Sleep\(200ms\)\)" with a mailbox_timeout of 1s and a reply_timeout of 50ms$"#
)]
async fn when_both_reply_governs(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(200)))
                .mailbox_timeout(Duration::from_secs(1))
                .reply_timeout(Duration::from_millis(50))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.timeout_none = Some(outcome == AskOutcome::TimeoutNone);
}

// --- reply_timeout of zero → Timeout(None) ----------------------------------

#[when(regex = r#"^the caller sends "ask\(Sleep\)" with a reply_timeout of 0ms$"#)]
async fn when_reply_timeout_zero(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::ZERO)
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.timeout_none = Some(outcome == AskOutcome::TimeoutNone);
}

// --- Duration::MAX reply_timeout → Ok ---------------------------------------

#[when(regex = r#"^the caller sends "ask\(Msg\)" with a reply_timeout of Duration::MAX$"#)]
async fn when_reply_timeout_max(world: &mut AskWorld) {
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(1)))
                .reply_timeout(Duration::MAX)
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    world.ok_reply = Some(outcome == AskOutcome::Ok);
}

#[then(regex = r"^the caller receives the handler's Ok reply without the timeout firing$")]
async fn then_ok_no_timeout(world: &mut AskWorld) {
    assert_eq!(
        world.ok_reply,
        Some(true),
        "a Duration::MAX reply_timeout is effectively unbounded → Ok"
    );
}

// --- concurrent: short reply_timeout fails, rest succeed --------------------

#[given(regex = r"^the handler sleeps 100ms before replying$")]
async fn given_handler_sleeps_100(world: &mut AskWorld) {
    world.handler_sleep_ms = Some(100);
}

#[when(
    regex = r#"^10 callers concurrently send "ask\(Sleep\(100ms\)\)" and one of them uses a reply_timeout of 10ms$"#
)]
async fn when_10_concurrent_one_short(world: &mut AskWorld) {
    // Paused current-thread runtime: spawn the actor + all 10 asks as tasks, then
    // block_on a join. The clock auto-advances so the 10ms caller times out while
    // the 100ms handler completes the rest. A LocalSet keeps tasks single-thread.
    let (short_none, others) = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let barrier = Arc::new(Barrier::new(10));
            let mut handles = Vec::new();
            for i in 0..10u64 {
                let actor = actor.clone();
                let barrier = Arc::clone(&barrier);
                handles.push(tokio::spawn(async move {
                    barrier.wait().await;
                    if i == 0 {
                        let r = actor
                            .ask(Sleep(Duration::from_millis(100)))
                            .reply_timeout(Duration::from_millis(10))
                            .send()
                            .await;
                        (true, classify(r))
                    } else {
                        let r = actor.ask(Sleep(Duration::from_millis(100))).send().await;
                        (false, classify(r))
                    }
                }));
            }
            let mut short = AskOutcome::Ok;
            let mut others = Vec::new();
            for h in handles {
                let (is_short, outcome) = h.await.expect("concurrent ask join");
                if is_short {
                    short = outcome;
                } else {
                    others.push(outcome);
                }
            }
            actor.kill();
            (short == AskOutcome::TimeoutNone, others)
        })
    })
    .await
    .expect("paused runtime thread join");
    world.short_timeout_none = Some(short_none);
    world.other_replies = others.into_iter().map(|o| o == AskOutcome::Ok).collect();
}

#[then(regex = r"^the short-timeout caller receives SendError::Timeout\(None\)$")]
async fn then_short_timeout_none(world: &mut AskWorld) {
    assert_eq!(
        world.short_timeout_none,
        Some(true),
        "the 10ms-reply_timeout caller must fail with Timeout(None)"
    );
}

#[then(regex = r"^every other caller receives the handler's Ok reply$")]
async fn then_others_ok(world: &mut AskWorld) {
    assert_eq!(world.other_replies.len(), 9, "nine other callers");
    assert!(
        world.other_replies.iter().all(|&ok| ok),
        "every non-short caller must receive Ok, got {:?}",
        world.other_replies
    );
}

// ===========================================================================
// @property / @model laws (request_ask.properties.feature)
// ===========================================================================

// -- @property: reply_timeout Ok iff d < t, else Timeout(None) ---------------

#[given(
    regex = r"^a bounded mailbox of capacity 100 with spare capacity so the mailbox wait is instant$"
)]
async fn law_given_spare_instant(_world: &mut AskWorld) {}

#[given(regex = r"^a handler that sleeps for any delay d before replying Ok$")]
async fn law_given_handler_delay_d(_world: &mut AskWorld) {}

#[when(regex = r#"^the caller sends "ask\(Sleep\(d\)\)" with any reply_timeout t$"#)]
async fn law_when_ask_sleep_d_t(_world: &mut AskWorld) {}

#[then(
    regex = r"^the call returns Ok\(reply\) iff d < t, and SendError::Timeout\(None\) iff d >= t$"
)]
async fn law_reply_timeout_predicate(_world: &mut AskWorld) {
    // GEN: d, t ∈ boundary-biased Duration {ZERO, 1ms, t-1, t, t+1, MAX}; include
    // d==0, t==0 (immediate Timeout(None)), t==MAX (Ok). Paused clock REQUIRED.
    // ORACLE: the boolean predicate d < t. Each (d, t) drives a fresh actor in a
    // dedicated paused runtime so the clock is exact.
    let ms = |n: u64| Duration::from_millis(n);
    let cases: &[(Duration, Duration)] = &[
        (Duration::ZERO, Duration::ZERO), // d>=t → Timeout(None)
        (Duration::ZERO, ms(1)),          // d<t  → Ok
        (ms(50), ms(49)),                 // t-1  → Timeout(None)
        (ms(50), ms(50)),                 // d==t → Timeout(None)
        (ms(50), ms(51)),                 // t+1  → Ok
        (ms(50), Duration::MAX),          // MAX  → Ok
        (ms(1), Duration::ZERO),          // t==0 → Timeout(None)
    ];
    for &(d, t) in cases {
        let outcome = with_paused(move || {
            Box::pin(async move {
                let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
                actor.wait_for_startup().await;
                let r = actor.ask(Sleep(d)).reply_timeout(t).send().await;
                actor.kill();
                classify(r)
            })
        })
        .await;
        let expected = if d < t {
            AskOutcome::Ok
        } else {
            AskOutcome::TimeoutNone
        };
        assert_eq!(
            outcome, expected,
            "d={d:?} t={t:?}: predicate d<t must decide Ok vs Timeout(None)"
        );
    }
}

#[then(regex = r"^the Timeout case is always None because the message was already enqueued$")]
async fn law_timeout_always_none(_world: &mut AskWorld) {
    // The predicate law above already asserts the Timeout arm is exactly
    // Timeout(None) (classify distinguishes Some/None); re-running a forced-timeout
    // case here makes this Then a real assertion rather than a pass-through.
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
            actor.wait_for_startup().await;
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .reply_timeout(Duration::from_millis(10))
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    assert_eq!(
        outcome,
        AskOutcome::TimeoutNone,
        "an enqueued-then-timed-out ask must be Timeout(None), never Some"
    );
}

// -- @property: no spare capacity → mailbox_timeout first, Timeout(Some) ------

#[given(regex = r"^a bounded mailbox of capacity 1 occupied so it has no spare capacity$")]
async fn law_given_cap1_occupied(_world: &mut AskWorld) {}

#[given(regex = r"^a handler that would sleep for any delay d$")]
async fn law_given_would_sleep_d(_world: &mut AskWorld) {}

#[when(
    regex = r#"^the caller sends "ask\(Sleep\(d\)\)" with any mailbox_timeout tm and any reply_timeout tr$"#
)]
async fn law_when_ask_tm_tr(_world: &mut AskWorld) {}

#[then(regex = r"^the call returns SendError::Timeout\(Some\(Sleep\(d\)\)\) for every tm, tr$")]
async fn law_no_capacity_always_some(_world: &mut AskWorld) {
    // GEN: tm, tr ∈ boundary-biased {ZERO, 1ms, 50ms, MAX}; d arbitrary. With the
    // single slot permanently busy, the mailbox wait always elapses → Timeout(Some).
    let ms = |n: u64| Duration::from_millis(n);
    let durs = [Duration::ZERO, ms(1), ms(50), Duration::MAX];
    let d = ms(100);
    for &tm in &durs {
        for &tr in &durs {
            // tm == MAX would wait forever for capacity that never frees, so the
            // mailbox wait can only ELAPSE for a finite tm. The # GEN includes MAX
            // as a reply_timeout boundary; for the mailbox_timeout we skip MAX
            // (an unbounded mailbox wait against a permanently-full slot is a hang
            // by construction, not a Timeout — the law is about FINITE tm).
            if tm == Duration::MAX {
                continue;
            }
            let outcome = with_paused(move || {
                Box::pin(async move {
                    let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(1));
                    actor.wait_for_startup().await;
                    let (_tx, rx) = watch::channel(false);
                    actor
                        .tell(Hold(rx.clone()))
                        .send()
                        .await
                        .expect("first hold");
                    tokio::time::sleep(ms(1)).await;
                    actor
                        .tell(Hold(rx))
                        .try_send()
                        .expect("second hold fills slot");
                    let r = actor
                        .ask(Sleep(d))
                        .mailbox_timeout(tm)
                        .reply_timeout(tr)
                        .send()
                        .await;
                    actor.kill();
                    classify(r)
                })
            })
            .await;
            assert_eq!(
                outcome,
                AskOutcome::TimeoutSome,
                "tm={tm:?} tr={tr:?}: no-capacity ask must be Timeout(Some(msg))"
            );
        }
    }
}

#[then(regex = r"^the reply_timeout clock never starts because capacity was never acquired$")]
async fn law_reply_clock_never_starts(_world: &mut AskWorld) {
    // Observable proof: the outcome carries the MESSAGE back (Timeout(Some)),
    // which only happens on the mailbox wait — the reply path (Timeout(None))
    // was never reached. Re-assert one representative case with a tiny reply
    // timeout that would otherwise fire first if the clock had started.
    let outcome = with_paused(|| {
        Box::pin(async {
            let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(1));
            actor.wait_for_startup().await;
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
            let r = actor
                .ask(Sleep(Duration::from_millis(100)))
                .mailbox_timeout(Duration::from_millis(50))
                .reply_timeout(Duration::ZERO)
                .send()
                .await;
            actor.kill();
            classify(r)
        })
    })
    .await;
    assert_eq!(
        outcome,
        AskOutcome::TimeoutSome,
        "a zero reply_timeout must NOT pre-empt the mailbox wait — outcome is Timeout(Some)"
    );
}

// -- @model: N concurrent asks, per-caller reply isolation -------------------

#[given(
    regex = r"^a bounded mailbox of any capacity c and an echo handler returning the integer it received$"
)]
async fn law_given_any_cap_echo(_world: &mut AskWorld) {}

#[given(regex = r"^any number N of callers each holding a distinct integer payload$")]
async fn law_given_n_distinct(_world: &mut AskWorld) {}

#[when(regex = r#"^all N callers concurrently send "ask\(n\)" with real overlap under a barrier$"#)]
async fn law_when_n_overlap(_world: &mut AskWorld) {}

#[then(regex = r"^every caller receives Ok\(n\) equal to the n it sent, exactly one reply each$")]
async fn law_model_per_caller(_world: &mut AskWorld) {
    // GEN: N ∈ {2, 4, 64} (include N==2 and N>c); c ∈ {1, 4, 64}; payloads distinct
    // ints. Real overlap via tokio::spawn + Barrier on the multi-thread runtime.
    // ORACLE: identity map n→n; the (caller, reply) set must be a bijection.
    for (c, n) in [(1usize, 2u64), (4, 8), (64, 64), (1, 64), (4, 64)] {
        run_model_isolation(c, n).await;
    }
}

#[then(
    regex = r"^no reply is delivered to the wrong caller and none is lost, for any N and any c$"
)]
async fn law_model_no_loss(_world: &mut AskWorld) {
    // Re-run a representative (c, N) so this Then is a real assertion: the
    // bijection check inside run_model_isolation is the no-cross-talk / no-loss
    // oracle (any cross-talk or loss breaks the n→n equality or the count).
    run_model_isolation(4, 64).await;
}

/// One (capacity c, caller count n) isolation case with REAL overlap. Asserts
/// each caller's reply equals its own n (no cross-talk) and exactly the n
/// distinct callers are answered (none lost / duplicated).
async fn run_model_isolation(c: usize, n: u64) {
    let actor = Asked::spawn_with_mailbox(Asked, mailbox::bounded(c));
    actor.wait_for_startup().await;
    let barrier = Arc::new(Barrier::new(n as usize));
    let handles: Vec<_> = (0..n)
        .map(|payload| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let reply = actor.ask(Echo(payload)).await.expect("echo ask");
                (payload, reply)
            })
        })
        .collect();
    let mut pairs = Vec::new();
    for h in handles {
        pairs.push(h.await.expect("model isolation join"));
    }
    for (payload, reply) in &pairs {
        assert_eq!(
            payload, reply,
            "c={c} N={n}: caller {payload} received {reply} (cross-talk)"
        );
    }
    let mut got: Vec<u64> = pairs.iter().map(|(p, _)| *p).collect();
    got.sort_unstable();
    assert_eq!(
        got,
        (0..n).collect::<Vec<_>>(),
        "c={c} N={n}: exactly the N distinct callers must be answered, none lost"
    );
    actor.kill();
}

// -- @model @timing: per-caller short timeout decided independently ----------

#[given(
    regex = r"^a bounded mailbox of capacity 100 and a handler that sleeps a fixed delay d before replying$"
)]
async fn law_given_cap100_fixed_d(_world: &mut AskWorld) {}

#[when(
    regex = r#"^N callers concurrently send "ask\(Sleep\(d\)\)", each with its own reply_timeout t_i, under a barrier$"#
)]
async fn law_when_n_per_caller_t(_world: &mut AskWorld) {}

#[then(
    regex = r"^each caller i receives Ok iff d < t_i, else SendError::Timeout\(None\), independently of the others$"
)]
async fn law_model_per_caller_timeout(_world: &mut AskWorld) {
    // GEN: N=16; d fixed; t_i ∈ boundary-biased {d-1, d, d+1, MAX} so both
    // outcomes occur. Paused clock. ORACLE: per-caller predicate d < t_i, each
    // independent.
    //
    // AUTHORING CORRECTION (single-writer queueing): the law is "each caller's
    // own reply_timeout governs that caller, INDEPENDENTLY of the others". A
    // single shared actor is single-writer — it serialises handlers FIFO, so
    // caller k's reply arrives at ~(k+1)·d, and a t_i = d+1 caller times out
    // purely from QUEUE position, not from its own delay d (verified by a
    // throwaway probe: with one bounded(100) actor only the t=MAX callers
    // succeed). That confounds the per-caller independence the law asserts. To
    // make d the delay EACH caller actually observes — the law's intent — each
    // caller asks its OWN actor (delay d, no queueing). Real overlap is still
    // exercised: all 16 caller tasks start at one Barrier on the paused clock.
    let outcomes = tokio::task::spawn_blocking(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("paused runtime");
        rt.block_on(async {
            let d = Duration::from_millis(50);
            // t_i cycles through {d-1, d, d+1, MAX} across N=16 callers.
            let ts = [
                Duration::from_millis(49),
                Duration::from_millis(50),
                Duration::from_millis(51),
                Duration::MAX,
            ];
            let n = 16usize;
            // One actor per caller so the observed delay is exactly d.
            let mut actors = Vec::with_capacity(n);
            for _ in 0..n {
                let a = Asked::spawn_with_mailbox(Asked, mailbox::bounded(100));
                a.wait_for_startup().await;
                actors.push(a);
            }
            let barrier = Arc::new(Barrier::new(n));
            let mut handles = Vec::new();
            for (i, actor) in actors.into_iter().enumerate() {
                let barrier = Arc::clone(&barrier);
                let t = ts[i % ts.len()];
                handles.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let r = actor.ask(Sleep(d)).reply_timeout(t).send().await;
                    actor.kill();
                    (d < t, classify(r))
                }));
            }
            let mut results = Vec::new();
            for h in handles {
                results.push(h.await.expect("per-caller timeout join"));
            }
            results
        })
    })
    .await
    .expect("paused runtime join");
    for (expect_ok, outcome) in outcomes {
        let expected = if expect_ok {
            AskOutcome::Ok
        } else {
            AskOutcome::TimeoutNone
        };
        assert_eq!(
            outcome, expected,
            "per-caller predicate d<t_i must decide each caller independently"
        );
    }
}
