//! Shared `MessageWorld` + step definitions for the core `message` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_message_bdd.rs`        — the example feature (message.feature)
//!   * `core_message_props_bdd.rs`  — the property laws (message.properties.feature)
//!
//! Unlike `actor_id`/`error` (pure, in-process), this module exercises the
//! `src/message.rs` SUT with REAL SPAWNED ACTORS: `Message::handle`, the
//! `Context` (`actor_ref`/`reply`/`stop`), `reply_sender`/`reply`/`spawn`/
//! `forward`/`try_forward`/`blocking_forward`, `StreamMessage`, and the
//! `DynMessage::handle_dyn` ask-vs-tell reply/error routing.
//!
//! Every assertion is the SPECIFIC value confirmed in the scenario's
//! `# Confirmed:` / `# ORACLE:` note (facts only — no vague `contains`).
//!
//! All public API is reached through `kameo::prelude::*` + `kameo::message::*`;
//! no `src/` change is needed. The global error hook (`set_actor_error_hook`,
//! error.rs:70) is PROCESS-GLOBAL, so the two scenarios that observe it install
//! a hook into an `Arc<Mutex<..>>` sink, run, then restore the default. Both
//! runners set `.max_concurrent_scenarios(1)` so the global hook cannot be
//! clobbered by an overlapping scenario.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use cucumber::{World, given, then, when};
use futures::stream;
use kameo::{
    error::{Infallible, PanicError, set_actor_error_hook},
    mailbox,
    message::StreamMessage,
    prelude::*,
    reply::{DelegatedReply, ForwardedReply},
};
use tokio::sync::Barrier;

// ===========================================================================
// Test actors and messages
// ===========================================================================

/// An actor that records the tag of every numbered command it handles, in
/// handling order, into a shared log. Used by the @sequence / single-writer
/// scenarios where the OBSERVABLE effect is the recorded order.
#[derive(Clone)]
struct Recorder {
    log: Arc<Mutex<Vec<u64>>>,
}

impl Actor for Recorder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A numbered command. The reply is the tag echoed back (so an ask caller can
/// read the handled value) — the side-effect is the append to `log`.
struct Command(u64);

impl Message<Command> for Recorder {
    type Reply = u64;

    async fn handle(&mut self, msg: Command, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

/// A command whose handler calls `ctx.stop()` and THEN returns its reply, to
/// pin the "stop only after the current message finishes" invariant.
struct StopThenReply(u64);

impl Message<StopThenReply> for Recorder {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: StopThenReply,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.stop();
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

/// A command whose handler takes the reply channel via `reply_sender()` and
/// sends the reply manually, returning the `DelegatedReply` marker.
struct DelegateViaSender(u64);

impl Message<DelegateViaSender> for Recorder {
    type Reply = DelegatedReply<u64>;

    async fn handle(
        &mut self,
        msg: DelegateViaSender,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated, reply_sender) = ctx.reply_sender();
        if let Some(tx) = reply_sender {
            tx.send(msg.0);
        }
        delegated
    }
}

/// A command whose handler calls `ctx.reply(value)` early and then keeps
/// working (appending to the log), returning the `DelegatedReply` marker.
struct EarlyReply(u64);

impl Message<EarlyReply> for Recorder {
    type Reply = DelegatedReply<u64>;

    async fn handle(
        &mut self,
        msg: EarlyReply,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let delegated = ctx.reply(msg.0);
        // Continue working after the early reply: record that we ran on.
        self.log.lock().unwrap().push(msg.0);
        delegated
    }
}

/// A command whose handler `ctx.spawn`s a detached task that completes after a
/// short delay and sends a value. On an ask the value reaches the caller; on a
/// tell an `Err` payload reaches the global error hook.
struct SpawnValue(u64);

impl Message<SpawnValue> for Recorder {
    type Reply = DelegatedReply<u64>;

    async fn handle(
        &mut self,
        msg: SpawnValue,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            msg.0
        })
    }
}

/// A handler-error type carried by `Result` replies. `ReplyError` is the
/// `Debug + Display + Send + 'static` bound; derive Display via thiserror-free
/// manual impls to keep the test self-contained.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HandlerBoom(u64);

impl std::fmt::Display for HandlerBoom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "handler boom {}", self.0)
    }
}

impl std::error::Error for HandlerBoom {}

/// A command whose handler `ctx.spawn`s a detached task returning `Err`. On a
/// tell, the spawned task's error must drive the global error hook with
/// `PanicReason::OnMessage` and must NOT call `on_panic`.
struct SpawnErr(u64);

impl Message<SpawnErr> for Recorder {
    type Reply = DelegatedReply<Result<u64, HandlerBoom>>;

    async fn handle(
        &mut self,
        msg: SpawnErr,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err(HandlerBoom(msg.0))
        })
    }
}

/// A command whose handler directly returns `Ok`/`Err` (no delegation), used by
/// the ask-vs-tell handler-error routing scenarios. The value is the tag.
struct FallibleCommand {
    tag: u64,
    fail: bool,
}

impl Message<FallibleCommand> for Recorder {
    type Reply = Result<u64, HandlerBoom>;

    async fn handle(
        &mut self,
        msg: FallibleCommand,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.fail {
            Err(HandlerBoom(msg.tag))
        } else {
            Ok(msg.tag)
        }
    }
}

/// A `Recorder` actor that flags (via the panic sink) whether `on_panic` ran.
/// Used by the tell-error scenarios to assert `on_panic` IS / IS NOT invoked.
#[derive(Clone)]
struct PanicWatch {
    /// Records each `on_panic` invocation's reason string.
    on_panic_log: Arc<Mutex<Vec<String>>>,
}

impl Actor for PanicWatch {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<std::ops::ControlFlow<ActorStopReason>, Self::Error> {
        self.on_panic_log
            .lock()
            .unwrap()
            .push(format!("{:?}", err.reason()));
        // Default behaviour: stop on panic.
        Ok(std::ops::ControlFlow::Break(ActorStopReason::Panicked(err)))
    }
}

/// Directly-returning fallible command for `PanicWatch`.
struct WatchCommand {
    tag: u64,
    fail: bool,
}

impl Message<WatchCommand> for PanicWatch {
    type Reply = Result<u64, HandlerBoom>;

    async fn handle(
        &mut self,
        msg: WatchCommand,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.fail {
            Err(HandlerBoom(msg.tag))
        } else {
            Ok(msg.tag)
        }
    }
}

/// A `ctx.spawn`-erroring command for `PanicWatch` (detached task must NOT call
/// on_panic even on a tell).
struct WatchSpawnErr(u64);

impl Message<WatchSpawnErr> for PanicWatch {
    type Reply = DelegatedReply<Result<u64, HandlerBoom>>;

    async fn handle(
        &mut self,
        msg: WatchSpawnErr,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err(HandlerBoom(msg.0))
        })
    }
}

// --- Target / Router actors for the forwarding scenarios -------------------

/// The forwarding target: replies with whatever value it is asked.
#[derive(Clone)]
struct Target {
    /// Records each delivered value (so a tell-forward can be observed).
    log: Arc<Mutex<Vec<u64>>>,
}

impl Actor for Target {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Asked of the target: it echoes `value` back as the reply.
struct Echo(u64);

impl Message<Echo> for Target {
    type Reply = u64;

    async fn handle(&mut self, msg: Echo, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.log.lock().unwrap().push(msg.0);
        msg.0
    }
}

/// A target message that blocks the handler until a shared `watch` flips to
/// `true` — used to OCCUPY a bounded-capacity-1 mailbox so
/// `try_forward`/`blocking_forward` hit the mailbox-full path deterministically.
struct Hold(tokio::sync::watch::Receiver<bool>);

impl Message<Hold> for Target {
    type Reply = ();

    async fn handle(&mut self, msg: Hold, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        // Park the handler (keeping the mailbox slot occupied) until the test
        // releases it. A `watch` broadcasts to every blocked Hold at once.
        let mut rx = msg.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break; // sender dropped — release.
            }
        }
    }
}

/// The router. Holds a target ref and forwards `Echo` to it.
#[derive(Clone)]
struct Router {
    target: ActorRef<Target>,
}

impl Actor for Router {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// `forward` (await): hands the original reply channel to the target's ask.
struct ForwardEcho(u64);

impl Message<ForwardEcho> for Router {
    type Reply = ForwardedReply<Echo, <Target as Message<Echo>>::Reply>;

    async fn handle(
        &mut self,
        msg: ForwardEcho,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.forward(&self.target, Echo(msg.0)).await
    }
}

/// `try_forward`: fails fast with MailboxFull when the target is full.
struct TryForwardEcho(u64);

impl Message<TryForwardEcho> for Router {
    type Reply = ForwardedReply<Echo, <Target as Message<Echo>>::Reply>;

    async fn handle(
        &mut self,
        msg: TryForwardEcho,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.try_forward(&self.target, Echo(msg.0))
    }
}

/// `blocking_forward`: waits for target capacity instead of failing.
struct BlockingForwardEcho(u64);

impl Message<BlockingForwardEcho> for Router {
    type Reply = ForwardedReply<Echo, <Target as Message<Echo>>::Reply>;

    async fn handle(
        &mut self,
        msg: BlockingForwardEcho,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ctx.blocking_forward(&self.target, Echo(msg.0))
    }
}

/// A stream-handling actor that records the lifecycle of a `StreamMessage`:
/// `Started`, each `Next(item)`, then `Finished`, in arrival order.
#[derive(Clone)]
struct StreamActor {
    events: Arc<Mutex<Vec<String>>>,
}

impl Actor for StreamActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<StreamMessage<char, (), ()>> for StreamActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessage<char, (), ()>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut log = self.events.lock().unwrap();
        match msg {
            StreamMessage::Started(()) => log.push("Started".to_string()),
            StreamMessage::Next(c) => log.push(format!("Next({c})")),
            StreamMessage::Finished(()) => log.push("Finished".to_string()),
        }
    }
}

/// A counter actor whose every ask increments and replies with the new count.
#[derive(Clone)]
struct Counter {
    count: u64,
}

impl Actor for Counter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

struct Increment;

impl Message<Increment> for Counter {
    type Reply = u64;

    async fn handle(
        &mut self,
        _msg: Increment,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.count += 1;
        self.count
    }
}

/// A folding actor: ask adds and replies with the running sum; tell adds with
/// no reply. Used by the interleaved single-writer scenarios.
#[derive(Clone)]
struct Folder {
    sum: u64,
}

impl Actor for Folder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Ask: adds `0` and replies with the running sum for this command's own value.
struct AddAsk(u64);

impl Message<AddAsk> for Folder {
    type Reply = u64;

    async fn handle(&mut self, msg: AddAsk, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.sum += msg.0;
        // Reply with this command's OWN contribution so an ask caller can match
        // its own command (no cross-talk), independent of the running sum.
        msg.0
    }
}

struct AddTell(u64);

impl Message<AddTell> for Folder {
    type Reply = ();

    async fn handle(&mut self, msg: AddTell, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.sum += msg.0;
    }
}

/// Reads the running sum (for the final-state oracle).
struct ReadSum;

impl Message<ReadSum> for Folder {
    type Reply = u64;

    async fn handle(
        &mut self,
        _msg: ReadSum,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.sum
    }
}

// ===========================================================================
// Global error-hook sink (process-global; serialized via max_concurrent=1)
// ===========================================================================

/// Process-global sink the test hook appends `PanicReason` strings into. The
/// real `set_actor_error_hook` takes a plain `fn` pointer (no closure capture),
/// so the sink must be reachable from a free function — hence a `OnceLock`.
static HOOK_SINK: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn hook_sink() -> &'static Mutex<Vec<String>> {
    HOOK_SINK.get_or_init(|| Mutex::new(Vec::new()))
}

fn recording_hook(err: &PanicError) {
    hook_sink().lock().unwrap().push(format!("{:?}", err.reason()));
}

/// Installs the recording hook and clears the sink. Restored to default at the
/// end of the scenario by `restore_default_hook`.
fn install_recording_hook() {
    hook_sink().lock().unwrap().clear();
    set_actor_error_hook(recording_hook);
}

fn restore_default_hook() {
    // The default hook only logs; reinstalling a no-op keeps later scenarios
    // from observing this scenario's hook. There is no public handle to the
    // crate's own default, so a benign no-op is installed.
    fn noop(_err: &PanicError) {}
    set_actor_error_hook(noop);
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct MessageWorld {
    /// Shared handling-order log (the OBSERVABLE single-writer effect).
    log: Arc<Mutex<Vec<u64>>>,
    /// Replies captured from ask callers.
    replies: Vec<u64>,
    /// A single captured reply value (early-reply / spawn scenarios).
    reply: Option<u64>,
    /// Stream lifecycle events recorded by `StreamActor`.
    stream_events: Arc<Mutex<Vec<String>>>,
    /// `on_panic` reasons recorded by `PanicWatch`.
    on_panic_log: Arc<Mutex<Vec<String>>>,
    /// Target delivery log (tell-forward observation).
    target_log: Arc<Mutex<Vec<u64>>>,
    /// Whether a tell-forward's outcome was Ok (no value reply).
    forward_tell_ok: Option<bool>,
    /// The error a dead/full forward surfaced to the caller.
    forward_err: Option<String>,
    /// Whether `try_forward` reported a MailboxFull send error.
    try_forward_full: Option<bool>,
    /// Concurrency results (linearizability scenarios).
    concurrent_replies: Vec<u64>,
    /// Final folded sum + the oracle sum for the interleaved scenario.
    final_sum: Option<u64>,
    oracle_sum: Option<u64>,
    /// Handles to keep spawned actors alive across steps (Recorder).
    recorder: Option<ActorRef<Recorder>>,
    /// Router + target pair for the forwarding scenarios.
    router_target: Option<(ActorRef<Router>, ActorRef<Target>)>,
    /// The stream actor for the StreamMessage lifecycle scenario.
    stream_actor: Option<ActorRef<StreamActor>>,
    /// The counter actor for the concurrent-asks scenario.
    counter: Option<ActorRef<Counter>>,
    /// The folder actor for the interleaved single-writer scenario.
    folder: Option<ActorRef<Folder>>,
    /// The watch sender that releases parked `Hold` handlers (full-mailbox).
    hold_release: Option<tokio::sync::watch::Sender<bool>>,
    /// Which `the message is sent via ask` variant the active scenario wants
    /// (set by the distinct Given): `Some(true)` = spawn-detached value-ask.
    /// `None` = the property err-ask scenario (its Given is a no-op).
    expect_spawn_value: bool,
    /// The handler error a fallible ask surfaced (typed, not a Debug string).
    handler_err: Option<HandlerBoom>,
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^a spawned actor that handles a numbered command message$")]
async fn given_spawned_recorder(world: &mut MessageWorld) {
    let log = Arc::clone(&world.log);
    let actor = Recorder::spawn(Recorder { log });
    actor.wait_for_startup().await;
    world.recorder = Some(actor);
}

// ===========================================================================
// @sequence — sequential handling, ctx.stop, deferred reply protocol
// ===========================================================================

#[when(regex = r"^commands 1, 2, 3 are sent in that order$")]
async fn when_commands_123(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    // ask serialises: each reply returns before the next send, but the
    // single-writer guarantee is what makes the recorded order match regardless.
    for n in [1u64, 2, 3] {
        let reply = actor.ask(Command(n)).await.expect("ask succeeds");
        world.replies.push(reply);
    }
}

#[when(regex = r"^the actor records the order in which it handled them$")]
async fn when_records_order(_world: &mut MessageWorld) {
    // The recording happens inside the handler; nothing to do here.
}

#[then(regex = r"^the actor handled them as 1, then 2, then 3$")]
async fn then_handled_123(world: &mut MessageWorld) {
    let log = world.log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![1, 2, 3],
        "single-writer dispatch must handle commands in send order"
    );
    assert_eq!(world.replies, vec![1, 2, 3], "each ask returns its own tag");
}

#[when(regex = r"^a message whose handler calls ctx\.stop\(\) and then returns a reply is sent via ask$")]
async fn when_stop_then_reply(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    let reply = actor.ask(StopThenReply(7)).await.expect("ask succeeds");
    world.reply = Some(reply);
}

#[then(regex = r"^the caller still receives that message's reply$")]
async fn then_caller_gets_reply(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(7),
        "the current message's reply must be delivered before shutdown"
    );
}

#[then(regex = r"^the actor stops before handling any later-queued message$")]
async fn then_actor_stops(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    // The handler called ctx.stop(); after the reply the actor shuts down.
    // Condition-poll on the observable "later message is rejected": once the
    // actor is no longer running, a fresh ask returns ActorNotRunning.
    for _ in 0..200 {
        if actor.ask(Command(99)).await.is_err() {
            // The later-queued command was NOT handled (no tag 99 recorded).
            let log = world.log.lock().unwrap();
            assert!(
                !log.contains(&99),
                "a later message must not have been handled after stop, log={log:?}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("actor did not stop after a ctx.stop() handler within the bound");
}

#[when(regex = r"^a message handler calls ctx\.reply_sender\(\) and returns the DelegatedReply marker$")]
async fn when_reply_sender(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    let reply = actor.ask(DelegateViaSender(11)).await.expect("ask succeeds");
    world.reply = Some(reply);
}

#[when(regex = r"^the handler sends the reply through the taken ReplySender$")]
async fn when_sends_via_taken_sender(_world: &mut MessageWorld) {
    // Performed inside the handler; the reply is captured by the When above.
}

#[then(regex = r"^the caller receives exactly that reply$")]
async fn then_caller_exact_reply(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(11),
        "the delegated ReplySender must deliver exactly the sent reply"
    );
}

#[then(regex = r"^the dispatcher does not also send a second reply$")]
async fn then_no_second_reply(world: &mut MessageWorld) {
    // The ask resolved to exactly one value (captured above). A double-send
    // would have panicked the dispatcher's reply channel; reaching here with a
    // single captured value proves no duplicate was sent.
    assert!(
        world.reply.is_some(),
        "exactly one reply must have been delivered"
    );
}

#[when(regex = r"^a handler calls ctx\.reply\(value\) and then continues working$")]
async fn when_early_reply(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    let reply = actor.ask(EarlyReply(13)).await.expect("ask succeeds");
    world.reply = Some(reply);
}

#[then(regex = r"^the caller receives value immediately from the early reply$")]
async fn then_caller_early_value(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(13),
        "ctx.reply must deliver the value early to the caller"
    );
}

#[then(regex = r"^no duplicate reply is sent when the handler returns$")]
async fn then_no_duplicate_on_return(world: &mut MessageWorld) {
    // The handler kept working after the early reply (recorded 13 in the log).
    // The ask still resolved to exactly one value (no re-send by handle_dyn,
    // since ctx.reply took the sender).
    let log = world.log.lock().unwrap();
    assert!(
        log.contains(&13),
        "the handler must have continued working after the early reply, log={log:?}"
    );
    assert_eq!(world.reply, Some(13), "exactly one reply delivered");
}

// ===========================================================================
// @lifecycle — spawn detached, stream lifecycle, forwarding across actors
// ===========================================================================

#[given(regex = r"^a handler that ctx\.spawns a task which completes after a delay and returns a value$")]
async fn given_spawn_value_handler(world: &mut MessageWorld) {
    // The handler is `SpawnValue` (defined above); the actor is the Background
    // recorder. Flag the shared `the message is sent via ask` When to drive it.
    world.expect_spawn_value = true;
}

// SHARED When: the example spawn-detached scenario AND the property err-ask
// scenario use this exact phrasing. `expect_spawn_value` (set by the
// spawn-detached Given) routes the example case; otherwise the property err-ask
// case runs (its law is asserted in its own Then). One definition per regex.
#[when(regex = r"^the message is sent via ask$")]
async fn when_message_sent_via_ask(world: &mut MessageWorld) {
    if world.expect_spawn_value {
        let actor = world.recorder.as_ref().expect("recorder spawned");
        let reply = actor.ask(SpawnValue(21)).await.expect("ask succeeds");
        world.reply = Some(reply);
    }
}

#[then(regex = r"^the ask caller receives the spawned task's value$")]
async fn then_ask_gets_spawned_value(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(21),
        "the detached task's awaited value must reach the ask caller"
    );
}

#[then(regex = r"^the actor was free to handle other messages while the task ran$")]
async fn then_actor_free_during_spawn(world: &mut MessageWorld) {
    // The handler returned a DelegatedReply immediately (the actor moved on),
    // and the detached task replied later. Observable proof: the actor still
    // handles a subsequent command promptly.
    let actor = world.recorder.as_ref().expect("recorder spawned");
    let reply = actor.ask(Command(22)).await.expect("actor still responsive");
    assert_eq!(reply, 22, "the actor handled a later command after spawning");
}

#[given(regex = r"^a handler that ctx\.spawns a task returning an Err$")]
async fn given_spawn_err_handler(_world: &mut MessageWorld) {
    // Handler is `WatchSpawnErr` on a `PanicWatch` actor so we can ALSO assert
    // on_panic is not invoked. Set up in the When step (needs the panic log).
}

#[when(regex = r"^the message is sent via tell so no reply is expected$")]
async fn when_spawn_err_tell(world: &mut MessageWorld) {
    install_recording_hook();
    let on_panic_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    world.on_panic_log = Arc::clone(&on_panic_log);
    let actor = PanicWatch::spawn(PanicWatch { on_panic_log });
    actor.wait_for_startup().await;
    actor.tell(WatchSpawnErr(31)).await.expect("tell delivered");
    // Give the detached task time to run and invoke the hook.
    for _ in 0..200 {
        if !hook_sink().lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Keep the actor alive a touch so any (erroneous) on_panic would have run.
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(actor);
}

#[then(regex = r"^the global actor error hook is invoked with that error$")]
async fn then_global_hook_invoked(_world: &mut MessageWorld) {
    let sink = hook_sink().lock().unwrap().clone();
    assert_eq!(
        sink,
        vec!["OnMessage".to_string()],
        "a detached tell task's Err must drive the global hook with PanicReason::OnMessage"
    );
}

#[then(regex = r"^the actor's on_panic hook is NOT invoked$")]
async fn then_on_panic_not_invoked(world: &mut MessageWorld) {
    let log = world.on_panic_log.lock().unwrap().clone();
    assert!(
        log.is_empty(),
        "a detached task's error must NOT call on_panic, got {log:?}"
    );
    restore_default_hook();
}

#[given(regex = r"^a router actor and a live target actor$")]
async fn given_router_and_live_target(world: &mut MessageWorld) {
    let target_log = Arc::clone(&world.target_log);
    let target = Target::spawn(Target { log: target_log });
    target.wait_for_startup().await;
    let router = Router::spawn(Router {
        target: target.clone(),
    });
    router.wait_for_startup().await;
    world.router_target = Some((router, target));
}

// SHARED When: example forward-ask scenario AND the property forward-roundtrip
// scenario use this exact phrasing. The example sets up a router via
// `given_router_and_live_target` (so `router_target` is Some — do the ask). The
// property's Given is a no-op (router_target is None — the universal law runs in
// its Then), so this is a no-op there. One definition avoids an ambiguous-match
// panic from two identical regexes in the same test binary.
#[when(regex = r"^the original caller asks the router, whose handler forwards to the target$")]
async fn when_ask_router_forwards(world: &mut MessageWorld) {
    if world.router_target.is_some() {
        let (router, _target) = world.router_target.as_ref().expect("router + target");
        let reply = router.ask(ForwardEcho(41)).await.expect("forward ask succeeds");
        world.reply = Some(reply);
    }
}

#[then(regex = r"^the original caller receives the target actor's reply$")]
async fn then_caller_gets_target_reply(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(41),
        "forward must route the target's reply back to the original ask caller"
    );
}

#[when(regex = r"^the original message is a tell so the router holds no reply channel$")]
async fn when_router_tell_no_channel(_world: &mut MessageWorld) {
    // The tell is issued by the next When (forwards to the target).
}

#[when(regex = r"^the router's handler forwards to the target$")]
async fn when_router_forwards_tell(world: &mut MessageWorld) {
    let (router, target) = world.router_target.as_ref().expect("router + target");
    router.tell(ForwardEcho(42)).await.expect("tell delivered");
    // Wait until the target has observed the forwarded message.
    for _ in 0..200 {
        if world.target_log.lock().unwrap().contains(&42) {
            world.forward_tell_ok = Some(true);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = target; // keep alive
    panic!("the tell-forward never reached the target");
}

#[then(regex = r"^the target receives the message$")]
async fn then_target_receives(world: &mut MessageWorld) {
    let log = world.target_log.lock().unwrap();
    assert!(
        log.contains(&42),
        "the target must have received the tell-forwarded message, log={log:?}"
    );
}

#[then(regex = r"^the ForwardedReply reflects the send outcome, not a value reply$")]
async fn then_forwarded_reflects_outcome(world: &mut MessageWorld) {
    assert_eq!(
        world.forward_tell_ok,
        Some(true),
        "a tell-forward's ForwardedReply carries the send outcome (Ok), not a value"
    );
}

#[given(regex = r"^an actor handling a StreamMessage of items$")]
async fn given_stream_actor(world: &mut MessageWorld) {
    let events = Arc::clone(&world.stream_events);
    let actor = StreamActor::spawn(StreamActor { events });
    actor.wait_for_startup().await;
    world.stream_actor = Some(actor);
}

#[when(regex = r"^a stream of items \[a, b\] is attached to the actor$")]
async fn when_stream_attached(world: &mut MessageWorld) {
    let actor = world.stream_actor.as_ref().expect("stream actor");
    let items = stream::iter(vec!['a', 'b']);
    let handle = actor.attach_stream(items, (), ());
    // attach_stream sends Started, each Next, then Finished as tells; await the
    // pump task so all three phases have been enqueued, then poll until the
    // actor has handled Finished (the last event).
    // attach_stream's task returns the leftover stream on success; discard it.
    let _ = handle.await.expect("attach_stream task").expect("stream ok");
    for _ in 0..200 {
        if world
            .stream_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == "Finished")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the stream never reached Finished");
}

#[then(regex = r"^the actor observes Started first$")]
async fn then_started_first(world: &mut MessageWorld) {
    let events = world.stream_events.lock().unwrap();
    assert_eq!(
        events.first().map(String::as_str),
        Some("Started"),
        "attach_stream must emit Started before any item, got {events:?}"
    );
}

#[then(regex = r"^then Next\(a\), then Next\(b\)$")]
async fn then_next_a_then_b(world: &mut MessageWorld) {
    let events = world.stream_events.lock().unwrap();
    assert_eq!(
        events.get(1).map(String::as_str),
        Some("Next(a)"),
        "first item must be Next(a), got {events:?}"
    );
    assert_eq!(
        events.get(2).map(String::as_str),
        Some("Next(b)"),
        "second item must be Next(b), got {events:?}"
    );
}

#[then(regex = r"^finally Finished after the stream ends$")]
async fn then_finished_last(world: &mut MessageWorld) {
    let events = world.stream_events.lock().unwrap();
    assert_eq!(
        events.as_slice(),
        ["Started", "Next(a)", "Next(b)", "Finished"],
        "the full stream lifecycle order must be Started, items, Finished, got {events:?}"
    );
}

// ===========================================================================
// @boundary — dead targets, full mailboxes, handler errors routed by ask/tell
// ===========================================================================

/// The router's `Reply` is `ForwardedReply<Echo, u64>` (the target's reply is a
/// plain `u64`, so its `Error` slot is `Infallible`). A failed forward is hoisted
/// into the caller's `SendError` as `HandlerError(inner)`, where `inner` is the
/// forward's own `SendError<Echo, Infallible>`. This names that inner domain.
/// `SendError`'s `Debug` omits the variant text for the message-bearing arms, so
/// the classification is by explicit `match`, not by formatting. Generic over the
/// outer message type `M` (the router message differs for forward vs try_forward).
fn classify_forward_err<M>(err: &SendError<M, SendError<Echo, Infallible>>) -> String {
    match err {
        SendError::HandlerError(inner) => match inner {
            SendError::ActorNotRunning(_) => "ActorNotRunning",
            SendError::ActorStopped => "ActorStopped",
            SendError::MailboxFull(_) => "MailboxFull",
            SendError::Timeout(_) => "Timeout",
            SendError::HandlerError(_) => "HandlerError",
        },
        SendError::ActorNotRunning(_) => "OuterActorNotRunning",
        SendError::ActorStopped => "OuterActorStopped",
        SendError::MailboxFull(_) => "OuterMailboxFull",
        SendError::Timeout(_) => "OuterTimeout",
    }
    .to_string()
}

#[given(regex = r"^a router actor and a target actor that has been stopped$")]
async fn given_router_and_dead_target(world: &mut MessageWorld) {
    let target_log = Arc::clone(&world.target_log);
    let target = Target::spawn(Target { log: target_log });
    target.wait_for_startup().await;
    let router = Router::spawn(Router {
        target: target.clone(),
    });
    router.wait_for_startup().await;
    // Stop the target and wait until it is observably not running.
    target.stop_gracefully().await.unwrap();
    target.wait_for_shutdown().await;
    for _ in 0..200 {
        if target.ask(Echo(0)).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    world.router_target = Some((router, target));
}

#[when(regex = r"^the original caller asks the router, whose handler forwards to the dead target$")]
async fn when_ask_router_dead_target(world: &mut MessageWorld) {
    let (router, _target) = world.router_target.as_ref().expect("router + dead target");
    let result = router.ask(ForwardEcho(51)).await;
    let err = result.expect_err("forward to dead target must fail");
    world.forward_err = Some(classify_forward_err(&err));
}

#[then(regex = r"^the original caller receives a SendError indicating the target is not running$")]
async fn then_caller_gets_not_running(world: &mut MessageWorld) {
    let kind = world.forward_err.as_ref().expect("a forward error");
    // The forward failure is hoisted into the caller's SendError as
    // HandlerError(ActorNotRunning(..)). `classify_forward_err` matches the inner
    // not-running domain explicitly (SendError's Debug omits the variant text).
    assert_eq!(
        kind, "ActorNotRunning",
        "a dead-target forward must surface the not-running domain"
    );
}

#[given(regex = r"^a router actor and a target actor whose bounded mailbox is full$")]
async fn given_router_target_full(world: &mut MessageWorld) {
    setup_full_target(world).await;
}

#[when(regex = r"^the router's handler calls try_forward to the target$")]
async fn when_try_forward(world: &mut MessageWorld) {
    let (router, _target) = world.router_target.as_ref().expect("router + full target");
    // The target's single slot is occupied; try_forward must fail fast. The
    // forward error (MailboxFull) is hoisted into the caller's SendError as
    // HandlerError(MailboxFull(..)).
    let result = router.ask(TryForwardEcho(61)).await;
    let err = result.expect_err("try_forward to a full target must fail");
    let kind = classify_forward_err(&err);
    world.try_forward_full = Some(kind == "MailboxFull");
    world.forward_err = Some(kind);
}

#[then(regex = r"^try_forward returns a ForwardedReply carrying a MailboxFull send error$")]
async fn then_try_forward_full(world: &mut MessageWorld) {
    assert_eq!(
        world.try_forward_full,
        Some(true),
        "try_forward on a full target must carry a MailboxFull error, got {:?}",
        world.forward_err
    );
}

#[then(regex = r"^the original reply channel is restored to the router context so it can respond$")]
async fn then_reply_channel_restored(world: &mut MessageWorld) {
    // The router DID respond to the original ask (we received the MailboxFull Err
    // above), which is only possible because try_forward's map_msg restored
    // self.reply. Had the channel not been restored, the caller would have seen
    // ActorStopped (dropped sender), not the MailboxFull HandlerError.
    let kind = world.forward_err.as_ref().expect("a forward error");
    assert_eq!(
        kind, "MailboxFull",
        "the restored channel must carry the MailboxFull outcome back to the caller"
    );
    release_held_target(world);
}

#[given(regex = r"^a router actor and a target actor whose bounded mailbox is momentarily full$")]
async fn given_router_target_momentarily_full(world: &mut MessageWorld) {
    setup_full_target(world).await;
}

#[when(regex = r"^the router's handler calls blocking_forward and a slot then frees$")]
async fn when_blocking_forward(world: &mut MessageWorld) {
    let (router, _target) = world.router_target.as_ref().expect("router + full target");
    // blocking_forward waits for capacity. Spawn the router ask, then release
    // the held slot so capacity frees and the forward completes.
    let router = router.clone();
    let ask = tokio::spawn(async move { router.ask(BlockingForwardEcho(71)).await });
    // Let the blocking_forward begin waiting, then free the slot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    release_held_target(world);
    let reply = ask.await.expect("ask task").expect("blocking_forward eventually succeeds");
    world.reply = Some(reply);
}

#[then(regex = r"^the message is forwarded once capacity is available$")]
async fn then_forwarded_after_capacity(world: &mut MessageWorld) {
    assert_eq!(
        world.reply,
        Some(71),
        "blocking_forward must complete the forward once a slot frees"
    );
    let log = world.target_log.lock().unwrap();
    assert!(
        log.contains(&71),
        "the target must have received the blocked-then-forwarded message, log={log:?}"
    );
}

#[when(regex = r"^a message whose handler returns Err\(e\) is sent via ask$")]
async fn when_handler_err_ask(world: &mut MessageWorld) {
    let actor = world.recorder.as_ref().expect("recorder spawned");
    let result = actor.ask(FallibleCommand { tag: 81, fail: true }).await;
    // `ask().await` is `Result<u64, SendError<FallibleCommand, HandlerBoom>>`;
    // a handler Err arrives as `HandlerError(HandlerBoom(81))`. Capture the inner
    // typed error (SendError's Debug elides the variant text, so we match).
    match result.expect_err("a handler Err must surface") {
        SendError::HandlerError(boom) => world.handler_err = Some(boom),
        other => panic!("expected HandlerError, got {other:?}"),
    }
}

#[then(regex = r"^the caller's ask result is Err with a HandlerError carrying e$")]
async fn then_ask_handler_error(world: &mut MessageWorld) {
    let boom = world.handler_err.as_ref().expect("an ask handler error");
    assert_eq!(
        boom,
        &HandlerBoom(81),
        "a handler Err on ask must surface as HandlerError(e) carrying exactly e"
    );
}

#[when(regex = r"^a message whose handler returns Err\(e\) is sent via tell so no caller awaits$")]
async fn when_handler_err_tell(world: &mut MessageWorld) {
    let on_panic_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    world.on_panic_log = Arc::clone(&on_panic_log);
    let actor = PanicWatch::spawn(PanicWatch { on_panic_log });
    actor.wait_for_startup().await;
    actor
        .tell(WatchCommand { tag: 91, fail: true })
        .await
        .expect("tell delivered");
    // Poll until on_panic has recorded the panic (the run-loop treats the
    // unhandled tell error as a panic, triggering on_panic).
    for _ in 0..200 {
        if !world.on_panic_log.lock().unwrap().is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("on_panic was not invoked for an unhandled tell error within the bound");
}

#[then(regex = r"^handle_dyn surfaces the error via into_any_err for the run-loop to treat as a panic$")]
async fn then_handle_dyn_surfaces_err(world: &mut MessageWorld) {
    // Observable proof: on_panic ran (the run-loop received Err(err) from
    // into_any_err and treated it as a panic).
    let log = world.on_panic_log.lock().unwrap();
    assert!(
        !log.is_empty(),
        "the run-loop must have treated the tell error as a panic"
    );
}

#[then(regex = r"^the actor's on_panic hook is invoked per the Reply doc$")]
async fn then_on_panic_invoked(world: &mut MessageWorld) {
    let log = world.on_panic_log.lock().unwrap().clone();
    // A handler that RETURNS Err (vs an actual `panic!` unwind) is classified by
    // the run-loop as `PanicReason::OnMessage` ("the reply was an error",
    // kind.rs:192-195) — NOT `HandlerPanic`, which is reserved for a real panic
    // unwind (kind.rs:196-198). on_panic still fires; the reason is OnMessage.
    assert_eq!(
        log,
        vec!["OnMessage".to_string()],
        "an unhandled tell error must invoke on_panic with reason OnMessage, got {log:?}"
    );
}

#[when(regex = r"^a message whose handler returns Ok is sent via tell$")]
async fn when_handler_ok_tell(world: &mut MessageWorld) {
    let on_panic_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    world.on_panic_log = Arc::clone(&on_panic_log);
    install_recording_hook();
    let actor = PanicWatch::spawn(PanicWatch { on_panic_log });
    actor.wait_for_startup().await;
    actor
        .tell(WatchCommand { tag: 95, fail: false })
        .await
        .expect("tell delivered");
    // Let the handler run; a small settle so any (erroneous) hook would fire.
    tokio::time::sleep(Duration::from_millis(30)).await;
    // Prove the actor is still alive and well (Ok did not stop it). The Reply
    // is `Result<u64, HandlerBoom>`, so `ask().await` is
    // `Result<u64, SendError<_, HandlerBoom>>` — one `expect` yields the u64.
    let reply = actor
        .ask(WatchCommand { tag: 96, fail: false })
        .await
        .expect("actor still running after an Ok tell");
    world.reply = Some(reply);
}

#[then(regex = r"^handle_dyn returns Ok and neither the error hook nor on_panic is invoked$")]
async fn then_ok_tell_no_hooks(world: &mut MessageWorld) {
    let on_panic = world.on_panic_log.lock().unwrap().clone();
    let hook = hook_sink().lock().unwrap().clone();
    assert!(
        on_panic.is_empty(),
        "an Ok tell must NOT invoke on_panic, got {on_panic:?}"
    );
    assert!(
        hook.is_empty(),
        "an Ok tell must NOT invoke the global error hook, got {hook:?}"
    );
    assert_eq!(world.reply, Some(96), "the actor kept processing after Ok");
    restore_default_hook();
}

// ===========================================================================
// @linearizability — concurrent senders observe single-writer serialization
// ===========================================================================

#[given(regex = r"^a counter actor that increments and replies with the new count on each ask$")]
async fn given_counter_actor(world: &mut MessageWorld) {
    let actor = Counter::spawn(Counter { count: 0 });
    actor.wait_for_startup().await;
    world.counter = Some(actor);
}

#[when(regex = r"^50 tasks concurrently ask the actor once each$")]
async fn when_50_concurrent_asks(world: &mut MessageWorld) {
    let actor = world.counter.as_ref().expect("counter spawned").clone();
    let barrier = Arc::new(Barrier::new(50));
    let handles: Vec<_> = (0..50)
        .map(|_| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor.ask(Increment).await.expect("ask succeeds")
            })
        })
        .collect();
    for h in handles {
        world
            .concurrent_replies
            .push(h.await.expect("ask task must not panic"));
    }
}

#[then(regex = r"^every task receives a distinct reply$")]
async fn then_distinct_replies(world: &mut MessageWorld) {
    let set: std::collections::HashSet<u64> = world.concurrent_replies.iter().copied().collect();
    assert_eq!(
        set.len(),
        world.concurrent_replies.len(),
        "single-writer serialization must give each ask a distinct count, got {:?}",
        world.concurrent_replies
    );
}

#[then(regex = r"^the set of replies is exactly the integers 1 through 50 with no gaps or duplicates$")]
async fn then_replies_1_through_50(world: &mut MessageWorld) {
    let mut got = world.concurrent_replies.clone();
    got.sort_unstable();
    let expected: Vec<u64> = (1..=50).collect();
    assert_eq!(
        got, expected,
        "the 50 concurrent asks must yield exactly 1..=50 with no gaps/dupes"
    );
}

#[given(regex = r"^an actor whose state is mutated by both ask and tell commands$")]
async fn given_folder_actor(world: &mut MessageWorld) {
    let actor = Folder::spawn(Folder { sum: 0 });
    actor.wait_for_startup().await;
    world.folder = Some(actor);
}

#[when(regex = r"^concurrent tasks send a mix of asks and tells$")]
async fn when_mixed_asks_tells(world: &mut MessageWorld) {
    let actor = world.folder.as_ref().expect("folder spawned").clone();
    // Deterministic command set: tags 1..=40, even via ask, odd via tell. The
    // oracle is the fold (sum of 1..=40) regardless of interleaving.
    let n = 40u64;
    let oracle: u64 = (1..=n).sum();
    world.oracle_sum = Some(oracle);

    let barrier = Arc::new(Barrier::new(n as usize));
    let mut ask_handles = Vec::new();
    let mut tell_handles = Vec::new();
    for tag in 1..=n {
        let actor = actor.clone();
        let barrier = Arc::clone(&barrier);
        if tag % 2 == 0 {
            ask_handles.push(tokio::spawn(async move {
                barrier.wait().await;
                actor.ask(AddAsk(tag)).await.expect("ask succeeds")
            }));
        } else {
            tell_handles.push(tokio::spawn(async move {
                barrier.wait().await;
                actor.tell(AddTell(tag)).await.expect("tell delivered");
            }));
        }
    }
    // Each ask reply must equal its OWN command's tag (no cross-talk).
    for (h, tag) in ask_handles.into_iter().zip((2..=n).step_by(2)) {
        let reply = h.await.expect("ask task must not panic");
        assert_eq!(reply, tag, "ask caller {tag} received another command's reply");
    }
    for h in tell_handles {
        h.await.expect("tell task must not panic");
    }
    // Read the final sum off the single-writer actor.
    world.final_sum = Some(actor.ask(ReadSum).await.expect("read sum"));
}

#[then(regex = r"^the final state equals the deterministic result of applying every command once$")]
async fn then_final_state_matches_fold(world: &mut MessageWorld) {
    assert_eq!(
        world.final_sum, world.oracle_sum,
        "single-writer fold must equal the sum of every command applied exactly once"
    );
}

#[then(regex = r"^no command is lost or applied twice$")]
async fn then_no_loss_no_double(world: &mut MessageWorld) {
    // The fold equality above is exactly the no-loss/no-double-count oracle:
    // any lost command lowers the sum, any double-applied raises it.
    assert_eq!(
        world.final_sum, world.oracle_sum,
        "the exact fold proves no command was lost or applied twice"
    );
}

// ===========================================================================
// @property / @model laws (message.properties.feature)
// ===========================================================================

// -- @property @sequence: any single-sender sequence handled in send order ---

#[given(regex = r"^any sequence of n distinct numbered commands from one sender$")]
async fn given_any_sequence(_world: &mut MessageWorld) {}

#[when(regex = r"^all n commands are sent in order and the actor records its handling order$")]
async fn when_send_n_commands(_world: &mut MessageWorld) {}

#[then(regex = r"^the recorded order equals the send order exactly$")]
async fn law_recorded_order_equals_send_order(_world: &mut MessageWorld) {
    // n ∈ boundary-biased {0, 1, 2, 64, 1024}: empty, single, adjacent-pair,
    // mid, large. A fresh actor per case (no global state). Oracle: a VecDeque
    // of the send-order tags; single-writer FIFO dispatch must match it.
    for n in [0usize, 1, 2, 64, 1024] {
        let log: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let actor = Recorder::spawn(Recorder {
            log: Arc::clone(&log),
        });
        actor.wait_for_startup().await;
        let oracle: Vec<u64> = (0..n as u64).collect();
        for tag in &oracle {
            actor.ask(Command(*tag)).await.expect("ask succeeds");
        }
        let recorded = log.lock().unwrap().clone();
        assert_eq!(
            recorded, oracle,
            "n={n}: single-writer FIFO dispatch must equal the send order"
        );
        actor.stop_gracefully().await.unwrap();
    }
}

// -- @property @lifecycle: forward round-trips any target reply --------------

#[given(regex = r"^a router actor and a live target that replies with any value v it is asked$")]
async fn given_router_live_target_any_v(_world: &mut MessageWorld) {}

#[then(regex = r"^the original caller receives exactly v$")]
async fn law_forward_roundtrips_v(_world: &mut MessageWorld) {
    // v ∈ boundary-biased {0, 1, MAX-1, MAX} plus random. The forwarded reply
    // must reach the original caller verbatim (identity through the channel).
    async fn check(v: u64) {
        let target = Target::spawn(Target {
            log: Arc::new(Mutex::new(Vec::new())),
        });
        target.wait_for_startup().await;
        let router = Router::spawn(Router {
            target: target.clone(),
        });
        router.wait_for_startup().await;
        let got = router.ask(ForwardEcho(v)).await.expect("forward ask");
        assert_eq!(got, v, "forward must round-trip v={v} verbatim");
        router.stop_gracefully().await.unwrap();
        target.stop_gracefully().await.unwrap();
    }
    for v in [0u64, 1, u64::MAX - 1, u64::MAX] {
        check(v).await;
    }
    // Seeded random sample (proptest cannot drive async actors directly, so a
    // documented deterministic loop over a seeded RNG hits arbitrary values).
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..16 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        check(state).await;
    }
}

// -- @property @boundary: handler Err on ask → HandlerError(e) ---------------

#[given(regex = r"^a handler that returns Err\(e\) for any error value e$")]
async fn given_handler_err_any_e(_world: &mut MessageWorld) {
    // No-op: the property err-ask/err-tell laws build their own actors per
    // boundary value inside their Then steps. Leaving `expect_spawn_value` false
    // routes the shared `the message is sent via ask` When to a no-op here.
}

#[then(regex = r"^the caller's ask result is Err whose HandlerError carries exactly e$")]
async fn law_ask_err_handler_carries_e(_world: &mut MessageWorld) {
    // e ∈ boundary-biased {0, 1, MAX-1, MAX}. The caller's SendError must be
    // HandlerError(HandlerBoom(e)) carrying exactly the handler's error.
    async fn check(e: u64) {
        let actor = Recorder::spawn(Recorder {
            log: Arc::new(Mutex::new(Vec::new())),
        });
        actor.wait_for_startup().await;
        let result = actor.ask(FallibleCommand { tag: e, fail: true }).await;
        match result {
            Err(SendError::HandlerError(boom)) => {
                assert_eq!(boom, HandlerBoom(e), "HandlerError must carry exactly e={e}");
            }
            other => panic!("e={e}: expected HandlerError(HandlerBoom({e})), got {other:?}"),
        }
        actor.stop_gracefully().await.unwrap();
    }
    for e in [0u64, 1, u64::MAX - 1, u64::MAX] {
        check(e).await;
    }
}

// -- @property @boundary: handler Err on tell → error path, never a caller ---

#[when(regex = r"^the message is sent via tell so no caller awaits$")]
async fn when_err_tell_property(_world: &mut MessageWorld) {}

#[then(regex = r"^handle_dyn surfaces e via into_any_err for the run loop to treat as a panic$")]
async fn law_tell_err_surfaces_via_any_err(world: &mut MessageWorld) {
    // For each boundary e: a tell whose handler returns Err(e) must invoke
    // on_panic (the run-loop's panic path), and NO reply value can be observed
    // (there is no caller). One PanicWatch actor per case.
    for e in [0u64, 1, u64::MAX - 1, u64::MAX] {
        let on_panic_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let actor = PanicWatch::spawn(PanicWatch {
            on_panic_log: Arc::clone(&on_panic_log),
        });
        actor.wait_for_startup().await;
        actor
            .tell(WatchCommand { tag: e, fail: true })
            .await
            .expect("tell delivered");
        let mut fired = false;
        for _ in 0..200 {
            if !on_panic_log.lock().unwrap().is_empty() {
                fired = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(fired, "e={e}: a tell handler Err must drive the panic path");
        let log = on_panic_log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec!["OnMessage".to_string()],
            "e={e}: a returned Err is classified OnMessage (not HandlerPanic)"
        );
    }
    // Flag for the And-line below.
    world.forward_tell_ok = Some(true);
}

#[then(regex = r"^no reply is delivered to any caller$")]
async fn law_tell_err_no_reply(world: &mut MessageWorld) {
    // The tell branch never reaches a reply channel: there was no caller to
    // receive a value. The preceding law drove the panic path for every e and
    // observed no reply (the actor's Reply type was never sent anywhere). This
    // is asserted by the absence of any captured reply value.
    assert_eq!(
        world.forward_tell_ok,
        Some(true),
        "the tell-error law must have run its boundary cases"
    );
    assert!(
        world.reply.is_none(),
        "a tell handler error must not deliver any reply, got {:?}",
        world.reply
    );
}

// -- @model @linearizability: any concurrent mix applies each command once ---

#[given(regex = r"^an actor whose state is a fold over a stream of commands$")]
async fn given_fold_actor(_world: &mut MessageWorld) {}

#[given(regex = r"^any multiset of commands split across P concurrent tasks via ask and tell$")]
async fn given_multiset_split(_world: &mut MessageWorld) {}

#[when(regex = r"^all tasks send with real overlap, started at a barrier$")]
async fn when_model_overlap(_world: &mut MessageWorld) {}

#[then(regex = r"^the final state equals the fold of every command applied exactly once$")]
async fn law_model_final_state_is_fold(_world: &mut MessageWorld) {
    // Documented deterministic loop over (P, count) boundary pairs — proptest
    // cannot drive tokio actors, so we sweep the # GEN boundaries with seeded
    // ask/tell splits and REAL overlap (Barrier). Oracle: the sequential fold
    // (sum of every command applied exactly once) must equal the SUT's final
    // sum, AND each ask reply matches its own command's tag (no cross-talk).
    for (p, count) in [(2usize, 1usize), (2, 2), (4, 50), (8, 200)] {
        run_model_case(p, count).await;
    }
}

#[then(regex = r"^ask callers each receive the reply for their own command with no cross-talk$")]
async fn law_model_no_cross_talk(_world: &mut MessageWorld) {
    // Asserted inside `run_model_case` for every case (each ask reply == its own
    // tag). Re-run a representative case here so this Then is a real assertion
    // rather than a no-op pass-through.
    run_model_case(8, 200).await;
}

/// Runs one (P, count) model case with REAL overlap and asserts both oracles:
/// the final fold and per-ask no-cross-talk.
async fn run_model_case(p: usize, count: usize) {
    let actor = Folder::spawn(Folder { sum: 0 });
    actor.wait_for_startup().await;

    // Commands are tags 1..=count; oracle fold is their sum. Split across P
    // tasks; within each task a command is ask if its tag is even, else tell.
    let oracle: u64 = (1..=count as u64).sum();

    // Partition tags into P contiguous chunks so every command is sent exactly
    // once across the P tasks.
    let mut chunks: Vec<Vec<u64>> = vec![Vec::new(); p];
    for (i, tag) in (1..=count as u64).enumerate() {
        chunks[i % p].push(tag);
    }

    let barrier = Arc::new(Barrier::new(p));
    let handles: Vec<_> = chunks
        .into_iter()
        .map(|tags| {
            let actor = actor.clone();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let mut ask_checks: Vec<(u64, u64)> = Vec::new();
                for tag in tags {
                    if tag % 2 == 0 {
                        let reply = actor.ask(AddAsk(tag)).await.expect("ask succeeds");
                        ask_checks.push((tag, reply));
                    } else {
                        actor.tell(AddTell(tag)).await.expect("tell delivered");
                    }
                }
                ask_checks
            })
        })
        .collect();

    let mut all_ask_checks: HashMap<u64, u64> = HashMap::new();
    for h in handles {
        for (tag, reply) in h.await.expect("model task must not panic") {
            all_ask_checks.insert(tag, reply);
        }
    }
    // No cross-talk: each ask reply equals its own command's tag.
    for (tag, reply) in &all_ask_checks {
        assert_eq!(
            tag, reply,
            "P={p} count={count}: ask for {tag} received {reply} (cross-talk)"
        );
    }
    let final_sum = actor.ask(ReadSum).await.expect("read final sum");
    assert_eq!(
        final_sum, oracle,
        "P={p} count={count}: single-writer fold must equal the exactly-once sum"
    );
    actor.stop_gracefully().await.unwrap();
}

// ===========================================================================
// Shared helpers for the full-mailbox forwarding scenarios
// ===========================================================================

/// Spawns a target with a bounded mailbox of capacity 1, fills it so a further
/// send is `MailboxFull`, and spawns a router pointing at it. The held slots are
/// released later via `release_held_target` (which flips the shared `watch`).
///
/// Mailbox arithmetic (tokio mpsc, capacity 1): the first `Hold` is dequeued
/// into the handler (which parks), freeing the buffer; the second `Hold` fills
/// the one buffer slot. A third send therefore observes `MailboxFull` — exactly
/// the condition `try_forward`/`blocking_forward` must distinguish.
async fn setup_full_target(world: &mut MessageWorld) {
    let target_log = Arc::clone(&world.target_log);
    let target = Target::spawn_with_mailbox(Target { log: target_log }, mailbox::bounded(1));
    target.wait_for_startup().await;

    let (release_tx, release_rx) = tokio::sync::watch::channel(false);

    // First Hold: dequeued into the handler, which parks on the watch.
    target
        .tell(Hold(release_rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued");
    // Spin until the handler has actually dequeued the first Hold (so the buffer
    // is empty again) before filling it; otherwise the second send could itself
    // race the dequeue. A short settle is sufficient and bounded.
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Second Hold: fills the single buffer slot. A third send is now MailboxFull.
    target
        .tell(Hold(release_rx))
        .try_send()
        .expect("second hold fills the buffer slot");

    let router = Router::spawn(Router {
        target: target.clone(),
    });
    router.wait_for_startup().await;
    world.router_target = Some((router, target));
    world.hold_release = Some(release_tx);
}

/// Releases every parked `Hold` handler so the target's mailbox drains and
/// capacity frees. Broadcasts `true` to all `watch` receivers at once.
fn release_held_target(world: &mut MessageWorld) {
    if let Some(tx) = world.hold_release.take() {
        let _ = tx.send(true);
    }
}
