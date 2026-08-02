//! Card #267 — drain-window mint equivalence for the supervision verbs
//! (`supervise` / `stop_child` / `unsupervise`), follow-up to #266's
//! watch/link equivalence oracle.
//!
//! The invariant (ADR-0010): a handler-context supervision verb behaves
//! identically whether the handler's `ActorRef` is the steady-state shared
//! upgrade or a drain-window mint. This differs from #266's watch/link
//! surface in WHERE the op lands: watch/link ops target the OTHER actor's
//! control lane (that actor's loop is running and applies them mid-script),
//! while supervision ops target the issuing supervisor's OWN control lane —
//! and the supervisor's loop is inside the handler while the script runs, so
//! a queued op is applied only after that handler returns. Every choreography
//! below is built around that fact.
//!
//! Two verified findings shape what is — and is not — tested here:
//!
//! 1. **Control-first merge beats lane closure** (`mailbox.rs::recv`
//!    try-polls the control lane before latching user-lane disconnect): in
//!    the drain window an op the handler queued is ALWAYS served and applied
//!    by the live loop arm (`apply_supervision_op`) before the
//!    `Closed → Collected` break. A supervision op from the mint therefore
//!    CANNOT race the `Collected` break; the only reachable mint-path race is
//!    the stop flag (Test D), which bypasses the last poll and lands the
//!    queued `Add` in the graceful epilogue's `drain_queued_supervision` —
//!    the mint-path re-assertion of the #248 never-orphaned invariant.
//! 2. **The `SendError`-handback disarm (#248) is UNREACHABLE from a mint**:
//!    the mint's own sender keeps both lanes open while the handler holds it,
//!    so `send_control` from a live handler to its own lane cannot observe
//!    `ControlClosed`. No test can construct the case, so this note — not a
//!    test — closes that card bullet ("handback disarm unaffected by ref
//!    shape").
//!
//! Falsifiability caveat: the 60 s `stop_grace` tripwire (a skipped
//! watch-edge install would stall the teardown sweep past `TERMINATE`,
//! failing the test by bounded-await instead of passing slowly — the #266
//! `on_stop_grace` pattern) holds on the REAL-TIME legs (Tests B–D), where
//! `terminate_bound()` is 15 s. Test A runs a paused clock, where a stall
//! auto-advances the grace instead: a missing edge then aborts the child,
//! and the `Stopped { Normal }` sweep assertion discriminates (it joins
//! `Killed`). Under miri the real-time bound is 30 min, so a missing edge
//! there would pass slowly instead of failing — a non-issue while the miri
//! leg excludes these integration tests, but stated here so the tripwire is
//! not misread as engine-independent.
//!
//! Oracle discipline (copied from `drain_equivalence.rs`, #266): ONE
//! mode-blind runner per scenario; `Mode` influences ONLY (a) whether an
//! external strong supervisor ref is held across the run and (b)
//! enqueue-before-run vs enqueue-after-run. Full-trace `assert_eq!` against a
//! `vec![..]` literal, then steady vs drain. Child fates are asserted via
//! the incarnation slot's exact `RunResult`s plus the started-count, not via
//! the trace (`on_stop` is skipped on kill, so an event log under-reports).
//! Every terminal await is `bounded()` under `TERMINATE` (#148); oneshot
//! gates only — no sleeps.

use core::convert::Infallible;
use std::{
    future::IntoFuture,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use bombay::{
    ActorId,
    actor::{Actor, ActorRef, Flow, Normal, PreparedActor, RunResult, SpawnConfig, WeakActorRef},
    capability,
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    reply::ReplySender,
    restart::{RestartConfig, RestartPolicy},
    test_support::terminate_bound,
};

/// The suite-wide fail-fast bound (#148 discipline): any terminal await past
/// this is a hung loop and fails the test rather than stalling the suite.
const TERMINATE: Duration = terminate_bound();

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid test capacity")
}

fn config(n: usize) -> SpawnConfig {
    SpawnConfig {
        capacity: cap(n),
        ..Default::default()
    }
}

/// Bounds every lifecycle await, exactly as `drain_equivalence.rs` does.
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    tokio::time::timeout(TERMINATE, fut)
        .await
        .expect("actor lifecycle op must terminate, not hang")
}

/// Canonical form of [`ActorStopReason`] (which deliberately has no
/// `PartialEq`): mirrors EVERY variant via an exhaustive match, so a new
/// variant breaks compilation here instead of silently comparing wrong.
#[derive(Debug, PartialEq, Eq, Clone)]
enum ReasonKind {
    Normal,
    SupervisorRestart,
    Collected,
    Killed,
    AlreadyDead,
    Panicked,
    LinkDied(Box<Self>),
    RestartLimitExceeded,
    ChildLifecycleFailed,
}

impl ReasonKind {
    fn of(reason: &ActorStopReason) -> Self {
        match reason {
            ActorStopReason::Normal => Self::Normal,
            ActorStopReason::SupervisorRestart => Self::SupervisorRestart,
            ActorStopReason::Collected => Self::Collected,
            ActorStopReason::Killed => Self::Killed,
            ActorStopReason::AlreadyDead => Self::AlreadyDead,
            ActorStopReason::Panicked(_) => Self::Panicked,
            ActorStopReason::LinkDied { reason: inner, .. } => {
                Self::LinkDied(Box::new(Self::of(inner)))
            }
            ActorStopReason::RestartLimitExceeded { .. } => Self::RestartLimitExceeded,
            ActorStopReason::ChildLifecycleFailed { .. } => Self::ChildLifecycleFailed,
        }
    }
}

/// One recorded step of the verb script (or of the supervisor's `on_stop`).
/// Each verb's outcome collapses to its `is_ok()` — the payload of a failure
/// (`TellError<()>`) carries no information beyond the fact of failure.
#[derive(Debug, PartialEq, Eq, Clone)]
enum TraceEvent {
    SuperviseOk(bool),
    StopChildOk(bool),
    UnsuperviseOk(bool),
    Finished(ReasonKind),
}

/// The mode knob. Anti-gaming rule: `mode` may influence ONLY (a) whether an
/// external strong supervisor ref is held across the run and (b) whether the
/// script messages are enqueued before the loop starts or after — no other
/// branch anywhere.
#[derive(Debug, Clone, Copy)]
enum Mode {
    Steady,
    Drain,
}

// ---------------------------------------------------------------- fixtures ---

/// The constant the `Child` liveness probe answers.
const PING: u32 = 42;

/// The supervised child: answers `Ping` with [`PING`] (the liveness probe)
/// and signals each incarnation's `on_start` on an unbounded channel the
/// test drains — deterministic start notification, no polling. No `Watch`
/// impl needed: being watched is universal.
struct Child;

#[derive(Debug)]
enum ChildMsg {
    Ping { reply: ReplySender<u32, Infallible> },
}
impl Msg for ChildMsg {}
impl Mailboxed for Child {
    type Msg = ChildMsg;
}
impl Actor for Child {
    type Args = mpsc::UnboundedSender<()>;
    type Error = Infallible;
    async fn on_start(started: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let _ = started.send(());
        Ok(Self)
    }
    async fn handle(&mut self, msg: ChildMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        match msg {
            ChildMsg::Ping { reply } => {
                let _ = reply.send(PING);
            }
        }
        Ok(Flow::Continue)
    }
}

/// Every incarnation the factory has built: its liveness-anchor ref (the
/// supervisor never pins a child, ADR-0003 — the stored ref is what keeps an
/// incarnation alive until the test says otherwise) plus its join handle for
/// exact per-incarnation `RunResult` assertions.
type Incarnations = Arc<Mutex<Vec<(ActorRef<Child>, JoinHandle<RunResult<Child>>)>>>;

/// The one-shot child factory handed to `supervise`: builds each incarnation
/// via `PreparedActor` (so the test gets the ref AND the join handle), pushes
/// both into the slot, and returns the ref. Captures ONLY the slot and the
/// start-notification sender — never any supervisor ref (kameo #171).
type ChildFactory = Box<dyn FnMut() -> ActorRef<Child> + Send>;

fn child_factory(slot: Incarnations, started: mpsc::UnboundedSender<()>) -> ChildFactory {
    Box::new(move || {
        let prepared = PreparedActor::<Child>::new(config(4));
        let child_ref = prepared.actor_ref().clone();
        let join = prepared.spawn(started.clone());
        slot.lock()
            .expect("slot lock")
            .push((child_ref.clone(), join));
        child_ref
    })
}

/// The child restart tuning: `Permanent` (a `Killed` incarnation rebuilds),
/// `min_backoff` zero (Test A's determinism — see its doc comment), and a
/// 60 s (1 min) `stop_grace` tripwire far past `TERMINATE` (a skipped watch-edge
/// install would stall the teardown sweep into a bounded-await failure
/// instead of passing silently).
const fn child_config() -> RestartConfig {
    RestartConfig::new(RestartPolicy::Permanent)
        .with_min_backoff(Duration::ZERO)
        .with_stop_grace(Duration::from_mins(1))
}

/// The oracle supervisor: one message variant per script step, each handler
/// performing its verbs against its handler-context `ActorRef` and recording
/// outcomes into the shared trace. Default strategy, default `Watch` hooks;
/// `on_stop` pushes `Finished(ReasonKind)`.
struct SupScript {
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    factory: Option<ChildFactory>,
    child_id: Option<ActorId>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

struct SupScriptArgs {
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    factory: ChildFactory,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

/// One variant per script step. `SuperviseAndStop` is Test D's single
/// message: supervise, then set the stop flag in the same handler invocation.
#[derive(Debug, Clone, Copy)]
enum SupMsg {
    Supervise,
    StopChild,
    Unsupervise,
    Park,
    SuperviseAndStop,
}
impl Msg for SupMsg {}

/// Stage-3 authority: the OTP watching policy + the `OneForOne` strategy as
/// plugged capabilities (was: empty `Watch`/`Supervisor` marker impls with the same
/// semantics — the default hook and the default strategy, now NAMED).
#[derive(bombay_macros::Provide)]
struct SupScriptCaps {
    watching: capability::Watching<capability::OtpPropagation>,
    supervising: capability::Supervising<capability::OneForOne>,
}
impl capability::CapSet<SupScript> for SupScriptCaps {
    fn build(_: &SupScriptArgs) -> Self {
        Self {
            watching: capability::Watching::new(),
            supervising: capability::Supervising::new(),
        }
    }
}

impl SupScript {
    fn record(&self, event: TraceEvent) {
        self.trace.lock().expect("trace lock").push(event);
    }

    /// The shared `Supervise` / `SuperviseAndStop` step: take the one-shot
    /// factory, `supervise` it against the handler-context ref (the steady
    /// shared upgrade or the drain-window mint — the thing under test), and
    /// record the outcome plus the first incarnation's id.
    async fn supervise_step(&mut self, actor_ref: &capability::Handle<Self>) {
        let factory = self.factory.take().expect("one supervise per script");
        let result = bounded(actor_ref.supervise(child_config(), factory)).await;
        let ok = result.is_ok();
        self.child_id = result.ok();
        self.record(TraceEvent::SuperviseOk(ok));
    }
}

impl capability::Actor for SupScript {
    type Msg = SupMsg;
    type Args = SupScriptArgs;
    type Error = Infallible;
    type Caps = SupScriptCaps;
    async fn init(args: Self::Args, _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        let SupScriptArgs {
            trace,
            factory,
            entered,
            release,
        } = args;
        Ok(Self {
            trace,
            factory: Some(factory),
            child_id: None,
            entered: Some(entered),
            release: Some(release),
        })
    }
    async fn handle(
        &mut self,
        msg: SupMsg,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            SupMsg::Supervise => self.supervise_step(cx.self_ref()).await,
            SupMsg::StopChild => {
                let id = self.child_id.expect("supervise ran first");
                let result = bounded(cx.self_ref().stop_child(id)).await;
                self.record(TraceEvent::StopChildOk(result.is_ok()));
            }
            SupMsg::Unsupervise => {
                let id = self.child_id.expect("supervise ran first");
                let result = bounded(cx.self_ref().unsupervise(id)).await;
                self.record(TraceEvent::UnsuperviseOk(result.is_ok()));
            }
            SupMsg::Park => {
                self.entered
                    .take()
                    .expect("entered once")
                    .send(())
                    .expect("the test is listening");
                // Park until the test has sequenced the child's fate, so the
                // loop resumes with that fate already queued.
                bounded(self.release.take().expect("release once"))
                    .await
                    .expect("the release channel is open");
            }
            SupMsg::SuperviseAndStop => {
                self.supervise_step(cx.self_ref()).await;
                return Ok(Flow::Stop(Normal));
            }
        }
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<capability::Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.record(TraceEvent::Finished(ReasonKind::of(&reason)));
        Ok(())
    }
}

// ------------------------------------------------------- the oracle harness ---

/// Starts the supervisor with `mode` deciding ONLY the two allowed things:
/// the held external ref (a) and enqueue-before-run vs after (b).
async fn start_supervisor(
    mode: Mode,
    trace: &Arc<Mutex<Vec<TraceEvent>>>,
    factory: ChildFactory,
    script: &[SupMsg],
) -> (
    Option<capability::Handle<SupScript>>,
    JoinHandle<RunResult<capability::Shell<SupScript>>>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (prepared, link_rx) = PreparedActor::<capability::Shell<SupScript>>::new_linked(config(8));
    let external = match mode {
        Mode::Steady => Some(prepared.actor_ref().clone()),
        Mode::Drain => None,
    };
    if matches!(mode, Mode::Drain) {
        for &msg in script {
            bounded(prepared.actor_ref().tell(msg))
                .await
                .expect("enqueue before run");
        }
    }
    let args = SupScriptArgs {
        trace: Arc::clone(trace),
        factory,
        entered: entered_tx,
        release: release_rx,
    };
    let sup_join = prepared.spawn_supervised_task(args, link_rx);
    if matches!(mode, Mode::Steady) {
        let held = external.as_ref().expect("steady holds a ref");
        for &msg in script {
            bounded(held.tell(msg)).await.expect("enqueue after run");
        }
    }
    (external, sup_join, entered, release)
}

/// One run of a scenario: the incarnation slot, the start-notification
/// channel, the recorded trace, and the supervisor handle set.
struct Rig {
    slot: Incarnations,
    started_rx: mpsc::UnboundedReceiver<()>,
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    external: Option<capability::Handle<SupScript>>,
    sup_join: JoinHandle<RunResult<capability::Shell<SupScript>>>,
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

async fn start_rig(mode: Mode, script: &[SupMsg]) -> Rig {
    let slot: Incarnations = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = mpsc::unbounded_channel::<()>();
    let trace: Arc<Mutex<Vec<TraceEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let factory = child_factory(Arc::clone(&slot), started_tx);
    let (external, sup_join, entered, release) =
        start_supervisor(mode, &trace, factory, script).await;
    Rig {
        slot,
        started_rx,
        trace,
        external,
        sup_join,
        entered,
        release,
    }
}

/// Pops the oldest incarnation out of the slot (its ref and join handle).
fn take_incarnation(slot: &Incarnations) -> (ActorRef<Child>, JoinHandle<RunResult<Child>>) {
    slot.lock().expect("slot lock").remove(0)
}

/// Asserts a child's exact graceful-cancel outcome, `Stopped { reason:
/// Normal }` — the sweep / `stop_child` signature, distinct from a
/// guard-aborted incarnation (which would join `Killed`).
fn assert_stopped_normal(outcome: &RunResult<Child>, context: &str) {
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "{context}, got {outcome:?}"
    );
}

/// Asserts the supervisor's exact ref-count-stop outcome: both modes end
/// `Collected` (the drain run on the emptied backlog, the steady run once
/// the test drops the held ref).
fn assert_collected(outcome: &RunResult<capability::Shell<SupScript>>) {
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "both modes end Collected, got {outcome:?}"
    );
}

fn take_trace(trace: &Arc<Mutex<Vec<TraceEvent>>>) -> Vec<TraceEvent> {
    std::mem::take(&mut *trace.lock().expect("trace lock"))
}

// ----------------------------------------------------------- the scenarios ---

/// Test A choreography (mode-blind): script `[Supervise, Park]`, kill
/// incarnation 1 while the supervisor is parked, and watch the drain-minted
/// supervise's restart edge rebuild it.
///
/// Determinism: the default 20% jitter scales the BASE delay
/// (`jittered_backoff`), and 20% of `Duration::ZERO` is zero — so
/// `min_backoff = 0` alone arms the retry with an AT-NOW deadline. Whether
/// that deadline polls ready at the immediately following select iteration
/// is clock-dependent (probe-verified, card session 2026-07-31): under a
/// REAL clock the timer wheel's deadline lapses only ~1 ms later, so the
/// drain-mode `Closed → Collected` break — ready at once, no yield between
/// the arming death arm and the next poll — would ALWAYS win and the rebuild
/// would never happen; under a PAUSED clock the virtual instant cannot
/// advance between arm and poll, so the at-now deadline is already expired
/// and the biased retries arm (ordered ahead of the mailbox arm) rebuilds
/// BEFORE the loop can take its break. The test therefore runs under
/// `start_paused` — the `control_lane.rs` / `dst_races.rs` discipline for
/// timer-order determinism — and all of its gates are task-driven (no
/// quiescence point could auto-advance the clock into a `bounded` timeout).
/// After the rebuild the drain loop collects on its emptied backlog and the
/// epilogue sweeps incarnation 2.
async fn run_supervise_restart(mode: Mode) -> Vec<TraceEvent> {
    let mut rig = start_rig(mode, &[SupMsg::Supervise, SupMsg::Park]).await;

    // `Park` entered ⇒ the queued `Add` was applied between the two handler
    // invocations (control-first merge): incarnation 1 exists with its table
    // entry and watch edge installed.
    bounded(rig.entered).await.expect("the supervisor parked");
    bounded(rig.started_rx.recv())
        .await
        .expect("incarnation 1 started");
    let (first_ref, first_join) = take_incarnation(&rig.slot);
    first_ref.kill();
    let first_outcome = bounded(first_join).await.expect("join incarnation 1");
    assert!(
        matches!(first_outcome, RunResult::Killed),
        "a killed incarnation reports Killed, got {first_outcome:?}"
    );
    drop(first_ref);

    rig.release.send(()).expect("the supervisor is parked");

    // The rebuild landed: incarnation 2 started.
    bounded(rig.started_rx.recv())
        .await
        .expect("incarnation 2 started");

    // Knob (a): dropping the held ref is what lets the STEADY loop see
    // `Closed`; the drain run held `None` and collects on its emptied
    // backlog. Mode-blind code — `drop(Option)` covers both.
    drop(rig.external);
    let sup_outcome = bounded(rig.sup_join).await.expect("join supervisor");
    assert_collected(&sup_outcome);
    drop(sup_outcome);

    // Incarnation 2 was swept by the supervisor-exit teardown — a graceful
    // cancel, proving the drain-minted supervise installed a live table
    // entry AND watch edge (a missing edge would stall the sweep past
    // `TERMINATE`; a missing entry would leave the child running).
    let (_second_ref, second_join) = take_incarnation(&rig.slot);
    let second_outcome = bounded(second_join).await.expect("join incarnation 2");
    assert_stopped_normal(&second_outcome, "incarnation 2 swept by teardown");
    assert!(
        rig.started_rx.try_recv().is_err(),
        "exactly two incarnations started"
    );

    take_trace(&rig.trace)
}

/// Test B choreography (mode-blind): script `[Supervise, StopChild, Park]`.
/// `Add` is applied between handlers 1 and 2, `Stop` between handlers 2 and
/// 3 (control-first merge), so by the time `Park` parks, the child is
/// cancelled and its deferred abort armed with the 60 s grace.
///
/// Choreography note: after `Stop` removes the table entry, the child's
/// `Normal` death notice still lands on the supervisor's link channel and
/// routes to the PEER `on_link_died` hook — harmless with the default hook
/// (a non-linked normal death → `Continue`), and in drain mode a notice
/// landing after the break decision is dropped by design (#266). Either way
/// the trace is unaffected.
async fn run_stop_child(mode: Mode) -> Vec<TraceEvent> {
    let mut rig = start_rig(mode, &[SupMsg::Supervise, SupMsg::StopChild, SupMsg::Park]).await;

    bounded(rig.entered).await.expect("the supervisor parked");
    bounded(rig.started_rx.recv())
        .await
        .expect("incarnation 1 started");
    let (child_ref, child_join) = take_incarnation(&rig.slot);
    // The child finished stopping BEFORE the supervisor may exit, so the
    // epilogue's `PendingAbort` drop-abort is a proven no-op, not a race.
    let child_outcome = bounded(child_join).await.expect("join child");
    assert_stopped_normal(&child_outcome, "stop_child cancels gracefully");
    drop(child_ref);

    rig.release.send(()).expect("the supervisor is parked");
    drop(rig.external);
    let sup_outcome = bounded(rig.sup_join).await.expect("join supervisor");
    assert_collected(&sup_outcome);
    drop(sup_outcome);

    assert!(
        rig.started_rx.try_recv().is_err(),
        "a stopped child is never rebuilt"
    );
    take_trace(&rig.trace)
}

/// Test C choreography (mode-blind): script `[Supervise, Unsupervise, Park]`.
/// `Add` then `Remove` are applied between the handlers, so by the time
/// `Park` parks the child is DETACHED: it keeps running (the test's stored
/// ref anchors it), is never rebuilt, and survives the supervisor's death.
async fn run_unsupervise(mode: Mode) -> Vec<TraceEvent> {
    let mut rig = start_rig(
        mode,
        &[SupMsg::Supervise, SupMsg::Unsupervise, SupMsg::Park],
    )
    .await;

    bounded(rig.entered).await.expect("the supervisor parked");
    bounded(rig.started_rx.recv())
        .await
        .expect("incarnation 1 started");
    let (child_ref, child_join) = take_incarnation(&rig.slot);
    let ping_parked = bounded(child_ref.ask(|reply| ChildMsg::Ping { reply }))
        .await
        .expect("the detached child answers while the supervisor is parked");
    assert_eq!(ping_parked, PING, "the liveness probe round-trips");

    rig.release.send(()).expect("the supervisor is parked");
    drop(rig.external);
    let sup_outcome = bounded(rig.sup_join).await.expect("join supervisor");
    assert_collected(&sup_outcome);
    drop(sup_outcome);

    // The detached child survives the supervisor's death: the teardown sweep
    // only sweeps table children, and `Remove` dropped the entry.
    let ping_after = bounded(child_ref.ask(|reply| ChildMsg::Ping { reply }))
        .await
        .expect("the detached child survives the supervisor");
    assert_eq!(ping_after, PING, "the liveness probe still round-trips");

    child_ref.kill();
    let child_outcome = bounded(child_join).await.expect("join child");
    assert!(
        matches!(child_outcome, RunResult::Killed),
        "cleanup kills the detached child, got {child_outcome:?}"
    );
    assert!(
        rig.started_rx.try_recv().is_err(),
        "a detached child is never rebuilt"
    );
    take_trace(&rig.trace)
}

/// Test D choreography (mode-blind): script `[SuperviseAndStop]` — supervise
/// races the supervisor's own stop. The handler queues `Add` and sets the
/// stop flag; the loop breaks `Normal` WITHOUT another mailbox poll (the
/// flag bypasses the last `recv`), so the queued `Add` is applied by the
/// graceful epilogue's `drain_queued_supervision` (insert + watch edge, the
/// #248 mechanism 1) and `teardown_children` sweeps the just-installed
/// child.
///
/// This pin is EXHAUSTIVE for the mint-path race: finding 1 (the control
/// first merge beats lane closure) makes the `Collected`-break race
/// unreachable from the mint, so the stop-flag path is the only race a
/// drain-minted supervision op can ever hit.
async fn run_supervise_and_stop(mode: Mode) -> Vec<TraceEvent> {
    let mut rig = start_rig(mode, &[SupMsg::SuperviseAndStop]).await;

    // Installed-then-swept: the child joins `Stopped { Normal }`. A
    // dropped-armed registration would abort it (`Killed`); a skipped
    // watch-edge install would stall the sweep past `TERMINATE`. Either
    // regression fails this test — the child is never orphaned and never
    // guard-aborted.
    bounded(rig.started_rx.recv())
        .await
        .expect("incarnation 1 started");
    let (child_ref, child_join) = take_incarnation(&rig.slot);
    let child_outcome = bounded(child_join).await.expect("join child");
    assert_stopped_normal(&child_outcome, "installed-then-swept, never guard-aborted");
    drop(child_ref);

    drop(rig.external);
    let sup_outcome = bounded(rig.sup_join).await.expect("join supervisor");
    assert!(
        matches!(
            sup_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "the stop flag's reason is Normal, not Collected, got {sup_outcome:?}"
    );
    drop(sup_outcome);

    assert!(
        rig.started_rx.try_recv().is_err(),
        "exactly one incarnation started"
    );
    take_trace(&rig.trace)
}

// ---------------------------------------------------------------- the tests ---

/// #267, Test A (paired-run): a drain-window `supervise` installs a working
/// restart edge — a killed child is rebuilt, and the rebuild is swept by the
/// supervisor's exit teardown. Steady and drain runs must each reproduce the
/// exact expected trace — and therefore each other. Paused clock: the
/// zero-backoff retry deadline must poll ready at the select iteration
/// immediately after it is armed (see `run_supervise_restart`), which only a
/// frozen virtual instant guarantees.
#[tokio::test(start_paused = true)]
async fn supervise_install_and_restart_edge_equal_steady_vs_drain() {
    let steady = run_supervise_restart(Mode::Steady).await;
    let drain = run_supervise_restart(Mode::Drain).await;
    let expected = vec![
        TraceEvent::SuperviseOk(true),
        TraceEvent::Finished(ReasonKind::Collected),
    ];
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "handler-context supervise is steady/drain equivalent (ADR-0010)"
    );
}

/// #267, Test B (paired-run): a drain-window `stop_child` cancels the child
/// gracefully and drops its edge — the child is never rebuilt, and the
/// supervisor still collects identically.
#[tokio::test]
async fn stop_child_equal_steady_vs_drain() {
    let steady = run_stop_child(Mode::Steady).await;
    let drain = run_stop_child(Mode::Drain).await;
    let expected = vec![
        TraceEvent::SuperviseOk(true),
        TraceEvent::StopChildOk(true),
        TraceEvent::Finished(ReasonKind::Collected),
    ];
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "handler-context stop_child is steady/drain equivalent (ADR-0010)"
    );
}

/// #267, Test C (paired-run): a drain-window `unsupervise` detaches the
/// child — it keeps answering its liveness probe AFTER the supervisor's
/// death and is never rebuilt.
#[tokio::test]
async fn unsupervise_detach_equal_steady_vs_drain() {
    let steady = run_unsupervise(Mode::Steady).await;
    let drain = run_unsupervise(Mode::Drain).await;
    let expected = vec![
        TraceEvent::SuperviseOk(true),
        TraceEvent::UnsuperviseOk(true),
        TraceEvent::Finished(ReasonKind::Collected),
    ];
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "handler-context unsupervise is steady/drain equivalent (ADR-0010)"
    );
}

/// #267, Test D (paired-run): a drain-window `supervise` racing the
/// supervisor's own stop flag is applied by the graceful epilogue and swept
/// — the #248 never-orphaned invariant re-asserted for the mint path.
#[tokio::test]
async fn supervise_racing_own_stop_equal_steady_vs_drain() {
    let steady = run_supervise_and_stop(Mode::Steady).await;
    let drain = run_supervise_and_stop(Mode::Drain).await;
    let expected = vec![
        TraceEvent::SuperviseOk(true),
        TraceEvent::Finished(ReasonKind::Normal),
    ];
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "drain-minted supervise vs stop flag is steady/drain equivalent (ADR-0010)"
    );
}
