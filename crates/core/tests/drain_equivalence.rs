//! Card #266 — adversarial invariant tests for drain-window watch/link.
//!
//! Follow-up to #260 / PR #265. The #260 unit tests (`spawn.rs`) verify the fix
//! mechanics; this suite pins the INVARIANT (ADR-0010): **handler-context
//! `watch`/`link` behaves identically whether the handler's `ActorRef` is the
//! steady-state shared upgrade or a drain-window mint** — external ref-count
//! liveness is unobservable through the watch verbs.
//!
//! The centerpiece is a strict equivalence oracle: ONE parameterized
//! `run_script(mode, death)` drives the same verb script through the real
//! public API in both modes and records a full `Vec<TraceEvent>`. `mode`
//! influences ONLY (a) whether an external strong watcher ref is held across
//! the run and (b) enqueue-before-run vs enqueue-after-run — nothing else
//! branches on it. The expected trace is a full `vec![..]` literal, so the
//! tests cannot pass on two equally-wrong runs.
//!
//! `pipe_to_self` / `send_after` are EXCLUDED from the oracle: their
//! drain-window result-drop is the spec'd fate (weak-upgrade seam,
//! ADR-0010/0017) and is pinned by dedicated divergence tests instead.

use core::{convert::Infallible, ops::ControlFlow};
use std::{
    future::IntoFuture,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::oneshot, task::JoinHandle};

use bombay::{
    ActorId,
    actor::{Actor, ActorRef, Flow, PreparedActor, RunResult, SpawnConfig, WeakActorRef},
    capability,
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    reply::ReplySender,
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

/// Bounds every lifecycle await, exactly as `dst_races.rs` does.
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    tokio::time::timeout(TERMINATE, fut)
        .await
        .expect("actor lifecycle op must terminate, not hang")
}

// ------------------------------------------------------------ trace types ----

/// Roles stand in for raw `ActorId`s in the trace: ids differ between the
/// steady and drain runs, roles are comparable. A notice naming none of the
/// three known ids maps to `Unknown(())` — which fails trace equality, as it
/// should.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Role {
    SelfActor,
    Target,
    Peer,
    Unknown(()),
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

/// One recorded step of the verb script (or of the watcher's hooks).
/// `Watch`'s `Err(())` collapses `ActorNotLinked` (a unit-like error whose
/// payload carries no information).
#[derive(Debug, PartialEq, Eq, Clone)]
enum TraceEvent {
    IsAlive(bool),
    TellSelf(bool),
    TellPeer(bool),
    AskPeer(Result<u32, ()>),
    Watch(Role, Result<(), ()>),
    Unwatch(Role),
    Link(Role, Result<(), ()>),
    /// The self-tell's own delivery (the second handler invocation).
    SelfDelivered,
    Notice {
        who: Role,
        reason: ReasonKind,
        linked: bool,
    },
    Finished(ReasonKind),
}

/// The shared notice-recording sink used by the recording `Watch` hooks.
type NoticeLog = Arc<Mutex<Vec<(ActorId, ReasonKind, bool)>>>;

// ---------------------------------------------------------------- fixtures ---

/// The watch TARGET: a trivial plain-spawned actor (being watched is
/// universal). Its message type is never sent — the `SupIdle` precedent in
/// `dst_races.rs`.
struct Target;
#[derive(Debug)]
struct TargetMsg;
impl Msg for TargetMsg {}
impl Mailboxed for Target {
    type Msg = TargetMsg;
}
impl Actor for Target {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
    async fn handle(&mut self, _: TargetMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// The ask/tell/link PEER: answers `Query` with a constant, accepts `Tick`,
/// crashes on `Boom`, and records every death notice it observes (the
/// peer-side edge assertions read this log). Linked-spawned, so `link` can
/// install both edges.
struct EchoPeer {
    notices: NoticeLog,
}
#[derive(Debug)]
enum PeerMsg {
    Tick,
    Query { reply: ReplySender<u32, Infallible> },
    Boom,
}
impl Msg for PeerMsg {}
impl Mailboxed for EchoPeer {
    type Msg = PeerMsg;
}
/// The peer's recording reaction, a named `WatchPolicy` (stage 3): log the
/// notice, never propagate — byte-identical to the removed hook override.
struct PeerRecord;
impl capability::WatchPolicy<EchoPeer> for PeerRecord {
    async fn on_link_died(
        actor: &mut EchoPeer,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor
            .notices
            .lock()
            .expect("lock")
            .push((id, ReasonKind::of(&reason), linked));
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(bombay_macros::Provide)]
struct EchoPeerCaps {
    watching: capability::Watching<PeerRecord>,
}
impl capability::CapSet<EchoPeer> for EchoPeerCaps {
    fn build(_: &NoticeLog) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for EchoPeer {
    type Msg = PeerMsg;
    type Args = NoticeLog;
    type Error = Infallible;
    type Caps = EchoPeerCaps;
    async fn init(notices: Self::Args, _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { notices })
    }
    async fn handle(
        &mut self,
        msg: PeerMsg,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            PeerMsg::Tick => {}
            PeerMsg::Query { reply } => {
                let _ = reply.send(42);
            }
            PeerMsg::Boom => panic!("peer crash on command"),
        }
        Ok(Flow::Continue)
    }
}

/// The ids a `Scripted` watcher maps to roles; `self_id` is captured in
/// `on_start` (the only place the actor sees its own ref).
struct RoleIds {
    self_id: ActorId,
    target: ActorId,
    peer: ActorId,
}

impl RoleIds {
    fn role(&self, id: ActorId) -> Role {
        if id == self.self_id {
            Role::SelfActor
        } else if id == self.target {
            Role::Target
        } else if id == self.peer {
            Role::Peer
        } else {
            Role::Unknown(())
        }
    }
}

/// The oracle watcher: its `Run` handler executes the whole verb script
/// against its handler-context `ActorRef`, pushing every step into the shared
/// trace; its `Noop` handler (the self-tell's delivery) records `SelfDelivered`
/// once. Parks on the `release` gate so the test sequences the target's death
/// while the loop is inside the handler (the `DrainWatcher` pattern).
struct Scripted {
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    ids: RoleIds,
    target: Option<ActorRef<Target>>,
    peer: Option<capability::Handle<EchoPeer>>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
    done: Option<oneshot::Sender<()>>,
}

struct ScriptedArgs {
    trace: Arc<Mutex<Vec<TraceEvent>>>,
    target: ActorRef<Target>,
    peer: capability::Handle<EchoPeer>,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
}

#[derive(Debug)]
enum ScriptedMsg {
    Run,
    Noop,
}
impl Msg for ScriptedMsg {}
impl Mailboxed for Scripted {
    type Msg = ScriptedMsg;
}

impl Scripted {
    fn record(&self, event: TraceEvent) {
        self.trace.lock().expect("lock").push(event);
    }

    /// The whole oracle script, run against the handler's ref. Every verb's
    /// outcome is recorded in call order; the two `Role::Target` watch calls
    /// bracket an `unwatch` so the second registration is the one that fires.
    async fn execute_script(&mut self, actor_ref: &capability::Handle<Self>) {
        let target = self.target.take().expect("the script runs once");
        let peer = self.peer.take().expect("the script runs once");
        self.record(TraceEvent::IsAlive(actor_ref.is_alive()));
        let tell_self = bounded(actor_ref.tell(ScriptedMsg::Noop)).await.is_ok();
        self.record(TraceEvent::TellSelf(tell_self));
        let tell_peer = bounded(peer.tell(PeerMsg::Tick)).await.is_ok();
        self.record(TraceEvent::TellPeer(tell_peer));
        let asked = bounded(peer.ask(|reply| PeerMsg::Query { reply }))
            .await
            .map_err(|_| ());
        self.record(TraceEvent::AskPeer(asked));
        let first = bounded(actor_ref.watch(&target)).await.map_err(|_| ());
        self.record(TraceEvent::Watch(Role::Target, first));
        bounded(actor_ref.unwatch(&target)).await;
        self.record(TraceEvent::Unwatch(Role::Target));
        let second = bounded(actor_ref.watch(&target)).await.map_err(|_| ());
        self.record(TraceEvent::Watch(Role::Target, second));
        let linked = bounded(actor_ref.link(&peer)).await.map_err(|_| ());
        self.record(TraceEvent::Link(Role::Peer, linked));
        // Release both pins BEFORE signalling: the watcher must not keep the
        // target alive (the `DrainWatcher` discipline) — else `Collected`
        // never fires and the test hangs.
        drop(target);
        drop(peer);
        self.entered
            .take()
            .expect("entered once")
            .send(())
            .expect("the test is listening");
        // Park until the target has actually died, so the notice is queued on
        // this actor's link channel when the loop resumes.
        bounded(self.release.take().expect("release once"))
            .await
            .expect("the release channel is open");
    }
}

/// The scripted watcher's recording reaction (stage 3): role-map the id and
/// push the notice into the shared trace — byte-identical semantics to the
/// removed hook override.
struct ScriptedRecord;
impl capability::WatchPolicy<Scripted> for ScriptedRecord {
    async fn on_link_died(
        actor: &mut Scripted,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor.record(TraceEvent::Notice {
            who: actor.ids.role(id),
            reason: ReasonKind::of(&reason),
            linked,
        });
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(bombay_macros::Provide)]
struct ScriptedCaps {
    watching: capability::Watching<ScriptedRecord>,
}
impl capability::CapSet<Scripted> for ScriptedCaps {
    fn build(_: &ScriptedArgs) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for Scripted {
    type Msg = ScriptedMsg;
    type Args = ScriptedArgs;
    type Error = Infallible;
    type Caps = ScriptedCaps;
    async fn init(args: Self::Args, cx: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        let ScriptedArgs {
            trace,
            target,
            peer,
            entered,
            release,
            done,
        } = args;
        let ids = RoleIds {
            self_id: cx.self_ref().id(),
            target: target.id(),
            peer: peer.id(),
        };
        Ok(Self {
            trace,
            ids,
            target: Some(target),
            peer: Some(peer),
            entered: Some(entered),
            release: Some(release),
            done: Some(done),
        })
    }
    async fn handle(
        &mut self,
        msg: ScriptedMsg,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            ScriptedMsg::Run => self.execute_script(cx.self_ref()).await,
            ScriptedMsg::Noop => {
                self.record(TraceEvent::SelfDelivered);
                if let Some(done) = self.done.take() {
                    let _ = done.send(());
                }
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

/// The mode knob. Anti-gaming rule: `mode` may influence ONLY (a) whether an
/// external strong watcher ref is held across the run and (b) whether the
/// script message is enqueued before the loop starts or after — no other
/// branch anywhere.
#[derive(Debug, Clone, Copy)]
enum Mode {
    Steady,
    Drain,
}

/// How the test kills the target — part of the SCRIPT (shared by both modes),
/// never of the mode.
#[derive(Debug, Clone, Copy)]
enum Death {
    Drop,
    Kill,
}

impl Death {
    const fn reason(self) -> ReasonKind {
        match self {
            Self::Drop => ReasonKind::Collected,
            Self::Kill => ReasonKind::Killed,
        }
    }
}

fn spawn_target() -> (ActorRef<Target>, JoinHandle<RunResult<Target>>) {
    let prepared = PreparedActor::<Target>::new(config(4));
    let target_ref = prepared.actor_ref().clone();
    (target_ref, prepared.spawn(()))
}

fn spawn_peer() -> (
    capability::Handle<EchoPeer>,
    JoinHandle<RunResult<capability::Shell<EchoPeer>>>,
    NoticeLog,
) {
    let notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let (prepared, link_rx) = PreparedActor::<capability::Shell<EchoPeer>>::new_linked(config(4));
    let peer_ref = prepared.actor_ref().clone();
    let peer_join = prepared.spawn_linked_task(Arc::clone(&notices), link_rx);
    (peer_ref, peer_join, notices)
}

/// Starts the watcher with `mode` deciding ONLY the two allowed things: the
/// held external ref (a) and enqueue-before-run vs after (b).
async fn start_watcher(
    mode: Mode,
    trace: &Arc<Mutex<Vec<TraceEvent>>>,
    target_ref: &ActorRef<Target>,
    peer_ref: &capability::Handle<EchoPeer>,
) -> (
    Option<capability::Handle<Scripted>>,
    JoinHandle<RunResult<capability::Shell<Scripted>>>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
) {
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (done_tx, done) = oneshot::channel();
    let (prepared, link_rx) = PreparedActor::<capability::Shell<Scripted>>::new_linked(config(8));
    let external = match mode {
        Mode::Steady => Some(prepared.actor_ref().clone()),
        Mode::Drain => None,
    };
    if matches!(mode, Mode::Drain) {
        bounded(prepared.actor_ref().tell(ScriptedMsg::Run))
            .await
            .expect("enqueue before run");
    }
    let args = ScriptedArgs {
        trace: Arc::clone(trace),
        target: target_ref.clone(),
        peer: peer_ref.clone(),
        entered: entered_tx,
        release: release_rx,
        done: done_tx,
    };
    let watcher_join = prepared.spawn_linked_task(args, link_rx);
    if matches!(mode, Mode::Steady) {
        let held = external.as_ref().expect("steady holds a ref");
        bounded(held.tell(ScriptedMsg::Run))
            .await
            .expect("enqueue after run");
    }
    (external, watcher_join, entered, release, done)
}

/// Asserts the target's exact terminal outcome for the scripted death mode.
fn assert_target_death(outcome: &RunResult<Target>, death: Death) {
    match death {
        Death::Drop => assert!(
            matches!(
                outcome,
                RunResult::Stopped {
                    reason: ActorStopReason::Collected,
                    ..
                }
            ),
            "a dropped target collects, got {outcome:?}"
        ),
        Death::Kill => assert!(
            matches!(outcome, RunResult::Killed),
            "a killed target reports Killed, got {outcome:?}"
        ),
    }
}

/// Runs the full scripted scenario once and returns the recorded trace. The
/// choreography is oneshot-gated end to end (never sleep-based): the watcher
/// parks inside its handler while the target dies, so the notice is queued on
/// the link channel before the loop resumes and is drained (biased link arm
/// first) ahead of the self-tell delivery — deterministic ordering.
async fn run_script(mode: Mode, death: Death) -> Vec<TraceEvent> {
    let (target_ref, target_join) = spawn_target();
    let (peer_ref, peer_join, _peer_notices) = spawn_peer();
    let trace: Arc<Mutex<Vec<TraceEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let (external, watcher_join, entered, release, done) =
        start_watcher(mode, &trace, &target_ref, &peer_ref).await;

    bounded(entered).await.expect("the watcher ran its script");
    match death {
        Death::Drop => drop(target_ref),
        Death::Kill => target_ref.kill(),
    }
    let target_outcome = bounded(target_join).await.expect("join target");
    assert_target_death(&target_outcome, death);

    release.send(()).expect("the watcher is parked");
    bounded(done).await.expect("the self-tell was delivered");
    drop(external);
    let watcher_outcome = bounded(watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "both modes end Collected, got {watcher_outcome:?}"
    );
    // `RunResult::Stopped` carries the final state; drop it before joining
    // anything its fields could pin (the #260 discipline).
    drop(watcher_outcome);

    drop(peer_ref);
    let peer_outcome = bounded(peer_join).await.expect("join peer");
    assert!(
        matches!(
            peer_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the peer collects once the test drops its ref, got {peer_outcome:?}"
    );

    std::mem::take(&mut *trace.lock().expect("lock"))
}

/// The exact expected oracle trace. Fully deterministic: the gate
/// choreography fixes every interleaving (notice drained by the biased link
/// arm ahead of the self-tell delivery; `Finished` recorded by `on_stop`).
fn expected_trace(death: Death) -> Vec<TraceEvent> {
    vec![
        TraceEvent::IsAlive(true),
        TraceEvent::TellSelf(true),
        TraceEvent::TellPeer(true),
        TraceEvent::AskPeer(Ok(42)),
        TraceEvent::Watch(Role::Target, Ok(())),
        TraceEvent::Unwatch(Role::Target),
        TraceEvent::Watch(Role::Target, Ok(())),
        TraceEvent::Link(Role::Peer, Ok(())),
        TraceEvent::Notice {
            who: Role::Target,
            reason: death.reason(),
            linked: false,
        },
        TraceEvent::SelfDelivered,
        TraceEvent::Finished(ReasonKind::Collected),
    ]
}

/// #266 oracle, graceful pair: the target's last ref is dropped while the
/// watcher is parked, so the notice is `Collected`. Steady and drain runs must
/// each reproduce the exact expected trace — and therefore each other.
#[tokio::test]
async fn graceful_script_trace_equal_steady_vs_drain() {
    let steady = run_script(Mode::Steady, Death::Drop).await;
    let drain = run_script(Mode::Drain, Death::Drop).await;
    let expected = expected_trace(Death::Drop);
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "handler-context verbs are steady/drain equivalent (ADR-0010)"
    );
}

/// #266 oracle, kill pair: same script, the target is `kill()`ed while the
/// watcher is parked, so the notice is `Killed`.
#[tokio::test]
async fn kill_script_trace_equal_steady_vs_drain() {
    let steady = run_script(Mode::Steady, Death::Kill).await;
    let drain = run_script(Mode::Drain, Death::Kill).await;
    let expected = expected_trace(Death::Kill);
    assert_eq!(steady, expected, "the steady-state run matches the oracle");
    assert_eq!(drain, expected, "the drain-window run matches the oracle");
    assert_eq!(
        steady, drain,
        "handler-context verbs are steady/drain equivalent (ADR-0010)"
    );
}

// ------------------------------------------- step 2: adversarial link tests ---

/// The step-2a/2b fixture: links the peer from inside its (drain-window)
/// handler, records the link outcome, drops its peer ref, signals, and parks
/// so the test sequences the peer's death (2a) or its own (2b) against the
/// parked loop.
struct LinkWatcher {
    peer: Option<capability::Handle<EchoPeer>>,
    notices: NoticeLog,
    link_result: Arc<Mutex<Option<Result<(), ()>>>>,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

struct LinkWatcherArgs {
    peer: capability::Handle<EchoPeer>,
    notices: NoticeLog,
    link_result: Arc<Mutex<Option<Result<(), ()>>>>,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[derive(Debug)]
struct LinkUp;
impl Msg for LinkUp {}
/// The link-watcher's recording reaction (stage 3) — the removed hook
/// override, byte-identical.
struct LinkRecord;
impl capability::WatchPolicy<LinkWatcher> for LinkRecord {
    async fn on_link_died(
        actor: &mut LinkWatcher,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor
            .notices
            .lock()
            .expect("lock")
            .push((id, ReasonKind::of(&reason), linked));
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(bombay_macros::Provide)]
struct LinkWatcherCaps {
    watching: capability::Watching<LinkRecord>,
}
impl capability::CapSet<LinkWatcher> for LinkWatcherCaps {
    fn build(_: &LinkWatcherArgs) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for LinkWatcher {
    type Msg = LinkUp;
    type Args = LinkWatcherArgs;
    type Error = Infallible;
    type Caps = LinkWatcherCaps;
    async fn init(args: Self::Args, _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        let LinkWatcherArgs {
            peer,
            notices,
            link_result,
            entered,
            release,
        } = args;
        Ok(Self {
            peer: Some(peer),
            notices,
            link_result,
            entered: Some(entered),
            release: Some(release),
        })
    }
    async fn handle(
        &mut self,
        _: LinkUp,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        let peer = self.peer.take().expect("one LinkUp enqueued");
        let outcome = bounded(cx.self_ref().link(&peer)).await.map_err(|_| ());
        *self.link_result.lock().expect("lock") = Some(outcome);
        drop(peer); // do not pin the peer (the DrainWatcher discipline)
        self.entered
            .take()
            .expect("entered once")
            .send(())
            .expect("the test is listening");
        bounded(self.release.take().expect("release once"))
            .await
            .expect("the release channel is open");
        Ok(Flow::Continue)
    }
}

/// The shared 2a/2b rig: a linked peer plus a drain-window watcher that links
/// it (message enqueued before run, no external watcher ref).
struct LinkRig {
    peer_ref: capability::Handle<EchoPeer>,
    peer_id: ActorId,
    peer_join: JoinHandle<RunResult<capability::Shell<EchoPeer>>>,
    peer_notices: NoticeLog,
    watcher_id: ActorId,
    watcher_join: JoinHandle<RunResult<capability::Shell<LinkWatcher>>>,
    watcher_notices: NoticeLog,
    link_result: Arc<Mutex<Option<Result<(), ()>>>>,
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

async fn link_rig() -> LinkRig {
    let peer_notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let (p_prepared, p_link_rx) =
        PreparedActor::<capability::Shell<EchoPeer>>::new_linked(config(4));
    let peer_ref = p_prepared.actor_ref().clone();
    let peer_id = peer_ref.id();
    let peer_join = p_prepared.spawn_linked_task(Arc::clone(&peer_notices), p_link_rx);

    let watcher_notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let link_result = Arc::new(Mutex::new(None));
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (w_prepared, w_link_rx) =
        PreparedActor::<capability::Shell<LinkWatcher>>::new_linked(config(4));
    let watcher_id = w_prepared.actor_ref().id();
    // The drain window: enqueue BEFORE running, hold no external watcher ref.
    bounded(w_prepared.actor_ref().tell(LinkUp))
        .await
        .expect("enqueue before run");
    let args = LinkWatcherArgs {
        peer: peer_ref.clone(),
        notices: Arc::clone(&watcher_notices),
        link_result: Arc::clone(&link_result),
        entered: entered_tx,
        release: release_rx,
    };
    let watcher_join = w_prepared.spawn_linked_task(args, w_link_rx);
    LinkRig {
        peer_ref,
        peer_id,
        peer_join,
        peer_notices,
        watcher_id,
        watcher_join,
        watcher_notices,
        link_result,
        entered,
        release,
    }
}

/// #266 (2a): a drain-window `link` installs the watcher-side edge for real —
/// the peer is killed while the watcher is parked, and the watcher's hook
/// receives exactly one notice carrying the peer's true reason, `linked`.
#[tokio::test]
async fn drain_window_link_installs_both_edges_watcher_side() {
    let rig = link_rig().await;
    bounded(rig.entered).await.expect("the link was installed");
    rig.peer_ref.kill();
    let peer_outcome = bounded(rig.peer_join).await.expect("join peer");
    assert!(
        matches!(peer_outcome, RunResult::Killed),
        "kill -> Killed, got {peer_outcome:?}"
    );
    rig.release.send(()).expect("the watcher is parked");
    let watcher_outcome = bounded(rig.watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the watcher drains its backlog and collects, got {watcher_outcome:?}"
    );
    drop(watcher_outcome);
    drop(rig.peer_ref);

    assert_eq!(
        *rig.link_result.lock().expect("lock"),
        Some(Ok(())),
        "the drain-window link itself succeeds"
    );
    assert_eq!(
        *rig.watcher_notices.lock().expect("lock"),
        vec![(rig.peer_id, ReasonKind::Killed, true)],
        "exactly one linked notice with the peer's true reason"
    );
}

/// #266 (2b): the reverse edge — after the drain-window `link`, the watcher
/// drains its backlog and collects; the PEER's hook receives exactly one
/// `linked` notice naming the watcher. This edge is registered onto the
/// watcher's OWN control lane mid-drain (`link` = `self.register_on(peer)` +
/// `peer.register_on(self)`) and only fires if the watcher's loop applied the
/// queued control signal. The post-join `ask` is a flush barrier: the biased
/// link arm drains the queued notice before the ask is handled.
#[tokio::test]
async fn drain_window_link_installs_both_edges_peer_side() {
    let rig = link_rig().await;
    bounded(rig.entered).await.expect("the link was installed");
    rig.release.send(()).expect("the watcher is parked");
    let watcher_outcome = bounded(rig.watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the watcher collects after its handler, got {watcher_outcome:?}"
    );
    drop(watcher_outcome);

    let flushed = bounded(rig.peer_ref.ask(|reply| PeerMsg::Query { reply }))
        .await
        .expect("the peer is alive");
    assert_eq!(flushed, 42, "the flush barrier round-trips");
    assert_eq!(
        *rig.peer_notices.lock().expect("lock"),
        vec![(rig.watcher_id, ReasonKind::Collected, true)],
        "exactly one linked notice naming the watcher"
    );
    assert_eq!(
        *rig.link_result.lock().expect("lock"),
        Some(Ok(())),
        "the drain-window link itself succeeds"
    );

    drop(rig.peer_ref);
    let peer_outcome = bounded(rig.peer_join).await.expect("join peer");
    assert!(matches!(
        peer_outcome,
        RunResult::Stopped {
            reason: ActorStopReason::Collected,
            ..
        }
    ));
}

/// The 2c fixture: a watcher with the DEFAULT `on_link_died` hook (a linked
/// abnormal death propagates). Handler 1 installs the link and drops the peer
/// ref; handler 2 parks so the test sequences the peer's panic against the
/// parked loop.
struct TrapWatcher {
    peer: Option<capability::Handle<EchoPeer>>,
    linked: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}
#[derive(Debug)]
enum TrapMsg {
    LinkUp,
    Park,
}
impl Msg for TrapMsg {}

/// 2c keeps the OTP semantics — under stage 3 that is the NAMED
/// `OtpPropagation` policy, chosen here rather than inherited.
#[derive(bombay_macros::Provide)]
struct TrapWatcherCaps {
    watching: capability::Watching<capability::OtpPropagation>,
}
impl capability::CapSet<TrapWatcher> for TrapWatcherCaps {
    fn build(_: &<TrapWatcher as capability::Actor>::Args) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for TrapWatcher {
    type Msg = TrapMsg;
    type Args = (
        capability::Handle<EchoPeer>,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
    );
    type Error = Infallible;
    type Caps = TrapWatcherCaps;
    async fn init(
        (peer, linked, release): Self::Args,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            peer: Some(peer),
            linked: Some(linked),
            release: Some(release),
        })
    }
    async fn handle(
        &mut self,
        msg: TrapMsg,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            TrapMsg::LinkUp => {
                let peer = self.peer.take().expect("LinkUp runs once");
                bounded(cx.self_ref().link(&peer))
                    .await
                    .expect("the drain-window link succeeds");
                drop(peer);
                self.linked
                    .take()
                    .expect("linked once")
                    .send(())
                    .expect("the test is listening");
            }
            TrapMsg::Park => {
                bounded(self.release.take().expect("release once"))
                    .await
                    .expect("the release channel is open");
            }
        }
        Ok(Flow::Continue)
    }
}

/// #266 (2c): a drain-window `link` propagates the peer's PANIC through the
/// default hook: the watcher stops `LinkDied { id: peer, reason: Panicked }`.
/// Two messages are enqueued before the run (drain window): handler 1 links
/// and signals, handler 2 parks; the link arm (biased first) delivers the
/// notice after the parked handler returns without setting `stop`.
#[tokio::test]
async fn drain_window_link_propagates_peer_panic_as_link_died() {
    let peer_notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let (p_prepared, p_link_rx) =
        PreparedActor::<capability::Shell<EchoPeer>>::new_linked(config(4));
    let peer_ref = p_prepared.actor_ref().clone();
    let peer_id = peer_ref.id();
    let peer_join = p_prepared.spawn_linked_task(peer_notices, p_link_rx);

    let (linked_tx, linked) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (w_prepared, w_link_rx) =
        PreparedActor::<capability::Shell<TrapWatcher>>::new_linked(config(4));
    // The drain window: BOTH messages enqueued before the run, no external ref.
    bounded(w_prepared.actor_ref().tell(TrapMsg::LinkUp))
        .await
        .expect("enqueue 1");
    bounded(w_prepared.actor_ref().tell(TrapMsg::Park))
        .await
        .expect("enqueue 2");
    let watcher_join =
        w_prepared.spawn_linked_task((peer_ref.clone(), linked_tx, release_rx), w_link_rx);

    bounded(linked).await.expect("the link was installed");
    bounded(peer_ref.tell(PeerMsg::Boom))
        .await
        .expect("the panicking message is enqueued");
    let peer_outcome = bounded(peer_join).await.expect("join peer");
    assert!(
        matches!(
            peer_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Panicked(_),
                ..
            }
        ),
        "the peer died Panicked, got {peer_outcome:?}"
    );
    release.send(()).expect("the watcher is parked");
    let watcher_outcome = bounded(watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            &watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::LinkDied { id, reason },
                ..
            } if *id == peer_id && matches!(reason.as_ref(), ActorStopReason::Panicked(_))
        ),
        "the default hook propagated the peer's panic as LinkDied, got {watcher_outcome:?}"
    );
    drop(watcher_outcome);
    drop(peer_ref);
}

/// The 2d fixture: TWO queued messages each watch the SAME target from their
/// own per-message drain-window mint (handler 1's mint dies when it returns;
/// handler 2 forces a fresh mint). Handler 2 drops the target ref and parks so
/// the test sequences the target's death.
struct DupWatcher {
    target: Option<ActorRef<Target>>,
    watch_results: Arc<Mutex<Vec<Result<(), ()>>>>,
    notices: NoticeLog,
    entered: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}
#[derive(Debug)]
enum DupMsg {
    WatchFirst,
    WatchSecond,
}
impl Msg for DupMsg {}
impl Mailboxed for DupWatcher {
    type Msg = DupMsg;
}

impl DupWatcher {
    fn record_watch(&self, outcome: Result<(), ()>) {
        self.watch_results.lock().expect("lock").push(outcome);
    }
}

/// The duplicate-edge watcher's recording reaction (stage 3).
struct DupRecord;
impl capability::WatchPolicy<DupWatcher> for DupRecord {
    async fn on_link_died(
        actor: &mut DupWatcher,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor
            .notices
            .lock()
            .expect("lock")
            .push((id, ReasonKind::of(&reason), linked));
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(bombay_macros::Provide)]
struct DupWatcherCaps {
    watching: capability::Watching<DupRecord>,
}
impl capability::CapSet<DupWatcher> for DupWatcherCaps {
    fn build(_: &<DupWatcher as capability::Actor>::Args) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for DupWatcher {
    type Msg = DupMsg;
    type Args = (
        ActorRef<Target>,
        Arc<Mutex<Vec<Result<(), ()>>>>,
        NoticeLog,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
    );
    type Error = Infallible;
    type Caps = DupWatcherCaps;
    async fn init(
        (target, watch_results, notices, entered, release): Self::Args,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            target: Some(target),
            watch_results,
            notices,
            entered: Some(entered),
            release: Some(release),
        })
    }
    async fn handle(
        &mut self,
        msg: DupMsg,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            DupMsg::WatchFirst => {
                let target = self.target.as_ref().expect("the target is present");
                let outcome = bounded(cx.self_ref().watch(target)).await.map_err(|_| ());
                self.record_watch(outcome);
            }
            DupMsg::WatchSecond => {
                let target = self.target.take().expect("the target is present");
                let outcome = bounded(cx.self_ref().watch(&target)).await.map_err(|_| ());
                self.record_watch(outcome);
                drop(target); // release the pin BEFORE the test drops its ref
                self.entered
                    .take()
                    .expect("entered once")
                    .send(())
                    .expect("the test is listening");
                bounded(self.release.take().expect("release once"))
                    .await
                    .expect("the release channel is open");
            }
        }
        Ok(Flow::Continue)
    }
}
/// #266 (2d): per-message drain-window mints register INDEPENDENT duplicate
/// edges (`Watchers::apply` keeps duplicates — repeated `watch` calls match
/// Erlang's independent monitors). Two queued messages watch the same target;
/// the target's death delivers TWO notices. A deduplicating mint fails the
/// count.
#[tokio::test]
async fn per_message_mints_register_independent_duplicate_edges() {
    let (target_ref, target_join) = spawn_target();
    let target_id = target_ref.id();
    let watch_results = Arc::new(Mutex::new(Vec::new()));
    let notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (w_prepared, w_link_rx) =
        PreparedActor::<capability::Shell<DupWatcher>>::new_linked(config(4));
    // Prerequisite for the per-message mint: no external ref, BOTH messages
    // enqueued before the run.
    bounded(w_prepared.actor_ref().tell(DupMsg::WatchFirst))
        .await
        .expect("enqueue 1");
    bounded(w_prepared.actor_ref().tell(DupMsg::WatchSecond))
        .await
        .expect("enqueue 2");
    let args = (
        target_ref.clone(),
        Arc::clone(&watch_results),
        Arc::clone(&notices),
        entered_tx,
        release_rx,
    );
    let watcher_join = w_prepared.spawn_linked_task(args, w_link_rx);

    bounded(entered).await.expect("both watches registered");
    drop(target_ref);
    let target_outcome = bounded(target_join).await.expect("join target");
    assert_target_death(&target_outcome, Death::Drop);
    release.send(()).expect("the watcher is parked");
    let watcher_outcome = bounded(watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the watcher collects after draining both notices, got {watcher_outcome:?}"
    );
    drop(watcher_outcome);

    assert_eq!(
        *watch_results.lock().expect("lock"),
        vec![Ok(()), Ok(())],
        "both mints' watches succeed"
    );
    assert_eq!(
        *notices.lock().expect("lock"),
        vec![
            (target_id, ReasonKind::Collected, false),
            (target_id, ReasonKind::Collected, false),
        ],
        "two independent edges deliver exactly two notices"
    );
}

/// The 2e fixture: watches the target in its only (drain-window) handler and
/// returns immediately — the loop then takes its `Collected` break with the
/// target still alive. `on_stop` signals the break decision and parks, so the
/// test can fire the target's death notice onto the by-then-undrained link
/// channel.
struct LateWatcher {
    target: Option<ActorRef<Target>>,
    watch_result: Arc<Mutex<Option<Result<(), ()>>>>,
    notices: NoticeLog,
    stopping: Option<oneshot::Sender<()>>,
    stop_release: Option<oneshot::Receiver<()>>,
}
#[derive(Debug)]
struct LateGo;
impl Msg for LateGo {}
/// The late watcher's recording reaction (stage 3) — must never fire in the
/// designed-lost test, which is the point.
struct LateRecord;
impl capability::WatchPolicy<LateWatcher> for LateRecord {
    async fn on_link_died(
        actor: &mut LateWatcher,
        id: ActorId,
        reason: ActorStopReason,
        linked: bool,
    ) -> Result<ControlFlow<ActorStopReason>, Infallible> {
        actor
            .notices
            .lock()
            .expect("lock")
            .push((id, ReasonKind::of(&reason), linked));
        Ok(ControlFlow::Continue(()))
    }
}

#[derive(bombay_macros::Provide)]
struct LateWatcherCaps {
    watching: capability::Watching<LateRecord>,
}
impl capability::CapSet<LateWatcher> for LateWatcherCaps {
    fn build(_: &<LateWatcher as capability::Actor>::Args) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for LateWatcher {
    type Msg = LateGo;
    type Args = (
        ActorRef<Target>,
        Arc<Mutex<Option<Result<(), ()>>>>,
        NoticeLog,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
    );
    type Error = Infallible;
    type Caps = LateWatcherCaps;
    async fn init(
        (target, watch_result, notices, stopping, stop_release): Self::Args,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            target: Some(target),
            watch_result,
            notices,
            stopping: Some(stopping),
            stop_release: Some(stop_release),
        })
    }
    async fn handle(
        &mut self,
        _: LateGo,
        cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        let target = self.target.take().expect("one LateGo enqueued");
        let outcome = bounded(cx.self_ref().watch(&target)).await.map_err(|_| ());
        *self.watch_result.lock().expect("lock") = Some(outcome);
        drop(target); // the watcher must not pin the target
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<capability::Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopping
            .take()
            .expect("on_stop runs once")
            .send(())
            .expect("the test is listening");
        // Parked past the loop's break decision; the configured 60 s grace
        // (SpawnConfig::on_stop_grace) never trips in this choreography.
        bounded(self.stop_release.take().expect("stop_release once"))
            .await
            .expect("the stop_release channel is open");
        Ok(())
    }
}

/// #266 (2e, decision 1 — designed-lost, GREEN pin): a death notice that
/// lands on the link channel AFTER the loop's break decision is dropped. The
/// watcher watches the target and returns; the backlog empties and the loop
/// breaks `Collected` (proved by `on_stop` running) while the target is still
/// alive. The target's later death notice is never delivered — Erlang parity:
/// a `DOWN` to an already-stopping process is dropped, and delivering
/// post-break would violate finish-current-then-stop.
#[tokio::test]
async fn late_notice_after_break_decision_is_dropped_by_design() {
    let (target_ref, target_join) = spawn_target();
    let watch_result = Arc::new(Mutex::new(None));
    let notices: NoticeLog = Arc::new(Mutex::new(Vec::new()));
    let (stopping_tx, stopping) = oneshot::channel();
    let (stop_release, stop_release_rx) = oneshot::channel();
    let (w_prepared, w_link_rx) =
        PreparedActor::<capability::Shell<LateWatcher>>::new_linked(SpawnConfig {
            capacity: cap(4),
            on_stop_grace: Duration::from_mins(1),
        });
    // The drain window: enqueue before the run, hold no external ref.
    bounded(w_prepared.actor_ref().tell(LateGo))
        .await
        .expect("enqueue before run");
    let args = (
        target_ref.clone(),
        Arc::clone(&watch_result),
        Arc::clone(&notices),
        stopping_tx,
        stop_release_rx,
    );
    let watcher_join = w_prepared.spawn_linked_task(args, w_link_rx);

    // The loop handled the message, found the backlog empty and every sender
    // gone, and took its break decision — `on_stop` running is the proof.
    bounded(stopping)
        .await
        .expect("the break decision was taken");
    drop(target_ref);
    let target_outcome = bounded(target_join).await.expect("join target");
    assert_target_death(&target_outcome, Death::Drop);
    stop_release.send(()).expect("on_stop is parked");
    let watcher_outcome = bounded(watcher_join).await.expect("join watcher");
    assert!(
        matches!(
            watcher_outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the watcher stopped Collected, got {watcher_outcome:?}"
    );
    drop(watcher_outcome);

    assert_eq!(
        *watch_result.lock().expect("lock"),
        Some(Ok(())),
        "the drain-window watch itself succeeded"
    );
    assert!(
        notices.lock().expect("lock").is_empty(),
        "designed-lost: nothing is delivered after the break decision (card #266)"
    );
}

// ------------------------------------- step 2f: spec'd divergence pins --------

/// The 2f fixture: pipes a ready future / arms a timer from its drain-window
/// handler. Both mechanisms hold only a WEAK self-ref (ADR-0017), so the
/// drain-window collection resolves before the delivery attempt and the
/// result is dropped — the spec'd fate (`pipe.rs`'s weak-upgrade seam,
/// `timer.rs`'s `send_after`).
struct PipeCounter {
    handled: Arc<AtomicU32>,
}
#[derive(Debug)]
enum PipeMsg {
    KickPipe,
    KickTimer,
    /// The pipe's delivery — payload-free: the pin asserts it NEVER arrives.
    Piped,
    Tick,
}
impl Msg for PipeMsg {}
impl Mailboxed for PipeCounter {
    type Msg = PipeMsg;
}
impl Actor for PipeCounter {
    type Args = Arc<AtomicU32>;
    type Error = Infallible;
    async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { handled })
    }
    async fn handle(
        &mut self,
        msg: PipeMsg,
        actor_ref: ActorRef<Self>,
    ) -> Result<Flow, Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        match msg {
            PipeMsg::KickPipe => {
                actor_ref.pipe_to_self(async { 42_u32 }, |outcome| {
                    let _ = outcome;
                    PipeMsg::Piped
                });
            }
            PipeMsg::KickTimer => {
                let _detached = actor_ref.send_after(Duration::from_millis(1), PipeMsg::Tick);
            }
            PipeMsg::Piped | PipeMsg::Tick => {}
        }
        Ok(Flow::Continue)
    }
}

/// #266 (2f, decision 2): a `pipe_to_self` armed in the drain window never
/// delivers — by the time the pipe task resolves, no strong ref exists and
/// the weak upgrade fails, so the result is dropped (the spec'd fate).
/// Steady-state delivery is already pinned by
/// `piped_future_result_arrives_as_mapped_message` (`pipe.rs`), so no steady
/// sibling is duplicated here. The "never arrives" side is bounded by the
/// `Collected` `RunResult`, not by a sleep.
#[tokio::test]
async fn drain_window_pipe_result_dropped_by_design() {
    let handled = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<PipeCounter>::new(config(4));
    bounded(prepared.actor_ref().tell(PipeMsg::KickPipe))
        .await
        .expect("enqueue before run");
    // No external ref: once the loop downgrades after on_start, only the
    // queued message pins the actor — the drain window.
    let outcome = bounded(prepared.run(Arc::clone(&handled))).await;
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the backlog drains and the actor collects, got {outcome:?}"
    );
    drop(outcome);
    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the piped result never arrives: the weak upgrade fails in the drain window"
    );
}

/// #266 (2f, decision 2): a `send_after` timer armed in the drain window
/// never delivers — same weak-upgrade seam as the pipe pin. Steady-state
/// delivery is already pinned by `send_after_fires_exact_value_after_delay`
/// (`timer.rs`).
#[tokio::test]
async fn drain_window_timer_message_dropped_by_design() {
    let handled = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<PipeCounter>::new(config(4));
    bounded(prepared.actor_ref().tell(PipeMsg::KickTimer))
        .await
        .expect("enqueue before run");
    let outcome = bounded(prepared.run(Arc::clone(&handled))).await;
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "the backlog drains and the actor collects, got {outcome:?}"
    );
    drop(outcome);
    assert_eq!(
        handled.load(Ordering::SeqCst),
        1,
        "the timer message never arrives: the weak upgrade fails in the drain window"
    );
}
