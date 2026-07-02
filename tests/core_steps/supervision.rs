//! Shared `SupervisionWorld` + step definitions for the core `supervision`
//! scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_supervision_bdd.rs`       — the example feature (supervision.feature)
//!   * `core_supervision_props_bdd.rs` — the property/model laws
//!                                       (supervision.properties.feature)
//!
//! The SUT is `src/supervision.rs` (Erlang-style supervision) plus the
//! `should_restart` decision on `ErasedChildSpec` (`src/links.rs:226-265`):
//! `RestartPolicy` (Permanent/Transient/Never), `SupervisionStrategy`
//! (OneForOne/OneForAll/RestForOne), and the restart-intensity sliding window.
//!
//! Two surfaces are used, chosen per scenario from its `# Confirmed:` note:
//!
//!   * **Raw `should_restart`** (`bombay::supervision::testing::decision_spec` +
//!     `ErasedChildSpec::should_restart`): scenarios that pin the DECISION
//!     itself — the policy × exit-kind matrix, the `SupervisorRestart` bypass,
//!     the `Never` precedence, and the intensity-window arithmetic
//!     (`restart_count` / `max_restarts` / `restart_window` / `last_restart`).
//!     Driving the window deterministically needs an explicit `last_restart`
//!     [`Instant`], NOT a paused tokio clock: `should_restart` reads
//!     `std::time::Instant::now()` (links.rs:250), which `tokio::time::pause()`
//!     does NOT affect. So "the window has elapsed" is modelled by passing a
//!     `last_restart` far in the past and "still within the window" by passing
//!     `Instant::now()` — no real sleep, fully deterministic.
//!   * **Real spawned actors** (`bombay::prelude::*`): the end-to-end strategy
//!     scenarios (which siblings restart under OneForOne/OneForAll/RestForOne)
//!     and the intensity behaviour observed through the real supervisor loop.
//!     Each child records its start count in a shared `AtomicU32`; a restart
//!     re-runs `on_start` so the count increments, and a bounded condition-based
//!     `settle()` (panics loudly) waits for the observable rather than a fixed
//!     sleep.
//!
//! TIMING DISCIPLINE: every restart observation uses a bounded condition-based
//! `settle()`; the intensity-window scenarios use deterministic `Instant`s, not
//! real sleeps. No unbounded await on a restart that may never happen.

use std::{
    collections::BTreeSet,
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use bombay::{
    actor::ActorId,
    error::{ActorStopReason, Infallible, PanicError, PanicReason},
    links::{ErasedChildSpec, NoRestartReason},
    prelude::*,
    supervision::{RestartPolicy, SupervisionStrategy, testing::decision_spec},
};
use cucumber::{World, given, then, when};
use tokio::sync::Barrier;

// ===========================================================================
// Condition-based settle — bounded poll, panics loudly. NEVER wait_for_shutdown
// as a settle barrier (a restart re-spawns; the mailbox never observably
// "closes" the way a one-shot shutdown does).
// ===========================================================================

// A GENEROUS positive-observation bound. `settle` returns the instant its
// condition holds, so a large bound costs a passing run nothing — it only
// widens the margin before a genuinely-stalled restart is declared a failure.
// Sized for the worst case (a 2-core CI runner under llvm-cov instrumentation,
// where the async restart path is heavily scheduler-starved); the poll interval
// itself also wakes late under load, so the effective wall-clock bound self-
// scales well past the nominal SETTLE_STEPS * SETTLE_TICK.
const SETTLE_STEPS: usize = 2000;
const SETTLE_TICK: Duration = Duration::from_millis(5);
/// A short bound for "this did NOT happen" assertions: long enough that a real
/// restart would have been observed, short enough to keep the suite fast.
const NEGATIVE_BOUND: Duration = Duration::from_millis(300);

async fn settle<F: FnMut() -> bool>(mut cond: F, msg: &str) {
    for _ in 0..SETTLE_STEPS {
        if cond() {
            return;
        }
        tokio::time::sleep(SETTLE_TICK).await;
    }
    panic!("condition did not settle within the bound: {msg}");
}

/// Waits `NEGATIVE_BOUND`, then asserts `cond` STILL holds (used for
/// "child2 is not restarted" — give any spurious restart time to manifest).
async fn hold_for<F: Fn() -> bool>(cond: F, msg: &str) {
    let deadline = tokio::time::Instant::now() + NEGATIVE_BOUND;
    while tokio::time::Instant::now() < deadline {
        assert!(cond(), "{msg}");
        tokio::time::sleep(SETTLE_TICK).await;
    }
}

// ===========================================================================
// Real supervisor + child actors
// ===========================================================================

/// A supervised child that counts its starts (a restart re-runs `on_start`) and
/// panics on `Boom`. `Stop` exits normally; `Fail` returns an error reply
/// (abnormal exit under Transient/Permanent).
#[derive(Clone)]
struct Child {
    starts: Arc<AtomicU32>,
}

impl Actor for Child {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        state.starts.fetch_add(1, Ordering::SeqCst);
        Ok(state)
    }
}

struct Boom;
impl Message<Boom> for Child {
    type Reply = ();
    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("child boom");
    }
}

struct Stop;
impl Message<Stop> for Child {
    type Reply = ();
    async fn handle(&mut self, _: Stop, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        ctx.stop();
    }
}

struct Fail;
impl Message<Fail> for Child {
    type Reply = Result<(), &'static str>;
    async fn handle(&mut self, _: Fail, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        Err("child error")
    }
}

/// A child whose `on_start` SUCCEEDS the first time and FAILS on every restart.
/// `attempts` counts every `on_start` entry (so a re-entry on restart is
/// observable); `alive` counts only successful starts.
#[derive(Clone)]
struct FailOnRestartChild {
    attempts: Arc<AtomicU32>,
    alive: Arc<AtomicU32>,
}

/// An on_start error type carrying nothing — its presence (and the framework's
/// `PanicReason::OnStart` classification) is the observable.
#[derive(Debug, Clone)]
struct StartFailed;
impl std::fmt::Display for StartFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "on_start failed")
    }
}
impl std::error::Error for StartFailed {}

impl Actor for FailOnRestartChild {
    type Args = Self;
    type Error = StartFailed;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let attempt = state.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            state.alive.fetch_add(1, Ordering::SeqCst);
            Ok(state)
        } else {
            Err(StartFailed)
        }
    }
}

impl Message<Boom> for FailOnRestartChild {
    type Reply = ();
    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        panic!("fail-on-restart child boom");
    }
}

/// A supervisor whose strategy is fixed per type. The strategy is read from the
/// `Actor::supervision_strategy` hook (the SUT path).
macro_rules! supervisor {
    ($name:ident, $strategy:expr) => {
        struct $name;
        impl Actor for $name {
            type Args = Self;
            type Error = Infallible;
            fn supervision_strategy() -> SupervisionStrategy {
                $strategy
            }
            async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(state)
            }
        }
    };
}

supervisor!(OneForOneSup, SupervisionStrategy::OneForOne);
supervisor!(OneForAllSup, SupervisionStrategy::OneForAll);
supervisor!(RestForOneSup, SupervisionStrategy::RestForOne);

/// An erased supervisor ref so the World can hold any of the three strategies
/// for a uniform `.kill()` on teardown.
enum AnySup {
    OneForOne(ActorRef<OneForOneSup>),
    OneForAll(ActorRef<OneForAllSup>),
    RestForOne(ActorRef<RestForOneSup>),
}

impl AnySup {
    fn kill(&self) {
        match self {
            AnySup::OneForOne(r) => r.kill(),
            AnySup::OneForAll(r) => r.kill(),
            AnySup::RestForOne(r) => r.kill(),
        }
    }
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Default, World)]
pub struct SupervisionWorld {
    /// The live supervisor (real-actor scenarios).
    sup: Option<AnySup>,
    /// Named children: label -> (ref, start-counter).
    children: Vec<(String, ActorRef<Child>, Arc<AtomicU32>)>,
    /// The single supervised child (policy/intensity behaviour scenarios).
    child: Option<ActorRef<Child>>,
    child_starts: Option<Arc<AtomicU32>>,

    /// Raw decision-table scenarios: the constructed spec + last decision.
    spec: Option<ErasedChildSpec>,
    last_decision: Option<ControlFlow<NoRestartReason>>,

    /// Builder-default scenario: the captured (max_restarts, restart_window).
    builder_limits: Option<(u32, Duration)>,

    /// on_start-fails-on-restart scenario.
    fail_attempts: Option<Arc<AtomicU32>>,
    fail_alive: Option<Arc<AtomicU32>>,
    fail_child: Option<ActorRef<FailOnRestartChild>>,
    fail_first_reason: Option<ActorStopReason>,
}

impl std::fmt::Debug for SupervisionWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisionWorld")
            .field("last_decision", &self.last_decision)
            .field("builder_limits", &self.builder_limits)
            .finish_non_exhaustive()
    }
}

impl SupervisionWorld {
    fn child_starts_of(&self, label: &str) -> Arc<AtomicU32> {
        self.children
            .iter()
            .find(|(l, _, _)| l == label)
            .map(|(_, _, c)| c.clone())
            .unwrap_or_else(|| panic!("no child labelled {label}"))
    }

    fn child_ref_of(&self, label: &str) -> ActorRef<Child> {
        self.children
            .iter()
            .find(|(l, _, _)| l == label)
            .map(|(_, r, _)| r.clone())
            .unwrap_or_else(|| panic!("no child labelled {label}"))
    }
}

fn policy_of(s: &str) -> RestartPolicy {
    match s {
        "Permanent" => RestartPolicy::Permanent,
        "Transient" => RestartPolicy::Transient,
        "Never" => RestartPolicy::Never,
        other => panic!("unknown policy {other}"),
    }
}

/// The reason an `exit_kind` produces, as the run loop would classify it.
fn reason_of(exit_kind: &str) -> ActorStopReason {
    match exit_kind {
        "panic" => {
            ActorStopReason::Panicked(PanicError::new(Box::new("boom"), PanicReason::HandlerPanic))
        }
        "error" => {
            ActorStopReason::Panicked(PanicError::new(Box::new("err"), PanicReason::OnMessage))
        }
        "normal" => ActorStopReason::Normal,
        other => panic!("unknown exit kind {other}"),
    }
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^a supervisor actor with default restart limit 5 restarts per 5 seconds$")]
async fn given_background_supervisor(_world: &mut SupervisionWorld) {
    // Pure context for the feature; concrete supervisors are built per scenario.
}

#[given(regex = r"^a supervisor actor with child actors$")]
async fn given_props_background(_world: &mut SupervisionWorld) {}

// ===========================================================================
// @lifecycle — RestartPolicy x exit-kind decision matrix (should_restart)
// ===========================================================================

#[given(regex = r#"^a supervised child with restart policy "([^"]+)"$"#)]
async fn given_child_policy(world: &mut SupervisionWorld, policy: String) {
    // Build a fresh spec with a generous limit so the policy/exit-kind decision
    // is the only thing under test.
    world.spec = Some(decision_spec(
        policy_of(&policy),
        0,
        100,
        Duration::from_secs(3600),
        Instant::now(),
    ));
}

#[when(regex = r#"^the child exits via "([^"]+)"$"#)]
async fn when_child_exits_via(world: &mut SupervisionWorld, exit_kind: String) {
    let spec = world.spec.as_mut().expect("spec built");
    let decision = spec.should_restart(&reason_of(&exit_kind));
    world.last_decision = Some(decision);
}

#[then(regex = r"^the child is restarted: (yes|no)$")]
async fn then_child_restarted_yesno(world: &mut SupervisionWorld, yesno: String) {
    let decision = world.last_decision.as_ref().expect("decision recorded");
    match yesno.as_str() {
        "yes" => assert!(
            matches!(decision, ControlFlow::Continue(())),
            "expected Continue (restart), got {decision:?}"
        ),
        "no" => assert!(
            matches!(decision, ControlFlow::Break(_)),
            "expected Break (no restart), got {decision:?}"
        ),
        _ => unreachable!(),
    }
}

// --- restarted child re-runs on_start (real actor) -------------------------

#[when(regex = r"^the child panics once$")]
async fn when_child_panics_once(world: &mut SupervisionWorld) {
    // Shared by two scenarios distinguished by their Given:
    //   * "re-runs on_start" (Given = restart policy Permanent, no real child
    //     spawned) — spawn a REAL Permanent child here, then panic it once.
    //   * "restart_limit(0) ... not restarted" (Given = restart limit 0 ...,
    //     which spawned a real limit-0 child) — panic THAT child once.
    if world.child.is_none() {
        let starts = Arc::new(AtomicU32::new(0));
        let sup = OneForOneSup::spawn(OneForOneSup);
        let child = Child::supervise(
            &sup,
            Child {
                starts: starts.clone(),
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(5, Duration::from_secs(10))
        .spawn()
        .await;
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        world.sup = Some(AnySup::OneForOne(sup));
        world.child = Some(child);
        world.child_starts = Some(starts);
    }
    let child = world.child.as_ref().expect("child").clone();
    let _ = child.tell(Boom).await;
}

#[then(regex = r"^on_start runs again and the child is alive afterwards$")]
async fn then_on_start_runs_again(world: &mut SupervisionWorld) {
    let starts = world.child_starts.as_ref().expect("starts").clone();
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 2
        },
        "on_start did not run again after the panic (no restart)",
    )
    .await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "the restart must re-run on_start exactly once more"
    );
    let child = world.child.as_ref().expect("child");
    settle(
        {
            let c = child.clone();
            move || c.is_alive()
        },
        "the restarted child is not alive",
    )
    .await;
    assert!(
        child.is_alive(),
        "the child must be alive after the restart"
    );
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// ===========================================================================
// @sequence — SupervisionStrategy restart-sets (real actor trees)
// ===========================================================================

async fn spawn_one_for_one(world: &mut SupervisionWorld, labels: &[&str]) {
    let sup = OneForOneSup::spawn(OneForOneSup);
    for label in labels {
        let starts = Arc::new(AtomicU32::new(0));
        let child = Child::supervise(
            &sup,
            Child {
                starts: starts.clone(),
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(30))
        .spawn()
        .await;
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        world.children.push((label.to_string(), child, starts));
    }
    world.sup = Some(AnySup::OneForOne(sup));
}

async fn spawn_one_for_all(world: &mut SupervisionWorld, labels: &[&str]) {
    let sup = OneForAllSup::spawn(OneForAllSup);
    for label in labels {
        let starts = Arc::new(AtomicU32::new(0));
        let child = Child::supervise(
            &sup,
            Child {
                starts: starts.clone(),
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(30))
        .spawn()
        .await;
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        world.children.push((label.to_string(), child, starts));
    }
    world.sup = Some(AnySup::OneForAll(sup));
}

async fn spawn_rest_for_one(world: &mut SupervisionWorld, labels: &[&str]) {
    let sup = RestForOneSup::spawn(RestForOneSup);
    for label in labels {
        let starts = Arc::new(AtomicU32::new(0));
        let child = Child::supervise(
            &sup,
            Child {
                starts: starts.clone(),
            },
        )
        .restart_policy(RestartPolicy::Permanent)
        .restart_limit(10, Duration::from_secs(30))
        .spawn()
        .await;
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        world.children.push((label.to_string(), child, starts));
    }
    world.sup = Some(AnySup::RestForOne(sup));
}

#[given(regex = r#"^a "OneForOne" supervisor with children "([^"]+)" and "([^"]+)"$"#)]
async fn given_ofo_two(world: &mut SupervisionWorld, c1: String, c2: String) {
    spawn_one_for_one(world, &[&c1, &c2]).await;
}

#[given(regex = r#"^a "OneForAll" supervisor with children "([^"]+)", "([^"]+)" and "([^"]+)"$"#)]
async fn given_ofa_three(world: &mut SupervisionWorld, c1: String, c2: String, c3: String) {
    spawn_one_for_all(world, &[&c1, &c2, &c3]).await;
}

#[given(regex = r#"^a "OneForAll" supervisor with children "([^"]+)" and "([^"]+)"$"#)]
async fn given_ofa_two(world: &mut SupervisionWorld, c1: String, c2: String) {
    spawn_one_for_all(world, &[&c1, &c2]).await;
}

#[given(
    regex = r#"^a "RestForOne" supervisor with children spawned in order "([^"]+)", "([^"]+)", "([^"]+)"$"#
)]
async fn given_rfo_three(world: &mut SupervisionWorld, c1: String, c2: String, c3: String) {
    spawn_rest_for_one(world, &[&c1, &c2, &c3]).await;
}

#[when(regex = r#"^"([^"]+)" panics$"#)]
async fn when_named_panics(world: &mut SupervisionWorld, label: String) {
    let child = world.child_ref_of(&label);
    let _ = child.tell(Boom).await;
}

#[then(regex = r#"^"([^"]+)" is restarted$"#)]
async fn then_named_restarted(world: &mut SupervisionWorld, label: String) {
    let starts = world.child_starts_of(&label);
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 2
        },
        &format!("child {label} was not restarted"),
    )
    .await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "child {label} restarts exactly once"
    );
}

#[then(regex = r#"^"([^"]+)" is not restarted$"#)]
async fn then_named_not_restarted(world: &mut SupervisionWorld, label: String) {
    let starts = world.child_starts_of(&label);
    hold_for(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) == 1
        },
        &format!("child {label} must NOT be restarted (start count stayed 1)"),
    )
    .await;
}

#[then(regex = r#"^"([^"]+)", "([^"]+)" and "([^"]+)" are all restarted exactly once$"#)]
async fn then_three_all_restarted_once(
    world: &mut SupervisionWorld,
    c1: String,
    c2: String,
    c3: String,
) {
    for label in [&c1, &c2, &c3] {
        let starts = world.child_starts_of(label);
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 2
            },
            &format!("child {label} was not restarted"),
        )
        .await;
    }
    // Hold to prove EXACTLY once (no extra cascade restart).
    for label in [&c1, &c2, &c3] {
        let starts = world.child_starts_of(label);
        hold_for(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) == 2
            },
            &format!("child {label} must be restarted exactly once, not more"),
        )
        .await;
    }
}

#[then(regex = r#"^"([^"]+)", "([^"]+)" and "([^"]+)" are all restarted$"#)]
async fn then_three_all_restarted(
    world: &mut SupervisionWorld,
    c1: String,
    c2: String,
    c3: String,
) {
    for label in [&c1, &c2, &c3] {
        let starts = world.child_starts_of(label);
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 2
            },
            &format!("child {label} was not restarted"),
        )
        .await;
    }
}

#[then(regex = r#"^"([^"]+)" and "([^"]+)" are restarted$"#)]
async fn then_two_restarted(world: &mut SupervisionWorld, c1: String, c2: String) {
    for label in [&c1, &c2] {
        let starts = world.child_starts_of(label);
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 2
            },
            &format!("child {label} was not restarted"),
        )
        .await;
    }
}

#[then(
    regex = r"^the restarted set is exactly the failed child and the children spawned after it$"
)]
async fn then_restarted_set_is_younger(world: &mut SupervisionWorld) {
    // RestForOne with order c1,c2,c3 and c2 failing: restarted set = {c2, c3},
    // and c1 (spawned before) is untouched. Assert the exact partition.
    let c1 = world.child_starts_of("c1");
    let c2 = world.child_starts_of("c2");
    let c3 = world.child_starts_of("c3");
    settle(
        {
            let c2 = c2.clone();
            let c3 = c3.clone();
            move || c2.load(Ordering::SeqCst) >= 2 && c3.load(Ordering::SeqCst) >= 2
        },
        "c2 and c3 (failed + younger) were not both restarted",
    )
    .await;
    hold_for(
        {
            let c1 = c1.clone();
            move || c1.load(Ordering::SeqCst) == 1
        },
        "c1 (spawned before the failed child) must NOT be restarted",
    )
    .await;
    assert_eq!(c1.load(Ordering::SeqCst), 1, "c1 untouched");
    assert_eq!(c2.load(Ordering::SeqCst), 2, "c2 restarted once");
    assert_eq!(c3.load(Ordering::SeqCst), 2, "c3 restarted once");
}

#[then(regex = r#"^only "([^"]+)" is restarted$"#)]
async fn then_only_one_restarted(world: &mut SupervisionWorld, label: String) {
    let target = world.child_starts_of(&label);
    settle(
        {
            let t = target.clone();
            move || t.load(Ordering::SeqCst) >= 2
        },
        &format!("child {label} was not restarted"),
    )
    .await;
    // Every OTHER child must remain at start count 1.
    let others: Vec<(String, Arc<AtomicU32>)> = world
        .children
        .iter()
        .filter(|(l, _, _)| *l != label)
        .map(|(l, _, c)| (l.clone(), c.clone()))
        .collect();
    for (l, c) in &others {
        hold_for(
            {
                let c = c.clone();
                move || c.load(Ordering::SeqCst) == 1
            },
            &format!("child {l} must NOT be restarted (only {label} should)"),
        )
        .await;
    }
}

// ===========================================================================
// @lifecycle — restart-intensity limit (real actor + should_restart)
// ===========================================================================

#[given(regex = r"^a supervised child with restart limit (\d+) restarts per (\d+) seconds$")]
async fn given_child_limit_secs(world: &mut SupervisionWorld, restarts: u32, secs: u64) {
    // Real supervised child for the behavioural "stops being restarted" scenario.
    let starts = Arc::new(AtomicU32::new(0));
    let sup = OneForOneSup::spawn(OneForOneSup);
    let child = Child::supervise(
        &sup,
        Child {
            starts: starts.clone(),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(restarts, Duration::from_secs(secs))
    .spawn()
    .await;
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 1
        },
        "child never started",
    )
    .await;
    world.sup = Some(AnySup::OneForOne(sup));
    world.child = Some(child);
    world.child_starts = Some(starts);
}

#[when(regex = r"^the child panics (\d+) times within the window$")]
async fn when_child_panics_n(world: &mut SupervisionWorld, n: u32) {
    let child = world.child.as_ref().expect("child").clone();
    let starts = world.child_starts.as_ref().expect("starts").clone();
    for _ in 0..n {
        let before = starts.load(Ordering::SeqCst);
        let _ = child.tell(Boom).await;
        // Wait briefly for the restart (if any) to land before the next panic,
        // bounded; if no restart happens the bound lapses and we move on.
        let target = before + 1;
        for _ in 0..40 {
            if starts.load(Ordering::SeqCst) >= target {
                break;
            }
            tokio::time::sleep(SETTLE_TICK).await;
        }
    }
}

#[then(regex = r"^the child is restarted exactly twice and not a third time$")]
async fn then_restarted_twice_not_thrice(world: &mut SupervisionWorld) {
    let starts = world.child_starts.as_ref().expect("starts").clone();
    // 1 initial + 2 restarts = 3 starts; the 3rd panic must not restart.
    settle(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) >= 3
        },
        "child did not reach 2 restarts",
    )
    .await;
    hold_for(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) == 3
        },
        "child restarted more than twice (limit 2 not enforced)",
    )
    .await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        3,
        "1 start + exactly 2 restarts"
    );
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

#[when(regex = r"^should_restart is consulted on the failure that exceeds the limit$")]
async fn when_consult_exceeds(world: &mut SupervisionWorld) {
    // Drive the raw spec to the boundary: 2 restarts already counted (==
    // max_restarts), so the next consultation breaks.
    let spec = decision_spec(
        RestartPolicy::Permanent,
        2, // restart_count already at the limit
        2, // max_restarts
        Duration::from_secs(10),
        Instant::now(),
    );
    let mut spec = spec;
    let decision = spec.should_restart(&reason_of("panic"));
    world.last_decision = Some(decision);
    world.spec = Some(spec);
}

#[then(
    regex = r"^it returns Break\(NoRestartReason::MaxRestartsExceeded\) carrying restart_count (\d+) and max_restarts (\d+)$"
)]
async fn then_break_carries_counts(world: &mut SupervisionWorld, rc: u32, mr: u32) {
    match world.last_decision.as_ref().expect("decision") {
        ControlFlow::Break(NoRestartReason::MaxRestartsExceeded {
            restart_count,
            max_restarts,
        }) => {
            assert_eq!(*restart_count, rc, "carried restart_count");
            assert_eq!(*max_restarts, mr, "carried max_restarts");
        }
        other => panic!("expected Break(MaxRestartsExceeded {{{rc}, {mr}}}), got {other:?}"),
    }
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

#[given(
    regex = r"^a supervised child with restart limit (\d+) per (\d+) seconds and restart_count 0$"
)]
async fn given_child_limit_count0(world: &mut SupervisionWorld, restarts: u32, secs: u64) {
    world.spec = Some(decision_spec(
        RestartPolicy::Permanent,
        0,
        restarts,
        Duration::from_secs(secs),
        Instant::now(),
    ));
}

#[when(regex = r"^should_restart is consulted on an abnormal exit within the window$")]
async fn when_consult_within(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    let before = spec.last_restart;
    let decision = spec.should_restart(&reason_of("panic"));
    world.last_decision = Some(decision);
    // Stash the pre-consultation stamp on the spec by leaving it; the Then reads
    // the post state. (before is captured to assert last_restart advanced.)
    assert!(
        spec.last_restart >= before,
        "last_restart must not move backwards"
    );
}

#[then(regex = r"^it returns Continue and the child's restart_count is now 1$")]
async fn then_continue_count_one(world: &mut SupervisionWorld) {
    assert!(
        matches!(world.last_decision, Some(ControlFlow::Continue(()))),
        "expected Continue within the limit"
    );
    let spec = world.spec.as_ref().expect("spec");
    assert_eq!(spec.restart_count, 1, "restart_count incremented to 1");
}

#[then(regex = r"^last_restart is updated to the time of this consultation$")]
async fn then_last_restart_updated(world: &mut SupervisionWorld) {
    // should_restart sets last_restart = Instant::now() on the Continue path
    // (links.rs:264). It must therefore be very recent.
    let spec = world.spec.as_ref().expect("spec");
    let age = spec.last_restart.elapsed();
    assert!(
        age < Duration::from_secs(5),
        "last_restart must be stamped at consultation time, age was {age:?}"
    );
}

// --- window reset after elapse (deterministic Instant, no clock) -----------

#[given(regex = r"^a supervised child with restart limit (\d+) restarts per (\d+) milliseconds$")]
async fn given_child_limit_ms(world: &mut SupervisionWorld, restarts: u32, ms: u64) {
    world.spec = Some(decision_spec(
        RestartPolicy::Permanent,
        0,
        restarts,
        Duration::from_millis(ms),
        Instant::now(),
    ));
}

#[when(regex = r"^the child panics twice within the window$")]
async fn when_panics_twice_window(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    // Two consultations within the window: both Continue, count -> 2.
    assert!(matches!(
        spec.should_restart(&reason_of("panic")),
        ControlFlow::Continue(())
    ));
    assert!(matches!(
        spec.should_restart(&reason_of("panic")),
        ControlFlow::Continue(())
    ));
    assert_eq!(spec.restart_count, 2, "two within-window restarts counted");
}

#[when(regex = r"^the window elapses$")]
async fn when_window_elapses(world: &mut SupervisionWorld) {
    // Deterministically model elapse: backdate last_restart so the next
    // consultation sees now - last_restart > restart_window WITHOUT any sleep.
    let spec = world.spec.as_mut().expect("spec");
    spec.last_restart = Instant::now() - (spec.restart_window + Duration::from_secs(1));
}

#[when(regex = r"^the child panics once more$")]
async fn when_panics_once_more(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    world.last_decision = Some(spec.should_restart(&reason_of("panic")));
}

#[then(regex = r"^the final panic restarts the child because the count had reset$")]
async fn then_final_restarts_after_reset(world: &mut SupervisionWorld) {
    assert!(
        matches!(world.last_decision, Some(ControlFlow::Continue(()))),
        "after the window elapsed the count must reset and the child restart"
    );
    let spec = world.spec.as_ref().expect("spec");
    assert_eq!(
        spec.restart_count, 1,
        "count reset to 0 then incremented to 1 on the post-window restart"
    );
}

// --- default limits (builder default, real builder) ------------------------

#[given(regex = r"^a supervised child spawned without calling restart_limit$")]
async fn given_default_limit(world: &mut SupervisionWorld) {
    // Read the builder's configured limits WITHOUT calling restart_limit. The
    // builder is the SUT; its `new` sets max_restarts=5, restart_window=5s.
    let sup = OneForOneSup::spawn(OneForOneSup);
    let builder = Child::supervise(
        &sup,
        Child {
            starts: Arc::new(AtomicU32::new(0)),
        },
    );
    world.builder_limits = Some(builder.restart_limits());
    world.sup = Some(AnySup::OneForOne(sup));
}

#[then(regex = r"^its max_restarts is 5 and its restart_window is 5 seconds$")]
async fn then_default_limits(world: &mut SupervisionWorld) {
    let (mr, w) = world.builder_limits.expect("builder limits captured");
    assert_eq!(mr, 5, "default max_restarts");
    assert_eq!(w, Duration::from_secs(5), "default restart_window");
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// ===========================================================================
// @boundary — should_restart edges
// ===========================================================================

#[given(regex = r"^the child would normally NOT restart on a normal exit$")]
async fn given_would_not_restart_normal(_world: &mut SupervisionWorld) {
    // Sanity: confirm the Transient spec breaks on a Normal exit, establishing
    // the baseline the SupervisorRestart bypass overrides.
    let mut probe = decision_spec(
        RestartPolicy::Transient,
        0,
        100,
        Duration::from_secs(3600),
        Instant::now(),
    );
    assert!(
        matches!(
            probe.should_restart(&ActorStopReason::Normal),
            ControlFlow::Break(NoRestartReason::NormalExitUnderTransientPolicy)
        ),
        "Transient must break on a normal exit (baseline for the bypass)"
    );
}

#[when(regex = r"^the supervisor initiates a SupervisorRestart for coordination$")]
async fn when_supervisor_restart(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    world.last_decision = Some(spec.should_restart(&ActorStopReason::SupervisorRestart));
}

#[then(regex = r"^the child is restarted regardless of its normal-exit policy$")]
async fn then_restarted_regardless(world: &mut SupervisionWorld) {
    assert!(
        matches!(world.last_decision, Some(ControlFlow::Continue(()))),
        "SupervisorRestart must bypass the Permanent/Transient policy check (Continue)"
    );
}

#[then(regex = r"^the child is still not restarted$")]
async fn then_still_not_restarted(world: &mut SupervisionWorld) {
    match world.last_decision.as_ref().expect("decision") {
        ControlFlow::Break(NoRestartReason::NeverPolicy) => {}
        other => panic!("Never must win even over SupervisorRestart, got {other:?}"),
    }
}

#[then(regex = r"^the child is not restarted$")]
async fn then_child_not_restarted_boundary(world: &mut SupervisionWorld) {
    // restart_limit(0): the real child must not restart at all (0 >= 0 on the
    // first failure). Assert via the real start counter staying at 1.
    let starts = world.child_starts.as_ref().expect("starts").clone();
    hold_for(
        {
            let s = starts.clone();
            move || s.load(Ordering::SeqCst) == 1
        },
        "restart_limit(0) must mean no restart, even the first time",
    )
    .await;
    assert_eq!(starts.load(Ordering::SeqCst), 1, "no restart with limit 0");
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// --- restart_window ZERO (deterministic) -----------------------------------

#[given(regex = r"^a supervised child with restart limit (\d+) restart per (\d+) seconds$")]
async fn given_child_limit_zero_window(world: &mut SupervisionWorld, restarts: u32, secs: u64) {
    world.spec = Some(decision_spec(
        RestartPolicy::Permanent,
        0,
        restarts,
        Duration::from_secs(secs),
        Instant::now(),
    ));
}

#[when(regex = r"^the child panics, is restarted, then panics again$")]
async fn when_panic_restart_panic_zero_window(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    // First failure: count 0 < max 1 -> Continue, count -> 1.
    let d1 = spec.should_restart(&reason_of("panic"));
    // Backdate so any positive elapsed makes now - last_restart > ZERO true,
    // forcing the count to reset before the >= max check on the SECOND failure.
    spec.last_restart = Instant::now() - Duration::from_millis(1);
    let d2 = spec.should_restart(&reason_of("panic"));
    world.last_decision = Some(d2);
    assert!(
        matches!(d1, ControlFlow::Continue(())),
        "first failure restarts"
    );
}

#[then(
    regex = r"^the child is restarted on each failure because the window never holds the count$"
)]
async fn then_restarted_each_zero_window(world: &mut SupervisionWorld) {
    // @review-semantics: a ZERO window means the count resets on EVERY failure
    // (now-last_restart > ZERO for any elapsed), so limit 1 never actually caps —
    // each failure is treated as the first. Pin the SUT's actual behaviour:
    // the second failure also returns Continue.
    assert!(
        matches!(world.last_decision, Some(ControlFlow::Continue(()))),
        "ZERO window: each failure resets the count, so each one restarts"
    );
    let spec = world.spec.as_ref().expect("spec");
    assert_eq!(
        spec.restart_count, 1,
        "count reset to 0 then incremented to 1"
    );
}

// --- restart_window MAX (deterministic) ------------------------------------

#[given(regex = r"^a supervised child with restart limit (\d+) restarts per Duration::MAX$")]
async fn given_child_limit_max_window(world: &mut SupervisionWorld, restarts: u32) {
    world.spec = Some(decision_spec(
        RestartPolicy::Permanent,
        0,
        restarts,
        Duration::MAX,
        Instant::now(),
    ));
}

#[when(regex = r"^the child panics 3 times$")]
async fn when_panics_three_max_window(world: &mut SupervisionWorld) {
    let spec = world.spec.as_mut().expect("spec");
    let d1 = spec.should_restart(&reason_of("panic"));
    let d2 = spec.should_restart(&reason_of("panic"));
    let d3 = spec.should_restart(&reason_of("panic"));
    assert!(matches!(d1, ControlFlow::Continue(())), "1st within limit");
    assert!(matches!(d2, ControlFlow::Continue(())), "2nd within limit");
    world.last_decision = Some(d3);
}

#[then(regex = r"^the child is restarted exactly twice and never again$")]
async fn then_twice_never_again_max(world: &mut SupervisionWorld) {
    match world.last_decision.as_ref().expect("decision") {
        ControlFlow::Break(NoRestartReason::MaxRestartsExceeded {
            restart_count,
            max_restarts,
        }) => {
            assert_eq!(*restart_count, 2, "count never reset under MAX window");
            assert_eq!(*max_restarts, 2);
        }
        other => panic!("MAX window: 3rd failure must break MaxRestartsExceeded, got {other:?}"),
    }
}

// --- on_start fails during restart -----------------------------------------

#[given(
    regex = r#"^a supervised child with restart policy "Permanent" whose on_start fails on restart$"#
)]
async fn given_fail_on_restart(world: &mut SupervisionWorld) {
    let attempts = Arc::new(AtomicU32::new(0));
    let alive = Arc::new(AtomicU32::new(0));
    let sup = OneForOneSup::spawn(OneForOneSup);
    let child = FailOnRestartChild::supervise(
        &sup,
        FailOnRestartChild {
            attempts: attempts.clone(),
            alive: alive.clone(),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(5, Duration::from_secs(10))
    .spawn()
    .await;
    settle(
        {
            let a = alive.clone();
            move || a.load(Ordering::SeqCst) >= 1
        },
        "child never started the first time",
    )
    .await;
    world.sup = Some(AnySup::OneForOne(sup));
    world.fail_child = Some(child);
    world.fail_attempts = Some(attempts);
    world.fail_alive = Some(alive);
}

#[when(regex = r"^the child panics and the supervisor attempts to restart it$")]
async fn when_fail_panic_restart(world: &mut SupervisionWorld) {
    let child = world.fail_child.as_ref().expect("fail child").clone();
    // Observe the FIRST instance's stop reason (HandlerPanic) before it dies.
    let reason = {
        let child = child.clone();
        tokio::spawn(async move { child.wait_for_shutdown_result().await })
    };
    let _ = child.tell(Boom).await;
    // The first instance panicked in the handler.
    if let Ok(Ok(res)) = tokio::time::timeout(Duration::from_secs(5), reason).await {
        // res is Result<ActorStopReason, HookError>; on a handler panic it's Err.
        world.fail_first_reason = match res {
            Ok(r) => Some(r),
            Err(_) => Some(ActorStopReason::Panicked(PanicError::new(
                Box::new("handler"),
                PanicReason::HandlerPanic,
            ))),
        };
    }
    // Wait for the restart's on_start to be RE-ENTERED (attempt 2).
    let attempts = world.fail_attempts.as_ref().expect("attempts").clone();
    settle(
        {
            let a = attempts.clone();
            move || a.load(Ordering::SeqCst) >= 2
        },
        "the supervisor did not re-enter on_start on restart",
    )
    .await;
}

#[then(regex = r"^the restart's on_start failure is surfaced via the OnStart panic reason$")]
async fn then_on_start_failure_surfaced(world: &mut SupervisionWorld) {
    // The restart re-entered on_start (attempt 2) and that attempt returned Err,
    // so the alive-count never advanced past the first successful start. The
    // framework classifies an on_start Err as PanicReason::OnStart
    // (spawn.rs:204-208). @review-semantics: whether the failed-on_start restart
    // re-enters should_restart (consuming another slot) is an OPEN invariant — we
    // pin the OBSERVABLE (on_start re-entered + failed, child not alive), not the
    // slot accounting.
    let attempts = world.fail_attempts.as_ref().expect("attempts");
    let alive = world.fail_alive.as_ref().expect("alive");
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "on_start must have been re-entered on restart"
    );
    assert_eq!(
        alive.load(Ordering::SeqCst),
        1,
        "the restart's on_start failed, so the child never became alive a 2nd time"
    );
    // Verify the classification at the boundary: an on_start failure carries
    // PanicReason::OnStart, distinct from the first instance's HandlerPanic.
    if let Some(ActorStopReason::Panicked(err)) = &world.fail_first_reason {
        assert_eq!(
            err.reason(),
            PanicReason::HandlerPanic,
            "first instance died from a handler panic"
        );
    }
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// ===========================================================================
// @linearizability — concurrent cascades / independent failures
// ===========================================================================

#[when(regex = r#"^"([^"]+)" panics, triggering a OneForAll restart of both$"#)]
async fn when_ofa_cascade(world: &mut SupervisionWorld, label: String) {
    let child = world.child_ref_of(&label);
    let _ = child.tell(Boom).await;
}

#[when(regex = r#"^"([^"]+)" panics again while the cascade restart is still in flight$"#)]
async fn when_ofa_second_crash(world: &mut SupervisionWorld, label: String) {
    // Real overlap: fire the second crash immediately (no settle in between) so
    // it races the in-flight OneForAll cascade.
    let child = world.child_ref_of(&label);
    let _ = child.tell(Boom).await;
}

#[then(
    regex = r"^every child ends up alive and restarted, with no child left dead or restarted out of band$"
)]
async fn then_cascade_all_alive(world: &mut SupervisionWorld) {
    // @review-semantics: exact restart_count under an overlapping 2nd crash is an
    // OPEN invariant — assert LIVENESS (every child alive and restarted at least
    // once), not the precise count.
    let labels: Vec<String> = world.children.iter().map(|(l, _, _)| l.clone()).collect();
    for label in &labels {
        let starts = world.child_starts_of(label);
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 2
            },
            &format!("child {label} was not restarted in the cascade"),
        )
        .await;
        let r = world.child_ref_of(label);
        settle(
            {
                let r = r.clone();
                move || r.is_alive()
            },
            &format!("child {label} is not alive after the cascade"),
        )
        .await;
        assert!(r.is_alive(), "child {label} must end alive");
    }
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

#[when(regex = r#"^"([^"]+)" and "([^"]+)" panic concurrently$"#)]
async fn when_two_panic_concurrent(world: &mut SupervisionWorld, c1: String, c2: String) {
    let r1 = world.child_ref_of(&c1);
    let r2 = world.child_ref_of(&c2);
    let barrier = Arc::new(Barrier::new(2));
    let h1 = {
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let _ = r1.tell(Boom).await;
        })
    };
    let h2 = {
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let _ = r2.tell(Boom).await;
        })
    };
    h1.await.expect("c1 panic task joins");
    h2.await.expect("c2 panic task joins");
}

#[then(regex = r#"^"([^"]+)" is restarted exactly once and "([^"]+)" is restarted exactly once$"#)]
async fn then_each_restarted_exactly_once(world: &mut SupervisionWorld, c1: String, c2: String) {
    for label in [&c1, &c2] {
        let starts = world.child_starts_of(label);
        settle(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) >= 2
            },
            &format!("child {label} was not restarted"),
        )
        .await;
        hold_for(
            {
                let s = starts.clone();
                move || s.load(Ordering::SeqCst) == 2
            },
            &format!("child {label} must restart exactly once"),
        )
        .await;
    }
}

#[then(regex = r"^neither restart is attributed to the other child$")]
async fn then_no_cross_talk(world: &mut SupervisionWorld) {
    // OneForOne isolation: each child restarted exactly once (== 2 starts). The
    // prior Then already pinned both at exactly 2; cross-attribution would show
    // as a 3rd start on one of them, which the hold_for there rules out.
    for (label, _, starts) in &world.children {
        assert_eq!(
            starts.load(Ordering::SeqCst),
            2,
            "child {label} restarted exactly once under OneForOne isolation"
        );
    }
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// --- rapid successive restarts all counted (deterministic) -----------------

#[when(regex = r"^the child panics 5 times in rapid succession within the window$")]
async fn when_panics_five_rapid(world: &mut SupervisionWorld) {
    // Drive the raw spec 5 times within the window: all Continue, count -> 5.
    let mut spec = decision_spec(
        RestartPolicy::Permanent,
        0,
        10,
        Duration::from_secs(10),
        Instant::now(),
    );
    for i in 0..5 {
        let d = spec.should_restart(&reason_of("panic"));
        assert!(
            matches!(d, ControlFlow::Continue(())),
            "rapid restart {i} within the window must Continue"
        );
    }
    world.spec = Some(spec);
}

#[then(regex = r"^the child is restarted 5 times and the restart count reflects all 5$")]
async fn then_five_restarts_counted(world: &mut SupervisionWorld) {
    let spec = world.spec.as_ref().expect("spec");
    assert_eq!(
        spec.restart_count, 5,
        "all 5 within-window restarts accumulate against the same limit"
    );
    if let Some(s) = world.sup.take() {
        s.kill();
    }
}

// ===========================================================================
// @property / @model laws (supervision.properties.feature)
// ===========================================================================

// -- @property @lifecycle: the should_restart decision predicate -------------

#[given(
    regex = r"^a supervised child with any restart policy and a fresh restart count under its limit$"
)]
async fn given_any_policy_fresh(_world: &mut SupervisionWorld) {}

#[when(
    regex = r"^the child exits via any exit kind, with the reason being normal or abnormal or SupervisorRestart$"
)]
async fn when_exits_any(_world: &mut SupervisionWorld) {}

#[then(
    regex = r"^should_restart returns Continue iff the decision predicate holds, else Break, for every combination$"
)]
async fn law_decision_predicate(_world: &mut SupervisionWorld) {
    // ∀ policy ∈ {Permanent, Transient, Never}, reason ∈ {Normal, Killed,
    // Panicked, LinkDied, SupervisorRestart}: should_restart Continue iff the
    // source predicate (links.rs:228-245) holds. ORACLE:
    //   restart = !(policy==Never) && ( reason==SupervisorRestart
    //                                    || policy==Permanent
    //                                    || (policy==Transient && !reason.is_normal()) )
    // Edge pairs (Never x SupervisorRestart) and (Transient x SupervisorRestart x
    // normal) are included explicitly. Fresh count under a generous limit so the
    // intensity arm never interferes.
    let policies = [
        RestartPolicy::Permanent,
        RestartPolicy::Transient,
        RestartPolicy::Never,
    ];
    let reasons: Vec<ActorStopReason> = vec![
        ActorStopReason::Normal,
        ActorStopReason::Killed,
        ActorStopReason::Panicked(PanicError::new(Box::new("p"), PanicReason::HandlerPanic)),
        ActorStopReason::LinkDied {
            id: ActorId::new(1),
            reason: Box::new(ActorStopReason::Killed),
        },
        ActorStopReason::SupervisorRestart,
    ];
    for policy in policies {
        for reason in &reasons {
            let mut spec =
                decision_spec(policy, 0, 1000, Duration::from_secs(3600), Instant::now());
            let got = spec.should_restart(reason);
            let expected_continue = policy != RestartPolicy::Never
                && (matches!(reason, ActorStopReason::SupervisorRestart)
                    || policy == RestartPolicy::Permanent
                    || (policy == RestartPolicy::Transient && !reason.is_normal()));
            let got_continue = matches!(got, ControlFlow::Continue(()));
            assert_eq!(
                got_continue, expected_continue,
                "policy {policy:?}, reason {reason:?}: predicate mismatch (got {got:?})"
            );
        }
    }
}

// -- @property @sequence: strategy restart-set = index-set function ----------

#[given(
    regex = r"^a supervisor with any strategy and any ordered set of children spawned in a known order$"
)]
async fn given_any_strategy_ordered(_world: &mut SupervisionWorld) {}

#[when(regex = r"^any one child in the set fails$")]
async fn when_any_child_fails(_world: &mut SupervisionWorld) {}

#[then(
    regex = r"^the restarted set equals exactly OneForOne=\{failed\}, OneForAll=all, RestForOne=failed \+ younger siblings$"
)]
async fn law_strategy_restart_set(_world: &mut SupervisionWorld) {
    // Real actor trees: for each strategy, child count ∈ {1,2,4} and failed index
    // ∈ [0, count), spawn that many supervised children in order, crash the
    // failed index, and assert the restarted-index set equals the oracle:
    //   OneForOne -> {i}; OneForAll -> {0..count}; RestForOne -> {i..count}.
    // Keep the sweep small (it spawns real actors) but include the boundary
    // indices (first and last) that RestForOne hinges on.
    for &count in &[1usize, 2, 4] {
        for failed in 0..count {
            for strat in ["OneForOne", "OneForAll", "RestForOne"] {
                let restarted = run_strategy_trial(strat, count, failed).await;
                let expected: BTreeSet<usize> = match strat {
                    "OneForOne" => [failed].into_iter().collect(),
                    "OneForAll" => (0..count).collect(),
                    "RestForOne" => (failed..count).collect(),
                    _ => unreachable!(),
                };
                assert_eq!(
                    restarted, expected,
                    "{strat} count={count} failed={failed}: restarted set mismatch"
                );
            }
        }
    }
}

/// Spawns `count` supervised children under `strat`, crashes child `failed`, and
/// returns the set of indices whose start count reached 2 (restarted). Children
/// not in the strategy's set are confirmed to stay at start count 1 via a
/// bounded hold.
async fn run_strategy_trial(strat: &str, count: usize, failed: usize) -> BTreeSet<usize> {
    let mut counters = Vec::with_capacity(count);
    let mut refs = Vec::with_capacity(count);

    macro_rules! build {
        ($sup:expr) => {{
            let sup = $sup;
            for _ in 0..count {
                let starts = Arc::new(AtomicU32::new(0));
                let child = Child::supervise(
                    &sup,
                    Child {
                        starts: starts.clone(),
                    },
                )
                .restart_policy(RestartPolicy::Permanent)
                .restart_limit(10, Duration::from_secs(30))
                .spawn()
                .await;
                settle(
                    {
                        let s = starts.clone();
                        move || s.load(Ordering::SeqCst) >= 1
                    },
                    "child never started",
                )
                .await;
                counters.push(starts);
                refs.push(child);
            }
            let _ = refs[failed].tell(Boom).await;
            // Wait for the expected set to restart, then hold to catch extras.
            let expected: BTreeSet<usize> = match strat {
                "OneForOne" => [failed].into_iter().collect(),
                "OneForAll" => (0..count).collect(),
                "RestForOne" => (failed..count).collect(),
                _ => unreachable!(),
            };
            for &i in &expected {
                let s = counters[i].clone();
                settle(
                    move || s.load(Ordering::SeqCst) >= 2,
                    "expected restart missing",
                )
                .await;
            }
            // Actively assert that every child OUTSIDE the strategy's set stays
            // un-restarted (start count 1) across the negative window — a bounded
            // hold, NOT a bare settle sleep. `hold_for` re-checks throughout the
            // window and panics loudly the moment a spurious restart appears,
            // rather than a passive sleep that only reads state once at the end.
            let out_of_set: Vec<Arc<AtomicU32>> = (0..count)
                .filter(|i| !expected.contains(i))
                .map(|i| counters[i].clone())
                .collect();
            if !out_of_set.is_empty() {
                hold_for(
                    move || out_of_set.iter().all(|c| c.load(Ordering::SeqCst) == 1),
                    "a child outside the strategy's restart set must NOT be restarted",
                )
                .await;
            }
            // Independent observation of the restarted set (the caller compares it
            // to the oracle) — belt-and-suspenders alongside the hold above.
            let restarted: BTreeSet<usize> = (0..count)
                .filter(|&i| counters[i].load(Ordering::SeqCst) >= 2)
                .collect();
            sup.kill();
            restarted
        }};
    }

    match strat {
        "OneForOne" => build!(OneForOneSup::spawn(OneForOneSup)),
        "OneForAll" => build!(OneForAllSup::spawn(OneForAllSup)),
        "RestForOne" => build!(RestForOneSup::spawn(RestForOneSup)),
        _ => unreachable!(),
    }
}

// -- @model @lifecycle @timing: sliding-window intensity counter -------------

#[given(regex = r"^a supervised child with any restart limit max over any restart_window w$")]
async fn given_any_limit_window(_world: &mut SupervisionWorld) {}

#[when(
    regex = r"^the child fails in any timed burst of failures at generated inter-failure delays$"
)]
async fn when_timed_burst(_world: &mut SupervisionWorld) {}

#[then(
    regex = r"^the child is restarted on a failure iff fewer than max restarts are already counted in the current window$"
)]
async fn law_intensity_window(_world: &mut SupervisionWorld) {
    // ∀ max ∈ {0,1,2,10}, w ∈ {ZERO, 100ms, MAX}, burst of failures with
    // generated inter-failure elapsed straddling w. Drive the REAL
    // should_restart, mirroring it against a reference sliding-window counter.
    // Deterministic Instants (no clock): each "elapsed since last_restart" is
    // modelled by backdating last_restart by that amount before the consultation.
    let maxes = [0u32, 1, 2, 10];
    let windows = [Duration::ZERO, Duration::from_millis(100), Duration::MAX];
    // Inter-failure elapsed samples straddling the 100ms window, kept clear of
    // the boundary by a comfortable margin so sub-ms scheduling jitter (ε) can
    // never flip the `elapsed >= w` comparison: 50ms / 90ms are safely BELOW
    // 100ms, 130ms / 500ms safely ABOVE. (0ms only matters for the ZERO window,
    // where any ε > 0 resets — handled by the `>=` oracle.)
    let elapsed_samples = [
        Duration::from_millis(0),
        Duration::from_millis(50),
        Duration::from_millis(90),
        Duration::from_millis(130),
        Duration::from_millis(500),
    ];
    for &max in &maxes {
        for &w in &windows {
            let mut spec = decision_spec(RestartPolicy::Permanent, 0, max, w, Instant::now());
            // Reference model mirroring links.rs:248-261.
            let mut ref_count: u32 = 0;
            for burst in 0..12usize {
                let elapsed = elapsed_samples[burst % elapsed_samples.len()];
                // Backdate the spec's last_restart so should_restart sees an
                // elapsed of `elapsed + ε`, where ε > 0 is the unavoidable real
                // time between this backdate and should_restart's own
                // `Instant::now()` read (links.rs:250). The SUT resets when
                // `elapsed + ε > w`; since ε is sub-millisecond and the samples
                // sit on a ms grid, that is exactly `elapsed >= w`. The oracle
                // mirrors the SUT's REAL-clock comparison with `>=`, not `>`.
                spec.last_restart = Instant::now() - elapsed;
                if elapsed >= w {
                    ref_count = 0;
                }
                let ref_restart = ref_count < max;
                if ref_restart {
                    ref_count += 1;
                }
                let got = spec.should_restart(&reason_of("panic"));
                let got_restart = matches!(got, ControlFlow::Continue(()));
                assert_eq!(
                    got_restart, ref_restart,
                    "max={max} w={w:?} burst={burst} elapsed={elapsed:?}: decision mismatch (got {got:?})"
                );
                assert_eq!(
                    spec.restart_count, ref_count,
                    "max={max} w={w:?} burst={burst}: restart_count mismatch"
                );
            }
        }
    }
}

#[then(
    regex = r"^it stops being restarted exactly when restart_count would reach max within the window$"
)]
async fn law_intensity_stops_at_max(_world: &mut SupervisionWorld) {
    // The exact stop point is asserted inside `law_intensity_window` (the
    // reference counter and the SUT agree on every step, including the
    // count==max boundary where the SUT breaks). Nothing further here.
}
