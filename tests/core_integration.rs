//! Cross-module integration scenarios for the surviving local core (card #87).
//!
//! #77 tests each core module (supervision, links, registry, mailbox, …) in
//! ISOLATION with one focused `World` per module. A regression that only manifests
//! in the INTERACTION between subsystems would slip through. These end-to-end tests
//! wire several subsystems into a single flow and assert the cross-cutting
//! invariants, driven entirely through the public `bombay::prelude` API against
//! REAL SPAWNED ACTORS.
//!
//! Coverage:
//!   1. supervision × mailbox × concurrency — a supervised child under concurrent
//!      message load loses no message and duplicates none across a restart.
//!   2. supervision × registry — a registered child stays resolvable and alive
//!      across a restart (the registry entry tracks the restarted instance).
//!   3. links × mailbox — a linked watcher's `on_link_died` fires for a dying peer
//!      while the watcher keeps draining its own mailbox under load.
//!   4. supervision strategy × mailbox — a `OneForAll` cascade restarts every
//!      sibling, and an in-flight message to a non-failing sibling still lands.
//!   5. supervision strategy — a `RestForOne` cascade restarts the failed child
//!      and those started after it, but not those started before.
//!
//! Timing discipline (mirrors the #77 supervision wiring): every observation uses a
//! bounded condition-based `settle()` that panics loudly — never an unbounded await
//! on a restart that may never happen. Message-conservation tests loop because the
//! original mailbox-drop bug (tqwewe/kameo#335) was non-deterministic.

use std::{
    collections::BTreeSet,
    ops::ControlFlow,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use bombay::{
    actor::{ActorId, WeakActorRef},
    error::{ActorStopReason, Infallible},
    prelude::*,
    supervision::{RestartPolicy, SupervisionStrategy},
};
use tokio::sync::Barrier;

// ===========================================================================
// Bounded settle — never an unbounded await on a restart that may never happen.
// ===========================================================================

const SETTLE_STEPS: usize = 600;
const SETTLE_TICK: Duration = Duration::from_millis(5);

async fn settle<F: FnMut() -> bool>(mut cond: F, msg: &str) {
    for _ in 0..SETTLE_STEPS {
        if cond() {
            return;
        }
        tokio::time::sleep(SETTLE_TICK).await;
    }
    panic!("condition did not settle within the bound: {msg}");
}

/// Process-unique registry names so the parallel tests never collide on the
/// process-global `ACTOR_REGISTRY`.
static NAME_SEQ: AtomicU64 = AtomicU64::new(0);
fn unique_name(prefix: &str) -> String {
    format!("itest-{prefix}-{}", NAME_SEQ.fetch_add(1, Ordering::SeqCst))
}

// ===========================================================================
// Supervisors (strategy fixed per type, read from the `supervision_strategy` hook)
// ===========================================================================

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

// ===========================================================================
// Worker child: counts starts (a restart re-runs `on_start`) and records every
// `Work(n)` it handles. `Boom` sleeps briefly then panics, so messages sent right
// after it are guaranteed to be queued behind it when the panic fires.
// ===========================================================================

#[derive(Clone)]
struct Worker {
    starts: Arc<AtomicU32>,
    seen: Arc<Mutex<Vec<u64>>>,
}

impl Actor for Worker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        state.starts.fetch_add(1, Ordering::SeqCst);
        Ok(state)
    }
}

struct Work(u64);
impl Message<Work> for Worker {
    type Reply = ();
    async fn handle(&mut self, Work(n): Work, _: &mut Context<Self, Self::Reply>) {
        self.seen.lock().unwrap().push(n);
    }
}

struct Boom;
impl Message<Boom> for Worker {
    type Reply = ();
    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) {
        tokio::time::sleep(Duration::from_millis(30)).await;
        panic!("worker boom");
    }
}

fn spawn_worker() -> (Worker, Arc<AtomicU32>, Arc<Mutex<Vec<u64>>>) {
    let starts = Arc::new(AtomicU32::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    (
        Worker {
            starts: Arc::clone(&starts),
            seen: Arc::clone(&seen),
        },
        starts,
        seen,
    )
}

// ===========================================================================
// 1. supervision × mailbox × concurrency — no message lost or duplicated
//    across a restart triggered while a concurrent producer load is queued.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn supervised_child_conserves_messages_across_restart_under_load() {
    const ITERATIONS: usize = 20;
    const N: u64 = 60;
    const PRODUCERS: u64 = 6;

    for iter in 0..ITERATIONS {
        let sup = OneForOneSup::spawn(OneForOneSup);
        let (worker, starts, seen) = spawn_worker();
        let child = Worker::supervise(&sup, worker)
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(10, Duration::from_secs(30))
            .spawn()
            .await;
        settle(
            {
                let s = Arc::clone(&starts);
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;

        // Enqueue the panic FIRST (its handler sleeps 30ms), then blast N distinct
        // Work messages concurrently so they queue behind the in-flight Boom and
        // must survive the restart.
        child.tell(Boom).await.expect("enqueue boom");
        let barrier = Arc::new(Barrier::new(PRODUCERS as usize));
        let per = N / PRODUCERS;
        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let child = child.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for k in 0..per {
                    let n = p * per + k;
                    child.tell(Work(n)).await.expect("enqueue work");
                }
            }));
        }
        for h in handles {
            h.await.expect("producer join");
        }

        // Every one of the N distinct messages must eventually be handled exactly once.
        settle(
            {
                let seen = Arc::clone(&seen);
                move || seen.lock().unwrap().len() >= N as usize
            },
            "not every message was handled after the restart (some were dropped)",
        )
        .await;
        let got = seen.lock().unwrap().clone();
        let unique: BTreeSet<u64> = got.iter().copied().collect();
        assert_eq!(
            unique.len(),
            N as usize,
            "iter {iter}: expected {N} distinct messages, got {} ({got:?})",
            unique.len()
        );
        assert_eq!(
            got.len(),
            N as usize,
            "iter {iter}: a message was duplicated across the restart ({got:?})"
        );
        assert_eq!(
            unique,
            (0..N).collect::<BTreeSet<u64>>(),
            "iter {iter}: the handled set is not exactly 0..{N}"
        );
        // The child restarted exactly once (one panic).
        assert_eq!(
            starts.load(Ordering::SeqCst),
            2,
            "iter {iter}: the child must have restarted exactly once"
        );
        sup.kill();
    }
}

// ===========================================================================
// 2. supervision × registry — the registry entry stays resolvable and alive
//    across a child restart (it tracks the restarted instance).
// ===========================================================================

/// A worker that registers itself under a name in `on_start` (re-running on every
/// restart). A re-registration of an existing name is a benign no-op, so the
/// `NameAlreadyRegistered` error is ignored — `on_start` must still succeed.
#[derive(Clone)]
struct RegWorker {
    starts: Arc<AtomicU32>,
    name: String,
}

impl Actor for RegWorker {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        state.starts.fetch_add(1, Ordering::SeqCst);
        let _ = actor_ref.register(state.name.clone());
        Ok(state)
    }
}

impl Message<Boom> for RegWorker {
    type Reply = ();
    async fn handle(&mut self, _: Boom, _: &mut Context<Self, Self::Reply>) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        panic!("reg worker boom");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_entry_consistent_across_child_restart() {
    let name = unique_name("reg");
    let sup = OneForOneSup::spawn(OneForOneSup);
    let starts = Arc::new(AtomicU32::new(0));
    let child = RegWorker::supervise(
        &sup,
        RegWorker {
            starts: Arc::clone(&starts),
            name: name.clone(),
        },
    )
    .restart_policy(RestartPolicy::Permanent)
    .restart_limit(10, Duration::from_secs(30))
    .spawn()
    .await;
    settle(
        {
            let s = Arc::clone(&starts);
            move || s.load(Ordering::SeqCst) >= 1
        },
        "reg worker never started",
    )
    .await;

    // Before the restart: the name resolves to an alive actor.
    let before = ActorRef::<RegWorker>::lookup(name.as_str())
        .expect("lookup must not error")
        .expect("the name must resolve before the restart");
    assert!(before.is_alive(), "the registered actor must be alive");

    // Restart it.
    child.tell(Boom).await.expect("enqueue boom");
    settle(
        {
            let s = Arc::clone(&starts);
            move || s.load(Ordering::SeqCst) >= 2
        },
        "the registered child never restarted",
    )
    .await;

    // After the restart: the SAME name still resolves to an alive actor.
    settle(
        {
            let name = name.clone();
            move || {
                ActorRef::<RegWorker>::lookup(name.as_str())
                    .ok()
                    .flatten()
                    .is_some_and(|r| r.is_alive())
            }
        },
        "the registry entry did not stay consistent across the restart",
    )
    .await;

    sup.kill();
    // Don't leak the process-global registry entry to other tests.
    let _ = bombay::registry::ACTOR_REGISTRY
        .lock()
        .unwrap()
        .remove(name.as_str());
}

// ===========================================================================
// 3. links × mailbox — a linked watcher's `on_link_died` fires for a dying peer
//    while the watcher keeps draining its own mailbox under concurrent load.
// ===========================================================================

#[derive(Clone)]
struct Watcher {
    deaths: Arc<Mutex<Vec<ActorId>>>,
    processed: Arc<AtomicU32>,
}

impl Actor for Watcher {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }

    async fn on_link_died(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        id: ActorId,
        _reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        self.deaths.lock().unwrap().push(id);
        // The watcher observes the death but keeps running.
        Ok(ControlFlow::Continue(()))
    }
}

struct Tick;
impl Message<Tick> for Watcher {
    type Reply = ();
    async fn handle(&mut self, _: Tick, _: &mut Context<Self, Self::Reply>) {
        self.processed.fetch_add(1, Ordering::SeqCst);
    }
}

struct Victim;
impl Actor for Victim {
    type Args = Self;
    type Error = Infallible;
    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn link_death_notification_fires_while_watcher_drains_mailbox() {
    const ITERATIONS: usize = 20;
    const TICKS: u32 = 50;

    for iter in 0..ITERATIONS {
        let deaths = Arc::new(Mutex::new(Vec::new()));
        let processed = Arc::new(AtomicU32::new(0));
        let watcher = Watcher::spawn(Watcher {
            deaths: Arc::clone(&deaths),
            processed: Arc::clone(&processed),
        });
        let victim = Victim::spawn(Victim);
        let victim_id = victim.id();
        watcher.link(&victim).await;

        // Concurrently load the watcher's mailbox and kill the linked victim.
        let load = {
            let watcher = watcher.clone();
            tokio::spawn(async move {
                for _ in 0..TICKS {
                    watcher.tell(Tick).await.expect("tick");
                }
            })
        };
        victim.kill();
        load.await.expect("load join");

        // The watcher observed exactly the victim's death...
        settle(
            {
                let deaths = Arc::clone(&deaths);
                move || !deaths.lock().unwrap().is_empty()
            },
            "on_link_died never fired for the killed linked peer",
        )
        .await;
        let observed = deaths.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![victim_id],
            "iter {iter}: the watcher must observe exactly the victim's death once"
        );

        // ...and still drained all of its own messages.
        settle(
            {
                let processed = Arc::clone(&processed);
                move || processed.load(Ordering::SeqCst) >= TICKS
            },
            "the watcher stopped draining its mailbox after the link death",
        )
        .await;
        assert_eq!(
            processed.load(Ordering::SeqCst),
            TICKS,
            "iter {iter}: the watcher must process every Tick despite the link death"
        );
        watcher.kill();
    }
}

// ===========================================================================
// 4. supervision strategy × mailbox — OneForAll restarts every sibling, and an
//    in-flight message to a non-failing sibling still lands.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn one_for_all_restarts_all_siblings_and_preserves_inflight() {
    let sup = OneForAllSup::spawn(OneForAllSup);
    let mut children = Vec::new();
    let mut seens = Vec::new();
    let mut starts_all = Vec::new();
    for _ in 0..3 {
        let (worker, starts, seen) = spawn_worker();
        let child = Worker::supervise(&sup, worker)
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(10, Duration::from_secs(30))
            .spawn()
            .await;
        settle(
            {
                let s = Arc::clone(&starts);
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        children.push(child);
        seens.push(seen);
        starts_all.push(starts);
    }

    // An in-flight message to a non-failing sibling, then panic child 0.
    children[1].tell(Work(7)).await.expect("inflight work");
    children[0].tell(Boom).await.expect("boom");

    // OneForAll: every sibling restarts (start count reaches 2).
    for (i, starts) in starts_all.iter().enumerate() {
        settle(
            {
                let s = Arc::clone(starts);
                move || s.load(Ordering::SeqCst) >= 2
            },
            "a sibling was not restarted under OneForAll",
        )
        .await;
        assert_eq!(
            starts.load(Ordering::SeqCst),
            2,
            "child {i} must restart exactly once under OneForAll"
        );
    }

    // The in-flight message to the non-failing sibling survived the cascade.
    settle(
        {
            let seen = Arc::clone(&seens[1]);
            move || seen.lock().unwrap().contains(&7)
        },
        "the in-flight message to a non-failing sibling was lost across the OneForAll restart",
    )
    .await;

    sup.kill();
}

// ===========================================================================
// 5. supervision strategy — RestForOne restarts the failed child and those
//    started after it, but not those started before.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn rest_for_one_restarts_failed_and_later_siblings_only() {
    let sup = RestForOneSup::spawn(RestForOneSup);
    let mut children = Vec::new();
    let mut starts_all = Vec::new();
    for _ in 0..3 {
        let (worker, starts, _seen) = spawn_worker();
        let child = Worker::supervise(&sup, worker)
            .restart_policy(RestartPolicy::Permanent)
            .restart_limit(10, Duration::from_secs(30))
            .spawn()
            .await;
        settle(
            {
                let s = Arc::clone(&starts);
                move || s.load(Ordering::SeqCst) >= 1
            },
            "child never started",
        )
        .await;
        children.push(child);
        starts_all.push(starts);
    }

    // Panic the MIDDLE child: RestForOne restarts it + child 2 (started after),
    // but not child 0 (started before).
    children[1].tell(Boom).await.expect("boom");

    settle(
        {
            let s = Arc::clone(&starts_all[1]);
            move || s.load(Ordering::SeqCst) >= 2
        },
        "the failed child was not restarted under RestForOne",
    )
    .await;
    settle(
        {
            let s = Arc::clone(&starts_all[2]);
            move || s.load(Ordering::SeqCst) >= 2
        },
        "the later sibling was not restarted under RestForOne",
    )
    .await;

    // Child 0 (started before the failed one) must NOT restart. Give any spurious
    // restart time to manifest, then assert it stayed put.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        starts_all[0].load(Ordering::SeqCst),
        1,
        "the earlier sibling must NOT restart under RestForOne"
    );
    assert_eq!(
        starts_all[1].load(Ordering::SeqCst),
        2,
        "the failed child must restart exactly once"
    );
    assert_eq!(
        starts_all[2].load(Ordering::SeqCst),
        2,
        "the later sibling must restart exactly once"
    );

    sup.kill();
}
