//! Bounded-stash behavior through the `caps` surface (ADR-0022 semantics
//! ported to the `Stashing` capability, card #279): replay order vs the
//! mailbox backlog, snapshot semantics end to end, stop-mode fates, restart
//! hygiene, and the one-queue livelock re-proof. Every terminal await is
//! bounded. Successor to the removed `tests/stash.rs`.

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use tokio::time::timeout;

use bombay::{
    actor::{
        ActorRef, Flow, PreparedActor, SpawnConfig, SpawnSupervised, Supervisor, Watch,
        WeakActorRef,
    },
    caps::{self, Ctx, Replay, Shell},
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    restart::{RestartConfig, RestartPolicy},
    test_support::{set_supervisor_rng_seed, terminate_bound},
};

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

/// The coffee-shop actor: `Item`s are stashed until `Open` flips the state;
/// serves are recorded on an external tape so post-mortem asserts work after
/// any stop mode.
struct Gate {
    open: bool,
    tape: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug, bombay_macros::Msg)]
enum GateMsg {
    Open,
    Item(u32),
    /// Served like `Item`, then stops the actor (mid-batch stop probe).
    ItemThenStop(u32),
    Read(ReplySender<Vec<u32>>),
    /// Panics the handler (restart probe).
    Boom,
}

/// The gate's cap set: bounded deferral only. `#[derive(Provide)]` emits both
/// the `Provide` access seam and the `Replay` loop hook.
#[derive(bombay_macros::Provide)]
struct GateCaps {
    stash: caps::Stashing<GateMsg>,
}

struct GatePolicy;

impl caps::StashPolicy<Gate> for GatePolicy {
    fn capacity(_: &<Gate as caps::Actor>::Args) -> Capacity {
        cap(8)
    }
}

impl caps::CapSet<Gate> for GateCaps {
    fn build(args: &<Gate as caps::Actor>::Args) -> Self {
        Self {
            stash: caps::Stashing::bounded(<GatePolicy as caps::StashPolicy<Gate>>::capacity(args)),
        }
    }
}

impl caps::Actor for Gate {
    type Msg = GateMsg;
    type Args = Arc<Mutex<Vec<u32>>>;
    type Error = Infallible;
    type Caps = GateCaps;

    async fn init(tape: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { open: false, tape })
    }

    async fn handle(&mut self, msg: GateMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        match msg {
            GateMsg::Open => {
                self.open = true;
                cx.cap::<caps::Stashing<GateMsg>>().unstash_all();
            }
            GateMsg::Boom => panic!("gate boom"),
            item @ (GateMsg::Item(_) | GateMsg::ItemThenStop(_)) if !self.open => {
                cx.cap::<caps::Stashing<GateMsg>>()
                    .stash(item)
                    .expect("test stash sized for the scenario");
            }
            GateMsg::Item(n) => self.tape.lock().expect("tape").push(n),
            GateMsg::ItemThenStop(n) => {
                self.tape.lock().expect("tape").push(n);
                return Ok(Flow::Stop);
            }
            GateMsg::Read(reply) => drop(reply.send(self.tape.lock().expect("tape").clone())),
        }
        Ok(Flow::Continue)
    }
}

fn tape() -> Arc<Mutex<Vec<u32>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn read(tape: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    tape.lock().expect("tape").clone()
}

/// Spawns a `Gate` with the message sequence pre-queued before the loop
/// starts — a deterministic mailbox, no racing sends. Runs on the caps `Shell`
/// adapter (`caps::Handle<Gate>` = `ActorRef<Shell<Gate>>`).
fn spawn_prequeued(msgs: Vec<GateMsg>, tape: Arc<Mutex<Vec<u32>>>) -> caps::Handle<Gate> {
    let prepared = PreparedActor::<Shell<Gate>>::new(SpawnConfig {
        capacity: cap(16),
        ..SpawnConfig::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    for msg in msgs {
        actor_ref
            .mailbox_sender()
            .try_send_message(msg)
            .expect("pre-queue fits the mailbox");
    }
    let _join = prepared.spawn(tape);
    actor_ref
}

/// Invariant 3 — the load-bearing ordering test. Queue [1, 2, Open, 3]:
/// 1 and 2 are stashed; Open unstashes; 3 sits in the mailbox backlog behind
/// Open. Replay runs in-step, so serves land [1, 2, 3]. A tail-reinject
/// implementation would serve [3, 1, 2]; a FIFO-breaking stash would permute
/// 1 and 2. Fails on either.
#[tokio::test]
async fn replay_runs_before_backlog_in_arrival_order() {
    let t = tape();
    let actor_ref = spawn_prequeued(
        vec![
            GateMsg::Item(1),
            GateMsg::Item(2),
            GateMsg::Open,
            GateMsg::Item(3),
        ],
        Arc::clone(&t),
    );
    let seen = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref
                .ask(GateMsg::Read)
                .await
                .expect("alive while draining");
            if seen.len() >= 3 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all three serves within the bound");
    assert_eq!(seen, vec![1, 2, 3], "stash replays first, in arrival order");
}

/// D2 end to end: after the first Open's batch drains, the stash must be
/// EMPTY — a later serve must not drag along any stale replay, and order
/// stays [1, 2]. (True mid-replay-stash snapshot coverage is the unit test
/// `unstash_is_a_snapshot_not_a_live_view`.)
#[tokio::test]
async fn no_stale_replay_after_batch_drains() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1), GateMsg::Open], Arc::clone(&t));
    let first = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref.ask(GateMsg::Read).await.expect("alive");
            if seen.len() >= 1 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first replay lands");
    assert_eq!(first, vec![1]);
    // Round 2: the gate is open now, so Item(2) serves directly — this
    // asserts the stash is EMPTY after its snapshot drained (nothing stale
    // replays alongside).
    actor_ref.tell(GateMsg::Item(2)).await.expect("deliver 2");
    let second = timeout(terminate_bound(), async {
        loop {
            let seen = actor_ref.ask(GateMsg::Read).await.expect("alive");
            if seen.len() >= 2 {
                return seen;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second serve lands");
    assert_eq!(second, vec![1, 2], "no stale replay, no reorder");
}

/// D6 mid-batch stop: a replayed message that sets `stop` ends the batch —
/// the rest of the stash is never served. Queue [ItemThenStop(1), Item(2),
/// Open]: both stash; replay serves 1 (stop) and must NOT serve 2.
#[tokio::test]
async fn replayed_stop_abandons_rest_of_batch() {
    let t = tape();
    let actor_ref = spawn_prequeued(
        vec![GateMsg::ItemThenStop(1), GateMsg::Item(2), GateMsg::Open],
        Arc::clone(&t),
    );
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor stops after the replayed stop");
    assert_eq!(read(&t), vec![1], "batch ends at stop; 2 is abandoned");
}

/// Invariants 4 + 7: a non-empty stash does not pin. Drop every external
/// ref while a message sits stashed → the actor ref-count-stops (Collected)
/// within the bound, and the stashed message is never served.
#[tokio::test]
async fn stashed_messages_do_not_pin_refcount_stop() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    // Sync: wait until the message was taken (and stashed) so the drop below
    // races nothing.
    let seen = timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive");
    assert_eq!(seen, Vec::<u32>::new(), "1 is stashed, not served");
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("non-empty stash must not keep the actor alive");
    assert_eq!(
        read(&t),
        Vec::<u32>::new(),
        "deferred message dies deferred"
    );
}

/// Invariant 5: in-band `Signal::Stop` with a non-empty stash — the actor
/// stops Normal and the stashed message is never served (Stop abandons the
/// queued backlog; the deferred backlog ranks no higher, spec D6).
#[tokio::test]
async fn inband_stop_drops_stash() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    timeout(
        terminate_bound(),
        actor_ref.mailbox_sender().send(Signal::Stop),
    )
    .await
    .expect("send within bound")
    .expect("stop enqueued");
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("in-band stop lands");
    assert_eq!(read(&t), Vec::<u32>::new(), "stash dropped on Stop");
}

/// Invariant 6: `kill()` with a non-empty stash — hard abort, stashed
/// message never served.
#[tokio::test]
async fn kill_drops_stash() {
    let t = tape();
    let actor_ref = spawn_prequeued(vec![GateMsg::Item(1)], Arc::clone(&t));
    timeout(terminate_bound(), actor_ref.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    actor_ref.kill();
    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("kill lands");
    assert_eq!(read(&t), Vec::<u32>::new(), "stash dropped on kill");
}

/// Minimal supervisor: exists only to own the `Gate` child.
struct Sup;

#[derive(Debug)]
struct SupMsg;
impl Msg for SupMsg {}
impl Mailboxed for Sup {
    type Msg = SupMsg;
}
impl bombay::actor::Actor for Sup {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Infallible> {
        Ok(Sup)
    }
    async fn handle(&mut self, _: SupMsg, _: ActorRef<Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}
impl Watch for Sup {}
impl Supervisor for Sup {}

/// Invariant 8: restart = new incarnation from Args; a stale stash must not
/// leak across incarnations. Incarnation 0 stashes 1 then panics; the
/// rebuilt incarnation is Opened and must serve NOTHING from the old stash.
#[tokio::test(start_paused = true)]
async fn restart_gets_a_fresh_stash() {
    set_supervisor_rng_seed(Some(7));
    let t = tape();
    let sup_ref = Sup::spawn_supervised(());

    let spawned: Arc<Mutex<Vec<caps::Handle<Gate>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_tape = Arc::clone(&t);
    let factory_spawned = Arc::clone(&spawned);
    let config = RestartConfig::new(RestartPolicy::Permanent)
        .with_min_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(1));
    timeout(
        terminate_bound(),
        sup_ref.supervise(config, move || {
            let child = caps::spawn::<Gate>(Arc::clone(&factory_tape));
            factory_spawned.lock().expect("spawned").push(child.clone());
            child
        }),
    )
    .await
    .expect("supervise within bound")
    .expect("supervisor alive");

    // Incarnation 0: stash 1 (closed gate), sync, then crash it.
    let inc0 = spawned.lock().expect("spawned")[0].clone();
    timeout(terminate_bound(), inc0.tell(GateMsg::Item(1)))
        .await
        .expect("tell within bound")
        .expect("delivered");
    timeout(terminate_bound(), inc0.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive — 1 is stashed");
    timeout(terminate_bound(), inc0.tell(GateMsg::Boom))
        .await
        .expect("tell within bound")
        .expect("boom delivered");

    // Paused clock: advance past the 1 ms backoff until incarnation 1 exists.
    let inc1 = timeout(terminate_bound(), async {
        loop {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if let Some(child) = spawned.lock().expect("spawned").get(1).cloned() {
                return child;
            }
        }
    })
    .await
    .expect("rebuild within bound");

    // Open the fresh incarnation: a leaked stash would now serve 1.
    timeout(terminate_bound(), inc1.tell(GateMsg::Open))
        .await
        .expect("tell within bound")
        .expect("open delivered");
    let seen = timeout(terminate_bound(), inc1.ask(GateMsg::Read))
        .await
        .expect("probe within bound")
        .expect("alive");
    assert_eq!(
        seen,
        Vec::<u32>::new(),
        "a stale stash must not survive restart (fresh incarnation from Args)"
    );
}

/// Mutant pin: the adapter reports the USER type's name, not the wrapper's —
/// logs name the interesting type.
#[test]
fn shell_name_is_the_user_type() {
    assert_eq!(
        <Shell<Gate> as bombay::actor::Actor>::name(),
        core::any::type_name::<Gate>(),
        "Shell reports the user type's name"
    );
}

/// The two-queue snapshot is load-bearing (ADR-0022 / ADR-0026 re-proof): a
/// handler that re-stashes each replayed message terminates its batch because
/// `held` is a SEPARATE queue from `ready`; a one-queue design feeds the
/// re-stash back into the queue being drained and never reaches empty.
#[test]
fn two_queue_snapshot_terminates_where_one_queue_livelocks() {
    // TWO-QUEUE (the real cap): re-stashing during replay lands in `held`, so
    // the `ready` snapshot drains and replay TERMINATES in exactly the batch
    // size. A one-queue `Stashing` would loop here past `drained <= 2`.
    let mut s = caps::Stashing::<u32>::bounded(cap(8));
    s.stash(1).expect("1");
    s.stash(2).expect("2");
    s.unstash_all();
    let mut drained = 0u32;
    while let Some(m) = Replay::next_replay(&mut s) {
        s.stash(m).expect("re-stash into held");
        drained = drained.checked_add(1).expect("no overflow");
        assert!(drained <= 2, "two-queue snapshot must terminate the batch");
    }
    assert_eq!(drained, 2, "exactly the snapshot batch replays");
    assert_eq!(s.len(), 2, "re-stashed messages wait for the next unstash");

    // ONE-QUEUE (the rejected model): re-stashing feeds the SAME queue being
    // drained — bounded here to 8 pops, after which it is STILL non-empty (a
    // real run loops forever; ADR-0026's instant-livelock re-proof).
    let mut one: VecDeque<u32> = VecDeque::from([1, 2]);
    for _ in 0..8 {
        let m = one.pop_front().expect("one-queue never drains");
        one.push_back(m);
    }
    assert!(
        !one.is_empty(),
        "one queue livelocks: it never reaches empty"
    );
}

/// Minimal probe for hook delegation: its `on_stop` pushes 999 onto the tape,
/// so a `Shell::on_stop` that forgot to delegate leaves the tape empty.
struct StopProbe {
    tape: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug)]
struct StopProbeMsg;
impl Msg for StopProbeMsg {}

impl caps::Actor for StopProbe {
    type Msg = StopProbeMsg;
    type Args = Arc<Mutex<Vec<u32>>>;
    type Error = Infallible;
    type Caps = ();

    async fn init(tape: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { tape })
    }

    async fn handle(&mut self, _: StopProbeMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Shell<Self>>,
        _: ActorStopReason,
    ) -> Result<(), Infallible> {
        self.tape.lock().expect("tape").push(999);
        Ok(())
    }
}

/// Mutant pin: `Shell::on_stop` DELEGATES to `caps::Actor::on_stop` — a
/// `-> Ok(())` body-replacement mutant skips the user hook and the tape never
/// sees 999.
#[tokio::test]
async fn shell_on_stop_delegates_to_user_hook() {
    let t = tape();
    let actor_ref = caps::spawn::<StopProbe>(Arc::clone(&t));
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while read(&t).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("on_stop ran within the bound");
    assert_eq!(
        read(&t),
        vec![999],
        "Shell::on_stop delegates to caps::Actor::on_stop"
    );
}
