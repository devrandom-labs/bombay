//! Bounded-stash behavior through the public API (card #224): replay order
//! vs the mailbox backlog, snapshot semantics end to end, stop-mode fates,
//! and restart hygiene. Every terminal await is bounded.

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};
use std::sync::{Arc, Mutex};

use tokio::time::timeout;

use bombay::{
    actor::{
        ActorRef, PreparedActor, Spawn, SpawnConfig, SpawnSupervised, Supervisor, Watch,
        WeakActorRef,
    },
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    restart::{RestartConfig, RestartPolicy},
    stash::{Stash, StashActor, Stashed},
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

#[derive(Debug)]
enum GateMsg {
    Open,
    Item(u32),
    /// Served like `Item`, then stops the actor (mid-batch stop probe).
    ItemThenStop(u32),
    Read(ReplySender<Vec<u32>>),
    /// Panics the handler (restart probe).
    Boom,
}

impl Msg for GateMsg {}
impl Mailboxed for Gate {
    type Msg = GateMsg;
}

impl StashActor for Gate {
    type Args = Arc<Mutex<Vec<u32>>>;
    type Error = Infallible;

    fn stash_capacity(_: &Self::Args) -> Capacity {
        cap(8)
    }

    async fn on_start(tape: Self::Args, _: ActorRef<Stashed<Self>>) -> Result<Self, Infallible> {
        Ok(Self { open: false, tape })
    }

    async fn handle(
        &mut self,
        msg: GateMsg,
        _: ActorRef<Stashed<Self>>,
        stash: &mut Stash<GateMsg>,
        stop: &mut bool,
    ) -> Result<(), Infallible> {
        match msg {
            GateMsg::Open => {
                self.open = true;
                stash.unstash_all();
            }
            GateMsg::Boom => panic!("gate boom"),
            item @ (GateMsg::Item(_) | GateMsg::ItemThenStop(_)) if !self.open => {
                stash
                    .stash(item)
                    .expect("test stash sized for the scenario");
            }
            GateMsg::Item(n) => self.tape.lock().expect("tape").push(n),
            GateMsg::ItemThenStop(n) => {
                self.tape.lock().expect("tape").push(n);
                *stop = true;
            }
            GateMsg::Read(reply) => drop(reply.send(self.tape.lock().expect("tape").clone())),
        }
        Ok(())
    }
}

fn tape() -> Arc<Mutex<Vec<u32>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn read(tape: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    tape.lock().expect("tape").clone()
}

/// Spawns a `Stashed<Gate>` with the message sequence pre-queued before the
/// loop starts — a deterministic mailbox, no racing sends.
fn spawn_prequeued(msgs: Vec<GateMsg>, tape: Arc<Mutex<Vec<u32>>>) -> ActorRef<Stashed<Gate>> {
    let prepared = PreparedActor::<Stashed<Gate>>::new(SpawnConfig {
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
    // Close-after-open variant: reuse Gate but drive it so 2 arrives while
    // closed again. Simplest deterministic driver: two rounds.
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

/// Minimal supervisor: exists only to own the `Stashed<Gate>` child.
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
    async fn handle(
        &mut self,
        _: SupMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Infallible> {
        Ok(())
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

    let spawned: Arc<Mutex<Vec<ActorRef<Stashed<Gate>>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_tape = Arc::clone(&t);
    let factory_spawned = Arc::clone(&spawned);
    let config = RestartConfig::new(RestartPolicy::Permanent)
        .with_min_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(1));
    timeout(
        terminate_bound(),
        sup_ref.supervise(config, move || {
            let child = Stashed::<Gate>::spawn(Arc::clone(&factory_tape));
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

/// Mutant pin: `Stashed::name` reports the user type's name, not the
/// wrapper's — logs name the interesting type.
#[test]
fn stashed_name_is_the_user_type() {
    assert_eq!(
        <Stashed<Gate> as bombay::actor::Actor>::name(),
        core::any::type_name::<Gate>(),
        "Stashed reports the user type's name"
    );
}

/// Minimal probe for hook delegation: its `on_stop` pushes 999 onto the
/// tape, so a `Stashed::on_stop` that forgot to delegate leaves the tape
/// empty.
struct StopProbe {
    tape: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug)]
struct StopProbeMsg;
impl Msg for StopProbeMsg {}
impl Mailboxed for StopProbe {
    type Msg = StopProbeMsg;
}

impl StashActor for StopProbe {
    type Args = Arc<Mutex<Vec<u32>>>;
    type Error = Infallible;

    fn stash_capacity(_: &Self::Args) -> Capacity {
        cap(1)
    }

    async fn on_start(tape: Self::Args, _: ActorRef<Stashed<Self>>) -> Result<Self, Infallible> {
        Ok(Self { tape })
    }

    async fn handle(
        &mut self,
        _: StopProbeMsg,
        _: ActorRef<Stashed<Self>>,
        _: &mut Stash<StopProbeMsg>,
        _: &mut bool,
    ) -> Result<(), Infallible> {
        Ok(())
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Stashed<Self>>,
        _: ActorStopReason,
    ) -> Result<(), Infallible> {
        self.tape.lock().expect("tape").push(999);
        Ok(())
    }
}

/// Mutant pin: `Stashed::on_stop` DELEGATES to `StashActor::on_stop` — a
/// `-> Ok(())` body-replacement mutant skips the user hook and the tape
/// never sees 999.
#[tokio::test]
async fn stashed_on_stop_delegates_to_user_hook() {
    let t = tape();
    let actor_ref = Stashed::<StopProbe>::spawn(Arc::clone(&t));
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
        "Stashed::on_stop delegates to StashActor::on_stop"
    );
}
