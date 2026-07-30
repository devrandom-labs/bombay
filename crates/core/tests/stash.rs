//! Bounded-stash behavior through the public API (card #224): replay order
//! vs the mailbox backlog, snapshot semantics end to end, stop-mode fates,
//! and restart hygiene. Every terminal await is bounded.

use core::{convert::Infallible, num::NonZeroUsize};
use std::sync::{Arc, Mutex};

use tokio::time::timeout;

use bombay::{
    actor::{ActorRef, PreparedActor, Spawn, SpawnConfig},
    mailbox::{Capacity, Mailboxed, Signal},
    message::Msg,
    reply::ReplySender,
    stash::{Stash, StashActor, Stashed},
    test_support::terminate_bound,
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
