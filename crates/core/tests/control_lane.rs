//! Card #225 — the control-signal lane (ADR-0021): watch registration and
//! supervision ops must NOT queue behind the user backlog.
//!
//! The lane split in one paragraph: `Signal` (user lane: `Message`, `Stop`)
//! stays bounded with per-sender FIFO; `ControlSignal` (watch/unwatch/
//! supervision) rides a second UNBOUNDED flume channel merged inside
//! `MailboxReceiver::recv` with a control-first biased select. These tests pin
//! the card's invariants: control reaches a full-mailbox actor before its
//! backlog drains (1), the control lane is FIFO within itself (2), user FIFO
//! and the zero-alloc send path are unchanged (3), a control signal may
//! overtake an earlier user message while `Stop` still drains what precedes it
//! (4), and the #195 teardown obligation moved lanes intact (5).
//!
//! Every terminal await is bounded — a regression that hangs the loop must
//! FAIL here, not stall the suite (the #148/#179 discipline).

use core::convert::Infallible;
use std::{
    future::IntoFuture,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::time::timeout;

use bombay::{
    SendContext,
    actor::{Actor, ActorRef, PreparedActor, RunResult, Spawn, Supervisor, Watch, WeakActorRef},
    error::ActorStopReason,
    mailbox::{ActorId, Capacity, ControlSignal, Mailbox, Mailboxed, Recv, Signal},
    message::Msg,
    restart::{RestartConfig, RestartPolicy},
    test_support::{set_supervisor_rng_seed, terminate_bound, watch_signal},
};

/// The suite-wide fail-fast bound (MIRI-scaled — see `terminate_bound`).
const TERMINATE: Duration = terminate_bound();

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid test capacity")
}

/// Bounds a pre-run send under the fail-fast bound (card #179): a mutant that
/// stalls the user lane must FAIL here, not hang the test binary.
async fn bounded<F: IntoFuture>(fut: F) -> F::Output {
    timeout(TERMINATE, fut)
        .await
        .expect("send must not hang: the mailbox stalled")
}

/// Fuzz-local actor for the mailbox-level tests: a `u64` message is enough.
struct Probe;
impl Mailboxed for Probe {
    type Msg = u64;
}

/// Counts handled messages and how many times `on_stop` ran. The SUT is the
/// real loop, never a reimplementation.
struct Spy {
    handled: Arc<AtomicU32>,
    stopped: Arc<AtomicU32>,
}
#[derive(Debug)]
struct Ping;
impl Msg for Ping {}
impl Mailboxed for Spy {
    type Msg = Ping;
}
impl Actor for Spy {
    type Args = (Arc<AtomicU32>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (handled, stopped): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self { handled, stopped })
    }
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A supervisor with no behaviour of its own: `supervise` supplies everything.
struct Sup {
    handled: Arc<AtomicU32>,
}
impl Mailboxed for Sup {
    type Msg = Ping;
}
impl Actor for Sup {
    type Args = Arc<AtomicU32>;
    type Error = Infallible;
    async fn on_start(handled: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { handled })
    }
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl Watch for Sup {}
impl Supervisor for Sup {}

/// A supervised child probe: reports each incarnation's birth sequence onto a
/// tape and counts how many incarnations ran `on_stop`.
struct LeafChild {
    stops: Arc<AtomicU32>,
}
impl Mailboxed for LeafChild {
    type Msg = Ping;
}
impl Actor for LeafChild {
    /// (birth sequence, birth tape, stop counter)
    type Args = (u32, flume::Sender<u32>, Arc<AtomicU32>);
    type Error = Infallible;
    async fn on_start(
        (seq, tape, stops): Self::Args,
        _: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        tape.try_send(seq).expect("the birth tape is unbounded");
        Ok(Self { stops })
    }
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Shared supervise-factory plumbing: each invocation spawns a fresh
/// incarnation, tapes its birth sequence, and stashes its strong ref.
struct Factory {
    next: u32,
    tape: flume::Sender<u32>,
    stops: Arc<AtomicU32>,
    stash: Arc<Mutex<Option<ActorRef<LeafChild>>>>,
}

impl Factory {
    fn new() -> (
        Self,
        flume::Receiver<u32>,
        Arc<AtomicU32>,
        Arc<Mutex<Option<ActorRef<LeafChild>>>>,
    ) {
        let (tape, tape_rx) = flume::unbounded();
        let stops = Arc::new(AtomicU32::new(0));
        let stash = Arc::new(Mutex::new(None));
        let factory = Self {
            next: 0,
            tape,
            stops: Arc::clone(&stops),
            stash: Arc::clone(&stash),
        };
        (factory, tape_rx, stops, stash)
    }

    fn spawn_next(&mut self) -> ActorRef<LeafChild> {
        let seq = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("incarnation counter overflow");
        let child = LeafChild::spawn((seq, self.tape.clone(), Arc::clone(&self.stops)));
        *self.stash.lock().expect("stash lock") = Some(child.clone());
        child
    }
}

fn permanent_fast() -> RestartConfig {
    RestartConfig::new(RestartPolicy::Permanent)
        .with_min_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(1))
}

/// Invariant 1 (watch): a watch registration sent to a FULL mailbox is served
/// before any of the earlier user messages — and, end to end, the edge it
/// installs still delivers the death notice.
#[tokio::test]
async fn watch_installs_before_full_backlog_drains() {
    // Mailbox-level ordering witness: cap 1, one user message (lane full), then
    // a control watch. `recv` must serve the watch FIRST.
    let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::from_raw_for_test(0));
    bounded(tx.send(Signal::Message {
        msg: 7,
        self_sender: tx.clone(),
        ctx: SendContext::capture(),
    }))
    .await
    .expect("the one user-lane slot fills");

    let (watch, _link_rx) = watch_signal(ActorId::from_raw_for_test(1), false);
    // Synchronous — the control lane has no capacity to await. Pre-#225 this
    // registration `.await`ed the full bounded mailbox.
    tx.send_control(watch)
        .expect("control send never waits on the user backlog");

    let first = bounded(rx.recv()).await.expect("an item is queued");
    assert!(
        matches!(first, Recv::Control(ControlSignal::Watch(_))),
        "the watch must be served before the full backlog drains",
    );
    assert!(
        matches!(
            bounded(rx.recv()).await.expect("the user message follows"),
            Recv::Signal(Signal::Message { msg: 7, .. })
        ),
        "the user backlog is undisturbed behind it",
    );

    // Actor-level end-to-end: the registration sent to a full-mailbox actor
    // installs a real edge — the death notice arrives at stop.
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<Spy>::new(cap(1));
    let actor_ref = prepared.actor_ref().clone();
    bounded(actor_ref.tell(Ping))
        .await
        .expect("the one slot fills");

    let (watch, link_rx) = watch_signal(ActorId::from_raw_for_test(2), false);
    actor_ref
        .mailbox_sender()
        .send_control(watch)
        .expect("the watch lands on the full actor");

    let run = prepared.spawn((handled, Arc::clone(&stopped)));
    drop(actor_ref);
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "ref-drop collection, got {outcome:?}",
    );
    let notice = bounded(link_rx.recv_async())
        .await
        .expect("the installed edge must deliver the death notice");
    assert!(
        matches!(notice.reason, ActorStopReason::Collected),
        "the true reason rides the notice, got {:?}",
        notice.reason,
    );
}

/// Invariant 1 (supervision): a `supervise` op sent to a supervisor whose user
/// lane is FULL is applied anyway — proven by the child being adopted (its
/// death drives a rebuild) while the queued user messages remain undisturbed.
#[tokio::test(start_paused = true)]
async fn supervise_op_applies_before_full_backlog_drains() {
    set_supervisor_rng_seed(Some(7));
    let handled = Arc::new(AtomicU32::new(0));
    let (prepared, link_rx) = PreparedActor::<Sup>::new_linked(cap(2));
    let sup_ref = prepared.actor_ref().clone();

    // Fill the user lane to capacity; nothing is draining yet.
    bounded(sup_ref.tell(Ping)).await.expect("fill slot 1");
    bounded(sup_ref.tell(Ping))
        .await
        .expect("fill slot 2 — full");

    let (mut factory, tape_rx, child_stops, stash) = Factory::new();
    // Pre-#225 this call parked on the full bounded mailbox until the loop
    // drained a slot; the bounded wrapper would have fired.
    let child_id = timeout(
        TERMINATE,
        sup_ref.supervise(permanent_fast(), move || factory.spawn_next()),
    )
    .await
    .expect("supervise must not hang on a full user lane")
    .expect("the supervisor is alive");

    // The first incarnation spawned inline at the call site.
    let first = timeout(TERMINATE, tape_rx.recv_async())
        .await
        .expect("birth within the bound")
        .expect("the tape is open");
    assert_eq!(first, 0, "incarnation 0 was born at the supervise call");

    let run = prepared.spawn_supervised_task(Arc::clone(&handled), link_rx);

    // Kill incarnation 0: only an APPLIED Add (table insert + installed watch
    // edge — or the installer's self-healing Closed path) can turn that death
    // into a rebuild. No rebuild means the op was lost to the backlog.
    stash
        .lock()
        .expect("stash lock")
        .as_ref()
        .expect("incarnation 0 stashed")
        .kill();
    let reborn = timeout(TERMINATE, tape_rx.recv_async())
        .await
        .expect("a rebuild must arrive — the supervise op was applied")
        .expect("the tape is open");
    assert_eq!(
        reborn, 1,
        "the flooded supervisor adopted and rebuilt the child"
    );

    drop(sup_ref); // collection: the backlog drains, then the supervisor stops
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "ref-drop collection, got {outcome:?}",
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        2,
        "both queued user messages drained, undisturbed by the control op",
    );
    assert_eq!(
        child_stops.load(Ordering::SeqCst),
        1,
        "the surviving incarnation was swept at supervisor exit (#245)",
    );
    assert_eq!(
        child_id,
        stash
            .lock()
            .expect("stash lock")
            .as_ref()
            .expect("incarnation 1 stashed")
            .id(),
        "the returned id names the supervised lineage"
    );
}

/// Invariant 2: FIFO *within* the control lane. watch→unwatch applies in order
/// (no edge survives, no notice); unwatch→watch likewise (the edge stays, the
/// notice arrives).
#[tokio::test]
async fn control_lane_fifo_watch_then_unwatch() {
    let watcher = ActorId::from_raw_for_test(1);

    // watch THEN unwatch: no edge survives — no death notice.
    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let (watch, link_rx) = watch_signal(watcher, false);
    actor_ref
        .mailbox_sender()
        .send_control(watch)
        .expect("watch enqueued");
    actor_ref
        .mailbox_sender()
        .send_control(ControlSignal::Unwatch(watcher))
        .expect("unwatch enqueued");
    let run = prepared.spawn((Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0))));
    drop(actor_ref);
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(outcome, RunResult::Stopped { .. }),
        "the actor stops, got {outcome:?}",
    );
    assert!(
        link_rx.try_recv().is_err(),
        "watch-then-unwatch leaves no edge — a notice here means intra-lane reordering",
    );

    // unwatch THEN watch: the edge survives — the death notice arrives.
    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();
    let target_id = actor_ref.id();
    actor_ref
        .mailbox_sender()
        .send_control(ControlSignal::Unwatch(watcher))
        .expect("unwatch enqueued (no-op)");
    let (watch, link_rx) = watch_signal(watcher, false);
    actor_ref
        .mailbox_sender()
        .send_control(watch)
        .expect("watch enqueued");
    let run = prepared.spawn((Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0))));
    drop(actor_ref);
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "ref-drop collection, got {outcome:?}",
    );
    let notice = bounded(link_rx.recv_async())
        .await
        .expect("unwatch-then-watch keeps the edge — the notice must arrive");
    assert_eq!(notice.id, target_id, "the notice names the watched actor");
}

/// Invariant 4 (overtake): a control signal enqueued AFTER a user message is
/// served BEFORE it — the deliberate ordering relaxation (ADR-0021).
#[tokio::test]
async fn control_overtakes_earlier_user_message() {
    let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(0));
    bounded(tx.send(Signal::Message {
        msg: 1,
        self_sender: tx.clone(),
        ctx: SendContext::capture(),
    }))
    .await
    .expect("user message queued first");
    tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(2)))
        .expect("control queued second");

    assert!(
        matches!(
            bounded(rx.recv()).await.expect("first item"),
            Recv::Control(ControlSignal::Unwatch(_))
        ),
        "the control signal overtakes the earlier user message",
    );
    assert!(
        matches!(
            bounded(rx.recv()).await.expect("second item"),
            Recv::Signal(Signal::Message { msg: 1, .. })
        ),
        "the user message follows, undisturbed",
    );
}

/// Invariant 4 (Stop): `Stop` stays on the USER lane — control signals may
/// overtake it, but it still drains every message queued before it.
#[tokio::test]
async fn stop_still_drains_prior_messages() {
    let handled = Arc::new(AtomicU32::new(0));
    let stopped = Arc::new(AtomicU32::new(0));
    let prepared = PreparedActor::<Spy>::new(cap(4));
    let actor_ref = prepared.actor_ref().clone();

    bounded(actor_ref.tell(Ping)).await.expect("message 1");
    bounded(actor_ref.tell(Ping)).await.expect("message 2");
    // A control signal queued BEFORE the Stop overtakes both messages — and
    // must not change Stop's drain semantics.
    let (watch, link_rx) = watch_signal(ActorId::from_raw_for_test(9), false);
    actor_ref
        .mailbox_sender()
        .send_control(watch)
        .expect("control overtakes");
    bounded(actor_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop queued last");

    let run = prepared.spawn((Arc::clone(&handled), Arc::clone(&stopped)));
    drop(actor_ref);
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "in-band Stop → Normal, got {outcome:?}",
    );
    assert_eq!(
        handled.load(Ordering::SeqCst),
        2,
        "both messages queued before the Stop were handled first",
    );
    assert_eq!(stopped.load(Ordering::SeqCst), 1, "on_stop ran once");
    let notice = bounded(link_rx.recv_async())
        .await
        .expect("the overtaken watch was applied before the stop — its notice fires");
    assert!(
        matches!(notice.reason, ActorStopReason::Normal),
        "the graceful reason rides the notice, got {:?}",
        notice.reason,
    );
}

/// Invariant 5: the #195 obligation moved lanes intact — a `Watch` still
/// queued on the CONTROL lane when the receiver drops is answered with a
/// synthetic death notice (`AlreadyDead`, Erlang's `noproc`), never silently
/// discarded.
#[tokio::test]
async fn queued_watch_answered_on_teardown() {
    let (tx, rx) = Mailbox::<Probe>::bounded(cap(4), ActorId::from_raw_for_test(77));
    let (watch, link_rx) = watch_signal(ActorId::from_raw_for_test(1), true);
    tx.send_control(watch)
        .expect("reg enqueued into the open control lane");

    drop(rx); // receiver gone with the reg still queued — the hard-kill edge

    let notice = link_rx
        .try_recv()
        .expect("a queued watch reg must be notified, never silently dropped");
    assert_eq!(
        notice.id,
        ActorId::from_raw_for_test(77),
        "the notice names the dead actor",
    );
    assert!(
        matches!(notice.reason, ActorStopReason::AlreadyDead),
        "true reason unknowable at the drop edge ⇒ AlreadyDead, got {:?}",
        notice.reason,
    );
    assert!(notice.linked, "the edge's linked flag rides the notice");
}

/// Step-5 regression: a supervise op queued BEHIND an in-band `Stop` still
/// lands — it rides the control lane, overtakes the Stop, and is applied
/// before the loop exits. The graceful-teardown semantics hold on the new
/// lane: the adopted child is stopped WITH the supervisor (#245's sweep), not
/// orphaned. (The op-arriving-after-loop-exit half of #248 rides the same
/// `drain_control` path; its races stay covered by the spawn.rs suite, which
/// now enqueues through `send_control`.)
#[tokio::test(start_paused = true)]
async fn supervise_op_queued_behind_stop_still_lands() {
    set_supervisor_rng_seed(Some(11));
    let handled = Arc::new(AtomicU32::new(0));
    let (prepared, link_rx) = PreparedActor::<Sup>::new_linked(cap(2));
    let sup_ref = prepared.actor_ref().clone();

    let (mut factory, tape_rx, child_stops, _stash) = Factory::new();
    timeout(
        TERMINATE,
        sup_ref.supervise(permanent_fast(), move || factory.spawn_next()),
    )
    .await
    .expect("supervise must not hang")
    .expect("the supervisor is alive");
    bounded(sup_ref.mailbox_sender().send(Signal::Stop))
        .await
        .expect("stop queued behind the supervise op");

    let run = prepared.spawn_supervised_task(Arc::clone(&handled), link_rx);
    let outcome = bounded(run).await.expect("run joins");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "in-band Stop → Normal, got {outcome:?}",
    );
    assert_eq!(
        tape_rx.try_recv().ok(),
        Some(0),
        "the supervise op landed: exactly one incarnation was born",
    );
    assert!(
        tape_rx.try_recv().is_err(),
        "and it was never rebuilt (a Normal stop is not restart-worthy)",
    );
    assert_eq!(
        child_stops.load(Ordering::SeqCst),
        1,
        "the adopted child was stopped with the supervisor — not orphaned",
    );
}

/// Invariant 7, load-bearing: the unbounded control lane is bounded by the
/// CALLER's call rate, not by any structural dedup. A sustained control flood
/// grows the lane without panic, keeps its intra-lane FIFO, and the user lane
/// still drains under the recv policy (control-first, then user).
#[tokio::test]
async fn control_flood_keeps_fifo_and_the_user_lane_still_drains() {
    const FLOOD: u64 = 10_000;

    let (tx, mut rx) = Mailbox::<Probe>::bounded(cap(1), ActorId::from_raw_for_test(0));
    bounded(tx.send(Signal::Message {
        msg: 42,
        self_sender: tx.clone(),
        ctx: SendContext::capture(),
    }))
    .await
    .expect("the one user-lane slot fills");

    // Sustained flood: every call enqueues exactly one op (no dedup — re-watch
    // keeps duplicate edges by design, `Watchers::apply`). The lane grows
    // unboundedly, at the caller's own rate.
    for tag in 0..FLOOD {
        tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(tag)))
            .expect("the unbounded lane accepts the flood");
    }

    // Intra-lane FIFO holds across the whole flood...
    for tag in 0..FLOOD {
        let item = bounded(rx.recv()).await.expect("a flooded control item");
        let Recv::Control(ControlSignal::Unwatch(id)) = item else {
            panic!("expected the control flood's Unwatch tag {tag}");
        };
        assert_eq!(
            id,
            ActorId::from_raw_for_test(tag),
            "intra-lane FIFO under flood"
        );
    }
    // ...and the user lane still drains via the recv policy: control-first,
    // then the backlog — never lost, never reordered.
    assert!(
        matches!(
            bounded(rx.recv())
                .await
                .expect("the user message follows the flood"),
            Recv::Signal(Signal::Message { msg: 42, .. })
        ),
        "the user lane drains after the control flood",
    );
}
