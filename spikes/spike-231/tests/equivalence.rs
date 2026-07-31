//! Mode-blind equivalence oracle (the #266 pattern): the same abstract
//! script drives both lifecycle variants; observable probe sequences must be
//! identical. Any forgotten bookkeeping step in the idiom variant ([F1]-[F6]
//! in `agg_idiom.rs`) breaks a scenario here.

use bombay::{
    actor::{ActorRef, Spawn},
    mailbox::Capacity,
    stash::Stashed,
    test_support::terminate_bound,
};
use spike_231::{
    Probe, agg_fsm,
    agg_fsm::AggFsm,
    agg_idiom,
    agg_idiom::AggIdiom,
    fsm::{Fsm, FsmMsg},
    probe_channel,
};
use tokio::time::timeout;

/// Variant-agnostic script operations.
#[derive(Debug, Clone, Copy)]
enum Op {
    Replay(u64, bool),
    Cmd(u64),
    Drain,
    /// Simulates the state-timeout message that fired-and-queued just before
    /// its cancellation — the race a queued-message timeout cannot avoid.
    StaleDeadline,
}

async fn run_idiom(cap: usize, ops: &[Op]) -> Vec<Probe> {
    let (tx, rx) = probe_channel();
    let cap = Capacity::try_from(cap).expect("valid stash capacity");
    let actor: ActorRef<Stashed<AggIdiom>> = Stashed::<AggIdiom>::spawn((tx, cap));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => agg_idiom::AggMsg::Replay { ev, last },
            Op::Cmd(id) => agg_idiom::AggMsg::Cmd { id },
            Op::Drain => agg_idiom::AggMsg::Drain,
            Op::StaleDeadline => agg_idiom::AggMsg::LoadDeadline,
        };
        actor.tell(msg).await.expect("tell during script");
    }
    collect(rx).await
}

async fn run_fsm(cap: usize, ops: &[Op]) -> Vec<Probe> {
    let (tx, rx) = probe_channel();
    let cap = Capacity::try_from(cap).expect("valid stash capacity");
    let actor: ActorRef<Fsm<AggFsm>> = Fsm::<AggFsm>::spawn((tx, cap));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => FsmMsg::User(agg_fsm::AggMsg::Replay { ev, last }),
            Op::Cmd(id) => FsmMsg::User(agg_fsm::AggMsg::Cmd { id }),
            Op::Drain => FsmMsg::User(agg_fsm::AggMsg::Drain),
            // Epoch 0 = the Loading arming; any transition has bumped past it.
            Op::StaleDeadline => FsmMsg::StateTimeout { epoch: 0 },
        };
        actor.tell(msg).await.expect("tell during script");
    }
    collect(rx).await
}

/// Drains the probe channel until the actor stops (drops its sender).
///
/// The guard must outlast the LONGEST timer a scenario waits on: under a
/// paused clock, auto-advance fires the EARLIEST pending timer first — a
/// 15 s guard would fire before the 30 s load deadline it is guarding.
async fn collect(mut rx: spike_231::ProbeRx) -> Vec<Probe> {
    let guard = terminate_bound().max(agg_fsm::LOAD_DEADLINE * 3);
    timeout(guard, async {
        let mut got = Vec::new();
        while let Some(p) = rx.recv().await {
            got.push(p);
        }
        got
    })
    .await
    .expect("actor must stop and close the probe channel")
}

async fn assert_equivalent(cap: usize, ops: &[Op], expected: &[Probe]) {
    let idiom = run_idiom(cap, ops).await;
    let fsm = run_fsm(cap, ops).await;
    assert_eq!(idiom, fsm, "variants diverged");
    assert_eq!(idiom, expected, "both variants match, but not the spec");
    assert!(
        !idiom.contains(&Probe::StaleTimeoutLeaked),
        "stale timeout observed by user code"
    );
}

/// S1: happy path — rehydrate with interleaved commands; deferred commands
/// replay ahead of the mailbox backlog on entering Ready; drain refuses
/// nothing (queue empty) and stops after the flush.
#[tokio::test]
async fn s1_happy_path() {
    let ops = [
        Op::Replay(1, false),
        Op::Cmd(10),
        Op::Cmd(11),
        Op::Replay(2, false),
        Op::Replay(3, true),
        Op::Cmd(12),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::Applied(2),
        Probe::Applied(3),
        Probe::Processed(10),
        Probe::Processed(11),
        Probe::Processed(12),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(8, &ops, &expected).await;
}

/// S2: bounded deferral — the stash refuses loudly at capacity; the shed
/// command is reported, the retained ones replay on Ready.
#[tokio::test]
async fn s2_stash_overflow_sheds_loudly() {
    let ops = [
        Op::Replay(1, false),
        Op::Cmd(10),
        Op::Cmd(11),
        Op::Cmd(12),
        Op::Replay(2, true),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::ShedFull(12),
        Probe::Applied(2),
        Probe::Processed(10),
        Probe::Processed(11),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(2, &ops, &expected).await;
}

/// S3: the rehydration deadline fires (paused clock auto-advances) — the
/// actor reports the timeout and stops; the deferred command dies with the
/// incarnation (stash D6), refused by silence, exactly equal in both.
#[tokio::test(start_paused = true)]
async fn s3_load_deadline_fires() {
    let ops = [Op::Replay(1, false), Op::Cmd(10)];
    let expected = [Probe::Applied(1), Probe::LoadTimedOut];
    assert_equivalent(8, &ops, &expected).await;
}

/// S4: the stale-timeout race — a deadline that fired-and-queued just before
/// its cancellation must be invisible: the idiom variant needs guard arms
/// ([F5]), the wrapper filters by epoch.
#[tokio::test]
async fn s4_stale_deadline_is_invisible() {
    let ops = [
        Op::Replay(1, true),
        Op::StaleDeadline,
        Op::Cmd(20),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::Processed(20),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(8, &ops, &expected).await;
}

/// S5: drain during Loading — deferred commands must be RELEASED into
/// Draining and explicitly refused (gen_statem: postponed events retried on
/// every state change), never silently dropped ([F6], [F4-dup]).
#[tokio::test]
async fn s5_drain_during_loading_refuses_deferred() {
    let ops = [Op::Replay(1, false), Op::Cmd(10), Op::Cmd(11), Op::Drain];
    let expected = [
        Probe::Applied(1),
        Probe::DrainStarted,
        Probe::Refused(10),
        Probe::Refused(11),
        Probe::Snapshotted,
    ];
    assert_equivalent(4, &ops, &expected).await;
}

/// Guard for S3: the deadline must NOT fire when rehydration completes in
/// time — i.e. cancellation on transition actually works in both variants.
#[tokio::test(start_paused = true)]
async fn s6_deadline_cancelled_on_ready() {
    let ops = [Op::Replay(1, true), Op::Cmd(20)];
    let expected = [Probe::Applied(1), Probe::Processed(20)];
    // No Drain: stop by dropping the ref is not wired here, so instead
    // let the paused clock run far past the deadline, then drain.
    let idiom = run_idiom_open(&ops).await;
    let fsm = run_fsm_open(&ops).await;
    assert_eq!(idiom, fsm, "variants diverged");
    assert_eq!(idiom, expected);
}

/// Open-ended runner for scenarios that do not stop the actor: sends the
/// script, advances the paused clock past the deadline, then compares
/// whatever was observed (a fired deadline would appear as LoadTimedOut).
async fn run_idiom_open(ops: &[Op]) -> Vec<Probe> {
    let (tx, mut rx) = probe_channel();
    let cap = Capacity::try_from(8usize).expect("valid stash capacity");
    let actor = Stashed::<AggIdiom>::spawn((tx, cap));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => agg_idiom::AggMsg::Replay { ev, last },
            Op::Cmd(id) => agg_idiom::AggMsg::Cmd { id },
            Op::Drain => agg_idiom::AggMsg::Drain,
            Op::StaleDeadline => agg_idiom::AggMsg::LoadDeadline,
        };
        actor.tell(msg).await.expect("tell during script");
    }
    tokio::time::sleep(agg_idiom::LOAD_DEADLINE * 3).await;
    let mut got = Vec::new();
    while let Ok(p) = rx.try_recv() {
        got.push(p);
    }
    drop(actor);
    got
}

async fn run_fsm_open(ops: &[Op]) -> Vec<Probe> {
    let (tx, mut rx) = probe_channel();
    let cap = Capacity::try_from(8usize).expect("valid stash capacity");
    let actor = Fsm::<AggFsm>::spawn((tx, cap));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => FsmMsg::User(agg_fsm::AggMsg::Replay { ev, last }),
            Op::Cmd(id) => FsmMsg::User(agg_fsm::AggMsg::Cmd { id }),
            Op::Drain => FsmMsg::User(agg_fsm::AggMsg::Drain),
            Op::StaleDeadline => FsmMsg::StateTimeout { epoch: 0 },
        };
        actor.tell(msg).await.expect("tell during script");
    }
    tokio::time::sleep(agg_fsm::LOAD_DEADLINE * 3).await;
    let mut got = Vec::new();
    while let Ok(p) = rx.try_recv() {
        got.push(p);
    }
    drop(actor);
    got
}
