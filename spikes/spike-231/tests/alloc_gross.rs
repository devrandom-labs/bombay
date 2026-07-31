//! Transition-path allocation probe (card #231 scope bullet: zero-box
//! target per the allocate-last rule), on the #207 gross-allocs pattern:
//! delta `gross_allocs()` around one message, compare a `Stay` message
//! against a `Goto` message. The transition itself must add ZERO gross
//! allocations (state tag by value, no boxed behavior).
//!
//! One-test binary: the counting allocator is process-global.

use std::alloc::System;
use std::time::Duration;

use bombay::{
    actor::Spawn,
    mailbox::Capacity,
    test_support::CountingAlloc,
};
use spike_231::{
    Probe, agg_fsm,
    agg_fsm::AggFsm,
    fsm::{Fsm, FsmMsg},
    probe_channel,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new(System);

#[tokio::test(flavor = "current_thread")]
async fn goto_adds_zero_gross_allocs_over_stay() {
    let (tx, mut rx) = probe_channel();
    let cap = Capacity::try_from(4usize).expect("valid stash capacity");
    let actor = Fsm::<AggFsm>::spawn((tx, cap));

    // Warm-up: first message touches lazily-initialized paths (mailbox,
    // probe channel block, timer wheel).
    actor
        .tell(FsmMsg::User(agg_fsm::AggMsg::Replay { ev: 1, last: false }))
        .await
        .expect("warm-up tell");
    assert_eq!(rx.recv().await, Some(Probe::Applied(1)));

    // Stay path: Replay(non-last) — handler runs, no transition.
    let before_stay = ALLOC.gross_allocs();
    actor
        .tell(FsmMsg::User(agg_fsm::AggMsg::Replay { ev: 2, last: false }))
        .await
        .expect("stay tell");
    assert_eq!(rx.recv().await, Some(Probe::Applied(2)));
    let stay_cost = ALLOC.gross_allocs() - before_stay;

    // Goto path: Replay(last) — same handler shape PLUS a state change
    // (epoch bump, timer cancel, state swap, unstash of an empty stash,
    // no new timeout to arm in Ready).
    let before_goto = ALLOC.gross_allocs();
    actor
        .tell(FsmMsg::User(agg_fsm::AggMsg::Replay { ev: 3, last: true }))
        .await
        .expect("goto tell");
    assert_eq!(rx.recv().await, Some(Probe::Applied(3)));
    let goto_cost = ALLOC.gross_allocs() - before_goto;

    // The claim under measurement: the transition machinery allocates
    // nothing beyond what an ordinary handled message costs.
    assert_eq!(
        goto_cost, stay_cost,
        "Goto (state change) must add zero gross allocations over Stay"
    );

    // Reference numbers for the ADR: what arming one state timeout costs
    // (one spawned timer task, ADR-0018 model) — measured directly.
    let before_arm = ALLOC.gross_allocs();
    let handle = actor.send_after(
        Duration::from_secs(3600),
        FsmMsg::User(agg_fsm::AggMsg::FlushDone),
    );
    let arm_cost = ALLOC.gross_allocs() - before_arm;
    handle.cancel();
    println!("gross allocs — stay: {stay_cost}, goto: {goto_cost}, arm(send_after): {arm_cost}");
}
