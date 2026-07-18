//! Exact allocation counts of the ref-model handle ops (card #186 / ADR-0010).
//!
//! ONE test, in its OWN binary, on purpose — same rationale as
//! `alloc_exact.rs`: a `#[global_allocator]` counts every allocation in its
//! process, and only a single-test binary is process-isolated under BOTH
//! harnesses. Don't add a second test here.
//!
//! The ADR-0010 claim being pinned: `clone`/`downgrade`/`upgrade` are pure
//! refcount traffic — **0 allocations** each. The single-allocation layout
//! must never trade its 1-RMW clone for a hidden per-clone heap allocation.

use std::alloc::System;

use bombay_core::{
    actor::{Actor, ActorRef},
    error::Infallible,
    mailbox::{Capacity, Mailbox, Mailboxed},
    message::Msg,
    test_support::{CountingAlloc, unstarted_actor},
};

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

struct Probe;

#[derive(Debug)]
struct Ping;
impl Msg for Ping {}
impl Mailboxed for Probe {
    type Msg = Ping;
}
impl Actor for Probe {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// One round of every handle op — used as warm-up and as the measured run.
/// Returns the gross allocation count across the whole round.
fn round(actor_ref: &ActorRef<Probe>) -> isize {
    let before = COUNTER.gross_allocs();
    let clone = actor_ref.clone();
    let weak = actor_ref.downgrade();
    let upgraded = weak.upgrade().expect("a strong ref is alive");
    let weak_clone = weak.clone();
    drop((clone, weak, upgraded, weak_clone));
    COUNTER.gross_allocs() - before
}

#[test]
fn clone_downgrade_upgrade_allocate_nothing() {
    let cap = Capacity::try_from(4_usize).expect("valid capacity");
    let (actor_ref, _rx) = unstarted_actor::<Probe>(Mailbox::<Probe>::bounded(cap));

    // Warm-up: identical round BEFORE measuring, so one-time lazy init never
    // pollutes the measurement.
    round(&actor_ref);

    let live_baseline = COUNTER.snapshot();
    let gross = round(&actor_ref);

    assert_eq!(
        gross, 0,
        "clone/downgrade/upgrade are pure refcount ops — zero heap allocations",
    );
    assert_eq!(
        COUNTER.snapshot(),
        live_baseline,
        "every handle op reclaims exactly — nothing leaks",
    );
}
