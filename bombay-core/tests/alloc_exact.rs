//! Exact-memory reclamation of the self-pinning signal cycle (card #151).
//!
//! ONE test, in its OWN binary, on purpose: a `#[global_allocator]` counts every
//! allocation in its process, and only a single-test binary is process-isolated
//! under BOTH harnesses (nextest runs per-process anyway; plain `cargo test`
//! shares a process per binary). Adding a second test here would silently break
//! the exactness guarantee — don't.

use std::alloc::System;

use bombay_core::{
    mailbox::{Capacity, Mailbox, Mailboxed},
    test_support::CountingAlloc,
};

#[global_allocator]
static COUNTER: CountingAlloc = CountingAlloc::new(System);

struct Probe;

impl Mailboxed for Probe {
    type Msg = Vec<u8>;
}

/// One round of the cycle the card names: bounded mailbox, N sends (each
/// `Signal::Message` embeds a strong `self_sender` clone — ADR-0003), then the
/// receiver drops MID-BACKLOG (messages still queued), then the sender drops.
fn cycle_round(messages: usize, payload_len: usize) {
    let capacity = Capacity::try_from(messages).expect("valid test capacity");
    let (tx, rx) = Mailbox::<Probe>::bounded(capacity);
    for _ in 0..messages {
        tx.try_send_message(vec![0_u8; payload_len])
            .expect("capacity holds all test messages");
    }
    drop(rx); // mid-backlog: every queued signal still holds a self_sender
    drop(tx);
}

#[test]
fn cycle_reclaims_to_exact_baseline() {
    // Warm-up: one full round BEFORE the baseline, so one-time lazy
    // initialization (harness, flume internals) never pollutes the measurement.
    cycle_round(8, 64);

    let baseline = COUNTER.snapshot();
    cycle_round(8, 64);
    let after = COUNTER.snapshot();

    assert_eq!(
        after, baseline,
        "the queue->Signal->Sender->Arc cycle must reclaim exactly (ADR-0003)"
    );
}
