//! Model-based differential fuzz of the synchronous mailbox state machine.
//! Drives `try_send` / `drain` / clone / drop (of BOTH senders and the
//! receiver) against a `VecDeque` oracle and asserts FIFO + exactly-once,
//! capacity backpressure (`Full`), and the closed path (`Closed` /
//! `is_closed` / `WeakMailboxSender::upgrade`). Sync, so the MIRI lane can run
//! it — unlike the actor-loop target, which is fuzz-only because bolero's
//! corpus is filesystem-backed (MIRI drives tokio fine; ADR-0005).

use std::collections::VecDeque;

use bolero::{check, TypeGenerator};
use bombay_core::mailbox::{Capacity, Mailbox, MailboxSender, Mailboxed, Signal, TrySendError};

/// Fuzz-local actor. The mailbox is domain-agnostic, so a `u64` message is
/// enough (`Probe` in `mailbox.rs` is `#[cfg(test)]` and unreachable here).
struct Probe;
impl Mailboxed for Probe {
    type Msg = u64;
}

#[derive(Debug, TypeGenerator)]
enum Op {
    TrySend(u64),
    Drain,
    CloneTx,
    /// Drops the sending handle currently used for `try_send` (the loop sends
    /// via `senders.last()`), so the sender count can actually reach the point
    /// where the mailbox teardown paths matter.
    DropTx,
    /// Drops the receiver, closing the mailbox from the read side.
    DropRx,
    IsClosed,
}

/// Map a fuzzer seed to a valid, mostly-small capacity so `try_send` actually
/// exercises the `Full` path, while keeping the `MAX`/`MAX-1` boundaries
/// reachable. `Capacity` rejects `0`, so the floor is `1`.
fn capacity_from_seed(seed: u16) -> Capacity {
    let value = match seed {
        0 => Capacity::MAX,            // upper boundary
        1 => Capacity::MAX - 1,        // MAX-1 boundary
        n => (usize::from(n) % 8) + 1, // 1..=8: small caps exercise `Full`
    };
    Capacity::try_from(value).expect("seed maps to a valid capacity")
}

fn message(msg: u64, tx: &MailboxSender<Probe>) -> Signal<Probe> {
    Signal::Message {
        msg,
        self_sender: tx.clone(),
    }
}

#[test]
fn mailbox_state_machine() {
    check!()
        .with_type::<(u16, Vec<Op>)>()
        .for_each(|(cap_seed, ops)| {
            let cap = capacity_from_seed(*cap_seed);
            let cap_n = cap.get();
            let (tx, rx) = Mailbox::<Probe>::bounded(cap);
            let mut senders: Vec<MailboxSender<Probe>> = vec![tx];
            // `rx` in an `Option` so `DropRx` can close the mailbox from the
            // read side; `None` means the receiver is gone.
            let mut rx = Some(rx);
            let mut model: VecDeque<u64> = VecDeque::new();

            for op in ops {
                match op {
                    Op::TrySend(m) => {
                        let Some(sender) = senders.last() else {
                            continue;
                        };
                        match sender.try_send(message(*m, sender)) {
                            Ok(()) => {
                                assert!(rx.is_some(), "accepted while receiver dropped");
                                assert!(model.len() < cap_n, "accepted past capacity");
                                model.push_back(*m);
                            }
                            Err(TrySendError::Full(_)) => {
                                assert!(rx.is_some(), "Full only while the receiver lives");
                                assert_eq!(model.len(), cap_n, "rejected below capacity");
                            }
                            Err(TrySendError::Closed(returned)) => {
                                assert!(rx.is_none(), "Closed only after the receiver is dropped");
                                assert!(
                                    matches!(returned, Signal::Message { msg, .. } if msg == *m),
                                    "Closed hands the undelivered signal back intact",
                                );
                            }
                        }
                    }
                    Op::Drain => {
                        // A dropped receiver cannot be drained; the model then
                        // holds entries that died with the mailbox, so skip.
                        let Some(rx) = rx.as_mut() else {
                            continue;
                        };
                        let drained: Vec<u64> = rx
                            .drain()
                            .map(|s| match s {
                                Signal::Message { msg, .. } => msg,
                                Signal::Stop => unreachable!("only Message enqueued, got Stop"),
                                Signal::LinkDied(_) => {
                                    unreachable!("only Message enqueued, got LinkDied")
                                }
                            })
                            .collect();
                        let expected: Vec<u64> = model.drain(..).collect();
                        assert_eq!(drained, expected, "drain must be FIFO + exactly-once");
                    }
                    Op::CloneTx => {
                        if let Some(sender) = senders.last() {
                            senders.push(sender.clone());
                        }
                    }
                    Op::DropTx => {
                        senders.pop();
                    }
                    Op::DropRx => {
                        rx = None;
                    }
                    Op::IsClosed => {
                        // Closed iff the receiver is gone — both directions now
                        // observable via `DropRx`.
                        if let Some(sender) = senders.last() {
                            assert_eq!(
                                sender.is_closed(),
                                rx.is_none(),
                                "a sender is closed exactly when the receiver is dropped",
                            );
                        }
                    }
                }
            }
        });
}

/// `WeakMailboxSender::upgrade` returns a strong sender only while one exists,
/// and `None` once the last strong sender is gone. Deterministic (not fuzzed):
/// in the state machine a queued message holds a strong `self_sender`, so the
/// strong count is not simply `senders.len()` — this pins the teardown edge
/// directly. `mailbox.rs:318` `upgrade` is otherwise structurally unreachable.
#[test]
fn weak_sender_upgrades_only_while_a_strong_sender_lives() {
    let cap = Capacity::try_from(1usize).expect("valid capacity");
    let (tx, _rx) = Mailbox::<Probe>::bounded(cap);
    let weak = tx.downgrade();
    assert!(
        weak.upgrade().is_some(),
        "upgrade yields a sender while the strong handle lives",
    );
    drop(tx);
    assert!(
        weak.upgrade().is_none(),
        "upgrade is None once the last strong sender is dropped",
    );
}
