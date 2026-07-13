//! Model-based differential fuzz of the synchronous mailbox state machine.
//! Drives `try_send` / `drain` / clone / drop against a `VecDeque` oracle and
//! asserts FIFO + exactly-once + capacity backpressure. Sync-only, so it is
//! also the surface #151's MIRI job can run (MIRI cannot drive tokio).

use std::collections::VecDeque;

use bolero::{check, TypeGenerator};
use bombay_core::mailbox::{Capacity, Mailbox, MailboxSender, Mailboxed, Signal};

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
    DropTx,
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
            let (tx, mut rx) = Mailbox::<Probe>::bounded(cap);
            let mut senders: Vec<MailboxSender<Probe>> = vec![tx];
            let mut model: VecDeque<u64> = VecDeque::new();

            for op in ops {
                match op {
                    // rx is never dropped in this loop, so try_send can only
                    // fail with `Full` — never `Closed`.
                    Op::TrySend(m) => {
                        let Some(sender) = senders.first() else {
                            continue;
                        };
                        match sender.try_send(message(*m, sender)) {
                            Ok(()) => {
                                assert!(model.len() < cap_n, "accepted past capacity");
                                model.push_back(*m);
                            }
                            Err(_) => {
                                assert_eq!(model.len(), cap_n, "rejected below capacity");
                            }
                        }
                    }
                    Op::Drain => {
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
                        if let Some(sender) = senders.first() {
                            senders.push(sender.clone());
                        }
                    }
                    Op::DropTx => {
                        senders.pop();
                    }
                    Op::IsClosed => {
                        // Only the "open while a sender lives" direction is
                        // observable — once every sender is dropped there is no
                        // handle left to query.
                        if let Some(sender) = senders.first() {
                            assert!(!sender.is_closed(), "open while a sender lives");
                        }
                    }
                }
            }
        });
}
