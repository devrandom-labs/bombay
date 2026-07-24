//! End-to-end: a `#[derive(Msg)]` command enum used as a real `Mailboxed::Msg`,
//! round-tripped through the actual `bombay-core` mailbox by value (card #114).

use core::time::Duration;

use bombay_core::mailbox::{ActorId, Capacity, Mailbox, Mailboxed, Signal};
use bombay_core::message::Msg;
use bombay_core::test_support::terminate_bound;
use tokio::time::timeout;

/// Upper bound on a by-value `send -> recv` round-trip: instant when the send
/// truly enqueues, so this only fires if a send silently drops the message —
/// converting an unbounded `recv` hang into a fast, legible failure. Scaled
/// under MIRI — see `terminate_bound`.
const DELIVERY: Duration = terminate_bound();

/// A realistic closed actor command set. `#[derive(Msg)]` gives it the
/// compile-time slot-size tripwire; it stays well under the 256 B default.
#[derive(Debug, PartialEq, Eq, bombay_macros::Msg)]
enum BankCmd {
    Deposit { cents: u64 },
    Withdraw { cents: u64 },
    Balance,
}

struct BankAccount;
impl Mailboxed for BankAccount {
    type Msg = BankCmd;
}

/// The derived `Msg` and the mailbox's `Mailboxed` coexist on one type, and a
/// guarded command survives a real by-value `send` -> `recv` round-trip.
#[tokio::test]
async fn derived_msg_command_round_trips_through_the_real_mailbox() {
    // The derive implemented Msg on the same type the mailbox will queue.
    assert_eq!(<BankCmd as Msg>::SLOT_BUDGET, 256);

    let cap = Capacity::try_from(8).expect("valid capacity");
    let (tx, mut rx) = Mailbox::<BankAccount>::bounded(cap, ActorId::new(0));

    // Bounded sends (card #179): under a `Capacity::get -> 0` mutant the queue
    // is a rendezvous with no receiver polling yet — the send must FAIL fast,
    // not hang the binary past the mutants sweep timeout.
    timeout(DELIVERY, tx.send_message(BankCmd::Deposit { cents: 250 }))
        .await
        .expect("send must not hang: the mailbox stalled")
        .expect("send should succeed");
    timeout(DELIVERY, tx.send_message(BankCmd::Balance))
        .await
        .expect("send must not hang: the mailbox stalled")
        .expect("send should succeed");

    let first = timeout(DELIVERY, rx.recv())
        .await
        .expect("the deposit must round-trip, not hang");
    assert!(matches!(
        first,
        Some(Signal::Message {
            msg: BankCmd::Deposit { cents: 250 },
            ..
        })
    ));
    let second = timeout(DELIVERY, rx.recv())
        .await
        .expect("the balance query must round-trip, not hang");
    assert!(matches!(
        second,
        Some(Signal::Message {
            msg: BankCmd::Balance,
            ..
        })
    ));
}

/// A `Signal::Stop` queued after a domain message is delivered in the same
/// FIFO order as it was sent — control signals and derived messages share one
/// by-value queue, there's no separate priority lane.
#[tokio::test]
async fn stop_signal_preserves_fifo_order_with_derived_messages() {
    let cap = Capacity::try_from(8).expect("valid capacity");
    let (tx, mut rx) = Mailbox::<BankAccount>::bounded(cap, ActorId::new(0));

    // Bounded sends (card #179) — see the round-trip test above.
    timeout(DELIVERY, tx.send_message(BankCmd::Withdraw { cents: 100 }))
        .await
        .expect("send must not hang: the mailbox stalled")
        .expect("send should succeed");
    timeout(DELIVERY, tx.send(Signal::Stop))
        .await
        .expect("send must not hang: the mailbox stalled")
        .expect("send should succeed");

    let msg = timeout(DELIVERY, rx.recv())
        .await
        .expect("the withdraw must round-trip, not hang");
    assert!(matches!(
        msg,
        Some(Signal::Message {
            msg: BankCmd::Withdraw { cents: 100 },
            ..
        })
    ));
    let stop = timeout(DELIVERY, rx.recv())
        .await
        .expect("the queued Stop must arrive, not hang");
    assert!(matches!(stop, Some(Signal::Stop)));
}
