//! End-to-end: a `#[derive(Msg)]` command enum used as a real `Mailboxed::Msg`,
//! round-tripped through the actual `bombay-core` mailbox by value (card #114).

use bombay_core::mailbox::{Capacity, Mailbox, Mailboxed, Signal};
use bombay_core::message::Msg;

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
    let (tx, mut rx) = Mailbox::<BankAccount>::bounded(cap);

    tx.send_message(BankCmd::Deposit { cents: 250 })
        .await
        .expect("send should succeed");
    tx.send_message(BankCmd::Balance)
        .await
        .expect("send should succeed");

    assert!(matches!(
        rx.recv().await,
        Some(Signal::Message {
            msg: BankCmd::Deposit { cents: 250 },
            ..
        })
    ));
    assert!(matches!(
        rx.recv().await,
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
    let (tx, mut rx) = Mailbox::<BankAccount>::bounded(cap);

    tx.send_message(BankCmd::Withdraw { cents: 100 })
        .await
        .expect("send should succeed");
    tx.send(Signal::Stop).await.expect("send should succeed");

    assert!(matches!(
        rx.recv().await,
        Some(Signal::Message {
            msg: BankCmd::Withdraw { cents: 100 },
            ..
        })
    ));
    assert!(matches!(rx.recv().await, Some(Signal::Stop)));
}
