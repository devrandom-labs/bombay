//! Runnable demonstration: `#[derive(Msg)]`'s compile-time slot-size tripwire
//! composed end to end with a real `bombay-core` mailbox (card #114).
//!
//! Run with `cargo run -p bombay-core --example msg_budget`.

use bombay_core::mailbox::{Capacity, Mailbox, Mailboxed, Signal};
use bombay_core::message::Msg;
use std::error::Error;
use std::mem::size_of;

/// A realistic closed actor command set — well inside the default 256 B
/// budget, so `#[derive(Msg)]` accepts it silently.
#[derive(Debug, bombay_macros::Msg)]
enum BankCmd {
    Deposit { cents: u64 },
    Withdraw { cents: u64 },
    Balance,
}

struct BankAccount;

impl Mailboxed for BankAccount {
    type Msg = BankCmd;
}

// Uncomment to watch the compile-time tripwire fire — `[u8; 4096]` blows the
// 256 B budget, so `#[derive(Msg)]` turns it into a *compile error*, not a
// silent per-slot tax:
//
//     #[derive(bombay_macros::Msg)]
//     enum TooFat {
//         Bulk([u8; 4096]),
//     }
//
// The escape hatch is `#[msg(budget = 8192)]` on the enum, or boxing the
// field (as `Signal` itself boxes the cold `LinkDied` payload).

/// A command set that legitimately needs more than the default budget —
/// `#[msg(budget = N)]` raises the ceiling instead of forcing a box.
#[derive(Debug, bombay_macros::Msg)]
#[msg(budget = 8192)]
enum BulkImportCmd {
    Chunk([u8; 4096]),
}

#[allow(
    clippy::print_stdout,
    reason = "runnable demo example — printing the budget/queue trace is the point"
)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "BankCmd:       size_of = {:>4} B, SLOT_BUDGET = {} B (default)",
        size_of::<BankCmd>(),
        <BankCmd as Msg>::SLOT_BUDGET
    );
    println!(
        "BulkImportCmd: size_of = {:>4} B, SLOT_BUDGET = {} B (raised via #[msg(budget = 8192)])",
        size_of::<BulkImportCmd>(),
        <BulkImportCmd as Msg>::SLOT_BUDGET
    );
    // A real instance of the raised-budget command still compiles and fits.
    let BulkImportCmd::Chunk(payload) = BulkImportCmd::Chunk([7u8; 4096]);
    println!(
        "BulkImportCmd instance built ({} B payload, first byte = {})",
        payload.len(),
        payload[0]
    );

    let cap = Capacity::try_from(8)?;
    let (tx, mut rx) = Mailbox::<BankAccount>::bounded(cap);

    tx.send_message(BankCmd::Deposit { cents: 500 })
        .await
        .map_err(|err| format!("{err:?}"))?;
    tx.send_message(BankCmd::Withdraw { cents: 120 })
        .await
        .map_err(|err| format!("{err:?}"))?;
    tx.send_message(BankCmd::Balance)
        .await
        .map_err(|err| format!("{err:?}"))?;
    tx.send(Signal::Stop)
        .await
        .map_err(|err| format!("{err:?}"))?;

    println!("draining the mailbox by value — no per-message heap box:");
    while let Some(signal) = rx.recv().await {
        match signal {
            Signal::Message {
                msg: BankCmd::Deposit { cents },
                ..
            } => {
                println!("  Signal::Message(Deposit {{ cents: {cents} }})");
            }
            Signal::Message {
                msg: BankCmd::Withdraw { cents },
                ..
            } => {
                println!("  Signal::Message(Withdraw {{ cents: {cents} }})");
            }
            Signal::Message {
                msg: BankCmd::Balance,
                ..
            } => println!("  Signal::Message(Balance)"),
            Signal::Stop => {
                println!("  Signal::Stop — done");
                break;
            }
            Signal::Watch(_) | Signal::Unwatch(_) => println!("  Signal::Watch/Unwatch(..)"),
        }
    }

    Ok(())
}
