//! The network owner keeps SDK driving separate from ordinary actor Work.

#[path = "../../../../../../examples/counter/src/counter.rs"]
mod counter;
#[path = "../../../../tests/application_clients/ledger.rs"]
mod ledger;

use counter::{Counter, CounterMessage};
use ledger::{LedgerCommand, MailLedger, Receipts};

mod replay;

mod json;
