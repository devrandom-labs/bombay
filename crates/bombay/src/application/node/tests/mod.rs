//! The network owner keeps SDK driving separate from ordinary actor Work.

#[path = "../../../../../../examples/counter/src/counter.rs"]
mod counter;
#[path = "../../../../tests/application_clients/ledger.rs"]
mod ledger;

use counter::{Counter, CounterMessage};
use ledger::{LedgerCommand, MailLedger, Receipts};

mod attachment;
mod replay;

mod json;

#[path = "../../../../tests/protected_counter/requester.rs"]
#[expect(
    dead_code,
    reason = "the shared finite Requester protocol also serves ordinary admission cases in its isolated owner"
)]
mod requester;
use requester::{
    CounterProofPurpose, CounterProtectedMeaning, CounterRequester, CounterRequesterMessage,
    CounterRequesterState, CounterRequests, CounterServiceInput, Permission, Scope,
};

use counter::CounterValue;
#[path = "../../../../tests/protected_counter/custody.rs"]
mod custody;
use custody::completed_actor;

#[path = "../../../../tests/protected_counter/providers.rs"]
mod providers;

mod requests;
