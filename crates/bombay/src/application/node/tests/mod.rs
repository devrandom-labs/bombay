//! The network owner keeps SDK driving separate from ordinary actor Work.

#[path = "../../../../../../examples/counter/src/counter.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Node and private Export tests independently compile the same public Counter application"
)]
mod counter;
#[path = "../../../../tests/application_clients/ledger.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Node and private Export tests independently compile the same Ledger application"
)]
mod ledger;
#[path = "../../../../tests/application_clients/protocol.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Node and private Export tests keep separate nominal fixture types from one owning source"
)]
mod protocol;

use counter::{Counter, CounterMessage};
use ledger::{LedgerCommand, MailLedger, Receipts};

mod attachment;
mod identity;
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
#[expect(
    clippy::duplicate_mod,
    reason = "independent Node and Export test owners reuse the same complete custody assertions"
)]
mod custody;
use custody::completed_actor;

#[path = "../../../../tests/protected_counter/providers.rs"]
mod providers;

mod requests;

mod invocation;

mod constructors;
mod pressure;
mod unwind;

mod retirement;

mod protection;

mod permissions;

mod security;
