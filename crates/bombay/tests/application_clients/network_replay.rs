//! Public client, export and rejection contracts remain external consumers.
use super::counter::CounterValue;
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::remote::{Admission, Metadata};

#[path = "../application_support/mod.rs"]
mod application_support;
use application_support::assert_completed;

#[path = "protocol.rs"]
#[expect(
    dead_code,
    reason = "public constructor and pressure tests share finite provider fixtures; actual verification and transport execute in Node-owned tests"
)]
mod protocol;
use protocol::*;

#[path = "network_replay/cancellation.rs"]
mod cancellation;

#[path = "network_replay/construction.rs"]
mod construction;

#[path = "network_replay/refusals.rs"]
mod refusals;

#[path = "network_replay/permissions.rs"]
mod permissions;

#[path = "network_replay/association.rs"]
mod association;

#[path = "network_replay/pressure.rs"]
mod pressure;
