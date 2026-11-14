//! Public cold Export construction remains an external consumer.
use super::{Counter, CounterMessage, LedgerCommand, MailLedger};

#[path = "../application_support/mod.rs"]
mod application_support;
use application_support::assert_completed;

#[path = "protocol.rs"]
#[expect(
    dead_code,
    reason = "public construction controls share finite providers; actual verification and transport execute in Node-owned tests"
)]
#[expect(
    clippy::duplicate_mod,
    reason = "construction and transport consumers independently compile the same nominal provider fixture"
)]
mod protocol;
use protocol::*;

#[path = "network_replay/construction.rs"]
mod construction;
