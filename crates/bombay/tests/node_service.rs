//! Focused consumer mount while the retired export callers await migration.
#![cfg(feature = "remote")]
#[path = "../../../examples/counter/src/counter.rs"]
mod counter;
#[path = "application_clients/ledger.rs"]
mod ledger;
#[path = "application_clients/node_exports.rs"]
mod node_exports;
