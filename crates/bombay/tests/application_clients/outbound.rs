//! Public outgoing requests retain actor, provider and native outcomes.
use super::{Counter, CounterMessage, CounterValue};

#[path = "network.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "independent public consumer cohorts compile the same concrete TLS fixture without duplicating its configuration source"
)]
mod network;

#[path = "../protected_counter/custody.rs"]
mod custody;
#[path = "../protected_counter/providers.rs"]
#[expect(
    dead_code,
    reason = "these outgoing traces use the existing finite Recorded policy while the shared fixture also owns Scheduled controls"
)]
mod providers;
#[path = "../protected_counter/requester.rs"]
#[expect(
    dead_code,
    reason = "this public read consumer reuses the nominal protected protocol without constructing its separate admission actor"
)]
mod requester;

#[path = "outbound/delivery.rs"]
mod delivery;
#[path = "outbound/disposal.rs"]
mod disposal;
#[path = "outbound/reader.rs"]
mod reader;
