//! Actor identity: the process-local handle (card #206).
//!
//! [`ActorId`] is bombay's **local** identity coordinate. The **global**
//! coordinate — a self-certifying KERI AID earned when an actor joins the
//! dataspace — is a *separate* type owned by card #121: it pairs with this
//! handle at the Zenoh remote boundary and never replaces it. The core
//! routes by this handle; the AID addresses across the dataspace.

use core::sync::atomic::{AtomicU64, Ordering};

/// A process-local, unforgeable actor handle: bombay's in-process routing key
/// (mailbox, death-watch, supervision).
///
/// # Process-local, never global
///
/// Values are unique only within one process incarnation (the counter restarts
/// with the process). The handle is deliberately **not serializable**: the
/// dataspace address of an actor is its KERI AID (#121); the local handle
/// never crosses the wire and is never persisted. Do not add
/// `Serialize`/`Deserialize` — a compile-time pin refuses the build.
///
/// # Pure name
///
/// The raw value is unreadable outside this crate — no getter, no
/// `From`/`Into<u64>`, no `Display`. An `ActorId` supports exactly copy,
/// comparison, and `Debug`.
///
/// # Designation, not authority
///
/// Holding an `ActorId` grants nothing: send-authority lives exclusively in
/// [`ActorRef`](crate::actor::ActorRef)/`Recipient` (they hold the channel).
/// No API may ever convert a bare `ActorId` into a ref — the registry stays
/// name-keyed. See ADR-0015.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorId(u64);

impl ActorId {
    /// Wraps a value minted by [`next_actor_id`]. In-crate only: outside this
    /// crate an `ActorId` is obtainable solely from a spawned actor.
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Test-only fabrication seam (unit suites, benches, fuzz, examples).
    /// Never a production path: production ids come from [`next_actor_id`].
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub const fn from_raw_for_test(raw: u64) -> Self {
        Self(raw)
    }
}

/// Monotonic process-local id source. Overflow/wrap policy is the
/// counter-hygiene follow-up card (see the #206 PR); 2^64 is unreachable in
/// practice (~585 years at 10^9 spawns/s).
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

/// Mints the next process-local id (spawn path).
pub(crate) fn next_actor_id() -> ActorId {
    // Relaxed is sufficient: correctness needs only that each `fetch_add`
    // returns a distinct value. Uniqueness is a property of atomic increment
    // alone and requires no happens-before with any other memory (CLAUDE
    // concurrency rule).
    ActorId::from_raw(NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed))
}
