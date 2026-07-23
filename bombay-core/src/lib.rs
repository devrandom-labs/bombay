//! Bombay runtime core — the rebuilt local actor spine.
//!
//! Built card-by-card (M1 epic #122) with kameo as a reference oracle, held to
//! the god-level clippy bar from line one. Transport- and domain-agnostic: the
//! Zenoh remote tier and the nexus aggregate-runner sit on top of this.
//!
//! Nothing here is public API yet — the spine is assembled part-by-part and the
//! surface is settled once the whole core lands (#112–#121).
//!
//! # Panic strategy: `unwind` is a hard requirement
//!
//! This crate is only correct under `panic = "unwind"`. The supervision
//! guarantee documented on [`actor::Actor`] — "a panic in `handle` is caught and
//! routed to `on_panic`" — is implemented with `catch_unwind`, which under
//! `panic = "abort"` catches nothing: the actor panic terminates the process.
//!
//! Cargo ignores the `panic` setting for tests, so no test can detect this
//! (verified on card #169: with `panic = "abort"` the suite stays fully green
//! while a release binary aborts on the very panic the suite asserts is caught).
//! The guard below is therefore the only thing that can report the misbuild.

#[cfg(panic = "abort")]
compile_error!(
    "bombay-core requires panic = \"unwind\", but this build selected \"abort\".\n\
     Actor supervision is implemented with catch_unwind (see actor::kind and \
     actor::spawn): under the abort strategy those boundaries catch nothing, so \
     an actor panic terminates the whole process instead of becoming an \
     inspectable PanicError routed to on_panic.\n\
     Fix: remove `panic = \"abort\"` from the active cargo profile. There is no \
     supported abort-mode configuration.\n\
     This is a compile error because nothing else can report it — cargo ignores \
     the `panic` setting for tests, so the suite stays green while release \
     binaries abort (card #169)."
);

pub mod actor;
pub mod error;
pub mod mailbox;
pub mod message;
pub mod registry;
pub mod reply;
pub mod request;
mod watch;

// Both arms are load-bearing: the feature arm serves integration tests and
// benches (dev-dep feature unification turns it on for external test binaries);
// the `test` arm keeps in-crate unit-test visibility independent of that
// unification subtlety.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
