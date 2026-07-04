//! Bombay runtime core — the rebuilt local actor spine.
//!
//! Built card-by-card (M1 epic #122) with kameo as a reference oracle, held to
//! the god-level clippy bar from line one. Transport- and domain-agnostic: the
//! Zenoh remote tier and the nexus aggregate-runner sit on top of this.
//!
//! Nothing here is public API yet — the spine is assembled part-by-part and the
//! surface is settled once the whole core lands (#112–#121).

pub mod error;
pub mod mailbox;
