//! Remote verification acquisition, available with the `remote` feature.
//!
//! The provider owns authentication policy. Network connections, authorization
//! and actor-mailbox admission retain their separate contracts.

mod verification;

pub use verification::verify;
