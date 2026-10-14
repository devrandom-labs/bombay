//! Remote verification and ingress retention, available with `remote`.
//!
//! The provider owns authentication policy. Network connections, authorization
//! and actor-mailbox admission retain their separate contracts.

mod ingress;
mod verification;

pub use ingress::{IngressError, try_retain};
pub use verification::verify;
