//! Remote declarations, verification and ingress retention, with `remote`.
//!
//! The provider owns authentication policy. Network connections, authorization
//! and actor-mailbox admission retain their separate contracts.

mod ingress;
mod metadata;
mod verification;

pub use ingress::{IngressError, try_retain};
pub use metadata::Metadata;
pub use verification::verify;
