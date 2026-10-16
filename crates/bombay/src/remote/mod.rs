//! Remote declarations, verification and ingress retention, with `remote`.
//!
//! The provider owns authentication policy. Network connections, authorization
//! and actor-mailbox admission retain their separate contracts.

mod export;
mod ingress;
pub mod json;
mod metadata;
mod network;
pub(crate) mod startup;
mod verification;

pub use export::{Admission, Export, ExportError, ExportLimits, RequestKey, RequestRefusal};
pub use ingress::{IngressError, try_retain};
pub use metadata::Metadata;
pub use network::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
pub use startup::NodeStartupRefusal;
pub use verification::verify;

pub use crate::application::node::{Node, NodeExecutionRefusal};
