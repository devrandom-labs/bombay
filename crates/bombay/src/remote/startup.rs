//! Refusals from private network construction and native startup.
use core::fmt;
use serde_json::Error as JsonError;
use std::error::Error;
use tokio::runtime::{RuntimeFlavor, TryCurrentError};

/// Startup was refused while retaining the original Application and Network.
pub enum NodeStartupRefusal {
    /// A supplied direction or generated setting cannot use the selected profile.
    Unsupported { field: &'static str },
    /// A credential file path cannot be represented without changing its bytes.
    CredentialPath { field: &'static str },
    /// The native address constructor refused the supplied address.
    Address {
        field: &'static str,
        cause: Box<dyn Error + Send + Sync>,
    },
    /// Conversion into the private native credential representation failed.
    CredentialValue {
        field: &'static str,
        cause: JsonError,
    },
    /// Capacity exceeds the native semaphore representation.
    QueryLimitExcessive,
    /// The native connection could not open; original inputs remain owned.
    Open { cause: Box<dyn Error + Send + Sync> },
    /// No entered Tokio host is available.
    Host { cause: TryCurrentError },
    /// Native opening requires an entered multithreaded Tokio host.
    HostFlavor { flavor: RuntimeFlavor },
}

impl fmt::Display for NodeStartupRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { field } => {
                write!(formatter, "unsupported Node connection setting: {field}")
            }
            Self::CredentialPath { field } => {
                write!(formatter, "unsupported credential path encoding: {field}")
            }
            Self::Address { field, .. } => {
                write!(formatter, "network address construction failed: {field}")
            }
            Self::CredentialValue { field, .. } => {
                write!(formatter, "network credential construction failed: {field}")
            }
            Self::QueryLimitExcessive => {
                formatter.write_str("retained query capacity exceeds the native supported maximum")
            }
            Self::Open { .. } => formatter.write_str("native Node startup failed"),
            Self::Host { cause } => write!(
                formatter,
                "the Node requires an entered Tokio runtime: {cause}"
            ),
            Self::HostFlavor { flavor } => write!(
                formatter,
                "the Node requires a multithreaded Tokio runtime, received {flavor:?}"
            ),
        }
    }
}

impl fmt::Debug for NodeStartupRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NodeStartupRefusal")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl Error for NodeStartupRefusal {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Open { cause } | Self::Address { cause, .. } => Some(cause.as_ref()),
            Self::CredentialValue { cause, .. } => Some(cause),
            Self::Host { cause } => Some(cause),
            Self::Unsupported { .. }
            | Self::CredentialPath { .. }
            | Self::QueryLimitExcessive
            | Self::HostFlavor { .. } => None,
        }
    }
}
