//! Refusals when constructing Application-certified actor clients.

use crate::application::ExternalActorError;
use core::fmt;
use std::error::Error;

/// Client construction refused; the operation separately returns its original target.
#[derive(Debug)]
pub enum ExportError {
    /// The target is not an actor associated with this exact Application.
    UnownedTarget,
    /// Association passed, but the existing native reply endpoint was refused.
    Endpoint { cause: ExternalActorError },
}

impl fmt::Display for ExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnownedTarget => {
                formatter.write_str("target is not an actor owned by this Application")
            }
            Self::Endpoint { cause } => {
                write!(formatter, "export reply endpoint was refused: {cause}")
            }
        }
    }
}

impl Error for ExportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnownedTarget => None,
            Self::Endpoint { cause } => Some(cause),
        }
    }
}
