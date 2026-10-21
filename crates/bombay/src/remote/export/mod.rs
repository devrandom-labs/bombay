//! Cold actor declarations and their bounded original request ownership.
mod declaration;
mod query;
pub(crate) use query::Exports;
mod request;

use crate::address::MailAddr;
use crate::application::ExternalActorError;
use behavior::Protocol;
use core::fmt;
use core::marker::PhantomData;
pub use declaration::{Reply, Stop, Submission, reply, stop, submission};
pub use request::Request;
use std::cell::RefCell;
use std::collections::TryReserveError;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use zenoh::query::{Query, Queryable};

/// An export operation was refused without reconstructing its owned inputs.
///
/// Construction returns its original target beside this cause. Retention returns
/// the supplied protected bytes, proof and opaque receipt beside this cause.
/// `Display` and `Debug` omit native declaration text; `Error::source` retains
/// that actual original cause.
pub enum ExportError {
    /// The target is not an actor associated with this exact Application.
    UnownedTarget,
    /// Association passed, but the existing native reply endpoint was refused.
    Endpoint { cause: ExternalActorError },
    /// This Application was not invoked by a Node.
    NodeAbsent,
    /// This Node's native Session is already closed.
    SessionClosed,
    /// A supplied query count exceeds the native channel/semaphore range.
    QueryCapacity,
    /// Request storage could not be reserved before declaration.
    Storage { cause: TryReserveError },
    /// The native declaration was refused.
    Declaration { cause: Box<dyn Error + Send + Sync> },
    /// A retained logical identity already protects different request bytes.
    Conflict,
    /// This export cannot retain another distinct request.
    RetentionCapacity,
    /// Another declaration selected the same exact actor.
    DuplicateTarget,
    /// This target has no supplied exact shutdown capability.
    StopAuthority,
    /// Protected request framing or bounded JSON was refused.
    Json { cause: super::json::Error },
    /// The native callback could not acquire query custody.
    IngressCapacity { cause: tokio::sync::TryAcquireError },
    /// Query disposal discharged an actual full ingress transfer.
    IngressFull,
    /// Query disposal discharged an actual closed ingress transfer.
    IngressClosed,
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
            Self::NodeAbsent => formatter.write_str("Application has no Node session"),
            Self::SessionClosed => formatter.write_str("Node session is closed"),
            Self::QueryCapacity => formatter.write_str("query count exceeds native capacity"),
            Self::Storage { cause } => write!(formatter, "request storage was refused: {cause}"),
            Self::Declaration { .. } => formatter.write_str("network declaration was refused"),
            Self::Conflict => formatter.write_str("request identity protects different bytes"),
            Self::RetentionCapacity => formatter.write_str("retained request capacity exhausted"),
            Self::DuplicateTarget => formatter.write_str("actor is already exported by this Node"),
            Self::StopAuthority => formatter.write_str("exact actor shutdown authority is absent"),
            Self::Json { .. } => formatter.write_str("protected request shape was refused"),
            Self::IngressCapacity { .. } => formatter.write_str("query custody is full"),
            Self::IngressFull => formatter.write_str("query ingress is full"),
            Self::IngressClosed => formatter.write_str("query ingress is closed"),
        }
    }
}

impl fmt::Debug for ExportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ExportError")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl Error for ExportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnownedTarget
            | Self::NodeAbsent
            | Self::SessionClosed
            | Self::QueryCapacity
            | Self::Conflict
            | Self::RetentionCapacity
            | Self::DuplicateTarget
            | Self::StopAuthority
            | Self::IngressFull
            | Self::IngressClosed => None,
            Self::Json { cause } => Some(cause),
            Self::IngressCapacity { cause } => Some(cause),
            Self::Endpoint { cause } => Some(cause),
            Self::Storage { cause } => Some(cause),
            Self::Declaration { cause } => Some(cause.as_ref()),
        }
    }
}

/// Explicit local query-channel, query-custody and retained-request maxima; no defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportLimits {
    pub queued_queries: NonZeroUsize,
    pub retained_queries: NonZeroUsize,
    pub retained_requests: NonZeroUsize,
    pub diagnostics: NonZeroUsize,
}
/// One runtime-issued request reference for its target protocol and issuing export.
/// Fields and construction stay private; a key carries no reply or lifecycle authority.
/// Keys are observations, not inputs for admission or lookup. Callers cannot
/// construct them, duplicate ownership, change their target protocol, or use
/// one to submit work to another declaration.
///
/// ```compile_fail,E0451
/// fn forge() -> bombay::remote::RequestKey<bombay::behavior::MessageProtocol<bombay::MailAddr, u8>> {
///     bombay::remote::RequestKey {
///         issuer: std::sync::Weak::new(),
///         row: 0,
///         protocol: std::marker::PhantomData,
///     }
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn duplicate(key: bombay::remote::RequestKey<bombay::behavior::MessageProtocol<bombay::MailAddr, u8>>) {
///     let _duplicate = key.clone();
/// }
/// ```
///
/// ```compile_fail,E0308
/// fn retarget(
///     key: bombay::remote::RequestKey<bombay::behavior::MessageProtocol<bombay::MailAddr, u8>>,
/// ) -> bombay::remote::RequestKey<bombay::behavior::MessageProtocol<bombay::MailAddr, u64>> {
///     key
/// }
/// ```
///
/// ```compile_fail,E0599
/// # type CounterProtocol = bombay::behavior::MessageProtocol<bombay::MailAddr, u8>;
/// fn resubmit(
///     key: &bombay::remote::RequestKey<CounterProtocol>,
///     limits: bombay::remote::ExportLimits,
///     json: bombay::remote::json::Limits,
/// ) {
/// #   let mut export = bombay::remote::Export::new(
/// #       "shop/counter", limits, json,
/// #       |_: &[u8]| Ok::<_, ()>(1_u8),
/// #       bombay::remote::submission::<_, CounterProtocol, _, _>(
/// #           |message: u8| Ok::<_, u8>(move || message),
/// #           |_: &(), _: &bombay::remote::Attempt<Result<(), bombay::TrySendError<u8>>>|
/// #               Ok::<_, ()>(Box::<[u8]>::from([1_u8])),
/// #       ),
/// #       async |_: &[u8], _: &[u8]| Ok::<_, ()>(()),
/// #       async |_: &[u8], _: &()| Ok::<_, ()>(Box::<[u8]>::from([2_u8])),
/// #       |left: &(), right: &()| left == right,
/// #       |_: &(), _: &str, _: bombay::MailAddr| Ok::<_, ()>(()),
/// #       |_: &(), _: &str, _: bombay::MailAddr| Ok::<_, ()>(()),
/// #   );
///     export.attempt(key);
/// }
/// ```
pub struct RequestKey<Target: Protocol<Addr = MailAddr>> {
    issuer: Weak<Semaphore>,
    row: usize,
    protocol: PhantomData<fn() -> Target>,
}

impl<Target: Protocol<Addr = MailAddr>> PartialEq for RequestKey<Target> {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.issuer, &other.issuer) && self.row == other.row
    }
}

impl<Target: Protocol<Addr = MailAddr>> Eq for RequestKey<Target> {}

impl<Target: Protocol<Addr = MailAddr>> fmt::Debug for RequestKey<Target> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RequestKey").finish_non_exhaustive()
    }
}
/// An actual attempted operation and its complete returned value.
/// Uncertain means entry without an acquired return; it grants no retry.
pub enum Attempt<Outcome> {
    Unattempted,
    Uncertain,
    Returned(Outcome),
}
impl<Outcome> fmt::Debug for Attempt<Outcome> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unattempted => "Unattempted",
            Self::Uncertain => "Uncertain",
            Self::Returned(_) => "Returned(..)",
        })
    }
}

/// Cold configuration, pure operation declarations and retained request facts.
/// Native transport ownership stays private. Consuming extraction surrenders any
/// unfinished declaration; only Node's joined result establishes cleanup.
pub struct Export<Inputs, Declarations, Entry> {
    inputs: Inputs,
    declarations: Declarations,
    startup: Option<Result<(), ExportError>>,
    withdrawal: Option<Result<(), zenoh::Error>>,
    ingress: Arc<Mutex<Vec<ExportError>>>,
    entries: RefCell<Vec<Entry>>,
    declaration: RefCell<Option<Queryable<()>>>,
    active: Option<(
        mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
        Weak<Semaphore>,
    )>,
}
impl<Inputs, Declarations, Entry> Export<Inputs, Declarations, Entry> {
    /// Transfer original configuration, declarations and every acquired fact.
    pub fn into_parts(
        self,
    ) -> (
        Inputs,
        Declarations,
        Option<Result<(), ExportError>>,
        Option<Result<(), zenoh::Error>>,
        Vec<ExportError>,
        Vec<Entry>,
    ) {
        let Self {
            inputs,
            declarations,
            startup,
            withdrawal,
            ingress,
            entries,
            declaration,
            active,
        } = self;
        drop(declaration.into_inner());
        drop(active);
        let diagnostics = match Arc::try_unwrap(ingress) {
            Ok(ingress) => ingress
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            Err(ingress) => std::mem::take(
                &mut *ingress
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            ),
        };
        (
            inputs,
            declarations,
            startup,
            withdrawal,
            diagnostics,
            entries.into_inner(),
        )
    }
}
impl<Inputs, Declarations, Entry> fmt::Debug for Export<Inputs, Declarations, Entry> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Export").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
