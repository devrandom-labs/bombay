//! One Application-associated declaration and its retained requests.
use super::{IngressError, try_retain};
use crate::TrySendError;
use crate::address::MailAddr;
use crate::application::{ActorInterface, ExternalActorError};
use behavior::{EstablishedRecipient, Protocol};
use core::fmt;
use core::future::{Future, poll_fn};
use core::marker::PhantomData;
use core::mem;
use core::ops::AsyncFn;
use core::pin::pin;
use core::task::Poll;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::TryReserveError;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use zenoh::query::{Query, Queryable};
use zenoh::{Error as ZenohError, Session};

/// An export operation was refused without reconstructing its owned inputs.
///
/// Construction returns its original target beside this cause. Retention returns
/// the supplied protected bytes, proof and opaque receipt beside this cause.
#[derive(Debug)]
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
    Declaration { cause: ZenohError },
    /// A retained logical identity already protects different request bytes.
    Conflict,
    /// This export cannot retain another distinct request.
    RetentionCapacity,
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
            Self::Declaration { cause } => {
                write!(formatter, "network declaration was refused: {cause}")
            }
            Self::Conflict => formatter.write_str("request identity protects different bytes"),
            Self::RetentionCapacity => formatter.write_str("retained request capacity exhausted"),
        }
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
            | Self::RetentionCapacity => None,
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
}
/// One runtime-issued request reference; fields and construction stay private.
pub struct RequestKey<Target: Protocol<Addr = MailAddr>> {
    export: Weak<Semaphore>,
    row: usize,
    target: PhantomData<fn() -> Target>,
}

impl<Target: Protocol<Addr = MailAddr>> PartialEq for RequestKey<Target> {
    fn eq(&self, other: &Self) -> bool {
        self.export.ptr_eq(&other.export) && self.row == other.row
    }
}

impl<Target: Protocol<Addr = MailAddr>> Eq for RequestKey<Target> {}

impl<Target: Protocol<Addr = MailAddr>> fmt::Debug for RequestKey<Target> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("RequestKey").finish_non_exhaustive()
    }
}
/// The actual mailbox attempt, including interrupted-return uncertainty.
/// Debug identifies the disposition while omitting the original command.
pub enum Admission<Command> {
    Unattempted,
    Uncertain,
    Returned(Result<(), TrySendError<Command>>),
}

/// This attempt issued no new admission; prior acquired facts remain retained.
/// Debug omits the cause; Display delegates to it. Standard Error source access
/// requires an Error + 'static cause; borrowed causes remain valid values.
pub enum RequestRefusal<AuthorityError> {
    /// The supplied key was not issued by this export.
    UnknownRequest,
    /// The current permission loan was refused with its original typed cause.
    Authority { cause: AuthorityError },
}
impl<Command> fmt::Debug for Admission<Command> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unattempted => "Unattempted",
            Self::Uncertain => "Uncertain",
            Self::Returned(Ok(())) => "Returned(Ok(()))",
            Self::Returned(Err(TrySendError::Full(_))) => "Returned(Err(Full(..)))",
            Self::Returned(Err(TrySendError::Closed(_))) => "Returned(Err(Closed(..)))",
        })
    }
}

impl<AuthorityError> fmt::Debug for RequestRefusal<AuthorityError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRequest => formatter.write_str("UnknownRequest"),
            Self::Authority { .. } => formatter.debug_struct("Authority").finish_non_exhaustive(),
        }
    }
}

impl<AuthorityError: fmt::Display> fmt::Display for RequestRefusal<AuthorityError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRequest => formatter.write_str("request was not issued by this export"),
            Self::Authority { cause } => write!(formatter, "current permission refused: {cause}"),
        }
    }
}

impl<AuthorityError: Error + 'static> Error for RequestRefusal<AuthorityError> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnknownRequest => None,
            Self::Authority { cause } => Some(cause),
        }
    }
}

/// One Application-associated declaration and retained concrete native operations.
/// Debug omits the target, requests, receipts and native owners.
pub struct Export<Target, Native, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
{
    session: Session,
    queryable: Queryable<()>,
    incoming: mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
    capacity: Weak<Semaphore>,
    target: ActorInterface<EstablishedRecipient<Target>>,
    maximum_entries: NonZeroUsize,
    same_request: fn(&Receipt, &Receipt) -> bool,
    entries: Vec<Request<Receipt, Native>>,
}

impl<Target, Native, Receipt> fmt::Debug for Export<Target, Native, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Export").finish_non_exhaustive()
    }
}

struct Request<Receipt, Native> {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
    receipt: Receipt,
    native: Native,
}

impl<Target, Native, Receipt> Export<Target, Native, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
{
    // Only Application supplies this interface after its exact native ownership check.
    pub(crate) async fn declare(
        session: Session,
        target: ActorInterface<EstablishedRecipient<Target>>,
        key: &str,
        limits: ExportLimits,
        same_request: fn(&Receipt, &Receipt) -> bool,
        refused: impl Fn(IngressError<Query>) + Send + Sync + 'static,
    ) -> Result<Self, ExportError> {
        let mut entries = Vec::new();
        if let Err(cause) = entries.try_reserve_exact(limits.retained_requests.get()) {
            return Err(ExportError::Storage { cause });
        }
        let (incoming, received) = mpsc::channel(limits.queued_queries.get());
        let capacity = Arc::new(Semaphore::new(limits.retained_queries.get()));
        let export_capacity = Arc::downgrade(&capacity);
        let declaration = session
            .declare_queryable(key)
            .callback(move |query| {
                if let Err(original) = try_retain(&incoming, &capacity, query) {
                    refused(original);
                }
            })
            .await;
        let queryable = match declaration {
            Ok(queryable) => queryable,
            Err(cause) => return Err(ExportError::Declaration { cause }),
        };
        Ok(Self {
            session,
            queryable,
            incoming: received,
            capacity: export_capacity,
            target,
            maximum_entries: limits.retained_requests,
            same_request,
            entries,
        })
    }

    /// Preserve the acquired channel result, including closure, before producer disposal.
    /// A full outside slot does not poll the channel again; keep each Query/permit together.
    pub async fn receive_query(
        &mut self,
        received: &mut Option<Option<(Query, OwnedSemaphorePermit)>>,
    ) {
        receive_query(&mut self.incoming, received).await;
    }

    /// Acquire a query or one eligible concrete native result.
    ///
    /// An occupied outside slot suppresses acquisition. Native work takes
    /// precedence; its trusted interpreter must store the original before
    /// producer disposal and permanently clear eligibility after acquisition.
    /// Under that contract, a ready query follows at most the configured
    /// lifetime request maximum native acquisitions. This is no latency bound.
    /// Each temporary future set allocates one node per eligible row. Cancelling
    /// this borrowed operation preserves rows and any acquired native originals.
    pub(crate) async fn receive(
        &mut self,
        query: &mut Option<Option<(Query, OwnedSemaphorePermit)>>,
        completed: &mut Option<RequestKey<Target>>,
        awaits_reply: impl Fn(&Native) -> bool,
        receive: impl AsyncFn(&mut Native),
    ) {
        if query.is_some() || completed.is_some() {
            return;
        }
        let receive = &receive;
        let capacity = &self.capacity;
        let mut replies = self
            .entries
            .iter_mut()
            .enumerate()
            .filter(|(_, request)| awaits_reply(&request.native))
            .map(|(row, request)| {
                let key = RequestKey {
                    export: capacity.clone(),
                    row,
                    target: PhantomData,
                };
                async move {
                    receive(&mut request.native).await;
                    key
                }
            })
            .collect::<FuturesUnordered<_>>();
        tokio::select! {
            biased;
            Some(key) = replies.next(), if !replies.is_empty() => {
                *completed = Some(key);
            }
            () = receive_query(&mut self.incoming, query) => {}
        }
    }

    /// Borrow exact originals and the concrete native operation.
    /// This trusted projection authenticates and publishes nothing. Recheck
    /// current reply permission before cached observation and publication.
    #[must_use]
    pub fn request(&self, key: &RequestKey<Target>) -> Option<(&[u8], &[u8], &Receipt, &Native)> {
        let row = self.request_row(key)?;
        let entry = &self.entries[row];
        Some((
            &entry.protected,
            &entry.proof,
            &entry.receipt,
            &entry.native,
        ))
    }

    // Only the concrete Bombay interpreter can construct/admit native work.
    // It retains the current permission loan through construction and storage.
    pub(crate) fn request_mut(
        &mut self,
        key: &RequestKey<Target>,
    ) -> Option<(
        &Receipt,
        &mut Native,
        &ActorInterface<EstablishedRecipient<Target>>,
    )> {
        let row = self.request_row(key)?;
        let entry = &mut self.entries[row];
        Some((&entry.receipt, &mut entry.native, &self.target))
    }

    /// Undeclare and return every accepted query and concrete native operation.
    /// Drain/dispose these affine owners under the supplied policy before
    /// connection retirement. Cancelling this whole operation surrenders them.
    #[expect(
        clippy::type_complexity,
        reason = "return each exact original and native owner without another custody product"
    )]
    pub async fn undeclare(
        self,
    ) -> (
        Result<(), ZenohError>,
        Session,
        mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
        impl ExactSizeIterator<Item = (RequestKey<Target>, Box<[u8]>, Box<[u8]>, Receipt, Native)>,
    ) {
        let Self {
            session,
            queryable,
            incoming,
            capacity,
            entries,
            ..
        } = self;
        let undeclared = queryable.undeclare().await;
        let requests = entries.into_iter().enumerate().map(move |(row, entry)| {
            let key = RequestKey {
                export: capacity.clone(),
                row,
                target: PhantomData,
            };
            (
                key,
                entry.protected,
                entry.proof,
                entry.receipt,
                entry.native,
            )
        });
        (undeclared, session, incoming, requests)
    }

    fn request_key(&self, row: usize) -> RequestKey<Target> {
        RequestKey {
            export: self.capacity.clone(),
            row,
            target: PhantomData,
        }
    }

    fn request_row(&self, request: &RequestKey<Target>) -> Option<usize> {
        if !request.export.ptr_eq(&self.capacity) {
            return None;
        }
        self.entries.get(request.row)?;
        Some(request.row)
    }

    /// Retain authenticated originals and their cold native operation.
    ///
    /// Call only after proof and current request-authority checks. This method
    /// performs neither. The supplied operation must be cold: no reply endpoint
    /// or control admission is constructed here. Identity and count checks
    /// precede the trusted interpreter's native construction. Rows are append-only
    /// for this export's lifetime; comparison must remain a stable equivalence
    /// relation over provider-owned logical identities across all operations.
    ///
    /// # Errors
    /// Conflict or count refusal returns every exact input. A duplicate returns
    /// its new originals and cold operation beside the existing request key.
    #[expect(
        clippy::type_complexity,
        reason = "return every original on duplicate and refusal without another custody product"
    )]
    pub fn retain(
        &mut self,
        protected: Box<[u8]>,
        proof: Box<[u8]>,
        receipt: Receipt,
        native: Native,
    ) -> Result<
        (
            RequestKey<Target>,
            Option<(Box<[u8]>, Box<[u8]>, Receipt, Native)>,
        ),
        ((Box<[u8]>, Box<[u8]>, Receipt, Native), ExportError),
    > {
        let existing = self
            .entries
            .iter()
            .position(|entry| (self.same_request)(&entry.receipt, &receipt));
        let original = (protected, proof, receipt, native);
        if let Some(row) = existing {
            let request = self.request_key(row);
            if self.entries[row].protected.as_ref() != original.0.as_ref() {
                return Err((original, ExportError::Conflict));
            }
            return Ok((request, Some(original)));
        }
        if self.entries.len() == self.maximum_entries.get() {
            return Err((original, ExportError::RetentionCapacity));
        }
        let row = self.entries.len();
        let (protected, proof, receipt, native) = original;
        self.entries.push(Request {
            protected,
            proof,
            receipt,
            native,
        });
        Ok((self.request_key(row), None))
    }
}

impl<Command> Admission<Command> {
    // Current permission is held by the concrete interpreter through this return.
    // A command constructor is pure; uncertainty begins before the actual send.
    pub(crate) fn attempt(
        &mut self,
        command: impl FnOnce() -> Command,
        send: impl FnOnce(Command) -> Result<(), TrySendError<Command>>,
    ) -> &Self {
        if matches!(self, Self::Unattempted) {
            let command = command();
            *self = Self::Uncertain;
            let original = send(command);
            *self = Self::Returned(original);
        }
        self
    }

    // Only the exact original Full result licenses another nonwaiting send.
    pub(crate) fn retry(
        &mut self,
        send: impl FnOnce(Command) -> Result<(), TrySendError<Command>>,
    ) -> &Self {
        let admission = mem::replace(self, Self::Uncertain);
        *self = match admission {
            Self::Returned(Err(TrySendError::Full(command))) => Self::Returned(send(command)),
            original => original,
        };
        self
    }
}

#[expect(
    clippy::option_option,
    reason = "preserve no acquisition, acquired closure and acquired Query as separate facts"
)]
async fn receive_query(
    incoming: &mut mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
    received: &mut Option<Option<(Query, OwnedSemaphorePermit)>>,
) {
    if received.is_some() {
        return;
    }
    let mut receiving = pin!(incoming.recv());
    poll_fn(|context| match receiving.as_mut().poll(context) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(original) => {
            *received = Some(original);
            Poll::Ready(())
        }
    })
    .await;
}

#[cfg(test)]
#[path = "export/requests.rs"]
mod requests;

#[cfg(test)]
#[path = "export/ledger.rs"]
mod ledger;
