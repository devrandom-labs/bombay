//! One Node-associated declaration and its retained requests.
use super::{IngressError, try_retain};
use crate::TrySendError;
use crate::address::MailAddr;
use crate::application::{ActorInterface, ApplicationClient, ExternalActorError};
use behavior::{EstablishedRecipient, Protocol, User};
use core::fmt;
use core::future::{Future, poll_fn};
use core::marker::PhantomData;
use core::mem;
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
/// One runtime-issued request reference for its target protocol and issuing export.
/// Fields and construction stay private; a key carries no reply or lifecycle authority.
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
/// The actual mailbox attempt, including interrupted-return uncertainty.
/// Debug identifies the disposition while omitting the original command.
pub enum Admission<Command> {
    Unattempted,
    Uncertain,
    Returned(Result<(), TrySendError<Command>>),
}

/// This attempt issued no new admission; prior acquired facts remain retained.
/// Debug omits the cause; Display delegates to it. Standard Error source access
/// requires an Error + 'static cause. Attempt and retry still accept borrowed causes.
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

/// One Node-associated declaration and retained requests with independent native reply clients.
/// Debug omits the target, requests, receipts and native owners.
pub struct Export<Target, Replies, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    session: Session,
    queryable: Queryable<()>,
    issuer: Weak<Semaphore>,
    incoming: mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
    target: ActorInterface<EstablishedRecipient<Target>>,
    maximum_entries: NonZeroUsize,
    same_request: fn(&Receipt, &Receipt) -> bool,
    entries: Vec<Request<Target, Replies, Receipt>>,
}
impl<Target, Replies, Receipt> fmt::Debug for Export<Target, Replies, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Export").finish_non_exhaustive()
    }
}

struct Request<Target, Replies, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    client: ApplicationClient<EstablishedRecipient<Target>, Replies>,
    protected: Box<[u8]>,
    proof: Box<[u8]>,
    receipt: Receipt,
    admission: Admission<Target::Msg>,
    #[expect(
        clippy::option_option,
        reason = "distinguish no acquisition from an acquired closed native stream"
    )]
    native: Option<Option<User<MailAddr, Replies::Msg>>>,
}

impl<Target, Replies, Receipt> Request<Target, Replies, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    fn awaits_reply(&self) -> bool {
        self.native.is_none()
            && matches!(
                self.admission,
                Admission::Uncertain | Admission::Returned(Ok(()))
            )
    }

    async fn receive(&mut self)
    where
        Replies::Msg: Send,
    {
        let native = &mut self.native;
        let mut receiving = pin!(self.client.receive());
        poll_fn(|context| match receiving.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(original) => {
                *native = Some(original);
                Poll::Ready(())
            }
        })
        .await;
    }
}

impl<Target, Replies, Receipt> Export<Target, Replies, Receipt>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
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
        let issuer = Arc::downgrade(&capacity);
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
            issuer,
            incoming: received,
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

    /// Acquire a query or one request's native receiving result.
    ///
    /// Either occupied outside slot suppresses further polling. A native key
    /// identifies an acquired User or EOF retained in its existing request row;
    /// it does not establish protocol validity or business completion.
    /// Actual pending native waits remain alive while this operation is parked.
    /// Dropping this operation cancels those waits, preserving acquired rows.
    ///
    /// Native acquisitions take precedence. Each append-only row acquires once,
    /// so a ready query follows at most the configured lifetime request maximum
    /// native acquisitions, assuming continued polling and available host budget.
    /// This is not a latency promise or mailbox-capacity notification.
    /// The temporary future set rebuilds one child node per eligible row, plus
    /// fixed collection overhead. Keep the Query and its count permit together.
    pub async fn receive(
        &mut self,
        query: &mut Option<Option<(Query, OwnedSemaphorePermit)>>,
        completed: &mut Option<RequestKey<Target>>,
    ) {
        if query.is_some() || completed.is_some() {
            return;
        }
        let issuer = &self.issuer;
        let mut replies = self
            .entries
            .iter_mut()
            .enumerate()
            .filter(|(_, request)| request.awaits_reply())
            .map(|(row, request)| {
                let key = RequestKey {
                    issuer: issuer.clone(),
                    row,
                    protocol: PhantomData,
                };
                async move {
                    request.receive().await;
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

    /// Borrow this retained request's exact originals and optional native acquisition.
    /// This trusted local projection authenticates nothing and publishes nothing.
    /// Recheck current reply permission before cached observation and again before publication.
    #[must_use]
    #[expect(
        clippy::type_complexity,
        reason = "borrow every independent retained original without a second request owner"
    )]
    pub fn request(
        &self,
        key: &RequestKey<Target>,
    ) -> Option<(
        &[u8],
        &[u8],
        &Receipt,
        &Admission<Target::Msg>,
        &Option<Option<User<MailAddr, Replies::Msg>>>,
    )> {
        let row = self.request_row(key)?;
        let entry = &self.entries[row];
        Some((
            &entry.protected,
            &entry.proof,
            &entry.receipt,
            &entry.admission,
            &entry.native,
        ))
    }

    /// Undeclare without discarding either accepted query prefix or native actor replies.
    /// The receiver and each returned request client remain affine. Drain/dispose them
    /// under the supplied policy, including late replies on previously acquired requests.
    /// Explicit Session close and Application native/two notifications remain independent.
    /// Dropping this whole operation surrenders its owners; a borrowed wait does not.
    pub async fn undeclare(
        self,
    ) -> (
        Result<(), ZenohError>,
        Session,
        mpsc::Receiver<(Query, OwnedSemaphorePermit)>,
        impl ExactSizeIterator<
            Item = (
                RequestKey<Target>,
                Box<[u8]>,
                Box<[u8]>,
                Receipt,
                Admission<Target::Msg>,
                Option<Option<User<MailAddr, Replies::Msg>>>,
                ApplicationClient<EstablishedRecipient<Target>, Replies>,
            ),
        >,
    ) {
        let Self {
            session,
            queryable,
            issuer,
            incoming,
            target: _,
            entries,
            maximum_entries: _,
            same_request: _,
        } = self;
        let undeclared = queryable.undeclare().await;
        let requests = entries.into_iter().enumerate().map(move |(row, entry)| {
            let key = RequestKey {
                issuer: issuer.clone(),
                row,
                protocol: PhantomData,
            };
            (
                key,
                entry.protected,
                entry.proof,
                entry.receipt,
                entry.admission,
                entry.native,
                entry.client,
            )
        });
        (undeclared, session, incoming, requests)
    }

    fn request_key(&self, row: usize) -> RequestKey<Target> {
        RequestKey {
            issuer: self.issuer.clone(),
            row,
            protocol: PhantomData,
        }
    }
    fn request_row(&self, request: &RequestKey<Target>) -> Option<usize> {
        if !Weak::ptr_eq(&self.issuer, &request.issuer) {
            return None;
        }
        self.entries.get(request.row).map(|_| request.row)
    }

    /// Retain authenticated request originals under the provider's logical identity.
    ///
    /// Call after proof and current request-authority checks. This method performs
    /// neither check. A duplicate returns its new original buffers and receipt;
    /// independent identities may have identical protected bytes. Entries are
    /// append-only for this export's lifetime, including completed requests.
    /// The constructor's comparison must be a stable equivalence relation over
    /// provider-owned logical request identities throughout that lifetime.
    ///
    /// # Errors
    /// Returns the exact supplied buffers and receipt on identity conflict or
    /// retained-request capacity exhaustion or native reply-endpoint refusal.
    /// Duplicate, conflict and count checks precede new endpoint allocation.
    /// No original is reconstructed.
    #[expect(
        clippy::type_complexity,
        reason = "return the exact request/proof/receipt product on both duplicate and refusal without another custody wrapper"
    )]
    pub fn retain(
        &mut self,
        protected: Box<[u8]>,
        proof: Box<[u8]>,
        receipt: Receipt,
    ) -> Result<
        (RequestKey<Target>, Option<(Box<[u8]>, Box<[u8]>, Receipt)>),
        ((Box<[u8]>, Box<[u8]>, Receipt), ExportError),
    > {
        let existing = self
            .entries
            .iter()
            .position(|entry| (self.same_request)(&entry.receipt, &receipt));
        let original = (protected, proof, receipt);
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
        let client = match self
            .target
            .client_to::<_, Replies>(self.target.api().clone())
        {
            Ok(client) => client,
            Err((target, cause)) => {
                // The certified interface retains this same immutable target capability.
                drop(target);
                return Err((original, ExportError::Endpoint { cause }));
            }
        };
        let row = self.entries.len();
        let (protected, proof, receipt) = original;
        self.entries.push(Request {
            client,
            protected,
            proof,
            receipt,
            admission: Admission::Unattempted,
            native: None,
        });
        Ok((self.request_key(row), None))
    }

    /// Acquire the next raw item from this request's own native reply client.
    ///
    /// Each newly retained request has a separate receiving endpoint; a late item
    /// on another request's endpoint cannot be consumed here. Protocol origin and
    /// correlation validation remain the application's responsibility.
    /// The original result is stored before the receiving future is disposed.
    /// Cancelling this borrowed wait preserves the row and its receiving endpoint.
    /// Unattempted, refused and already acquired requests do not poll again.
    /// Uncertain admission may still have produced a reply; waiting does not prove it did.
    /// Returns None only for a key not issued by this export; Some does not promise
    /// a business reply when this row has no receivable native operation.
    pub async fn receive_native(&mut self, request: &RequestKey<Target>) -> Option<()> {
        let row = self.request_row(request)?;
        let retained = &mut self.entries[row];
        if retained.awaits_reply() {
            retained.receive().await;
        }
        Some(())
    }

    /// Check current permission and attempt this retained command without waiting.
    ///
    /// The caller's current permission/time snapshot must prevent local authority
    /// updates while its returned loan is held. Bombay retains that loan through
    /// command construction, native mailbox attempt and result storage. There is
    /// no serialization or capacity wait here. This is a logical local decision,
    /// not a promise about physical queue-publication time or remote revocation.
    ///
    /// Only an unattempted request constructs a command. Interrupted native return
    /// remains Uncertain and cannot license another send. A repeated call checks
    /// current authority again and leaves the prior admission fact unchanged.
    /// Permission refusal never rewrites an already acquired native result.
    ///
    /// # Errors
    /// Returns the original caller-owned permission cause or an unknown key. Native Full/Closed
    /// originals remain in Admission rather than becoming permission failures.
    pub fn attempt<'entry, Guard, AuthorityError>(
        &'entry mut self,
        request: &RequestKey<Target>,
        current: impl FnOnce(&'entry Receipt) -> Result<Guard, AuthorityError>,
        command: impl FnOnce(EstablishedRecipient<Replies>) -> Target::Msg,
    ) -> Result<&'entry Admission<Target::Msg>, RequestRefusal<AuthorityError>> {
        let row = self
            .request_row(request)
            .ok_or(RequestRefusal::UnknownRequest)?;
        let retained = &mut self.entries[row];
        let authority =
            current(&retained.receipt).map_err(|cause| RequestRefusal::Authority { cause })?;
        if matches!(&retained.admission, Admission::Unattempted) {
            let command = command(retained.client.recipient());
            retained.admission = Admission::Uncertain;
            let original = retained.client.try_send(command);
            retained.admission = Admission::Returned(original);
        }
        drop(authority);
        Ok(&retained.admission)
    }

    /// Retry the exact command returned by a full mailbox, without waiting.
    ///
    /// As with [`Self::attempt`], the current permission/time loan must exclude
    /// local authority updates. Every recognized request obtains a fresh loan;
    /// refusal leaves all retained facts unchanged. No command is reconstructed.
    /// Only a retained native Full result permits another send through the same
    /// request's client. Unattempted, uncertain, admitted and closed results stay
    /// unchanged. An Ok return borrows that fact, not a promise of a new send.
    ///
    /// The complete native return is stored before releasing the permission
    /// loan. Interrupted native return remains uncertain and cannot be retried.
    /// Capacity waiting, scheduling and deadlines belong to the caller's policy.
    ///
    /// # Errors
    /// Returns an unknown-key refusal or the original current-permission cause.
    /// Native Full/Closed originals remain in the returned admission fact.
    pub fn retry<'entry, Guard, AuthorityError>(
        &'entry mut self,
        request: &RequestKey<Target>,
        current: impl FnOnce(&'entry Receipt) -> Result<Guard, AuthorityError>,
    ) -> Result<&'entry Admission<Target::Msg>, RequestRefusal<AuthorityError>> {
        let row = self
            .request_row(request)
            .ok_or(RequestRefusal::UnknownRequest)?;
        let retained = &mut self.entries[row];
        let authority =
            current(&retained.receipt).map_err(|cause| RequestRefusal::Authority { cause })?;
        let admission = mem::replace(&mut retained.admission, Admission::Uncertain);
        retained.admission = match admission {
            Admission::Returned(Err(TrySendError::Full(command))) => {
                let original = retained.client.try_send(command);
                Admission::Returned(original)
            }
            original => original,
        };
        drop(authority);
        Ok(&retained.admission)
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
