//! Node-owned outgoing requests, acquired responses and their independent facts.
//! Native query resources stay private; consuming handoff explicitly discharges
//! unused envelopes while preserving original typed results and charged errors.

use super::export::Exports;
use super::verification::provide;
use super::{Attempt, ExportError, IngressError, json, try_retain};
use crate::MailAddr;
use crate::application::{ApplicationHandle, ExternalActor};
use crate::local::endpoint::{ActorRef, ExtractLocalEndpoint};
use behavior::{EstablishedRecipient, Protocol, User};
use communication::TrySendError;
use futures_util::future::{Either, Shared};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::any::Any;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::fmt;
use std::mem::replace;
use std::num::NonZeroUsize;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use zenoh::Session;
use zenoh::qos::CongestionControl;
use zenoh::query::{
    ConsolidationMode, Querier, QueryTarget, Reply as NativeReply, ReplyError as NativeReplyError,
};
use zenoh::sample::Locality;

/// A local outgoing boundary refusal; originals remain in their owning row.
#[derive(Error)]
pub enum ClientError {
    /// The declared numeric origin differs from the checked return address.
    /// This comparison is routing policy, not sender authentication.
    #[error("declared outgoing origin does not match the return address")]
    Origin { expected: MailAddr },
    /// Protected framing or a selected byte/parser limit was refused.
    #[error("outgoing protected framing was refused")]
    Json {
        #[source]
        cause: json::Error,
    },
    /// A successful native response had no separate proof attachment.
    #[error("response proof attachment is absent")]
    MissingProof,
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ClientError")
            .field(&format_args!("{self}"))
            .finish()
    }
}

/// One acquired response and its independent verification and native-delivery facts.
///
/// The native envelope and count charge remain private. Borrowed raw inspection
/// does not authenticate the payload or claim successful verification/admission.
pub struct Response<Receipt, Message, VerificationError, DecodingError, ReplyError> {
    // Exact native object and its count charge stay together, in this order.
    original: (NativeReply, OwnedSemaphorePermit),
    #[expect(
        clippy::type_complexity,
        reason = "the original protected bytes and separate proof coexist without another owner"
    )]
    wire: RefCell<Option<(Box<[u8]>, Box<[u8]>)>>,
    shape: RefCell<Option<Result<(), ClientError>>>,
    verified: RefCell<Option<Result<Receipt, VerificationError>>>,
    #[expect(
        clippy::type_complexity,
        reason = "one concrete slot retains the decoded message or its exact native return and the original decode cause"
    )]
    decoded: RefCell<
        Option<Result<Either<Message, Attempt<Result<(), TrySendError<Message>>>>, DecodingError>>,
    >,
    permission: RefCell<Option<ReplyError>>,
    verification_factory: RefCell<Option<Box<dyn Any + Send>>>,
    verification_poll: RefCell<Option<Box<dyn Any + Send>>>,
    verification_disposal: RefCell<Option<Box<dyn Any + Send>>>,
}

impl<Receipt, Message, VerificationError, DecodingError, ReplyError> fmt::Debug
    for Response<Receipt, Message, VerificationError, DecodingError, ReplyError>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Response").finish_non_exhaustive()
    }
}

impl<Receipt, Message, VerificationError, DecodingError, ReplyError>
    Response<Receipt, Message, VerificationError, DecodingError, ReplyError>
{
    fn new(original: (NativeReply, OwnedSemaphorePermit)) -> Self {
        Self {
            original,
            wire: RefCell::new(None),
            shape: RefCell::new(None),
            verified: RefCell::new(None),
            decoded: RefCell::new(None),
            permission: RefCell::new(None),
            verification_factory: RefCell::new(None),
            verification_poll: RefCell::new(None),
            verification_disposal: RefCell::new(None),
        }
    }

    /// Borrow the exact received payload fragments without copying them.
    pub fn payload(&self) -> impl Iterator<Item = &[u8]> {
        match self.original.0.result() {
            Ok(sample) => sample.payload().slices(),
            Err(cause) => cause.payload().slices(),
        }
    }

    /// Borrow the actual optional success attachment, retaining empty versus absent.
    pub fn proof(&self) -> Option<impl Iterator<Item = &[u8]>> {
        self.original
            .0
            .result()
            .ok()
            .and_then(|sample| sample.attachment())
            .map(|proof| proof.slices())
    }

    /// Borrow the exact native error; its private payload remains owned here.
    pub fn cause(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.original
            .0
            .result()
            .err()
            .map(|cause| cause as &(dyn StdError + Send + Sync + 'static))
    }

    /// Transfer acquired typed facts and discharge unused native envelope metadata.
    ///
    /// A successful Sample is disposed before its count charge. An actual native
    /// error retains its payload and charge together in the returned standard
    /// error owner; `Error::source` borrows that exact native cause. No rejected
    /// payload is materialized by this handoff. Inspect borrowed fragments first
    /// when those original bytes are needed. Native metadata is deliberately
    /// discharged, not projected as protected or authenticated evidence.
    #[expect(
        clippy::type_complexity,
        reason = "consuming handoff preserves every independent typed response result and original cause"
    )]
    pub fn into_parts(
        self,
    ) -> (
        Option<Box<dyn StdError + Send + Sync>>,
        Option<(Box<[u8]>, Box<[u8]>)>,
        Option<Result<(), ClientError>>,
        Option<Result<Receipt, VerificationError>>,
        Option<Result<Either<Message, Attempt<Result<(), TrySendError<Message>>>>, DecodingError>>,
        Option<ReplyError>,
        (
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
        ),
    ) {
        let (original, permit) = self.original;
        let cause: Option<Box<dyn StdError + Send + Sync>> = match original.into_result() {
            Ok(sample) => {
                drop(sample);
                drop(permit);
                None
            }
            Err(cause) => Some(Box::new(ResponseError {
                cause,
                _permit: permit,
            })),
        };
        (
            cause,
            self.wire.into_inner(),
            self.shape.into_inner(),
            self.verified.into_inner(),
            self.decoded.into_inner(),
            self.permission.into_inner(),
            (
                self.verification_factory.into_inner(),
                self.verification_poll.into_inner(),
                self.verification_disposal.into_inner(),
            ),
        )
    }
}

// This private owner conserves a native error's charged original until actual
// error disposal. It is not a public metadata facade. Field order is deliberate.
#[derive(Error)]
#[error("native response contains an error")]
struct ResponseError {
    #[source]
    cause: NativeReplyError,
    _permit: OwnedSemaphorePermit,
}

impl fmt::Debug for ResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseError")
            .finish_non_exhaustive()
    }
}

struct Request<
    Requests,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
> where
    Requests: Protocol<Addr = MailAddr>,
{
    original: User<MailAddr, Requests::Msg>,
    refusal: RefCell<Option<ClientError>>,
    #[expect(
        clippy::type_complexity,
        reason = "encoder failure, the protocol no-Get choice and actual owned bytes remain distinct"
    )]
    encoded: RefCell<Option<Result<Option<Box<[u8]>>, EncodingError>>>,
    protected: RefCell<Option<Result<Box<[u8]>, ProtectionError>>>,
    permission: RefCell<Option<RequestError>>,
    capacity: RefCell<Option<AcquireError>>,
    query: RefCell<Attempt<Result<(), Box<dyn StdError + Send + Sync>>>>,
    incoming: RefCell<Option<mpsc::Receiver<(NativeReply, OwnedSemaphorePermit)>>>,
    callback_refusal: Arc<Mutex<Option<IngressError<NativeReply>>>>,
    #[expect(
        clippy::type_complexity,
        reason = "each response retains its concrete receipt, message and independent verification and permission causes"
    )]
    replies: RefCell<Vec<Response<Receipt, Message, VerificationError, DecodingError, ReplyError>>>,
    // Acquired ONLY from actual receiver None. Native timeout Reply is not EOF.
    callback_eof: RefCell<Option<()>>,
    protection_factory: RefCell<Option<Box<dyn Any + Send>>>,
    protection_poll: RefCell<Option<Box<dyn Any + Send>>>,
    protection_disposal: RefCell<Option<Box<dyn Any + Send>>>,
    operation_factory: RefCell<Option<Box<dyn Any + Send>>>,
    operation_poll: RefCell<Option<Box<dyn Any + Send>>>,
    operation_disposal: RefCell<Option<Box<dyn Any + Send>>>,
    // Unit says the operation returned, not that any protocol stage succeeded.
    completed: RefCell<Option<()>>,
}

impl<
    Requests,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
>
    Request<
        Requests,
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
{
    fn new(original: User<MailAddr, Requests::Msg>) -> Self {
        Self {
            original,
            refusal: RefCell::new(None),
            encoded: RefCell::new(None),
            protected: RefCell::new(None),
            permission: RefCell::new(None),
            capacity: RefCell::new(None),
            query: RefCell::new(Attempt::Unattempted),
            incoming: RefCell::new(None),
            callback_refusal: Arc::new(Mutex::new(None)),
            replies: RefCell::new(Vec::new()),
            callback_eof: RefCell::new(None),
            protection_factory: RefCell::new(None),
            protection_poll: RefCell::new(None),
            protection_disposal: RefCell::new(None),
            operation_factory: RefCell::new(None),
            operation_poll: RefCell::new(None),
            operation_disposal: RefCell::new(None),
            completed: RefCell::new(None),
        }
    }
}

/// Explicit outgoing operation and retained-original maxima; no defaults.
///
/// These count limits do not bound aggregate bytes, provider allocations or the
/// already accepted unread prefix retained by the original closed source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientLimits {
    /// Maximum operations holding an active request permit through protection,
    /// native Get and response handling. Waiting source rows count separately.
    pub concurrent_requests: NonZeroUsize,
    /// Maximum acquired source rows, including no-Get acknowledgments. Reaching
    /// this count closes source admission; an unread accepted prefix stays owned.
    pub retained_requests: NonZeroUsize,
    /// Maximum charged native response originals per query, across callback
    /// queue and acquired rows. The first refused original is retained separately
    /// for diagnostics: total native custody is this count plus at most one.
    /// Later overflow originals are explicitly discharged.
    pub retained_responses: NonZeroUsize,
}

/// A cold outgoing source, explicit policies, and bounded acquired request custody.
pub struct Client<
    Requests,
    Inputs,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
> where
    Requests: Protocol<Addr = MailAddr>,
{
    source: ExternalActor<Requests>,
    inputs: Inputs,
    #[expect(
        clippy::type_complexity,
        reason = "each source row retains concrete originals and results outside its disposable producer"
    )]
    requests: RefCell<
        Vec<
            Rc<
                Request<
                    Requests,
                    Receipt,
                    Message,
                    EncodingError,
                    ProtectionError,
                    VerificationError,
                    DecodingError,
                    RequestError,
                    ReplyError,
                >,
            >,
        >,
    >,
    querier: Option<Querier<'static>>,
    startup: Option<Result<(), ExportError>>,
    withdrawal: Option<Result<(), Box<dyn StdError + Send + Sync>>>,
    joined: Option<Result<(), RecvError>>,
    // Acquired from actual source.receive() == None; quota exhaustion leaves None.
    source_eof: Option<()>,
}

impl<
    Requests,
    Inputs,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
> fmt::Debug
    for Client<
        Requests,
        Inputs,
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Client").finish_non_exhaustive()
    }
}

impl<
    Requests,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
    Encode,
    Protect,
    Verify,
    Decode,
    CurrentRequest,
    CurrentReply,
    RequestGuard,
    ReplyGuard,
>
    Client<
        Requests,
        (
            String,
            ClientLimits,
            json::Limits,
            Duration,
            Encode,
            Protect,
            Verify,
            Decode,
            CurrentRequest,
            CurrentReply,
        ),
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
    Requests::Msg: Send,
    Encode: Fn(
        &User<MailAddr, Requests::Msg>,
        &str,
        MailAddr,
    ) -> Result<Option<Box<[u8]>>, EncodingError>,
    Protect: AsyncFn(&[u8], &User<MailAddr, Requests::Msg>) -> Result<Box<[u8]>, ProtectionError>,
    Verify: AsyncFn(&[u8], &[u8]) -> Result<Receipt, VerificationError>,
    Decode: Fn(&Receipt, &[u8], &User<MailAddr, Requests::Msg>) -> Result<Message, DecodingError>,
    CurrentRequest:
        Fn(&User<MailAddr, Requests::Msg>, &str, MailAddr) -> Result<RequestGuard, RequestError>,
    CurrentReply: Fn(
        &Receipt,
        &User<MailAddr, Requests::Msg>,
        &str,
        MailAddr,
    ) -> Result<ReplyGuard, ReplyError>,
{
    /// Create a cold outgoing client with explicit source and query policies.
    ///
    /// `source` receives the actor's actual messages; `key` selects the native
    /// query destination. `limits` explicitly selects concurrent operations,
    /// acquired source rows and charged responses per query. It also documents
    /// the separately retained first-refusal original and source quota policy.
    /// `json_limits` independently selects the protected framing checks.
    /// `timeout` selects the native query deadline and
    /// does not certify actor termination or application processing.
    ///
    /// The encoder's `Ok(None)` selects no Get. The protector, verifier, decoder
    /// and current-permission policies remain distinct caller-supplied inputs.
    /// Construction retains these inputs without starting providers or a query.
    #[expect(
        clippy::too_many_arguments,
        reason = "the source, destination, limits and independent typed policies are explicit inputs"
    )]
    pub fn new(
        source: ExternalActor<Requests>,
        key: impl Into<String>,
        limits: ClientLimits,
        json_limits: json::Limits,
        timeout: Duration,
        encode: Encode,
        protect: Protect,
        verify: Verify,
        decode: Decode,
        current_request: CurrentRequest,
        current_reply: CurrentReply,
    ) -> Self {
        Self {
            source,
            inputs: (
                key.into(),
                limits,
                json_limits,
                timeout,
                encode,
                protect,
                verify,
                decode,
                current_request,
                current_reply,
            ),
            requests: RefCell::new(Vec::new()),
            querier: None,
            startup: None,
            withdrawal: None,
            joined: None,
            source_eof: None,
        }
    }

    // Synchronous selection is also used for the remaining accepted prefix at
    // join. No new provider/Get starts while that prefix is being retired.
    fn select(
        inputs: &(
            String,
            ClientLimits,
            json::Limits,
            Duration,
            Encode,
            Protect,
            Verify,
            Decode,
            CurrentRequest,
            CurrentReply,
        ),
        request: &Request<
            Requests,
            Receipt,
            Message,
            EncodingError,
            ProtectionError,
            VerificationError,
            DecodingError,
            RequestError,
            ReplyError,
        >,
        runtime: &str,
        target: MailAddr,
    ) {
        // This compares a DECLARED routing address only. MailAddr can repeat
        // across Applications and public send_from can supply it explicitly.
        // Neither this match nor Source custody authenticates the sender.
        if request.original.from != target {
            *request.refusal.borrow_mut() = Some(ClientError::Origin { expected: target });
            return;
        }
        let permission = match (inputs.8)(&request.original, runtime, target) {
            Ok(permission) => permission,
            Err(cause) => {
                *request.permission.borrow_mut() = Some(cause);
                return;
            }
        };
        *request.encoded.borrow_mut() = Some((inputs.4)(&request.original, runtime, target));
        drop(permission);
    }

    #[expect(
        clippy::too_many_arguments,
        clippy::await_holding_refcell_ref,
        reason = "immutable original encoding and proof and the sole native receiver stay outside producers; authority guards end before suspending waits, while Get enters and returns synchronously through the selected Ready future"
    )]
    async fn request<Target>(
        inputs: &(
            String,
            ClientLimits,
            json::Limits,
            Duration,
            Encode,
            Protect,
            Verify,
            Decode,
            CurrentRequest,
            CurrentReply,
        ),
        request: &Request<
            Requests,
            Receipt,
            Message,
            EncodingError,
            ProtectionError,
            VerificationError,
            DecodingError,
            RequestError,
            ReplyError,
        >,
        querier: &Querier<'static>,
        endpoint: &ActorRef<Target>,
        source: MailAddr,
        target: MailAddr,
        runtime: &str,
        capacity: Arc<Semaphore>,
        joined: Shared<oneshot::Receiver<()>>,
    ) where
        Target: Protocol<Addr = MailAddr, Msg = Message>,
    {
        let encoding = request.encoded.borrow();
        let Some(Ok(Some(encoded))) = encoding.as_ref() else {
            return;
        };
        let _permit = match capacity.acquire_owned().await {
            Ok(permit) => permit,
            Err(cause) => {
                *request.capacity.borrow_mut() = Some(cause);
                return;
            }
        };
        provide(
            || (inputs.5)(encoded, &request.original),
            &request.protected,
            &request.protection_factory,
            &request.protection_poll,
            &request.protection_disposal,
            joined.clone(),
        )
        .await;
        if request.protection_factory.borrow().is_some()
            || request.protection_poll.borrow().is_some()
            || request.protection_disposal.borrow().is_some()
        {
            return;
        }
        let protection = request.protected.borrow();
        let Some(Ok(proof)) = protection.as_ref() else {
            return;
        };
        if let Err(cause) = json::check(encoded, proof, &inputs.2) {
            *request.refusal.borrow_mut() = Some(ClientError::Json { cause });
            return;
        }
        // The original provider output is already outside its disposed future.
        let permission = match (inputs.8)(&request.original, runtime, target) {
            Ok(permission) => permission,
            Err(cause) => {
                *request.permission.borrow_mut() = Some(cause);
                return;
            }
        };
        // R bounds charged native replies, including retained terminal rows.
        // The explicit first-refusal policy below retains at most one further
        // original: total native reply custody is R + 1 per submitted request.
        let maximum = inputs.1.retained_responses.get();
        let (publication, incoming) = mpsc::channel(maximum);
        *request.incoming.borrow_mut() = Some(incoming);
        let replies = Arc::new(Semaphore::new(maximum));
        let refused = Arc::clone(&request.callback_refusal);
        *request.query.borrow_mut() = Attempt::Uncertain;
        // IntoFuture performs actual synchronous native entry; state above
        // precedes it. Only native Send owners enter this callback.
        let returned = querier
            .get()
            .payload(encoded.to_vec())
            .attachment(proof.to_vec())
            .callback(move |reply| {
                if let Err(original) = try_retain(&publication, &replies, reply) {
                    let mut refused = refused.lock().unwrap_or_else(PoisonError::into_inner);
                    match refused.as_ref() {
                        None => *refused = Some(original),
                        Some(_) => {
                            // Explicit finite overflow discharge: preserve the
                            // first exact typed refusal, dispose later originals.
                            // Queue tuple disposal keeps Reply before permit.
                            drop(original);
                        }
                    }
                }
            })
            .await;
        *request.query.borrow_mut() = Attempt::Returned(returned);
        drop(permission);
        // Do not hold raw/proof Ref guards for native reply reception.
        drop(protection);
        drop(encoding);
        loop {
            let original = {
                let mut incoming = request.incoming.borrow_mut();
                let Some(incoming) = incoming.as_mut() else {
                    return;
                };
                incoming.recv().await
            };
            let Some(original) = original else {
                *request.callback_eof.borrow_mut() = Some(());
                return;
            };
            let reply = Response::new(original);
            let position = request.replies.borrow().len();
            request.replies.borrow_mut().push(reply);
            let replies = request.replies.borrow();
            Self::reply(
                inputs,
                request,
                &replies[position],
                endpoint,
                source,
                target,
                runtime,
                joined.clone(),
            )
            .await;
        }
    }

    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::await_holding_refcell_ref,
        reason = "one response operation retains immutable wire and receipt facts beside native returns; fresh authority and mutable message loans end before capacity waits"
    )]
    async fn reply<Target>(
        inputs: &(
            String,
            ClientLimits,
            json::Limits,
            Duration,
            Encode,
            Protect,
            Verify,
            Decode,
            CurrentRequest,
            CurrentReply,
        ),
        request: &Request<
            Requests,
            Receipt,
            Message,
            EncodingError,
            ProtectionError,
            VerificationError,
            DecodingError,
            RequestError,
            ReplyError,
        >,
        reply: &Response<Receipt, Message, VerificationError, DecodingError, ReplyError>,
        endpoint: &ActorRef<Target>,
        source: MailAddr,
        target: MailAddr,
        runtime: &str,
        joined: Shared<oneshot::Receiver<()>>,
    ) where
        Target: Protocol<Addr = MailAddr, Msg = Message>,
    {
        // The exact native Err remains in original, not a fake checked message.
        let Ok(sample) = reply.original.0.result() else {
            return;
        };
        let Some(proof) = sample.attachment() else {
            *reply.shape.borrow_mut() = Some(Err(ClientError::MissingProof));
            return;
        };
        if sample.payload().len() > inputs.2.input_bytes.get() {
            *reply.shape.borrow_mut() = Some(Err(ClientError::Json {
                cause: json::Error::InputBytes,
            }));
            return;
        }
        if proof.len() > inputs.2.proof_bytes.get() {
            *reply.shape.borrow_mut() = Some(Err(ClientError::Json {
                cause: json::Error::ProofBytes,
            }));
            return;
        }
        let original = (
            sample.payload().to_bytes().into_owned().into_boxed_slice(),
            proof.to_bytes().into_owned().into_boxed_slice(),
        );
        let shape = json::check(&original.0, &original.1, &inputs.2)
            .map_err(|cause| ClientError::Json { cause });
        *reply.wire.borrow_mut() = Some(original);
        *reply.shape.borrow_mut() = Some(shape);
        if matches!(reply.shape.borrow().as_ref(), Some(Err(_))) {
            return;
        }
        let wire = reply.wire.borrow();
        let Some(wire) = wire.as_ref() else {
            return;
        };
        provide(
            || (inputs.6)(&wire.0, &wire.1),
            &reply.verified,
            &reply.verification_factory,
            &reply.verification_poll,
            &reply.verification_disposal,
            joined.clone(),
        )
        .await;
        if reply.verification_factory.borrow().is_some()
            || reply.verification_poll.borrow().is_some()
            || reply.verification_disposal.borrow().is_some()
        {
            return;
        }
        let verified = reply.verified.borrow();
        let Some(Ok(receipt)) = verified.as_ref() else {
            return;
        };
        let permission = match (inputs.9)(receipt, &request.original, runtime, target) {
            Ok(permission) => permission,
            Err(cause) => {
                *reply.permission.borrow_mut() = Some(cause);
                return;
            }
        };
        // Preserve the actual constructed message before ending this loan and
        // requesting fresh authority. A refusal cannot erase the message.
        *reply.decoded.borrow_mut() =
            Some((inputs.7)(receipt, &wire.0, &request.original).map(Either::Left));
        drop(permission);
        loop {
            let permission = match (inputs.9)(receipt, &request.original, runtime, target) {
                Ok(permission) => permission,
                Err(cause) => {
                    *reply.permission.borrow_mut() = Some(cause);
                    return;
                }
            };
            {
                let mut decoded = reply.decoded.borrow_mut();
                let Some(Ok(original)) = decoded.as_mut() else {
                    return;
                };
                // Only an owned unattempted message or exact native Full is
                // eligible. Returned success/Closed/Uncertain never re-enter.
                match original {
                    Either::Left(_)
                    | Either::Right(Attempt::Returned(Err(TrySendError::Full(_)))) => {}
                    Either::Right(_) => return,
                }
                let original = replace(original, Either::Right(Attempt::Uncertain));
                let (Either::Left(message)
                | Either::Right(Attempt::Returned(Err(TrySendError::Full(message))))) = original
                else {
                    unreachable!("the exact eligible original was checked without an await")
                };
                let returned = endpoint.try_send_from(source, message);
                *decoded = Some(Ok(Either::Right(Attempt::Returned(returned))));
            }
            drop(permission);
            {
                let decoded = reply.decoded.borrow();
                match decoded.as_ref() {
                    Some(Ok(Either::Right(Attempt::Returned(Err(TrySendError::Full(_)))))) => {}
                    _ => return,
                }
            }
            // Capacity is observation only; fresh authority is reacquired at
            // the loop head. No native-message or authority loan crosses await.
            tokio::select! {
                biased;
                _ = joined.clone() => return,
                _ = endpoint.wait_for_capacity() => {},
            }
        }
    }
}

impl<
    Requests,
    Inputs,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
>
    Client<
        Requests,
        Inputs,
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
    Requests::Msg: Send,
{
    // Also called by Node after an escaped/cancelled serving operation. It does
    // not repeat encoding, verification, protection, decoding or native sends.
    #[expect(
        clippy::await_holding_refcell_ref,
        reason = "after producer disposal, terminal rows and their sole native receivers retain every acquired original through actual EOF"
    )]
    async fn retire(&mut self) {
        self.source.close_admission();
        if let Some(querier) = self.querier.take() {
            self.withdrawal = Some(querier.undeclare().await);
        }
        for request in self.requests.borrow().iter() {
            if request.callback_eof.borrow().is_some() {
                continue;
            }
            let mut incoming = request.incoming.borrow_mut();
            let Some(incoming) = incoming.as_mut() else {
                continue;
            };
            while let Some(original) = incoming.recv().await {
                request.replies.borrow_mut().push(Response::new(original));
            }
            *request.callback_eof.borrow_mut() = Some(());
        }
    }
}

impl<
    Root,
    Event,
    Families,
    Target,
    Requests,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
    Encode,
    Protect,
    Verify,
    Decode,
    CurrentRequest,
    CurrentReply,
    RequestGuard,
    ReplyGuard,
> Exports<Root, Event, Families, EstablishedRecipient<Target>>
    for Client<
        Requests,
        (
            String,
            ClientLimits,
            json::Limits,
            Duration,
            Encode,
            Protect,
            Verify,
            Decode,
            CurrentRequest,
            CurrentReply,
        ),
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Root: Protocol<Addr = MailAddr>,
    Target: Protocol<Addr = MailAddr, Msg = Message>,
    Requests: Protocol<Addr = MailAddr>,
    Requests::Msg: Send,
    Encode: Fn(
        &User<MailAddr, Requests::Msg>,
        &str,
        MailAddr,
    ) -> Result<Option<Box<[u8]>>, EncodingError>,
    Protect: AsyncFn(&[u8], &User<MailAddr, Requests::Msg>) -> Result<Box<[u8]>, ProtectionError>,
    Verify: AsyncFn(&[u8], &[u8]) -> Result<Receipt, VerificationError>,
    Decode: Fn(&Receipt, &[u8], &User<MailAddr, Requests::Msg>) -> Result<Message, DecodingError>,
    CurrentRequest:
        Fn(&User<MailAddr, Requests::Msg>, &str, MailAddr) -> Result<RequestGuard, RequestError>,
    CurrentReply: Fn(
        &Receipt,
        &User<MailAddr, Requests::Msg>,
        &str,
        MailAddr,
    ) -> Result<ReplyGuard, ReplyError>,
{
    type Controls = (ActorRef<Target>, MailAddr);

    fn preflight(
        &mut self,
        application: &ApplicationHandle<Root, Event, Families>,
        target: &EstablishedRecipient<Target>,
        _addresses: &mut Vec<MailAddr>,
    ) -> Result<Self::Controls, ()> {
        // This is not an incoming declaration: it reserves no incoming actor
        // slot and may return to the same actor as a genuine incoming Export.
        match application.actor_address(target) {
            Ok(origin) => Ok((target.clone().interpret(&mut ExtractLocalEndpoint), origin)),
            Err(cause) => {
                self.startup = Some(Err(cause));
                Err(())
            }
        }
    }

    async fn declare(&mut self, session: &Session) -> Result<(), ()> {
        let ClientLimits {
            concurrent_requests: pending,
            retained_requests: retained,
            retained_responses: replies,
        } = self.inputs.1;
        if pending.get() > Semaphore::MAX_PERMITS || replies.get() > Semaphore::MAX_PERMITS {
            self.startup = Some(Err(ExportError::QueryCapacity));
            return Err(());
        }
        let reserved = self.requests.borrow_mut().try_reserve_exact(retained.get());
        if let Err(cause) = reserved {
            self.startup = Some(Err(ExportError::Storage { cause }));
            return Err(());
        }
        // The owned String yields an owned KeyExpr/Querier, not a borrow of
        // Node's Session. Native Querier owns a WeakSession internally.
        let declaration = session
            .declare_querier(self.inputs.0.clone())
            .target(QueryTarget::BestMatching)
            .consolidation(ConsolidationMode::None)
            .congestion_control(CongestionControl::Drop)
            .allowed_destination(Locality::Any)
            .timeout(self.inputs.3)
            .await;
        match declaration {
            Ok(querier) => {
                self.querier = Some(querier);
                self.startup = Some(Ok(()));
                Ok(())
            }
            Err(cause) => {
                self.startup = Some(Err(ExportError::Declaration { cause }));
                Err(())
            }
        }
    }

    async fn withdraw(&mut self) {
        self.retire().await;
    }

    async fn serve(
        &mut self,
        (endpoint, target): Self::Controls,
        runtime: String,
        mut joined: Shared<oneshot::Receiver<()>>,
    ) {
        {
            let inputs = &self.inputs;
            let querier = self
                .querier
                .as_ref()
                .expect("successful native startup owns its actual Querier");
            let source = &mut self.source;
            let source_address = source.address();
            let retained = &self.requests;
            let capacity = Arc::new(Semaphore::new(inputs.1.concurrent_requests.get()));
            let mut requests = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    actual = &mut joined => { self.joined = Some(actual); break; }
                    Some(()) = requests.next(), if !requests.is_empty() => {},
                    original = source.receive(), if retained.borrow().len() < inputs.1.retained_requests.get() => {
                        let Some(original) = original else {
                            self.source_eof = Some(());
                            // No repoll after the actual one-shot user EOF.
                            while let Some(()) = requests.next().await {}
                            self.joined = Some(joined.clone().await);
                            break;
                        };
                        let request = Rc::new(Request::new(original));
                        retained.borrow_mut().push(Rc::clone(&request));
                        if retained.borrow().len() == inputs.1.retained_requests.get() {
                            // Quota closes admission now, not after an actor's
                            // final send could block forever on this mailbox.
                            source.close_admission();
                        }
                        // Selection belongs to acquisition: joined may win
                        // before the queued operation receives its first poll.
                        Self::select(inputs, &request, &runtime, target);
                        let endpoint = &endpoint;
                        let runtime = &runtime;
                        let capacity = Arc::clone(&capacity);
                        let ending = joined.clone();
                        requests.push(async move {
                            provide(
                                || Self::request(inputs, &request, querier, endpoint,
                                    source_address, target, runtime, capacity, ending.clone()),
                                &request.completed,
                                &request.operation_factory,
                                &request.operation_poll,
                                &request.operation_disposal,
                                ending.clone(),
                            ).await;
                            // Preserve existing Node poll-unwind policy, moving
                            // the SAME payload only after actual producer Drop.
                            // Simultaneous disposal cause remains independently
                            // retained in its own outside slot.
                            let construction = request.operation_factory.borrow_mut().take();
                            if let Some(original) = construction { resume_unwind(original); }
                            let polling = request.operation_poll.borrow_mut().take();
                            if let Some(original) = polling { resume_unwind(original); }
                            // A completed FU child is disposed during FU.poll;
                            // a sole disposal panic therefore reaches Node.poll.
                            let disposal = request.operation_disposal.borrow_mut().take();
                            if let Some(original) = disposal { resume_unwind(original); }
                        });
                    }
                }
            }
            // Each actual operation is inside the existing provide owner.
            // Its disposal catches that producer's Drop before FU drops siblings.
            drop(requests);
        }
        self.source.close_admission();
        // Only unused row budget can receive accepted prefix. Exhaustion leaves
        // the closed original Source and remaining prefix in this Client.
        if self.source_eof.is_none() {
            while self.requests.borrow().len() < self.inputs.1.retained_requests.get() {
                let Some(original) = self.source.receive().await else {
                    self.source_eof = Some(());
                    break;
                };
                let request = Rc::new(Request::new(original));
                self.requests.borrow_mut().push(Rc::clone(&request));
                // This can observe the genuine terminal ACK/no-Get message;
                // it starts no provider, native query or actor delivery.
                Self::select(&self.inputs, &request, &runtime, target);
            }
        }
        self.retire().await;
    }
}

impl<
    Requests,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
>
    Request<
        Requests,
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
{
    #[expect(
        clippy::type_complexity,
        reason = "the source row transfers each independent original, typed response, native result and producer cause"
    )]
    fn into_parts(
        self,
    ) -> (
        User<MailAddr, Requests::Msg>,
        Option<ClientError>,
        Option<Result<Option<Box<[u8]>>, EncodingError>>,
        Option<Result<Box<[u8]>, ProtectionError>>,
        Option<RequestError>,
        Option<AcquireError>,
        Attempt<Result<(), Box<dyn StdError + Send + Sync>>>,
        Option<Box<dyn StdError + Send + Sync>>,
        Vec<Response<Receipt, Message, VerificationError, DecodingError, ReplyError>>,
        Option<()>,
        (
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
        ),
        (
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
            Option<Box<dyn Any + Send>>,
        ),
        Option<()>,
    ) {
        // Actual callback EOF precedes successful Node extraction. The original
        // IngressError is boxed intact, including any refused reply count charge.
        let refused: Option<Box<dyn StdError + Send + Sync>> = self
            .callback_refusal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .map(|original| Box::new(original) as Box<dyn StdError + Send + Sync>);
        (
            self.original,
            self.refusal.into_inner(),
            self.encoded.into_inner(),
            self.protected.into_inner(),
            self.permission.into_inner(),
            self.capacity.into_inner(),
            self.query.into_inner(),
            refused,
            self.replies.into_inner(),
            self.callback_eof.into_inner(),
            (
                self.protection_factory.into_inner(),
                self.protection_poll.into_inner(),
                self.protection_disposal.into_inner(),
            ),
            (
                self.operation_factory.into_inner(),
                self.operation_poll.into_inner(),
                self.operation_disposal.into_inner(),
            ),
            self.completed.into_inner(),
        )
    }
}

impl<
    Requests,
    Inputs,
    Receipt,
    Message,
    EncodingError,
    ProtectionError,
    VerificationError,
    DecodingError,
    RequestError,
    ReplyError,
>
    Client<
        Requests,
        Inputs,
        Receipt,
        Message,
        EncodingError,
        ProtectionError,
        VerificationError,
        DecodingError,
        RequestError,
        ReplyError,
    >
where
    Requests: Protocol<Addr = MailAddr>,
{
    /// Transfer acquired originals and typed results without exposing SDK owners.
    ///
    /// Cold extraction retains absent startup/withdrawal/EOF results. Successful
    /// Node extraction follows real producer disposal and native withdrawal.
    /// Any remaining native Querier is explicitly surrendered through its Drop;
    /// this does not fabricate an undeclare result or callback EOF observation.
    /// Response owners keep each acquired reply paired with its count charge.
    /// Source quota exhaustion returns the exact closed Source and unread prefix.
    ///
    /// # Panics
    /// Panics if an internal request producer still owns a row at terminal handoff.
    /// This violates the ownership invariant: cold owners have no rows, and Node
    /// extraction follows producer disposal and native withdrawal.
    #[expect(
        clippy::type_complexity,
        reason = "consuming extraction preserves every original input, request result and independent native cleanup fact"
    )]
    pub fn into_parts(
        self,
    ) -> (
        ExternalActor<Requests>,
        Inputs,
        Option<Result<(), ExportError>>,
        Option<Result<(), Box<dyn StdError + Send + Sync>>>,
        Option<Result<(), RecvError>>,
        Option<()>,
        Vec<(
            User<MailAddr, Requests::Msg>,
            Option<ClientError>,
            Option<Result<Option<Box<[u8]>>, EncodingError>>,
            Option<Result<Box<[u8]>, ProtectionError>>,
            Option<RequestError>,
            Option<AcquireError>,
            Attempt<Result<(), Box<dyn StdError + Send + Sync>>>,
            Option<Box<dyn StdError + Send + Sync>>,
            Vec<Response<Receipt, Message, VerificationError, DecodingError, ReplyError>>,
            Option<()>,
            (
                Option<Box<dyn Any + Send>>,
                Option<Box<dyn Any + Send>>,
                Option<Box<dyn Any + Send>>,
            ),
            (
                Option<Box<dyn Any + Send>>,
                Option<Box<dyn Any + Send>>,
                Option<Box<dyn Any + Send>>,
            ),
            Option<()>,
        )>,
    ) {
        // The native owner is private even on the advanced consuming path.
        drop(self.querier);
        (
            self.source,
            self.inputs,
            self.startup,
            self.withdrawal,
            self.joined,
            self.source_eof,
            self.requests
                .into_inner()
                .into_iter()
                .map(|request| {
                    Rc::try_unwrap(request)
                        .unwrap_or_else(|_| {
                            panic!("terminal projection requires disposed Node operations")
                        })
                        .into_parts()
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests;
