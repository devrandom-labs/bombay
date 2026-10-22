use super::{ExportError, RequestKey};
use crate::address::MailAddr;
use behavior::Protocol;
use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::OwnedSemaphorePermit;
use zenoh::query::Query;

type Panic = Box<dyn Any + Send>;
pub(super) enum Identity<Target: Protocol<Addr = MailAddr>> {
    Unassigned,
    Canonical(RequestKey<Target>),
    Duplicate(RequestKey<Target>),
}

/// Affine ownership of a request's original inputs and independently acquired facts.
/// Private producers may borrow these facts. Consuming extraction refuses while
/// any producer lease survives, returning the same request without reconstruction.
pub struct Request<
    Target: Protocol<Addr = MailAddr>,
    Receipt,
    Input,
    Native,
    VerificationError,
    DecodingError,
    EncodingError,
    ProtectionError,
    RequestError,
    ReplyError,
> {
    #[expect(
        clippy::type_complexity,
        reason = "the private producer lease preserves each concrete request, native result and error type"
    )]
    pub(super) custody: Rc<
        Custody<
            Target,
            Receipt,
            Input,
            Native,
            VerificationError,
            DecodingError,
            EncodingError,
            ProtectionError,
            RequestError,
            ReplyError,
        >,
    >,
}
pub(super) struct Custody<
    Target: Protocol<Addr = MailAddr>,
    Receipt,
    Input,
    Native,
    VerificationError,
    DecodingError,
    EncodingError,
    ProtectionError,
    RequestError,
    ReplyError,
> {
    pub identity: RefCell<Identity<Target>>,
    pub original: (Box<[u8]>, Box<[u8]>),
    pub query: RefCell<Option<(Query, OwnedSemaphorePermit)>>,
    pub verified: RefCell<Option<Result<Receipt, VerificationError>>>,
    pub decoded: RefCell<Option<Result<Input, DecodingError>>>,
    pub native: RefCell<Option<Native>>,
    pub encoding: RefCell<Option<Result<Box<[u8]>, EncodingError>>>,
    pub protection: RefCell<Option<Result<Box<[u8]>, ProtectionError>>>,
    pub publications: RefCell<Vec<Result<(), zenoh::Error>>>,
    pub request_refusal: RefCell<Option<RequestError>>,
    pub reply_refusal: RefCell<Option<ReplyError>>,
    pub refusal: RefCell<Option<ExportError>>,
    pub verification_factory: RefCell<Option<Panic>>,
    pub verification_poll: RefCell<Option<Panic>>,
    pub verification_disposal: RefCell<Option<Panic>>,
    pub protection_factory: RefCell<Option<Panic>>,
    pub protection_poll: RefCell<Option<Panic>>,
    pub protection_disposal: RefCell<Option<Panic>>,
    // The actual unit return acquired outside the request operation. It says
    // nothing about authentication, admission, publication or native EOF.
    pub completed: RefCell<Option<()>>,
}
impl<
    Target: Protocol<Addr = MailAddr>,
    Receipt,
    Input,
    Native,
    VerificationError,
    DecodingError,
    EncodingError,
    ProtectionError,
    RequestError,
    ReplyError,
>
    Request<
        Target,
        Receipt,
        Input,
        Native,
        VerificationError,
        DecodingError,
        EncodingError,
        ProtectionError,
        RequestError,
        ReplyError,
    >
{
    pub(super) fn new(
        original: (Box<[u8]>, Box<[u8]>),
        query: (Query, OwnedSemaphorePermit),
    ) -> Self {
        Self {
            custody: Rc::new(Custody {
                identity: RefCell::new(Identity::Unassigned),
                original,
                query: RefCell::new(Some(query)),
                verified: RefCell::new(None),
                decoded: RefCell::new(None),
                native: RefCell::new(None),
                encoding: RefCell::new(None),
                protection: RefCell::new(None),
                publications: RefCell::new(Vec::new()),
                request_refusal: RefCell::new(None),
                reply_refusal: RefCell::new(None),
                refusal: RefCell::new(None),
                verification_factory: RefCell::new(None),
                verification_poll: RefCell::new(None),
                verification_disposal: RefCell::new(None),
                protection_factory: RefCell::new(None),
                protection_poll: RefCell::new(None),
                protection_disposal: RefCell::new(None),
                completed: RefCell::new(None),
            }),
        }
    }

    /// Transfer complete original request and native operation values unchanged.
    /// An unfinished query is deliberately surrendered, disposing it before its
    /// count permit. Successful extraction itself is no cleanup certificate.
    ///
    /// # Errors
    /// Returns the same request while a private producer still owns a custody lease.
    #[expect(
        clippy::type_complexity,
        reason = "consuming extraction retains every original typed value and cause without an aggregate wrapper"
    )]
    pub fn try_into_parts(
        self,
    ) -> Result<
        (
            Option<RequestKey<Target>>,
            (Box<[u8]>, Box<[u8]>),
            Option<Result<Receipt, VerificationError>>,
            Option<Result<Input, DecodingError>>,
            Option<Native>,
            Option<Result<Box<[u8]>, EncodingError>>,
            Option<Result<Box<[u8]>, ProtectionError>>,
            Vec<Result<(), zenoh::Error>>,
            (
                Option<RequestError>,
                Option<ReplyError>,
                Option<ExportError>,
            ),
            (
                Option<Panic>,
                Option<Panic>,
                Option<Panic>,
                Option<Panic>,
                Option<Panic>,
                Option<Panic>,
            ),
        ),
        Self,
    > {
        let custody = match Rc::try_unwrap(self.custody) {
            Ok(custody) => custody,
            Err(custody) => return Err(Self { custody }),
        };
        discard_query(custody.query.into_inner());
        let key = match custody.identity.into_inner() {
            Identity::Unassigned => None,
            Identity::Canonical(key) | Identity::Duplicate(key) => Some(key),
        };
        Ok((
            key,
            custody.original,
            custody.verified.into_inner(),
            custody.decoded.into_inner(),
            custody.native.into_inner(),
            custody.encoding.into_inner(),
            custody.protection.into_inner(),
            custody.publications.into_inner(),
            (
                custody.request_refusal.into_inner(),
                custody.reply_refusal.into_inner(),
                custody.refusal.into_inner(),
            ),
            (
                custody.verification_factory.into_inner(),
                custody.verification_poll.into_inner(),
                custody.verification_disposal.into_inner(),
                custody.protection_factory.into_inner(),
                custody.protection_poll.into_inner(),
                custody.protection_disposal.into_inner(),
            ),
        ))
    }
}

pub(super) fn discard_query(original: Option<(Query, OwnedSemaphorePermit)>) {
    if let Some((query, permit)) = original {
        drop(query);
        drop(permit);
    }
}
