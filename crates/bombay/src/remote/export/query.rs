use super::declaration::{Commands, Delivery, Reception, Target as ActorTarget};
use super::request::{Identity, Request, discard_query, provide};
use super::{Export, ExportError, ExportLimits, RequestKey};
use crate::address::MailAddr;
use crate::application::{ActorInterface, ApplicationHandle, ApplicationLifecycle};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::verification::poll_verification;
use crate::remote::{IngressError, json, try_retain};
use crate::{ActorRetirementReport, InstalledActor};
use behavior::{EstablishedRecipient, Protocol};
use communication::Capacity;
use core::future::{Future, poll_fn};
use core::marker::PhantomData;
use core::pin::pin;
use futures_util::{
    StreamExt,
    future::{OptionFuture, Shared},
    stream::FuturesUnordered,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use zenoh::{Session, Wait};

pub(crate) trait Exports<Root: Protocol<Addr = MailAddr>, Event, Families, Targets> {
    type Controls;
    fn preflight(
        &mut self,
        application: &ApplicationHandle<Root, Event, Families>,
        targets: &Targets,
        addresses: &mut Vec<MailAddr>,
    ) -> Result<Self::Controls, ()>;
    async fn declare(&mut self, session: &Session) -> Result<(), ()>;
    async fn withdraw(&mut self);
    fn serve(
        &mut self,
        controls: Self::Controls,
        runtime: String,
        joined: Shared<oneshot::Receiver<()>>,
    ) -> impl Future<Output = ()>;
}

#[allow(
    private_bounds,
    reason = "pure inferred declarations keep their interpreter contracts private"
)]
impl<
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
    Decode,
    Declarations,
    Verify,
    Protect,
    CurrentRequest,
    CurrentReply,
    RequestGuard,
    ReplyGuard,
>
    Export<
        (
            String,
            ExportLimits,
            json::Limits,
            Decode,
            Verify,
            Protect,
            fn(&Receipt, &Receipt) -> bool,
            CurrentRequest,
            CurrentReply,
        ),
        Declarations,
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
        >,
    >
where
    Target: Protocol<Addr = MailAddr>,
    Decode: Fn(&[u8]) -> Result<Input, DecodingError>,
    Declarations: Commands<Input, Receipt, Target, Native = Native, EncodingError = EncodingError>
        + Reception<Native>,
    Verify: AsyncFn(&[u8], &[u8]) -> Result<Receipt, VerificationError>,
    Protect: AsyncFn(&[u8], &Receipt) -> Result<Box<[u8]>, ProtectionError>,
    CurrentRequest: Fn(&Receipt, &str, MailAddr) -> Result<RequestGuard, RequestError>,
    CurrentReply: Fn(&Receipt, &str, MailAddr) -> Result<ReplyGuard, ReplyError>,
{
    /// Construct one cold actor export with explicit limits and typed policies.
    /// Node certifies targets and opens declarations before Work. Provider facts
    /// are retained independently; these callbacks run outside Behavior folds.
    pub fn new(
        key: impl Into<String>,
        limits: ExportLimits,
        json: json::Limits,
        decode: Decode,
        declarations: Declarations,
        verify: Verify,
        protect: Protect,
        same_request: fn(&Receipt, &Receipt) -> bool,
        current_request: CurrentRequest,
        current_reply: CurrentReply,
    ) -> Self {
        Self {
            inputs: (
                key.into(),
                limits,
                json,
                decode,
                verify,
                protect,
                same_request,
                current_request,
                current_reply,
            ),
            declarations,
            startup: None,
            withdrawal: None,
            ingress: Arc::new(Mutex::new(Vec::new())),
            entries: RefCell::new(Vec::new()),
            active: None,
            declaration: RefCell::new(None),
        }
    }
    async fn request<Control>(
        &self,
        row: usize,
        issuer: Weak<Semaphore>,
        interface: &ActorInterface<EstablishedRecipient<Target>>,
        control: &Control,
        runtime: &str,
        ending: impl Future<Output = ActorRetirementReport>,
        joined: Shared<oneshot::Receiver<()>>,
    ) where
        Declarations: Delivery<Native, Control>,
        Control: Retirement,
    {
        let custody = Rc::clone(&self.entries.borrow()[row].custody);
        let (key, _, limits, decode, verify, protect, same_request, current_request, current_reply) =
            &self.inputs;
        if let Err(cause) = json::check(&custody.original.0, &custody.original.1, limits) {
            *custody.refusal.borrow_mut() = Some(ExportError::Json { cause });
            return;
        }
        provide(
            || verify(&custody.original.0, &custody.original.1),
            &custody.verified,
            &custody.verification_factory,
            &custody.verification_poll,
            &custody.verification_disposal,
            joined.clone(),
        )
        .await;
        if custody.verification_factory.borrow().is_some()
            || custody.verification_poll.borrow().is_some()
            || custody.verification_disposal.borrow().is_some()
        {
            return;
        }
        let verified = custody.verified.borrow();
        let Some(Ok(receipt)) = verified.as_ref() else {
            return;
        };
        let target = interface.api().clone().interpret(&mut ExtractLocalEndpoint);
        let address = target.address();
        let permission = match current_request(receipt, runtime, address) {
            Ok(permission) => permission,
            Err(cause) => {
                *custody.request_refusal.borrow_mut() = Some(cause);
                return;
            }
        };
        let canonical = self.entries.borrow().iter().find_map(|request| {
            let identity = request.custody.identity.borrow();
            let Identity::Canonical(key) = &*identity else {
                return None;
            };
            let verified = request.custody.verified.borrow();
            let Some(Ok(original)) = verified.as_ref() else {
                return None;
            };
            same_request(original, receipt).then(|| RequestKey {
                issuer: key.issuer.clone(),
                row: key.row,
                protocol: PhantomData,
            })
        });
        if let Some(canonical) = canonical {
            let original = Rc::clone(&self.entries.borrow()[canonical.row].custody);
            if original.original.0 != custody.original.0 {
                *custody.refusal.borrow_mut() = Some(ExportError::Conflict);
                return;
            }
            let verified = original.verified.borrow();
            if let Some(Ok(receipt)) = verified.as_ref()
                && let Err(cause) = current_reply(receipt, runtime, address)
            {
                *custody.reply_refusal.borrow_mut() = Some(cause);
                return;
            }
            *custody.identity.borrow_mut() = Identity::Duplicate(canonical);
            return;
        }
        *custody.decoded.borrow_mut() = Some(decode(&custody.original.0));
        let decoded = custody.decoded.borrow_mut().take();
        let input = match decoded {
            Some(Ok(input)) => input,
            original => {
                *custody.decoded.borrow_mut() = original;
                return;
            }
        };
        let native = match self.declarations.select(input) {
            Ok(native) => native,
            Err(input) => {
                *custody.decoded.borrow_mut() = Some(Ok(input));
                return;
            }
        };
        *custody.native.borrow_mut() = Some(native);
        {
            let mut native = custody.native.borrow_mut();
            let Some(native) = native.as_mut() else {
                return;
            };
            if matches!(self.declarations.prepare(native, interface), Some(Err(_))) {
                return;
            }
            if let Err(cause) = self.declarations.attempt(native, control, || {
                *custody.identity.borrow_mut() = Identity::Canonical(RequestKey {
                    issuer,
                    row,
                    protocol: PhantomData,
                });
            }) {
                *custody.refusal.borrow_mut() = Some(cause);
                return;
            }
        }
        drop(permission);
        let mut ending = pin!(ending);
        let mut retired = pin!(OptionFuture::from(control.retirement()));
        while {
            let native = custody.native.borrow();
            let Some(native) = native.as_ref() else {
                return;
            };
            self.declarations.is_full(native)
        } {
            tokio::select! {
                biased;
                Some(_) = &mut retired => return,
                _ = &mut ending => return,
                capacity = target.wait_for_capacity() => match capacity {
                    // Both observations require fresh authority and an actual
                    // nonwaiting call; neither establishes its native result.
                    Capacity::Available | Capacity::Closed => {}
                },
            }
            let permission = match current_request(receipt, runtime, address) {
                Ok(permission) => permission,
                Err(cause) => {
                    *custody.request_refusal.borrow_mut() = Some(cause);
                    return;
                }
            };
            {
                let mut native = custody.native.borrow_mut();
                let Some(native) = native.as_mut() else {
                    return;
                };
                if let Err(cause) = self.declarations.attempt(native, control, || {}) {
                    *custody.refusal.borrow_mut() = Some(cause);
                    return;
                }
            }
            drop(permission);
        }
        {
            let mut native = custody.native.borrow_mut();
            if let Some(native) = native.as_mut()
                && let Some(receiving) = self.declarations.receive(native, control, ending)
            {
                receiving.await;
            }
        }
        let permission = match current_reply(receipt, runtime, address) {
            Ok(permission) => permission,
            Err(cause) => {
                *custody.reply_refusal.borrow_mut() = Some(cause);
                return;
            }
        };
        if let Some(native) = custody.native.borrow().as_ref() {
            *custody.encoding.borrow_mut() = self.declarations.encode(receipt, native);
        }
        drop(permission);
        let encoding = custody.encoding.borrow();
        let Some(Ok(encoded)) = encoding.as_ref() else {
            return;
        };
        provide(
            || protect(encoded, receipt),
            &custody.protection,
            &custody.protection_factory,
            &custody.protection_poll,
            &custody.protection_disposal,
            joined,
        )
        .await;
        let _ = key;
    }
    fn publish(&self, runtime: &str, address: MailAddr) {
        for request in self.entries.borrow().iter() {
            let custody = &request.custody;
            if custody.completed.borrow().is_none() || custody.query.borrow().is_none() {
                continue;
            }
            let identity = custody.identity.borrow();
            let original = match &*identity {
                Identity::Unassigned => {
                    discard_query(custody.query.borrow_mut().take());
                    continue;
                }
                Identity::Canonical(_) => Rc::clone(custody),
                Identity::Duplicate(key) => Rc::clone(&self.entries.borrow()[key.row].custody),
            };
            if original.completed.borrow().is_none() {
                continue;
            }
            let verified = original.verified.borrow();
            let encoded = original.encoding.borrow();
            let protected = original.protection.borrow();
            if original.protection_factory.borrow().is_none()
                && original.protection_poll.borrow().is_none()
                && original.protection_disposal.borrow().is_none()
                && let (Some(Ok(receipt)), Some(Ok(encoded)), Some(Ok(proof))) =
                    (verified.as_ref(), encoded.as_ref(), protected.as_ref())
            {
                match (self.inputs.8)(receipt, runtime, address) {
                    Ok(permission) => {
                        if let Some((query, _permit)) = custody.query.borrow().as_ref() {
                            custody.publications.borrow_mut().push(
                                query
                                    .reply(&self.inputs.0, encoded.to_vec())
                                    .attachment(proof.to_vec())
                                    .wait(),
                            );
                        }
                        drop(permission);
                    }
                    Err(cause) => *custody.reply_refusal.borrow_mut() = Some(cause),
                }
            }
            discard_query(custody.query.borrow_mut().take());
        }
    }
    async fn serving<Control, Root: Protocol<Addr = MailAddr>, Event>(
        &mut self,
        interface: ActorInterface<EstablishedRecipient<Target>>,
        control: Control,
        runtime: String,
        application: ApplicationLifecycle<Root, Event>,
        mut joined: Shared<oneshot::Receiver<()>>,
    ) where
        Declarations: Delivery<Native, Control>,
        Control: Retirement,
    {
        let (mut incoming, issuer) = self
            .active
            .take()
            .expect("successful declaration retains its native owner");
        let address = interface
            .api()
            .clone()
            .interpret(&mut ExtractLocalEndpoint)
            .address();
        let mut requests = FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                // Work may still request a cached result after its target retires.
                _ = &mut joined => break,
                Some(()) = requests.next(), if !requests.is_empty() => { self.publish(&runtime, address); }
                original = incoming.recv() => {
                    let Some((query, permit)) = original else { break; };
                    if self.entries.borrow().len() == self.inputs.1.retained_requests.get() {
                        discard_query(Some((query, permit)));
                        retain_diagnostic(&self.ingress, self.inputs.1.diagnostics.get(), ExportError::RetentionCapacity);
                        continue;
                    }
                    let raw = query.payload()
                        .map(|bytes| bytes.to_bytes().into_owned().into_boxed_slice())
                        .unwrap_or_default();
                    let proof = query.attachment()
                        .map(|bytes| bytes.to_bytes().into_owned().into_boxed_slice())
                        .unwrap_or_default();
                    let row = self.entries.borrow().len();
                    self.entries.borrow_mut().push(Request::new((raw, proof), (query, permit)));
                    let custody = Rc::clone(&self.entries.borrow()[row].custody);
                    let issuer = issuer.clone();
                    let interface = &interface;
                    let control = &control;
                    let runtime = &runtime;
                    let export = &*self;
                    let ending = application.retirement();
                    let joined = joined.clone();
                    requests.push(async move {
                        let mut operation = pin!(export.request(row, issuer, interface, control, runtime, ending, joined));
                        poll_fn(|cx| poll_verification(operation.as_mut(), &mut custody.completed.borrow_mut(), cx)).await;
                    });
                }
            }
        }
        let withdrawal = match self.declaration.borrow_mut().take() {
            Some(queryable) => Some(queryable.undeclare().await),
            None => None,
        };
        incoming.close();
        while let Ok(original) = incoming.try_recv() {
            discard_query(Some(original));
        }
        while let Some(()) = requests.next().await {
            self.publish(&runtime, address);
        }
        drop(requests);
        self.withdrawal = withdrawal;
        self.drain().await;
    }
    async fn drain(&self) {
        for request in self.entries.borrow().iter() {
            let custody = Rc::clone(&request.custody);
            discard_query(custody.query.borrow_mut().take());
            if let Some(native) = custody.native.borrow_mut().as_mut()
                && let Some(draining) = self.declarations.drain(native)
            {
                draining.await;
            }
        }
    }
}

fn retain_diagnostic(ingress: &Mutex<Vec<ExportError>>, maximum: usize, cause: ExportError) {
    let mut ingress = ingress
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if ingress.len() < maximum {
        ingress.push(cause);
    }
}

pub(super) trait Retirement {
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>>;
}
impl<B: behavior::Behavior<Protocol: Protocol<Addr = MailAddr>>> Retirement for InstalledActor<B> {
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>> {
        Some(self.retirement())
    }
}
impl<P: Protocol<Addr = MailAddr>, Event> Retirement for Option<ApplicationLifecycle<P, Event>> {
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>> {
        self.as_ref().map(ApplicationLifecycle::retirement)
    }
}

impl<
    Root,
    Event,
    Families,
    Endpoint,
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
    Decode,
    Declarations,
    Verify,
    Protect,
    CurrentRequest,
    CurrentReply,
    RequestGuard,
    ReplyGuard,
> Exports<Root, Event, Families, Endpoint>
    for Export<
        (
            String,
            ExportLimits,
            json::Limits,
            Decode,
            Verify,
            Protect,
            fn(&Receipt, &Receipt) -> bool,
            CurrentRequest,
            CurrentReply,
        ),
        Declarations,
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
        >,
    >
where
    Root: Protocol<Addr = MailAddr>,
    Target: Protocol<Addr = MailAddr>,
    Endpoint: ActorTarget<Root, Event, Families, Protocol = Target>,
    Endpoint::Control: Retirement,
    Decode: Fn(&[u8]) -> Result<Input, DecodingError>,
    Declarations: Commands<Input, Receipt, Target, Native = Native, EncodingError = EncodingError>
        + Reception<Native>
        + Delivery<Native, Endpoint::Control>,
    Verify: AsyncFn(&[u8], &[u8]) -> Result<Receipt, VerificationError>,
    Protect: AsyncFn(&[u8], &Receipt) -> Result<Box<[u8]>, ProtectionError>,
    CurrentRequest: Fn(&Receipt, &str, MailAddr) -> Result<RequestGuard, RequestError>,
    CurrentReply: Fn(&Receipt, &str, MailAddr) -> Result<ReplyGuard, ReplyError>,
{
    type Controls = (
        ActorInterface<EstablishedRecipient<Target>>,
        Endpoint::Control,
        ApplicationLifecycle<Root, Event>,
    );
    fn preflight(
        &mut self,
        application: &ApplicationHandle<Root, Event, Families>,
        target: &Endpoint,
        addresses: &mut Vec<MailAddr>,
    ) -> Result<Self::Controls, ()> {
        let result = target.bind(application).and_then(|controls| {
            let address = application.actor_address(controls.0.api())?;
            if addresses.contains(&address) {
                Err(ExportError::DuplicateTarget)
            } else {
                addresses.push(address);
                Ok(controls)
            }
        });
        result
            .map(|(interface, control)| (interface, control, application.lifecycle()))
            .map_err(|cause| {
                self.startup = Some(Err(cause));
            })
    }
    async fn declare(&mut self, session: &Session) -> Result<(), ()> {
        let (_, limits, _, ..) = &self.inputs;
        let preparation = if limits.queued_queries.get() > Semaphore::MAX_PERMITS
            || limits.retained_queries.get() > Semaphore::MAX_PERMITS
        {
            Err(ExportError::QueryCapacity)
        } else {
            self.entries
                .borrow_mut()
                .try_reserve_exact(limits.retained_requests.get())
                .map_err(|cause| ExportError::Storage { cause })
        };
        if let Err(cause) = preparation {
            self.startup = Some(Err(cause));
            return Err(());
        }
        let (publication, received) = mpsc::channel(limits.queued_queries.get());
        let capacity = Arc::new(Semaphore::new(limits.retained_queries.get()));
        let issuer = Arc::downgrade(&capacity);
        let ingress = Arc::clone(&self.ingress);
        let maximum = limits.diagnostics.get();
        let declaration = session
            .declare_queryable(&self.inputs.0)
            .callback(move |query| {
                if let Err(original) = try_retain(&publication, &capacity, query) {
                    let cause = match original {
                        IngressError::Capacity { original, cause } => {
                            drop(original);
                            ExportError::IngressCapacity { cause }
                        }
                        IngressError::Queue { cause } => {
                            let (cause, original) = match cause {
                                TrySendError::Full(original) => {
                                    (ExportError::IngressFull, original)
                                }
                                TrySendError::Closed(original) => {
                                    (ExportError::IngressClosed, original)
                                }
                            };
                            discard_query(Some(original));
                            cause
                        }
                    };
                    retain_diagnostic(&ingress, maximum, cause);
                }
            })
            .await;
        match declaration {
            Ok(queryable) => {
                *self.declaration.borrow_mut() = Some(queryable);
                self.active = Some((received, issuer));
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
        if let Some(queryable) = self.declaration.borrow_mut().take() {
            self.withdrawal = Some(queryable.undeclare().await);
        }
        if let Some((mut incoming, _)) = self.active.take() {
            incoming.close();
            while let Ok(original) = incoming.try_recv() {
                discard_query(Some(original));
            }
        }
        self.drain().await;
    }
    fn serve(
        &mut self,
        (interface, control, application): Self::Controls,
        runtime: String,
        joined: Shared<oneshot::Receiver<()>>,
    ) -> impl Future<Output = ()> {
        self.serving(interface, control, runtime, application, joined)
    }
}
impl<Root: Protocol<Addr = MailAddr>, Event, Families, LeftTargets, RightTargets, Left, Right>
    Exports<Root, Event, Families, (LeftTargets, RightTargets)> for (Left, Right)
where
    Left: Exports<Root, Event, Families, LeftTargets>,
    Right: Exports<Root, Event, Families, RightTargets>,
{
    type Controls = (Left::Controls, Right::Controls);
    fn preflight(
        &mut self,
        application: &ApplicationHandle<Root, Event, Families>,
        targets: &(LeftTargets, RightTargets),
        addresses: &mut Vec<MailAddr>,
    ) -> Result<Self::Controls, ()> {
        Ok((
            self.0.preflight(application, &targets.0, addresses)?,
            self.1.preflight(application, &targets.1, addresses)?,
        ))
    }
    async fn declare(&mut self, session: &Session) -> Result<(), ()> {
        self.0.declare(session).await?;
        if self.1.declare(session).await.is_err() {
            self.0.withdraw().await;
            return Err(());
        }
        Ok(())
    }
    async fn withdraw(&mut self) {
        tokio::join!(self.0.withdraw(), self.1.withdraw());
    }
    async fn serve(
        &mut self,
        (left, right): Self::Controls,
        runtime: String,
        joined: Shared<oneshot::Receiver<()>>,
    ) {
        tokio::join!(
            self.0.serve(left, runtime.clone(), joined.clone()),
            self.1.serve(right, runtime, joined)
        );
    }
}
