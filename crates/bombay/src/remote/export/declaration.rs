use super::query::Retirement;
use super::{Attempt, ExportError};
use crate::TrySendError;
use crate::address::MailAddr;
use crate::application::{
    ActorInterface, ApplicationClient, ApplicationHandle, ApplicationLifecycle, ApplicationSender,
    ExternalActorError,
};
use crate::local::endpoint::{ExtractLocalEndpoint, InstalledActor};
use crate::terminal::ActorRetirementReport;
use behavior::{
    Behavior, EstablishedActor, EstablishedRecipient, Ingress, InjectEvent, Protocol, User,
};
use behavior_actors::{ShutdownRejection, ShutdownRequested};
use core::future::{Future, Ready, poll_fn};
use core::marker::PhantomData;
use core::mem;
use core::pin::pin;
use core::task::Poll;
use futures_util::future::{Either, OptionFuture};

/// Pure operation selection and reply encoding for one reply role.
pub struct Reply<Input, Target, Replies, Projection, Encode> {
    projection: Projection,
    encode: Encode,
    protocol: PhantomData<fn(Input) -> (Target, Replies)>,
}
/// Pure operation selection and acknowledgement encoding without a reply endpoint.
pub struct Submission<Input, Target, Projection, Encode> {
    projection: Projection,
    encode: Encode,
    protocol: PhantomData<fn(Input) -> Target>,
}
/// Pure operation selection and encoding using the exact target's shutdown capability.
pub struct Stop<Input, Path, Projection, Encode> {
    projection: Projection,
    encode: Encode,
    protocol: PhantomData<fn(Input, Path)>,
}

pub fn reply<Input, Target, Replies, Projection, Encode>(
    projection: Projection,
    encode: Encode,
) -> Reply<Input, Target, Replies, Projection, Encode> {
    Reply {
        projection,
        encode,
        protocol: PhantomData,
    }
}
pub fn submission<Input, Target, Projection, Encode>(
    projection: Projection,
    encode: Encode,
) -> Submission<Input, Target, Projection, Encode> {
    Submission {
        projection,
        encode,
        protocol: PhantomData,
    }
}
pub fn stop<Input, Path, Projection, Encode>(
    projection: Projection,
    encode: Encode,
) -> Stop<Input, Path, Projection, Encode> {
    Stop {
        projection,
        encode,
        protocol: PhantomData,
    }
}

type ReplyOriginal<Target, Replies, Constructor> = (
    Option<Constructor>,
    Attempt<
        Result<
            ApplicationClient<EstablishedRecipient<Target>, Replies>,
            (EstablishedRecipient<Target>, ExternalActorError),
        >,
    >,
    Attempt<Result<(), TrySendError<<Target as Protocol>::Msg>>>,
    Option<Option<User<MailAddr, <Replies as Protocol>::Msg>>>,
    Vec<Option<User<MailAddr, <Replies as Protocol>::Msg>>>,
);
type SubmissionOriginal<Target, Constructor> = (
    Option<Constructor>,
    Attempt<
        Result<
            ApplicationSender<EstablishedRecipient<Target>>,
            (EstablishedRecipient<Target>, ExternalActorError),
        >,
    >,
    Attempt<Result<(), TrySendError<<Target as Protocol>::Msg>>>,
);
type StopOriginal = (
    Attempt<Result<(), ShutdownRejection>>,
    Option<ActorRetirementReport>,
);

pub(super) trait Commands<Input, Receipt, Target: Protocol<Addr = MailAddr>> {
    type Native;
    type EncodingError;
    fn select(&self, input: Input) -> Result<Self::Native, Input>;
    fn is_full(&self, native: &Self::Native) -> bool;
    fn prepare<'native>(
        &self,
        native: &'native mut Self::Native,
        interface: &ActorInterface<EstablishedRecipient<Target>>,
    ) -> Option<Result<(), &'native ExternalActorError>>;
    fn encode(
        &self,
        receipt: &Receipt,
        native: &Self::Native,
    ) -> Option<Result<Box<[u8]>, Self::EncodingError>>;
}
pub(super) trait Delivery<Native, Control> {
    fn attempt(
        &self,
        native: &mut Native,
        control: &Control,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError>;
    fn receive<'native>(
        &'native self,
        native: &'native mut Native,
        control: &'native Control,
        ending: impl Future<Output = ActorRetirementReport> + 'native,
    ) -> Option<impl Future<Output = ()> + 'native>;
}
pub(super) trait Reception<Native> {
    fn drain<'native>(
        &'native self,
        native: &'native mut Native,
    ) -> Option<impl Future<Output = ()> + 'native>;
}

impl<Input, Receipt, Target, Replies, Projection, Encode, Constructor, EncodingError>
    Commands<Input, Receipt, Target> for Reply<Input, Target, Replies, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
    Projection: Fn(Input) -> Result<Constructor, Input>,
    Constructor: FnOnce(EstablishedRecipient<Replies>) -> Target::Msg,
    Encode: Fn(&Receipt, &User<MailAddr, Replies::Msg>) -> Result<Box<[u8]>, EncodingError>,
{
    type Native = ReplyOriginal<Target, Replies, Constructor>;
    type EncodingError = EncodingError;
    fn is_full(&self, native: &Self::Native) -> bool {
        matches!(native.2, Attempt::Returned(Err(TrySendError::Full(_))))
    }
    fn select(&self, input: Input) -> Result<Self::Native, Input> {
        (self.projection)(input).map(|constructor| {
            (
                Some(constructor),
                Attempt::Unattempted,
                Attempt::Unattempted,
                None,
                Vec::new(),
            )
        })
    }
    fn prepare<'native>(
        &self,
        native: &'native mut Self::Native,
        interface: &ActorInterface<EstablishedRecipient<Target>>,
    ) -> Option<Result<(), &'native ExternalActorError>> {
        native.1 = Attempt::Uncertain;
        native.1 = Attempt::Returned(interface.client_to(interface.api().clone()));
        match &native.1 {
            Attempt::Returned(Ok(_)) => Some(Ok(())),
            Attempt::Returned(Err((_, cause))) => Some(Err(cause)),
            Attempt::Unattempted | Attempt::Uncertain => None,
        }
    }
    fn encode(
        &self,
        receipt: &Receipt,
        native: &Self::Native,
    ) -> Option<Result<Box<[u8]>, EncodingError>> {
        native
            .3
            .as_ref()?
            .as_ref()
            .map(|reply| (self.encode)(receipt, reply))
    }
}
impl<Input, Target, Replies, Projection, Encode, Constructor, Control>
    Delivery<ReplyOriginal<Target, Replies, Constructor>, Control>
    for Reply<Input, Target, Replies, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
    Constructor: FnOnce(EstablishedRecipient<Replies>) -> Target::Msg,
    Control: Retirement,
{
    fn attempt(
        &self,
        native: &mut ReplyOriginal<Target, Replies, Constructor>,
        _: &Control,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        if let Attempt::Returned(Ok(client)) = &native.1 {
            let command = if let Some(constructor) = native.0.take() {
                canonical();
                constructor(client.recipient())
            } else if matches!(native.2, Attempt::Returned(Err(TrySendError::Full(_)))) {
                let Attempt::Returned(Err(TrySendError::Full(command))) =
                    mem::replace(&mut native.2, Attempt::Uncertain)
                else {
                    unreachable!("only the retained native Full enters retry")
                };
                command
            } else {
                return Ok(());
            };
            native.2 = Attempt::Uncertain;
            native.2 = Attempt::Returned(client.try_send(command));
        }
        Ok(())
    }
    fn receive<'native>(
        &'native self,
        native: &'native mut ReplyOriginal<Target, Replies, Constructor>,
        control: &'native Control,
        ending: impl Future<Output = ActorRetirementReport> + 'native,
    ) -> Option<impl Future<Output = ()> + 'native> {
        if !matches!(native.2, Attempt::Uncertain | Attempt::Returned(Ok(()))) {
            return None;
        }
        let (_, client, _, received, _) = native;
        let Attempt::Returned(Ok(client)) = client else {
            return None;
        };
        Some(async move {
            let original = {
                let mut receiving = pin!(client.receive());
                let mut retired = pin!(OptionFuture::from(control.retirement()));
                tokio::select! {
                    biased;
                    original = &mut receiving => Some(original),
                    Some(_) = &mut retired => None,
                    _ = ending => None,
                }
            };
            *received = Some(if let Some(original) = original {
                original
            } else {
                client.close_admission();
                client.receive().await
            });
        })
    }
}
impl<Input, Target, Replies, Projection, Encode, Constructor>
    Reception<ReplyOriginal<Target, Replies, Constructor>>
    for Reply<Input, Target, Replies, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    fn drain<'native>(
        &'native self,
        native: &'native mut ReplyOriginal<Target, Replies, Constructor>,
    ) -> Option<impl Future<Output = ()> + 'native> {
        let (_, client, _, received, tail) = native;
        if matches!(received, Some(None)) {
            return None;
        }
        let Attempt::Returned(Ok(client)) = client else {
            return None;
        };
        Some(async move {
            client.close_admission();
            while !matches!(tail.last(), Some(None)) {
                let mut receiving = pin!(client.receive());
                poll_fn(|cx| match receiving.as_mut().poll(cx) {
                    Poll::Ready(original) => {
                        tail.push(original);
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                })
                .await;
            }
        })
    }
}

impl<Input, Receipt, Target, Projection, Encode, Constructor, EncodingError>
    Commands<Input, Receipt, Target> for Submission<Input, Target, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Projection: Fn(Input) -> Result<Constructor, Input>,
    Constructor: FnOnce() -> Target::Msg,
    Encode: Fn(
        &Receipt,
        &Attempt<Result<(), TrySendError<Target::Msg>>>,
    ) -> Result<Box<[u8]>, EncodingError>,
{
    type Native = SubmissionOriginal<Target, Constructor>;
    type EncodingError = EncodingError;
    fn is_full(&self, native: &Self::Native) -> bool {
        matches!(native.2, Attempt::Returned(Err(TrySendError::Full(_))))
    }
    fn select(&self, input: Input) -> Result<Self::Native, Input> {
        (self.projection)(input).map(|constructor| {
            (
                Some(constructor),
                Attempt::Unattempted,
                Attempt::Unattempted,
            )
        })
    }
    fn prepare<'native>(
        &self,
        native: &'native mut Self::Native,
        interface: &ActorInterface<EstablishedRecipient<Target>>,
    ) -> Option<Result<(), &'native ExternalActorError>> {
        native.1 = Attempt::Uncertain;
        native.1 = Attempt::Returned(interface.sender_to(interface.api().clone()));
        match &native.1 {
            Attempt::Returned(Ok(_)) => Some(Ok(())),
            Attempt::Returned(Err((_, cause))) => Some(Err(cause)),
            Attempt::Unattempted | Attempt::Uncertain => None,
        }
    }
    fn encode(
        &self,
        receipt: &Receipt,
        native: &Self::Native,
    ) -> Option<Result<Box<[u8]>, EncodingError>> {
        Some((self.encode)(receipt, &native.2))
    }
}
impl<Input, Target, Projection, Encode, Constructor, Control>
    Delivery<SubmissionOriginal<Target, Constructor>, Control>
    for Submission<Input, Target, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Constructor: FnOnce() -> Target::Msg,
{
    fn attempt(
        &self,
        native: &mut SubmissionOriginal<Target, Constructor>,
        _: &Control,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        if let Attempt::Returned(Ok(sender)) = &native.1 {
            let command = if let Some(constructor) = native.0.take() {
                canonical();
                constructor()
            } else if matches!(native.2, Attempt::Returned(Err(TrySendError::Full(_)))) {
                let Attempt::Returned(Err(TrySendError::Full(command))) =
                    mem::replace(&mut native.2, Attempt::Uncertain)
                else {
                    unreachable!("only the retained native Full enters retry")
                };
                command
            } else {
                return Ok(());
            };
            native.2 = Attempt::Uncertain;
            native.2 = Attempt::Returned(sender.try_send(command));
        }
        Ok(())
    }
    fn receive<'native>(
        &'native self,
        _: &'native mut SubmissionOriginal<Target, Constructor>,
        _: &'native Control,
        _: impl Future<Output = ActorRetirementReport> + 'native,
    ) -> Option<impl Future<Output = ()> + 'native> {
        None::<Ready<()>>
    }
}
impl<Input, Target, Projection, Encode, Constructor>
    Reception<SubmissionOriginal<Target, Constructor>>
    for Submission<Input, Target, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
{
    fn drain<'native>(
        &'native self,
        _: &'native mut SubmissionOriginal<Target, Constructor>,
    ) -> Option<impl Future<Output = ()> + 'native> {
        None::<Ready<()>>
    }
}

pub(super) trait Shutdown<Path> {
    fn request(
        &self,
        admission: &mut Attempt<Result<(), ShutdownRejection>>,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError>;
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>>;
}
impl<P, Event, Path> Shutdown<Path> for Option<ApplicationLifecycle<P, Event>>
where
    P: Protocol<Addr = MailAddr>,
    Event: InjectEvent<ShutdownRequested, Path>,
{
    fn request(
        &self,
        admission: &mut Attempt<Result<(), ShutdownRejection>>,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        let Some(control) = self else {
            return Err(ExportError::StopAuthority);
        };
        canonical();
        *admission = Attempt::Uncertain;
        *admission = Attempt::Returned(control.shutdown::<Path>());
        Ok(())
    }
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>> {
        self.as_ref().map(ApplicationLifecycle::retirement)
    }
}
impl<B, Path> Shutdown<Path> for InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    B::Event: InjectEvent<ShutdownRequested, Path>,
{
    fn request(
        &self,
        admission: &mut Attempt<Result<(), ShutdownRejection>>,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        canonical();
        *admission = Attempt::Uncertain;
        *admission =
            Attempt::Returned(self.request_shutdown(Ingress::<ShutdownRequested, Path>::new()));
        Ok(())
    }
    fn retirement(&self) -> Option<impl Future<Output = ActorRetirementReport>> {
        Some(self.retirement())
    }
}
impl<Input, Receipt, Target, Path, Projection, Encode, EncodingError>
    Commands<Input, Receipt, Target> for Stop<Input, Path, Projection, Encode>
where
    Target: Protocol<Addr = MailAddr>,
    Projection: Fn(Input) -> Result<(), Input>,
    Encode: Fn(
        &Receipt,
        &Attempt<Result<(), ShutdownRejection>>,
        &Option<ActorRetirementReport>,
    ) -> Result<Box<[u8]>, EncodingError>,
{
    type Native = StopOriginal;
    type EncodingError = EncodingError;
    fn is_full(&self, _: &StopOriginal) -> bool {
        false
    }
    fn select(&self, input: Input) -> Result<Self::Native, Input> {
        (self.projection)(input).map(|()| (Attempt::Unattempted, None))
    }
    fn prepare<'native>(
        &self,
        _: &'native mut StopOriginal,
        _: &ActorInterface<EstablishedRecipient<Target>>,
    ) -> Option<Result<(), &'native ExternalActorError>> {
        None
    }
    fn encode(
        &self,
        receipt: &Receipt,
        native: &StopOriginal,
    ) -> Option<Result<Box<[u8]>, EncodingError>> {
        Some((self.encode)(receipt, &native.0, &native.1))
    }
}
impl<Input, Path, Projection, Encode, Control: Shutdown<Path>> Delivery<StopOriginal, Control>
    for Stop<Input, Path, Projection, Encode>
{
    fn attempt(
        &self,
        native: &mut StopOriginal,
        control: &Control,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        control.request(&mut native.0, canonical)
    }
    fn receive<'native>(
        &'native self,
        native: &'native mut StopOriginal,
        control: &'native Control,
        _: impl Future<Output = ActorRetirementReport> + 'native,
    ) -> Option<impl Future<Output = ()> + 'native> {
        control.retirement().map(|retirement| async move {
            let mut retirement = pin!(retirement);
            poll_fn(|cx| match retirement.as_mut().poll(cx) {
                Poll::Ready(original) => {
                    native.1 = Some(original);
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            })
            .await;
        })
    }
}
impl<Input, Path, Projection, Encode> Reception<StopOriginal>
    for Stop<Input, Path, Projection, Encode>
{
    fn drain<'native>(
        &'native self,
        _: &'native mut StopOriginal,
    ) -> Option<impl Future<Output = ()> + 'native> {
        None::<Ready<()>>
    }
}

impl<Input, Receipt, Target, Left, Right> Commands<Input, Receipt, Target> for (Left, Right)
where
    Target: Protocol<Addr = MailAddr>,
    Left: Commands<Input, Receipt, Target>,
    Right: Commands<Input, Receipt, Target, EncodingError = Left::EncodingError>,
{
    type Native = Either<Left::Native, Right::Native>;
    type EncodingError = Left::EncodingError;
    fn is_full(&self, native: &Self::Native) -> bool {
        match native {
            Either::Left(native) => self.0.is_full(native),
            Either::Right(native) => self.1.is_full(native),
        }
    }
    fn select(&self, input: Input) -> Result<Self::Native, Input> {
        match self.0.select(input) {
            Ok(native) => Ok(Either::Left(native)),
            Err(input) => self.1.select(input).map(Either::Right),
        }
    }
    fn prepare<'native>(
        &self,
        native: &'native mut Self::Native,
        interface: &ActorInterface<EstablishedRecipient<Target>>,
    ) -> Option<Result<(), &'native ExternalActorError>> {
        match native {
            Either::Left(native) => self.0.prepare(native, interface),
            Either::Right(native) => self.1.prepare(native, interface),
        }
    }
    fn encode(
        &self,
        receipt: &Receipt,
        native: &Self::Native,
    ) -> Option<Result<Box<[u8]>, Self::EncodingError>> {
        match native {
            Either::Left(native) => self.0.encode(receipt, native),
            Either::Right(native) => self.1.encode(receipt, native),
        }
    }
}
impl<Left, Right, LeftNative, RightNative, Control>
    Delivery<Either<LeftNative, RightNative>, Control> for (Left, Right)
where
    Left: Delivery<LeftNative, Control>,
    Right: Delivery<RightNative, Control>,
{
    fn attempt(
        &self,
        native: &mut Either<LeftNative, RightNative>,
        control: &Control,
        canonical: impl FnOnce(),
    ) -> Result<(), ExportError> {
        match native {
            Either::Left(native) => self.0.attempt(native, control, canonical),
            Either::Right(native) => self.1.attempt(native, control, canonical),
        }
    }
    fn receive<'native>(
        &'native self,
        native: &'native mut Either<LeftNative, RightNative>,
        control: &'native Control,
        ending: impl Future<Output = ActorRetirementReport> + 'native,
    ) -> Option<impl Future<Output = ()> + 'native> {
        match native {
            Either::Left(native) => self.0.receive(native, control, ending).map(Either::Left),
            Either::Right(native) => self.1.receive(native, control, ending).map(Either::Right),
        }
    }
}
impl<Left, Right, LeftNative, RightNative> Reception<Either<LeftNative, RightNative>>
    for (Left, Right)
where
    Left: Reception<LeftNative>,
    Right: Reception<RightNative>,
{
    fn drain<'native>(
        &'native self,
        native: &'native mut Either<LeftNative, RightNative>,
    ) -> Option<impl Future<Output = ()> + 'native> {
        match native {
            Either::Left(native) => self.0.drain(native).map(Either::Left),
            Either::Right(native) => self.1.drain(native).map(Either::Right),
        }
    }
}

pub(crate) trait Target<Root: Protocol<Addr = MailAddr>, Event, Families> {
    type Protocol: Protocol<Addr = MailAddr>;
    type Control;
    #[expect(
        clippy::type_complexity,
        reason = "binding returns the existing exact actor interface and its independent lifecycle authority"
    )]
    fn bind(
        &self,
        application: &ApplicationHandle<Root, Event, Families>,
    ) -> Result<
        (
            ActorInterface<EstablishedRecipient<Self::Protocol>>,
            Self::Control,
        ),
        ExportError,
    >;
}
impl<Root: Protocol<Addr = MailAddr>, Event, Families> Target<Root, Event, Families>
    for EstablishedRecipient<Root>
{
    type Protocol = Root;
    type Control = Option<ApplicationLifecycle<Root, Event>>;
    fn bind(
        &self,
        application: &ApplicationHandle<Root, Event, Families>,
    ) -> Result<(ActorInterface<Self>, Self::Control), ExportError> {
        application.actor_address(self)?;
        let control = if *self == application.root().established_recipient() {
            Some(application.lifecycle())
        } else {
            None
        };
        Ok((application.interface(self.clone()), control))
    }
}
impl<
    Root: Protocol<Addr = MailAddr>,
    Event,
    Families,
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
> Target<Root, Event, Families> for EstablishedActor<B>
{
    type Protocol = B::Protocol;
    type Control = InstalledActor<B>;
    fn bind(
        &self,
        application: &ApplicationHandle<Root, Event, Families>,
    ) -> Result<
        (
            ActorInterface<EstablishedRecipient<B::Protocol>>,
            Self::Control,
        ),
        ExportError,
    > {
        let recipient = self.recipient();
        application.actor_address(&recipient)?;
        let control = self.clone().interpret_actor(&mut ExtractLocalEndpoint);
        Ok((application.interface(recipient), control))
    }
}
