//! Actor-local service construction beside the standard native capabilities.
use super::{ApplicationCapabilities, ApplicationCapabilityInputs, NoParent};
use crate::address::MailAddr;
use crate::launch::ActorSpace;
use crate::topology::{ApplicationTopology, Hosts, RootRegistration};
use behavior::{ActionItem, Behavior, BirthMode, InterpretItem, Never, Protocol};
use core::future::Future;
use core::hash::Hash;
use std::sync::Arc;

/// A request's concrete application-supplied interpreter owner.
/// Standard native requests retain their existing interpreter implementations.
pub trait ExternalRequest: ActionItem {
    type Interpreter: Send;
}

/// Construct a separately owned interpreter for each actor.
/// Construction runs after pure initialization, before commitment or effects.
pub trait InterpreterFactory: Send + Sync {
    type Interpreter: Send;
    fn create(&self) -> Self::Interpreter;
}

impl<F, S> InterpreterFactory for F
where
    F: Fn() -> S + Send + Sync,
    S: Send,
{
    type Interpreter = S;
    fn create(&self) -> S {
        self()
    }
}

/// The constructor for standard native actor capabilities and optional services.
/// This value selects construction; it does not own an executor or start actors.
pub struct Runtime<Factory = ()> {
    factory: Factory,
}

impl Runtime {
    /// Select standard Address, Communication, Observe and Timer capabilities.
    #[must_use]
    pub const fn new() -> Self {
        Self { factory: () }
    }

    /// Select standard capabilities plus separately constructed actor services.
    #[must_use]
    pub const fn with_interpreter<F: InterpreterFactory>(factory: F) -> Runtime<F> {
        Runtime { factory }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

/// Select the actual constructor used when a namespace hosts native execution.
/// Pure namespace and topology metadata do not require this capability.
pub trait NativeHost {
    type Runtime;
    fn runtime(&self) -> &Self::Runtime;
}

// No shared namespace still permits exact-only child registration and execution.
// This selects native construction without granting logical hosting capabilities.
impl NativeHost for () {
    type Runtime = Runtime;
    fn runtime(&self) -> &Runtime {
        const RUNTIME: Runtime = Runtime::new();
        &RUNTIME
    }
}

impl<P> NativeHost for ActorSpace<P>
where
    P: Protocol,
    P::Addr: Hash,
{
    type Runtime = Runtime;
    fn runtime(&self) -> &Runtime {
        const RUNTIME: Runtime = Runtime::new();
        &RUNTIME
    }
}

impl<N: NativeHost> NativeHost for Arc<N> {
    type Runtime = N::Runtime;
    fn runtime(&self) -> &Self::Runtime {
        self.as_ref().runtime()
    }
}

impl<N, F> NativeHost for (N, Runtime<F>) {
    type Runtime = Runtime<F>;
    fn runtime(&self) -> &Self::Runtime {
        &self.1
    }
}

impl<P, N, F> RootRegistration<P> for (N, Runtime<F>)
where
    P: Protocol<Addr = MailAddr>,
    N: RootRegistration<P>,
{
    fn root_space(&self) -> &ActorSpace<P> {
        self.0.root_space()
    }
}

impl<P, N, F> Hosts<P> for (N, Runtime<F>)
where
    P: Protocol,
    P::Addr: Hash,
    N: Hosts<P>,
{
    fn space(&self) -> &ActorSpace<P> {
        self.0.space()
    }
}

impl<Root, P, F> ApplicationTopology<Root, ()> for (ActorSpace<P>, Runtime<F>)
where
    Root: Behavior<Protocol = P, Birth: BirthMode<Child = Never>>,
    P: Protocol<Addr = MailAddr>,
{
    type Descendants = Never;
}

/// Complete construction is available only through the owning native runtime.
pub(crate) trait NativeConstruction: Sized {
    type Services: Send;
    fn services() -> Self::Services;

    fn capabilities<C, N, Bindings, Origins>(
        inputs: ApplicationCapabilityInputs<C, N>,
        bindings: Bindings,
    ) -> ApplicationCapabilities<C, N, NoParent, Bindings, Origins, Self::Services>
    where
        C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
        N: NativeHost<Runtime = Self>,
    {
        let services = Self::services();
        ApplicationCapabilities::new_with_services(inputs, bindings, services)
    }
}

impl NativeConstruction for Runtime {
    type Services = ();
    fn services() {}
}

impl<F> NativeConstruction for Runtime<F>
where
    F: InterpreterFactory,
{
    type Services = Option<F::Interpreter>;
    fn services() -> Self::Services {
        None
    }
}

pub(crate) type NativeServices<N> = <<N as NativeHost>::Runtime as NativeConstruction>::Services;

pub(crate) type NativeCapabilities<C, N, P, Bindings, Origins> =
    ApplicationCapabilities<C, N, P, Bindings, Origins, NativeServices<N>>;

/// The actual service is absent until guarded startup constructs it once.
pub(crate) trait InterpreterStartup<N> {
    fn initialize(&mut self, host: &N);
}

impl<N> InterpreterStartup<N> for () {
    fn initialize(&mut self, _: &N) {}
}

impl<N, F, S> InterpreterStartup<N> for Option<S>
where
    N: NativeHost<Runtime = Runtime<F>>,
    F: InterpreterFactory<Interpreter = S>,
{
    fn initialize(&mut self, host: &N) {
        self.get_or_insert_with(|| host.runtime().factory.create());
    }
}

impl<C, N, P, Bindings, Origins, F, S, Request, Event, Path> InterpretItem<Request, Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins, Option<S>>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    N: NativeHost<Runtime = Runtime<F>>,
    F: InterpreterFactory<Interpreter = S>,
    Request: ExternalRequest<Interpreter = S>,
    S: InterpretItem<Request, Event, Path>,
    Self: Send,
{
    fn interpret_item<'a>(
        &'a mut self,
        input: Request::Input<'a>,
        received: &'a mut Option<Request::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        Request: 'a,
    {
        let service = self
            .services
            .as_mut()
            .expect("guarded native startup constructs supplied services before effects");
        service.interpret_item(input, received)
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeConstruction, Runtime};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::MailboxCapacity;
    use crate::launch::ActorSpace;
    use crate::local::children::{NoChildBindings, StructuralOrigins};
    use crate::local::effects::{
        ActionInterpreter, ApplicationCapabilityInputs, LocalTerminalReports,
    };
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::{LocalRetirementRequest, OwnerCancellation};
    use crate::local::ingress::StandardIngress;
    use crate::observe;
    use crate::{ActorFailureAssessment, ApplicationScope, RetirementAssessment};
    use behavior::{
        Actions, Behavior, BehaviorActed, EstablishedDelivery, EstablishedRecipient,
        Interpretation, ItemSettlement, MessageProtocol, Never, SettledItem, Step,
    };
    use bombay_engine::{ActiveEnvironment, Environment};
    use communication::Config;
    use core::future::Future;
    use core::ops::ControlFlow;
    use core::pin::pin;
    use core::task::{Context, Waker};
    use std::fs::File;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;
    use tokio::sync::oneshot;

    type DocumentNotice = MessageProtocol<MailAddr, Box<[u8]>>;

    struct Document {
        notice: Option<Box<[u8]>>,
        recipient: EstablishedRecipient<DocumentNotice>,
    }
    #[crate::actor(message = Never, sends = { notices: Vec<EstablishedDelivery<DocumentNotice>> })]
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the pure actor initialization returns typed Actions"
    )]
    impl Document {
        fn init(&mut self) -> BehaviorActed<Self> {
            let notice = self
                .notice
                .take()
                .expect("the original affine initialization message");
            Ok(Actions::cont()
                .send_notices(EstablishedDelivery::new(self.recipient.clone(), notice)))
        }
    }

    struct DocumentReader {
        file: File,
        retirements: Arc<Mutex<Vec<io::Result<u64>>>>,
    }

    impl Drop for DocumentReader {
        fn drop(&mut self) {
            let metadata = self.file.metadata().map(|metadata| metadata.len());
            self.retirements
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(metadata);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "one native borrowing-activation retry preserves pending ACK, original initialization, resource lifetime and complete retirement"
    )]
    async fn recreated_activation_waiters_keep_the_original_interpreter_until_retirement() {
        let address = MailAddr(227);
        let scope = ApplicationScope::new()
            .client_capacity(MailboxCapacity::new(2).expect("one affine notice fits"));
        let mut replies = scope
            .external::<DocumentNotice>()
            .expect("the exact known reply recipient");
        assert_ne!(replies.address(), address);
        let original = Box::<[u8]>::from([233, 239]);
        let original_pointer = original.as_ptr();
        let mut document = Document {
            notice: Some(original),
            recipient: replies.recipient(),
        };
        let initialization = behavior::initialize(&mut document)
            .expect("pure initialization emits the original owned message");
        let space = ActorSpace::<<Document as Behavior>::Protocol>::new();
        let addresses = space.clone();
        let constructions = Arc::new(AtomicUsize::new(0));
        let observed_constructions = Arc::clone(&constructions);
        let retirements = Arc::new(Mutex::new(Vec::new()));
        let observed_retirements = Arc::clone(&retirements);
        let hosts = Arc::new((
            space.clone(),
            Runtime::with_interpreter(move || {
                constructions.fetch_add(1, Ordering::SeqCst);
                DocumentReader {
                    file: File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
                        .expect("the constructor opens one real actor-local File"),
                    retirements: Arc::clone(&retirements),
                }
            }),
        ));
        let (_publication, termination) = observe::pair();
        let (owner, cancellation) = oneshot::channel();
        let (terminal, _terminal_reports) = oneshot::channel();
        let (commitment, mut committed) = oneshot::channel();
        let mailbox_capacity =
            MailboxCapacity::new(2).expect("one concrete native mailbox capacity");
        let environment = LocalEnvironment::<Document, _, StandardIngress>::prepare(
            address,
            space,
            Config::new(mailbox_capacity.get()),
            termination,
            cancellation,
            move |control, timers, observations| {
                let inputs = ApplicationCapabilityInputs {
                    actor_spaces: hosts,
                    address,
                    mailbox_capacity,
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers,
                    observations,
                    terminal_reports: LocalTerminalReports::new(terminal),
                };
                ActionInterpreter::new(Runtime::<_>::capabilities::<
                    Document,
                    _,
                    _,
                    StructuralOrigins<Document>,
                >(
                    inputs, NoChildBindings::<Never>::default()
                ))
            },
        )
        .with_private_commitment(commitment);
        assert_eq!(observed_constructions.load(Ordering::SeqCst), 0);
        let mut environment = Some(environment);
        let mut actions = Some(initialization);
        let mut received = None;
        let first = {
            let activating = <_ as Environment<Document>>::activate(
                &mut environment,
                &mut actions,
                &mut received,
            );
            let mut activating = pin!(activating);
            activating
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(first.is_pending());
        assert!(environment.is_some());
        assert!(actions.is_none());
        assert!(received.is_none());
        assert_eq!(observed_constructions.load(Ordering::SeqCst), 1);
        assert!(
            observed_retirements
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert!(addresses.resolve(&address).is_none());
        let received_notice = {
            let receiving = replies.receive();
            let mut receiving = pin!(receiving);
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(
            received_notice.is_pending(),
            "initialization messages cannot run before the original binding ACK"
        );
        let (endpoint, acknowledge) = committed
            .try_recv()
            .expect("the actual private binding arrives before public visibility");
        assert_eq!(endpoint.address(), address);
        let second = {
            let activating = <_ as Environment<Document>>::activate(
                &mut environment,
                &mut actions,
                &mut received,
            );
            let mut activating = pin!(activating);
            activating
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(second.is_pending());
        assert!(environment.is_some());
        assert!(actions.is_none());
        assert!(received.is_none());
        assert_eq!(observed_constructions.load(Ordering::SeqCst), 1);
        assert!(
            observed_retirements
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert!(addresses.resolve(&address).is_none());
        let received_notice = {
            let receiving = replies.receive();
            let mut receiving = pin!(receiving);
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        assert!(
            received_notice.is_pending(),
            "initialization messages cannot run before the original binding ACK"
        );
        let acknowledged = acknowledge.send(());
        acknowledged.expect(
            "the same original pending ACK receiver survives both dropped operation futures",
        );
        <_ as Environment<Document>>::activate(&mut environment, &mut actions, &mut received).await;
        assert!(environment.is_none());
        assert!(actions.is_none());
        let Some(Ok((mut active, Interpretation::Complete(initialization)))) = received else {
            panic!("the recreated operation completes the same accepted initialization after ACK");
        };
        assert!(matches!(
            initialization.sends.notices.as_slice(),
            [SettledItem::Attempted(ItemSettlement::Accepted(()))]
        ));
        assert!(initialization.creations.is_empty());
        assert!(matches!(initialization.become_, Step::Continue));
        assert_eq!(observed_constructions.load(Ordering::SeqCst), 1);
        assert!(
            observed_retirements
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
        assert!(addresses.resolve(&address).is_none());
        let notice = tokio::time::timeout(Duration::from_secs(5), replies.receive())
            .await
            .expect("the original affine initialization must finish after ACK")
            .expect("the original initialization message is delivered after ACK");
        assert_eq!(notice.from, address);
        assert_eq!(notice.message.as_ptr(), original_pointer);
        assert_eq!(&*notice.message, &[233, 239]);
        replies.close_admission();
        let remainder = replies.receive().await;
        assert!(
            remainder.is_none(),
            "the complete accepted prefix contains exactly one original initialization message"
        );
        assert!(document.notice.is_none());
        assert_eq!(document.recipient, replies.recipient());
        let published = active.publish();
        assert!(matches!(published, ControlFlow::Continue(())));
        let resolved = addresses
            .resolve(&address)
            .expect("actual publication commits the original address claim");
        assert_eq!(resolved, endpoint);
        let cancellation = owner.send(OwnerCancellation);
        assert!(
            matches!(cancellation, Ok(())),
            "the original cancellation authority remains available"
        );
        let request = active.next().await;
        assert!(matches!(
            request,
            ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(OwnerCancellation))
        ));
        let mut active = Some(active);
        let mut actions = None;
        let mut interpretation = None;
        let mut source = None;
        let mut source_index = None;
        let mut ingress = None;
        let mut settlements = Some(vec![initialization]);
        let mut retired = None;
        <_ as ActiveEnvironment<Document>>::retire(
            &mut active,
            &mut actions,
            &mut interpretation,
            &mut source,
            &mut source_index,
            &mut ingress,
            &mut settlements,
            &mut retired,
        )
        .await;
        assert!(active.is_none());
        assert!(actions.is_none());
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(source_index.is_none());
        assert!(ingress.is_none());
        assert!(settlements.is_none());
        let residual = retired
            .expect("native retirement acquires its original complete residual")
            .settle_activation_tasks()
            .await;
        let LocalResidual::Retired {
            interpretation,
            source,
            settlements,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            ingress,
            activation_tasks,
            descendants,
            operation_failures,
            descendant_report,
            capability_failures,
            terminal_report,
            retirement_failures,
            unread_owner_cancellation,
        } = residual
        else {
            panic!("the published original environment completes ordinary native retirement");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants.0.len(), 0);
        assert_eq!(descendants.1.len(), 0);
        assert_eq!(operation_failures, ActorFailureAssessment::NoFailuresFound);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        assert!(capability_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        let [initialization] = settlements.as_slice() else {
            panic!("the exact original initialization settlement remains whole");
        };
        assert!(matches!(
            initialization.sends.notices.as_slice(),
            [SettledItem::Attempted(ItemSettlement::Accepted(()))]
        ));
        assert!(initialization.creations.is_empty());
        assert!(matches!(initialization.become_, Step::Continue));
        assert!(addresses.resolve(&address).is_none());
        assert_eq!(observed_constructions.load(Ordering::SeqCst), 1);
        let mut retirements = observed_retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(retirements.len(), 1);
        let length = retirements
            .pop()
            .expect("the original service is disposed exactly once")
            .expect("its original File remains live until disposal");
        assert_eq!(
            length,
            u64::try_from(include_bytes!("../../../Cargo.toml").len())
                .expect("the manifest length fits")
        );
    }
}
