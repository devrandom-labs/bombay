use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{
    AdmissionFailure, EntityDefinition, EntityFamilyAt, EntityRef, NativeEntityHost, Passivation,
};
use crate::local::endpoint::{ActorRef, ExtractLocalEndpoint, SendError, request_actor_shutdown};
use crate::local::ingress::{Admission, AdmissionClosure};
use crate::observe::{Observation, Publisher, pair};
use crate::terminal::ActorRetirementReport;
use crate::termination::Termination;
use behavior::{
    AllocationRejection, EstablishedRecipient, Here, Ingress, InjectEvent, Never, Protocol, User,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested};
use communication::{Consumer, ControlSender, Received, mailbox_channel};
use core::fmt;
use core::future::Future;
use std::sync::{Arc, Weak};

impl<P, Event, Families> fmt::Debug for ApplicationHandle<P, Event, Families>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationHandle")
            .field("root", &self.root)
            .field("families_type", &core::any::type_name::<Families>())
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event, Families: Clone> Clone for ApplicationHandle<P, Event, Families> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            shutdown_control: self.shutdown_control.clone(),
            retirement: self.retirement.clone(),
            allocations: self.allocations.clone(),
            families: self.families.clone(),
        }
    }
}

impl<P: Protocol, Event, Families> ApplicationHandle<P, Event, Families> {
    pub(in crate::application) fn new(
        root: ActorRef<P>,
        shutdown_control: Weak<ControlSender<Event>>,
        retirement: Observation<ActorRetirementReport>,
        allocations: ApplicationAddresses,
        families: Families,
    ) -> Self {
        Self {
            root,
            shutdown_control,
            retirement,
            allocations,
            families,
        }
    }

    /// Borrow the exact live root's delivery reference.
    #[must_use]
    pub const fn root(&self) -> &ActorRef<P> {
        &self.root
    }

    /// Form a transport-neutral interface from an explicit receptionist product.
    pub fn interface<Api>(&self, api: Api) -> ActorInterface<Api> {
        ActorInterface::new(api, self.allocations.clone())
    }

    /// Project lifecycle authority without exposing topology ownership.
    #[must_use]
    pub fn lifecycle(&self) -> ApplicationLifecycle<P, Event> {
        ApplicationLifecycle {
            root: self.root.clone(),
            control: self.shutdown_control.clone(),
            retirement: self.retirement.clone(),
        }
    }

    /// Select one statically declared native Entity family by semantic role.
    #[must_use]
    pub fn entities<Role, Position>(
        &self,
        _: Role,
    ) -> crate::entity::Entities<<Families as EntityFamilyAt<Role, Position>>::Definition>
    where
        Families: EntityFamilyAt<Role, Position>,
    {
        self.families.select_family()
    }

    /// Begin passivation of one identity in a statically declared family.
    #[must_use]
    #[allow(
        private_bounds,
        reason = "native execution remains a sealed application composition proof"
    )]
    pub fn passivate_entity<Role, Position>(
        &self,
        role: Role,
        id: &<<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Id,
    ) -> Passivation
    where
        Families: EntityFamilyAt<Role, Position>,
        <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Hosts:
            NativeEntityHost<
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Behavior,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Terminal,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::ChildFailures,
            >,
{
        self.entities(role).passivate(id)
    }
}

impl<P, Event> fmt::Debug for ApplicationLifecycle<P, Event>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationLifecycle")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event> Clone for ApplicationLifecycle<P, Event> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            control: self.control.clone(),
            retirement: self.retirement.clone(),
        }
    }
}

impl<P: Protocol, Event> ApplicationLifecycle<P, Event> {
    /// Request shutdown through the root lifecycle lane.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection when the root is already stopping or has
    /// already stopped.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRejection>
    where
        Event: InjectEvent<ShutdownRequested, Here>,
    {
        if self.root.termination_observation().try_get().is_some() {
            return Err(ShutdownRejection::AlreadyStopped);
        }
        let Some(control) = self.control.upgrade() else {
            return Err(self.root.shutdown_rejection());
        };
        request_actor_shutdown(&self.root, &control, Ingress::new())
    }

    /// Observe the exact root's joined-retirement assessment.
    ///
    /// The report is available while application work is still running; it
    /// does not wait for final native-result handoff or Entity family shutdown.
    /// Independent observers share the fact, and cancelling a borrowed wait
    /// cannot cancel cleanup or another observer.
    pub fn retirement(&self) -> impl Future<Output = ActorRetirementReport> + use<P, Event> {
        let retirement = self.retirement.clone();
        async move { retirement.await }
    }

    /// Observe termination of the exact root incarnation.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P, Event> {
        self.root.termination()
    }
}

/// One statically typed destination accepted by [`ExternalActor::send`].
///
/// Bombay implements this sealed contract for exact actor recipients and
/// stable Entity references. Applications select a capability value; they do
/// not implement routing or inspect addresses.
pub trait ExternalTarget: target_sealed::Sealed {
    /// Message admitted by this capability.
    type Message: Send + 'static;
    /// Exact rejection returned by this capability.
    type Error;

    #[doc(hidden)]
    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Target> target_sealed::Sealed for EstablishedRecipient<Target> where
    Target: Protocol<Addr = MailAddr>
{
}

impl<Target> ExternalTarget for EstablishedRecipient<Target>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
{
    type Message = Target::Msg;
    type Error = SendError<Target::Msg>;

    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let mut interpreter = ExtractLocalEndpoint;
        let endpoint = self.clone().interpret(&mut interpreter);
        async move { endpoint.send_from(origin, message).await }
    }
}

impl<D> target_sealed::Sealed for EntityRef<D> where D: EntityDefinition {}

impl<D> ExternalTarget for EntityRef<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    type Message = behavior::BehaviorMessage<D::Behavior>;
    type Error = AdmissionFailure<Self::Message>;

    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.admit_from(origin, message)
    }
}

impl<Api> fmt::Debug for ActorInterface<Api> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorInterface")
            .field("api_type", &core::any::type_name::<Api>())
            .finish_non_exhaustive()
    }
}

impl<Api> Clone for ActorInterface<Api>
where
    Api: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api: self.api.clone(),
            allocations: self.allocations.clone(),
        }
    }
}

impl<Api> ActorInterface<Api> {
    pub(crate) const fn new(api: Api, allocations: ApplicationAddresses) -> Self {
        Self { api, allocations }
    }

    /// Borrow the statically declared receptionist product.
    #[must_use]
    pub const fn api(&self) -> &Api {
        &self.api
    }

    /// Establish one real external actor for a concrete reply protocol.
    ///
    /// The returned actor owns one fresh allocated address, one exact cloneable
    /// recipient, and the only receive authority for its mailbox.
    ///
    /// # Errors
    ///
    /// Returns the exact allocation failure. No raw address or unbound
    /// recipient is returned on failure.
    pub fn external<P>(&self) -> Result<ExternalActor<P>, ExternalActorError>
    where
        P: Protocol<Addr = MailAddr>,
        P::Msg: Send,
    {
        ExternalActor::establish(&self.allocations)
    }
}

impl<P> fmt::Debug for ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalActor")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<P> ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
    P::Msg: Send,
{
    fn establish(allocations: &ApplicationAddresses) -> Result<Self, ExternalActorError> {
        let address = allocations
            .allocate()
            .map_err(ExternalActorError::Allocation)?;
        let (control_liveness, owner, mailbox, receiver) =
            mailbox_channel::<Never, User<MailAddr, P::Msg>>(communication::Config::new(
                EXTERNAL_USER_CAPACITY,
            ));
        let admission = Arc::new(Admission::new(owner));
        let (termination, observation) = pair();
        let endpoint =
            ActorRef::external(address, mailbox, Arc::downgrade(&admission), observation);
        let recipient = endpoint.established_recipient();
        Ok(Self {
            address,
            recipient,
            admission,
            _control_liveness: control_liveness,
            receiver,
            termination: Some(termination),
        })
    }

    /// The fresh logical identity supplied as `User::from` by [`Self::send`].
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Clone the exact reply capability without duplicating receive authority.
    #[must_use]
    pub fn recipient(&self) -> EstablishedRecipient<P> {
        self.recipient.clone()
    }

    /// Send directly to one exact exported or extruded actor capability.
    ///
    /// The external actor's own allocated address is always used as the message
    /// origin. The reply destination remains an explicit field of the domain
    /// message when the protocol requires one.
    ///
    /// # Errors
    ///
    /// Returns the exact rejected message when the destination incarnation is
    /// no longer accepting user messages.
    pub fn send<'a, Target>(
        &'a self,
        target: &'a Target,
        message: Target::Message,
    ) -> impl Future<Output = Result<(), Target::Error>> + Send + use<'a, P, Target>
    where
        Target: ExternalTarget,
    {
        target.send_from(self.address, message)
    }

    /// Receive the next message and its truthful actor origin.
    ///
    /// Returns `None` after admission closes and every accepted message has
    /// been yielded.
    pub async fn receive(&mut self) -> Option<User<MailAddr, P::Msg>> {
        match self.receiver.recv().await? {
            Received::User(message) => Some(message),
            Received::UserLaneClosed => None,
            Received::Control(never) => match never {},
        }
    }

    /// Close new reply admission while retaining the already accepted prefix.
    pub fn close_admission(&self) {
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
    }
}

impl<P> Drop for ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    fn drop(&mut self) {
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        if let Some(termination) = self.termination.take() {
            termination.complete(Ok(Exit::Normal));
        }
    }
}

mod target_sealed {
    pub trait Sealed {}
}

/// A live application's typed root and capability projections.
pub struct ApplicationHandle<P: Protocol, Event, Families = ()> {
    root: ActorRef<P>,
    shutdown_control: Weak<ControlSender<Event>>,
    retirement: Observation<ActorRetirementReport>,
    allocations: ApplicationAddresses,
    families: Families,
}

/// Explicit application lifecycle authority, separate from actor messaging.
pub struct ApplicationLifecycle<P: Protocol, Event> {
    root: ActorRef<P>,
    control: Weak<ControlSender<Event>>,
    retirement: Observation<ActorRetirementReport>,
}

const EXTERNAL_USER_CAPACITY: usize = 1_024;

/// A transport-neutral product of explicitly exported actor capabilities.
///
/// `Api` is application-defined. Bombay neither infers exports from topology
/// nor grants lifecycle authority through this value.
pub struct ActorInterface<Api> {
    api: Api,
    allocations: ApplicationAddresses,
}

/// Exact failure to establish an external actor endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ExternalActorError {
    /// Bombay's one never-wrapping application address source is exhausted.
    #[error("the Bombay application has exhausted its local address space")]
    Allocation(#[source] AllocationRejection),
}

/// One allocated external actor with affine receive authority.
///
/// This value intentionally does not implement `Clone`. Cloneable producer
/// authority is available separately through [`Self::recipient`].
pub struct ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    address: MailAddr,
    recipient: EstablishedRecipient<P>,
    admission: Arc<Admission<User<MailAddr, P::Msg>>>,
    _control_liveness: ControlSender<Never>,
    receiver: Consumer<Never, User<MailAddr, P::Msg>>,
    termination: Option<Publisher<Termination<MailAddr>>>,
}

#[cfg(test)]
mod tests {
    use behavior::MessageProtocol;
    use behavior_actors::Exit;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ExternalActor;
    use crate::local::endpoint::ExtractLocalEndpoint;

    #[test]
    fn external_actor_drop_publishes_normal_termination_to_its_exact_recipient() {
        let allocations = ApplicationAddresses::new();
        let external = ExternalActor::<MessageProtocol<MailAddr, u64>>::establish(&allocations)
            .expect("the external actor is established");
        let address = external.address();
        let recipient = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let description = format!("{external:?}");
        assert!(description.contains("ExternalActor"));
        assert!(description.contains(&format!("{address:?}")));

        drop(external);
        let terminal = recipient.termination_observation().try_get();
        assert_eq!(terminal, Some(Ok(Exit::Normal)));
    }
}

#[cfg(test)]
mod installed_shutdown_contract {

    use behavior::{Become, BehaviorBase, Never, NoSends, Step, Stopped};
    use behavior_actors::{Exit, ShutdownRejection, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::Config;
    use core::future::{Future, poll_fn};
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::mem;
    use std::panic::{panic_any, resume_unwind};
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ApplicationLifecycle;
    use crate::application::{ActorInterface, App, ApplicationOutcome};
    use crate::launch::launch_inert;

    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::LocalResidual;
    use crate::observe;
    use crate::terminal::{
        ActorFailureAssessment, ActorRetirement, RetirementAssessment, RetirementNotificationError,
    };
    use crate::topology::Hosts;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};

    use crate::{LedgerProtocol, ShutdownLedger};
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both joined incarnations and stale-view observations in one trace."
    )]
    async fn old_root_lifecycle_cannot_close_actual_same_address_replacement() {
        let actors = ActorSpace::<LedgerProtocol>::new();
        let address = MailAddr(541);
        let old_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&old_actions);
        let old = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![43],
                received: Vec::new(),
            }),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("old root launch"));
        let (_old_report_publication, old_retirement) = observe::pair();
        let lifecycle = ApplicationLifecycle {
            root: old.actor.clone(),
            control: old.shutdown_control.clone(),
            retirement: old_retirement,
        };
        let repeated_lifecycle = lifecycle.clone();
        let accepted = lifecycle.request_shutdown();
        let repeated = repeated_lifecycle.request_shutdown();
        assert_eq!(accepted, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let old_termination = lifecycle.termination().await;
        assert_eq!(old_termination, Ok(Exit::Normal));
        let (old_result, termination_notification) = old.task.finish().await;
        termination_notification.expect("the actual ordinary termination notification succeeded");
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    operation_failures,
                    descendant_report,
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = old_result
        else {
            panic!("joined old root")
        };
        assert_eq!(operation_failures, ActorFailureAssessment::Incomplete);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::NotEstablished
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::Incomplete
        );
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.base().entries, [43]);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(ingress.control.is_empty() && ingress.user.is_empty());
        assert_eq!(descendants, ());
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        assert_eq!(
            *old_actions.lock().expect("old full actions"),
            [Step::Continue, Step::Stop(Stopped)]
        );
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        assert!(lifecycle.control.upgrade().is_none());
        let fresh_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&fresh_actions);
        let fresh = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![47],
                received: Vec::new(),
            }),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("fresh root claim"));
        let (_fresh_report_publication, fresh_retirement) = observe::pair();
        let fresh_lifecycle = ApplicationLifecycle {
            root: fresh.actor.clone(),
            control: fresh.shutdown_control.clone(),
            retirement: fresh_retirement,
        };
        let old_request = lifecycle.request_shutdown();
        let replay = repeated_lifecycle.request_shutdown();
        assert_eq!(old_request, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopped));
        let mut fresh_termination = pin!(fresh_lifecycle.termination());
        let mut context = Context::from_waker(Waker::noop());
        let waiting = Future::poll(fresh_termination.as_mut(), &mut context);
        assert!(matches!(waiting, Poll::Pending));
        let values = vec![53, 59];
        let allocation = values.as_ptr();
        let delivered = fresh.actor.send_from(MailAddr(61), values).await;
        assert!(delivered.is_ok());
        let new_request = fresh_lifecycle.request_shutdown();
        assert_eq!(new_request, Ok(()));
        let stopped = fresh_termination.await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let (retired, termination_notification) = fresh.task.finish().await;
        termination_notification.expect("the actual ordinary termination notification succeeded");
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    operation_failures,
                    descendant_report,
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = retired
        else {
            panic!("whole joined replacement")
        };
        assert_eq!(operation_failures, ActorFailureAssessment::Incomplete);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::NotEstablished
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::Incomplete
        );
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        assert_eq!(behavior.base().entries, [47]);
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(descendants, ());
        let mut expected_actions = vec![Step::Continue; 1 + behavior.base().received.len()];
        expected_actions.push(Step::Stop(Stopped));
        assert_eq!(
            *fresh_actions.lock().expect("fresh full actions"),
            expected_actions
        );
        let mut originals = behavior.base().received.iter().chain(ingress.user.iter());
        let original = originals.next().expect("one admitted original user input");
        assert_eq!(original.from, MailAddr(61));
        assert_eq!(original.message, [53, 59]);
        assert_eq!(original.message.as_ptr(), allocation);
        assert!(originals.next().is_none());
        assert_ne!(settlements.len(), 0);
        let final_turn = settlements.len() - 1;
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            match settlement.become_ {
                Step::Continue => assert!(turn < final_turn),
                Step::Stop(_) => assert_eq!(turn, final_turn),
            }
        }
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        let fresh_replay = fresh_lifecycle.request_shutdown();
        assert_eq!(fresh_replay, Err(ShutdownRejection::AlreadyStopped));
        let old_replay = lifecycle.request_shutdown();
        assert_eq!(old_replay, Err(ShutdownRejection::AlreadyStopped));
    }
    enum LedgerSpaceDisposal {
        Return,
        #[expect(
            clippy::redundant_allocation,
            reason = "Box is the original panic carrier; Arc separately proves cause lifetime without opening or reboxing the native payload"
        )]
        Unwind(Box<Arc<Vec<u64>>>),
    }

    struct RetiringLedgerSpace {
        actors: ActorSpace<LedgerProtocol>,
        disposal: LedgerSpaceDisposal,
        retired_spaces: Arc<AtomicUsize>,
    }

    impl Hosts<LedgerProtocol> for RetiringLedgerSpace {
        fn space(&self) -> &ActorSpace<LedgerProtocol> {
            &self.actors
        }
    }

    impl Drop for RetiringLedgerSpace {
        fn drop(&mut self) {
            self.retired_spaces.fetch_add(1, Ordering::SeqCst);
            match mem::replace(&mut self.disposal, LedgerSpaceDisposal::Return) {
                LedgerSpaceDisposal::Return => {}
                LedgerSpaceDisposal::Unwind(payload) => resume_unwind(payload),
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one public Application controller proves independent report axes, actual space disposal, work barriers and complete native custody"
    )]
    async fn root_retirement_report_is_available_before_application_work_finishes() {
        for disposal in [
            LedgerSpaceDisposal::Return,
            LedgerSpaceDisposal::Unwind(Box::new(Arc::new(vec![167, 173]))),
        ] {
            let expected_cause = match &disposal {
                LedgerSpaceDisposal::Return => None,
                LedgerSpaceDisposal::Unwind(payload) => Some((
                    Arc::downgrade(payload.as_ref()),
                    ptr::from_ref(payload.as_ref()).cast::<()>(),
                )),
            };
            let expected_failures = match &expected_cause {
                None => ActorFailureAssessment::NoFailuresFound,
                Some(_) => ActorFailureAssessment::FailuresFound,
            };
            let actors = ActorSpace::<LedgerProtocol>::new();
            let retired_spaces = Arc::new(AtomicUsize::new(0));
            let payload = vec![157, 163];
            let allocation = payload.as_ptr();
            let (report_publication, report_received) = oneshot::channel();
            let (release_work, work_released) = oneshot::channel();
            let (execution, receiving) = App::new(
                StopOnShutdown::new(ShutdownLedger {
                    entries: payload,
                    received: Vec::new(),
                }),
                RetiringLedgerSpace {
                    actors: actors.clone(),
                    disposal,
                    retired_spaces: Arc::clone(&retired_spaces),
                },
            )
            .execute_with::<Never, (), _, _>(move |application| async move {
                let lifecycle = application.lifecycle();
                {
                    let mut cancelled = pin!(lifecycle.retirement());
                    let mut context = Context::from_waker(Waker::noop());
                    let pending = Future::poll(cancelled.as_mut(), &mut context);
                    assert!(matches!(pending, Poll::Pending));
                }
                let independent = lifecycle.clone();
                let requested = lifecycle.request_shutdown();
                requested.expect("the exact root accepts its first stop request");
                let report = lifecycle.retirement().await;
                let independently_observed = independent.retirement().await;
                assert_eq!(independently_observed, report);
                let published = report_publication.send(report);
                published.expect("the independent work observer remains present");
                work_released
                    .await
                    .expect("work has its explicit outside release");
                report
            })
            .unwrap_or_else(|_| panic!("the public application uses the entered executor"));
            let mut execution = pin!(execution);
            let report = timeout(Duration::from_secs(10), async {
                tokio::select! {
                    report = report_received => report.expect("work acquired the actual joined report"),
                    () = &mut execution => panic!("work cannot finish before its outside release"),
                }
            })
            .await
            .expect("joined report must be available without the work-completion barrier");
            assert_eq!(retired_spaces.load(Ordering::SeqCst), 1);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), expected_failures);
            let missing = actors.resolve(&MailAddr::APPLICATION_ROOT);
            assert!(missing.is_none());
            let mut receiving = pin!(receiving);
            let mut context = Context::from_waker(Waker::noop());
            let pending_receiving = Future::poll(receiving.as_mut(), &mut context);
            assert!(matches!(pending_receiving, Poll::Pending));
            let released = release_work.send(());
            released.expect("the report did not finish or cancel application work");
            execution.await;
            let (work, native, notifications) = receiving.await;
            let ApplicationOutcome::Completed {
                output,
                cleanup: Ok(()),
            } = work
            else {
                panic!("the actual caller-local work and cleanup finish independently");
            };
            assert_eq!(output, report);
            let notifications =
                notifications.expect("both original notification results are retained");
            notifications
                .termination
                .expect("ordinary termination succeeded");
            notifications
                .retirement
                .expect("joined-report publication succeeded");
            let (origin, native) =
                native.expect("the native root result remains independently retained");
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            let ActorRetirement::Completed {
                behavior,
                completion: Completion::Stopped,
                interpretation: None,
                source: None,
                settlements,
                control,
                user,
                descendants,
                child_failures: (),
                capability_failures,
                additional_failures,
                received_interpretation: None,
                received_source: None,
                source_index: None,
                acquired_ingress: None,
                retirement_failures,
                terminal_report: None,
                unread_owner_cancellation: None,
            } = native
            else {
                panic!("the exact stopped native root and all of its lanes survive");
            };
            assert_eq!(behavior.base().entries, [157, 163]);
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(behavior.base().received.len(), 0);
            assert!(control.is_empty() && user.is_empty());
            assert_eq!(descendants.len(), 0);
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            match expected_cause {
                None => assert!(retirement_failures.is_empty()),
                Some((original_cause, allocation)) => {
                    assert_eq!(original_cause.strong_count(), 1);
                    assert_eq!(retirement_failures.len(), 1);
                    let payload = retirement_failures
                        .into_iter()
                        .next()
                        .expect("the exact capability disposal cause remains native");
                    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), allocation);
                    drop(payload);
                    assert_eq!(original_cause.strong_count(), 0);
                }
            }
            assert_eq!(settlements.len(), 1);
            let settlement = settlements
                .into_iter()
                .next()
                .expect("one actual stop settlement");
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
        }
    }

    struct RetirementObserver {
        failure: Mutex<Option<Arc<Vec<u64>>>>,
        application_work: Waker,
    }

    impl Wake for RetirementObserver {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // Ensure the real work can inspect the already committed fact even
            // when this independently registered observer panics during wake.
            self.application_work.wake_by_ref();
            let failure = self
                .failure
                .lock()
                .expect("the observer owns its original panic")
                .take();
            panic_any(failure.expect("the observer panics once with its original allocation"));
        }
    }

    #[tokio::test]
    async fn root_report_notification_fault_retains_native_result_and_committed_report() {
        let actors = ActorSpace::<LedgerProtocol>::new();
        let failure = Arc::new(vec![167_u64, 173]);
        let retained_failure = Arc::downgrade(&failure);
        let values = vec![179, 181];
        let allocation = values.as_ptr();
        let (execution, receiving) = App::new(
            StopOnShutdown::new(ShutdownLedger {
                entries: values,
                received: Vec::new(),
            }),
            actors.clone(),
        )
        .execute_with::<Never, (), _, _>(move |application| async move {
            let lifecycle = application.lifecycle();
            let mut observer = pin!(lifecycle.retirement());
            poll_fn(|context| {
                let observer_waker = Waker::from(Arc::new(RetirementObserver {
                    failure: Mutex::new(Some(Arc::clone(&failure))),
                    application_work: context.waker().clone(),
                }));
                let mut observer_context = Context::from_waker(&observer_waker);
                let pending = Future::poll(observer.as_mut(), &mut observer_context);
                assert!(matches!(pending, Poll::Pending));
                Poll::Ready(())
            })
            .await;
            drop(failure);
            let stopped = lifecycle.request_shutdown();
            stopped.expect("the actual root accepts shutdown");
            let report = lifecycle.retirement().await;
            let independently_observed = observer.await;
            assert_eq!(report, independently_observed);
            report
        })
        .unwrap_or_else(|_| panic!("the public application uses the entered executor"));
        let ((), (work, native, notifications)) = timeout(Duration::from_secs(10), async {
            tokio::join!(execution, receiving)
        })
        .await
        .expect("a report observer panic cannot lose the actor result or stall cleanup");
        let ApplicationOutcome::Completed {
            output: report,
            cleanup: Ok(()),
        } = work
        else {
            panic!("work observes the committed report and actual cleanup finishes");
        };
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        // The notification fault occurs after this immutable actor snapshot.
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let notifications = notifications.expect("the complete notification product is retained");
        notifications
            .termination
            .expect("ordinary termination succeeded independently");
        let Err(RetirementNotificationError::Panicked { payload }) = notifications.retirement
        else {
            panic!("the original report observer panic has its own receipt");
        };
        assert_eq!(retained_failure.strong_count(), 1);
        let (_, native) = native.expect("the complete native root survives report notification");
        let ActorRetirement::Completed {
            behavior,
            completion: Completion::Stopped,
            ..
        } = native
        else {
            panic!("the native stopped actor retains its original state");
        };
        assert_eq!(behavior.base().entries, [179, 181]);
        assert_eq!(behavior.base().entries.as_ptr(), allocation);
        let missing = actors.resolve(&MailAddr::APPLICATION_ROOT);
        assert!(missing.is_none());
        drop(payload);
        assert_eq!(retained_failure.strong_count(), 0);
    }

    #[tokio::test]
    async fn external_customer_owns_admission_without_behavior_shutdown() {
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let mut external = interface
            .external::<LedgerProtocol>()
            .expect("real external endpoint");
        let endpoint = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let values = vec![127, 131];
        let allocation = values.as_ptr();
        let accepted = endpoint.send_from(MailAddr(137), values).await;
        assert!(accepted.is_ok());
        external.close_admission();
        let values = vec![139, 149];
        let allocation_rejected = values.as_ptr();
        let rejected = endpoint.send_from(MailAddr(151), values).await;
        let original = rejected
            .expect_err("external owner closed admission")
            .into_message();
        assert_eq!(original, [139, 149]);
        assert_eq!(original.as_ptr(), allocation_rejected);
        let admitted = external
            .receive()
            .await
            .expect("exact admitted external message");
        assert_eq!(admitted.from, MailAddr(137));
        assert_eq!(admitted.message, [127, 131]);
        assert_eq!(admitted.message.as_ptr(), allocation);
        let exhausted = external.receive().await;
        assert!(exhausted.is_none());
        drop(external);
        let terminal = endpoint.termination().await;
        assert_eq!(terminal, Ok(Exit::Normal));
    }
}
