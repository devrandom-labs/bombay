//! One finalized local Application associated with one freshly opened native runtime.

use super::composition::ComposeApplication;
use super::execution::{ApplicationWorkPresence, RootInterpreter};
use super::work::invoke;
use super::{Application, ApplicationHandle, ApplicationOutcome, ApplicationScope, WorkOutcome};
use crate::NativeHost;
use crate::address::MailAddr;
use crate::entity::InstallEntityFamilies;
use crate::local::children::{
    ChildBindings, ChildBindingsIn, RetireChildTasks, RuntimeChildBindings,
};
use crate::local::effects::CommitActions;
use crate::local::effects::services::NativeConstruction;
use crate::remote::{Exports as ActorExports, Network, NodeStartupRefusal};
use crate::terminal::{ActorRetirement, RetirementSchema, RootOrigin};
use crate::topology::{ApplicationTopology, RootRegistration};
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Never, Protocol,
};
use core::fmt;
use core::future::{Future, Ready, poll_fn};
use core::task::Poll;
use std::any::Any;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use tokio::runtime::{Handle, RuntimeFlavor, TryCurrentError};
use zenoh::Session;

type Panic = Box<dyn Any + Send>;

enum Binding<Call, Receipt> {
    Uninvoked(Call),
    Invoked(Option<Receipt>),
}

/// One finalized ordinary Application and its fresh owned native Session.
///
/// Construct topology, children and Entity families on Application before opening. Node issues
/// that declaration once; it adds no actor lifecycle or replacement policy.
/// Borrowed execution waits leave this owner and its Session outside the wait.
/// Dropping an entire operation is surrender, not a joined retirement fact.
pub struct Node<
    Root,
    Spaces,
    Members = (),
    Families = (),
    Exports = (),
    BindingFn = (),
    BindResult = Never,
> {
    application: Option<Application<Root, Spaces, Members, Families>>,
    session: Session,
    exports: Exports,
    binding: Option<Binding<BindingFn, BindResult>>,
    closed: Option<Result<(), zenoh::Error>>,
    poll_panic: Option<Panic>,
    disposal_panic: Option<Panic>,
}

impl<Root, Spaces, Members, Families, Exports, BindingFn, BindResult> fmt::Debug
    for Node<Root, Spaces, Members, Families, Exports, BindingFn, BindResult>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let application = match &self.application {
            Some(_) => "declared",
            None => "declaration transferred",
        };
        formatter
            .debug_struct("Node")
            .field("application", &application)
            .finish_non_exhaustive()
    }
}

/// Execution was not issued; the original callable remains available.
pub enum NodeExecutionRefusal<Work> {
    /// This Node already transferred its one Application declaration.
    ExecutionIssued { work: Work },
    /// The native Session is closed; no Application input transferred.
    SessionClosed { work: Work },
    /// The host is absent; the original Application was restored to this Node.
    Host { work: Work, cause: TryCurrentError },
}

impl<Work> fmt::Display for NodeExecutionRefusal<Work> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionIssued { .. } => {
                formatter.write_str("the Node application declaration was already transferred")
            }
            Self::SessionClosed { .. } => formatter.write_str("the Node session is closed"),
            Self::Host { cause, .. } => write!(
                formatter,
                "the Node execution requires an entered Tokio runtime: {cause}"
            ),
        }
    }
}

impl<Work> Error for NodeExecutionRefusal<Work> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Host { cause, .. } => Some(cause),
            Self::ExecutionIssued { .. } | Self::SessionClosed { .. } => None,
        }
    }
}

impl<Work> fmt::Debug for NodeExecutionRefusal<Work> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionIssued { .. } => formatter
                .debug_struct("ExecutionIssued")
                .finish_non_exhaustive(),
            Self::SessionClosed { .. } => formatter
                .debug_struct("SessionClosed")
                .finish_non_exhaustive(),
            Self::Host { cause, .. } => formatter
                .debug_struct("Host")
                .field("cause", cause)
                .finish_non_exhaustive(),
        }
    }
}

impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families> {
    /// Open one private network connection for this Application.
    ///
    /// Network inputs explicitly supply each direction, its credentials, deployment
    /// roots and query capacity. Native configuration and fresh identity stay private.
    /// Successful startup transfers this declaration; every refusal returns the
    /// original Application and Network. Cancelling the entire opening surrenders
    /// those owned inputs. Native timeout/retry defaults establish no readiness or
    /// bounded startup guarantee.
    ///
    /// # Errors
    /// Returns the original Application and Network with the actual mapping, host
    /// or native opening cause. Outer diagnostics omit credentials and native causes.
    #[expect(
        clippy::result_large_err,
        reason = "return the actual Application and Network without boxing originals"
    )]
    pub async fn open_node(
        self,
        network: Network,
    ) -> Result<Node<Root, Spaces, Members, Families>, (Self, Network, NodeStartupRefusal)> {
        let config = match network.to_configuration() {
            Ok(config) => config,
            Err(cause) => return Err((self, network, cause)),
        };
        let host = match Handle::try_current() {
            Ok(host) => host,
            Err(cause) => return Err((self, network, NodeStartupRefusal::Host { cause })),
        };
        let flavor = host.runtime_flavor();
        if flavor != RuntimeFlavor::MultiThread {
            return Err((self, network, NodeStartupRefusal::HostFlavor { flavor }));
        }
        match zenoh::open(config).await {
            Ok(session) => Ok(Node {
                application: Some(self),
                session,
                exports: (),
                binding: None,
                closed: None,
                poll_panic: None,
                disposal_panic: None,
            }),
            Err(cause) => Err((self, network, NodeStartupRefusal::Open { cause })),
        }
    }
}

impl<Root, Spaces, Members, Families, Exports, BindingFn, BindResult>
    Node<Root, Spaces, Members, Families, Exports, BindingFn, BindResult>
{
    /// Read this connection owner's runtime identity as metadata text.
    ///
    /// The identity remains the same after connection close. Authentication and
    /// operation permission remain the provider's separate policies.
    #[must_use]
    pub fn runtime_id(&self) -> String {
        self.session.zid().to_string()
    }

    #[expect(
        clippy::type_complexity,
        reason = "return the original declaration and callable together without a wrapper or a self loan"
    )]
    fn take_application<Work>(
        &mut self,
        work: Work,
    ) -> Result<(Application<Root, Spaces, Members, Families>, Work), NodeExecutionRefusal<Work>>
    {
        if self.application.is_none() {
            return Err(NodeExecutionRefusal::ExecutionIssued { work });
        }
        if self.session.is_closed() {
            return Err(NodeExecutionRefusal::SessionClosed { work });
        }
        match self.application.take() {
            Some(application) => Ok((application, work)),
            None => Err(NodeExecutionRefusal::ExecutionIssued { work }),
        }
    }

    /// Close the Node connection independently of Application retirement.
    ///
    /// First acquire the execution's original work/outcome, native root and both
    /// notifications. Retain this separate close Result. Paired execution and
    /// receiving futures retain their Node borrow until their owners leave scope.
    /// Closing after surrendering an operation does not prove joined retirement.
    ///
    /// # Panics
    /// Retains the native SDK's host requirements. Open checks a multithreaded
    /// Tokio host, but does not bind later work or close to that same host.
    ///
    /// # Errors
    /// Preserves the SDK's original close error; this operation is not actor admission.
    pub async fn close(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.session.close().await
    }
}

#[allow(
    private_bounds,
    reason = "reuse the existing private Application installation proof"
)]
impl<Root, Spaces, Members, Families> Node<Root, Spaces, Members, Families>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Families: InstallEntityFamilies<Spaces> + Send,
    Spaces: RootRegistration<Root::Protocol> + Send + Sync + 'static,
{
    /// Await caller-local work and the complete joined application retirement.
    ///
    /// Work receives the ordinary Application handle for typed actor clients,
    /// lifecycle requests and composition, using the same callback contract as
    /// `Application::run_with_entities`. Work and its result may borrow caller-local values
    /// and need not be Send. This operation is cold until polled. Canceling a borrowed wait
    /// leaves the owned operation available; dropping the whole operation
    /// surrenders it.
    ///
    /// # Errors
    /// Returns the original callable when execution is refused. A missing host
    /// also restores the original declaration for retry on this same Node.
    ///
    /// # Panics
    /// Retains the existing application setup, work and native SDK contracts.
    pub async fn run_with<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        &mut self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Application<Root, Spaces, Members, Families>,
                Work,
                WorkFuture::Output,
                (),
                (
                    Root,
                    Spaces,
                    Families,
                    Work,
                    StagingFailure,
                    ApplicationScope,
                ),
                (Actor, Arc<Spaces>, ApplicationScope),
            >,
            (
                RootOrigin<Root>,
                ActorRetirement<
                    Actor,
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    ChildFailures,
                >,
            ),
            Families::Shutdowns,
        >,
        NodeExecutionRefusal<Work>,
    >
    where
        Spaces: ApplicationTopology<Root, Members> + NativeHost,
        Spaces::Runtime: NativeConstruction,
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<
                RuntimeChildBindings<
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    Members::Origins,
                >,
            > + Send
            + 'static,
        ChildBindings<
            Actor,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: ChildBindingsIn<Actor, Arc<Spaces>>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Arc<Spaces>,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: CommitActions<
                Actor,
                Retired = (
                    Vec<<Spaces as ApplicationTopology<Root, Members>>::Descendants>,
                    ChildFailures,
                ),
            > + Send
            + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        <Spaces as ApplicationTopology<Root, Members>>::Descendants:
            RetirementSchema + Send + 'static,
        ChildFailures: Send + 'static,
        Families::Installed: 'static,
        Work: FnOnce(
            ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> WorkFuture,
        WorkFuture: Future,
    {
        let (execution, receiving) = self.execute_with(work)?;
        execution.await;
        Ok(receiving.await)
    }

    /// Issue the declared application with ordinary typed actor work.
    ///
    /// Retain both returned operations outside disposable borrowed waits. Receiving
    /// conserves the original Work/outcome, complete native root/descendants and
    /// independently acquired termination and retirement notification results.
    /// Work receives the same Application handle as joined execution and may
    /// retain caller-local borrows and non-Send values.
    ///
    /// # Errors
    /// Returns original Work for closed Session, already-issued declaration or
    /// missing host; only the latter restores the original declaration for retry.
    #[expect(
        clippy::type_complexity,
        reason = "retain existing independent native and notification products without a new receiving wrapper"
    )]
    pub fn execute_with<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        &mut self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    WorkOutcome<
                        Application<Root, Spaces, Members, Families>,
                        Work,
                        <WorkFuture as Future>::Output,
                        (),
                        (
                            Root,
                            Spaces,
                            Families,
                            Work,
                            StagingFailure,
                            ApplicationScope,
                        ),
                        (Actor, Arc<Spaces>, ApplicationScope),
                    >,
                    (
                        RootOrigin<Root>,
                        ActorRetirement<
                            Actor,
                            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                            ChildFailures,
                        >,
                    ),
                    Families::Shutdowns,
                >,
            >,
        ),
        NodeExecutionRefusal<Work>,
    >
    where
        Spaces: ApplicationTopology<Root, Members> + NativeHost,
        Spaces::Runtime: NativeConstruction,
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<
                RuntimeChildBindings<
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    Members::Origins,
                >,
            > + Send
            + 'static,
        ChildBindings<
            Actor,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: ChildBindingsIn<Actor, Arc<Spaces>>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Arc<Spaces>,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: CommitActions<
                Actor,
                Retired = (
                    Vec<<Spaces as ApplicationTopology<Root, Members>>::Descendants>,
                    ChildFailures,
                ),
            > + Send
            + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        <Spaces as ApplicationTopology<Root, Members>>::Descendants:
            RetirementSchema + Send + 'static,
        ChildFailures: Send + 'static,
        Families::Installed: 'static,
        Work: FnOnce(
            ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> WorkFuture,
        WorkFuture: Future,
    {
        let (application, work) = self.take_application(work)?;
        match application.execute_with_entities_invocation(
            ApplicationWorkPresence::Supplied((
                work,
                move |work: Work,
                      application: ApplicationHandle<
                    Root::Protocol,
                    Actor::Event,
                    Families::Receptionists,
                >| { invoke(work, application) },
            )),
            None::<fn(&_) -> Ready<Result<Option<Ready<()>>, ()>>>,
        ) {
            Ok((execution, (receiving, received_families))) => Ok((execution, async move {
                let (work, root_report, root, notifications) = receiving.await;
                let work = work.into_supplied_with_entities();
                let families = received_families.await;
                ApplicationOutcome::new(work, root_report, root, notifications, families)
            })),
            Err((application, work, cause)) => {
                self.application = Some(application);
                Err(NodeExecutionRefusal::Host {
                    work: {
                        let (work, _invoke) = work.into_supplied();
                        work
                    },
                    cause,
                })
            }
        }
    }
}

#[allow(
    private_bounds,
    reason = "the exact composed actor supplies binding capability types"
)]
impl<Root, Spaces, Members, Families> Node<Root, Spaces, Members, Families>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Families: InstallEntityFamilies<Spaces> + Send,
{
    /// Attach one closed export product and its actual asynchronous actor binding.
    /// `BindingFn` and all target checks precede declaration startup and supplied Work.
    pub fn exports<Exports, BindingFn, Targets, BindError>(
        self,
        exports: Exports,
        binding: BindingFn,
    ) -> Node<Root, Spaces, Members, Families, Exports, BindingFn, Result<Targets, BindError>>
    where
        BindingFn: for<'application> AsyncFnOnce(
            &'application ApplicationHandle<
                Root::Protocol,
                <Members::Actor as Behavior>::Event,
                Families::Receptionists,
            >,
        ) -> Result<Targets, BindError>,
    {
        Node {
            application: self.application,
            session: self.session,
            exports,
            binding: Some(Binding::Uninvoked(binding)),
            closed: None,
            poll_panic: self.poll_panic,
            disposal_panic: self.disposal_panic,
        }
    }
}
impl<Root, Spaces, Members, Families, Exports, BindingFn, Targets, BindError>
    Node<Root, Spaces, Members, Families, Exports, BindingFn, Result<Targets, BindError>>
{
    /// Consume exports, binding, post-join close and original kernel panic causes.
    /// Refuse with the same Node until joined execution acquired native close.
    ///
    /// # Errors
    /// Returns the same Node if either its binding result or close result is absent.
    #[expect(
        clippy::type_complexity,
        reason = "consuming extraction preserves each original binding, close and panic value without an aggregate wrapper"
    )]
    pub fn into_parts(
        self,
    ) -> Result<
        (
            Exports,
            Option<Result<Targets, BindError>>,
            Result<(), zenoh::Error>,
            Option<Panic>,
            Option<Panic>,
        ),
        Self,
    > {
        match self {
            Self {
                exports,
                binding: Some(Binding::Invoked(bound @ Some(_))),
                closed: Some(closed),
                poll_panic,
                disposal_panic,
                ..
            } => Ok((exports, bound, closed, poll_panic, disposal_panic)),
            original => Err(original),
        }
    }
}

#[allow(
    private_bounds,
    reason = "reuse the existing private Application and static export proofs"
)]
impl<Root, Spaces, Members, Families, Exports, BindingFn, Targets, BindError>
    Node<Root, Spaces, Members, Families, Exports, BindingFn, Result<Targets, BindError>>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Families: InstallEntityFamilies<Spaces> + Send,
    Spaces: RootRegistration<Root::Protocol> + Send + Sync + 'static,
{
    /// Run supplied caller-local work beside the Node-owned exports and join cleanup.
    ///
    /// # Errors
    /// Returns the original callable if execution was already issued, the Session
    /// is closed, or the Tokio host is absent. Host refusal restores the Application.
    /// Binding or declaration refusal remains in the joined outcome and receipts.
    ///
    /// # Panics
    /// Native declaration withdrawal and Session close retain their SDK contracts.
    /// Kernel poll and disposal panic causes remain stored in this Node.
    pub async fn run_with<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        &mut self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Application<Root, Spaces, Members, Families>,
                Work,
                WorkFuture::Output,
                (),
                (
                    Root,
                    Spaces,
                    Families,
                    Work,
                    StagingFailure,
                    ApplicationScope,
                ),
                (Actor, Arc<Spaces>, ApplicationScope),
            >,
            (
                RootOrigin<Root>,
                ActorRetirement<
                    Actor,
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    ChildFailures,
                >,
            ),
            Families::Shutdowns,
        >,
        NodeExecutionRefusal<Work>,
    >
    where
        Spaces: ApplicationTopology<Root, Members> + NativeHost,
        Spaces::Runtime: NativeConstruction,
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<
                RuntimeChildBindings<
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    Members::Origins,
                >,
            > + Send
            + 'static,
        ChildBindings<
            Actor,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: ChildBindingsIn<Actor, Arc<Spaces>>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Arc<Spaces>,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: CommitActions<
                Actor,
                Retired = (
                    Vec<<Spaces as ApplicationTopology<Root, Members>>::Descendants>,
                    ChildFailures,
                ),
            > + Send
            + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        <Spaces as ApplicationTopology<Root, Members>>::Descendants:
            RetirementSchema + Send + 'static,
        ChildFailures: Send + 'static,
        Families::Installed: 'static,
        Work: FnOnce(
            ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> WorkFuture,
        WorkFuture: Future,
        BindingFn: for<'application> AsyncFnOnce(
            &'application ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> Result<Targets, BindError>,
        Exports: ActorExports<Root::Protocol, Actor::Event, Families::Receptionists, Targets>,
    {
        let (application, work) = self.take_application(work)?;
        if let Err(cause) = Handle::try_current() {
            self.application = Some(application);
            return Err(NodeExecutionRefusal::Host { work, cause });
        }
        let binding = &mut self.binding;
        let exports = &mut self.exports;
        let session = &self.session;
        let runtime = session.zid().to_string();
        let startup = async move |application: &ApplicationHandle<
            Root::Protocol,
            Actor::Event,
            Families::Receptionists,
        >| {
            let joined = application
                .cleanup()
                .expect("Node binding receives its actual Application cleanup observation");
            let call = match binding.take() {
                Some(Binding::Uninvoked(call)) => call,
                Some(Binding::Invoked(_)) | None => {
                    unreachable!("the Application declaration issues its binding once")
                }
            };
            *binding = Some(Binding::Invoked(None));
            let receipt = call(application).await;
            *binding = Some(Binding::Invoked(Some(receipt)));
            let Some(Binding::Invoked(Some(Ok(targets)))) = binding.as_ref() else {
                return Err(());
            };
            let controls = exports.preflight(application, targets, &mut Vec::new())?;
            exports.declare(session).await?;
            Ok(Some(exports.serve(controls, runtime, joined)))
        };
        let outcome = match application.execute_with_entities_invocation(
            ApplicationWorkPresence::Supplied((work, invoke)),
            Some(startup),
        ) {
            Ok((execution, (receiving, families))) => {
                let mut execution = Box::pin(execution);
                self.poll_panic = poll_fn(|context| {
                    match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        original => Poll::Ready(original),
                    }
                })
                .await
                .err();
                self.disposal_panic = catch_unwind(AssertUnwindSafe(|| drop(execution))).err();
                let (work, root_report, native, notes) = receiving.await;
                let work = work.into_supplied_with_entities();
                let families = families.await;
                Ok(ApplicationOutcome::new(
                    work,
                    root_report,
                    native,
                    notes,
                    families,
                ))
            }
            Err((application, work, cause)) => {
                self.application = Some(application);
                let (work, _invoke) = work.into_supplied();
                Err(NodeExecutionRefusal::Host { work, cause })
            }
        };
        match outcome {
            Ok(outcome) => {
                self.exports.withdraw().await;
                self.closed = Some(self.session.close().await);
                Ok(outcome)
            }
            Err(cause) => Err(cause),
        }
    }
    /// Run exports with genuine Work absence, then retain native cleanup and close.
    ///
    /// # Errors
    /// Refuses if execution was already issued, the Session is closed, or the Tokio
    /// host is absent. Host refusal restores the Application for another attempt.
    /// Binding or declaration refusal remains in the joined outcome and receipts.
    ///
    /// # Panics
    /// Native declaration withdrawal and Session close retain their SDK contracts.
    /// Kernel poll and disposal panic causes remain stored in this Node.
    pub async fn run<Actor, StagingFailure, ChildFailures>(
        &mut self,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Application<Root, Spaces, Members, Families>,
                Option<Never>,
                Option<Never>,
                (),
                (
                    Root,
                    Spaces,
                    Families,
                    Option<Never>,
                    StagingFailure,
                    ApplicationScope,
                ),
                (Actor, Arc<Spaces>, ApplicationScope),
            >,
            (
                RootOrigin<Root>,
                ActorRetirement<
                    Actor,
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    ChildFailures,
                >,
            ),
            Families::Shutdowns,
        >,
        NodeExecutionRefusal<Option<Never>>,
    >
    where
        Spaces: ApplicationTopology<Root, Members> + NativeHost,
        Spaces::Runtime: NativeConstruction,
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<
                RuntimeChildBindings<
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    Members::Origins,
                >,
            > + Send
            + 'static,
        ChildBindings<
            Actor,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: ChildBindingsIn<Actor, Arc<Spaces>>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Arc<Spaces>,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Members::Origins,
        >: CommitActions<
                Actor,
                Retired = (
                    Vec<<Spaces as ApplicationTopology<Root, Members>>::Descendants>,
                    ChildFailures,
                ),
            > + Send
            + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        <Spaces as ApplicationTopology<Root, Members>>::Descendants:
            RetirementSchema + Send + 'static,
        ChildFailures: Send + 'static,
        Families::Installed: 'static,
        BindingFn: for<'application> AsyncFnOnce(
            &'application ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> Result<Targets, BindError>,
        Exports: ActorExports<Root::Protocol, Actor::Event, Families::Receptionists, Targets>,
    {
        let (application, work) = self.take_application(None)?;
        if let Err(cause) = Handle::try_current() {
            self.application = Some(application);
            return Err(NodeExecutionRefusal::Host { work, cause });
        }
        let binding = &mut self.binding;
        let exports = &mut self.exports;
        let session = &self.session;
        let runtime = session.zid().to_string();
        let startup = async move |application: &ApplicationHandle<
            Root::Protocol,
            Actor::Event,
            Families::Receptionists,
        >| {
            let joined = application
                .cleanup()
                .expect("Node binding receives its actual Application cleanup observation");
            let call = match binding.take() {
                Some(Binding::Uninvoked(call)) => call,
                Some(Binding::Invoked(_)) | None => {
                    unreachable!("the Application declaration issues its binding once")
                }
            };
            *binding = Some(Binding::Invoked(None));
            let receipt = call(application).await;
            *binding = Some(Binding::Invoked(Some(receipt)));
            let Some(Binding::Invoked(Some(Ok(targets)))) = binding.as_ref() else {
                return Err(());
            };
            let controls = exports.preflight(application, targets, &mut Vec::new())?;
            exports.declare(session).await?;
            Ok(Some(exports.serve(controls, runtime, joined)))
        };
        let outcome = match application
            .execute_with_entities_invocation::<Actor, StagingFailure, ChildFailures, Never, fn(
                Never,
                ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
            ) -> Ready<Never>, Ready<Never>, (), _, _>(
                ApplicationWorkPresence::Absent(()),
                Some(startup),
            ) {
            Ok((execution, (receiving, families))) => {
                let mut execution = Box::pin(execution);
                self.poll_panic = poll_fn(|context| {
                    match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        original => Poll::Ready(original),
                    }
                })
                .await
                .err();
                self.disposal_panic = catch_unwind(AssertUnwindSafe(|| drop(execution))).err();
                let (work, root_report, native, notes) = receiving.await;
                let work = work.into_absent_with_entities();
                let families = families.await;
                Ok(ApplicationOutcome::new(
                    work,
                    root_report,
                    native,
                    notes,
                    families,
                ))
            }
            Err((application, _, cause)) => {
                self.application = Some(application);
                Err(NodeExecutionRefusal::Host { work, cause })
            }
        };
        match outcome {
            Ok(outcome) => {
                self.exports.withdraw().await;
                self.closed = Some(self.session.close().await);
                Ok(outcome)
            }
            Err(cause) => Err(cause),
        }
    }
}

#[cfg(test)]
#[path = "node/tests/mod.rs"]
mod tests;
