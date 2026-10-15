//! One finalized local Application associated with one freshly opened native runtime.

use super::composition::ComposeApplication;
use super::execution::RootInterpreter;
use super::{Application, ApplicationHandle, ApplicationOutcome, ApplicationScope, WorkOutcome};
use crate::NativeHost;
use crate::address::MailAddr;
use crate::local::children::{
    ChildBindings, ChildBindingsIn, RetireChildTasks, RuntimeChildBindings,
};
use crate::local::effects::CommitActions;
use crate::local::effects::services::NativeConstruction;
use crate::remote::startup::{NodeStartupRefusal, check_node_configuration};
use crate::terminal::{ActorRetirement, RetirementSchema, RootOrigin};
use crate::topology::{ApplicationTopology, RootRegistration};
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Never, Protocol,
};
use core::fmt;
use core::future::Future;
use std::error::Error;
use tokio::runtime::{Handle, RuntimeFlavor, TryCurrentError};
use zenoh::{Error as ZenohError, Session};
use zenoh_config::Config;

/// One finalized ordinary Application and its fresh owned native Session.
///
/// Construct topology and children on Application before opening. Node issues
/// that declaration once; it adds no actor lifecycle or replacement policy.
/// Borrowed execution waits leave this owner and its Session outside the wait.
/// Dropping an entire operation is surrender, not a joined retirement fact.
pub struct Node<Root, Spaces, Members = ()> {
    application: Option<Application<Root, Spaces, Members>>,
    session: Session,
}

impl<Root, Spaces, Members> fmt::Debug for Node<Root, Spaces, Members> {
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

impl<Root, Spaces, Members> Application<Root, Spaces, Members> {
    /// Validate the raw native configuration before opening one fresh runtime.
    ///
    /// Open an SDK-defined clone so a startup refusal retains the supplied Config.
    /// Native ID expansion affects that opening clone only. No supplied Session
    /// or shared runtime is accepted. Successful startup transfers this declaration.
    ///
    /// # Errors
    /// Returns the original Application and Config with the actual profile or SDK
    /// cause. Cancelling the whole owned opening operation surrenders those inputs.
    #[expect(
        clippy::result_large_err,
        reason = "return the actual Application and native configuration without boxing originals"
    )]
    pub async fn open_node(
        self,
        config: Config,
    ) -> Result<Node<Root, Spaces, Members>, (Self, Config, NodeStartupRefusal)> {
        let config = match check_node_configuration(config) {
            Ok(config) => config,
            Err((config, cause)) => return Err((self, config, cause)),
        };
        let host = match Handle::try_current() {
            Ok(host) => host,
            Err(cause) => return Err((self, config, NodeStartupRefusal::Host { cause })),
        };
        let flavor = host.runtime_flavor();
        if flavor != RuntimeFlavor::MultiThread {
            return Err((self, config, NodeStartupRefusal::HostFlavor { flavor }));
        }
        let opening = config.clone();
        match zenoh::open(opening).await {
            Ok(session) => Ok(Node {
                application: Some(self),
                session,
            }),
            Err(cause) => Err((self, config, NodeStartupRefusal::Open { cause })),
        }
    }
}

impl<Root, Spaces, Members> Node<Root, Spaces, Members> {
    #[expect(
        clippy::type_complexity,
        reason = "return the original declaration and callable together without a wrapper or a self loan"
    )]
    fn take_application<Work>(
        &mut self,
        work: Work,
    ) -> Result<(Application<Root, Spaces, Members>, Work), NodeExecutionRefusal<Work>> {
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
    pub async fn close(&mut self) -> Result<(), ZenohError> {
        self.session.close().await
    }
}

#[allow(
    private_bounds,
    reason = "reuse the existing private Application installation proof"
)]
impl<Root, Spaces, Members> Node<Root, Spaces, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Spaces: RootRegistration<Root::Protocol> + Send + Sync + 'static,
{
    /// Await caller-local work and the complete joined application retirement.
    ///
    /// Work receives the ordinary Application handle for typed actor clients,
    /// lifecycle requests and composition, using the same callback contract as
    /// `Application::run_with`. Work and its result may borrow caller-local values
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
                Application<Root, Spaces, Members>,
                Work,
                WorkFuture::Output,
                (),
                (Root, Spaces, Work, StagingFailure, ApplicationScope),
                (Actor, Spaces, ApplicationScope),
            >,
            (
                RootOrigin<Root>,
                ActorRetirement<
                    Actor,
                    <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                    ChildFailures,
                >,
            ),
            (),
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
        >: ChildBindingsIn<Actor, Spaces>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Spaces,
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
        Work: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> WorkFuture,
        WorkFuture: Future,
    {
        let (application, work) = self.take_application(work)?;
        let session = &self.session;
        match application.execute_with_invocation(work, move |work: Work, mut application| {
            application.session = Some(session.clone());
            work(application)
        }) {
            Ok((execution, receiving)) => {
                execution.await;
                Ok(receiving.await)
            }
            Err((application, work, cause)) => {
                self.application = Some(application);
                Err(NodeExecutionRefusal::Host { work, cause })
            }
        }
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
                        Application<Root, Spaces, Members>,
                        Work,
                        <WorkFuture as Future>::Output,
                        (),
                        (Root, Spaces, Work, StagingFailure, ApplicationScope),
                        (Actor, Spaces, ApplicationScope),
                    >,
                    (
                        RootOrigin<Root>,
                        ActorRetirement<
                            Actor,
                            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                            ChildFailures,
                        >,
                    ),
                    (),
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
        >: ChildBindingsIn<Actor, Spaces>
            + RetireChildTasks<
                Root = <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                Failures = ChildFailures,
            > + Send
            + 'static,
        RootInterpreter<
            Actor,
            Spaces,
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
        Work: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> WorkFuture,
        WorkFuture: Future,
    {
        let (application, work) = self.take_application(work)?;
        let session = &self.session;
        match application.execute_with_invocation(work, move |work: Work, mut application| {
            application.session = Some(session.clone());
            work(application)
        }) {
            Ok(products) => Ok(products),
            Err((application, work, cause)) => {
                self.application = Some(application);
                Err(NodeExecutionRefusal::Host { work, cause })
            }
        }
    }
}
