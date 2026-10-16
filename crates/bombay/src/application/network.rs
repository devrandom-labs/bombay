//! Network startup and ordinary work share the existing Application owner.

use super::composition::ComposeApplication;
use super::execution::{
    ApplicationWorkCustody, ApplicationWorkPresence, ApplicationWorkPublication,
    PreparedApplication, RootInterpreter, execute_application_with,
};
use super::{Application, ApplicationHandle, ApplicationOutcome, ApplicationScope};
use crate::address::MailAddr;
use crate::entity::{InstallEntityFamilies, InstalledEntityFamilies};
use crate::local::children::{
    ChildBindings, ChildBindingsIn, RetireChildTasks, RuntimeChildBindings,
};
use crate::local::effects::CommitActions;
use crate::remote::{Connection, Network, NodeStartupRefusal};
use crate::terminal::{ActorNotificationReceipts, ActorRetirement, RetirementSchema, RootOrigin};
use crate::topology::{ApplicationTopology, RootRegistration};
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Never, Protocol,
};
use core::future::Future;
use std::error::Error;
use std::sync::Arc;
use tokio::runtime::{Handle, TryCurrentError};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

#[allow(
    private_bounds,
    reason = "reuse the existing closed Application composition and native retirement proofs"
)]
impl<Root, Spaces, Members, Families, Actor, StagingFailure, ChildFailures>
    Application<Root, Spaces, Members, Families>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
    Families: InstallEntityFamilies<Spaces> + Send,
    Families::Installed: 'static,
    Spaces: RootRegistration<Root::Protocol>
        + ApplicationTopology<Root, Members>
        + Send
        + Sync
        + 'static,
    Actor:
        BehaviorBase + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never> + Send + 'static,
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
    <Spaces as ApplicationTopology<Root, Members>>::Descendants: RetirementSchema + Send + 'static,
    ChildFailures: Send + 'static,
{
    /// Execute ordinary application work with an Application-owned connection.
    ///
    /// Construction is cold. Retain all three returned operations outside
    /// disposable borrowed waits. Startup refusal returns the original cold
    /// Application, Network and Work through `Unstarted`; the independent
    /// connection receiver retains its sole actual cause. Native actor,
    /// notification and Entity-family results keep their ordinary contracts.
    ///
    /// Connection retirement follows joined root retirement, the work barrier
    /// and family shutdown. Its receiver preserves producer join and original
    /// close result separately. Dropping that whole receiver surrenders unread
    /// results without transferring connection retirement to the caller.
    ///
    /// This entry point establishes connection ownership; work receives the
    /// ordinary typed `ApplicationHandle`. It does not itself authenticate or
    /// admit remote commands, publish exports or establish a TLS handshake.
    ///
    /// # Errors
    /// Returns original Application, Network and Work with a missing-host error.
    /// Later startup refusal is owned by independent connection receiving.
    ///
    /// # Panics
    /// Application composition and Work retain their existing unwind contract.
    /// Retained receiving is independent of the execution wait.
    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        clippy::result_large_err,
        reason = "preserve actual cold, prepared, native, notification, family and connection products through the existing execution owner"
    )]
    pub fn execute_remote<Work, WorkFuture>(
        self,
        network: Network,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = (
                    ApplicationOutcome<
                        (Self, Network),
                        (Network, Work),
                        WorkFuture::Output,
                        (),
                        (
                            Root,
                            Spaces,
                            Families,
                            Network,
                            Work,
                            StagingFailure,
                            ApplicationScope,
                        ),
                        (Actor, Arc<Spaces>, ApplicationScope),
                        Work,
                    >,
                    Result<
                        (
                            RootOrigin<Root>,
                            ActorRetirement<
                                Actor,
                                <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                                ChildFailures,
                            >,
                        ),
                        RecvError,
                    >,
                    Result<ActorNotificationReceipts, RecvError>,
                    Families::Shutdowns,
                ),
            >,
            impl Future<
                Output = Result<
                    Result<
                        (
                            Result<(), JoinError>,
                            Result<Result<(), Box<dyn Error + Send + Sync>>, RecvError>,
                        ),
                        NodeStartupRefusal,
                    >,
                    RecvError,
                >,
            >,
        ),
        (Self, Network, Work, TryCurrentError),
    >
    where
        Work: FnOnce(
            ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> WorkFuture,
        WorkFuture: Future,
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(cause) => return Err((self, network, work, cause)),
        };
        let entity_executor = executor.clone();
        let (family_publications, received_families) = Families::Installed::shutdown_receiving();
        let (acquire_connection, connection_receiving) = Connection::startup();
        let (execution, receiving) = execute_application_with::<
            _,
            Root,
            Actor,
            Arc<Spaces>,
            Members::Origins,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            Families::Installed,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            Never,
        >(
            executor,
            (self, network),
            (work, |(network, work): (Network, Work), application| {
                drop(network);
                work(application)
            }),
            async move |(application, network): &(Self, Network)| {
                let startup = acquire_connection(network).await;
                (startup, application.allocations())
            },
            move |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
                  (connection, publication),
                  allocations| {
                let refused_startup = publication.err();
                let Some(connection) = connection else {
                    // The original cold publication already owns every input.
                    // A surrendered connection receiver gives up this cause.
                    drop(refused_startup);
                    return Err(original_work);
                };
                let ((application, network), (work, invoke), publication) =
                    original_work.into_unstarted();
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Supplied(((network, work), invoke)),
                            None,
                        ),
                        publication,
                    )),
                };
                let (root, spaces, members, families, scope) = application.into_parts();
                let scope = scope.unwrap_or_else(|| ApplicationScope {
                    allocations: allocations.clone(),
                });
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        let ((network, work), invoke) = work.into_supplied();
                        let _ = invoke;
                        let rejected = ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected((
                                    root, spaces, families, network, work, failure, scope,
                                )),
                                publication,
                            )),
                        };
                        connection.retire();
                        drop(refused_startup);
                        return Err(rejected);
                    }
                };
                let spaces = Arc::new(spaces);
                let installed_families =
                    families.install(Arc::clone(&spaces), allocations, entity_executor);
                let (work, publication) = original_work.into_not_invoked();
                let prepared = PreparedApplication {
                    work: ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces, scope), work),
                            publication,
                        )),
                    },
                    families: installed_families,
                    connection: Some(connection),
                };
                // Keep the refused original until its connection has an owner.
                // Surrender is explicit; it does not stop that owner's cleanup.
                drop(refused_startup);
                Ok(prepared)
            },
            (
                family_publications,
                |executor: Handle, installed_families: Families::Installed, publications| {
                    Some(executor.spawn(installed_families.shutdown(publications)))
                },
            ),
        );
        let receiving = async move {
            let (outcome, native, notifications) = receiving.await;
            let families = received_families.await;
            (outcome.into_supplied(), native, notifications, families)
        };
        Ok((execution, receiving, connection_receiving))
    }

    /// Drive the same remote execution and return its five independent results.
    ///
    /// Use `execute_remote` when receiving must outlive cancellation of an
    /// execution wait. Cancelling this entire convenience future surrenders
    /// its owned operations and unread results under the ordinary contract.
    ///
    /// # Errors
    /// Returns original cold inputs with the actual missing-host error.
    ///
    /// # Panics
    /// Composition and Work retain their original unwind behavior.
    #[expect(
        clippy::result_large_err,
        reason = "retain the existing four Application results and recover the original cold inputs without boxing"
    )]
    pub async fn run_remote<Work, WorkFuture>(
        self,
        network: Network,
        work: Work,
    ) -> Result<
        (
            ApplicationOutcome<
                (Self, Network),
                (Network, Work),
                WorkFuture::Output,
                (),
                (
                    Root,
                    Spaces,
                    Families,
                    Network,
                    Work,
                    StagingFailure,
                    ApplicationScope,
                ),
                (Actor, Arc<Spaces>, ApplicationScope),
                Work,
            >,
            Result<
                (
                    RootOrigin<Root>,
                    ActorRetirement<
                        Actor,
                        <Spaces as ApplicationTopology<Root, Members>>::Descendants,
                        ChildFailures,
                    >,
                ),
                RecvError,
            >,
            Result<ActorNotificationReceipts, RecvError>,
            Families::Shutdowns,
            Result<
                Result<
                    (
                        Result<(), JoinError>,
                        Result<Result<(), Box<dyn Error + Send + Sync>>, RecvError>,
                    ),
                    NodeStartupRefusal,
                >,
                RecvError,
            >,
        ),
        (Self, Network, Work, TryCurrentError),
    >
    where
        Work: FnOnce(
            ApplicationHandle<Root::Protocol, Actor::Event, Families::Receptionists>,
        ) -> WorkFuture,
        WorkFuture: Future,
    {
        let (execution, receiving, connection_receiving) = self.execute_remote(network, work)?;
        execution.await;
        let (outcome, native, notifications, families) = receiving.await;
        let connection = connection_receiving.await;
        Ok((outcome, native, notifications, families, connection))
    }
}
