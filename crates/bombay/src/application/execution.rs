use super::{ApplicationScope, MailboxCapacity};
use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::composition::ComposeApplication;
use crate::application::{Application, ApplicationHandle, ApplicationOutcome, WorkOutcome};
use crate::entity::{InstallEntityFamilies, InstalledEntityFamilies};
use crate::launch::spawn_local_execution;
use crate::local::children::{
    ChildBindings, ChildBindingsIn, RetireChildTasks, RuntimeChildBindings,
};
use crate::local::effects::{
    ActionInterpreter, ApplicationCapabilities, ApplicationCapabilityInputs, CommitActions,
    NoParent,
};
use crate::local::ingress::{DEFAULT_USER_CAPACITY, StandardIngress};
use crate::observe;
use crate::terminal::{
    ActorNotificationReceipts, ActorRetirement, ActorRetirementReport, RetirementNotificationError,
    RetirementSchema, RootOrigin, publish_retirement_report,
};
use crate::topology::{ApplicationTopology, RootRegistration};
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Never, Protocol,
};
use core::fmt;
use core::future::{Future, Ready, poll_fn};
use core::ops::AsyncFnOnce;
use core::pin::pin;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use tokio::runtime::{Builder, Handle, TryCurrentError};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::{JoinError, JoinHandle};

impl fmt::Debug for RunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BlockingInEnteredRuntime => "BlockingInEnteredRuntime",
            Self::Runtime(_) => "Runtime",
        })
    }
}

#[allow(
    private_bounds,
    reason = "native installation and launch proofs remain private static composition"
)]
impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Families: InstallEntityFamilies<Spaces> + Send,
    Spaces: RootRegistration<Root::Protocol> + Send + Sync + 'static,
{
    /// Pair caller-local Entity work with original root and family retirement receiving.
    /// The application's topology selects the complete descendant schema;
    /// callers need no retirement type argument.
    /// The receiving product keeps work/startup phase, root receiving and each family receiving result.
    /// Its cleanup join remains the actual unit producer outcome beside independently acquired facts.
    /// A receiving error never infers root absence or completed family shutdown.
    /// Definitions install before receptionists; the original borrowed setup cause stays native.
    ///
    /// # Errors
    /// Returns untouched application and bare Work with actual missing-host error.
    ///
    /// # Panics
    /// Original borrowed setup or Work causes unwind in the caller; retained receiving remains independent.
    #[expect(
        clippy::type_complexity,
        reason = "retain independent work phase, original root reply and role-indexed shutdown receiving results"
    )]
    pub fn execute_with_entities<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    WorkOutcome<
                        Self,
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
        (Self, Work, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, work, error)),
        };
        let entity_executor = executor.clone();
        let (family_publications, received_families) =
            <Families::Installed as InstalledEntityFamilies>::shutdown_receiving();
        let (execution, receiving) = execute_application_with::<
            Self,
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
            self,
            (work, |work: Work, application| work(application)),
            async |application: &Self| ((), application.allocations()),
            move |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
                  startup_inputs,
                  allocations| {
                let (application, cold_work, publication) = original_work.into_unstarted();
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Supplied(cold_work),
                            None,
                        ),
                        publication,
                    )),
                };
                let () = startup_inputs;
                let (root, spaces, members, families, scope) = application.into_parts();
                let scope = scope.unwrap_or_else(|| ApplicationScope {
                    allocations: allocations.clone(),
                    client_capacity: MailboxCapacity::default(),
                });
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        let (work, invoke) = work.into_supplied();
                        let _ = invoke;
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected((
                                    root, spaces, families, work, failure, scope,
                                )),
                                publication,
                            )),
                        });
                    }
                };
                let spaces = Arc::new(spaces);
                let installed_families =
                    families.install(Arc::clone(&spaces), allocations, entity_executor);
                let (work, publication) = original_work.into_not_invoked();
                Ok((
                    ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces, scope), work),
                            publication,
                        )),
                    },
                    installed_families,
                ))
            },
            (
                family_publications,
                |executor: Handle, installed_families: Families::Installed, publications| {
                    Some(executor.spawn(installed_families.shutdown(publications)))
                },
            ),
        );
        let receiving = async move {
            let (work, root_retirement, termination_notification) = receiving.await;
            let work_outcome = work.into_supplied();
            let family_retirements = received_families.await;
            ApplicationOutcome::new(
                work_outcome,
                root_retirement,
                termination_notification,
                family_retirements,
            )
        };
        Ok((execution, receiving))
    }

    /// Await caller-local Entity work and the exact independent root/family receiving product.
    /// Descendant custody is selected by the same application topology as ordinary execution.
    /// Use `execute_with_entities` to retain receiving independently after execution is dropped.
    ///
    /// # Errors
    /// Returns original application and Work with the actual entered-host error.
    ///
    /// # Panics
    /// Setup and Work can unwind; original native causes remain caller-owned.
    pub async fn run_with_entities<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Self,
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
        (Self, Work, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
        let (execution, result) = self.execute_with_entities(work)?;
        execution.await;
        Ok(result.await)
    }
}

#[allow(
    private_bounds,
    reason = "the running application and its launch proof remain private"
)]
impl<Root, Spaces, Members> Application<Root, Spaces, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Spaces: RootRegistration<Root::Protocol> + Send + Sync + 'static,
{
    /// Execute declared cold composition and caller-local work with an independent result receiver.
    /// The application's topology selects the complete descendant schema;
    /// callers need no retirement type argument.
    ///
    /// A cold staging rejection owns the actual partial and original callable without a cleanup fiction.
    /// Original interpretation and source custody travel with actor retirement;
    /// their `Send + 'static` requirements do not constrain caller-local work.
    ///
    /// # Errors
    ///
    /// Returns the original application and callable with the exact `TryCurrentError`
    /// when no Tokio runtime is currently entered; neither input has been started.
    #[expect(
        clippy::type_complexity,
        reason = "return independent local work and actual cold-partial or joined actor products without another wrapper"
    )]
    pub fn execute_with<Actor, StagingFailure, ChildFailures, Work, WorkFuture>(
        self,
        work: Work,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    WorkOutcome<
                        Self,
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
        (Self, Work, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, work, error)),
        };
        let (execution, receiving) = execute_application_with::<
            Self,
            Root,
            Actor,
            Spaces,
            Members::Origins,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            (),
            (Root, Spaces, Work, StagingFailure, ApplicationScope),
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
            self,
            (work, |work: Work, application| work(application)),
            async |application: &Self| ((), application.allocations()),
            |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
             startup_inputs,
             allocations| {
                let (application, cold_work, publication) = original_work.into_unstarted();
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Supplied(cold_work),
                            None,
                        ),
                        publication,
                    )),
                };
                let () = startup_inputs;
                let (root, spaces, members, (), scope) = application.into_parts();
                let scope = match scope {
                    Some(scope) => scope,
                    None => ApplicationScope {
                        allocations,
                        client_capacity: MailboxCapacity::default(),
                    },
                };
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        let (work, invoke) = work.into_supplied();
                        let _ = invoke;
                        let inputs = (root, spaces, work, failure, scope);
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected(inputs),
                                publication,
                            )),
                        });
                    }
                };
                let installed_families = ();
                let (work, publication) = original_work.into_not_invoked();
                Ok((
                    ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces, scope), work),
                            publication,
                        )),
                    },
                    installed_families,
                ))
            },
            ((), |_executor, (), ()| None),
        );
        let receiving = async move {
            let (work, root, termination_notification) = receiving.await;
            ApplicationOutcome::new(work.into_supplied(), root, termination_notification, ())
        };
        Ok((execution, receiving))
    }

    /// Construct genuine absent work with original inputs and joined raw root facts.
    /// Descendant custody is selected by the same application topology as supplied work.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    #[expect(
        clippy::type_complexity,
        reason = "the existing concrete pair preserves original inputs and independent exact root custody without a new wrapper"
    )]
    pub fn execute<Actor, StagingFailure, ChildFailures>(
        self,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    WorkOutcome<
                        Self,
                        Option<Never>,
                        Option<Never>,
                        (),
                        (Root, Spaces, StagingFailure, ApplicationScope),
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
        (Self, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err((self, error)),
        };
        let (execution, receiving) = execute_application_with::<
            Self,
            Root,
            Actor,
            Spaces,
            Members::Origins,
            <Spaces as ApplicationTopology<Root, Members>>::Descendants,
            (),
            (Root, Spaces, StagingFailure, ApplicationScope),
            _,
            _,
            _,
            Never,
            fn(Never, ApplicationHandle<Root::Protocol, Actor::Event>) -> Ready<Never>,
            Ready<Never>,
            _,
            _,
            (),
        >(
            executor,
            self,
            (),
            async |application: &Self| ((), application.allocations()),
            |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
             startup_inputs,
             allocations| {
                let (application, cold_work, publication) = original_work.into_unstarted();
                let () = cold_work;
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Absent(()),
                            None,
                        ),
                        publication,
                    )),
                };
                let () = startup_inputs;
                let (root, spaces, members, (), scope) = application.into_parts();
                let scope = match scope {
                    Some(scope) => scope,
                    None => ApplicationScope {
                        allocations,
                        client_capacity: MailboxCapacity::default(),
                    },
                };
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        match work {
                            ApplicationWorkPresence::Absent(()) => {}
                            ApplicationWorkPresence::Supplied((never, _invoke)) => match never {},
                        }
                        let inputs = (root, spaces, failure, scope);
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected(inputs),
                                publication,
                            )),
                        });
                    }
                };
                let installed_families = ();
                let (work, publication) = original_work.into_not_invoked();
                Ok((
                    ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces, scope), work),
                            publication,
                        )),
                    },
                    installed_families,
                ))
            },
            ((), |_executor, (), ()| None),
        );
        let receiving = async move {
            let (work, root, termination_notification) = receiving.await;
            ApplicationOutcome::new(work.into_absent(), root, termination_notification, ())
        };
        Ok((execution, receiving))
    }

    /// Await genuine absent work with original inputs and joined raw root facts.
    /// `Completed { output: None }` means an actual startup grant was acquired without supplied work.
    /// This comparison selects no terminal projection and constructs no callable value.
    /// The execution remains cold; only polling stages or starts the actor.
    ///
    /// # Errors
    /// Returns the untouched application and actual entered-host error.
    ///
    /// # Panics
    /// Execution propagates staging panics; receiving preserves the actual cleanup publication result.
    pub async fn run<Actor, StagingFailure, ChildFailures>(
        self,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Self,
                Option<Never>,
                Option<Never>,
                (),
                (Root, Spaces, StagingFailure, ApplicationScope),
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
        (Self, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
    {
        let (execution, result) = self.execute()?;
        execution.await;
        Ok(result.await)
    }

    /// Drive this same cold application future on one configured owned Tokio host.
    /// Configure and enable the required drivers on the supplied Builder first.
    /// Neither scheduler nor worker count is substituted or defaulted here.
    ///
    /// The outer error returns the untouched declaration and the same Builder.
    /// `RunError::BlockingInEnteredRuntime` rejects an entered Tokio runtime;
    /// `RunError::Runtime` retains the actual construction error. Neither starts an actor.
    /// The outer success retains the existing checked async result unchanged.
    ///
    /// # Errors
    /// Returns original inputs before construction when called inside Tokio;
    /// construction failure preserves the actual error and retryable inputs.
    /// Async execution retains its complete original result and entered-host error.
    ///
    /// # Panics
    /// Invalid Builder configuration may have panicked before this call. Native
    /// declaration/setup panics propagate as for run; receiver surrender and host
    /// destruction are not a promise that async cleanup completed.
    #[expect(
        clippy::type_complexity,
        reason = "the existing async result and exact cold Builder/input refusal remain independent"
    )]
    #[expect(
        clippy::result_large_err,
        reason = "return the actual configured Builder and declaration without boxing originals"
    )]
    pub fn run_blocking<Actor, StagingFailure, ChildFailures>(
        self,
        mut builder: Builder,
    ) -> Result<
        Result<
            ApplicationOutcome<
                WorkOutcome<
                    Self,
                    Option<Never>,
                    Option<Never>,
                    (),
                    (Root, Spaces, StagingFailure, ApplicationScope),
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
            (Self, TryCurrentError),
        >,
        (Self, Builder, RunError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
    {
        if let Ok(entered_executor) = Handle::try_current() {
            drop(entered_executor);
            return Err((self, builder, RunError::BlockingInEnteredRuntime));
        }
        let application_host = match builder.build() {
            Ok(application_host) => application_host,
            Err(source) => return Err((self, builder, RunError::Runtime(source))),
        };
        Ok(application_host.block_on(self.run::<Actor, StagingFailure, ChildFailures>()))
    }

    /// Await declared composition, caller-local work and the exact joined result.
    ///
    /// This future is cold until polled. Bare original work survives actual staging
    /// refusal; supplied work and completed output occupy their bare actual axes.
    /// Use `execute_with` to retain the result receiver independently of execution.
    ///
    /// # Errors
    /// Returns the untouched declaration and work with the actual entered-host error.
    ///
    /// # Panics
    /// Setup and work may unwind; the actual cause remains with the caller.
    pub async fn run_with<Actor, ChildFailures, StagingFailure, Work, WorkFuture>(
        self,
        work: Work,
    ) -> Result<
        ApplicationOutcome<
            WorkOutcome<
                Self,
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
        (Self, Work, TryCurrentError),
    >
    where
        Spaces: ApplicationTopology<Root, Members>,
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
        let (execution, result) = self.execute_with(work)?;
        execution.await;
        Ok(result.await)
    }
}

impl<Work> ApplicationWorkPresence<Work, Never> {
    pub(in crate::application) fn into_supplied(self) -> Work {
        match self {
            Self::Supplied(work) => work,
            Self::Absent(absence) => match absence {},
        }
    }
}

impl<Inputs, ColdWork, Work, Output, StagingInputs, PreparedInputs>
    ApplicationWorkPublication<Inputs, ColdWork, Work, Output, StagingInputs, PreparedInputs>
{
    // Same existing Unstarted handoff invariant, now owned by its affine publication.
    #[expect(
        clippy::type_complexity,
        reason = "return coexisting original application inputs, untouched callable and its exact affine publication without a forwarding alias"
    )]
    pub(in crate::application) fn into_unstarted(
        mut self,
    ) -> (
        Inputs,
        ColdWork,
        oneshot::Sender<
            ApplicationWorkCustody<Inputs, ColdWork, Work, Output, StagingInputs, PreparedInputs>,
        >,
    ) {
        let Some((ApplicationWorkCustody::Unstarted(application, work), publication)) =
            self.publication.take()
        else {
            unreachable!("execution owns the single original input publication");
        };
        (application, work, publication)
    }

    // Same existing pre-startup NotInvoked handoff; not a new presence assertion.
    #[expect(
        clippy::type_complexity,
        reason = "return the same uninvoked callable and its exact affine publication without an extra product owner"
    )]
    pub(in crate::application) fn into_not_invoked(
        mut self,
    ) -> (
        Work,
        oneshot::Sender<
            ApplicationWorkCustody<Inputs, ColdWork, Work, Output, StagingInputs, PreparedInputs>,
        >,
    ) {
        let Some((ApplicationWorkCustody::NotInvoked(work, None), publication)) =
            self.publication.take()
        else {
            unreachable!("preparation owns the single uninvoked callable publication");
        };
        (work, publication)
    }
}

impl<ApplicationInputs, ColdWork, Work, Output, StagingInputs, PreparedInputs> Drop
    for ApplicationWorkPublication<
        ApplicationInputs,
        ColdWork,
        Work,
        Output,
        StagingInputs,
        PreparedInputs,
    >
{
    fn drop(&mut self) {
        if let Some((custody, publication)) = self.publication.take() {
            match publication.send(custody) {
                Ok(()) => {}
                Err(custody) => drop(custody),
            }
        }
    }
}

type RootCapabilities<Actor, Spaces, Terminal, Origins> = ApplicationCapabilities<
    Actor,
    Spaces,
    NoParent,
    ChildBindings<Actor, Terminal, Origins>,
    Origins,
>;

pub(in crate::application) type RootInterpreter<Actor, Spaces, Terminal, Origins> =
    ActionInterpreter<RootCapabilities<Actor, Spaces, Terminal, Origins>>;

/// Failure while constructing or entering the owned blocking executor.
#[derive(thiserror::Error)]
pub enum RunError {
    /// Blocking execution was requested while a Tokio runtime was entered.
    #[error("blocking application execution is forbidden inside an entered Tokio runtime")]
    BlockingInEnteredRuntime,
    /// Tokio could not construct the application executor.
    #[error("Bombay could not construct its Tokio runtime")]
    Runtime(#[source] io::Error),
}

/// Failure while receiving or joining the actual application cleanup owner.
/// A closed publication does not classify actor existence or completion.
/// The actor task's native failure remains `ActorRetirement::ActorTaskFailed`.
#[derive(Debug, thiserror::Error)]
pub enum ApplicationCleanupError {
    /// The sole original cleanup handle could not be received.
    #[error("application cleanup handle publication closed")]
    PublicationClosed(#[source] RecvError),
    /// The actual cleanup task failed before returning its own result.
    #[error("application cleanup task failed")]
    TaskFailed(#[source] JoinError),
}

pub(in crate::application) enum ApplicationWorkPresence<Work, NoWork> {
    Supplied(Work),
    Absent(NoWork),
}

pub(in crate::application) enum ApplicationWorkCustody<
    ApplicationInputs,
    ColdWork,
    Work,
    Output,
    StagingInputs = Never,
    PreparedInputs = Never,
> {
    StagingRejected(StagingInputs),
    Unstarted(ApplicationInputs, ColdWork),
    Prepared(PreparedInputs, Work),
    NotInvoked(Work, Option<RecvError>),
    Completed(Output),
}

#[expect(
    clippy::type_complexity,
    reason = "one affine publication pairs exact phase custody with its sole original sender"
)]
pub(in crate::application) struct ApplicationWorkPublication<
    ApplicationInputs,
    ColdWork,
    Work,
    Output,
    StagingInputs = Never,
    PreparedInputs = Never,
> {
    pub(in crate::application) publication: Option<(
        ApplicationWorkCustody<
            ApplicationInputs,
            ColdWork,
            Work,
            Output,
            StagingInputs,
            PreparedInputs,
        >,
        oneshot::Sender<
            ApplicationWorkCustody<
                ApplicationInputs,
                ColdWork,
                Work,
                Output,
                StagingInputs,
                PreparedInputs,
            >,
        >,
    )>,
}

#[expect(
    clippy::type_complexity,
    clippy::too_many_lines,
    reason = "one existing affine cold-input/startup/work/permission/join owner preserves exact products at every drop cut"
)]
pub(in crate::application) fn execute_application_with<
    Inputs,
    Owner,
    Actor,
    Spaces,
    Origins,
    Terminal,
    InstalledFamilies,
    StagingInputs,
    AcquireStartupInputs,
    StartupInputs,
    Prepare,
    Work,
    Invoke,
    WorkFuture,
    RetireUnstartedFamilies,
    ColdWork,
    NoWork,
>(
    executor: Handle,
    application: Inputs,
    work: ColdWork,
    acquire_startup_inputs: AcquireStartupInputs,
    prepare: Prepare,
    (family_publications, retire_unstarted_families): (
        InstalledFamilies::ShutdownPublications,
        RetireUnstartedFamilies,
    ),
) -> (
    impl Future<Output = ()>,
    impl Future<
        Output = (
            WorkOutcome<
                Inputs,
                ApplicationWorkPresence<(Work, Invoke), NoWork>,
                ApplicationWorkPresence<WorkFuture::Output, NoWork>,
                (),
                StagingInputs,
                (Actor, Spaces, ApplicationScope),
                ColdWork,
            >,
            Result<
                (
                    RootOrigin<Owner>,
                    ActorRetirement<
                        Actor,
                        Terminal,
                        <ChildBindings<Actor, Terminal, Origins> as RetireChildTasks>::Failures,
                    >,
                ),
                RecvError,
            >,
            Result<ActorNotificationReceipts, RecvError>,
        ),
    >,
)
where
    Actor: BehaviorBase
        + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>
        + Send
        + 'static,
    Actor::Event: Send + 'static,
    BehaviorMessage<Actor>: Send + 'static,
    Actor::Sends: Send + 'static,
    Actor::Error: Send + 'static,
    Actor::InterpretationCustody: Send + 'static,
    Actor::SourceCustody: Send + 'static,
    <Actor::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Origins>> + Send + 'static,
    Spaces: RootRegistration<Actor::Protocol> + Send + Sync + 'static,
    ChildBindings<Actor, Terminal, Origins>:
        ChildBindingsIn<Actor, Spaces> + RetireChildTasks<Root = Terminal> + Send + 'static,
    RootInterpreter<Actor, Spaces, Terminal, Origins>: CommitActions<
            Actor,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Actor, Terminal, Origins> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Actor as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    RootOrigin<Owner>: Send + 'static,
    Terminal: RetirementSchema + Send + 'static,
    <ChildBindings<Actor, Terminal, Origins> as RetireChildTasks>::Failures: Send + 'static,
    AcquireStartupInputs: AsyncFnOnce(&Inputs) -> (StartupInputs, ApplicationAddresses),
    InstalledFamilies: InstalledEntityFamilies + Send + 'static,
    Prepare: FnOnce(
        ApplicationWorkPublication<
            Inputs,
            ColdWork,
            ApplicationWorkPresence<(Work, Invoke), NoWork>,
            ApplicationWorkPresence<<WorkFuture as Future>::Output, NoWork>,
            StagingInputs,
            (Actor, Spaces, ApplicationScope),
        >,
        StartupInputs,
        ApplicationAddresses,
    ) -> Result<
        (
            ApplicationWorkPublication<
                Inputs,
                ColdWork,
                ApplicationWorkPresence<(Work, Invoke), NoWork>,
                ApplicationWorkPresence<<WorkFuture as Future>::Output, NoWork>,
                StagingInputs,
                (Actor, Spaces, ApplicationScope),
            >,
            InstalledFamilies,
        ),
        ApplicationWorkPublication<
            Inputs,
            ColdWork,
            ApplicationWorkPresence<(Work, Invoke), NoWork>,
            ApplicationWorkPresence<<WorkFuture as Future>::Output, NoWork>,
            StagingInputs,
            (Actor, Spaces, ApplicationScope),
        >,
    >,
    Invoke: FnOnce(
        Work,
        ApplicationHandle<Actor::Protocol, Actor::Event, InstalledFamilies::Receptionists>,
    ) -> WorkFuture,
    WorkFuture: Future,
    RetireUnstartedFamilies: FnOnce(
        Handle,
        InstalledFamilies,
        InstalledFamilies::ShutdownPublications,
    ) -> Option<JoinHandle<()>>,
{
    let (publication, work_result) = oneshot::channel();
    let original_work = ApplicationWorkPublication {
        publication: Some((
            ApplicationWorkCustody::Unstarted(application, work),
            publication,
        )),
    };
    let (cleanup_publication, cleanup_result) = oneshot::channel();
    let (root_publication, received_root) = oneshot::channel();
    let (notification_publication, received_notification) = oneshot::channel();
    let execution = async move {
        let mut original_work = original_work;
        let (startup_inputs, allocations) = {
            let Some((ApplicationWorkCustody::Unstarted(application, _), _)) =
                original_work.publication.as_ref()
            else {
                unreachable!("startup acquisition borrows the original cold inputs");
            };
            let mut startup_acquisition = pin!(acquire_startup_inputs(application));
            poll_fn(|context| {
                let entered_executor = executor.enter();
                let acquired = startup_acquisition.as_mut().poll(context);
                drop(entered_executor);
                acquired
            })
            .await
        };
        let (prepared_work, installed_families) =
            match prepare(original_work, startup_inputs, allocations.clone()) {
                Ok(prepared) => prepared,
                Err(rejected) => {
                    drop(rejected);
                    return;
                }
            };
        original_work = prepared_work;
        let setup = catch_unwind(AssertUnwindSafe(|| {
            let Some((ApplicationWorkCustody::Prepared((_, spaces, scope), _), _)) =
                original_work.publication.as_ref()
            else {
                unreachable!("setup borrows the single prepared input publication");
            };
            let roots = <Spaces as RootRegistration<Actor::Protocol>>::root_space(spaces).clone();
            let bindings = ChildBindings::<Actor, Terminal, Origins>::new_in(spaces);
            let receptionists = installed_families.receptionists();
            (roots, bindings, receptionists, scope.client_capacity)
        }));
        let (roots, bindings, receptionists, client_capacity) = match setup {
            Ok(setup) => setup,
            Err(cause) => {
                // The actor is still inside Prepared; no task was handed off at this cut.
                if let Some(cleanup) = retire_unstarted_families(
                    executor.clone(),
                    installed_families,
                    family_publications,
                ) {
                    match cleanup_publication.send(cleanup) {
                        Ok(()) => {}
                        Err(cleanup) => drop(cleanup),
                    }
                }
                resume_unwind(cause);
            }
        };
        let interface_allocations = allocations.clone();
        let (permission, permitted) = oneshot::channel();
        let (authority, startup, shutdown_control, actor_join, termination_notification) = {
            let entered_executor = executor.enter();
            let address = MailAddr::APPLICATION_ROOT;
            let config = communication::Config::new(DEFAULT_USER_CAPACITY);
            let Some((ApplicationWorkCustody::Prepared((root, spaces, scope), work), publication)) =
                original_work.publication.take()
            else {
                unreachable!("handoff consumes the single prepared input publication");
            };
            original_work.publication =
                Some((ApplicationWorkCustody::NotInvoked(work, None), publication));
            drop(scope);
            let actor_spaces = Arc::new(spaces);
            let started = spawn_local_execution::<Actor, _, StandardIngress, _, _, _>(
                roots,
                config,
                address,
                root,
                move |control, terminal_reports, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<
                        Actor,
                        Spaces,
                        NoParent,
                        ChildBindings<Actor, Terminal, Origins>,
                        Origins,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        bindings,
                    ))
                },
                |environment| {
                    let (publication, startup) = oneshot::channel();
                    let control = environment.shutdown_control();
                    let environment =
                        environment.publish_with(move |actor| match publication.send(actor) {
                            Ok(()) => {}
                            Err(actor) => drop(actor),
                        });
                    (environment, startup, control)
                },
            );
            drop(entered_executor);
            started
        };
        let (retirement_publication, retirement_observation) = observe::pair();
        let (joined_publication, joined) = oneshot::channel();
        let origin = RootOrigin::<Owner>::new(MailAddr::APPLICATION_ROOT);
        let cleanup = executor.spawn(async move {
            // Joining observes the actual actor; it does not request a stop.
            // Publish its small report while application work can still await it.
            let joined_actor = actor_join.await;
            let termination_notification = match termination_notification.await {
                Ok(notification) => notification,
                Err(error) => Err(RetirementNotificationError::ReceiptClosed { error }),
            };
            let report =
                ActorRetirementReport::from_joined(&joined_actor, &termination_notification);
            let retirement_notification = publish_retirement_report(retirement_publication, report);
            let notifications = ActorNotificationReceipts {
                termination: termination_notification,
                retirement: retirement_notification,
            };
            // Native handoff and Application-owned families keep their work barrier.
            // All original values remain outside the report publication's unwind.
            match permitted.await {
                Ok(()) | Err(_) => {}
            }
            let retirement = match joined_actor {
                Ok(local) => ActorRetirement::from_local(local),
                Err(error) => ActorRetirement::ActorTaskFailed(error),
            };
            // Receiving first joins this producer. Neither transfer receiver has
            // registered a waker while these original values are handed off.
            let refused_root = root_publication.send((origin, retirement)).err();
            let refused_notification = notification_publication.send(notifications).err();
            installed_families.shutdown(family_publications).await;
            // Abandoning the whole receiving owner surrenders unread originals.
            // Their disposal cannot preempt the already acquired family cleanup.
            drop((refused_root, refused_notification));
            match joined_publication.send(()) {
                Ok(()) | Err(()) => {}
            }
        });
        match cleanup_publication.send(cleanup) {
            Ok(()) => {}
            Err(cleanup) => drop(cleanup),
        }
        let started = startup.await;
        let Some((ApplicationWorkCustody::NotInvoked(work, None), publication)) =
            original_work.publication.take()
        else {
            unreachable!("startup owns the single uninvoked callable publication");
        };
        match started {
            Ok(actor) => {
                match work {
                    ApplicationWorkPresence::Supplied((work, invoke)) => {
                        let application = ApplicationHandle::new(
                            actor,
                            shutdown_control,
                            retirement_observation,
                            interface_allocations,
                            client_capacity,
                            receptionists,
                        );
                        {
                            let mut application_work = pin!(invoke(work, application));
                            let output =
                                poll_fn(|context| application_work.as_mut().poll(context)).await;
                            original_work.publication = Some((
                                ApplicationWorkCustody::Completed(
                                    ApplicationWorkPresence::Supplied(output),
                                ),
                                publication,
                            ));
                        }
                    }
                    ApplicationWorkPresence::Absent(absence) => {
                        // Discharge the genuine unused startup projections in their existing field order.
                        original_work.publication = Some((
                            ApplicationWorkCustody::Completed(ApplicationWorkPresence::Absent(
                                absence,
                            )),
                            publication,
                        ));
                        drop((
                            actor,
                            shutdown_control,
                            interface_allocations,
                            receptionists,
                        ));
                    }
                }
                drop(original_work);
            }
            Err(error) => {
                match publication.send(ApplicationWorkCustody::NotInvoked(work, Some(error))) {
                    Ok(()) => {}
                    Err(work) => drop(work),
                }
            }
        }
        match permission.send(()) {
            Ok(()) | Err(()) => {}
        }
        match joined.await {
            Ok(()) | Err(_) => {}
        }
        drop(authority);
    };
    let result = async move {
        let custody = work_result.await;
        let outcome = match custody {
            Ok(ApplicationWorkCustody::Unstarted(application, work)) => {
                WorkOutcome::Unstarted { application, work }
            }
            Ok(ApplicationWorkCustody::StagingRejected(inputs)) => {
                WorkOutcome::StagingRejected { inputs }
            }
            custody => {
                // Publication failure does not classify actor or task existence.
                // Retain its actual error alongside the acquired work custody.
                let cleanup = match cleanup_result.await {
                    Ok(cleanup) => match cleanup.await {
                        Ok(cleanup) => Ok(cleanup),
                        Err(error) => Err(ApplicationCleanupError::TaskFailed(error)),
                    },
                    Err(error) => Err(ApplicationCleanupError::PublicationClosed(error)),
                };
                match custody {
                    Ok(ApplicationWorkCustody::Prepared(inputs, work)) => WorkOutcome::Prepared {
                        inputs,
                        work,
                        cleanup,
                    },
                    Ok(ApplicationWorkCustody::NotInvoked(work, startup_error)) => {
                        WorkOutcome::NotInvoked {
                            work,
                            startup_error,
                            cleanup,
                        }
                    }
                    Ok(ApplicationWorkCustody::Completed(output)) => {
                        WorkOutcome::Completed { output, cleanup }
                    }
                    Err(_) => WorkOutcome::Interrupted { cleanup },
                    Ok(ApplicationWorkCustody::StagingRejected(_)) => {
                        unreachable!("the cold staging rejection branch already returned")
                    }
                    Ok(ApplicationWorkCustody::Unstarted(_, _)) => {
                        unreachable!("the original untouched input branch already returned")
                    }
                }
            }
        };
        let root = received_root.await;
        let termination_notification = received_notification.await;
        (outcome, root, termination_notification)
    };
    (execution, result)
}
