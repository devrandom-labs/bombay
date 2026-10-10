//! Application-owned preparation of workers requested by Behavior Actors.

use core::future::Future;
use core::ops::ControlFlow;

use behavior::{Behavior, Never};
use behavior_actors::atomic::{
    ActivationPlan, StartingWorkerPreparation, WorkerPreparation, WorkerSource, WorkerSubmission,
};

/// The first role's preparation can reject the complete source before a
/// worker attempt begins. Later roles have already admitted that source.
#[must_use = "the first worker preparation must settle its source and role"]
pub enum WorkerPreparationStart<Worker, Plan, WorkerRejection, SourceRejection>
where
    Worker: Behavior + Send,
    Plan: ActivationPlan,
{
    /// The source supplied the requested worker and activation plan.
    Submitted(WorkerSubmission<Worker, Plan>),
    /// The source admitted the request but rejected this role.
    WorkerRejected(WorkerRejection),
    /// The source rejected the complete request before any role was prepared.
    SourceRejected(SourceRejection),
}

/// Concrete application operation for an affine Behavior Actors worker source.
///
/// `WorkerSource` declares the types retained by the pure supervisor or pool.
/// This port performs preparation only when Bombay interprets its emitted
/// `PrepareWorkers` action. The first attempt may reject the source; after a
/// successful first admission, later attempts can reject only their own role.
/// No method is called inside a Behavior transition.
pub trait WorkerPreparationSource<Role, Worker, Plan>: WorkerSource<Role, Worker, Plan>
where
    Role: Sync,
    Worker: Behavior + Send,
    Plan: ActivationPlan,
{
    /// Prepare the first role or reject the entire, still-owned source.
    fn prepare_first(
        &mut self,
        role: &Role,
    ) -> impl Future<
        Output = WorkerPreparationStart<Worker, Plan, Self::WorkerRejection, Self::SourceRejection>,
    > + Send;

    /// Prepare a later role after the source has been admitted.
    fn prepare_next(
        &mut self,
        role: &Role,
    ) -> impl Future<Output = Result<WorkerSubmission<Worker, Plan>, Self::WorkerRejection>> + Send;
}

impl<Role, Worker, Plan> WorkerPreparationSource<Role, Worker, Plan> for Never
where
    Role: Sync,
    Worker: Behavior + Send,
    Plan: ActivationPlan,
{
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_first(
        &mut self,
        _: &Role,
    ) -> WorkerPreparationStart<Worker, Plan, Never, Never> {
        match *self {}
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_next(&mut self, _: &Role) -> Result<WorkerSubmission<Worker, Plan>, Never> {
        match *self {}
    }
}

pub(crate) async fn settle_worker_preparation<Source, Role, Worker, Plan>(
    mut request: StartingWorkerPreparation<Source, Role, Worker, Plan>,
) -> WorkerPreparation<Source, Role, Worker, Plan>
where
    Source: WorkerPreparationSource<Role, Worker, Plan>,
    Role: Send + Sync,
    Worker: Behavior + Send,
    Plan: ActivationPlan,
{
    let (source, role) = request.source_and_role();
    let first = source.prepare_first(role).await;
    let mut preparation = match first {
        WorkerPreparationStart::Submitted(submission) => request.accept(submission),
        WorkerPreparationStart::WorkerRejected(reason) => {
            return request.reject(reason);
        }
        WorkerPreparationStart::SourceRejected(reason) => {
            return request.reject_source(reason);
        }
    };
    loop {
        preparation = match preparation {
            ControlFlow::Break(prepared) => return prepared,
            ControlFlow::Continue(mut pending) => {
                let (source, role) = pending.source_and_role();
                match source.prepare_next(role).await {
                    Ok(submission) => pending.accept(submission),
                    Err(reason) => return pending.reject(reason),
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::{Future, poll_fn};
    use std::mem;
    use std::ops::ControlFlow;
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    use std::pin::pin;
    use std::ptr;
    use std::sync::{Weak, mpsc};
    use std::task::Poll;

    use behavior::{
        ActionItem, Here, InterpretItem, InterpretSends, Interpretation, InterpretationProgress,
        SendEffects, SourceActions,
    };
    use behavior_actors::atomic::{DiagnosticAction, StartingWorkerPreparation, WorkerPreparation};
    use tokio::runtime::Builder;
    use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore, TryAcquireError};
    use tokio::task::{AbortHandle, JoinError, JoinHandle, spawn_blocking};

    use crate::launch::{OwnedTask, spawn_owned_with};
    use crate::local::effects::{ActionSettlementOf, RetireCapabilities};
    use crate::local::effects::{CapabilityRetirement, CommitActions};
    use crate::local::environment::LocalResidual;
    use crate::local::execution::{ActivationTasks, LocalRetirementRequest, OwnerCancellation};
    use crate::{ActorExecutionOutcome, ActorSpace, MailAddr};
    use behavior::{
        ActionItemResult, BehaviorSettlements, EventIngress, InitializationTurn, MessageProtocol,
        SourceAdmission, SourceProgress, SourceSettlementCustody, UserEvent,
    };
    use behavior_actors::Crash;
    use behavior_actors::atomic::WorkerPreparationStarted;
    use bombay_engine::{ActionsOf, Completion as EngineCompletion};
    use communication::{Config, ControlClosed, ControlSender};
    use std::convert::Infallible;
    use std::marker::PhantomData;
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    use behavior::{
        Actions, ActiveTurn, Address, Behavior, BehaviorActed, BehaviorBase, ChildCreationOutcome,
        ChildHead, CommittedChild, CreationSettlement, CreationsSettled, EndpointAddress,
        EstablishedActor, InterpreterRequests, ItemSettlement, Never, NoBirths, Protocol,
        ReportToParent, SettledItem, Step, User,
    };
    use behavior_actors::atomic::{
        ActivationPolicy, ActorDrainPolicy, Assignment, BacklogCapacity, Completion,
        DiagnosticDisposition, FifoCommand, FifoEvent, FifoPool, ImmediateActivation, Interruption,
        OrderedRoles, PoolFailureReaction, PoolRecovery, PrepareWorkers, RestartLimit,
        RestartRelease, WorkerInitializationOutcome, WorkerSource, WorkerSubmission, fifo,
    };
    use behavior_actors::{Activate as _, Active, ChildStopped, Exit, StopOnShutdown};
    use tokio::sync::oneshot;

    use crate::worker_preparation::{
        WorkerPreparationSource, WorkerPreparationStart, settle_worker_preparation,
    };

    macro_rules! start_preparation {
        ($pool:expr, $request:expr) => {{
            let (receipt, starting) = $request.start();
            let started = $pool
                .transition(FifoEvent::WorkerPreparationStarted(SettledItem::Attempted(
                    ItemSettlement::Accepted(receipt),
                )))
                .expect("the exact preparation start is accepted");
            assert!(started.creates.is_empty());
            starting
        }};
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PoolMailAddr(u64);

    impl Address for PoolMailAddr {
        type Nonce = u64;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PoolEndpoint(u64);

    struct PoolInstalled<B: Behavior>(PoolEndpoint, PhantomData<fn() -> B>);

    impl<B: Behavior> Clone for PoolInstalled<B> {
        fn clone(&self) -> Self {
            Self(self.0, PhantomData)
        }
    }

    impl EndpointAddress for PoolMailAddr {
        type Established<P>
            = PoolEndpoint
        where
            P: Protocol<Addr = Self>;

        type Installed<B>
            = PoolInstalled<B>
        where
            B: Behavior<Protocol: Protocol<Addr = Self>>;

        fn recipient<B>(installed: &Self::Installed<B>) -> Self::Established<B::Protocol>
        where
            B: Behavior<Protocol: Protocol<Addr = Self>>,
        {
            installed.0
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SearchRole {
        Primary,
    }

    struct SearchWorker;

    impl Protocol for SearchWorker {
        type Addr = PoolMailAddr;
        type Msg = Assignment<u8>;
    }

    impl BehaviorBase for SearchWorker {
        type Base = Self;

        fn base(&self) -> &Self::Base {
            self
        }
    }

    impl Behavior for SearchWorker {
        type Protocol = Self;
        type Event = User<PoolMailAddr, Assignment<u8>>;
        type Sends = InterpreterRequests<ReportToParent<Completion<u16>>>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, assignment: Self::Event) -> BehaviorActed<Self> {
            let result = u16::from(*assignment.message.payload());
            Ok(Actions::cont().with_send(assignment.message.complete(result)))
        }
    }

    // Only this closed source selection crosses the pure FIFO fold boundary.
    enum WorkshopSelection {
        Submit,
        RejectWorker,
        RejectSource,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkerRejection {
        Unavailable,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkshopRejection {
        Closed,
    }

    impl WorkerSource<SearchRole, SearchWorker, ImmediateActivation> for WorkshopSelection {
        type WorkerRejection = WorkerRejection;
        type SourceRejection = WorkshopRejection;
    }

    impl WorkerPreparationSource<SearchRole, SearchWorker, ImmediateActivation> for WorkshopSelection {
        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn prepare_first(
            &mut self,
            _: &SearchRole,
        ) -> WorkerPreparationStart<
            SearchWorker,
            ImmediateActivation,
            WorkerRejection,
            WorkshopRejection,
        > {
            match self {
                WorkshopSelection::Submit => {
                    WorkerPreparationStart::Submitted(WorkerSubmission::immediate(SearchWorker))
                }
                WorkshopSelection::RejectWorker => {
                    WorkerPreparationStart::WorkerRejected(WorkerRejection::Unavailable)
                }
                WorkshopSelection::RejectSource => {
                    WorkerPreparationStart::SourceRejected(WorkshopRejection::Closed)
                }
            }
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn prepare_next(
            &mut self,
            _: &SearchRole,
        ) -> Result<WorkerSubmission<SearchWorker, ImmediateActivation>, WorkerRejection> {
            Ok(WorkerSubmission::immediate(SearchWorker))
        }
    }

    // Runtime preparation resources stay with this outside operation owner.
    // The FIFO stores WorkshopSelection and its actual typed preparation ticket.
    struct SearchWorkshop {
        roles: Arc<Mutex<Vec<SearchRole>>>,
        release: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    }

    impl SearchWorkshop {
        async fn prepare(
            &mut self,
            mut starting: StartingWorkerPreparation<
                WorkshopSelection,
                SearchRole,
                SearchWorker,
                ImmediateActivation,
            >,
        ) -> WorkerPreparation<WorkshopSelection, SearchRole, SearchWorker, ImmediateActivation>
        {
            let (_, role) = starting.source_and_role();
            self.roles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(*role);
            if let Some((started, release)) = self.release.take() {
                started
                    .send(())
                    .unwrap_or_else(|()| panic!("the supervisor awaits preparation"));
                release.await.expect("the test releases worker preparation");
            }
            settle_worker_preparation(starting).await
        }
    }

    type SearchPool<Source = WorkshopSelection> =
        FifoPool<SearchRole, SearchWorker, ImmediateActivation, Source, Infallible, u8, u16>;

    #[expect(
        clippy::too_many_lines,
        clippy::drop_non_drop,
        reason = "Keep the complete pure FIFO initialization trace and explicit unused worker-definition discharge together."
    )]
    async fn replacement_request<Source>(
        source: Source,
    ) -> (
        Active<SearchPool<Source>>,
        PrepareWorkers<Source, SearchRole, SearchWorker, ImmediateActivation>,
    )
    where
        Source: WorkerSource<SearchRole, SearchWorker, ImmediateActivation>,
    {
        let roles =
            OrderedRoles::new(SearchRole::Primary, []).expect("one role is a valid worker roster");
        let pool = fifo(
            |_: &SearchRole| Ok::<_, Never>(WorkerSubmission::immediate(SearchWorker)),
            roles,
            ActivationPolicy::new(1).expect("one activation is valid capacity"),
            PoolRecovery::permanent(
                source,
                RestartLimit::new(1, Duration::from_secs(30)),
                RestartRelease::immediate(),
                PoolFailureReaction::RetireRole,
            ),
            BacklogCapacity::new(0),
            Interruption::Fail,
            ActorDrainPolicy::WaitForActorGraph,
            DiagnosticDisposition::<Infallible>::terminate(),
        )
        .unwrap_or_else(|_| panic!("the worker declaration constructs one pool"));
        let initialized = pool
            .initialize()
            .unwrap_or_else(|_| panic!("the pool initializes"));
        let mut pool = initialized.behavior;
        assert!(initialized.actions.sends.worker_initializations.is_empty());
        assert!(initialized.actions.sends.worker_activations.is_empty());
        assert!(
            initialized
                .actions
                .sends
                .customer_outcomes
                .as_slice()
                .is_empty()
        );
        assert!(initialized.actions.sends.worker_assignments.is_empty());
        assert!(initialized.actions.sends.worker_preparations.is_empty());
        assert!(initialized.actions.sends.restart_schedules.is_empty());
        assert!(initialized.actions.sends.worker_shutdowns.is_empty());
        assert!(initialized.actions.sends.diagnostics.is_empty());
        let mut observations = initialized
            .actions
            .sends
            .worker_observations
            .into_requests();
        assert_eq!(observations.len(), 1);
        let observation = observations.pop().expect("one initial worker observation");
        assert_eq!(initialized.actions.creates.len(), 1);
        assert!(matches!(initialized.actions.become_, Step::Continue));
        let creation = initialized
            .actions
            .creates
            .into_iter()
            .next()
            .expect("initialization creates one direct worker");
        let (worker, definition, kind) = creation.into_parts();
        assert_eq!(observation.child, worker);
        // This pure FIFO setup simulates establishment; it does not run a child actor.
        drop(definition);
        let established = CommittedChild::new(
            worker,
            kind,
            EstablishedActor::issued(PoolInstalled(PoolEndpoint(1), PhantomData)),
        );
        let settled = CreationsSettled::new(CreationSettlement::Settled(
            [SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::<StopOnShutdown<SearchWorker>, ChildHead>::Established(
                    established,
                ),
            ))]
            .into_iter()
            .collect(),
        ));
        let committed = pool
            .on(settled)
            .unwrap_or_else(|_| panic!("the direct worker is established"));
        assert!(committed.sends.worker_observations.is_empty());
        assert!(committed.sends.worker_activations.is_empty());
        assert!(committed.sends.customer_outcomes.as_slice().is_empty());
        assert!(committed.sends.worker_assignments.is_empty());
        assert!(committed.sends.worker_preparations.is_empty());
        assert!(committed.sends.restart_schedules.is_empty());
        assert!(committed.sends.worker_shutdowns.is_empty());
        assert!(committed.sends.diagnostics.is_empty());
        assert!(committed.creates.is_empty());
        assert!(matches!(committed.become_, Step::Continue));
        let mut initializations = committed.sends.worker_initializations.into_requests();
        assert_eq!(initializations.len(), 1);
        let initialization = initializations
            .pop()
            .expect("the new worker awaits initialization");
        let worker = initialization.worker().creation();
        let initialized_worker = pool
            .on(initialization.resolve(WorkerInitializationOutcome::ReadyForActivation))
            .unwrap_or_else(|_| panic!("the worker initializes"));
        assert!(initialized_worker.sends.worker_observations.is_empty());
        assert!(initialized_worker.sends.worker_initializations.is_empty());
        assert!(
            initialized_worker
                .sends
                .customer_outcomes
                .as_slice()
                .is_empty()
        );
        assert!(initialized_worker.sends.worker_assignments.is_empty());
        assert!(initialized_worker.sends.worker_preparations.is_empty());
        assert!(initialized_worker.sends.restart_schedules.is_empty());
        assert!(initialized_worker.sends.worker_shutdowns.is_empty());
        assert!(initialized_worker.sends.diagnostics.is_empty());
        assert!(initialized_worker.creates.is_empty());
        assert!(matches!(initialized_worker.become_, Step::Continue));
        let mut activations = initialized_worker.sends.worker_activations.into_requests();
        assert_eq!(activations.len(), 1);
        let activation = activations
            .pop()
            .expect("the initialized worker begins activation");
        let started = activation.started();
        let activated = pool
            .on(started)
            .unwrap_or_else(|_| panic!("the activation start is admitted"));
        assert!(activated.sends.worker_observations.is_empty());
        assert!(activated.sends.worker_initializations.is_empty());
        assert!(activated.sends.worker_activations.is_empty());
        assert!(activated.sends.customer_outcomes.as_slice().is_empty());
        assert!(activated.sends.worker_assignments.is_empty());
        assert!(activated.sends.worker_preparations.is_empty());
        assert!(activated.sends.restart_schedules.is_empty());
        assert!(activated.sends.worker_shutdowns.is_empty());
        assert!(activated.sends.diagnostics.is_empty());
        assert!(activated.creates.is_empty());
        assert!(matches!(activated.become_, Step::Continue));
        let ready = activation.activate().await;
        let activated = pool
            .on(ready)
            .unwrap_or_else(|_| panic!("the worker becomes ready"));
        assert!(activated.sends.worker_observations.is_empty());
        assert!(activated.sends.worker_initializations.is_empty());
        assert!(activated.sends.worker_activations.is_empty());
        assert!(activated.sends.customer_outcomes.as_slice().is_empty());
        assert!(activated.sends.worker_assignments.is_empty());
        assert!(activated.sends.worker_preparations.is_empty());
        assert!(activated.sends.restart_schedules.is_empty());
        assert!(activated.sends.worker_shutdowns.is_empty());
        assert!(activated.sends.diagnostics.is_empty());
        assert!(activated.creates.is_empty());
        assert!(matches!(activated.become_, Step::Continue));
        let stopped = pool
            .on(ChildStopped::new(worker, Ok(Exit::Normal), Instant::now()))
            .unwrap_or_else(|_| panic!("the worker stop starts replacement preparation"));
        assert!(stopped.sends.worker_observations.is_empty());
        assert!(stopped.sends.worker_initializations.is_empty());
        assert!(stopped.sends.worker_activations.is_empty());
        assert!(stopped.sends.customer_outcomes.as_slice().is_empty());
        assert!(stopped.sends.worker_assignments.is_empty());
        assert!(stopped.sends.restart_schedules.is_empty());
        assert!(stopped.sends.worker_shutdowns.is_empty());
        assert!(stopped.sends.diagnostics.is_empty());
        assert!(stopped.creates.is_empty());
        assert!(matches!(stopped.become_, Step::Continue));
        let mut preparations = stopped.sends.worker_preparations.into_items();
        assert_eq!(preparations.len(), 1);
        let request = preparations
            .pop()
            .expect("permanent recovery emits worker preparation");
        (pool, request)
    }

    #[tokio::test]
    async fn accepted_source_prepares_the_exact_replacement_role() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let mut workshop = SearchWorkshop {
            roles: Arc::clone(&roles),
            release: None,
        };
        let (mut pool, request) = replacement_request(WorkshopSelection::Submit).await;
        let starting = start_preparation!(pool, request);
        let preparation = workshop.prepare(starting).await;
        assert_eq!(
            *roles.lock().unwrap_or_else(PoisonError::into_inner),
            vec![SearchRole::Primary]
        );
        let resumed = pool
            .transition(FifoEvent::WorkerPreparationReturned(preparation))
            .expect("the pool accepts its own preparation result");
        assert_eq!(resumed.creates.len(), 1);
    }

    #[tokio::test]
    async fn source_rejection_returns_the_complete_request() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let mut workshop = SearchWorkshop {
            roles: Arc::clone(&roles),
            release: None,
        };
        let (mut pool, request) = replacement_request(WorkshopSelection::RejectSource).await;
        let starting = start_preparation!(pool, request);
        let preparation = workshop.prepare(starting).await;
        assert_eq!(
            *roles.lock().unwrap_or_else(PoisonError::into_inner),
            vec![SearchRole::Primary]
        );
        let returned = pool
            .transition(FifoEvent::WorkerPreparationReturned(preparation))
            .expect("the pool receives the exact source rejection");
        assert!(returned.creates.is_empty());
        assert!(returned.sends.worker_preparations.is_empty());
        assert_eq!(returned.sends.diagnostics.len(), 1);
    }

    #[tokio::test]
    async fn rejected_worker_preserves_the_source_for_pool_recovery() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let mut workshop = SearchWorkshop {
            roles: Arc::clone(&roles),
            release: None,
        };
        let (mut pool, request) = replacement_request(WorkshopSelection::RejectWorker).await;
        let starting = start_preparation!(pool, request);
        let preparation = workshop.prepare(starting).await;
        assert_eq!(
            *roles.lock().unwrap_or_else(PoisonError::into_inner),
            vec![SearchRole::Primary]
        );
        let resumed = pool
            .transition(FifoEvent::WorkerPreparationReturned(preparation))
            .expect("the pool receives the exact worker rejection");
        assert!(resumed.creates.is_empty());
        assert!(resumed.sends.worker_preparations.is_empty());
    }

    #[tokio::test]
    async fn unattempted_preparation_returns_without_calling_the_source() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let workshop = SearchWorkshop {
            roles: Arc::clone(&roles),
            release: None,
        };
        let (mut pool, mut request) = replacement_request(WorkshopSelection::Submit).await;
        let (source, role) = request.source_and_role();
        assert_eq!(*role, SearchRole::Primary);
        assert!(matches!(source, WorkshopSelection::Submit));
        assert!(Arc::ptr_eq(&workshop.roles, &roles));

        let shutdown = pool
            .receive(PoolMailAddr(7), FifoCommand::shutdown())
            .expect("shutdown awaits the outstanding preparation");
        assert!(matches!(shutdown.become_, Step::Continue));
        assert!(shutdown.creates.is_empty());
        let returned = pool
            .transition(FifoEvent::WorkerPreparationStarted(
                SettledItem::Unattempted(request),
            ))
            .expect("the exact unattempted source settles shutdown");
        assert!(matches!(returned.become_, Step::Stop(_)));
        assert!(returned.creates.is_empty());
        assert!(returned.sends.worker_preparations.is_empty());
        assert!(
            roles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn shutdown_during_preparation_retains_the_return_without_restarting() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let (started, preparation_started) = oneshot::channel();
        let (release_preparation, release) = oneshot::channel();
        let mut workshop = SearchWorkshop {
            roles: Arc::clone(&roles),
            release: Some((started, release)),
        };
        let (mut pool, request) = replacement_request(WorkshopSelection::Submit).await;
        let starting = start_preparation!(pool, request);
        let preparation = tokio::spawn(async move { workshop.prepare(starting).await });
        preparation_started
            .await
            .expect("the source begins its first worker preparation");

        let shutdown = pool
            .receive(PoolMailAddr(7), FifoCommand::shutdown())
            .expect("shutdown awaits the in-flight source action");
        assert!(matches!(shutdown.become_, Step::Continue));
        assert!(shutdown.creates.is_empty());
        release_preparation
            .send(())
            .unwrap_or_else(|()| panic!("the preparation still awaits release"));
        let prepared = preparation.await.expect("the source task completes");
        assert_eq!(
            *roles.lock().unwrap_or_else(PoisonError::into_inner),
            vec![SearchRole::Primary]
        );
        let returned = pool
            .transition(FifoEvent::WorkerPreparationReturned(prepared))
            .expect("the pool receives its late preparation result");
        assert!(matches!(returned.become_, Step::Stop(_)));
        assert!(returned.creates.is_empty());
        assert!(returned.sends.worker_preparations.is_empty());
    }

    // Pure request input. Admission, threads and gates belong only to the host below.
    struct IndexSource {
        original: Option<Arc<Vec<u64>>>,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum IndexRejection {
        Saturated(Arc<Vec<u64>>),
        Closed(Arc<Vec<u64>>),
    }

    #[derive(Clone, Copy)]
    enum IndexAdmission {
        Saturated,
        Closed,
    }

    impl WorkerSource<SearchRole, SearchWorker, ImmediateActivation> for IndexSource {
        type WorkerRejection = Never;
        type SourceRejection = IndexRejection;
    }

    enum IndexOperation {
        Asynchronous(oneshot::Receiver<()>),
        Blocking {
            started: oneshot::Sender<()>,
            submitted: oneshot::Sender<tokio::task::Id>,
            release: mpsc::Receiver<()>,
        },
    }

    enum IndexContinuation {
        Complete,
        NativeFailure(Arc<String>),
        ObserveAbort(oneshot::Sender<AbortHandle>),
    }

    #[derive(Debug, Eq, PartialEq)]
    enum IndexCleanup {
        Waiting,
        Returned,
        NativeFailure,
    }

    enum IndexPreparation {
        Blocking {
            starting: StartingWorkerPreparation<
                IndexSource,
                SearchRole,
                SearchWorker,
                ImmediateActivation,
            >,
            task: JoinHandle<(Arc<Vec<u64>>, Vec<u64>)>,
        },
        Asynchronous {
            starting: StartingWorkerPreparation<
                IndexSource,
                SearchRole,
                SearchWorker,
                ImmediateActivation,
            >,
            permit: OwnedSemaphorePermit,
            release: oneshot::Receiver<()>,
        },
        Completed {
            starting: StartingWorkerPreparation<
                IndexSource,
                SearchRole,
                SearchWorker,
                ImmediateActivation,
            >,
            received: Result<(Arc<Vec<u64>>, Vec<u64>), JoinError>,
        },
    }

    impl IndexPreparation {
        async fn receive(preparation: &mut Option<Self>) {
            let received = match preparation.as_mut() {
                Some(Self::Blocking { task, .. }) => task.await,
                Some(Self::Asynchronous {
                    starting, release, ..
                }) => {
                    release
                        .await
                        .expect("outside owner releases direct async work");
                    let (source, role) = starting.source_and_role();
                    assert_eq!(*role, SearchRole::Primary);
                    let original = source
                        .original
                        .take()
                        .expect("direct async input remains in its original row until completion");
                    let mut sorted = (*original).clone();
                    sorted.sort_unstable();
                    Ok((original, sorted))
                }
                Some(Self::Completed { .. }) | None => return,
            };
            // The actual reply is acquired before disposing its concrete original join/gate.
            // No user-defined future or destructor is called by this exact closed fixture.
            let original = preparation
                .take()
                .expect("the borrowed original remains available until reply acquisition");
            let starting = match original {
                Self::Blocking { starting, task } => {
                    drop(task);
                    starting
                }
                Self::Asynchronous {
                    starting,
                    permit,
                    release,
                } => {
                    drop((release, permit));
                    starting
                }
                Self::Completed { starting, received } => {
                    drop((starting, received));
                    panic!("an already completed row cannot produce another reply");
                }
            };
            *preparation = Some(Self::Completed { starting, received });
        }
    }

    struct IndexCapabilities {
        admission: Arc<Semaphore>,
        operations: VecDeque<(IndexOperation, IndexContinuation)>,
        rows: Vec<Arc<AsyncMutex<Option<IndexPreparation>>>>,
        rejected:
            Vec<WorkerPreparation<IndexSource, SearchRole, SearchWorker, ImmediateActivation>>,
        tasks: Option<ActivationTasks<Never>>,
    }

    impl
        InterpretItem<
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            Never,
            Here,
        > for IndexCapabilities
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "Keep the selected effect port's explicit input/reply loans and future lifetime visible."
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation> as ActionItem>::Input<'a>,
            received: &'a mut Option<<PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(request) = input.take() else {
                    return;
                };
                let (receipt, mut starting) = request.start();
                let permit = match Arc::clone(&self.admission).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(reason) => {
                        let (source, _) = starting.source_and_role();
                        let original = source
                            .original
                            .take()
                            .expect("rejected source owns its original input");
                        let reason = match reason {
                            TryAcquireError::NoPermits => IndexRejection::Saturated(original),
                            TryAcquireError::Closed => IndexRejection::Closed(original),
                        };
                        self.rejected.push(starting.reject_source(reason));
                        *received = Some(ItemSettlement::Accepted(receipt));
                        return;
                    }
                };
                let (operation, continuation) = self
                    .operations
                    .pop_front()
                    .expect("one explicit admitted operation");
                let row = match operation {
                    IndexOperation::Blocking {
                        started,
                        submitted,
                        release,
                    } => {
                        let (source, role) = starting.source_and_role();
                        assert_eq!(*role, SearchRole::Primary);
                        let original = source
                            .original
                            .take()
                            .expect("one original index per admitted request");
                        let blocking = spawn_blocking(move || {
                            started
                                .send(())
                                .expect("blocking start has an outside receiver");
                            release
                                .recv()
                                .expect("outside owner releases noncooperative work");
                            let mut sorted = (*original).clone();
                            sorted.sort_unstable();
                            drop(permit);
                            (original, sorted)
                        });
                        let blocking_id = blocking.id();
                        let row = Arc::new(AsyncMutex::new(Some(IndexPreparation::Blocking {
                            starting,
                            task: blocking,
                        })));
                        // The host owns the actual join before even the submission signal.
                        self.rows.push(Arc::clone(&row));
                        submitted
                            .send(blocking_id)
                            .expect("outside owner observes actual blocking task ID");
                        row
                    }
                    IndexOperation::Asynchronous(release) => {
                        let row = Arc::new(AsyncMutex::new(Some(IndexPreparation::Asynchronous {
                            starting,
                            permit,
                            release,
                        })));
                        self.rows.push(Arc::clone(&row));
                        row
                    }
                };
                self.tasks.as_mut().expect("the original capability tasks are live").spawn(async move {
                    match continuation {
                        IndexContinuation::Complete => {}
                        IndexContinuation::NativeFailure(original_cause) => panic_any(original_cause),
                        IndexContinuation::ObserveAbort(observation) => {
                            let retained = row.lock().await;
                            let Some(IndexPreparation::Blocking { task, .. }) = retained.as_ref() else {
                                panic!("the selected abort observation belongs to actual blocking work");
                            };
                            let abort = task.abort_handle();
                            drop(retained);
                            observation.send(abort).expect("the outside observer retains native abort permission");
                        }
                    }
                    let mut retained = row.lock().await;
                    IndexPreparation::receive(&mut retained).await;
                    Ok(())
                });
                *received = Some(ItemSettlement::Accepted(receipt));
            }
        }
    }

    impl RetireCapabilities for IndexCapabilities {
        type Event = Never;
        type Descendants = (
            Vec<(
                WorkerPreparation<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
                Result<(Arc<Vec<u64>>, Vec<u64>), JoinError>,
            )>,
            Vec<WorkerPreparation<IndexSource, SearchRole, SearchWorker, ImmediateActivation>>,
            Vec<(IndexOperation, IndexContinuation)>,
        );

        async fn next_local_event(&mut self) -> Result<Never, JoinError> {
            self.tasks
                .as_mut()
                .expect("the same live capability task owner remains")
                .next_event()
                .await
        }

        fn next_deadline(&mut self) -> Option<Instant> {
            None
        }
        fn pop_due(&mut self, _: Instant) -> Option<Never> {
            None
        }

        async fn receive_retirement(
            capabilities: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
        ) {
            let Some(owner) = capabilities.as_mut() else {
                return;
            };
            owner.admission.close();
            match (&owner.tasks, received.as_ref()) {
                (Some(_), Some(_)) | (None, None) => return,
                (None, Some(_)) => {}
                (Some(_), None) => {
                    let tasks = owner
                        .tasks
                        .take()
                        .expect("the original task owner is present");
                    *received = Some(CapabilityRetirement {
                        activation_tasks: tasks,
                        descendants: (
                            Vec::new(),
                            mem::take(&mut owner.rejected),
                            mem::take(&mut owner.operations).into(),
                        ),
                        terminal_report: None,
                        retirement_failures: Vec::new(),
                    });
                }
            }
            let retirement = received
                .as_mut()
                .expect("original partial retirement lanes are outside");
            while let Some(row) = owner.rows.first() {
                let mut retained = row.lock().await;
                IndexPreparation::receive(&mut retained).await;
                let Some(IndexPreparation::Completed { .. }) = retained.as_ref() else {
                    return;
                };
                let original = retained.take().expect("one genuine completed original row");
                let IndexPreparation::Completed {
                    starting,
                    received: result,
                } = original
                else {
                    unreachable!("the complete original row was observed");
                };
                // This existing closed fixture declares one role and a unit worker.
                // No arbitrary user callback or destructor occurs in this transfer.
                let returned = match starting.accept(WorkerSubmission::immediate(SearchWorker)) {
                    ControlFlow::Break(returned) => returned,
                    ControlFlow::Continue(pending) => {
                        drop(pending);
                        panic!("the existing fixture declares exactly one role");
                    }
                };
                retirement.descendants.0.push((returned, result));
                drop(retained);
                let acquired_row = owner.rows.remove(0);
                drop(acquired_row);
            }
            let exhausted = capabilities.take();
            drop(exhausted);
        }

        async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
            let mut capabilities = Some(self);
            let mut received = None;
            Self::receive_retirement(&mut capabilities, &mut received).await;
            assert!(
                capabilities.is_none(),
                "incomplete work retains its original capabilities"
            );
            received.expect("completed work acquires all original retirement lanes")
        }
    }

    fn index_lanes(sends: &<SearchPool<IndexSource> as Behavior>::Sends) {
        assert!(sends.worker_observations.is_empty());
        assert!(sends.worker_initializations.is_empty());
        assert!(sends.worker_activations.is_empty());
        assert!(sends.customer_outcomes.as_slice().is_empty());
        assert!(sends.worker_assignments.is_empty());
        assert!(sends.worker_preparations.is_empty());
        assert!(sends.restart_schedules.is_empty());
        assert!(sends.worker_shutdowns.is_empty());
        assert!(sends.diagnostics.is_empty());
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "One finite transaction observes typed admission, queued/running work, both real joins, complete FIFO effects and final original discharges."
    )]
    fn bounded_index_retirement_joins_after_native_producer_failure() {
        let runtime = Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("one blocking worker");
        let observed = runtime.block_on(async {
            let admission = Arc::new(Semaphore::new(2));
            let mut host = IndexCapabilities {
                admission: Arc::clone(&admission),
                operations: VecDeque::new(),
                rows: Vec::new(),
                rejected: Vec::new(),
                tasks: Some(ActivationTasks::new()),
            };
            let mut sends = SourceActions::empty();
            let mut pools = Vec::new();
            let mut originals = Vec::new();
            let mut causes = Vec::new();
            let mut releases = Vec::new();
            let mut starts = Vec::new();
            let mut submissions = Vec::new();
            for values in [vec![7, 3, 5], vec![23, 19, 17]] {
                let original = Arc::new(values);
                originals.push(Arc::downgrade(&original));
                let (pool, request) = replacement_request(IndexSource {
                    original: Some(original),
                })
                .await;
                pools.push(pool);
                sends.send(request);
                let cause = Arc::new(String::from("original preparation continuation failure"));
                causes.push(Arc::downgrade(&cause));
                let (started, start) = oneshot::channel();
                let (submitted, submission) = oneshot::channel();
                let (release, gate) = mpsc::channel();
                host.operations.push_back((
                    IndexOperation::Blocking {
                        started,
                        submitted,
                        release: gate,
                    },
                    IndexContinuation::NativeFailure(cause),
                ));
                starts.push(start);
                submissions.push(submission);
                releases.push(release);
            }
            let mut interpretation = Some(InterpretationProgress::Original(sends));
            <SourceActions<_> as InterpretSends<_, Never, Here>>::interpret(
                &mut interpretation,
                &mut host,
            )
            .await;
            let Some(InterpretationProgress::Completed(Interpretation::Complete(settlements))) =
                interpretation.take()
            else {
                panic!("typed source actions complete both starts");
            };
            let inputs = settlements.into_inputs();
            // Full typed FIFO start actions are observed before blocking execution waits.
            assert_eq!(inputs.len(), 2);
            for (pool, input) in pools.iter_mut().zip(inputs) {
                let actions = pool
                    .transition(FifoEvent::WorkerPreparationStarted(input))
                    .unwrap_or_else(|_| panic!("the original FIFO accepts its own receipt"));
                index_lanes(&actions.sends);
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.become_, Step::Continue));
            }
            let first_id = submissions
                .remove(0)
                .await
                .expect("first actual submission");
            let second_id = submissions
                .remove(0)
                .await
                .expect("second actual submission");
            starts
                .remove(0)
                .await
                .expect("first actual running operation");
            let queued = starts[0].try_recv();
            let admitted_permits = admission.available_permits();
            // Actual capability failures do not replace either original blocking result.
            let mut failures = Vec::new();
            for _ in 0..2 {
                match host
                    .tasks
                    .as_mut()
                    .expect("the original task owner remains")
                    .next_event()
                    .await
                {
                    Err(original) => failures.push(original),
                    Ok(never) => match never {},
                }
            }
            let retained_causes = causes.iter().map(Weak::strong_count).collect::<Vec<_>>();
            let mut original_blocking_ids = Vec::new();
            for row in &host.rows {
                let retained = row.lock().await;
                original_blocking_ids.push(match retained.as_ref() {
                    Some(IndexPreparation::Blocking { task, .. }) => Some(task.id()),
                    Some(
                        IndexPreparation::Asynchronous { .. } | IndexPreparation::Completed { .. },
                    )
                    | None => None,
                });
            }
            let mut capabilities = Some(host);
            let mut received = None;
            let (first_observation, interrupted_observation) = {
                let mut cleanup = pin!(IndexCapabilities::receive_retirement(
                    &mut capabilities,
                    &mut received
                ));
                let first_cut = poll_fn(|cx| {
                    Poll::Ready(catch_unwind(AssertUnwindSafe(|| cleanup.as_mut().poll(cx))))
                })
                .await;
                let first_observation = match &first_cut {
                    Ok(Poll::Pending) => IndexCleanup::Waiting,
                    Ok(Poll::Ready(())) => IndexCleanup::Returned,
                    Err(_) => IndexCleanup::NativeFailure,
                };
                releases.remove(0).send(()).expect("release running work");
                starts
                    .remove(0)
                    .await
                    .expect("queued work starts after the first completes");
                let next_cut = match first_cut {
                    Ok(Poll::Pending) => {
                        poll_fn(|cx| {
                            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                                cleanup.as_mut().poll(cx)
                            })))
                        })
                        .await
                    }
                    acquired => acquired,
                };
                let interrupted_observation = match &next_cut {
                    Ok(Poll::Pending) => IndexCleanup::Waiting,
                    Ok(Poll::Ready(())) => IndexCleanup::Returned,
                    Err(_) => IndexCleanup::NativeFailure,
                };
                // Leaving this scope drops the original borrowing coroutine without losing prefix or tail.
                if let Err(original_receiver_cause) = next_cut {
                    drop(original_receiver_cause);
                }
                (first_observation, interrupted_observation)
            };
            let acquired_prefix = received.as_ref().and_then(|retirement| {
                let [(_, Ok((original, sorted)))] = retirement.descendants.0.as_slice() else {
                    return None;
                };
                Some((
                    Arc::as_ptr(original),
                    original.as_ref().clone(),
                    sorted.clone(),
                ))
            });
            let remaining_rows = capabilities.as_ref().map(|owner| owner.rows.len());
            let retained_inputs = originals.iter().map(Weak::strong_count).collect::<Vec<_>>();
            // No custody assertion can abandon the still-gated noncooperative operation.
            releases
                .remove(0)
                .send(())
                .expect("release formerly queued work");
            let completed = {
                let mut cleanup = pin!(IndexCapabilities::receive_retirement(
                    &mut capabilities,
                    &mut received
                ));
                poll_fn(
                    |cx| match catch_unwind(AssertUnwindSafe(|| cleanup.as_mut().poll(cx))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                        Err(original) => Poll::Ready(Err(original)),
                    },
                )
                .await
            };
            let report = match (completed, capabilities, received) {
                (
                    Ok(()),
                    None,
                    Some(CapabilityRetirement {
                        activation_tasks,
                        descendants,
                        terminal_report,
                        retirement_failures,
                    }),
                ) => {
                    let (control, unexpected_failures) = activation_tasks.settle().await;
                    Some((
                        descendants,
                        terminal_report,
                        retirement_failures,
                        control,
                        unexpected_failures,
                    ))
                }
                (Err(original_receiver_cause), surviving, partial) => {
                    drop((original_receiver_cause, surviving, partial));
                    None
                }
                (Ok(()), surviving, partial) => {
                    drop((surviving, partial));
                    None
                }
            };
            (
                report,
                pools,
                originals,
                causes,
                retained_causes,
                failures,
                admission,
                first_observation,
                interrupted_observation,
                acquired_prefix,
                remaining_rows,
                retained_inputs,
                first_id,
                second_id,
                original_blocking_ids,
                queued,
                admitted_permits,
            )
        });
        // Every release was used before any missing-product, prefix or identity oracle.
        // Runtime disposal is never a substitute for an available original blocking join.
        drop(runtime);
        let (
            report,
            pools,
            originals,
            causes,
            retained_causes,
            failures,
            admission,
            first_observation,
            interrupted_observation,
            acquired_prefix,
            remaining_rows,
            retained_inputs,
            first_id,
            second_id,
            original_blocking_ids,
            queued,
            admitted_permits,
        ) = observed;
        let actual_capability_ids = failures.iter().map(JoinError::id).collect::<Vec<_>>();
        let native_capability_failures = failures.iter().all(JoinError::is_panic);
        drop(failures);
        let (
            (joined, rejected, operations),
            terminal_report,
            retirement_failures,
            control,
            unexpected_failures,
        ) = report.expect("the original advanced work retirement product survives");
        assert_eq!(
            acquired_prefix,
            Some((originals[0].as_ptr(), vec![7, 3, 5], vec![3, 5, 7])),
            "interrupted retirement preserves the first acquired original reply outside its coroutine"
        );
        assert_eq!(remaining_rows, Some(1));
        assert_eq!(retained_inputs, [1, 1]);
        assert_eq!(original_blocking_ids, [Some(first_id), Some(second_id)]);
        assert_ne!(first_id, second_id);
        assert!(matches!(queued, Err(oneshot::error::TryRecvError::Empty)));
        assert_eq!(admitted_permits, 0);
        assert_eq!(first_observation, IndexCleanup::Waiting);
        assert_eq!(interrupted_observation, IndexCleanup::Waiting);
        assert!(native_capability_failures);
        assert_eq!(retained_causes, [1, 1]);
        assert_eq!(actual_capability_ids.len(), 2);
        assert_ne!(actual_capability_ids[0], actual_capability_ids[1]);
        assert_eq!(admission.available_permits(), 2);
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert_eq!(control, [] as [Never; 0]);
        assert!(unexpected_failures.is_empty());
        assert!(rejected.is_empty());
        assert!(operations.is_empty());
        assert_eq!(joined.len(), 2);
        for ((mut pool, (returned, result)), (original, (expected_original, expected_sorted))) in
            pools.into_iter().zip(joined).zip(originals.iter().zip([
                (vec![7, 3, 5], vec![3, 5, 7]),
                (vec![23, 19, 17], vec![17, 19, 23]),
            ]))
        {
            let (received, sorted) =
                result.expect("actual original blocking handle joined normally");
            assert_eq!(Arc::as_ptr(&received), original.as_ptr());
            assert_eq!(received.as_slice(), expected_original.as_slice());
            assert_eq!(sorted, expected_sorted);
            let actions = pool
                .on(returned)
                .unwrap_or_else(|_| panic!("whole original preparation returns to its own FIFO"));
            assert!(actions.sends.worker_initializations.is_empty());
            assert!(actions.sends.worker_activations.is_empty());
            assert!(actions.sends.customer_outcomes.as_slice().is_empty());
            assert!(actions.sends.worker_assignments.is_empty());
            assert!(actions.sends.worker_preparations.is_empty());
            assert!(actions.sends.restart_schedules.is_empty());
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert!(actions.sends.diagnostics.is_empty());
            let observations = actions.sends.worker_observations.into_requests();
            assert!(matches!(actions.become_, Step::Continue));
            assert_eq!(observations.len(), 1);
            assert_eq!(actions.creates.len(), 1);
            let creation = actions
                .creates
                .into_iter()
                .next()
                .expect("one actual replacement creation");
            assert_eq!(creation.id(), observations[0].child);
            drop((creation, received, sorted, observations, pool));
        }
        assert_eq!(
            causes.iter().map(Weak::strong_count).collect::<Vec<_>>(),
            [0, 0]
        );
        assert_eq!(
            originals.iter().map(Weak::strong_count).collect::<Vec<_>>(),
            [0, 0]
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep original admission, complete async return, native retirement and final ownership discharge in one observable trace."
    )]
    fn bounded_index_async_completion_keeps_original_reply_until_retirement() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one finite async host");
        let (descendants, mut pool, original, admission) = runtime.block_on(async {
            let input = Arc::new(vec![13, 11, 7]);
            let original = Arc::downgrade(&input);
            let (mut pool, request) = replacement_request(IndexSource {
                original: Some(input),
            })
            .await;
            let admission = Arc::new(Semaphore::new(1));
            let (release, gate) = oneshot::channel();
            let mut host = IndexCapabilities {
                admission: Arc::clone(&admission),
                operations: [(
                    IndexOperation::Asynchronous(gate),
                    IndexContinuation::Complete,
                )]
                .into(),
                rows: Vec::new(),
                rejected: Vec::new(),
                tasks: Some(ActivationTasks::new()),
            };
            let mut sends = SourceActions::empty();
            sends.send(request);
            let mut interpretation = Some(InterpretationProgress::Original(sends));
            <SourceActions<_> as InterpretSends<_, Never, Here>>::interpret(
                &mut interpretation,
                &mut host,
            )
            .await;
            let Some(InterpretationProgress::Completed(Interpretation::Complete(settlements))) =
                interpretation.take()
            else {
                panic!("one genuine asynchronous start completes");
            };
            let mut receipts = settlements.into_inputs();
            let receipt = receipts
                .pop()
                .expect("one authoritative preparation receipt");
            assert!(receipts.is_empty());
            let started = pool
                .transition(FifoEvent::WorkerPreparationStarted(receipt))
                .unwrap_or_else(|_| panic!("its FIFO accepts the actual receipt"));
            index_lanes(&started.sends);
            assert!(started.creates.is_empty());
            assert!(matches!(started.become_, Step::Continue));
            assert_eq!(admission.available_permits(), 0);
            let mut wait = Box::pin(
                host.tasks
                    .as_mut()
                    .expect("original tasks remain live")
                    .next_event(),
            );
            let cut = poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx))).await;
            assert!(matches!(cut, Poll::Pending));
            drop(wait);
            release
                .send(())
                .expect("release actual asynchronous operation");
            let CapabilityRetirement {
                activation_tasks,
                descendants,
                terminal_report,
                retirement_failures,
            } = host.retire().await;
            assert!(terminal_report.is_none());
            assert!(retirement_failures.is_empty());
            let (events, failures) = activation_tasks.settle().await;
            assert_eq!(events, [] as [Never; 0]);
            assert!(failures.is_empty());
            (descendants, pool, original, admission)
        });
        drop(runtime);
        let (mut completed, rejected, operations) = descendants;
        assert!(rejected.is_empty());
        assert!(operations.is_empty());
        let (returned, output) = completed.pop().expect("one joined complete output");
        assert!(completed.is_empty());
        let (received, sorted) =
            output.expect("original async output remains owned after its producer completed");
        assert_eq!(Arc::as_ptr(&received), original.as_ptr());
        assert_eq!(received.as_slice(), [13, 11, 7]);
        assert_eq!(sorted, [7, 11, 13]);
        assert_eq!(admission.available_permits(), 1);
        let actions = pool
            .on(returned)
            .unwrap_or_else(|_| panic!("normal return remains a genuine FIFO input"));
        assert!(matches!(actions.become_, Step::Continue));
        assert_eq!(actions.creates.len(), 1);
        assert_eq!(actions.sends.worker_observations.len(), 1);
        assert!(actions.sends.worker_initializations.is_empty());
        assert!(actions.sends.worker_activations.is_empty());
        assert!(actions.sends.customer_outcomes.as_slice().is_empty());
        assert!(actions.sends.worker_assignments.is_empty());
        assert!(actions.sends.worker_preparations.is_empty());
        assert!(actions.sends.restart_schedules.is_empty());
        assert!(actions.sends.worker_shutdowns.is_empty());
        assert!(actions.sends.diagnostics.is_empty());
        let observation = actions
            .sends
            .worker_observations
            .into_requests()
            .pop()
            .expect("one exact replacement observation");
        let creation = actions
            .creates
            .into_iter()
            .next()
            .expect("one original replacement creation");
        assert_eq!(creation.id(), observation.child);
        drop((creation, observation, received, sorted, pool));
        assert_eq!(original.strong_count(), 0);
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "Both actual admission failures preserve the original source reason through all named FIFO lanes and its owning consuming diagnostic."
    )]
    fn bounded_index_admission_rejection_returns_original_source_through_fifo_policy() {
        for selected in [IndexAdmission::Saturated, IndexAdmission::Closed] {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("one finite rejected host");
            let (descendants, mut pool, original, original_role) = runtime.block_on(async {
                let input = Arc::new(vec![41, 31, 37]);
                let original = Arc::downgrade(&input);
                let (mut pool, mut request) = replacement_request(IndexSource {
                    original: Some(input),
                })
                .await;
                let (_, role) = request.source_and_role();
                let original_role = ptr::from_ref(role);
                let admission = match selected {
                    IndexAdmission::Saturated => Arc::new(Semaphore::new(0)),
                    IndexAdmission::Closed => {
                        let admission = Arc::new(Semaphore::new(1));
                        admission.close();
                        admission
                    }
                };
                let mut host = IndexCapabilities {
                    admission,
                    operations: VecDeque::new(),
                    rows: Vec::new(),
                    rejected: Vec::new(),
                    tasks: Some(ActivationTasks::new()),
                };
                let mut sends = SourceActions::empty();
                sends.send(request);
                let mut interpretation = Some(InterpretationProgress::Original(sends));
                <SourceActions<_> as InterpretSends<_, Never, Here>>::interpret(
                    &mut interpretation,
                    &mut host,
                )
                .await;
                let Some(InterpretationProgress::Completed(Interpretation::Complete(settlements))) =
                    interpretation.take()
                else {
                    panic!("late source refusal retains a complete real start receipt");
                };
                let mut receipts = settlements.into_inputs();
                let receipt = receipts.pop().expect("one original issued receipt");
                assert!(receipts.is_empty());
                let started = pool
                    .transition(FifoEvent::WorkerPreparationStarted(receipt))
                    .unwrap_or_else(|_| {
                        panic!("the pool accepts its own actual preparation start")
                    });
                index_lanes(&started.sends);
                assert!(started.creates.is_empty());
                assert!(matches!(started.become_, Step::Continue));
                assert!(host.rows.is_empty());
                assert!(
                    host.tasks
                        .as_ref()
                        .expect("unstarted tasks remain owned")
                        .is_empty()
                );
                assert!(host.operations.is_empty());
                let CapabilityRetirement {
                    activation_tasks,
                    descendants,
                    terminal_report,
                    retirement_failures,
                } = host.retire().await;
                assert!(terminal_report.is_none());
                assert!(retirement_failures.is_empty());
                let (events, failures) = activation_tasks.settle().await;
                assert_eq!(events, [] as [Never; 0]);
                assert!(failures.is_empty());
                (descendants, pool, original, original_role)
            });
            drop(runtime);
            let (completed, mut rejected, operations) = descendants;
            assert!(completed.is_empty());
            assert!(operations.is_empty());
            let returned = rejected.pop().expect("one exact late source rejection");
            assert!(rejected.is_empty());
            let actions = pool.on(returned).unwrap_or_else(|_| {
                panic!("existing FIFO source policy receives the actual rejection")
            });
            assert!(matches!(actions.become_, Step::Continue));
            assert!(actions.creates.is_empty());
            assert!(actions.sends.worker_observations.is_empty());
            assert!(actions.sends.worker_initializations.is_empty());
            assert!(actions.sends.worker_activations.is_empty());
            assert!(actions.sends.customer_outcomes.as_slice().is_empty());
            assert!(actions.sends.worker_assignments.is_empty());
            assert!(actions.sends.worker_preparations.is_empty());
            assert!(actions.sends.restart_schedules.is_empty());
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert_eq!(actions.sends.diagnostics.len(), 1);
            let diagnostic = actions
                .sends
                .diagnostics
                .into_requests()
                .pop()
                .expect("one original source diagnostic");
            let diagnostic = match diagnostic {
                DiagnosticAction::Terminal { diagnostic } => diagnostic,
                DiagnosticAction::Deliver { route, .. } => match route {},
            };
            let extraction = diagnostic.into_source_rejection();
            let Ok((role, previous, stopped, returned_source, reason)) = extraction else {
                panic!("real source refusal remains directly consumable");
            };
            assert_eq!(*role, SearchRole::Primary);
            assert_eq!(Arc::as_ptr(&role), original_role);
            // These are actual owned previous/stopped facts from the diagnostic, not predicted IDs.
            // The generic construction fixture supplies no independent lineage oracle for them.
            assert!(returned_source.is_none());
            let recovered = match (selected, reason) {
                (IndexAdmission::Saturated, IndexRejection::Saturated(original))
                | (IndexAdmission::Closed, IndexRejection::Closed(original)) => original,
                (_, unexpected) => {
                    drop(unexpected);
                    panic!("admission failure has its exact distinct cause");
                }
            };
            assert_eq!(Arc::as_ptr(&recovered), original.as_ptr());
            assert_eq!(recovered.as_slice(), [41, 31, 37]);
            assert_eq!(original.strong_count(), 1);
            drop((role, previous, stopped, returned_source, recovered, pool));
            assert_eq!(original.strong_count(), 0);
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "One existing bounded owner proves actual running/queued saturation, exact third rejection, permit reuse and complete joined custody before assertions."
    )]
    fn bounded_index_saturation_returns_third_original_and_completion_reopens_admission() {
        let runtime = Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("one actual blocking worker");
        let observed = runtime.block_on(async {
            let admission = Arc::new(Semaphore::new(2));
            let mut host = IndexCapabilities {
                admission: Arc::clone(&admission),
                operations: VecDeque::new(),
                rows: Vec::new(),
                rejected: Vec::new(),
                tasks: Some(ActivationTasks::new()),
            };
            let mut pools = Vec::new();
            let mut requests = VecDeque::new();
            let mut originals = Vec::new();
            let mut original_roles = Vec::new();
            // Complete the pure FIFO construction before any blocking task is admitted.
            for values in [
                vec![7, 3, 5],
                vec![23, 19, 17],
                vec![41, 31, 37],
                vec![59, 53, 47],
            ] {
                let input = Arc::new(values);
                originals.push(Arc::downgrade(&input));
                let (pool, mut request) = replacement_request(IndexSource {
                    original: Some(input),
                })
                .await;
                let (_, role) = request.source_and_role();
                original_roles.push(ptr::from_ref(role));
                pools.push(pool);
                requests.push_back(request);
            }
            let mut releases = Vec::new();
            let mut starts = Vec::new();
            let mut submissions = Vec::new();
            // Two initial operations and one available later operation use the same port.
            for _ in 0..3 {
                let (started, start) = oneshot::channel();
                let (submitted, submission) = oneshot::channel();
                let (release, gate) = mpsc::channel();
                host.operations.push_back((
                    IndexOperation::Blocking {
                        started,
                        submitted,
                        release: gate,
                    },
                    IndexContinuation::Complete,
                ));
                releases.push(release);
                starts.push(start);
                submissions.push(submission);
            }
            let mut receipts = Vec::new();
            let mut returned_inputs = Vec::new();
            for _ in 0..2 {
                let mut input = requests.pop_front();
                let mut received = None;
                host.interpret_item(&mut input, &mut received).await;
                returned_inputs.push(input);
                receipts.push(received);
            }
            let first_submission = submissions[0].try_recv();
            let second_submission = submissions[1].try_recv();
            let running = (&mut starts[0]).await;
            let queued = starts[1].try_recv();
            let occupied_permits = admission.available_permits();
            let occupied_rows = host.rows.len();
            let occupied_operations = host.operations.len();
            // The third original is submitted while both real permits are still occupied.
            let mut third = requests.pop_front();
            let mut third_receipt = None;
            host.interpret_item(&mut third, &mut third_receipt).await;
            returned_inputs.push(third);
            receipts.push(third_receipt);
            let saturated_rows = host.rows.len();
            let saturated_rejections = host.rejected.len();
            let saturated_operations = host.operations.len();
            let saturated_permits = admission.available_permits();
            let retained_third = originals[2].strong_count();
            // Only actual first-operation completion may return its permit.
            let first_release = releases.remove(0).send(());
            let first_completion = {
                let mut retained = host.rows[0].lock().await;
                IndexPreparation::receive(&mut retained).await;
                match retained.as_ref() {
                    Some(IndexPreparation::Completed {
                        received: Ok((original, sorted)),
                        ..
                    }) => Some((
                        Arc::as_ptr(original),
                        original.as_ref().clone(),
                        sorted.clone(),
                    )),
                    Some(
                        IndexPreparation::Blocking { .. }
                        | IndexPreparation::Asynchronous { .. }
                        | IndexPreparation::Completed {
                            received: Err(_), ..
                        },
                    )
                    | None => None,
                }
            };
            let completed_permits = admission.available_permits();
            // An accidental third admission consumes this last operation. Keep that
            // counterfactual finite instead of panicking while its blocking gate is owned.
            let mut fourth = requests.pop_front();
            let mut fourth_receipt = None;
            if host.operations.front().is_some() {
                host.interpret_item(&mut fourth, &mut fourth_receipt).await;
            }
            returned_inputs.push(fourth);
            receipts.push(fourth_receipt);
            let resumed_submission = submissions[2].try_recv();
            let resumed_permits = admission.available_permits();
            let resumed_rows = host.rows.len();
            let resumed_rejections = host.rejected.len();
            let resumed_operations = host.operations.len();
            // All remaining running/queued gates are released before any negative oracle.
            let remaining_releases = releases
                .into_iter()
                .map(|release| release.send(()))
                .collect::<Vec<_>>();
            let CapabilityRetirement {
                activation_tasks,
                descendants,
                terminal_report,
                retirement_failures,
            } = host.retire().await;
            let (events, failures) = activation_tasks.settle().await;
            (
                admission,
                pools,
                originals,
                original_roles,
                requests,
                receipts,
                returned_inputs,
                first_submission,
                second_submission,
                resumed_submission,
                running,
                queued,
                occupied_permits,
                occupied_rows,
                occupied_operations,
                saturated_rows,
                saturated_rejections,
                saturated_operations,
                saturated_permits,
                retained_third,
                first_release,
                first_completion,
                completed_permits,
                resumed_permits,
                resumed_rows,
                resumed_rejections,
                resumed_operations,
                remaining_releases,
                descendants,
                terminal_report,
                retirement_failures,
                events,
                failures,
            )
        });
        // Actual joins, not runtime disposal, discharge all admitted work.
        drop(runtime);
        let (
            admission,
            mut pools,
            originals,
            original_roles,
            requests,
            receipts,
            returned_inputs,
            first_submission,
            second_submission,
            resumed_submission,
            running,
            queued,
            occupied_permits,
            occupied_rows,
            occupied_operations,
            saturated_rows,
            saturated_rejections,
            saturated_operations,
            saturated_permits,
            retained_third,
            first_release,
            first_completion,
            completed_permits,
            resumed_permits,
            resumed_rows,
            resumed_rejections,
            resumed_operations,
            remaining_releases,
            (joined, mut rejected, operations),
            terminal_report,
            retirement_failures,
            events,
            failures,
        ) = observed;
        assert!(requests.is_empty());
        assert!(returned_inputs.iter().all(Option::is_none));
        assert!(running.is_ok());
        assert!(matches!(queued, Err(oneshot::error::TryRecvError::Empty)));
        assert_eq!(
            (occupied_permits, occupied_rows, occupied_operations),
            (0, 2, 1)
        );
        assert_eq!(
            (
                saturated_rows,
                saturated_rejections,
                saturated_operations,
                saturated_permits
            ),
            (2, 1, 1, 0)
        );
        assert_eq!(retained_third, 1);
        assert!(first_release.is_ok());
        assert!(remaining_releases.iter().all(Result::is_ok));
        assert_eq!(
            first_completion,
            Some((originals[0].as_ptr(), vec![7, 3, 5], vec![3, 5, 7]))
        );
        assert_eq!(completed_permits, 1);
        assert_eq!(
            (
                resumed_permits,
                resumed_rows,
                resumed_rejections,
                resumed_operations
            ),
            (0, 3, 1, 0)
        );
        let first_submission =
            first_submission.expect("first admitted task has its original native ID");
        let second_submission =
            second_submission.expect("queued admitted task has its original native ID");
        let resumed_submission =
            resumed_submission.expect("completion permits an actual later admitted task");
        assert_ne!(first_submission, second_submission);
        assert_ne!(first_submission, resumed_submission);
        assert_ne!(second_submission, resumed_submission);
        assert_eq!(admission.available_permits(), 2);
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert_eq!(events, [] as [Never; 0]);
        assert!(failures.is_empty());
        assert!(operations.is_empty());
        assert_eq!(receipts.len(), 4);
        for (pool, receipt) in pools.iter_mut().zip(receipts) {
            let Some(ItemSettlement::Accepted(receipt)) = receipt else {
                panic!("each exact request retains its genuine preparation-start receipt");
            };
            let actions = pool
                .transition(FifoEvent::WorkerPreparationStarted(SettledItem::Attempted(
                    ItemSettlement::Accepted(receipt),
                )))
                .unwrap_or_else(|_| panic!("each original FIFO accepts its own start"));
            index_lanes(&actions.sends);
            assert!(actions.creates.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }
        assert_eq!(rejected.len(), 1);
        let returned = rejected
            .pop()
            .expect("exact third request has its real late source rejection");
        let mut third_pool = pools.remove(2);
        let actions = third_pool
            .on(returned)
            .unwrap_or_else(|_| panic!("actual third rejection reaches its owning FIFO policy"));
        assert!(matches!(actions.become_, Step::Continue));
        assert!(actions.creates.is_empty());
        assert!(actions.sends.worker_observations.is_empty());
        assert!(actions.sends.worker_initializations.is_empty());
        assert!(actions.sends.worker_activations.is_empty());
        assert!(actions.sends.customer_outcomes.as_slice().is_empty());
        assert!(actions.sends.worker_assignments.is_empty());
        assert!(actions.sends.worker_preparations.is_empty());
        assert!(actions.sends.restart_schedules.is_empty());
        assert!(actions.sends.worker_shutdowns.is_empty());
        assert_eq!(actions.sends.diagnostics.len(), 1);
        let diagnostic = actions
            .sends
            .diagnostics
            .into_requests()
            .pop()
            .expect("one complete source rejection diagnostic");
        let diagnostic = match diagnostic {
            DiagnosticAction::Terminal { diagnostic } => diagnostic,
            DiagnosticAction::Deliver { route, .. } => match route {},
        };
        let extraction = diagnostic.into_source_rejection();
        let Ok((role, previous, stopped, returned_source, reason)) = extraction else {
            panic!("third admission rejection remains fully typed and owned");
        };
        assert_eq!(*role, SearchRole::Primary);
        assert_eq!(Arc::as_ptr(&role), original_roles[2]);
        assert!(returned_source.is_none());
        let IndexRejection::Saturated(recovered) = reason else {
            panic!("two admitted originals cause genuine Saturated, not Closed");
        };
        assert_eq!(Arc::as_ptr(&recovered), originals[2].as_ptr());
        assert_eq!(recovered.as_slice(), [41, 31, 37]);
        assert_eq!(originals[2].strong_count(), 1);
        drop((
            role,
            previous,
            stopped,
            returned_source,
            recovered,
            third_pool,
        ));
        assert_eq!(originals[2].strong_count(), 0);
        assert_eq!(joined.len(), 3);
        for ((mut pool, (returned, result)), (original, expected)) in
            pools.into_iter().zip(joined).zip(
                [&originals[0], &originals[1], &originals[3]]
                    .into_iter()
                    .zip([
                        (vec![7, 3, 5], vec![3, 5, 7]),
                        (vec![23, 19, 17], vec![17, 19, 23]),
                        (vec![59, 53, 47], vec![47, 53, 59]),
                    ]),
            )
        {
            let (received, sorted) =
                result.expect("each original admitted blocking handle is joined");
            assert_eq!(Arc::as_ptr(&received), original.as_ptr());
            assert_eq!(received.as_slice(), expected.0.as_slice());
            assert_eq!(sorted, expected.1);
            let actions = pool
                .on(returned)
                .unwrap_or_else(|_| panic!("completed original admission returns to its own FIFO"));
            assert!(matches!(actions.become_, Step::Continue));
            assert!(actions.sends.worker_initializations.is_empty());
            assert!(actions.sends.worker_activations.is_empty());
            assert!(actions.sends.customer_outcomes.as_slice().is_empty());
            assert!(actions.sends.worker_assignments.is_empty());
            assert!(actions.sends.worker_preparations.is_empty());
            assert!(actions.sends.restart_schedules.is_empty());
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert!(actions.sends.diagnostics.is_empty());
            let observations = actions.sends.worker_observations.into_requests();
            assert_eq!(observations.len(), 1);
            assert_eq!(actions.creates.len(), 1);
            let creation = actions
                .creates
                .into_iter()
                .next()
                .expect("one original replacement creation");
            assert_eq!(creation.id(), observations[0].child);
            drop((creation, observations, received, sorted, pool));
        }
        assert_eq!(
            originals.iter().map(Weak::strong_count).collect::<Vec<_>>(),
            [0, 0, 0, 0]
        );
    }

    #[tokio::test]
    async fn unpolled_index_admission_retains_the_original_request() {
        let original = Arc::new(vec![7, 3, 5]);
        let retained = Arc::downgrade(&original);
        let (pool, request) = replacement_request(IndexSource {
            original: Some(original),
        })
        .await;
        let admission = Arc::new(Semaphore::new(2));
        let mut host = IndexCapabilities {
            admission: Arc::clone(&admission),
            operations: VecDeque::new(),
            rows: Vec::new(),
            rejected: Vec::new(),
            tasks: Some(ActivationTasks::new()),
        };
        let mut input = Some(request);
        let mut reply = None;
        let unpolled = host.interpret_item(&mut input, &mut reply);
        drop(unpolled);
        let permits = admission.available_permits();
        let rows = host.rows.len();
        let rejected = host.rejected.len();
        let operations = host.operations.len();
        let tasks = host
            .tasks
            .take()
            .expect("the unpolled host still owns its task set");
        let (events, failures) = tasks.settle().await;
        drop(host);
        let Some(mut request) = input else {
            panic!("unpolled admission preserves its exact original request");
        };
        let (source, role) = request.source_and_role();
        let recovered = source
            .original
            .take()
            .expect("unpolled admission preserves its original source allocation");
        assert_eq!(Arc::as_ptr(&recovered), retained.as_ptr());
        assert_eq!(recovered.as_slice(), [7, 3, 5]);
        assert_eq!(*role, SearchRole::Primary);
        assert!(reply.is_none());
        assert_eq!((permits, rows, rejected, operations), (2, 0, 0, 0));
        assert_eq!(events, [] as [Never; 0]);
        assert!(failures.is_empty());
        assert_eq!(retained.strong_count(), 1);
        drop((request, recovered, pool, admission));
        assert_eq!(retained.strong_count(), 0);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum IndexRuntimeEnd {
        Awaited,
        Destroyed,
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "One finite normal/destruction comparison retains real blocking joins and all original FIFO outcomes before any assertions."
    )]
    fn index_runtime_destruction_retains_original_blocking_joins() {
        for ending in [IndexRuntimeEnd::Awaited, IndexRuntimeEnd::Destroyed] {
            let runtime = Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .expect("one real blocking worker");
            let admission = Arc::new(Semaphore::new(2));
            let mut host = IndexCapabilities {
                admission: Arc::clone(&admission),
                operations: VecDeque::new(),
                rows: Vec::new(),
                rejected: Vec::new(),
                tasks: Some(ActivationTasks::new()),
            };
            let mut pools = Vec::new();
            let mut originals = Vec::new();
            let mut receipts = Vec::new();
            let mut replies = Vec::new();
            let mut releases = Vec::new();
            let mut starts = Vec::new();
            let mut submissions = Vec::new();
            let mut admission_attempts = Vec::new();
            runtime.block_on(async {
                for values in [vec![7, 3, 5], vec![23, 19, 17]] {
                    let original = Arc::new(values);
                    originals.push(Arc::downgrade(&original));
                    let (pool, request) = replacement_request(IndexSource {
                        original: Some(original),
                    })
                    .await;
                    let (started, start) = oneshot::channel();
                    let (submitted, submission) = oneshot::channel();
                    let (release, gate) = mpsc::channel();
                    host.operations.push_back((
                        IndexOperation::Blocking {
                            started,
                            submitted,
                            release: gate,
                        },
                        IndexContinuation::Complete,
                    ));
                    pools.push(pool);
                    releases.push(release);
                    starts.push(start);
                    submissions.push(submission);
                    let mut input = Some(request);
                    let mut receipt = None;
                    {
                        let attempt = host.interpret_item(&mut input, &mut receipt);
                        let mut attempt = pin!(attempt);
                        let admission_poll =
                            poll_fn(|context| Poll::Ready(attempt.as_mut().poll(context))).await;
                        admission_attempts.push(match admission_poll {
                            Poll::Pending => IndexCleanup::Waiting,
                            Poll::Ready(()) => IndexCleanup::Returned,
                        });
                    }
                    replies.push(input);
                    receipts.push(receipt);
                }
            });
            let running = runtime.block_on(&mut starts[0]);
            let queued = starts[1].try_recv();
            let first_submission = submissions[0].try_recv();
            let second_submission = submissions[1].try_recv();
            let occupied = admission.available_permits();
            let held_before_end = originals.iter().map(Weak::strong_count).collect::<Vec<_>>();
            let (retired, events, failures, signal_receipts) = match ending {
                IndexRuntimeEnd::Awaited => {
                    let signal_receipts = releases
                        .into_iter()
                        .map(|release| release.send(()))
                        .collect::<Vec<_>>();
                    let (retired, events, failures) = runtime.block_on(async {
                        let retired = host.retire().await;
                        let CapabilityRetirement {
                            activation_tasks,
                            descendants,
                            terminal_report,
                            retirement_failures,
                        } = retired;
                        let (events, failures) = activation_tasks.settle().await;
                        (
                            (descendants, terminal_report, retirement_failures),
                            events,
                            failures,
                        )
                    });
                    drop(runtime);
                    (retired, events, failures, signal_receipts)
                }
                IndexRuntimeEnd::Destroyed => {
                    // Original host and every genuine blocking handle stay outside H.
                    // This stops waiting; it neither preempts nor joins blocking work.
                    runtime.shutdown_background();
                    let receiver_host = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a real surviving host receives original joins");
                    let signal_receipts = releases
                        .into_iter()
                        .map(|release| release.send(()))
                        .collect::<Vec<_>>();
                    let (retired, events, failures) = receiver_host.block_on(async {
                        let retired = host.retire().await;
                        let CapabilityRetirement {
                            activation_tasks,
                            descendants,
                            terminal_report,
                            retirement_failures,
                        } = retired;
                        let (events, failures) = activation_tasks.settle().await;
                        (
                            (descendants, terminal_report, retirement_failures),
                            events,
                            failures,
                        )
                    });
                    drop(receiver_host);
                    (retired, events, failures, signal_receipts)
                }
            };
            let ((joined, rejected, operations), terminal_report, retirement_failures) = retired;
            assert!(running.is_ok());
            assert!(matches!(queued, Err(oneshot::error::TryRecvError::Empty)));
            assert_eq!(occupied, 0);
            assert_eq!(held_before_end, [1, 1]);
            assert!(signal_receipts.iter().all(Result::is_ok));
            let first_submission =
                first_submission.expect("first actual blocking submission retains its native ID");
            let second_submission =
                second_submission.expect("queued actual blocking submission retains its native ID");
            assert_ne!(first_submission, second_submission);
            assert_eq!(admission.available_permits(), 2);
            assert!(admission.is_closed());
            assert_eq!(
                admission_attempts,
                [IndexCleanup::Returned, IndexCleanup::Returned]
            );
            assert!(replies.iter().all(Option::is_none));
            assert!(rejected.is_empty());
            assert!(operations.is_empty());
            assert!(terminal_report.is_none());
            assert!(retirement_failures.is_empty());
            assert_eq!(events, [] as [Never; 0]);
            match ending {
                IndexRuntimeEnd::Awaited => assert!(failures.is_empty()),
                IndexRuntimeEnd::Destroyed => {
                    assert_eq!(failures.len(), 2);
                    assert!(failures.iter().all(JoinError::is_cancelled));
                }
            }
            assert_eq!(receipts.len(), 2);
            assert_eq!(joined.len(), 2);
            for (((mut pool, receipt), (returned, result)), (original, expected)) in pools
                .into_iter()
                .zip(receipts)
                .zip(joined)
                .zip(originals.iter().zip([
                    (vec![7, 3, 5], vec![3, 5, 7]),
                    (vec![23, 19, 17], vec![17, 19, 23]),
                ]))
            {
                let Some(ItemSettlement::Accepted(receipt)) = receipt else {
                    panic!("each original admission retains its complete actual start receipt");
                };
                let started = pool
                    .transition(FifoEvent::WorkerPreparationStarted(SettledItem::Attempted(
                        ItemSettlement::Accepted(receipt),
                    )))
                    .unwrap_or_else(|_| panic!("the original pool accepts its own actual start"));
                index_lanes(&started.sends);
                assert!(started.creates.is_empty());
                assert!(matches!(started.become_, Step::Continue));
                let (received, sorted) = result.expect("the retained genuine blocking handle returns its original result even after losing H");
                assert_eq!(Arc::as_ptr(&received), original.as_ptr());
                assert_eq!(received.as_slice(), expected.0.as_slice());
                assert_eq!(sorted, expected.1);
                let actions = pool.on(returned).unwrap_or_else(|_| {
                    panic!("each genuine complete return reaches its original FIFO")
                });
                assert!(matches!(actions.become_, Step::Continue));
                assert!(actions.sends.worker_initializations.is_empty());
                assert!(actions.sends.worker_activations.is_empty());
                assert!(actions.sends.customer_outcomes.as_slice().is_empty());
                assert!(actions.sends.worker_assignments.is_empty());
                assert!(actions.sends.worker_preparations.is_empty());
                assert!(actions.sends.restart_schedules.is_empty());
                assert!(actions.sends.worker_shutdowns.is_empty());
                assert!(actions.sends.diagnostics.is_empty());
                let observations = actions.sends.worker_observations.into_requests();
                assert_eq!(observations.len(), 1);
                assert_eq!(actions.creates.len(), 1);
                let creation = actions
                    .creates
                    .into_iter()
                    .next()
                    .expect("one original worker replacement remains owned");
                assert_eq!(creation.id(), observations[0].child);
                drop((creation, observations, received, sorted, pool));
            }
            drop(failures);
            assert_eq!(
                originals.iter().map(Weak::strong_count).collect::<Vec<_>>(),
                [0, 0]
            );
        }
    }

    enum IndexDepotEvent {
        User(User<MailAddr, Never>),
        Started(
            ActionItemResult<
                PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            >,
        ),
        Requested(PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>),
    }

    impl UserEvent for IndexDepotEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(from: MailAddr, message: Never) -> Self {
            Self::User(User::new(from, message))
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            match self {
                Self::User(user) => Ok(user),
                event => Err(event),
            }
        }
    }

    impl
        EventIngress<
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            ActionItemResult<
                PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            >,
        > for IndexDepotEvent
    {
        fn ingress(
            input: ActionItemResult<
                PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            >,
        ) -> Self {
            Self::Started(input)
        }
    }

    struct IndexDepot {
        requests: Vec<PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>>,
        started: Vec<WorkerPreparationStarted>,
    }

    impl BehaviorBase for IndexDepot {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    impl Behavior for IndexDepot {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = IndexDepotEvent;
        type Sends = SourceActions<
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
        >;
        type Ph = Never;
        type Error = ActionItemResult<
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
        >;
        type Birth = NoBirths;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let mut actions = ActionsOf::<Self>::cont();
            for request in self.requests.drain(..) {
                actions.sends.send(request);
            }
            Ok(actions)
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            let mut actions = ActionsOf::<Self>::cont();
            match event {
                IndexDepotEvent::User(user) => match user.message {},
                IndexDepotEvent::Requested(request) => actions.sends.send(request),
                IndexDepotEvent::Started(SettledItem::Attempted(ItemSettlement::Accepted(
                    receipt,
                ))) => self.started.push(receipt),
                IndexDepotEvent::Started(input) => return Err(input),
            }
            Ok(actions)
        }
    }

    // This concrete product owns real actor receipt admission alongside original
    // outside work/partial retirement. It introduces no production interpreter.
    struct IndexDepotCapabilities {
        work: Option<IndexCapabilities>,
        received_work: Option<
            CapabilityRetirement<Never, <IndexCapabilities as RetireCapabilities>::Descendants>,
        >,
        work_controls: Vec<Never>,
        work_failures: Vec<JoinError>,
        control: ControlSender<IndexDepotEvent>,
    }

    impl
        SourceAdmission<
            IndexDepotEvent,
            PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            ActionItemResult<
                PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
            >,
        > for IndexDepotCapabilities
    {
        async fn admit_source(
            &mut self,
            input: &mut Option<
                ActionItemResult<
                    PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
                >,
            >,
            reply: &mut Option<
                Result<
                    (),
                    ActionItemResult<
                        PrepareWorkers<IndexSource, SearchRole, SearchWorker, ImmediateActivation>,
                    >,
                >,
            >,
        ) {
            if reply.is_some() {
                return;
            }
            let Some(original) = input.take() else {
                return;
            };
            *reply = Some(
                match self.control.send(IndexDepotEvent::Started(original)) {
                    Ok(()) => Ok(()),
                    Err(ControlClosed(IndexDepotEvent::Started(original))) => Err(original),
                    Err(ControlClosed(IndexDepotEvent::User(user))) => match user.message {},
                    Err(ControlClosed(IndexDepotEvent::Requested(request))) => {
                        drop(request);
                        unreachable!("this exact source adapter sends only the Started variant");
                    }
                },
            );
        }
    }

    impl CommitActions<IndexDepot> for IndexDepotCapabilities {
        type Retired = (
            <IndexCapabilities as RetireCapabilities>::Descendants,
            Vec<Never>,
            Vec<JoinError>,
        );

        async fn commit(
            &mut self,
            progress: &mut Option<
                InterpretationProgress<
                    ActionsOf<IndexDepot>,
                    <IndexDepot as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<IndexDepot>,
                >,
            >,
        ) {
            // Item interpretation does not consume an actor event; source admission
            // below delivers its genuine result through the actor's actual protocol.
            ActionsOf::<IndexDepot>::interpret::<IndexCapabilities, Never, Here>(
                progress,
                self.work
                    .as_mut()
                    .expect("live actor work retains its original capability owner"),
            )
            .await;
        }

        async fn offer_next(
            &mut self,
            progress: &mut Option<
                SourceProgress<
                    ActionSettlementOf<IndexDepot>,
                    <IndexDepot as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<IndexDepot> as SourceSettlementCustody<Self, IndexDepotEvent>>::prepare_source(progress);
            if let Some(SourceProgress::Offering(custody)) = progress {
                <ActionSettlementOf<IndexDepot> as SourceSettlementCustody<
                    Self,
                    IndexDepotEvent,
                >>::offer_next_to_source(custody, self)
                .await;
            }
            <ActionSettlementOf<IndexDepot> as SourceSettlementCustody<Self, IndexDepotEvent>>::finish_source(progress);
        }

        async fn next_local_event(&mut self) -> Result<IndexDepotEvent, JoinError> {
            match RetireCapabilities::next_local_event(
                self.work
                    .as_mut()
                    .expect("actual work remains live until retirement"),
            )
            .await
            {
                Ok(never) => match never {},
                Err(failure) => Err(failure),
            }
        }
        fn next_deadline(&mut self) -> Option<Instant> {
            None
        }
        fn pop_due(&mut self, _: Instant) -> Option<IndexDepotEvent> {
            None
        }

        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<IndexDepotEvent, Self::Retired>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(owner) = interpreter.as_mut() else {
                return;
            };
            RetireCapabilities::receive_retirement(&mut owner.work, &mut owner.received_work).await;
            if owner.work.is_some() {
                return;
            }
            let Some(work) = owner.received_work.as_mut() else {
                return;
            };
            // Genuine original task set and all already acquired errors remain
            // outside this borrowed, cancellable settlement future.
            work.activation_tasks
                .receive_settlement(&mut owner.work_controls, &mut owner.work_failures)
                .await;
            let Some(mut owner) = interpreter.take() else {
                return;
            };
            let Some(CapabilityRetirement {
                activation_tasks,
                descendants,
                terminal_report,
                retirement_failures,
            }) = owner.received_work.take()
            else {
                unreachable!("actual completed work was observed outside");
            };
            // The original Never task set was fully joined; no actor-event tasks
            // were spawned. Its genuine controls/errors remain separate below.
            drop(activation_tasks);
            *received = Some(CapabilityRetirement {
                activation_tasks: ActivationTasks::new(),
                descendants: (descendants, owner.work_controls, owner.work_failures),
                terminal_report,
                retirement_failures,
            });
            drop(owner.control);
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "One real actor proves saturation, exact rejected input, completion-driven admission and nonpreemptible running/queued shutdown before any custody oracle."
    )]
    fn index_actor_retirement_joins_running_and_queued_original_work() {
        let runtime = Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("one actual blocking worker");
        let observed = runtime.block_on(async {
            let admission = Arc::new(Semaphore::new(2));
            let mut work = IndexCapabilities {
                admission: Arc::clone(&admission), operations: VecDeque::new(),
                rows: Vec::new(), rejected: Vec::new(), tasks: Some(ActivationTasks::new()),
            };
            let mut requests = Vec::new();
            let mut pools = Vec::new();
            let mut originals = Vec::new();
            let mut original_roles = Vec::new();
            for values in [vec![7, 3, 5], vec![23, 19, 17], vec![41, 31, 37], vec![59, 53, 47]] {
                let original = Arc::new(values);
                originals.push(Arc::downgrade(&original));
                let (pool, mut request) = replacement_request(IndexSource { original: Some(original) }).await;
                let (_, role) = request.source_and_role();
                original_roles.push(ptr::from_ref(role));
                pools.push(pool);
                requests.push(request);
            }
            let mut later_request = requests.pop();
            let (abort_notice, abort_received) = oneshot::channel();
            let mut continuations = VecDeque::from([
                IndexContinuation::ObserveAbort(abort_notice),
                IndexContinuation::Complete,
                IndexContinuation::Complete,
            ]);
            let mut releases = Vec::new();
            let mut starts = Vec::new();
            let mut submissions = Vec::new();
            for continuation in continuations.drain(..) {
                let (started, start) = oneshot::channel();
                let (submitted, submission) = oneshot::channel();
                let (release, gate) = mpsc::channel();
                work.operations.push_back((IndexOperation::Blocking { started, submitted, release: gate }, continuation));
                releases.push(release);
                starts.push(start);
                submissions.push(submission);
            }
            let addresses = ActorSpace::new();
            let mut actor = spawn_owned_with(
                addresses.clone(), Config::new(4), MailAddr(119), IndexDepot { requests, started: Vec::new() },
                move |control, reports, timers, observations| {
                    drop((reports, timers, observations));
                    IndexDepotCapabilities { work: Some(work), received_work: None, work_controls: Vec::new(), work_failures: Vec::new(), control }
                },
            ).await.unwrap_or_else(|_| panic!("the real actor admits the complete original preparation lane"));
            actor.acknowledge_binding();
            let first_submission = (&mut submissions[0]).await;
            let second_submission = (&mut submissions[1]).await;
            let first_running = (&mut starts[0]).await;
            let queued_second = starts[1].try_recv();
            let initial_later_submission = submissions[2].try_recv();
            let occupied = admission.available_permits();
            let saturated_original = originals[2].strong_count();
            // This is the actual native abort permission for the already-started
            // blocking task. Its original JoinHandle remains in the owning row.
            let abort = abort_received.await.expect("the outside observer acquires the original blocking abort permission");
            let aborted_task = abort.id();
            abort.abort();
            let held_after_abort = admission.available_permits();
            drop(abort);
            let first_release = releases.remove(0).send(());
            let second_running = (&mut starts[1]).await;
            let completed_permits = admission.available_permits();
            // The finite capacity inversion may already have admitted the third
            // input. Do not consume a nonexistent fourth operation in that case.
            // Its truthful failure is checked only after all original joins.
            let later_admission = match &initial_later_submission {
                Err(oneshot::error::TryRecvError::Empty) => {
                    let request = later_request.take().expect("the untouched later request remains with its caller until capacity returns");
                    Some(actor.control.send(IndexDepotEvent::Requested(request)))
                }
                Ok(_) | Err(oneshot::error::TryRecvError::Closed) => None,
            };
            let later_submission = match &initial_later_submission {
                Err(oneshot::error::TryRecvError::Empty) => Some((&mut submissions[2]).await),
                Ok(id) => Some(Ok(*id)),
                Err(oneshot::error::TryRecvError::Closed) => None,
            };
            let queued_later = starts[2].try_recv();
            let resumed_permits = admission.available_permits();
            let endpoint = actor.actor;
            drop(actor.control);
            let mut task = Some(actor.task);
            let mut received = None;
            let mut termination_notification = None;
            let mut cleanup = Vec::new();
            {
                let attempt = OwnedTask::receive_retirement(&mut task, &mut received, &mut termination_notification);
                let mut attempt = pin!(attempt);
                let poll = poll_fn(|context| Poll::Ready(attempt.as_mut().poll(context))).await;
                cleanup.push(match poll { Poll::Pending => IndexCleanup::Waiting, Poll::Ready(()) => IndexCleanup::Returned });
            }
            let second_release = releases.remove(0).send(());
            let later_running = (&mut starts[2]).await;
            {
                let attempt = OwnedTask::receive_retirement(&mut task, &mut received, &mut termination_notification);
                let mut attempt = pin!(attempt);
                let poll = poll_fn(|context| Poll::Ready(attempt.as_mut().poll(context))).await;
                cleanup.push(match poll { Poll::Pending => IndexCleanup::Waiting, Poll::Ready(()) => IndexCleanup::Returned });
            }
            let later_release = releases.remove(0).send(());
            OwnedTask::receive_retirement(&mut task, &mut received, &mut termination_notification).await;
            let joined = received.expect("the outside receiver acquires the actual whole actor result");
            let notification = termination_notification.expect("the outside receiver acquires the original notification result");
            notification.expect("the ordinary termination publication succeeded");
            let termination = endpoint.termination().await;
            let retired_endpoint = addresses.resolve(&MailAddr(119));
            drop(later_request);
            (admission, pools, originals, original_roles, first_submission, second_submission, first_running,
                queued_second, initial_later_submission, occupied, saturated_original, aborted_task, held_after_abort,
                first_release, second_running, completed_permits, later_admission, later_submission, queued_later,
                resumed_permits, second_release, later_running, later_release, cleanup, task, joined, termination, retired_endpoint)
        });
        drop(runtime);
        let (
            admission,
            mut pools,
            originals,
            original_roles,
            first_submission,
            second_submission,
            first_running,
            queued_second,
            initial_later_submission,
            occupied,
            saturated_original,
            aborted_task,
            held_after_abort,
            first_release,
            second_running,
            completed_permits,
            later_admission,
            later_submission,
            queued_later,
            resumed_permits,
            second_release,
            later_running,
            later_release,
            cleanup,
            task,
            joined,
            termination,
            retired_endpoint,
        ) = observed;
        let joined = joined.unwrap_or_else(|_| {
            panic!("the genuine actor retirement joins without native task loss")
        });
        let ActorExecutionOutcome::Completed {
            behavior:
                IndexDepot {
                    requests,
                    mut started,
                },
            additional_failures,
            completion:
                EngineCompletion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                    OwnerCancellation,
                )),
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    ingress,
                    activation_tasks,
                    descendants:
                        ((joined_work, mut rejected, operations), work_controls, work_failures),
                    capability_failures,
                    terminal_report,
                    retirement_failures,
                    unread_owner_cancellation,
                },
        } = joined
        else {
            panic!(
                "actual owner cancellation preserves the complete actor and native work residual"
            );
        };
        assert!(
            matches!(
                initial_later_submission,
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "two real admitted operations saturate the actor's existing typed work port before the third input starts"
        );
        assert_eq!(occupied, 0);
        assert_eq!(
            saturated_original, 1,
            "the integrated saturated rejection retains its original input owner"
        );
        let first_submission =
            first_submission.expect("the first actual native task was submitted");
        let second_submission =
            second_submission.expect("the second actual native task was queued");
        let Some(Ok(later_submission)) = later_submission else {
            panic!(
                "actual completion admits the caller's later original request through the actor"
            );
        };
        assert_ne!(first_submission, second_submission);
        assert_ne!(first_submission, later_submission);
        assert_ne!(second_submission, later_submission);
        assert_eq!(aborted_task, first_submission);
        assert_eq!(held_after_abort, 0);
        assert!(first_running.is_ok());
        assert!(matches!(
            queued_second,
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(second_running.is_ok());
        assert_eq!(completed_permits, 1);
        assert!(matches!(later_admission, Some(Ok(()))));
        assert!(matches!(
            queued_later,
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(resumed_permits, 0);
        assert!(later_running.is_ok());
        assert!(first_release.is_ok());
        assert!(second_release.is_ok());
        assert!(later_release.is_ok());
        assert_eq!(cleanup, [IndexCleanup::Waiting, IndexCleanup::Waiting]);
        assert!(task.is_none());
        assert_eq!(admission.available_permits(), 2);
        assert!(admission.is_closed());
        assert!(requests.is_empty());
        assert!(additional_failures.is_empty());
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(settlements.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        // Preserve the genuine later start from whichever factual retirement
        // lane owns it. Cancellation does not promise it was already folded.
        let folded_receipts = started.len();
        let drained_receipts = ingress.control.len();
        for event in ingress.control {
            let IndexDepotEvent::Started(SettledItem::Attempted(ItemSettlement::Accepted(receipt))) =
                event
            else {
                panic!(
                    "only an actual complete late source-start receipt can remain in the drained actor control lane"
                );
            };
            started.push(receipt);
        }
        let acquired_receipts = match acquired_ingress {
            None => 0,
            Some(ControlFlow::Continue(Some(IndexDepotEvent::Started(
                SettledItem::Attempted(ItemSettlement::Accepted(receipt)),
            )))) => {
                started.push(receipt);
                1
            }
            Some(_) => {
                panic!("an acquired retirement ingress must retain its actual late start receipt")
            }
        };
        assert!(matches!(
            (folded_receipts, drained_receipts, acquired_receipts),
            (4, 0, 0) | (3, 1, 0) | (3, 0, 1)
        ));
        assert_eq!(started.len(), 4);
        assert_eq!(ingress.user, [] as [User<MailAddr, Never>; 0]);
        assert!(activation_tasks.is_empty());
        assert!(operations.is_empty());
        assert_eq!(work_controls, [] as [Never; 0]);
        assert!(work_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(retired_endpoint.is_none());
        assert_eq!(termination, Err(Crash::Cancelled));
        for (pool, receipt) in pools.iter_mut().zip(started) {
            let actions = pool
                .transition(FifoEvent::WorkerPreparationStarted(SettledItem::Attempted(
                    ItemSettlement::Accepted(receipt),
                )))
                .unwrap_or_else(|_| {
                    panic!("each actual actor receipt retains its original FIFO correlation")
                });
            index_lanes(&actions.sends);
            assert!(actions.creates.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }
        assert_eq!(rejected.len(), 1);
        let returned = rejected
            .pop()
            .expect("the third exact input has its actual late source rejection");
        let mut rejected_pool = pools.remove(2);
        let actions = rejected_pool.on(returned).unwrap_or_else(|_| {
            panic!("the actor's actual rejected input reaches its owning FIFO policy")
        });
        assert!(matches!(actions.become_, Step::Continue));
        assert!(actions.creates.is_empty());
        assert!(actions.sends.worker_observations.is_empty());
        assert!(actions.sends.worker_initializations.is_empty());
        assert!(actions.sends.worker_activations.is_empty());
        assert!(actions.sends.customer_outcomes.as_slice().is_empty());
        assert!(actions.sends.worker_assignments.is_empty());
        assert!(actions.sends.worker_preparations.is_empty());
        assert!(actions.sends.restart_schedules.is_empty());
        assert!(actions.sends.worker_shutdowns.is_empty());
        assert_eq!(actions.sends.diagnostics.len(), 1);
        let diagnostic = actions
            .sends
            .diagnostics
            .into_requests()
            .pop()
            .expect("one complete owning source rejection diagnostic");
        let diagnostic = match diagnostic {
            DiagnosticAction::Terminal { diagnostic } => diagnostic,
            DiagnosticAction::Deliver { route, .. } => match route {},
        };
        let Ok((role, previous, stopped, returned_source, reason)) =
            diagnostic.into_source_rejection()
        else {
            panic!("the actual rejection remains consuming and fully typed");
        };
        let IndexRejection::Saturated(recovered) = reason else {
            panic!("two actual occupied permits retain Saturated rather than Closed");
        };
        assert_eq!(*role, SearchRole::Primary);
        assert_eq!(Arc::as_ptr(&role), original_roles[2]);
        assert!(returned_source.is_none());
        assert_eq!(
            Arc::as_ptr(&recovered),
            originals[2].as_ptr(),
            "the integrated rejected operation returns its exact original allocation"
        );
        assert_eq!(recovered.as_slice(), [41, 31, 37]);
        assert_eq!(originals[2].strong_count(), 1);
        drop((
            role,
            previous,
            stopped,
            returned_source,
            recovered,
            rejected_pool,
        ));
        assert_eq!(originals[2].strong_count(), 0);
        assert_eq!(joined_work.len(), 3);
        for ((mut pool, (returned, result)), (original, expected)) in
            pools.into_iter().zip(joined_work).zip(
                [&originals[0], &originals[1], &originals[3]]
                    .into_iter()
                    .zip([
                        (vec![7, 3, 5], vec![3, 5, 7]),
                        (vec![23, 19, 17], vec![17, 19, 23]),
                        (vec![59, 53, 47], vec![47, 53, 59]),
                    ]),
            )
        {
            let (received, sorted) = result.expect("every original blocking handle, including the actually aborted started task, joins normally");
            assert_eq!(Arc::as_ptr(&received), original.as_ptr());
            assert_eq!(received.as_slice(), expected.0.as_slice());
            assert_eq!(sorted, expected.1);
            let actions = pool
                .on(returned)
                .unwrap_or_else(|_| panic!("each joined actor operation returns to its own FIFO"));
            assert!(matches!(actions.become_, Step::Continue));
            assert!(actions.sends.worker_initializations.is_empty());
            assert!(actions.sends.worker_activations.is_empty());
            assert!(actions.sends.customer_outcomes.as_slice().is_empty());
            assert!(actions.sends.worker_assignments.is_empty());
            assert!(actions.sends.worker_preparations.is_empty());
            assert!(actions.sends.restart_schedules.is_empty());
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert!(actions.sends.diagnostics.is_empty());
            let observations = actions.sends.worker_observations.into_requests();
            assert_eq!(observations.len(), 1);
            assert_eq!(actions.creates.len(), 1);
            let creation = actions
                .creates
                .into_iter()
                .next()
                .expect("one genuine correlated replacement creation");
            assert_eq!(creation.id(), observations[0].child);
            drop((creation, observations, received, sorted, pool));
        }
        assert_eq!(
            originals.iter().map(Weak::strong_count).collect::<Vec<_>>(),
            [0, 0, 0, 0]
        );
    }
}
