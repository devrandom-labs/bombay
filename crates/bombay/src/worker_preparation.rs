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
    async fn prepare_first(
        &mut self,
        _: &Role,
    ) -> WorkerPreparationStart<Worker, Plan, Never, Never> {
        match *self {}
    }

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

    use super::{WorkerPreparationSource, WorkerPreparationStart, settle_worker_preparation};

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

    enum WorkshopSelection {
        Submit,
        RejectWorker,
        RejectSource,
        AwaitRelease {
            started: Option<oneshot::Sender<()>>,
            release: oneshot::Receiver<()>,
        },
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkerRejection {
        Unavailable,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum WorkshopRejection {
        Closed,
    }

    struct SearchWorkshop {
        selection: WorkshopSelection,
        roles: Arc<Mutex<Vec<SearchRole>>>,
    }

    impl WorkerSource<SearchRole, SearchWorker, ImmediateActivation> for SearchWorkshop {
        type WorkerRejection = WorkerRejection;
        type SourceRejection = WorkshopRejection;
    }

    impl WorkerPreparationSource<SearchRole, SearchWorker, ImmediateActivation> for SearchWorkshop {
        async fn prepare_first(
            &mut self,
            role: &SearchRole,
        ) -> WorkerPreparationStart<
            SearchWorker,
            ImmediateActivation,
            WorkerRejection,
            WorkshopRejection,
        > {
            self.roles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(*role);
            match &mut self.selection {
                WorkshopSelection::Submit => {
                    WorkerPreparationStart::Submitted(WorkerSubmission::immediate(SearchWorker))
                }
                WorkshopSelection::RejectWorker => {
                    WorkerPreparationStart::WorkerRejected(WorkerRejection::Unavailable)
                }
                WorkshopSelection::RejectSource => {
                    WorkerPreparationStart::SourceRejected(WorkshopRejection::Closed)
                }
                WorkshopSelection::AwaitRelease { started, release } => {
                    started
                        .take()
                        .expect("preparation begins once")
                        .send(())
                        .unwrap_or_else(|()| panic!("the supervisor awaits preparation"));
                    release.await.expect("the test releases worker preparation");
                    WorkerPreparationStart::Submitted(WorkerSubmission::immediate(SearchWorker))
                }
            }
        }

        async fn prepare_next(
            &mut self,
            role: &SearchRole,
        ) -> Result<WorkerSubmission<SearchWorker, ImmediateActivation>, WorkerRejection> {
            self.roles
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(*role);
            Ok(WorkerSubmission::immediate(SearchWorker))
        }
    }

    type SearchPool = FifoPool<
        SearchRole,
        SearchWorker,
        ImmediateActivation,
        SearchWorkshop,
        Infallible,
        u8,
        u16,
    >;

    async fn replacement_request(
        workshop: SearchWorkshop,
    ) -> (
        Active<SearchPool>,
        PrepareWorkers<SearchWorkshop, SearchRole, SearchWorker, ImmediateActivation>,
    ) {
        let roles =
            OrderedRoles::new(SearchRole::Primary, []).expect("one role is a valid worker roster");
        let pool = fifo(
            |_: &SearchRole| Ok::<_, Never>(WorkerSubmission::immediate(SearchWorker)),
            roles,
            ActivationPolicy::new(1).expect("one activation is valid capacity"),
            PoolRecovery::permanent(
                workshop,
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
        let initialized = pool.initialize().expect("the pool initializes");
        let mut pool = initialized.behavior;
        let creation = initialized
            .actions
            .creates
            .into_iter()
            .next()
            .expect("initialization creates one direct worker");
        let (worker, _, kind) = creation.into_parts();
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
        let committed = pool.on(settled).expect("the direct worker is established");
        let initialization = committed
            .sends
            .worker_initializations
            .into_requests()
            .pop()
            .expect("the new worker awaits initialization");
        let worker = initialization.worker().creation();
        let initialized_worker = pool
            .on(initialization.resolve(WorkerInitializationOutcome::ReadyForActivation))
            .expect("the worker initializes");
        let activation = initialized_worker
            .sends
            .worker_activations
            .into_requests()
            .pop()
            .expect("the initialized worker begins activation");
        let started = activation.started();
        let activated = pool.on(started).expect("the activation start is admitted");
        assert!(activated.sends.worker_assignments.is_empty());
        let ready = activation.activate().await;
        let activated = pool.on(ready).expect("the worker becomes ready");
        assert!(activated.sends.worker_assignments.is_empty());
        let stopped = pool
            .on(ChildStopped::new(worker, Ok(Exit::Normal), Instant::now()))
            .expect("the worker stop starts replacement preparation");
        let request = stopped
            .sends
            .worker_preparations
            .into_items()
            .pop()
            .expect("permanent recovery emits worker preparation");
        (pool, request)
    }

    #[tokio::test]
    async fn accepted_source_prepares_the_exact_replacement_role() {
        let roles = Arc::new(Mutex::new(Vec::new()));
        let workshop = SearchWorkshop {
            selection: WorkshopSelection::Submit,
            roles: Arc::clone(&roles),
        };
        let (mut pool, request) = replacement_request(workshop).await;
        let starting = start_preparation!(pool, request);
        let preparation = settle_worker_preparation(starting).await;
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
        let workshop = SearchWorkshop {
            selection: WorkshopSelection::RejectSource,
            roles: Arc::clone(&roles),
        };
        let (mut pool, request) = replacement_request(workshop).await;
        let starting = start_preparation!(pool, request);
        let preparation = settle_worker_preparation(starting).await;
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
        let workshop = SearchWorkshop {
            selection: WorkshopSelection::RejectWorker,
            roles: Arc::clone(&roles),
        };
        let (mut pool, request) = replacement_request(workshop).await;
        let starting = start_preparation!(pool, request);
        let preparation = settle_worker_preparation(starting).await;
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
            selection: WorkshopSelection::Submit,
            roles: Arc::clone(&roles),
        };
        let (mut pool, mut request) = replacement_request(workshop).await;
        let (source, role) = request.source_and_role();
        assert_eq!(*role, SearchRole::Primary);
        assert!(Arc::ptr_eq(&source.roles, &roles));

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
        let workshop = SearchWorkshop {
            selection: WorkshopSelection::AwaitRelease {
                started: Some(started),
                release,
            },
            roles: Arc::clone(&roles),
        };
        let (mut pool, request) = replacement_request(workshop).await;
        let starting = start_preparation!(pool, request);
        let preparation = tokio::spawn(settle_worker_preparation(starting));
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
}
