//! Capabilities and resources owned by one actor incarnation.

use core::future::{Future, pending};

use behavior::{Address, Behavior, CreationResolved, Delivery, TimerElapsed, TimerId};
use bombay_timers::TimerQueue;
use tokio::time::{Instant, sleep_until};

use super::ChildScope;
#[cfg(test)]
use super::ScheduleAfterError;
use crate::DeliveryRouter;

struct MonitorTask(tokio::task::JoinHandle<()>);

impl MonitorTask {
    fn spawn(future: impl Future<Output = ()> + Send + 'static) -> Self {
        Self(tokio::spawn(future))
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl Drop for MonitorTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Marker used by a root incarnation, which has no parent event lane.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct NoParent;

/// Exact parent event capability inherited by one child incarnation.
#[doc(hidden)]
pub struct ParentReporter<A: Address, S> {
    pub(super) nonce: A::Nonce,
    pub(super) response: S,
}

impl<A: Address, S> ParentReporter<A, S> {
    pub(crate) const fn new(nonce: A::Nonce, response: S) -> Self {
        Self { nonce, response }
    }
}

/// Typed product of all effect capabilities owned by one actor generation.
#[doc(hidden)]
/// One recorded same-action creation result and its consumption state.
enum CreationSlot<N> {
    /// Committed and not yet observed through the creation lane.
    Pending(CreationResolved<N>),
    /// Already delivered exactly once; still visible for lane-order pairing.
    Consumed(CreationResolved<N>),
}

impl<N> CreationSlot<N> {
    fn resolved(&self) -> &CreationResolved<N> {
        match self {
            Self::Pending(resolved) | Self::Consumed(resolved) => resolved,
        }
    }
}

#[doc(hidden)]
pub struct IncarnationEffects<R, N, L, S, P, PA> {
    pub(super) router: R,
    pub(super) timers: TimerQueue<Instant, TimerId, TimerElapsed>,
    pub(super) children: ChildScope<N, L>,
    creations: Vec<(N, CreationSlot<N>)>,
    pub(super) response: S,
    pub(super) parent: P,
    monitors: Vec<MonitorTask>,
    peer_monitors: Vec<(PA, MonitorTask)>,
}

impl<R, N, L, S, P, PA> IncarnationEffects<R, N, L, S, P, PA> {
    pub(crate) fn new(router: R, response: S, parent: P) -> Self {
        Self {
            router,
            timers: TimerQueue::new(),
            children: ChildScope::new(),
            creations: Vec::new(),
            response,
            parent,
            monitors: Vec::new(),
            peer_monitors: Vec::new(),
        }
    }

    pub(crate) fn response(&self) -> S
    where
        S: Clone,
    {
        self.response.clone()
    }

    pub(crate) async fn next_timer(&mut self) -> TimerElapsed {
        loop {
            if let Some(expired) = self.timers.pop_due(Instant::now()) {
                return expired.value;
            }
            let Some(deadline) = self.timers.next_deadline() else {
                pending::<()>().await;
                unreachable!("a pending future cannot complete");
            };
            sleep_until(deadline).await;
            if let Some(expired) = self.timers.pop_due(Instant::now()) {
                return expired.value;
            }
        }
    }

    pub(crate) fn reserve_child(&mut self, nonce: N)
    where
        N: Copy + Eq,
    {
        self.children.reserve(nonce);
    }

    pub(crate) fn install_child(&mut self, nonce: N, child: L)
    where
        N: Copy + Eq,
    {
        self.children.install(nonce, child);
    }

    pub(crate) fn child_nonce_is_fresh(&self, nonce: N) -> bool
    where
        N: Copy + Eq,
    {
        self.children.child_nonce_is_fresh(nonce)
    }

    /// Discard the previous transition's committed creation results.
    pub(crate) fn begin_creation_resolution(&mut self) {
        self.creations.clear();
    }

    /// Record one committed creation for same-action observation.
    pub(crate) fn record_creation(&mut self, resolved: CreationResolved<N>)
    where
        N: Copy + Eq,
    {
        self.creations
            .push((resolved.nonce, CreationSlot::Pending(resolved)));
    }

    /// Take the committed result of one same-action creation exactly once.
    pub(crate) fn take_creation(
        &mut self,
        nonce: N,
    ) -> Result<CreationResolved<N>, super::CreationObservationError<N>>
    where
        N: Copy + Eq,
    {
        let index = self
            .creations
            .iter()
            .position(|(candidate, slot)| {
                *candidate == nonce && matches!(slot, CreationSlot::Pending(_))
            })
            .ok_or(super::CreationObservationError::Unknown(nonce))?;
        let CreationSlot::Pending(resolved) = &self.creations[index].1 else {
            unreachable!("position only selects pending slots");
        };
        let resolved = *resolved;
        self.creations[index].1 = CreationSlot::Consumed(resolved);
        Ok(resolved)
    }

    /// Whether the current transition committed a rejection at `nonce`.
    ///
    /// Consumed rejections remain visible here: an `ObserveChild` paired with
    /// the same attempt must stay inert regardless of lane routing order.
    pub(crate) fn creation_was_rejected(&self, nonce: N) -> bool
    where
        N: Copy + Eq,
    {
        self.creations
            .iter()
            .any(|(candidate, slot)| *candidate == nonce && slot.resolved().result.is_err())
    }

    /// Cancel a child reservation whose creation failed before installation.
    pub(crate) fn cancel_child_reservation(&mut self, nonce: N)
    where
        N: Copy + Eq,
    {
        self.children.cancel_reservation(nonce);
    }

    pub(super) fn monitor(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        self.monitors.retain(|task| !task.is_finished());
        self.monitors.push(MonitorTask::spawn(future));
    }

    /// Install one peer-observation monitor under its exact peer identity.
    pub(super) fn monitor_peer(
        &mut self,
        peer: PA,
        future: impl Future<Output = ()> + Send + 'static,
    ) {
        self.peer_monitors.retain(|(_, task)| !task.is_finished());
        self.peer_monitors.push((peer, MonitorTask::spawn(future)));
    }

    /// Cancel the current observation relationship with `peer`, if present.
    ///
    /// Only the most recently installed monitor for this exact peer is
    /// cancelled; other peers and earlier generations keep their monitors.
    /// A request for a relationship that is no longer present is inert.
    pub(super) fn unwatch_peer(&mut self, peer: &PA)
    where
        PA: PartialEq,
    {
        if let Some(index) = self
            .peer_monitors
            .iter()
            .rposition(|(candidate, _)| candidate == peer)
        {
            self.peer_monitors.remove(index);
        }
    }

    pub(crate) async fn retire_children(&mut self)
    where
        L: super::CoordinatedChild,
    {
        self.children.retire().await;
    }
}

impl<B, R, N, L, S, P, PA> DeliveryRouter<B> for IncarnationEffects<R, N, L, S, P, PA>
where
    B: Behavior,
    B::Addr: Send,
    <B::Addr as Address>::Nonce: Send,
    B::Msg: Send,
    R: DeliveryRouter<B> + Send + Sync,
    N: Sync,
    L: Sync,
    S: Sync,
    P: Sync,
    PA: Sync,
{
    type Error = R::Error;

    async fn deliver(&self, from: B::Addr, delivery: Delivery<B>) -> Result<(), Self::Error> {
        <R as DeliveryRouter<B>>::deliver(&self.router, from, delivery).await
    }
}
#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use behavior::{
        Crash, Exit, MailAddr, Never, ObservePeer, PeerStopped, ReportWorkerStopped, RestartDenial,
        ScheduleAfter, ScheduleAt, ServiceSends, SupervisionEvent, SupervisionFailureReason,
        TimerGeneration, TimerId, UnwatchPeer, User, WatchEvent, WorkerStopped,
    };
    use std::time::{Duration, Instant};
    use tokio::time::advance;

    use crate::runtime::classify_task;
    use crate::{
        AddressRouter, EndpointRegistry, EventSender, IncarnationEndpoint, PeerObservationError,
        RouteSends, RunError, RunExit, TaskOutcome,
    };

    type RecordedEvent = SupervisionEvent<User<MailAddr, Never>>;

    struct PeerState;

    #[behavior::behavior(addr = MailAddr, message = Never, sends = Vec<Never>, births = behavior::NoBirths, error = Never)]
    impl PeerState {
        fn receive(
            &mut self,
            _from: MailAddr,
            message: Never,
        ) -> behavior::Acted<MailAddr, Never, Vec<Never>, behavior::NoBirths, Never> {
            match message {}
        }
    }

    type PeerBehavior = PeerState;

    #[derive(Clone, Default)]
    struct RecordingEvents(Arc<Mutex<Vec<RecordedEvent>>>);

    impl EventSender for RecordingEvents {
        type Event = RecordedEvent;
        type Error = Infallible;

        async fn send(&self, event: RecordedEvent) -> Result<(), Self::Error> {
            self.0.lock().expect("event record lock").push(event);
            Ok(())
        }
    }

    type RecordedPeerEvent = WatchEvent<User<MailAddr, Never>>;

    #[derive(Clone, Default)]
    struct RecordingPeerEvents(Arc<Mutex<Vec<RecordedPeerEvent>>>);

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl EventSender for RecordingPeerEvents {
        type Event = RecordedPeerEvent;
        type Error = Infallible;

        async fn send(&self, event: RecordedPeerEvent) -> Result<(), Self::Error> {
            self.0.lock().expect("peer event record lock").push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn monitor_reaps_only_finished_tasks_before_installing_the_next() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        services.monitor(async {});
        tokio::task::yield_now().await;
        assert!(services.monitors[0].is_finished());

        services.monitor(core::future::pending());
        assert_eq!(services.monitors.len(), 1);
        assert!(!services.monitors[0].is_finished());
    }

    #[tokio::test]
    async fn unknown_peer_fails_before_a_monitor_can_emit() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );

        assert_eq!(
            ServiceSends::one(ObservePeer { peer: MailAddr(9) })
                .route(MailAddr(7), &mut services)
                .await,
            Err(PeerObservationError::Unknown(MailAddr(9)))
        );
        tokio::task::yield_now().await;
        assert!(events.0.lock().expect("peer event record lock").is_empty());
    }

    #[tokio::test]
    async fn dropping_incarnation_cancels_its_pending_peer_monitors() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let peer_space = observe::ObservationSpace::new();
        let mut peer_subject = peer_space.subject(()).expect("fresh peer subject");
        let endpoint = IncarnationEndpoint::new((), peer_space);
        let _lease =
            EndpointRegistry::<PeerBehavior, _>::register(&router, MailAddr(9), endpoint).unwrap();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );
        ServiceSends::one(ObservePeer { peer: MailAddr(9) })
            .route(MailAddr(7), &mut services)
            .await
            .unwrap();

        drop(services);
        peer_subject.complete(Ok(Exit::Normal));
        tokio::task::yield_now().await;
        assert!(events.0.lock().expect("peer event record lock").is_empty());
    }

    #[tokio::test]
    async fn zero_relative_timer_is_ready_without_a_timer_driver_turn() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        ServiceSends::one(ScheduleAfter {
            id: TimerId(6),
            generation: TimerGeneration(0),
            after: Duration::ZERO,
        })
        .route(MailAddr(1), &mut services)
        .await
        .unwrap();

        let next = services.next_timer();
        tokio::pin!(next);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(next.as_mut().poll(&mut context), Poll::Ready(_)));
    }

    #[tokio::test]
    async fn dropping_parent_cancels_monitor_and_releases_observed_child() {
        type Returned = Result<RunExit<Exit<MailAddr>>, RunError<(), ()>>;
        let events = RecordingEvents::default();
        let mut services =
            super::IncarnationEffects::<_, u64, _, _, (), MailAddr>::new((), events, ());
        let drops = Arc::new(AtomicUsize::new(0));
        let completion_space = observe::ObservationSpace::<(), TaskOutcome<Returned>>::new();
        let _completion_subject = completion_space.subject(()).expect("fresh completion");
        let completion = completion_space
            .observe(&())
            .expect("registered completion");
        let task = tokio::spawn(core::future::pending());
        let control = task.abort_handle();
        let child_completion = crate::runtime::Completion::new(task, completion);
        services.reserve_child(7);
        services.install_child(
            7,
            crate::ChildLease::new(DropProbe(drops.clone()), control, child_completion),
        );
        ServiceSends::one(behavior::ObserveChild { nonce: 7 })
            .route(MailAddr(1), &mut services)
            .await
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(services);
        tokio::task::yield_now().await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn terminal_classification_is_total_and_domain_exact() {
        type Returned = Result<RunExit<Exit<MailAddr>>, RunError<(), ()>>;

        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Ok(RunExit::Stopped(
                Exit::Normal
            ),))),
            Ok(Exit::Normal)
        );
        let supervision_failure = Exit::SupervisionFailed(SupervisionFailureReason::RestartDenied(
            RestartDenial::BudgetExceeded {
                restarts_in_window: 1,
                replacements_requested: 2,
                maximum_restarts: 2,
            },
        ));
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Ok(RunExit::Stopped(
                supervision_failure,
            )))),
            Ok(supervision_failure)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Ok(
                RunExit::EnvironmentClosed,
            ))),
            Ok(Exit::Collected)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Err(
                RunError::Behavior(()),
            ))),
            Err(Crash::Failed)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Err(
                RunError::Environment(()),
            ))),
            Err(Crash::EnvironmentFailed)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Returned(Err(RunError::Poisoned,))),
            Err(Crash::Panicked)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Panicked),
            Err(Crash::Panicked)
        );
        assert_eq!(
            classify_task::<MailAddr, Returned>(&TaskOutcome::Cancelled),
            Err(Crash::Cancelled)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_generation_replaces_the_same_timer_identity() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        let now = Instant::now();
        ServiceSends::one(ScheduleAt {
            id: TimerId(7),
            generation: TimerGeneration(0),
            at: now + Duration::from_secs(1),
        })
        .route(MailAddr(1), &mut services)
        .await
        .unwrap();
        ServiceSends::one(ScheduleAt {
            id: TimerId(7),
            generation: TimerGeneration(1),
            at: now + Duration::from_secs(2),
        })
        .route(MailAddr(1), &mut services)
        .await
        .unwrap();

        let next = services.next_timer();
        tokio::pin!(next);
        advance(Duration::from_secs(1)).await;
        advance(Duration::from_secs(1)).await;
        assert_eq!(next.await.generation, TimerGeneration(1));
    }

    #[tokio::test(start_paused = true)]
    async fn relative_schedule_is_anchored_when_the_effect_is_interpreted() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        ServiceSends::one(ScheduleAfter {
            id: TimerId(8),
            generation: TimerGeneration(3),
            after: Duration::from_secs(2),
        })
        .route(MailAddr(1), &mut services)
        .await
        .unwrap();

        let next = services.next_timer();
        tokio::pin!(next);
        advance(Duration::from_secs(1)).await;
        tokio::select! {
            value = &mut next => panic!("relative timer fired early: {value:?}"),
            () = core::future::ready(()) => {}
        }
        advance(Duration::from_secs(1)).await;
        assert_eq!(
            next.await,
            behavior::TimerElapsed {
                id: TimerId(8),
                generation: TimerGeneration(3),
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unrepresentable_relative_deadline_is_a_typed_interpreter_error() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        let result = ServiceSends::one(ScheduleAfter {
            id: TimerId(9),
            generation: TimerGeneration(0),
            after: Duration::MAX,
        })
        .route(MailAddr(1), &mut services)
        .await;

        assert_eq!(result, Err(super::ScheduleAfterError));
    }

    #[tokio::test]
    async fn worker_report_is_stamped_with_the_emitting_child_nonce() {
        let events = RecordingEvents::default();
        let at = Instant::now();
        let reporter = super::ParentReporter::new(7, events.clone());
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), _, MailAddr>::new((), (), reporter);

        ServiceSends::one(ReportWorkerStopped {
            worker: 3,
            outcome: Err(Crash::Panicked),
            at,
        })
        .route(MailAddr(11), &mut services)
        .await
        .unwrap();

        let recorded = events.0.lock().expect("event record lock");
        assert!(matches!(
            &recorded[..],
            [SupervisionEvent::WorkerStopped(WorkerStopped {
                proxy: 7,
                worker: 3,
                outcome: Err(Crash::Panicked),
                at: recorded_at,
            })] if *recorded_at == at
        ));
    }

    #[tokio::test]
    async fn creation_observation_returns_the_committed_result_exactly_once() {
        let events = RecordingEvents::default();
        let mut services =
            super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new((), events.clone(), ());

        services.begin_creation_resolution();
        services.record_creation(behavior::CreationResolved::new(
            7,
            behavior::CreationKind::replacement_of(3),
            Ok(()),
        ));

        ServiceSends::one(behavior::ObserveCreation::new(7))
            .route(MailAddr(11), &mut services)
            .await
            .unwrap();

        let committed = {
            let recorded = events.0.lock().expect("event record lock");
            recorded.as_slice()
                == [SupervisionEvent::CreationResolved(
                    behavior::CreationResolved::new(
                        7,
                        behavior::CreationKind::replacement_of(3),
                        Ok(()),
                    ),
                )]
        };
        assert!(
            committed,
            "the committed nonce, kind, and result arrive unchanged"
        );

        assert_eq!(
            ServiceSends::one(behavior::ObserveCreation::new(7))
                .route(MailAddr(11), &mut services)
                .await,
            Err(crate::runtime::CreationObservationError::Unknown(7)),
            "a committed creation result is taken exactly once"
        );
        assert_eq!(
            ServiceSends::one(behavior::ObserveCreation::new(9))
                .route(MailAddr(11), &mut services)
                .await,
            Err(crate::runtime::CreationObservationError::Unknown(9)),
            "an unstaged nonce is a typed observation failure"
        );
    }

    #[test]
    fn a_later_transition_cannot_observe_an_earlier_creations_result() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        services.record_creation(behavior::CreationResolved::birth(5));
        services.begin_creation_resolution();
        assert_eq!(
            services.take_creation(5),
            Err(crate::runtime::CreationObservationError::Unknown(5))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn named_lanes_keep_errors_exact_and_do_not_merge() {
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), (), MailAddr>::new((), (), ());
        let sends = behavior::ReceiveTimeoutSends {
            behavior: ServiceSends::one(ScheduleAt {
                id: TimerId(1),
                generation: TimerGeneration(0),
                at: Instant::now(),
            }),
            schedules: ServiceSends::one(ScheduleAfter {
                id: TimerId(9),
                generation: TimerGeneration(0),
                after: Duration::MAX,
            }),
        };

        assert_eq!(
            sends.route(MailAddr(1), &mut services).await,
            Err(crate::routing::ReceiveTimeoutSendsError::Schedules(
                super::ScheduleAfterError
            )),
            "the failing lane keeps its exact typed error"
        );
        assert!(
            services.timers.next_deadline().is_some(),
            "the independent behavior lane was still routed first"
        );
    }

    #[tokio::test]
    async fn child_observation_without_child_or_rejection_is_an_exact_error() {
        type Returned = Result<RunExit<Exit<MailAddr>>, RunError<(), ()>>;
        type Lease = crate::ChildLease<(), Returned>;
        let events = RecordingEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, Lease, _, (), MailAddr>::new(
            (),
            events.clone(),
            (),
        );
        services.record_creation(behavior::CreationResolved::rejected(
            9,
            behavior::CreationKind::Birth,
            behavior::CreationRejection::InitializationFailed,
        ));

        assert_eq!(
            ServiceSends::one(behavior::ObserveChild::new(7))
                .route(MailAddr(11), &mut services)
                .await,
            Err(crate::runtime::ChildObservationError::Unknown(7)),
            "a rejection at another nonce never makes an unbooked nonce inert"
        );
        tokio::task::yield_now().await;
        assert!(events.0.lock().expect("event record lock").is_empty());
    }

    #[tokio::test]
    async fn installed_child_is_observed_even_with_a_rejected_log_entry() {
        type Returned = Result<RunExit<Exit<MailAddr>>, RunError<(), ()>>;
        let events = RecordingEvents::default();
        let mut services =
            super::IncarnationEffects::<_, u64, _, _, (), MailAddr>::new((), events.clone(), ());
        let completion_space = observe::ObservationSpace::<(), TaskOutcome<Returned>>::new();
        let mut completion_subject = completion_space.subject(()).expect("fresh completion");
        let completion = completion_space
            .observe(&())
            .expect("registered completion");
        let gate = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn({
            let gate = gate.clone();
            async move { gate.notified().await }
        });
        let control = task.abort_handle();
        let child_completion = crate::runtime::Completion::new(task, completion);
        services.reserve_child(7);
        services.install_child(7, crate::ChildLease::new((), control, child_completion));
        services.record_creation(behavior::CreationResolved::rejected(
            7,
            behavior::CreationKind::Birth,
            behavior::CreationRejection::NonceAlreadyBound,
        ));

        ServiceSends::one(behavior::ObserveChild::new(7))
            .route(MailAddr(11), &mut services)
            .await
            .expect("an installed child is observed even with a rejected log entry");

        completion_subject.complete(TaskOutcome::Returned(Ok(RunExit::Stopped(Exit::Normal))));
        drop(completion_subject);
        gate.notify_one();
        for _ in 0..100 {
            if !events.0.lock().expect("event record lock").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(
                events.0.lock().expect("event record lock").as_slice(),
                [SupervisionEvent::ChildStopped(_)]
            ),
            "the installed child's monitor was installed, not skipped"
        );
    }

    #[tokio::test]
    async fn worker_replacement_reports_project_exact_installation_and_rejection() {
        let events = RecordingEvents::default();
        let reporter = super::ParentReporter::new(7, events.clone());
        let mut services =
            super::IncarnationEffects::<_, u64, (), (), _, MailAddr>::new((), (), reporter);

        ServiceSends::one(behavior::ReportWorkerCreationResolved::new(
            3,
            behavior::CreationKind::replacement_of(1),
            Err(behavior::CreationRejection::InitializationFailed),
        ))
        .route(MailAddr(11), &mut services)
        .await
        .unwrap();

        ServiceSends::one(behavior::ReportWorkerCreationResolved::new(
            4,
            behavior::CreationKind::replacement_of(3),
            Ok(()),
        ))
        .route(MailAddr(11), &mut services)
        .await
        .unwrap();

        let recorded = events.0.lock().expect("event record lock");
        assert!(
            recorded.as_slice()
                == [
                    SupervisionEvent::WorkerCreationResolved(
                        behavior::WorkerCreationResolved::new(
                            7,
                            3,
                            behavior::CreationKind::replacement_of(1),
                            Err(behavior::CreationRejection::InitializationFailed),
                        ),
                    ),
                    SupervisionEvent::WorkerCreationResolved(
                        behavior::WorkerCreationResolved::new(
                            7,
                            4,
                            behavior::CreationKind::replacement_of(3),
                            Ok(()),
                        ),
                    ),
                ],
            "the parent receives proxy, worker, kind, and result unchanged"
        );

        let projections = recorded
            .iter()
            .map(|event| match event {
                SupervisionEvent::WorkerCreationResolved(resolved) => {
                    (*resolved).into_replacement()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            projections,
            [
                Some(behavior::ReplacementResolution::Rejected {
                    proxy: 7,
                    replaced: 1,
                    attempt: 3,
                    rejection: behavior::CreationRejection::InitializationFailed,
                }),
                Some(behavior::ReplacementResolution::Installed {
                    proxy: 7,
                    replaced: 3,
                    replacement: 4,
                }),
            ],
            "replacement resolution uses explicit provenance and stays distinct from death"
        );
    }

    #[tokio::test]
    async fn unwatch_cancels_only_the_matching_peer_monitor() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let space_nine = observe::ObservationSpace::new();
        let mut subject_nine = space_nine.subject(()).expect("fresh peer subject");
        let space_ten = observe::ObservationSpace::new();
        let mut subject_ten = space_ten.subject(()).expect("fresh peer subject");
        let _lease_nine = EndpointRegistry::<PeerBehavior, _>::register(
            &router,
            MailAddr(9),
            IncarnationEndpoint::new((), space_nine),
        )
        .unwrap();
        let _lease_ten = EndpointRegistry::<PeerBehavior, _>::register(
            &router,
            MailAddr(10),
            IncarnationEndpoint::new((), space_ten),
        )
        .unwrap();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );

        ServiceSends::new(vec![
            ObservePeer::new(MailAddr(9)),
            ObservePeer::new(MailAddr(10)),
        ])
        .route(MailAddr(7), &mut services)
        .await
        .unwrap();
        ServiceSends::one(UnwatchPeer::new(MailAddr(9)))
            .route(MailAddr(7), &mut services)
            .await
            .unwrap();

        subject_nine.complete(Ok(Exit::Normal));
        subject_ten.complete(Err(Crash::Panicked));
        for _ in 0..100 {
            if !events.0.lock().expect("peer event record lock").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let recorded = events.0.lock().expect("peer event record lock");
        assert!(
            matches!(
                recorded.as_slice(),
                [WatchEvent::PeerStopped(PeerStopped {
                    peer: MailAddr(10),
                    outcome: Err(Crash::Panicked),
                })]
            ),
            "only the un-watched peer's monitor was cancelled: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn unwatch_is_inert_when_the_relationship_is_absent() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );

        ServiceSends::new(vec![
            UnwatchPeer::new(MailAddr(9)),
            UnwatchPeer::new(MailAddr(9)),
        ])
        .route(MailAddr(7), &mut services)
        .await
        .expect("an absent or repeated cancellation is inert");
    }

    #[tokio::test]
    async fn unwatch_keeps_the_earlier_generations_monitor() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let space_one = observe::ObservationSpace::new();
        let mut subject_one = space_one.subject(()).expect("fresh gen-one subject");
        let lease_one = EndpointRegistry::<PeerBehavior, _>::register(
            &router,
            MailAddr(9),
            IncarnationEndpoint::new((), space_one),
        )
        .unwrap();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );
        ServiceSends::one(ObservePeer::new(MailAddr(9)))
            .route(MailAddr(7), &mut services)
            .await
            .unwrap();

        drop(lease_one);
        let space_two = observe::ObservationSpace::new();
        let mut subject_two = space_two.subject(()).expect("fresh gen-two subject");
        let _lease_two = EndpointRegistry::<PeerBehavior, _>::register(
            &services.router,
            MailAddr(9),
            IncarnationEndpoint::new((), space_two),
        )
        .unwrap();
        ServiceSends::one(ObservePeer::new(MailAddr(9)))
            .route(MailAddr(7), &mut services)
            .await
            .unwrap();
        ServiceSends::one(UnwatchPeer::new(MailAddr(9)))
            .route(MailAddr(7), &mut services)
            .await
            .unwrap();

        subject_one.complete(Ok(Exit::Normal));
        subject_two.complete(Err(Crash::Panicked));
        for _ in 0..100 {
            if !events.0.lock().expect("peer event record lock").is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let recorded = events.0.lock().expect("peer event record lock");
        assert!(
            matches!(
                recorded.as_slice(),
                [WatchEvent::PeerStopped(PeerStopped {
                    peer: MailAddr(9),
                    outcome: Ok(Exit::Normal),
                })]
            ),
            "cancellation removed only the current generation's monitor: {recorded:?}"
        );
    }

    #[tokio::test]
    async fn watch_lanes_keep_observation_errors_exact_and_independent() {
        let router = AddressRouter::<MailAddr, IncarnationEndpoint<MailAddr, ()>>::default();
        let peer_space = observe::ObservationSpace::new();
        let mut peer_subject = peer_space.subject(()).expect("fresh peer subject");
        let endpoint = IncarnationEndpoint::new((), peer_space);
        let lease =
            EndpointRegistry::<PeerBehavior, _>::register(&router, MailAddr(9), endpoint).unwrap();
        let events = RecordingPeerEvents::default();
        let mut services = super::IncarnationEffects::<_, u64, (), _, (), MailAddr>::new(
            router,
            events.clone(),
            (),
        );

        let sends = behavior::WatchSends {
            behavior: ServiceSends::one(ObservePeer { peer: MailAddr(9) }),
            observations: ServiceSends::one(ObservePeer { peer: MailAddr(10) }),
        };
        assert_eq!(
            sends.route(MailAddr(7), &mut services).await,
            Err(crate::routing::WatchSendsError::Observations(
                PeerObservationError::Unknown(MailAddr(10))
            )),
            "the observation lane keeps its exact typed error"
        );

        peer_subject.complete(Ok(Exit::Normal));
        tokio::task::yield_now().await;
        assert!(
            matches!(
                events.0.lock().expect("peer event record lock").as_slice(),
                [WatchEvent::PeerStopped(_)]
            ),
            "the independent behavior lane still installed its monitor"
        );
        drop(lease);
    }
}
