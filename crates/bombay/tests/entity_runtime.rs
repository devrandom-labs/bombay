use std::any::Any;
use std::convert::Infallible;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::mem;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use bombay::entity::{
    Activated, ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, DrainStage, EntityId,
    EntityRuntime, FenceFailure, LocalEntityRuntime, Passivation, Refusal, RetirementMode,
};

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[derive(Clone)]
struct TestRuntime {
    state: Arc<TestRuntimeState>,
}

struct TestRuntimeState {
    event_gate: Mutex<()>,
    event_changed: Condvar,
    activations: AtomicUsize,
    activation_completions: AtomicUsize,
    activation_failures: AtomicUsize,
    activation_failure: Mutex<Option<()>>,
    delivery_rejection: Mutex<Option<()>>,
    delivered: Mutex<Vec<u64>>,
    activation_gate: Mutex<Option<Arc<ActivationGate>>>,
    delivery_gate: Mutex<Option<Arc<ActivationGate>>>,
    fence_gate: Mutex<Option<Arc<ActivationGate>>>,
    fence_failure: Mutex<Option<FenceFailure>>,
    fences: AtomicUsize,
    retirements: AtomicUsize,
    retirement_modes: Mutex<Vec<RetirementMode>>,
}

impl TestRuntime {
    fn new() -> Self {
        Self {
            state: Arc::new(TestRuntimeState {
                event_gate: Mutex::new(()),
                event_changed: Condvar::new(),
                activations: AtomicUsize::new(0),
                activation_completions: AtomicUsize::new(0),
                activation_failures: AtomicUsize::new(0),
                activation_failure: Mutex::new(None),
                delivery_rejection: Mutex::new(None),
                delivered: Mutex::new(Vec::new()),
                activation_gate: Mutex::new(None),
                delivery_gate: Mutex::new(None),
                fence_gate: Mutex::new(None),
                fence_failure: Mutex::new(None),
                fences: AtomicUsize::new(0),
                retirements: AtomicUsize::new(0),
                retirement_modes: Mutex::new(Vec::new()),
            }),
        }
    }
}

impl TestRuntimeState {
    fn wait_for_runtime_fact(&self, observed: impl Fn(&Self) -> bool) {
        let mut event_gate = self.event_gate.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !observed(self) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "runtime fact was not observed");
            (event_gate, _) = self
                .event_changed
                .wait_timeout(event_gate, remaining)
                .unwrap();
        }
    }
}

impl<I: Send + 'static> LocalEntityRuntime<I, u64> for TestRuntime {
    type Origin = ();
    type Endpoint = u64;
    type Lease = u64;
    type ActivationError = ();
    type Task = Option<thread::JoinHandle<()>>;
    type TaskFailure = Box<dyn Any + Send>;
    type RetirementFailure = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        Some(thread::spawn(move || block_on(task)))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        task.take()
            .expect("the exact lifecycle task is joined once")
            .join()
    }

    async fn activate(
        &self,
        _: EntityId<I>,
        activation_id: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        self.state.activations.fetch_add(1, Ordering::Relaxed);
        let gate = self.state.activation_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        {
            let _event_gate = self.state.event_gate.lock().unwrap();
            self.state
                .activation_completions
                .fetch_add(1, Ordering::Release);
            self.state.event_changed.notify_all();
        }
        let activation_failure = *self.state.activation_failure.lock().unwrap();
        match activation_failure {
            Some(error) => Err(error),
            None => Ok(Activated {
                endpoint: activation_id.get().get(),
                lease: activation_id.get().get(),
            }),
        }
    }

    fn activation_failed(&self, _: EntityId<I>, _: ActivationId, (): Self::ActivationError) {
        self.state
            .activation_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    async fn deliver(&self, _: Self::Endpoint, (): Self::Origin, command: u64) -> Result<(), u64> {
        let gate = self.state.delivery_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        let delivery_rejection = *self.state.delivery_rejection.lock().unwrap();
        if let Some(()) = delivery_rejection {
            Err(command)
        } else {
            let _event_gate = self.state.event_gate.lock().unwrap();
            self.state.delivered.lock().unwrap().push(command);
            self.state.event_changed.notify_all();
            Ok(())
        }
    }

    async fn fence(&self, _: Self::Endpoint) -> Result<(), FenceFailure> {
        let gate = self.state.fence_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        self.state.fences.fetch_add(1, Ordering::Relaxed);
        match *self.state.fence_failure.lock().unwrap() {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "The cold retirement future captures only self and retirement, not the ignored non-Sync identity."
    )]
    fn retire(
        &self,
        _: &EntityId<I>,
        _: ActivationId,
        _: Self::Lease,
        retirement: RetirementMode,
    ) -> impl Future<Output = Result<(), Self::RetirementFailure>> + Send {
        async move {
            let _event_gate = self.state.event_gate.lock().unwrap();
            self.state.retirement_modes.lock().unwrap().push(retirement);
            self.state.retirements.fetch_add(1, Ordering::Release);
            self.state.event_changed.notify_all();
            Ok(())
        }
    }
}

struct MoveOnlyCommand {
    value: String,
    drops: Arc<AtomicUsize>,
}

impl Drop for MoveOnlyCommand {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct RejectingMoveOnlyRuntime;

impl LocalEntityRuntime<u64, MoveOnlyCommand> for RejectingMoveOnlyRuntime {
    type Origin = ();
    type Endpoint = ();
    type Lease = ();
    type ActivationError = Infallible;
    type Task = Option<thread::JoinHandle<()>>;
    type TaskFailure = Box<dyn Any + Send>;
    type RetirementFailure = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        Some(thread::spawn(move || block_on(task)))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        task.take()
            .expect("the exact lifecycle task is joined once")
            .join()
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(
        &self,
        _: EntityId<u64>,
        _: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        Ok(Activated {
            endpoint: (),
            lease: (),
        })
    }

    fn activation_failed(&self, _: EntityId<u64>, _: ActivationId, error: Self::ActivationError) {
        match error {}
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn deliver(
        &self,
        (): Self::Endpoint,
        (): Self::Origin,
        command: MoveOnlyCommand,
    ) -> Result<(), MoveOnlyCommand> {
        Err(command)
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn fence(&self, (): Self::Endpoint) -> Result<(), FenceFailure> {
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Retirement completion remains a cold trait-port future."
    )]
    async fn retire(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        (): Self::Lease,
        _: RetirementMode,
    ) -> Result<(), Self::RetirementFailure> {
        Ok(())
    }
}

struct HashGate {
    state: Mutex<HashGateState>,
    changed: Condvar,
}

enum HashGatePhase {
    Counting,
    Blocked,
    Released,
}

struct HashGateState {
    blocked_thread: Option<thread::ThreadId>,
    calls: usize,
    phase: HashGatePhase,
}

impl HashGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(HashGateState {
                blocked_thread: None,
                calls: 0,
                phase: HashGatePhase::Counting,
            }),
            changed: Condvar::new(),
        })
    }

    fn block_third_hash_on_this_thread(&self) {
        self.state.lock().unwrap().blocked_thread = Some(thread::current().id());
    }

    fn wait_until_blocked(&self) {
        let mut state = self.state.lock().unwrap();
        while matches!(state.phase, HashGatePhase::Counting) {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.phase = HashGatePhase::Released;
        self.changed.notify_all();
    }
}

#[derive(Clone)]
struct GatedId {
    value: u64,
    gate: Arc<HashGate>,
}

impl PartialEq for GatedId {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for GatedId {}

impl Hash for GatedId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        let mut gate = self.gate.state.lock().unwrap();
        if gate.blocked_thread == Some(thread::current().id()) {
            gate.calls += 1;
            if gate.calls == 3 {
                gate.phase = HashGatePhase::Blocked;
                self.gate.changed.notify_all();
                while matches!(gate.phase, HashGatePhase::Blocked) {
                    gate = self.gate.changed.wait(gate).unwrap();
                }
            }
        }
    }
}

struct ActivationGate {
    phase: Mutex<ActivationGatePhase>,
}

enum ActivationGatePhase {
    Closed(Option<Waker>),
    Open,
}

impl ActivationGate {
    fn closed() -> Arc<Self> {
        Arc::new(Self {
            phase: Mutex::new(ActivationGatePhase::Closed(None)),
        })
    }

    fn wait(self: &Arc<Self>) -> GateFuture {
        GateFuture(Arc::clone(self))
    }

    fn open(&self) {
        let waker = match mem::replace(&mut *self.phase.lock().unwrap(), ActivationGatePhase::Open)
        {
            ActivationGatePhase::Closed(waker) => waker,
            ActivationGatePhase::Open => None,
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct GateFuture(Arc<ActivationGate>);

impl Future for GateFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self.0.phase.lock().unwrap() {
            ActivationGatePhase::Closed(waker) => {
                *waker = Some(context.waker().clone());
                Poll::Pending
            }
            ActivationGatePhase::Open => Poll::Ready(()),
        }
    }
}

#[test]
fn application_admission_is_one_asynchronous_operation() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();

    block_on(entities.admit((), EntityId::new(42), 7)).unwrap();

    assert_eq!(observations.state.activations.load(Ordering::Relaxed), 1);
    assert_eq!(*observations.state.delivered.lock().unwrap(), [7]);
}

#[test]
fn failed_delivery_returns_the_original_command() {
    let actor_runtime = TestRuntime::new();
    *actor_runtime.state.delivery_rejection.lock().unwrap() = Some(());
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();

    let failure = block_on(entities.admit((), EntityId::new(9), 77)).unwrap_err();

    assert!(matches!(
        failure,
        AdmissionFailure::Refused {
            command: 77,
            reason: Refusal::Unavailable,
        }
    ));
}

#[test]
fn failed_delivery_returns_one_exact_move_only_command() {
    let drops = Arc::new(AtomicUsize::new(0));
    let command = MoveOnlyCommand {
        value: String::from("owned command"),
        drops: Arc::clone(&drops),
    };
    let allocation = command.value.as_ptr();
    let entities = EntityRuntime::new(DirectoryConfig::default(), RejectingMoveOnlyRuntime)
        .unwrap_or_else(|_| panic!("valid directory configuration must construct"));

    let failure = block_on(entities.admit((), EntityId::new(9), command)).unwrap_err();
    let AdmissionFailure::Refused { command, reason } = failure else {
        panic!("delivery must preserve the rejected command");
    };

    assert_eq!(reason, Refusal::Unavailable);
    assert_eq!(command.value, "owned command");
    assert_eq!(command.value.as_ptr(), allocation);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(command);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_activation_returns_the_command_and_eventually_allows_retry() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    *actor_runtime.state.activation_failure.lock().unwrap() = Some(());
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
    let entity_id = EntityId::new(11);

    let activation_refusal = block_on(entities.admit((), entity_id, 81));
    assert!(matches!(
        activation_refusal,
        Err(AdmissionFailure::Refused {
            command: 81,
            reason: Refusal::Unavailable,
        })
    ));
    *observations.state.activation_failure.lock().unwrap() = None;
    let mut command = 82;
    for _ in 0..1_000 {
        match block_on(entities.admit((), entity_id, command)) {
            Ok(()) => break,
            Err(AdmissionFailure::Refused {
                command: returned,
                reason: Refusal::Unavailable,
            }) => {
                command = returned;
                thread::yield_now();
            }
            Err(failure) => panic!("unexpected retry failure: {failure:?}"),
        }
    }

    assert_eq!(observations.state.activations.load(Ordering::Acquire), 2);
    assert_eq!(*observations.state.delivered.lock().unwrap(), [82]);
}

#[test]
fn canceling_admission_does_not_cancel_shared_activation_or_deliver_command() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let gate = ActivationGate::closed();
    *actor_runtime.state.activation_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
    let mut admission = Box::pin(entities.admit((), EntityId::new(5), 91));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);

    let pending = admission.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    drop(admission);
    gate.open();
    observations
        .state
        .wait_for_runtime_fact(|state| state.activation_completions.load(Ordering::Acquire) == 1);

    assert_eq!(observations.state.activations.load(Ordering::Relaxed), 1);
    assert_eq!(
        observations
            .state
            .activation_completions
            .load(Ordering::Acquire),
        1
    );
    block_on(entities.admit((), EntityId::new(5), 92)).unwrap();
    assert_eq!(*observations.state.delivered.lock().unwrap(), [92]);
}

#[test]
fn dropping_active_admission_does_not_retract_owned_delivery() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime.clone()).unwrap();
    let entity_id = EntityId::new(8);
    block_on(entities.admit((), entity_id, 1)).unwrap();

    let gate = ActivationGate::closed();
    *actor_runtime.state.delivery_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let mut admission = Box::pin(entities.admit((), entity_id, 2));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let pending = admission.as_mut().poll(&mut context);
    assert!(pending.is_pending());

    drop(admission);
    gate.open();
    observations
        .state
        .wait_for_runtime_fact(|state| state.delivered.lock().unwrap().len() == 2);

    assert_eq!(*observations.state.delivered.lock().unwrap(), [1, 2]);
}

#[test]
fn passivation_reports_not_active_for_unknown_entity() {
    let entities = EntityRuntime::new(DirectoryConfig::default(), TestRuntime::new()).unwrap();

    let passivation = entities.passivate(&EntityId::new(99));
    assert_eq!(passivation, Passivation::NotActive);
}

#[test]
fn repeated_passivation_reports_already_passivating() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let fence_gate = ActivationGate::closed();
    *actor_runtime.state.fence_gate.lock().unwrap() = Some(Arc::clone(&fence_gate));
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
    let entity_id = EntityId::new(7);
    block_on(entities.admit((), entity_id, 1)).unwrap();

    let first_passivation = entities.passivate(&entity_id);
    assert_eq!(first_passivation, Passivation::Begun);
    let repeated_passivation = entities.passivate(&entity_id);
    assert_eq!(repeated_passivation, Passivation::AlreadyPassivating);

    fence_gate.open();
    observations
        .state
        .wait_for_runtime_fact(|state| state.retirements.load(Ordering::Acquire) == 1);
    assert_eq!(observations.state.fences.load(Ordering::Relaxed), 1);
    assert_eq!(observations.state.retirements.load(Ordering::Relaxed), 1);
}

#[test]
fn passivation_gate_serializes_competing_calls() {
    let gate = HashGate::new();
    let entity_id = EntityId::new(GatedId {
        value: 8,
        gate: Arc::clone(&gate),
    });
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let fence_gate = ActivationGate::closed();
    *actor_runtime.state.fence_gate.lock().unwrap() = Some(Arc::clone(&fence_gate));
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
    block_on(entities.admit((), entity_id.clone(), 1)).unwrap();

    let racing_runtime = entities.clone();
    let racing_id = entity_id.clone();
    let racing_gate = Arc::clone(&gate);
    let passivation = thread::spawn(move || {
        racing_gate.block_third_hash_on_this_thread();
        racing_runtime.passivate(&racing_id)
    });
    gate.wait_until_blocked();
    gate.release();
    let disposition = passivation.join().unwrap();
    assert_eq!(disposition, Passivation::Begun);
    let repeated_passivation = entities.passivate(&entity_id);
    assert_eq!(repeated_passivation, Passivation::AlreadyPassivating);
    fence_gate.open();
    observations
        .state
        .wait_for_runtime_fact(|state| state.retirements.load(Ordering::Acquire) == 1);
    assert_eq!(observations.state.retirements.load(Ordering::Acquire), 1);
    assert_eq!(observations.state.activations.load(Ordering::Acquire), 1);
}

#[test]
fn passivation_fences_and_retires_the_exact_incarnation() {
    let actor_runtime = TestRuntime::new();
    let observations = actor_runtime.clone();
    let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
    let entity_id = EntityId::new(6);
    block_on(entities.admit((), entity_id, 1)).unwrap();

    let passivation = entities.passivate(&entity_id);
    assert_eq!(passivation, Passivation::Begun);
    observations
        .state
        .wait_for_runtime_fact(|state| state.retirements.load(Ordering::Acquire) == 1);
    assert_eq!(observations.state.fences.load(Ordering::Relaxed), 1);
    assert_eq!(observations.state.retirements.load(Ordering::Relaxed), 1);
    assert_eq!(observations.state.activations.load(Ordering::Relaxed), 1);
}

#[test]
fn fence_failures_preserve_the_forced_retirement_stage() {
    for (failure, stage) in [
        (FenceFailure::Enqueue, DrainStage::FenceEnqueue),
        (
            FenceFailure::Acknowledgement,
            DrainStage::FenceAcknowledgement,
        ),
    ] {
        let actor_runtime = TestRuntime::new();
        let observations = actor_runtime.clone();
        *actor_runtime.state.fence_failure.lock().unwrap() = Some(failure);
        let entities = EntityRuntime::new(DirectoryConfig::default(), actor_runtime).unwrap();
        let entity_id = EntityId::new(10);
        block_on(entities.admit((), entity_id, 1)).unwrap();

        let passivation = entities.passivate(&entity_id);
        assert_eq!(passivation, Passivation::Begun);
        observations
            .state
            .wait_for_runtime_fact(|state| state.retirements.load(Ordering::Acquire) == 1);

        assert_eq!(
            observations
                .state
                .retirement_modes
                .lock()
                .unwrap()
                .as_slice(),
            [RetirementMode::Forced(DrainFailure {
                stage,
                outstanding_reservations: 0,
            })]
        );
    }
}
