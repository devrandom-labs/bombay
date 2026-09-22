//! Mechanism tests for the crate-internal entity lifecycle composition.

use core::future::Future;
use core::hash::{Hash, Hasher};
use core::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::directory::{EntityLifecycle, EntityTaskGroup, PendingCommand};
use super::{
    ActivationId, AdmissionFailure, DirectoryConfig, DispatchId, DrainFailure, DrainStage,
    EffectInterpreter, EntityId, FenceFailure, LocalDirectory, Passivation, Refusal,
    RetirementMode,
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
    let mut future = core::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

#[derive(Clone)]
struct TestInterpreter<I> {
    state: Arc<TestState>,
    directory: Arc<LocalDirectory<I, PendingCommand<(), u64>, u64, u64>>,
    settlement: Arc<EntityTaskGroup>,
}

#[derive(Default)]
struct TestState {
    activations: AtomicUsize,
    activation_completions: AtomicUsize,
    activation_failures: AtomicUsize,
    fail_activation: AtomicBool,
    fail_delivery: AtomicBool,
    delivered: Mutex<Vec<u64>>,
    activation_gate: Mutex<Option<Arc<ActivationGate>>>,
    delivery_gate: Mutex<Option<Arc<ActivationGate>>>,
    fence_gate: Mutex<Option<Arc<ActivationGate>>>,
    fence_failure: Mutex<Option<FenceFailure>>,
    fences: AtomicUsize,
    retirements: AtomicUsize,
    retirement_modes: Mutex<Vec<RetirementMode>>,
}

impl<I: Clone + Eq + Hash + Send + Sync + 'static> TestInterpreter<I> {
    fn new(
        directory: Arc<LocalDirectory<I, PendingCommand<(), u64>, u64, u64>>,
        settlement: Arc<EntityTaskGroup>,
    ) -> Self {
        Self {
            state: Arc::new(TestState::default()),
            directory,
            settlement,
        }
    }

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let guard = EntityTaskGroup::begin(&self.settlement);
        thread::spawn(move || {
            let _guard = guard;
            block_on(task);
        });
    }
}

impl<I: Clone + Eq + Hash + Send + Sync + 'static>
    EffectInterpreter<I, PendingCommand<(), u64>, u64, u64> for TestInterpreter<I>
{
    fn start_activation(&self, entity_id: EntityId<I>, activation_id: ActivationId) {
        let interpreter = self.clone();
        self.spawn(async move {
            interpreter
                .state
                .activations
                .fetch_add(1, Ordering::Relaxed);
            let gate = interpreter.state.activation_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            interpreter
                .state
                .activation_completions
                .fetch_add(1, Ordering::Release);
            let output = if interpreter.state.fail_activation.load(Ordering::Relaxed) {
                interpreter
                    .state
                    .activation_failures
                    .fetch_add(1, Ordering::Relaxed);
                interpreter
                    .directory
                    .activation_failed(&entity_id, activation_id)
            } else {
                let incarnation = activation_id.get().get();
                interpreter.directory.activation_succeeded(
                    &entity_id,
                    activation_id,
                    incarnation,
                    incarnation,
                )
            };
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        _endpoint: u64,
        pending: PendingCommand<(), u64>,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            let PendingCommand {
                origin,
                command,
                publisher,
            } = pending;
            let gate = interpreter.state.delivery_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            let failure = if interpreter.state.fail_delivery.load(Ordering::Relaxed) {
                Some((
                    dispatch_id,
                    PendingCommand {
                        origin,
                        command,
                        publisher,
                    },
                ))
            } else {
                interpreter.state.delivered.lock().unwrap().push(command);
                publisher.complete(Ok(()));
                None
            };
            let output =
                interpreter
                    .directory
                    .delivery_resolved(&entity_id, activation_id, failure);
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn reject(&self, _: DispatchId, pending: PendingCommand<(), u64>, reason: Refusal) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    fn enqueue_fence(&self, entity_id: EntityId<I>, activation_id: ActivationId, _endpoint: u64) {
        let interpreter = self.clone();
        self.spawn(async move {
            let gate = interpreter.state.fence_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.wait().await;
            }
            interpreter.state.fences.fetch_add(1, Ordering::Relaxed);
            let output = match *interpreter.state.fence_failure.lock().unwrap() {
                Some(failure) => interpreter.directory.force_drain(
                    &entity_id,
                    activation_id,
                    DrainFailure {
                        stage: match failure {
                            FenceFailure::Enqueue => DrainStage::FenceEnqueue,
                            FenceFailure::Acknowledgement => DrainStage::FenceAcknowledgement,
                        },
                        outstanding_reservations: 0,
                    },
                ),
                None => interpreter
                    .directory
                    .fence_acknowledged(&entity_id, activation_id),
            };
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn retire(
        &self,
        entity_id: EntityId<I>,
        activation_id: ActivationId,
        _lease: u64,
        retirement: RetirementMode,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            interpreter
                .state
                .retirement_modes
                .lock()
                .unwrap()
                .push(retirement);
            interpreter
                .state
                .retirements
                .fetch_add(1, Ordering::Release);
            let output = interpreter.directory.terminated(&entity_id, activation_id);
            interpreter.directory.interpret(output, &interpreter);
        });
    }
}

type TestComposition<I> = (
    Arc<EntityLifecycle<I, (), u64, u64, u64, TestInterpreter<I>>>,
    Arc<TestInterpreter<I>>,
);

fn composition<I: Clone + Eq + Hash + Send + Sync + 'static>() -> TestComposition<I> {
    let directory = Arc::new(
        LocalDirectory::<I, PendingCommand<(), u64>, u64, u64>::new(DirectoryConfig::default())
            .expect("valid directory configuration must construct"),
    );
    let settlement = Arc::new(EntityTaskGroup::new());
    let interpreter = Arc::new(TestInterpreter::new(
        Arc::clone(&directory),
        Arc::clone(&settlement),
    ));
    let lifecycle = Arc::new(EntityLifecycle::from_parts(
        directory,
        settlement,
        Arc::clone(&interpreter),
    ));
    (lifecycle, interpreter)
}

#[derive(Debug)]
struct MoveOnlyCommand {
    value: String,
    drops: Arc<AtomicUsize>,
}

impl Drop for MoveOnlyCommand {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct RejectingMoveOnlyInterpreter {
    directory: Arc<LocalDirectory<u64, PendingCommand<(), MoveOnlyCommand>, (), ()>>,
    settlement: Arc<EntityTaskGroup>,
}

impl RejectingMoveOnlyInterpreter {
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let guard = EntityTaskGroup::begin(&self.settlement);
        thread::spawn(move || {
            let _guard = guard;
            block_on(task);
        });
    }
}

impl EffectInterpreter<u64, PendingCommand<(), MoveOnlyCommand>, (), ()>
    for RejectingMoveOnlyInterpreter
{
    fn start_activation(&self, entity_id: EntityId<u64>, activation_id: ActivationId) {
        let interpreter = self.clone();
        self.spawn(async move {
            let output =
                interpreter
                    .directory
                    .activation_succeeded(&entity_id, activation_id, (), ());
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        _endpoint: (),
        pending: PendingCommand<(), MoveOnlyCommand>,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            let PendingCommand {
                origin,
                command,
                publisher,
            } = pending;
            let failure = Some((
                dispatch_id,
                PendingCommand {
                    origin,
                    command,
                    publisher,
                },
            ));
            let output =
                interpreter
                    .directory
                    .delivery_resolved(&entity_id, activation_id, failure);
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn reject(&self, _: DispatchId, pending: PendingCommand<(), MoveOnlyCommand>, reason: Refusal) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    fn enqueue_fence(&self, entity_id: EntityId<u64>, activation_id: ActivationId, _endpoint: ()) {
        let interpreter = self.clone();
        self.spawn(async move {
            let output = interpreter
                .directory
                .fence_acknowledged(&entity_id, activation_id);
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn retire(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        _lease: (),
        _retirement: RetirementMode,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            let output = interpreter.directory.terminated(&entity_id, activation_id);
            interpreter.directory.interpret(output, &interpreter);
        });
    }
}

type MoveOnlyComposition = (
    Arc<EntityLifecycle<u64, (), MoveOnlyCommand, (), (), RejectingMoveOnlyInterpreter>>,
    Arc<RejectingMoveOnlyInterpreter>,
);

fn move_only_composition() -> MoveOnlyComposition {
    let directory = Arc::new(
        LocalDirectory::<u64, PendingCommand<(), MoveOnlyCommand>, (), ()>::new(
            DirectoryConfig::default(),
        )
        .expect("valid directory configuration must construct"),
    );
    let settlement = Arc::new(EntityTaskGroup::new());
    let interpreter = Arc::new(RejectingMoveOnlyInterpreter {
        directory: Arc::clone(&directory),
        settlement: Arc::clone(&settlement),
    });
    let lifecycle = Arc::new(EntityLifecycle::from_parts(
        directory,
        settlement,
        Arc::clone(&interpreter),
    ));
    (lifecycle, interpreter)
}

#[derive(Clone)]
struct RecordingInterpreter {
    state: Arc<RecordingState>,
    directory: Arc<LocalDirectory<u64, PendingCommand<(), u64>, u64, u64>>,
    settlement: Arc<EntityTaskGroup>,
}

#[derive(Default)]
struct RecordingState {
    activations_started: AtomicUsize,
    activations: AtomicUsize,
    deliveries: Mutex<Vec<u64>>,
    fences: AtomicUsize,
    retirements: AtomicUsize,
}

#[derive(Debug, PartialEq, Eq)]
struct RuntimeTrace {
    activations: usize,
    deliveries: Vec<u64>,
    fences: usize,
    retirements: usize,
}

impl RecordingInterpreter {
    fn trace(&self) -> RuntimeTrace {
        RuntimeTrace {
            activations: self.state.activations.load(Ordering::Acquire),
            deliveries: self.state.deliveries.lock().unwrap().clone(),
            fences: self.state.fences.load(Ordering::Acquire),
            retirements: self.state.retirements.load(Ordering::Acquire),
        }
    }

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let guard = EntityTaskGroup::begin(&self.settlement);
        thread::spawn(move || {
            let _guard = guard;
            block_on(task);
        });
    }
}

impl EffectInterpreter<u64, PendingCommand<(), u64>, u64, u64> for RecordingInterpreter {
    fn start_activation(&self, entity_id: EntityId<u64>, activation_id: ActivationId) {
        let interpreter = self.clone();
        self.spawn(async move {
            interpreter
                .state
                .activations_started
                .fetch_add(1, Ordering::Release);
            interpreter
                .state
                .activations
                .fetch_add(1, Ordering::Release);
            let incarnation = activation_id.get().get();
            let output = interpreter.directory.activation_succeeded(
                &entity_id,
                activation_id,
                incarnation,
                incarnation,
            );
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        _dispatch_id: DispatchId,
        _endpoint: u64,
        pending: PendingCommand<(), u64>,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            let PendingCommand {
                origin: (),
                command,
                publisher,
            } = pending;
            interpreter.state.deliveries.lock().unwrap().push(command);
            publisher.complete(Ok(()));
            let output = interpreter
                .directory
                .delivery_resolved(&entity_id, activation_id, None);
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn reject(&self, _: DispatchId, pending: PendingCommand<(), u64>, reason: Refusal) {
        pending.publisher.complete(Err(AdmissionFailure::Refused {
            command: pending.command,
            reason,
        }));
    }

    fn enqueue_fence(&self, entity_id: EntityId<u64>, activation_id: ActivationId, _endpoint: u64) {
        let interpreter = self.clone();
        self.spawn(async move {
            interpreter.state.fences.fetch_add(1, Ordering::Release);
            let output = interpreter
                .directory
                .fence_acknowledged(&entity_id, activation_id);
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn retire(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        _lease: u64,
        _retirement: RetirementMode,
    ) {
        let interpreter = self.clone();
        self.spawn(async move {
            interpreter
                .state
                .retirements
                .fetch_add(1, Ordering::Release);
            let output = interpreter.directory.terminated(&entity_id, activation_id);
            interpreter.directory.interpret(output, &interpreter);
        });
    }
}

type RecordingComposition = (
    Arc<EntityLifecycle<u64, (), u64, u64, u64, RecordingInterpreter>>,
    Arc<RecordingInterpreter>,
);

fn recording_composition() -> RecordingComposition {
    let directory = Arc::new(
        LocalDirectory::<u64, PendingCommand<(), u64>, u64, u64>::new(DirectoryConfig::default())
            .expect("valid directory configuration must construct"),
    );
    let settlement = Arc::new(EntityTaskGroup::new());
    let interpreter = Arc::new(RecordingInterpreter {
        state: Arc::new(RecordingState::default()),
        directory: Arc::clone(&directory),
        settlement: Arc::clone(&settlement),
    });
    let lifecycle = Arc::new(EntityLifecycle::from_parts(
        directory,
        settlement,
        Arc::clone(&interpreter),
    ));
    (lifecycle, interpreter)
}

#[derive(Clone)]
struct GatedInterpreter {
    inner: RecordingInterpreter,
    gate: Arc<ActivationGate>,
}

impl EffectInterpreter<u64, PendingCommand<(), u64>, u64, u64> for GatedInterpreter {
    fn start_activation(&self, entity_id: EntityId<u64>, activation_id: ActivationId) {
        let interpreter = self.inner.clone();
        let gate = Arc::clone(&self.gate);
        self.inner.spawn(async move {
            interpreter
                .state
                .activations_started
                .fetch_add(1, Ordering::Release);
            gate.wait().await;
            interpreter
                .state
                .activations
                .fetch_add(1, Ordering::Release);
            let incarnation = activation_id.get().get();
            let output = interpreter.directory.activation_succeeded(
                &entity_id,
                activation_id,
                incarnation,
                incarnation,
            );
            interpreter.directory.interpret(output, &interpreter);
        });
    }

    fn deliver(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: u64,
        pending: PendingCommand<(), u64>,
    ) {
        self.inner
            .deliver(entity_id, activation_id, dispatch_id, endpoint, pending);
    }

    fn reject(&self, dispatch_id: DispatchId, pending: PendingCommand<(), u64>, reason: Refusal) {
        self.inner.reject(dispatch_id, pending, reason);
    }

    fn enqueue_fence(&self, entity_id: EntityId<u64>, activation_id: ActivationId, endpoint: u64) {
        self.inner.enqueue_fence(entity_id, activation_id, endpoint);
    }

    fn retire(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        lease: u64,
        retirement: RetirementMode,
    ) {
        self.inner
            .retire(entity_id, activation_id, lease, retirement);
    }
}

type GatedComposition = (
    Arc<EntityLifecycle<u64, (), u64, u64, u64, GatedInterpreter>>,
    Arc<GatedInterpreter>,
);

fn gated_composition(gate: Arc<ActivationGate>) -> GatedComposition {
    let directory = Arc::new(
        LocalDirectory::<u64, PendingCommand<(), u64>, u64, u64>::new(DirectoryConfig::default())
            .expect("valid directory configuration must construct"),
    );
    let settlement = Arc::new(EntityTaskGroup::new());
    let recording = RecordingInterpreter {
        state: Arc::new(RecordingState::default()),
        directory: Arc::clone(&directory),
        settlement: Arc::clone(&settlement),
    };
    let interpreter = Arc::new(GatedInterpreter {
        inner: recording,
        gate,
    });
    let lifecycle = Arc::new(EntityLifecycle::from_parts(
        directory,
        settlement,
        Arc::clone(&interpreter),
    ));
    (lifecycle, interpreter)
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
    open: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl ActivationGate {
    fn closed() -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(false),
            waker: Mutex::new(None),
        })
    }

    fn wait(self: &Arc<Self>) -> GateFuture {
        GateFuture(Arc::clone(self))
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct GateFuture(Arc<ActivationGate>);

impl Future for GateFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.open.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *self.0.waker.lock().unwrap() = Some(context.waker().clone());
            if self.0.open.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}

#[test]
fn application_admission_is_one_asynchronous_operation() {
    let (lifecycle, interpreter) = composition();

    block_on(EntityLifecycle::admit(&lifecycle, (), EntityId::new(42), 7)).unwrap();

    assert_eq!(interpreter.state.activations.load(Ordering::Relaxed), 1);
    assert_eq!(*interpreter.state.delivered.lock().unwrap(), [7]);
}

#[test]
fn failed_delivery_returns_the_original_command() {
    let (lifecycle, interpreter) = composition();
    interpreter
        .state
        .fail_delivery
        .store(true, Ordering::Relaxed);

    let failure =
        block_on(EntityLifecycle::admit(&lifecycle, (), EntityId::new(9), 77)).unwrap_err();

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
    let (lifecycle, _) = move_only_composition();

    let failure = block_on(EntityLifecycle::admit(
        &lifecycle,
        (),
        EntityId::new(9),
        command,
    ))
    .unwrap_err();
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
    let (lifecycle, interpreter) = composition();
    interpreter
        .state
        .fail_activation
        .store(true, Ordering::Relaxed);
    let entity_id = EntityId::new(11);

    assert!(matches!(
        block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, 81)),
        Err(AdmissionFailure::Refused {
            command: 81,
            reason: Refusal::Unavailable,
        })
    ));
    interpreter
        .state
        .fail_activation
        .store(false, Ordering::Relaxed);
    let mut command = 82;
    for _ in 0..1_000 {
        match block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, command)) {
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

    assert_eq!(interpreter.state.activations.load(Ordering::Acquire), 2);
    assert_eq!(*interpreter.state.delivered.lock().unwrap(), [82]);
}

#[test]
fn canceling_admission_does_not_cancel_shared_activation_or_deliver_command() {
    let (lifecycle, interpreter) = composition();
    let gate = ActivationGate::closed();
    *interpreter.state.activation_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let mut admission = Box::pin(EntityLifecycle::admit(&lifecycle, (), EntityId::new(5), 91));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);

    assert!(admission.as_mut().poll(&mut context).is_pending());
    drop(admission);
    gate.open();
    for _ in 0..100 {
        if interpreter
            .state
            .activation_completions
            .load(Ordering::Acquire)
            == 1
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(interpreter.state.activations.load(Ordering::Relaxed), 1);
    assert_eq!(
        interpreter
            .state
            .activation_completions
            .load(Ordering::Acquire),
        1
    );
    block_on(EntityLifecycle::admit(&lifecycle, (), EntityId::new(5), 92)).unwrap();
    assert_eq!(*interpreter.state.delivered.lock().unwrap(), [92]);
}

#[test]
fn dropping_active_admission_does_not_retract_owned_delivery() {
    let (lifecycle, interpreter) = composition();
    let entity_id = EntityId::new(8);
    block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, 1)).unwrap();

    let gate = ActivationGate::closed();
    *interpreter.state.delivery_gate.lock().unwrap() = Some(Arc::clone(&gate));
    let mut admission = Box::pin(EntityLifecycle::admit(&lifecycle, (), entity_id, 2));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    assert!(admission.as_mut().poll(&mut context).is_pending());

    drop(admission);
    gate.open();
    for _ in 0..100 {
        if interpreter.state.delivered.lock().unwrap().len() == 2 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(*interpreter.state.delivered.lock().unwrap(), [1, 2]);
}

#[test]
fn passivation_reports_not_active_for_unknown_entity() {
    let (lifecycle, _) = composition();

    assert_eq!(
        lifecycle.passivate(&EntityId::new(99)),
        Passivation::NotActive
    );
}

#[test]
fn repeated_passivation_reports_already_passivating() {
    let (lifecycle, interpreter) = composition();
    let fence_gate = ActivationGate::closed();
    *interpreter.state.fence_gate.lock().unwrap() = Some(Arc::clone(&fence_gate));
    let entity_id = EntityId::new(7);
    block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, 1)).unwrap();

    assert_eq!(lifecycle.passivate(&entity_id), Passivation::Begun);
    assert_eq!(
        lifecycle.passivate(&entity_id),
        Passivation::AlreadyPassivating
    );

    fence_gate.open();
    for _ in 0..100 {
        if interpreter.state.retirements.load(Ordering::Acquire) == 1 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(interpreter.state.fences.load(Ordering::Relaxed), 1);
    assert_eq!(interpreter.state.retirements.load(Ordering::Relaxed), 1);
}

#[test]
fn passivation_reports_superseded_after_incarnation_replacement() {
    let gate = HashGate::new();
    let entity_id = EntityId::new(GatedId {
        value: 8,
        gate: Arc::clone(&gate),
    });
    let (lifecycle, interpreter) = composition();
    block_on(EntityLifecycle::admit(&lifecycle, (), entity_id.clone(), 1)).unwrap();

    let racing_lifecycle = Arc::clone(&lifecycle);
    let racing_id = entity_id.clone();
    let racing_gate = Arc::clone(&gate);
    let passivation = thread::spawn(move || {
        racing_gate.block_third_hash_on_this_thread();
        racing_lifecycle.passivate(&racing_id)
    });
    gate.wait_until_blocked();

    assert_eq!(lifecycle.passivate(&entity_id), Passivation::Begun);
    for _ in 0..1_000 {
        if interpreter.state.retirements.load(Ordering::Acquire) != 0 {
            break;
        }
        thread::yield_now();
    }
    assert_eq!(interpreter.state.retirements.load(Ordering::Acquire), 1);
    let mut command = 2;
    let mut replacement_activated = false;
    for _ in 0..1_000 {
        match block_on(EntityLifecycle::admit(
            &lifecycle,
            (),
            entity_id.clone(),
            command,
        )) {
            Ok(()) => {
                replacement_activated = true;
                break;
            }
            Err(AdmissionFailure::Refused {
                command: returned, ..
            }) => {
                command = returned;
                thread::yield_now();
            }
            Err(failure) => panic!("unexpected replacement admission failure: {failure}"),
        }
    }
    assert!(
        replacement_activated,
        "replacement activation did not complete"
    );

    gate.release();
    assert_eq!(passivation.join().unwrap(), Passivation::Superseded);
    assert_eq!(interpreter.state.activations.load(Ordering::Acquire), 2);
}

#[test]
fn passivation_fences_and_retires_the_exact_incarnation() {
    let (lifecycle, interpreter) = composition();
    let entity_id = EntityId::new(6);
    block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, 1)).unwrap();

    assert_eq!(lifecycle.passivate(&entity_id), Passivation::Begun);
    for _ in 0..100 {
        if interpreter.state.retirements.load(Ordering::Acquire) == 1 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(interpreter.state.fences.load(Ordering::Relaxed), 1);
    assert_eq!(interpreter.state.retirements.load(Ordering::Relaxed), 1);
    assert_eq!(interpreter.state.activations.load(Ordering::Relaxed), 1);
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
        let (lifecycle, interpreter) = composition();
        *interpreter.state.fence_failure.lock().unwrap() = Some(failure);
        let entity_id = EntityId::new(10);
        block_on(EntityLifecycle::admit(&lifecycle, (), entity_id, 1)).unwrap();

        assert_eq!(lifecycle.passivate(&entity_id), Passivation::Begun);
        for _ in 0..1_000 {
            if interpreter.state.retirements.load(Ordering::Acquire) != 0 {
                break;
            }
            thread::yield_now();
        }

        assert_eq!(
            interpreter
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

#[test]
fn family_shutdown_closes_admission_drains_every_slot_and_joins_tasks() {
    let (lifecycle, interpreter) = recording_composition();

    block_on(EntityLifecycle::admit(&lifecycle, (), EntityId::new(41), 7)).unwrap();
    block_on(EntityLifecycle::admit(
        &lifecycle,
        (),
        EntityId::new(73),
        11,
    ))
    .unwrap();

    let shutdown = block_on(lifecycle.shutdown());

    assert_eq!(shutdown.represented, 2);
    assert_eq!(
        interpreter.trace(),
        RuntimeTrace {
            activations: 2,
            deliveries: vec![7, 11],
            fences: 2,
            retirements: 2,
        }
    );
    assert!(matches!(
        block_on(EntityLifecycle::admit(
            &lifecycle,
            (),
            EntityId::new(89),
            13
        )),
        Err(AdmissionFailure::Refused {
            command: 13,
            reason: Refusal::Shutdown,
        })
    ));
}

#[test]
fn shutdown_settles_an_installed_activation_before_draining_and_joining() {
    let gate = ActivationGate::closed();
    let (lifecycle, interpreter) = gated_composition(Arc::clone(&gate));
    let admission = {
        let lifecycle = Arc::clone(&lifecycle);
        thread::spawn(move || {
            block_on(EntityLifecycle::admit(&lifecycle, (), EntityId::new(41), 7))
        })
    };
    for _ in 0..1_000 {
        if interpreter
            .inner
            .state
            .activations_started
            .load(Ordering::Acquire)
            != 0
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    let shutdown = {
        let lifecycle = Arc::clone(&lifecycle);
        thread::spawn(move || block_on(lifecycle.shutdown()))
    };
    for _ in 0..1_000 {
        if lifecycle.admission_is_closed() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        lifecycle.admission_is_closed(),
        "shutdown must close admission before the refusal attempt"
    );

    assert!(matches!(
        block_on(EntityLifecycle::admit(
            &lifecycle,
            (),
            EntityId::new(73),
            11
        )),
        Err(AdmissionFailure::Refused {
            command: 11,
            reason: Refusal::Shutdown,
        })
    ));
    assert!(!shutdown.is_finished());

    gate.open();
    admission.join().unwrap().unwrap();
    assert_eq!(shutdown.join().unwrap().represented, 1);
}

#[test]
fn runtime_admission_returns_the_exact_move_only_command() {
    let drops = Arc::new(AtomicUsize::new(0));
    let command = MoveOnlyCommand {
        value: String::from("owned command"),
        drops: Arc::clone(&drops),
    };
    let allocation = command.value.as_ptr();
    let (lifecycle, _) = move_only_composition();

    let failure = block_on(EntityLifecycle::admit(
        &lifecycle,
        (),
        EntityId::new(9),
        command,
    ))
    .unwrap_err();
    let AdmissionFailure::Refused { command, reason } = failure else {
        panic!("delivery must preserve the rejected command");
    };

    assert_eq!(reason, Refusal::Unavailable);
    assert_eq!(command.value, "owned command");
    assert_eq!(command.value.as_ptr(), allocation);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(command);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(block_on(lifecycle.shutdown()).represented, 1);
}
