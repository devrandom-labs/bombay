use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, DrainFailure, DrainStage, EffectInterpreter,
    EntityId, LocalDirectory, Refusal, RetirementMode,
};

type Directory = LocalDirectory<u64, u64, u64, u64>;

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Start(ActivationId),
    Deliver(DispatchId, u64),
    Reject(DispatchId, u64, Refusal),
    Fence(ActivationId),
    Retire(ActivationId, u64, RetirementMode),
}

#[derive(Default)]
struct Recorder(Mutex<Vec<Action>>);

impl EffectInterpreter<u64, u64, u64, u64> for Recorder {
    fn start_activation(&self, _: EntityId<u64>, activation_id: ActivationId) {
        self.0.lock().unwrap().push(Action::Start(activation_id));
    }

    fn deliver(
        &self,
        _: EntityId<u64>,
        _: ActivationId,
        dispatch_id: DispatchId,
        _: u64,
        command: u64,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(Action::Deliver(dispatch_id, command));
    }

    fn reject(&self, dispatch_id: DispatchId, command: u64, reason: Refusal) {
        self.0
            .lock()
            .unwrap()
            .push(Action::Reject(dispatch_id, command, reason));
    }

    fn enqueue_fence(&self, _: EntityId<u64>, activation_id: ActivationId, _: u64) {
        self.0.lock().unwrap().push(Action::Fence(activation_id));
    }

    fn retire(
        &self,
        _: EntityId<u64>,
        activation_id: ActivationId,
        lease: u64,
        retirement: RetirementMode,
    ) {
        self.0
            .lock()
            .unwrap()
            .push(Action::Retire(activation_id, lease, retirement));
    }
}

fn activation(value: u64) -> ActivationId {
    ActivationId::new(NonZeroU64::new(value).unwrap())
}

#[derive(Default)]
struct ConstantHasher;

impl Hasher for ConstantHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _: &[u8]) {}
}

#[test]
fn custom_hasher_collisions_preserve_distinct_slots_and_exact_removal() {
    let directory =
        LocalDirectory::<u64, u64, u64, u64, BuildHasherDefault<ConstantHasher>>::with_hasher(
            DirectoryConfig {
                shards: NonZeroUsize::MIN,
                activation_waiters: NonZeroUsize::new(2).unwrap(),
            },
            BuildHasherDefault::default(),
        )
        .unwrap();
    let runtime = Recorder::default();
    let first = EntityId::new(1);
    let second = EntityId::new(2);

    directory.interpret(directory.dispatch(first, 11).unwrap().output, &runtime);
    directory.interpret(directory.dispatch(second, 22).unwrap().output, &runtime);
    assert_eq!(directory.len(), 2);
    let (first_activation, second_activation) = match runtime.0.lock().unwrap().as_slice() {
        [Action::Start(first), Action::Start(second)] => (*first, *second),
        actions => panic!("unexpected actions: {actions:?}"),
    };

    directory.interpret(
        directory.activation_succeeded(&first, first_activation, 101, 201),
        &runtime,
    );
    directory.interpret(
        directory.activation_succeeded(&second, second_activation, 102, 202),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&first, first_activation, None),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&second, second_activation, None),
        &runtime,
    );
    directory.interpret(directory.begin_drain(&first, first_activation), &runtime);
    directory.interpret(
        directory.fence_acknowledged(&first, first_activation),
        &runtime,
    );
    directory.interpret(directory.terminated(&first, first_activation), &runtime);

    assert_eq!(directory.len(), 1);
    directory.interpret(directory.dispatch(second, 33).unwrap().output, &runtime);
    let actions = runtime.0.lock().unwrap();
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Deliver(_, 11)))
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Deliver(_, 22)))
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Deliver(_, 33)))
    );
}

#[derive(Clone)]
struct CountingId {
    value: u64,
    hashes: Arc<AtomicUsize>,
}

impl PartialEq for CountingId {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for CountingId {}

impl Hash for CountingId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hashes.fetch_add(1, Ordering::Relaxed);
        self.value.hash(state);
    }
}

#[derive(Default)]
struct CountingRecorder(Mutex<Option<ActivationId>>);

impl EffectInterpreter<CountingId, u64, u64, u64> for CountingRecorder {
    fn start_activation(&self, _: EntityId<CountingId>, activation_id: ActivationId) {
        *self.0.lock().unwrap() = Some(activation_id);
    }

    fn deliver(&self, _: EntityId<CountingId>, _: ActivationId, _: DispatchId, _: u64, _: u64) {}
    fn reject(&self, _: DispatchId, _: u64, _: Refusal) {}
    fn enqueue_fence(&self, _: EntityId<CountingId>, _: ActivationId, _: u64) {}
    fn retire(&self, _: EntityId<CountingId>, _: ActivationId, _: u64, _: RetirementMode) {}
}

#[test]
fn active_dispatch_hashes_for_shard_and_table() {
    let hashes = Arc::new(AtomicUsize::new(0));
    let entity_id = EntityId::new(CountingId {
        value: 1,
        hashes: Arc::clone(&hashes),
    });
    let directory =
        LocalDirectory::<CountingId, u64, u64, u64>::new(DirectoryConfig::default()).unwrap();
    let runtime = CountingRecorder::default();
    directory.interpret(
        directory.dispatch(entity_id.clone(), 1).unwrap().output,
        &runtime,
    );
    let activation_id = runtime.0.lock().unwrap().unwrap();
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 1, 1),
        &runtime,
    );

    hashes.store(0, Ordering::Relaxed);
    let _output = directory.dispatch(entity_id, 2).unwrap();

    assert_eq!(hashes.load(Ordering::Relaxed), 2);
}

#[test]
fn concurrent_first_dispatches_share_one_bounded_activation() {
    const CALLERS: usize = 16;
    let directory = Arc::new(
        Directory::new(DirectoryConfig {
            shards: NonZeroUsize::new(8).unwrap(),
            activation_waiters: NonZeroUsize::new(CALLERS).unwrap(),
        })
        .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(CALLERS));
    let threads: Vec<_> = (0..CALLERS)
        .map(|command| {
            let directory = Arc::clone(&directory);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                directory
                    .dispatch(EntityId::new(7), command as u64)
                    .unwrap()
                    .output
            })
        })
        .collect();
    let outputs: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let runtime = Recorder::default();
    for output in outputs {
        directory.interpret(output, &runtime);
    }

    assert_eq!(directory.len(), 1);
    let activation_id = match runtime.0.lock().unwrap().as_slice() {
        [Action::Start(activation_id)] => *activation_id,
        actions => panic!("unexpected actions: {actions:?}"),
    };
    let activated = directory.activation_succeeded(&EntityId::new(7), activation_id, 11, 12);
    directory.interpret(activated, &runtime);
    assert_eq!(
        runtime
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|action| matches!(action, Action::Deliver(..)))
            .count(),
        CALLERS
    );
}

#[test]
fn draining_closes_admission_before_fence_execution() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(9);
    let claimed = directory.dispatch(entity_id, 1).unwrap().output;
    directory.interpret(claimed, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    let activated = directory.activation_succeeded(&entity_id, activation_id, 2, 3);
    directory.interpret(activated, &runtime);
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &runtime,
    );
    directory.interpret(directory.begin_drain(&entity_id, activation_id), &runtime);
    directory.interpret(directory.dispatch(entity_id, 4).unwrap().output, &runtime);

    let actions = runtime.0.lock().unwrap();
    assert!(matches!(actions[2], Action::Fence(id) if id == activation_id));
    assert!(matches!(
        actions[3],
        Action::Reject(_, 4, Refusal::Draining)
    ));
}

#[test]
fn stale_successful_activation_retires_its_exact_lease() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let output = directory.activation_succeeded(&EntityId::new(1), activation(99), 2, 7);
    directory.interpret(output, &runtime);

    assert!(matches!(
        runtime.0.lock().unwrap().as_slice(),
        [Action::Retire(id, 7, RetirementMode::Forced(_))] if *id == activation(99)
    ));
}

#[test]
fn exact_termination_removes_the_matching_slot() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(4);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &runtime,
    );
    directory.interpret(directory.begin_drain(&entity_id, activation_id), &runtime);
    directory.interpret(
        directory.fence_acknowledged(&entity_id, activation_id),
        &runtime,
    );
    directory.interpret(directory.terminated(&entity_id, activation_id), &runtime);
    assert!(directory.is_empty());
}

struct ReentrantRuntime {
    directory: Arc<Directory>,
    actions: Mutex<Vec<&'static str>>,
}

impl EffectInterpreter<u64, u64, u64, u64> for ReentrantRuntime {
    fn start_activation(&self, _: EntityId<u64>, _: ActivationId) {
        self.actions.lock().unwrap().push("start");
    }

    fn deliver(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        _: DispatchId,
        _: u64,
        _: u64,
    ) {
        self.actions.lock().unwrap().push("deliver");
        let follow_up = self
            .directory
            .delivery_resolved(&entity_id, activation_id, None);
        self.directory.interpret(follow_up, self);
    }

    fn reject(&self, _: DispatchId, _: u64, _: Refusal) {
        self.actions.lock().unwrap().push("reject");
    }

    fn enqueue_fence(&self, _: EntityId<u64>, _: ActivationId, _: u64) {
        self.actions.lock().unwrap().push("fence");
    }

    fn retire(&self, _: EntityId<u64>, _: ActivationId, _: u64, _: RetirementMode) {
        self.actions.lock().unwrap().push("retire");
    }
}

#[test]
fn reentrant_delivery_resolution_appends_fence_to_current_interpreter() {
    let directory = Arc::new(Directory::new(DirectoryConfig::default()).unwrap());
    let recorder = Recorder::default();
    let entity_id = EntityId::new(8);
    directory.interpret(directory.dispatch(entity_id, 10).unwrap().output, &recorder);
    directory.interpret(directory.dispatch(entity_id, 11).unwrap().output, &recorder);
    let Action::Start(activation_id) = recorder.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    let activated = directory.activation_succeeded(&entity_id, activation_id, 2, 3);
    let drain = directory.begin_drain(&entity_id, activation_id);
    let runtime = ReentrantRuntime {
        directory: Arc::clone(&directory),
        actions: Mutex::new(Vec::new()),
    };

    directory.interpret(drain, &runtime);
    directory.interpret(activated, &runtime);

    assert_eq!(
        runtime.actions.lock().unwrap().as_slice(),
        ["deliver", "deliver", "fence"]
    );
}

#[test]
fn forced_retirement_preserves_the_exact_failure_stage() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(12);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &runtime,
    );
    directory.interpret(directory.begin_drain(&entity_id, activation_id), &runtime);
    let failure = DrainFailure {
        stage: DrainStage::FenceAcknowledgement,
        outstanding_reservations: 0,
    };
    directory.interpret(
        directory.force_drain(&entity_id, activation_id, failure),
        &runtime,
    );

    assert!(runtime.0.lock().unwrap().iter().any(|action| matches!(
        action,
        Action::Retire(id, 3, RetirementMode::Forced(observed))
            if *id == activation_id && *observed == failure
    )));
}

#[test]
fn stale_termination_cannot_remove_the_live_incarnation() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(13);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        &runtime,
    );
    directory.interpret(
        directory.terminated(&entity_id, activation(activation_id.get().get() + 1)),
        &runtime,
    );

    assert_eq!(directory.len(), 1);
    directory.interpret(directory.dispatch(entity_id, 9).unwrap().output, &runtime);
    assert!(
        runtime
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|action| matches!(action, Action::Deliver(_, 9)))
    );
}

#[test]
fn failed_activation_rejects_waiters_and_allows_fresh_activation() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(21);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    directory.interpret(directory.dispatch(entity_id, 2).unwrap().output, &runtime);
    let Action::Start(failed) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };

    directory.interpret(directory.activation_failed(&entity_id, failed), &runtime);

    {
        let actions = runtime.0.lock().unwrap();
        assert!(matches!(
            actions[1],
            Action::Reject(_, 1, Refusal::Unavailable)
        ));
        assert!(matches!(
            actions[2],
            Action::Reject(_, 2, Refusal::Unavailable)
        ));
    }
    assert!(directory.is_empty());

    directory.interpret(directory.dispatch(entity_id, 3).unwrap().output, &runtime);
    let actions = runtime.0.lock().unwrap();
    let Some(Action::Start(replacement)) = actions.last() else {
        panic!("replacement activation not started");
    };
    assert_ne!(failed, *replacement);
}

#[test]
fn bounded_activation_waiters_reject_excess_with_busy() {
    let directory = Directory::new(DirectoryConfig {
        shards: NonZeroUsize::new(8).unwrap(),
        activation_waiters: NonZeroUsize::MIN,
    })
    .unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(24);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);

    directory.interpret(directory.dispatch(entity_id, 2).unwrap().output, &runtime);

    let actions = runtime.0.lock().unwrap();
    assert_eq!(actions.len(), 2);
    assert!(matches!(actions[0], Action::Start(_)));
    assert!(matches!(actions[1], Action::Reject(_, 2, Refusal::Busy)));
}

#[test]
fn graceful_retirement_waits_for_fence_acknowledgement() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(23);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &runtime,
    );

    directory.interpret(directory.begin_drain(&entity_id, activation_id), &runtime);
    {
        let actions = runtime.0.lock().unwrap();
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::Fence(id) if *id == activation_id))
        );
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::Retire(..)))
        );
    }

    directory.interpret(directory.terminated(&entity_id, activation_id), &runtime);
    assert_eq!(directory.len(), 1);
    {
        let actions = runtime.0.lock().unwrap();
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, Action::Retire(..)))
        );
    }

    directory.interpret(
        directory.fence_acknowledged(&entity_id, activation_id),
        &runtime,
    );
    {
        let actions = runtime.0.lock().unwrap();
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Retire(id, _, RetirementMode::Graceful) if *id == activation_id
        )));
    }
    directory.interpret(directory.terminated(&entity_id, activation_id), &runtime);
    assert!(directory.is_empty());
}

fn activate_and_reserve(
    directory: &Directory,
    runtime: &Recorder,
    entity_id: EntityId<u64>,
) -> (ActivationId, DispatchId) {
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        runtime,
    );
    directory.interpret(directory.dispatch(entity_id, 2).unwrap().output, runtime);
    let reserved = {
        let actions = runtime.0.lock().unwrap();
        let Some(Action::Deliver(dispatch_id, 2)) = actions.last() else {
            panic!("second dispatch not reserved for delivery");
        };
        *dispatch_id
    };
    directory.interpret(directory.begin_drain(&entity_id, activation_id), runtime);
    directory.interpret(
        directory.force_drain(
            &entity_id,
            activation_id,
            DrainFailure {
                stage: DrainStage::Reservations,
                outstanding_reservations: 1,
            },
        ),
        runtime,
    );
    (activation_id, reserved)
}

#[test]
fn late_failed_delivery_into_retiring_returns_the_command() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(25);
    let (activation_id, reserved) = activate_and_reserve(&directory, &runtime, entity_id);

    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, Some((reserved, 2))),
        &runtime,
    );

    let actions = runtime.0.lock().unwrap();
    assert!(actions.iter().any(|action| matches!(
        action,
        Action::Reject(dispatch_id, 2, Refusal::Unavailable) if *dispatch_id == reserved
    )));
}

#[test]
fn late_failed_delivery_after_removal_returns_the_command() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(26);
    let (activation_id, reserved) = activate_and_reserve(&directory, &runtime, entity_id);
    directory.interpret(directory.terminated(&entity_id, activation_id), &runtime);
    assert!(directory.is_empty());

    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, Some((reserved, 2))),
        &runtime,
    );

    let actions = runtime.0.lock().unwrap();
    assert!(actions.iter().any(|action| matches!(
        action,
        Action::Reject(dispatch_id, 2, Refusal::Unavailable) if *dispatch_id == reserved
    )));
}

#[test]
fn retiring_rejects_dispatch_and_retires_stale_activation() {
    let directory = Directory::new(DirectoryConfig::default()).unwrap();
    let runtime = Recorder::default();
    let entity_id = EntityId::new(27);
    directory.interpret(directory.dispatch(entity_id, 1).unwrap().output, &runtime);
    let Action::Start(activation_id) = runtime.0.lock().unwrap()[0] else {
        panic!("activation not started");
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 2, 3),
        &runtime,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &runtime,
    );
    directory.interpret(directory.begin_drain(&entity_id, activation_id), &runtime);
    directory.interpret(
        directory.fence_acknowledged(&entity_id, activation_id),
        &runtime,
    );

    directory.interpret(directory.dispatch(entity_id, 9).unwrap().output, &runtime);
    let stale = activation(activation_id.get().get() + 1);
    directory.interpret(
        directory.activation_succeeded(&entity_id, stale, 8, 9),
        &runtime,
    );

    {
        let actions = runtime.0.lock().unwrap();
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, Action::Reject(_, 9, Refusal::Draining)))
        );
        assert!(actions.iter().any(|action| matches!(
            action,
            Action::Retire(id, 9, RetirementMode::Forced(DrainFailure {
                stage: DrainStage::Retirement,
                outstanding_reservations: 0,
            })) if *id == stale
        )));
    }

    directory.interpret(directory.terminated(&entity_id, activation_id), &runtime);
    assert!(directory.is_empty());
}
