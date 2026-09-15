use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, EffectInterpreter, EntityId, LocalDirectory,
    Refusal, RetirementMode,
};

type Directory = LocalDirectory<usize, usize, usize, usize>;

#[derive(Default)]
struct Bootstrap(Mutex<Option<ActivationId>>);

impl EffectInterpreter<usize, usize, usize, usize> for Bootstrap {
    fn start_activation(&self, _: EntityId<usize>, activation_id: ActivationId) {
        *self.0.lock().unwrap() = Some(activation_id);
    }

    fn deliver(&self, _: EntityId<usize>, _: ActivationId, _: DispatchId, _: usize, _: usize) {}
    fn reject(&self, _: DispatchId, _: usize, _: Refusal) {}
    fn enqueue_fence(&self, _: EntityId<usize>, _: ActivationId, _: usize) {}
    fn retire(&self, _: EntityId<usize>, _: ActivationId, _: usize, _: RetirementMode) {}
}

struct DeliveryInterpreter {
    directory: Arc<Directory>,
    delivered: AtomicUsize,
}

impl EffectInterpreter<usize, usize, usize, usize> for DeliveryInterpreter {
    fn start_activation(&self, _: EntityId<usize>, _: ActivationId) {
        panic!("active entity started another activation");
    }

    fn deliver(
        &self,
        entity_id: EntityId<usize>,
        activation_id: ActivationId,
        _: DispatchId,
        _: usize,
        _: usize,
    ) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
        let output = self
            .directory
            .delivery_resolved(&entity_id, activation_id, None);
        self.directory.interpret(output, self);
    }

    fn reject(&self, _: DispatchId, _: usize, reason: Refusal) {
        panic!("active delivery rejected: {reason:?}");
    }

    fn enqueue_fence(&self, _: EntityId<usize>, _: ActivationId, _: usize) {}
    fn retire(&self, _: EntityId<usize>, _: ActivationId, _: usize, _: RetirementMode) {}
}

#[test]
fn hot_entity_resolves_every_concurrent_dispatch() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 2_000;
    let directory = Arc::new(Directory::new(DirectoryConfig::default()).unwrap());
    let entity_id = EntityId::new(1);
    let bootstrap = Bootstrap::default();
    directory.interpret(directory.dispatch(entity_id, 0).unwrap().output, &bootstrap);
    let activation_id = bootstrap.0.lock().unwrap().unwrap();
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 1, 1),
        &bootstrap,
    );
    directory.interpret(
        directory.delivery_resolved(&entity_id, activation_id, None),
        &bootstrap,
    );
    let interpreter = Arc::new(DeliveryInterpreter {
        directory: Arc::clone(&directory),
        delivered: AtomicUsize::new(0),
    });
    let threads: Vec<_> = (0..THREADS)
        .map(|worker| {
            let directory = Arc::clone(&directory);
            let interpreter = Arc::clone(&interpreter);
            thread::spawn(move || {
                for sequence in 0..PER_THREAD {
                    let command = worker * PER_THREAD + sequence;
                    let output = directory
                        .dispatch(EntityId::new(1), command)
                        .unwrap()
                        .output;
                    directory.interpret(output, interpreter.as_ref());
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(
        interpreter.delivered.load(Ordering::Relaxed),
        THREADS * PER_THREAD
    );
}

struct ActivationCounter(AtomicUsize);

impl EffectInterpreter<usize, usize, usize, usize> for ActivationCounter {
    fn start_activation(&self, _: EntityId<usize>, _: ActivationId) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn deliver(&self, _: EntityId<usize>, _: ActivationId, _: DispatchId, _: usize, _: usize) {}
    fn reject(&self, _: DispatchId, _: usize, _: Refusal) {}
    fn enqueue_fence(&self, _: EntityId<usize>, _: ActivationId, _: usize) {}
    fn retire(&self, _: EntityId<usize>, _: ActivationId, _: usize, _: RetirementMode) {}
}

#[test]
fn many_independent_entities_progress_across_shards() {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 500;
    let directory = Arc::new(
        Directory::new(DirectoryConfig {
            shards: NonZeroUsize::new(64).unwrap(),
            activation_waiters: NonZeroUsize::MIN,
        })
        .unwrap(),
    );
    let interpreter = Arc::new(ActivationCounter(AtomicUsize::new(0)));
    let threads: Vec<_> = (0..THREADS)
        .map(|worker| {
            let directory = Arc::clone(&directory);
            let interpreter = Arc::clone(&interpreter);
            thread::spawn(move || {
                for sequence in 0..PER_THREAD {
                    let id = worker * PER_THREAD + sequence;
                    let output = directory.dispatch(EntityId::new(id), id).unwrap().output;
                    directory.interpret(output, interpreter.as_ref());
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(directory.len(), THREADS * PER_THREAD);
    assert_eq!(interpreter.0.load(Ordering::Relaxed), THREADS * PER_THREAD);
}
