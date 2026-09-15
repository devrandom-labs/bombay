//! Model checks over the real `LocalDirectory` synchronization machinery.

#![cfg(bombay_entity_loom)]

use std::num::NonZeroUsize;

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, EffectInterpreter, EntityId, LocalDirectory,
    Refusal, RetirementMode,
};
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

type Directory = LocalDirectory<usize, usize, usize, usize>;

#[derive(Default)]
struct Recording {
    activation: Mutex<Option<ActivationId>>,
    starts: AtomicUsize,
    deliveries: AtomicUsize,
}

impl EffectInterpreter<usize, usize, usize, usize> for Recording {
    fn start_activation(&self, _: EntityId<usize>, activation_id: ActivationId) {
        self.starts.fetch_add(1, Ordering::Relaxed);
        *self.activation.lock().unwrap() = Some(activation_id);
    }

    fn deliver(&self, _: EntityId<usize>, _: ActivationId, _: DispatchId, _: usize, _: usize) {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
    }

    fn reject(&self, _: DispatchId, _: usize, reason: Refusal) {
        panic!("admitted command rejected: {reason:?}");
    }

    fn enqueue_fence(&self, _: EntityId<usize>, _: ActivationId, _: usize) {}

    fn retire(&self, _: EntityId<usize>, _: ActivationId, _: usize, _: RetirementMode) {}
}

#[test]
fn real_directory_concurrent_claims_share_one_activation() {
    loom::model(|| {
        let directory = Arc::new(
            Directory::new(DirectoryConfig {
                shards: NonZeroUsize::MIN,
                activation_waiters: NonZeroUsize::new(3).unwrap(),
            })
            .unwrap(),
        );
        let recording = Arc::new(Recording::default());

        let callers: Vec<_> = (0..2)
            .map(|command| {
                let directory = Arc::clone(&directory);
                let recording = Arc::clone(&recording);
                thread::spawn(move || {
                    let output = directory.dispatch(EntityId::new(1), command).unwrap();
                    directory.interpret(output.output, recording.as_ref());
                })
            })
            .collect();
        for caller in callers {
            caller.join().unwrap();
        }

        assert_eq!(recording.starts.load(Ordering::Relaxed), 1);
        let activation_id = recording.activation.lock().unwrap().unwrap();
        directory.interpret(
            directory.activation_succeeded(&EntityId::new(1), activation_id, 7, 9),
            recording.as_ref(),
        );
        let output = directory.dispatch(EntityId::new(1), 2).unwrap();
        directory.interpret(output.output, recording.as_ref());

        assert_eq!(recording.deliveries.load(Ordering::Relaxed), 3);
        assert_eq!(directory.len(), 1);
    });
}
