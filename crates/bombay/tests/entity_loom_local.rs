//! Model checks over the real `LocalDirectory` synchronization machinery.

#![cfg(bombay_entity_loom)]

use std::num::NonZeroUsize;

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, EffectInterpreter, EntityId, LocalDirectory,
    Refusal, RetirementMode, TransitionEvidence,
};
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

type Directory = LocalDirectory<usize, usize, usize, usize>;

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Start(ActivationId),
    Deliver(ActivationId, DispatchId, usize, usize),
    Reject(DispatchId, usize, Refusal),
    Fence(ActivationId, usize),
    Retire(ActivationId, usize, RetirementMode),
}

#[derive(Clone, Copy)]
enum DispatchAdmission {
    Admitted,
    Rejected,
}

#[derive(Default)]
struct Recording {
    activation: Mutex<Option<ActivationId>>,
    starts: AtomicUsize,
    deliveries: AtomicUsize,
    commands: Mutex<Vec<usize>>,
    actions: Mutex<Vec<Action>>,
}

impl EffectInterpreter<usize, usize, usize, usize> for Recording {
    fn start_activation(&self, _: EntityId<usize>, activation_id: ActivationId) {
        self.starts.fetch_add(1, Ordering::Relaxed);
        *self.activation.lock().unwrap() = Some(activation_id);
        self.actions
            .lock()
            .unwrap()
            .push(Action::Start(activation_id));
    }

    fn deliver(
        &self,
        _: EntityId<usize>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: usize,
        command: usize,
    ) {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        self.commands.lock().unwrap().push(command);
        self.actions.lock().unwrap().push(Action::Deliver(
            activation_id,
            dispatch_id,
            endpoint,
            command,
        ));
    }

    fn reject(&self, dispatch_id: DispatchId, command: usize, reason: Refusal) {
        self.actions
            .lock()
            .unwrap()
            .push(Action::Reject(dispatch_id, command, reason));
    }

    fn enqueue_fence(&self, _: EntityId<usize>, activation_id: ActivationId, endpoint: usize) {
        self.actions
            .lock()
            .unwrap()
            .push(Action::Fence(activation_id, endpoint));
    }

    fn retire(
        &self,
        _: EntityId<usize>,
        activation_id: ActivationId,
        lease: usize,
        retirement: RetirementMode,
    ) {
        self.actions
            .lock()
            .unwrap()
            .push(Action::Retire(activation_id, lease, retirement));
    }
}

fn directory() -> Directory {
    Directory::new(DirectoryConfig {
        shards: NonZeroUsize::MIN,
        activation_waiters: NonZeroUsize::new(3).unwrap(),
    })
    .unwrap()
}

#[test]
fn pending_delivery_resolution_precedes_the_fence_in_every_interleaving() {
    loom::model(|| {
        let directory = Arc::new(directory());
        let recording = Arc::new(Recording::default());
        let entity_id = EntityId::new(1);
        let first = directory.dispatch(entity_id, 11).unwrap();
        let first_dispatch = first.dispatch_id;
        directory.interpret(first.decision, recording.as_ref());
        let activation_id = recording.activation.lock().unwrap().unwrap();
        directory.interpret(
            directory.activation_succeeded(&entity_id, activation_id, 7, 9),
            recording.as_ref(),
        );
        let second = directory.dispatch(entity_id, 12).unwrap();
        let second_dispatch = second.dispatch_id;
        directory.interpret(second.decision, recording.as_ref());

        let resolving_directory = Arc::clone(&directory);
        let resolving_recording = Arc::clone(&recording);
        let resolution = thread::spawn(move || {
            resolving_directory.interpret(
                resolving_directory.delivery_resolved(&entity_id, activation_id, None),
                resolving_recording.as_ref(),
            );
        });
        let draining_directory = Arc::clone(&directory);
        let draining_recording = Arc::clone(&recording);
        let drain = thread::spawn(move || {
            draining_directory.interpret(
                draining_directory.begin_drain(&entity_id, activation_id),
                draining_recording.as_ref(),
            );
        });
        resolution.join().unwrap();
        drain.join().unwrap();
        directory.interpret(
            directory.delivery_resolved(&entity_id, activation_id, None),
            recording.as_ref(),
        );

        assert_eq!(
            recording.actions.lock().unwrap().as_slice(),
            [
                Action::Start(activation_id),
                Action::Deliver(activation_id, first_dispatch, 7, 11),
                Action::Deliver(activation_id, second_dispatch, 7, 12),
                Action::Fence(activation_id, 7),
            ]
        );
        directory.interpret(
            directory.fence_acknowledged(&entity_id, activation_id),
            recording.as_ref(),
        );
        directory.interpret(
            directory.terminated(&entity_id, activation_id),
            recording.as_ref(),
        );
        assert_eq!(
            recording.actions.lock().unwrap().last(),
            Some(&Action::Retire(activation_id, 9, RetirementMode::Graceful))
        );
        assert!(directory.is_empty());
    });
}

#[test]
fn delayed_old_removal_cannot_remove_a_replacement_slot() {
    loom::model(|| {
        let directory = Arc::new(directory());
        let recording = Arc::new(Recording::default());
        let entity_id = EntityId::new(2);
        let first = directory.dispatch(entity_id, 21).unwrap();
        let first_dispatch = first.dispatch_id;
        directory.interpret(first.decision, recording.as_ref());
        let old = recording.activation.lock().unwrap().unwrap();
        directory.interpret(
            directory.activation_succeeded(&entity_id, old, 7, 9),
            recording.as_ref(),
        );
        directory.interpret(
            directory.delivery_resolved(&entity_id, old, None),
            recording.as_ref(),
        );
        directory.interpret(directory.begin_drain(&entity_id, old), recording.as_ref());
        directory.interpret(
            directory.fence_acknowledged(&entity_id, old),
            recording.as_ref(),
        );
        let old_removal = directory.terminated(&entity_id, old);

        let removing_directory = Arc::clone(&directory);
        let removing_recording = Arc::clone(&recording);
        let removal = thread::spawn(move || {
            removing_directory.interpret(old_removal, removing_recording.as_ref());
        });
        let replacing_directory = Arc::clone(&directory);
        let replacing_recording = Arc::clone(&recording);
        let replacement = thread::spawn(move || {
            let dispatched = replacing_directory.dispatch(entity_id, 22).unwrap();
            let dispatch_id = dispatched.dispatch_id;
            replacing_directory.interpret(dispatched.decision, replacing_recording.as_ref());
            dispatch_id
        });
        removal.join().unwrap();
        let second_dispatch = replacement.join().unwrap();

        let replacement_command = match recording.starts.load(Ordering::Relaxed) {
            1 => {
                let third = directory.dispatch(entity_id, 23).unwrap();
                let third_dispatch = third.dispatch_id;
                directory.interpret(third.decision, recording.as_ref());
                (
                    third_dispatch,
                    23,
                    Some(Action::Reject(second_dispatch, 22, Refusal::Unavailable)),
                )
            }
            2 => (second_dispatch, 22, None),
            starts => panic!("unexpected activation count after replacement race: {starts}"),
        };
        let new = recording.activation.lock().unwrap().unwrap();
        assert_ne!(new, old);
        directory.interpret(directory.terminated(&entity_id, old), recording.as_ref());
        directory.interpret(
            directory.activation_succeeded(&entity_id, new, 8, 10),
            recording.as_ref(),
        );
        let mut expected = vec![
            Action::Start(old),
            Action::Deliver(old, first_dispatch, 7, 21),
            Action::Fence(old, 7),
            Action::Retire(old, 9, RetirementMode::Graceful),
        ];
        if let Some(rejected) = replacement_command.2 {
            expected.push(rejected);
        }
        expected.extend([
            Action::Start(new),
            Action::Deliver(new, replacement_command.0, 8, replacement_command.1),
        ]);
        assert_eq!(*recording.actions.lock().unwrap(), expected);
        assert_eq!(directory.len(), 1);
    });
}

#[test]
fn dispatch_racing_a_drain_retains_exact_admission_custody() {
    loom::model(|| {
        let directory = Arc::new(directory());
        let recording = Arc::new(Recording::default());
        let entity_id = EntityId::new(3);
        let first = directory.dispatch(entity_id, 31).unwrap();
        let first_dispatch = first.dispatch_id;
        directory.interpret(first.decision, recording.as_ref());
        let activation_id = recording.activation.lock().unwrap().unwrap();
        directory.interpret(
            directory.activation_succeeded(&entity_id, activation_id, 7, 9),
            recording.as_ref(),
        );

        let dispatching_directory = Arc::clone(&directory);
        let dispatching_recording = Arc::clone(&recording);
        let dispatch = thread::spawn(move || {
            let second = dispatching_directory.dispatch(entity_id, 32).unwrap();
            let dispatch_id = second.dispatch_id;
            dispatching_directory.interpret(second.decision, dispatching_recording.as_ref());
            dispatch_id
        });
        let draining_directory = Arc::clone(&directory);
        let draining_recording = Arc::clone(&recording);
        let drain = thread::spawn(move || {
            draining_directory.interpret(
                draining_directory.begin_drain(&entity_id, activation_id),
                draining_recording.as_ref(),
            );
        });
        let second_dispatch = dispatch.join().unwrap();
        drain.join().unwrap();
        let admission = {
            let actions = recording.actions.lock().unwrap();
            match actions.last() {
                Some(Action::Deliver(id, dispatch, 7, 32))
                    if *id == activation_id && *dispatch == second_dispatch =>
                {
                    DispatchAdmission::Admitted
                }
                Some(Action::Reject(dispatch, 32, Refusal::Draining))
                    if *dispatch == second_dispatch =>
                {
                    DispatchAdmission::Rejected
                }
                other => panic!("unexpected dispatch result at drain: {other:?}"),
            }
        };
        directory.interpret(
            directory.delivery_resolved(&entity_id, activation_id, None),
            recording.as_ref(),
        );
        if let DispatchAdmission::Admitted = admission {
            directory.interpret(
                directory.delivery_resolved(&entity_id, activation_id, None),
                recording.as_ref(),
            );
        }
        let mut expected = vec![
            Action::Start(activation_id),
            Action::Deliver(activation_id, first_dispatch, 7, 31),
        ];
        expected.push(match admission {
            DispatchAdmission::Admitted => Action::Deliver(activation_id, second_dispatch, 7, 32),
            DispatchAdmission::Rejected => Action::Reject(second_dispatch, 32, Refusal::Draining),
        });
        expected.push(Action::Fence(activation_id, 7));
        assert_eq!(*recording.actions.lock().unwrap(), expected);
        directory.interpret(
            directory.fence_acknowledged(&entity_id, activation_id),
            recording.as_ref(),
        );
        directory.interpret(
            directory.terminated(&entity_id, activation_id),
            recording.as_ref(),
        );
        assert_eq!(
            recording.actions.lock().unwrap().last(),
            Some(&Action::Retire(activation_id, 9, RetirementMode::Graceful))
        );
        assert!(directory.is_empty());
    });
}

#[derive(Debug)]
struct CustodyCommand {
    id: usize,
    drops: Arc<AtomicUsize>,
}

impl Drop for CustodyCommand {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct CustodyRecording {
    activation: Mutex<Option<ActivationId>>,
    actions: Mutex<Vec<Action>>,
}

impl EffectInterpreter<usize, CustodyCommand, usize, usize> for CustodyRecording {
    fn start_activation(&self, _: EntityId<usize>, activation_id: ActivationId) {
        *self.activation.lock().unwrap() = Some(activation_id);
        self.actions
            .lock()
            .unwrap()
            .push(Action::Start(activation_id));
    }

    fn deliver(
        &self,
        _: EntityId<usize>,
        activation_id: ActivationId,
        dispatch_id: DispatchId,
        endpoint: usize,
        command: CustodyCommand,
    ) {
        self.actions.lock().unwrap().push(Action::Deliver(
            activation_id,
            dispatch_id,
            endpoint,
            command.id,
        ));
    }

    fn reject(&self, dispatch_id: DispatchId, command: CustodyCommand, reason: Refusal) {
        self.actions
            .lock()
            .unwrap()
            .push(Action::Reject(dispatch_id, command.id, reason));
    }

    fn enqueue_fence(&self, _: EntityId<usize>, _: ActivationId, _: usize) {
        panic!("activation custody test did not request a drain");
    }

    fn retire(&self, _: EntityId<usize>, _: ActivationId, _: usize, _: RetirementMode) {
        panic!("activation custody test did not request retirement");
    }
}

#[test]
fn canceled_waiter_and_activation_completion_preserve_one_command_owner() {
    loom::model(|| {
        let directory = Arc::new(
            LocalDirectory::new(DirectoryConfig {
                shards: NonZeroUsize::MIN,
                activation_waiters: NonZeroUsize::new(3).unwrap(),
            })
            .unwrap(),
        );
        let recording = Arc::new(CustodyRecording::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let entity_id = EntityId::new(4);
        let first = directory
            .dispatch(
                entity_id,
                CustodyCommand {
                    id: 41,
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        let first_dispatch = first.dispatch_id;
        directory.interpret(first.decision, recording.as_ref());
        let activation_id = recording.activation.lock().unwrap().unwrap();
        let second = directory
            .dispatch(
                entity_id,
                CustodyCommand {
                    id: 42,
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        let second_dispatch = second.dispatch_id;
        directory.interpret(second.decision, recording.as_ref());

        let canceling_directory = Arc::clone(&directory);
        let canceling_recording = Arc::clone(&recording);
        let cancel = thread::spawn(move || {
            let output =
                canceling_directory.cancel_waiter(&entity_id, activation_id, second_dispatch);
            let evidence = output.evidence;
            canceling_directory.interpret(output, canceling_recording.as_ref());
            evidence
        });
        let activating_directory = Arc::clone(&directory);
        let activating_recording = Arc::clone(&recording);
        let activation = thread::spawn(move || {
            activating_directory.interpret(
                activating_directory.activation_succeeded(&entity_id, activation_id, 7, 9),
                activating_recording.as_ref(),
            );
        });
        let cancel_evidence = cancel.join().unwrap();
        activation.join().unwrap();

        let mut expected = vec![
            Action::Start(activation_id),
            Action::Deliver(activation_id, first_dispatch, 7, 41),
        ];
        match cancel_evidence {
            TransitionEvidence::SelfLoop => {}
            TransitionEvidence::Ignored => {
                expected.push(Action::Deliver(activation_id, second_dispatch, 7, 42));
            }
            TransitionEvidence::Traversed(edge) => {
                panic!("canceling a waiter changed lifecycle phase: {edge:?}");
            }
        }
        assert_eq!(*recording.actions.lock().unwrap(), expected);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert_eq!(directory.len(), 1);
    });
}

#[test]
fn real_slot_dispatch_preserves_linearized_output_order() {
    loom::model(|| {
        let directory = Arc::new(
            Directory::new(DirectoryConfig {
                shards: NonZeroUsize::MIN,
                activation_waiters: NonZeroUsize::new(3).unwrap(),
            })
            .unwrap(),
        );
        let recording = Arc::new(Recording::default());
        directory.interpret(
            directory.dispatch(EntityId::new(1), 0).unwrap().decision,
            recording.as_ref(),
        );
        let activation_id = recording.activation.lock().unwrap().unwrap();
        directory.interpret(
            directory.activation_succeeded(&EntityId::new(1), activation_id, 7, 9),
            recording.as_ref(),
        );

        let first = directory.dispatch(EntityId::new(1), 1).unwrap();
        let second = directory.dispatch(EntityId::new(1), 2).unwrap();
        let first_directory = Arc::clone(&directory);
        let first_recording = Arc::clone(&recording);
        let first_thread = thread::spawn(move || {
            first_directory.interpret(first.decision, first_recording.as_ref());
        });
        let second_directory = Arc::clone(&directory);
        let second_recording = Arc::clone(&recording);
        let second_thread = thread::spawn(move || {
            second_directory.interpret(second.decision, second_recording.as_ref());
        });
        first_thread.join().unwrap();
        second_thread.join().unwrap();

        assert_eq!(recording.commands.lock().unwrap().as_slice(), [0, 1, 2]);
    });
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
                    directory.interpret(output.decision, recording.as_ref());
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
        directory.interpret(output.decision, recording.as_ref());

        assert_eq!(recording.deliveries.load(Ordering::Relaxed), 3);
        assert_eq!(directory.len(), 1);
    });
}
