use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, EffectInterpreter, EntityId, LocalDirectory,
    Refusal, RetirementMode,
};

const ITERATIONS: u64 = 1_000_000;
const INDEPENDENT_ITERATIONS: u64 = 100_000;
const THREADS: usize = 8;
const CONTENDED_ITERATIONS: u64 = 100_000;

type Directory = LocalDirectory<u64, u64, u64, u64>;

fn config(waiters: usize) -> DirectoryConfig {
    DirectoryConfig {
        shards: NonZeroUsize::new(64).unwrap(),
        activation_waiters: NonZeroUsize::new(waiters).unwrap(),
    }
}

struct Discard {
    activation: AtomicU64,
}

impl Discard {
    fn new() -> Self {
        Self {
            activation: AtomicU64::new(0),
        }
    }
}

impl EffectInterpreter<u64, u64, u64, u64> for Discard {
    fn start_activation(&self, _: EntityId<u64>, activation_id: ActivationId) {
        self.activation
            .store(activation_id.get().get(), Ordering::Relaxed);
    }
    fn deliver(&self, _: EntityId<u64>, _: ActivationId, _: DispatchId, _: u64, _: u64) {}
    fn reject(&self, _: DispatchId, _: u64, _: Refusal) {}
    fn enqueue_fence(&self, _: EntityId<u64>, _: ActivationId, _: u64) {}
    fn retire(&self, _: EntityId<u64>, _: ActivationId, _: u64, _: RetirementMode) {}
}

struct Resolve {
    directory: Arc<Directory>,
}

impl EffectInterpreter<u64, u64, u64, u64> for Resolve {
    fn start_activation(&self, _: EntityId<u64>, _: ActivationId) {}
    fn deliver(
        &self,
        entity_id: EntityId<u64>,
        activation_id: ActivationId,
        _: DispatchId,
        _: u64,
        _: u64,
    ) {
        let output = self
            .directory
            .delivery_resolved(&entity_id, activation_id, None);
        self.directory.interpret(output, self);
    }
    fn reject(&self, _: DispatchId, _: u64, _: Refusal) {}
    fn enqueue_fence(&self, _: EntityId<u64>, _: ActivationId, _: u64) {}
    fn retire(&self, _: EntityId<u64>, _: ActivationId, _: u64, _: RetirementMode) {}
}

fn activating_hot_key() -> Duration {
    let directory = Directory::new(config(64)).unwrap();
    let discard = Discard::new();
    let started = Instant::now();
    for command in 0..ITERATIONS {
        let output = directory
            .dispatch(EntityId::new(1), command)
            .unwrap()
            .output;
        directory.interpret(output, &discard);
    }
    started.elapsed()
}

fn active_hot_key() -> Duration {
    let directory = Arc::new(Directory::new(config(64)).unwrap());
    let discard = Discard::new();
    let entity_id = EntityId::new(1);
    directory.interpret(directory.dispatch(entity_id, 0).unwrap().output, &discard);
    let activation_id =
        ActivationId::new(NonZeroU64::new(discard.activation.load(Ordering::Relaxed)).unwrap());
    let resolve = Resolve {
        directory: Arc::clone(&directory),
    };
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 1, 1),
        &resolve,
    );
    let started = Instant::now();
    for command in 0..ITERATIONS {
        let output = directory.dispatch(entity_id, command).unwrap().output;
        directory.interpret(output, &resolve);
    }
    started.elapsed()
}

fn independent_keys() -> Duration {
    let directory = Directory::new(config(1)).unwrap();
    let discard = Discard::new();
    let started = Instant::now();
    for command in 0..INDEPENDENT_ITERATIONS {
        let output = directory
            .dispatch(EntityId::new(command), command)
            .unwrap()
            .output;
        directory.interpret(output, &discard);
    }
    started.elapsed()
}

/// Stale lifecycle callbacks addressed to an entity with no directory entry,
/// the steady state after passivation churn.
fn stale_absent_callbacks() -> Duration {
    let directory = Directory::new(config(64)).unwrap();
    let discard = Discard::new();
    let entity_id = EntityId::new(1);
    let started = Instant::now();
    for sequence in 1..=ITERATIONS {
        let activation_id = ActivationId::new(NonZeroU64::new(sequence).unwrap());
        let output = directory.terminated(&entity_id, activation_id);
        directory.interpret(output, &discard);
    }
    started.elapsed()
}

fn contended_active_key() -> Duration {
    let directory = Arc::new(Directory::new(config(THREADS)).unwrap());
    let discard = Discard::new();
    let entity_id = EntityId::new(1);
    directory.interpret(directory.dispatch(entity_id, 0).unwrap().output, &discard);
    let activation_id =
        ActivationId::new(NonZeroU64::new(discard.activation.load(Ordering::Relaxed)).unwrap());
    let resolve = Arc::new(Resolve {
        directory: Arc::clone(&directory),
    });
    directory.interpret(
        directory.activation_succeeded(&entity_id, activation_id, 1, 1),
        resolve.as_ref(),
    );
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|worker| {
            let directory = Arc::clone(&directory);
            let resolve = Arc::clone(&resolve);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for command in 0..CONTENDED_ITERATIONS {
                    let output = directory
                        .dispatch(EntityId::new(1), command + worker as u64)
                        .unwrap()
                        .output;
                    directory.interpret(output, resolve.as_ref());
                }
            })
        })
        .collect();
    barrier.wait();
    let started = Instant::now();
    for thread in threads {
        thread.join().unwrap();
    }
    started.elapsed()
}

const REPETITIONS: usize = 7;

/// Run a workload repeatedly, returning (minimum, median) elapsed time.
///
/// The minimum is the least-noise estimator for cross-change comparison; the
/// median exposes typical behavior. A wide min/median spread means machine
/// noise dominates and small deltas must not be trusted.
fn repeat(mut workload: impl FnMut() -> Duration) -> (Duration, Duration) {
    let mut samples = [Duration::ZERO; REPETITIONS];
    for sample in &mut samples {
        *sample = workload();
    }
    samples.sort_unstable();
    (samples[0], samples[REPETITIONS / 2])
}

fn main() {
    let (activating_min, activating_med) = repeat(activating_hot_key);
    let (active_min, active_med) = repeat(active_hot_key);
    let (independent_min, independent_med) = repeat(independent_keys);
    let (contended_min, contended_med) = repeat(contended_active_key);
    let (stale_min, stale_med) = repeat(stale_absent_callbacks);
    black_box((
        activating_min,
        active_min,
        independent_min,
        contended_min,
        stale_min,
    ));
    println!("iterations={ITERATIONS}");
    println!("repetitions={REPETITIONS}");
    println!("activating_hot_key_min={activating_min:?}");
    println!("activating_hot_key_median={activating_med:?}");
    println!("active_hot_key_min={active_min:?}");
    println!("active_hot_key_median={active_med:?}");
    println!("independent_iterations={INDEPENDENT_ITERATIONS}");
    println!("independent_keys_min={independent_min:?}");
    println!("independent_keys_median={independent_med:?}");
    println!("threads={THREADS}");
    println!("contended_iterations_per_thread={CONTENDED_ITERATIONS}");
    println!("contended_active_key_min={contended_min:?}");
    println!("contended_active_key_median={contended_med:?}");
    println!("stale_absent_callbacks_min={stale_min:?}");
    println!("stale_absent_callbacks_median={stale_med:?}");
}
