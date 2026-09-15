use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use bombay::entity::{
    ActivationId, DispatchId, LifecycleEdge, SlotEvent, TransitionEvidence, lifecycle_machine,
};
use bombay_machine::Machine;

const ITERATIONS: u64 = 1_000_000;

fn activation(value: u64) -> ActivationId {
    ActivationId::new(NonZeroU64::new(value).unwrap())
}

const REPETITIONS: usize = 7;

fn measure(mut operation: impl FnMut()) -> Duration {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    started.elapsed()
}

/// Measure repeatedly, returning (minimum, median). See the directory bench
/// for the rationale.
fn repeat(mut operation: impl FnMut()) -> (Duration, Duration) {
    let mut samples = [Duration::ZERO; REPETITIONS];
    for sample in &mut samples {
        *sample = measure(&mut operation);
    }
    samples.sort_unstable();
    (samples[0], samples[REPETITIONS / 2])
}

fn main() {
    let (ignored_min, ignored_med) = repeat(|| {
        let machine = lifecycle_machine::<u8, u8, u8>();
        let (output, machine) = machine.step(SlotEvent::Terminated {
            activation_id: activation(2),
        });
        black_box((output.evidence, machine));
    });

    let (activation_min, activation_med) = repeat(|| {
        let machine = lifecycle_machine::<u8, u8, u8>();
        let (output, machine) = machine.step(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: DispatchId::new(NonZeroU64::MIN),
            command: 1,
            waiter_limit: NonZeroUsize::MIN,
        });
        assert_eq!(
            output.evidence,
            TransitionEvidence::Traversed(LifecycleEdge::ClaimActivation)
        );
        black_box(machine);
    });

    println!("iterations={ITERATIONS}");
    println!("repetitions={REPETITIONS}");
    println!("ignored_step_min={ignored_min:?}");
    println!("ignored_step_median={ignored_med:?}");
    println!("claim_activation_step_min={activation_min:?}");
    println!("claim_activation_step_median={activation_med:?}");
}
