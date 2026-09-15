use std::hint::black_box;
use std::time::{Duration, Instant};

use bombay_machine::executor::{
    ExclusiveExecutor, LinearizedExecutor, Machine, OutputEvidence, SerializedExecutor, TurnOutcome,
};
use bombay_machine::{Structure, Topology, Vertex, VertexId};

const TURNS: u32 = 1_024;
const BATCHES: u32 = 2_000;
const REPETITIONS: usize = 9;
const VERTICES: &[Vertex] = &[Vertex {
    id: VertexId(0),
    label: "ready",
}];
const TOPOLOGY: Topology = Topology {
    name: "executor-benchmark",
    initial: VertexId(0),
    vertices: VERTICES,
    transitions: &[],
};

#[derive(Clone)]
struct Payload([u64; 8]);

impl OutputEvidence for Payload {
    type Evidence = u64;

    fn evidence(&self) -> Self::Evidence {
        self.0[0]
    }
}

struct NoopMachine;

impl Machine for NoopMachine {
    type Input = u64;
    type Output = u64;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        (input, self)
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        visitor.base(TOPOLOGY)
    }
}

struct PayloadMachine(u64);

impl Machine for PayloadMachine {
    type Input = u64;
    type Output = Payload;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        let state = self.0.wrapping_add(input);
        (Payload([state; 8]), Self(state))
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        visitor.base(TOPOLOGY)
    }
}

fn repeat(mut workload: impl FnMut()) -> (Duration, Duration) {
    let mut samples = [Duration::ZERO; REPETITIONS];
    for sample in &mut samples {
        let started = Instant::now();
        workload();
        *sample = started.elapsed();
    }
    samples.sort_unstable();
    (samples[0], samples[REPETITIONS / 2])
}

fn direct_noop() {
    for _ in 0..BATCHES {
        let mut machine = NoopMachine;
        for input in 0..u64::from(TURNS) {
            let (output, successor) = direct_noop_turn(machine, black_box(input));
            black_box(output);
            machine = successor;
        }
        black_box(machine);
    }
}

fn exclusive_noop() {
    for _ in 0..BATCHES {
        let mut executor = ExclusiveExecutor::new(NoopMachine);
        for input in 0..u64::from(TURNS) {
            black_box(exclusive_noop_turn(&mut executor, black_box(input)));
        }
        black_box(executor);
    }
}

#[inline(never)]
fn direct_noop_turn(machine: NoopMachine, input: u64) -> (u64, NoopMachine) {
    machine.step(input)
}

#[inline(never)]
fn exclusive_noop_turn(executor: &mut ExclusiveExecutor<NoopMachine>, input: u64) -> u64 {
    executor.turn(input).unwrap()
}

fn direct_payload() {
    for _ in 0..BATCHES {
        let mut machine = PayloadMachine(0);
        for input in 0..u64::from(TURNS) {
            let (output, successor) = direct_payload_turn(machine, black_box(input));
            black_box(output);
            machine = successor;
        }
        black_box(machine);
    }
}

fn exclusive_payload() {
    for _ in 0..BATCHES {
        let mut executor = ExclusiveExecutor::new(PayloadMachine(0));
        for input in 0..u64::from(TURNS) {
            black_box(exclusive_payload_turn(&mut executor, black_box(input)));
        }
        black_box(executor);
    }
}

// Keep transition boundaries visible so the optimizer cannot prove that a
// caller catching unwind will never observe an executor again.
#[inline(never)]
fn direct_payload_turn(machine: PayloadMachine, input: u64) -> (Payload, PayloadMachine) {
    machine.step(input)
}

#[inline(never)]
fn exclusive_payload_turn(executor: &mut ExclusiveExecutor<PayloadMachine>, input: u64) -> Payload {
    executor.turn(input).unwrap()
}

fn serialized_payload() {
    for _ in 0..BATCHES {
        let executor = SerializedExecutor::new(PayloadMachine(0));
        for input in 0..u64::from(TURNS) {
            let receipt = executor
                .submit(black_box(input), &|output| {
                    black_box(output);
                })
                .unwrap();
            assert_eq!(receipt.wait(), TurnOutcome::Completed);
        }
        black_box(executor);
    }
}

fn linearized_payload() {
    for _ in 0..BATCHES {
        let executor = LinearizedExecutor::new(PayloadMachine(0));
        for input in 0..u64::from(TURNS) {
            black_box(executor.submit(black_box(input)));
            black_box(executor.dispatch_pending(&|output| {
                black_box(output);
            }));
        }
        black_box(executor);
    }
}

fn report(name: &str, sample: (Duration, Duration)) {
    let turns = f64::from(BATCHES) * f64::from(TURNS);
    println!(
        "{name}_min_ns_per_turn={:.3}",
        sample.0.as_secs_f64() * 1e9 / turns
    );
    println!(
        "{name}_median_ns_per_turn={:.3}",
        sample.1.as_secs_f64() * 1e9 / turns
    );
}

fn main() {
    println!("turns_per_batch={TURNS}");
    println!("batches={BATCHES}");
    println!("repetitions={REPETITIONS}");
    report("direct_noop", repeat(direct_noop));
    report("exclusive_noop", repeat(exclusive_noop));
    report("direct_payload", repeat(direct_payload));
    report("exclusive_payload", repeat(exclusive_payload));
    report("serialized_payload", repeat(serialized_payload));
    report("linearized_payload", repeat(linearized_payload));
}
