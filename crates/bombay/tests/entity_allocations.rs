//! Allocation profile for the representative hot dispatch path.

use std::sync::Arc;
use std::sync::Mutex;

use bombay::entity::{
    ActivationId, DirectoryConfig, DispatchId, EffectInterpreter, EntityId, LocalDirectory,
    Refusal, RetirementMode,
};
use bombay_machine::executor::ExclusiveExecutor;
use bombay_machine::{Machine, Structure, Topology, Vertex, VertexId};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type Directory = LocalDirectory<usize, usize, usize, usize>;

const VERTICES: &[Vertex] = &[Vertex {
    id: VertexId(0),
    label: "ready",
}];
const TOPOLOGY: Topology = Topology {
    name: "allocation-test",
    initial: VertexId(0),
    vertices: VERTICES,
    transitions: &[],
};

struct CounterMachine(usize);

impl Machine for CounterMachine {
    type Input = usize;
    type Output = usize;

    fn step(self, input: Self::Input) -> (Self::Output, Self) {
        let successor = self.0.wrapping_add(input);
        (successor, Self(successor))
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        visitor.base(TOPOLOGY)
    }
}

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

struct Resolving {
    directory: Arc<Directory>,
}

impl EffectInterpreter<usize, usize, usize, usize> for Resolving {
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
fn active_dispatch_stays_under_allocation_ceiling() {
    const ITERATIONS: usize = 10_000;

    let _profiler = dhat::Profiler::builder().testing().build();
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
    let interpreter = Resolving {
        directory: Arc::clone(&directory),
    };

    // Exclude platform-dependent lazy initialization from the steady-state
    // measurement with an equal-sized exercise of the exact same active path.
    for command in 0..ITERATIONS {
        let output = directory.dispatch(entity_id, command).unwrap().output;
        directory.interpret(output, &interpreter);
    }

    let before = dhat::HeapStats::get();
    for command in 0..ITERATIONS {
        let output = directory.dispatch(entity_id, command).unwrap().output;
        directory.interpret(output, &interpreter);
    }
    let after = dhat::HeapStats::get();

    let blocks = after.total_blocks - before.total_blocks;
    let bytes = after.total_bytes - before.total_bytes;
    // Measured 2026-08-11: the representative active dispatch path is
    // allocation-free; any introduced allocation fails this test.
    assert_eq!(
        (blocks, bytes),
        (0, 0),
        "allocations over {ITERATIONS} dispatches"
    );

    let mut executor = ExclusiveExecutor::new(CounterMachine(0));
    let before = dhat::HeapStats::get();
    for input in 0..ITERATIONS {
        std::hint::black_box(executor.turn(std::hint::black_box(input)).unwrap());
    }
    let after = dhat::HeapStats::get();
    assert_eq!(
        (
            after.total_blocks - before.total_blocks,
            after.total_bytes - before.total_bytes,
        ),
        (0, 0),
        "exclusive executor allocations over {ITERATIONS} turns"
    );
}
