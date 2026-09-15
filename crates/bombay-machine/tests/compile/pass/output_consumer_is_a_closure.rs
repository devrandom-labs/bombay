use std::cell::Cell;

use bombay_machine::executor::{SerializedExecutor, TurnOutcome};
use bombay_machine::{Base, Topology, Vertex, VertexId};

const VERTICES: &[Vertex] = &[Vertex {
    id: VertexId(0),
    label: "ready",
}];
const TOPOLOGY: Topology = Topology {
    name: "closure-consumer",
    initial: VertexId(0),
    vertices: VERTICES,
    transitions: &[],
};

fn transition(state: u8, input: u8) -> (u8, u8) {
    (input, state + input)
}

fn main() {
    let machine = Base::new(
        0_u8,
        TOPOLOGY.validated().unwrap(),
        transition as fn(u8, u8) -> (u8, u8),
    );
    let executor = SerializedExecutor::new(machine);
    let observed = Cell::new(None);
    let receipt = executor
        .submit(3, &|output| observed.set(Some(output)))
        .unwrap();
    let outcome = receipt.wait();

    assert_eq!(outcome, TurnOutcome::Completed);
    assert_eq!(observed.get(), Some(3));
}
