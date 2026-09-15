use bombay_machine::executor::{OutputHandler, SerializedExecutor};
use bombay_machine::{Base, Topology, Vertex, VertexId};

const VERTICES: &[Vertex] = &[Vertex {
    id: VertexId(0),
    label: "ready",
}];
const TOPOLOGY: Topology = Topology {
    name: "obsolete-output-port",
    initial: VertexId(0),
    vertices: VERTICES,
    transitions: &[],
};

fn transition(state: u8, input: u8) -> (u8, u8) {
    (input, state + input)
}

struct NamedConsumer;

impl OutputHandler<u8> for NamedConsumer {
    fn handle(&self, _output: u8) {}
}

fn main() {
    let machine = Base::new(
        0_u8,
        TOPOLOGY.validated().unwrap(),
        transition as fn(u8, u8) -> (u8, u8),
    );
    let executor = SerializedExecutor::new(machine);
    let _receipt = executor.submit(1, &NamedConsumer).unwrap();
}
