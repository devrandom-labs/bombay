use bombay::TerminalProjection;

struct WorkerOrigin;

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Worker { origin: WorkerOrigin },
}

fn main() {}
