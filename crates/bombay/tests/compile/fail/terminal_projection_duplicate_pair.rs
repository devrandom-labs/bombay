use bombay::behavior::Never;
use bombay::prelude::StopOnShutdown;
use bombay::{RootOrigin, ActorRetirement, TerminalProjection};

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Primary {
        origin: RootOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self, ()>,
    },
    Replica {
        origin: RootOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self, ()>,
    },
}

fn main() {}
