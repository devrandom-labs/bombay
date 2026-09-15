use bombay::behavior::Never;
use bombay::prelude::StopOnShutdown;
use bombay::{ActorOrigin, ActorRetirement, TerminalProjection};

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Primary {
        origin: ActorOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self>,
    },
    Replica {
        origin: ActorOrigin<StopOnShutdown<Root>>,
        terminal: ActorRetirement<StopOnShutdown<Root>, Self>,
    },
}

fn main() {}
