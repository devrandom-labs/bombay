//! Terminal observation for one exact incarnation generation.

use observe::Publisher;

use super::local::Termination;
use super::{IncarnationOutcome, Retirement};

pub(crate) struct TerminalOverride<A: behavior::Address> {
    outcome: std::sync::Mutex<Option<Termination<A>>>,
}

impl<A: behavior::Address> TerminalOverride<A> {
    pub(crate) const fn new() -> Self {
        Self {
            outcome: std::sync::Mutex::new(None),
        }
    }

    pub(crate) fn set(&self, outcome: Termination<A>) {
        *self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(outcome);
    }

    fn resolve(&self, outcome: Termination<A>) -> Termination<A> {
        self.outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or(outcome)
    }
}

pub(crate) struct NormalizedRetirement<A: behavior::Address> {
    publisher: Publisher<Termination<A>>,
    terminal_override: std::sync::Arc<TerminalOverride<A>>,
}

impl<A: behavior::Address> NormalizedRetirement<A> {
    pub(crate) const fn new(
        publisher: Publisher<Termination<A>>,
        terminal_override: std::sync::Arc<TerminalOverride<A>>,
    ) -> Self {
        Self {
            publisher,
            terminal_override,
        }
    }
}

impl<A, B, Activation, Environment> Retirement<B, Activation, Environment>
    for NormalizedRetirement<A>
where
    A: behavior::Address,
{
    fn retire(self, outcome: IncarnationOutcome<B, Activation, Environment>) {
        use behavior::{Crash, Exit};
        use bombay_engine::Completion;

        let outcome = match outcome {
            IncarnationOutcome::Completed(Completion::Stopped) => Ok(Exit::Normal),
            IncarnationOutcome::Completed(Completion::Exhausted) => Ok(Exit::Collected),
            IncarnationOutcome::BehaviorFailed(_) => Err(Crash::Failed),
            IncarnationOutcome::ActivationFailed(_) | IncarnationOutcome::EnvironmentFailed(_) => {
                Err(Crash::EnvironmentFailed)
            }
            IncarnationOutcome::Panicked => Err(Crash::Panicked),
            IncarnationOutcome::Cancelled => Err(Crash::Cancelled),
        };
        self.publisher
            .complete(self.terminal_override.resolve(outcome));
    }
}
