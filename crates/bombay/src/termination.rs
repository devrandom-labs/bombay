//! Publication of one exact actor termination.

use std::sync::{Arc, Mutex, PoisonError};

use behavior_actors::{Crash, Exit};
use bombay_engine::Completion;

use crate::local::Termination;
use crate::observe::Publisher;
use crate::{IncarnationOutcome, Retirement};

#[derive(Clone, Copy)]
pub(crate) enum TerminalReportDisposition {
    Retain,
    Discard,
}

/// Action-scoped terminal outcome selected by an interpreted report.
pub(crate) struct TerminationSelection<A: behavior::Address> {
    outcome: Mutex<Option<Termination<A>>>,
}

impl<A: behavior::Address> TerminationSelection<A> {
    pub(crate) const fn new() -> Self {
        Self {
            outcome: Mutex::new(None),
        }
    }

    pub(crate) fn begin(&self) {
        *self.outcome.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    pub(crate) fn select(&self, outcome: Termination<A>) {
        let mut selected = self.outcome.lock().unwrap_or_else(PoisonError::into_inner);
        if selected.is_none() {
            *selected = Some(outcome);
        }
    }

    pub(crate) fn finish(&self, disposition: TerminalReportDisposition) {
        match disposition {
            TerminalReportDisposition::Retain => {}
            TerminalReportDisposition::Discard => self.begin(),
        }
    }

    fn resolve(&self, fallback: Termination<A>) -> Termination<A> {
        self.outcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or(fallback)
    }
}

pub(crate) struct TerminationPublication<A: behavior::Address> {
    publisher: Publisher<Termination<A>>,
    selection: Arc<TerminationSelection<A>>,
}

impl<A: behavior::Address> TerminationPublication<A> {
    pub(crate) const fn new(
        publisher: Publisher<Termination<A>>,
        selection: Arc<TerminationSelection<A>>,
    ) -> Self {
        Self {
            publisher,
            selection,
        }
    }

    pub(crate) fn publish<B, Residual, BehaviorError, Activation>(
        self,
        outcome: &IncarnationOutcome<B, Residual, BehaviorError, Activation>,
    ) {
        let termination = match outcome {
            IncarnationOutcome::Completed {
                completion: Completion::Stopped,
                ..
            } => Ok(Exit::Normal),
            IncarnationOutcome::Completed {
                completion: Completion::Exhausted,
                ..
            } => Ok(Exit::Collected),
            IncarnationOutcome::BehaviorFailed { .. } => Err(Crash::Failed),
            IncarnationOutcome::ActivationFailed { .. }
            | IncarnationOutcome::SettlementFailed { .. } => Err(Crash::EnvironmentFailed),
            IncarnationOutcome::Panicked => Err(Crash::Panicked),
            IncarnationOutcome::Cancelled => Err(Crash::Cancelled),
        };
        let termination = match outcome {
            IncarnationOutcome::Completed {
                completion: Completion::Stopped,
                ..
            } => self.selection.resolve(termination),
            IncarnationOutcome::Completed {
                completion: Completion::Exhausted,
                ..
            }
            | IncarnationOutcome::BehaviorFailed { .. }
            | IncarnationOutcome::ActivationFailed { .. }
            | IncarnationOutcome::SettlementFailed { .. }
            | IncarnationOutcome::Panicked
            | IncarnationOutcome::Cancelled => termination,
        };
        self.publisher.complete(termination);
    }

    pub(crate) fn publish_owner_cancellation(self) {
        self.selection.finish(TerminalReportDisposition::Discard);
        self.publisher.complete(Err(Crash::Cancelled));
    }
}

impl<A, B, Residual, BehaviorError, Activation> Retirement<B, Residual, BehaviorError, Activation>
    for TerminationPublication<A>
where
    A: behavior::Address,
{
    type Output = ();

    fn retire(self, outcome: IncarnationOutcome<B, Residual, BehaviorError, Activation>) {
        self.publish(&outcome);
    }
}
