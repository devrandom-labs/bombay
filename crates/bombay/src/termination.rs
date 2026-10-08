//! Publication of one exact actor termination.

use behavior_actors::{Crash, Exit};
use bombay_engine::Completion;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

use crate::ActorExecutionOutcome;

use crate::observe::Publisher;

#[derive(Clone, Copy)]
pub(crate) enum TerminalReportDisposition {
    Retain,
    Discard,
}

/// Action-scoped terminal outcome selected by an interpreted report.
pub(crate) enum TerminationSelection<A: behavior::Address> {
    Unselected,
    Selected(Termination<A>),
}

impl<A: behavior::Address> TerminationSelection<A> {
    pub(crate) const fn new() -> Self {
        Self::Unselected
    }

    pub(crate) fn begin(&mut self) {
        *self = Self::Unselected;
    }

    pub(crate) fn select(&mut self, outcome: Termination<A>) {
        if let Self::Unselected = self {
            *self = Self::Selected(outcome);
        }
    }

    pub(crate) fn finish(&mut self, disposition: TerminalReportDisposition) {
        match disposition {
            TerminalReportDisposition::Retain => {}
            TerminalReportDisposition::Discard => self.begin(),
        }
    }
}

pub(crate) struct TerminationPublication<A: behavior::Address> {
    publisher: Publisher<Termination<A>>,
    selected_report: oneshot::Receiver<Termination<A>>,
}

impl<A: behavior::Address> TerminationPublication<A> {
    pub(crate) const fn new(
        publisher: Publisher<Termination<A>>,
        selected_report: oneshot::Receiver<Termination<A>>,
    ) -> Self {
        Self {
            publisher,
            selected_report,
        }
    }

    pub(crate) fn publish<B, Residual, BehaviorError, Activation, Request>(
        mut self,
        outcome: &ActorExecutionOutcome<B, Residual, BehaviorError, Activation, Request>,
    ) {
        let termination = match outcome {
            ActorExecutionOutcome::Completed {
                completion: Completion::Stopped,
                ..
            } => Ok(Exit::Normal),
            ActorExecutionOutcome::Completed {
                completion: Completion::Exhausted,
                ..
            } => Ok(Exit::Collected),
            ActorExecutionOutcome::Completed {
                completion: Completion::RetirementRequested(_),
                ..
            } => {
                unreachable!("the local receiver publishes its exact retirement request separately")
            }
            ActorExecutionOutcome::BehaviorFailed { .. } => Err(Crash::Failed),
            ActorExecutionOutcome::InitializationPanicked { .. }
            | ActorExecutionOutcome::TransitionPanicked { .. }
            | ActorExecutionOutcome::HostExecutionPanicked { .. }
            | ActorExecutionOutcome::ActivationPanicked { .. }
            | ActorExecutionOutcome::RetirementPanicked { .. }
            | ActorExecutionOutcome::Panicked => Err(Crash::Panicked),
            ActorExecutionOutcome::ActivationFailed { .. }
            | ActorExecutionOutcome::InterpreterContractFailed { .. }
            | ActorExecutionOutcome::SettlementFailed { .. } => Err(Crash::EnvironmentFailed),
            ActorExecutionOutcome::Cancelled => Err(Crash::Cancelled),
        };
        let termination = match outcome {
            ActorExecutionOutcome::Completed {
                completion: Completion::Stopped,
                ..
            } => match self.selected_report.try_recv() {
                Ok(selected) => selected,
                Err(TryRecvError::Closed) => termination,
                Err(TryRecvError::Empty) => {
                    unreachable!("the interpreter retires before terminal publication")
                }
            },
            ActorExecutionOutcome::Completed {
                completion: Completion::Exhausted | Completion::RetirementRequested(_),
                ..
            }
            | ActorExecutionOutcome::BehaviorFailed { .. }
            | ActorExecutionOutcome::InitializationPanicked { .. }
            | ActorExecutionOutcome::TransitionPanicked { .. }
            | ActorExecutionOutcome::HostExecutionPanicked { .. }
            | ActorExecutionOutcome::ActivationPanicked { .. }
            | ActorExecutionOutcome::RetirementPanicked { .. }
            | ActorExecutionOutcome::ActivationFailed { .. }
            | ActorExecutionOutcome::InterpreterContractFailed { .. }
            | ActorExecutionOutcome::SettlementFailed { .. }
            | ActorExecutionOutcome::Panicked
            | ActorExecutionOutcome::Cancelled => termination,
        };
        self.publisher.complete(termination);
    }

    pub(crate) fn publish_host_panic(self) {
        self.publisher.complete(Err(Crash::Panicked));
    }

    pub(crate) fn publish_capability_failure(self) {
        self.publisher.complete(Err(Crash::CapabilityFailed));
    }

    pub(crate) fn publish_owner_cancellation(self) {
        self.publisher.complete(Err(Crash::Cancelled));
    }
}

pub(crate) type Termination<A> = Result<Exit<A>, behavior_actors::Crash>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MailAddr;
    use crate::observe;

    fn published_termination(
        outcome: &ActorExecutionOutcome<(), (), (), ()>,
    ) -> Termination<MailAddr> {
        let (publisher, observed) = observe::pair();
        let (report, selected) = oneshot::channel();
        match report.send(Ok(Exit::LinkDied(MailAddr(19)))) {
            Ok(()) => {}
            Err(outcome) => panic!("retirement must own the report receiver: {outcome:?}"),
        }
        TerminationPublication::new(publisher, selected).publish(outcome);
        observed.wait()
    }

    #[test]
    fn selected_report_applies_only_to_stopped_completion() {
        let termination = published_termination(&ActorExecutionOutcome::Completed {
            behavior: (),
            residual: (),
            additional_failures: Vec::new(),
            completion: Completion::Stopped,
        });
        assert_eq!(termination, Ok(Exit::LinkDied(MailAddr(19))));
        let termination = published_termination(&ActorExecutionOutcome::Completed {
            behavior: (),
            residual: (),
            additional_failures: Vec::new(),
            completion: Completion::Exhausted,
        });
        assert_eq!(termination, Ok(Exit::Collected));
        let termination = published_termination(&ActorExecutionOutcome::ActivationFailed {
            behavior: (),
            residual: (),
            additional_failures: Vec::new(),
            error: (),
        });
        assert_eq!(termination, Err(Crash::EnvironmentFailed));
        let termination = published_termination(&ActorExecutionOutcome::Panicked);
        assert_eq!(termination, Err(Crash::Panicked));
        let termination = published_termination(&ActorExecutionOutcome::Cancelled);
        assert_eq!(termination, Err(Crash::Cancelled));
    }

    #[test]
    fn owner_cancellation_discards_an_earlier_selected_report() {
        let (publisher, observed) = observe::pair();
        let (report, selected) = oneshot::channel();
        match report.send(Ok(Exit::LinkDied(MailAddr(19)))) {
            Ok(()) => {}
            Err(outcome) => panic!("retirement must own the report receiver: {outcome:?}"),
        }
        TerminationPublication::new(publisher, selected).publish_owner_cancellation();
        let termination = observed.wait();
        assert_eq!(termination, Err(Crash::Cancelled));
    }

    #[test]
    fn retirement_trait_publishes_the_selected_terminal_report() {
        let (publisher, observed) = observe::pair();
        let (report, selected) = oneshot::channel();
        let selected_termination = Ok(Exit::LinkDied(MailAddr(23)));
        match report.send(selected_termination) {
            Ok(()) => {}
            Err(termination) => panic!("retirement must own the report receiver: {termination:?}"),
        }

        TerminationPublication::new(publisher, selected).publish(&ActorExecutionOutcome::<
            (),
            (),
            (),
            (),
        >::Completed {
            behavior: (),
            residual: (),
            additional_failures: Vec::new(),
            completion: Completion::Stopped,
        });

        let termination = observed.wait();
        assert_eq!(termination, selected_termination);
    }
}
