use super::{Counter, CounterMessage, CounterValue};
use bombay::behavior::{EstablishedDelivery, EstablishedRecipient, MessageProtocol};
use bombay::prelude::{Actions, BehaviorActed, MailAddr, ShutdownRejection};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use std::fmt;
use std::mem::replace;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterOperation {
    Increment,
    Read,
    Stop,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) deployment: &'static str,
    pub(super) caller: &'static str,
    pub(super) controller: &'static str,
    pub(super) actor: &'static str,
    pub(super) host: &'static str,
    pub(super) protocol: &'static str,
    pub(super) runtime_generation: u64,
    pub(super) actor_generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Identity {
    pub(super) binding: Binding,
    pub(super) sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Scope {
    pub(super) identity: Identity,
    pub(super) operation: CounterOperation,
    pub(super) grant_issuer: &'static str,
    pub(super) authority_generation: u64,
}
#[derive(Clone, Copy)]
pub(super) enum Permission {
    Granted,
    Revoked,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterRefusal {
    Invalid,
    Stale,
    Unavailable,
    Binding,
    Grant,
    Conflict,
    RetentionFull,
    Revoked,
    Expired,
    Count,
    IndividualBytes,
    TotalBytes,
    Accounting,
    Exhausted,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterStatus {
    IncrementAdmitted,
    ReadAdmitted,
    StopAccepted,
    StopRejected(ShutdownRejection),
    PossiblyAdmitted,
    Closed,
    Refused(CounterRefusal),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CounterReport {
    pub(super) correlation: u64,
    pub(super) status: CounterStatus,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterProofPurpose {
    Request,
    Refused,
    MailboxAdmitted,
    IncrementProcessed,
    ReadReply,
    StopAccepted,
    RetirementReport,
}
impl CounterProofPurpose {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Refused => "refused",
            Self::MailboxAdmitted => "mailbox_admitted",
            Self::IncrementProcessed => "increment_processed",
            Self::ReadReply => "read_reply",
            Self::StopAccepted => "stop_accepted",
            Self::RetirementReport => "retirement_report",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterProtectedMeaning {
    Request,
    Refused(CounterRefusal),
    IncrementAdmitted,
    ReadAdmitted,
    IncrementProcessed,
    ReadReply(u64),
    StopAccepted,
    RetirementReport {
        retirement: RetirementAssessment,
        failures: ActorFailureAssessment,
    },
}
impl CounterProtectedMeaning {
    pub(super) fn purpose(self) -> CounterProofPurpose {
        match self {
            Self::Request => CounterProofPurpose::Request,
            Self::Refused(_) => CounterProofPurpose::Refused,
            Self::IncrementAdmitted | Self::ReadAdmitted => CounterProofPurpose::MailboxAdmitted,
            Self::IncrementProcessed => CounterProofPurpose::IncrementProcessed,
            Self::ReadReply(_) => CounterProofPurpose::ReadReply,
            Self::StopAccepted => CounterProofPurpose::StopAccepted,
            Self::RetirementReport { .. } => CounterProofPurpose::RetirementReport,
        }
    }
}
pub(super) enum CounterServiceInput {
    Verify(u64),
    #[cfg(test)]
    ReportConsumed(u64),
    #[cfg(test)]
    ReadObserved {
        correlation: u64,
        value: u64,
    },
    ProtectedConsumed {
        correlation: u64,
        purpose: CounterProofPurpose,
    },
}
pub(super) type CounterRequests = MessageProtocol<MailAddr, CounterServiceInput>;
pub(super) enum CounterRequesterMessage {
    BeginProtected {
        correlation: u64,
        scope: Scope,
    },
    ProtectedReply {
        correlation: u64,
        scope: Scope,
        meaning: CounterProtectedMeaning,
    },
    #[cfg(test)]
    Begin(u64),
    #[cfg(test)]
    VerificationCompleted(CounterReport),
    #[cfg(test)]
    ReadObserved {
        correlation: u64,
        value: u64,
    },
}
pub(super) enum CounterRequesterState {
    Protected {
        correlation: u64,
        scope: Scope,
        receipts: Vec<CounterProtectedMeaning>,
    },
    Ready,
    #[cfg(test)]
    Verifying(u64),
    #[cfg(test)]
    WaitingRead(CounterReport),
    #[cfg(test)]
    Complete {
        report: CounterReport,
        observation: Option<u64>,
    },
}
#[derive(thiserror::Error)]
pub(super) enum CounterRequesterError {
    #[error("unexpected {kind}", kind = match .0.as_ref() {
        CounterRequesterMessage::BeginProtected { .. } => "protected verification trigger",
        CounterRequesterMessage::ProtectedReply { .. } => "protected completion",
        #[cfg(test)]
        CounterRequesterMessage::Begin(_) => "verification trigger",
        #[cfg(test)]
        CounterRequesterMessage::VerificationCompleted(_) => "verification completion",
        #[cfg(test)]
        CounterRequesterMessage::ReadObserved { .. } => "Counter snapshot",
    })]
    UnexpectedMessage(Box<CounterRequesterMessage>),
}
impl fmt::Debug for CounterRequesterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CounterRequesterError({self})")
    }
}
pub(super) struct CounterRequester {
    pub(super) service: EstablishedRecipient<CounterRequests>,
    #[cfg(test)]
    pub(super) counter: EstablishedRecipient<Counter>,
    #[cfg(test)]
    pub(super) replies: EstablishedRecipient<CounterValue>,
    pub(super) state: CounterRequesterState,
}
#[bombay::actor(
    sends = pub(super) {
        verification: Vec<EstablishedDelivery<CounterRequests>>,
        reads: Vec<EstablishedDelivery<Counter>>,
    },
    error = CounterRequesterError,
)]
impl CounterRequester {
    pub(super) fn receive(&mut self, message: CounterRequesterMessage) -> BehaviorActed<Self> {
        match message {
            CounterRequesterMessage::BeginProtected { correlation, scope } => {
                if !matches!(self.state, CounterRequesterState::Ready) {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::BeginProtected { correlation, scope },
                    )));
                }
                self.state = CounterRequesterState::Protected {
                    correlation,
                    scope,
                    receipts: Vec::new(),
                };
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::Verify(correlation),
                )))
            }
            CounterRequesterMessage::ProtectedReply {
                correlation,
                scope,
                meaning,
            } => {
                let CounterRequesterState::Protected {
                    correlation: expected,
                    scope: captured,
                    receipts,
                } = &mut self.state
                else {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::ProtectedReply {
                            correlation,
                            scope,
                            meaning,
                        },
                    )));
                };
                let compatible = matches!(
                    (scope.operation, meaning),
                    (_, CounterProtectedMeaning::Refused(_))
                        | (
                            CounterOperation::Increment,
                            CounterProtectedMeaning::IncrementAdmitted
                                | CounterProtectedMeaning::IncrementProcessed,
                        )
                        | (
                            CounterOperation::Read,
                            CounterProtectedMeaning::ReadAdmitted
                                | CounterProtectedMeaning::ReadReply(_),
                        )
                        | (
                            CounterOperation::Stop,
                            CounterProtectedMeaning::StopAccepted
                                | CounterProtectedMeaning::RetirementReport { .. },
                        )
                );
                let contradicted = receipts.iter().any(|prior| {
                    matches!(
                        (prior, meaning),
                        (CounterProtectedMeaning::Refused(_), _)
                            | (_, CounterProtectedMeaning::Refused(_))
                    )
                });
                if *expected != correlation
                    || *captured != scope
                    || !compatible
                    || contradicted
                    || receipts
                        .iter()
                        .any(|prior| prior.purpose() == meaning.purpose())
                {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::ProtectedReply {
                            correlation,
                            scope,
                            meaning,
                        },
                    )));
                }
                receipts.push(meaning);
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::ProtectedConsumed {
                        correlation,
                        purpose: meaning.purpose(),
                    },
                )))
            }
            #[cfg(test)]
            admission => self.receive_admission_report(admission),
        }
    }
}
#[cfg(test)]
impl CounterRequester {
    fn receive_admission_report(
        &mut self,
        message: CounterRequesterMessage,
    ) -> BehaviorActed<Self> {
        match message {
            CounterRequesterMessage::Begin(correlation) => {
                if !matches!(self.state, CounterRequesterState::Ready) {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::Begin(correlation),
                    )));
                }
                self.state = CounterRequesterState::Verifying(correlation);
                Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::Verify(correlation),
                )))
            }
            CounterRequesterMessage::VerificationCompleted(report) => {
                if !matches!(self.state, CounterRequesterState::Verifying(expected) if expected == report.correlation)
                {
                    return Err(CounterRequesterError::UnexpectedMessage(Box::new(
                        CounterRequesterMessage::VerificationCompleted(report),
                    )));
                }
                self.state = match report.status {
                    CounterStatus::IncrementAdmitted | CounterStatus::ReadAdmitted => {
                        CounterRequesterState::WaitingRead(report)
                    }
                    CounterStatus::StopAccepted | CounterStatus::StopRejected(_) => {
                        CounterRequesterState::Complete {
                            report,
                            observation: None,
                        }
                    }
                    CounterStatus::PossiblyAdmitted
                    | CounterStatus::Closed
                    | CounterStatus::Refused(_) => CounterRequesterState::Complete {
                        report,
                        observation: None,
                    },
                };
                let actions = Actions::cont().send_verification(EstablishedDelivery::new(
                    self.service.clone(),
                    CounterServiceInput::ReportConsumed(report.correlation),
                ));
                match report.status {
                    // Read is an independent native snapshot, never an Increment acknowledgement.
                    CounterStatus::IncrementAdmitted => {
                        Ok(actions.send_reads(EstablishedDelivery::new(
                            self.counter.clone(),
                            CounterMessage::Read(self.replies.clone()),
                        )))
                    }
                    CounterStatus::ReadAdmitted
                    | CounterStatus::StopAccepted
                    | CounterStatus::StopRejected(_)
                    | CounterStatus::PossiblyAdmitted
                    | CounterStatus::Closed
                    | CounterStatus::Refused(_) => Ok(actions),
                }
            }
            CounterRequesterMessage::ReadObserved { correlation, value } => {
                let prior = replace(&mut self.state, CounterRequesterState::Ready);
                match prior {
                    CounterRequesterState::WaitingRead(report)
                        if report.correlation == correlation =>
                    {
                        self.state = CounterRequesterState::Complete {
                            report,
                            observation: Some(value),
                        };
                        Ok(Actions::cont().send_verification(EstablishedDelivery::new(
                            self.service.clone(),
                            CounterServiceInput::ReadObserved { correlation, value },
                        )))
                    }
                    retained => {
                        self.state = retained;
                        Err(CounterRequesterError::UnexpectedMessage(Box::new(
                            CounterRequesterMessage::ReadObserved { correlation, value },
                        )))
                    }
                }
            }
            message => Err(CounterRequesterError::UnexpectedMessage(Box::new(message))),
        }
    }
}
