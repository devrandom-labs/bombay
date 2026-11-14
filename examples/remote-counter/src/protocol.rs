//! Application DTOs and a pure three-operation caller; no controller or native IO.
use super::deployment::{Operation, Purpose, Receipt};
use bombay::behavior::{Behavior, EstablishedRecipient, MessageProtocol, User};
use bombay::prelude::*;
use bombay::remote::{Attempt, Metadata};
use bombay::{ActorFailureAssessment, ActorRetirementReport, RetirementAssessment};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

use crate::counter::{CounterIncremented, CounterMessage, CounterProcessed, CounterValue};

pub(crate) const KEY: &str = "bombay/example/remote-counter";
// Explicit finite TEST-job correlations, not predicted address/generation IDs.
pub(crate) fn sequence(operation: Operation) -> u64 {
    match operation {
        Operation::Increment => 101,
        Operation::Read => 102,
        Operation::Stop => 103,
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Value {
    Processed(u64),
    Read(u64),
    Stopped,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) bombay: Metadata,
    pub(crate) kind: Purpose,
    pub(crate) operation: Operation,
}
#[derive(Serialize)]
struct RequestRef<'a> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Response {
    pub(crate) bombay: Metadata,
    pub(crate) kind: Purpose,
    pub(crate) operation: Operation,
    pub(crate) value: Value,
}
#[derive(Serialize)]
struct ResponseRef<'a> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
    value: Value,
}
#[derive(Error)]
pub(crate) enum Refusal {
    #[error("counter protocol scope was refused")]
    Scope,
    #[error("counter JSON was refused")]
    Json {
        #[source]
        cause: serde_json::Error,
    },
}
impl fmt::Debug for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scope => f.write_str("Scope"),
            Self::Json { .. } => f.write_str("Json(..)"),
        }
    }
}
pub(crate) fn request(metadata: &Metadata, operation: Operation) -> Result<Box<[u8]>, Refusal> {
    serde_json::to_vec(&RequestRef {
        bombay: metadata,
        kind: Purpose::Request,
        operation,
    })
    .map(Vec::into_boxed_slice)
    .map_err(|cause| Refusal::Json { cause })
}
pub(crate) fn response(
    metadata: &Metadata,
    operation: Operation,
    value: Value,
) -> Result<Box<[u8]>, Refusal> {
    serde_json::to_vec(&ResponseRef {
        bombay: metadata,
        kind: Purpose::Reply,
        operation,
        value,
    })
    .map(Vec::into_boxed_slice)
    .map_err(|cause| Refusal::Json { cause })
}
#[expect(
    clippy::result_large_err,
    reason = "unselected declarations return the complete original request for the next declaration"
)]
pub(crate) fn increment(
    input: Request,
) -> Result<impl FnOnce(EstablishedRecipient<CounterProcessed>) -> CounterMessage, Request> {
    match (input.kind, input.operation) {
        (Purpose::Request, Operation::Increment) => {
            let request_sequence = input.bombay.request_sequence;
            Ok(move |reply_to| CounterMessage::IncrementAndReply {
                request_sequence,
                reply_to,
            })
        }
        _ => Err(input),
    }
}
#[expect(
    clippy::result_large_err,
    reason = "unselected declarations return the complete original request for the next declaration"
)]
pub(crate) fn read(
    input: Request,
) -> Result<impl FnOnce(EstablishedRecipient<CounterValue>) -> CounterMessage, Request> {
    match (input.kind, input.operation) {
        (Purpose::Request, Operation::Read) => Ok(CounterMessage::Read),
        _ => Err(input),
    }
}
#[expect(
    clippy::result_large_err,
    reason = "unselected declarations return the complete original request for the next declaration"
)]
pub(crate) fn stop(input: Request) -> Result<(), Request> {
    match (input.kind, input.operation) {
        (Purpose::Request, Operation::Stop) => Ok(()),
        _ => Err(input),
    }
}
pub(crate) fn processed(
    receipt: &Receipt,
    original: &User<MailAddr, CounterIncremented>,
) -> Result<Box<[u8]>, Refusal> {
    if receipt.operation() != Operation::Increment
        || original.from.0 != receipt.metadata().actor_generation
        || original.message.request_sequence != receipt.metadata().request_sequence
    {
        return Err(Refusal::Scope);
    }
    response(
        receipt.metadata(),
        Operation::Increment,
        Value::Processed(original.message.request_sequence),
    )
}
pub(crate) fn value(
    receipt: &Receipt,
    original: &User<MailAddr, u64>,
) -> Result<Box<[u8]>, Refusal> {
    if receipt.operation() != Operation::Read
        || original.from.0 != receipt.metadata().actor_generation
    {
        return Err(Refusal::Scope);
    }
    response(
        receipt.metadata(),
        Operation::Read,
        Value::Read(original.message),
    )
}
#[expect(
    clippy::ref_option,
    clippy::trivially_copy_pass_by_ref,
    reason = "the owning Stop declaration borrows its actual optional retirement report"
)]
pub(crate) fn stopped(
    receipt: &Receipt,
    attempt: &Attempt<Result<(), ShutdownRejection>>,
    report: &Option<ActorRetirementReport>,
) -> Result<Box<[u8]>, Refusal> {
    if receipt.operation() != Operation::Stop || !matches!(attempt, Attempt::Returned(Ok(()))) {
        return Err(Refusal::Scope);
    }
    let Some(report) = report else {
        return Err(Refusal::Scope);
    };
    if report.retirement() != RetirementAssessment::Established
        || report.failures() != ActorFailureAssessment::NoFailuresFound
    {
        return Err(Refusal::Scope);
    }
    response(receipt.metadata(), Operation::Stop, Value::Stopped)
}

pub(crate) type Requests = MessageProtocol<MailAddr, Operation>;
pub(crate) struct Reply {
    pub(crate) operation: Operation,
    pub(crate) sequence: u64,
    pub(crate) value: Value,
}
pub(crate) type Results = MessageProtocol<MailAddr, Outcome>;
pub(crate) enum Outcome {
    Complete(u64),
    Uncertain(Operation),
    Failed(Unexpected),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Journey {
    Increment,
    Read,
    Stop(u64),
    Complete(u64),
}
pub(crate) struct Caller {
    pub(crate) service: EstablishedRecipient<Requests>,
    pub(crate) results: EstablishedRecipient<Results>,
    pub(crate) journey: Journey,
}
#[derive(Error)]
#[error("reply does not belong to the current counter operation")]
pub(crate) struct Unexpected {
    pub(crate) original: Reply,
}
impl fmt::Debug for Unexpected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Unexpected")
            .field("operation", &self.original.operation)
            .finish_non_exhaustive()
    }
}
#[bombay::actor(sends = pub(crate) {
    requests: Vec<EstablishedDelivery<Requests>>,
    results: Vec<EstablishedDelivery<Results>>,
})]
impl Caller {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the owning Behavior initialization contract returns Result even with an uninhabited error"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().send_requests(EstablishedDelivery::new(
            self.service.clone(),
            Operation::Increment,
        )))
    }
    fn receive(&mut self, reply: Reply) -> BehaviorActed<Self> {
        if reply.sequence != sequence(reply.operation) {
            return self.refuse(reply);
        }
        match (self.journey, reply.operation, &reply.value) {
            (Journey::Increment, Operation::Increment, Value::Processed(actual))
                if *actual == reply.sequence =>
            {
                self.journey = Journey::Read;
                Ok(Actions::cont().send_requests(EstablishedDelivery::new(
                    self.service.clone(),
                    Operation::Read,
                )))
            }
            (Journey::Read, Operation::Read, Value::Read(value)) => {
                self.journey = Journey::Stop(*value);
                Ok(Actions::cont().send_requests(EstablishedDelivery::new(
                    self.service.clone(),
                    Operation::Stop,
                )))
            }
            (Journey::Stop(value), Operation::Stop, Value::Stopped) => {
                self.journey = Journey::Complete(value);
                Ok(Actions::stop().send_results(EstablishedDelivery::new(
                    self.results.clone(),
                    Outcome::Complete(value),
                )))
            }
            _ => self.refuse(reply),
        }
    }
}

impl Caller {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "both refusal branches return the complete owning Behavior fold result"
    )]
    fn refuse(&self, original: Reply) -> BehaviorActed<Self> {
        Ok(Actions::stop().send_results(EstablishedDelivery::new(
            self.results.clone(),
            Outcome::Failed(Unexpected { original }),
        )))
    }

    pub(crate) fn expire(
        &mut self,
    ) -> Actions<MailAddr, Never, <Self as Behavior>::Sends, <Self as Behavior>::Birth> {
        let operation = match self.journey {
            Journey::Increment => Operation::Increment,
            Journey::Read => Operation::Read,
            Journey::Stop(_) => Operation::Stop,
            Journey::Complete(_) => return Actions::stop(),
        };
        Actions::stop().send_results(EstablishedDelivery::new(
            self.results.clone(),
            Outcome::Uncertain(operation),
        ))
    }
}
