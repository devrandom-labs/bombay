//! Ordered, statically dispatched interpretation of complete actor actions.

use core::future::Future;
use core::hash::Hash;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use behavior::{
    Actions, Address, Behavior, BehaviorAddr, BirthMode, Create, CreationResolved, DispatchBirth,
    InstallBirth, Protocol, SendInterpreter, Step,
};
use bombay_engine::ActionsOf;

use crate::address::MailAddr;
use crate::local::{CapabilityRetirement, CommitActions};
use crate::observation::ObservationError;
use crate::reports::TerminalReportTransaction;
use crate::termination::TerminalReportDisposition;
use crate::time::TimerError;

/// Failure to interpret one concrete Bombay runtime capability request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EffectInterpretationError {
    #[error("no live logical recipient exists at {0:?}")]
    UnknownRecipient(MailAddr),
    #[error("the logical recipient at {0:?} has closed")]
    ClosedRecipient(MailAddr),
    #[error("the exact recipient at {0:?} has closed")]
    ClosedEstablishedRecipient(MailAddr),
    #[error("no committed child binding exists at nonce {0}")]
    UnknownChild(u64),
    #[error("the child recipient at nonce {0} has closed")]
    ClosedChild(u64),
    #[error("the child control lane at nonce {0} has closed")]
    ClosedChildControl(u64),
    #[error("the same action contains no creation result at nonce {0}")]
    UnknownCreation(u64),
    #[error("the same-action creation at nonce {0} was rejected")]
    RejectedCreation(u64),
    #[error("the relative timer deadline cannot be represented")]
    TimerDeadlineOverflow,
    #[error("the actor timer queue exhausted its generation domain")]
    TimerGenerationExhausted,
    #[error("the actor timer queue exhausted its insertion sequence")]
    TimerSequenceExhausted,
    #[error("no live actor generation exists at the requested peer address {0:?}")]
    UnknownPeer(MailAddr),
}

impl From<TimerError> for EffectInterpretationError {
    fn from(error: TimerError) -> Self {
        match error {
            TimerError::DeadlineOverflow => Self::TimerDeadlineOverflow,
            TimerError::GenerationExhausted => Self::TimerGenerationExhausted,
            TimerError::SequenceExhausted => Self::TimerSequenceExhausted,
        }
    }
}

impl From<ObservationError<MailAddr>> for EffectInterpretationError {
    fn from(error: ObservationError<MailAddr>) -> Self {
        match error {
            ObservationError::Unknown(address) => Self::UnknownPeer(address),
        }
    }
}

/// Same-action creation results keyed by the creator-local nonce.
pub(crate) struct CreationResults<A>
where
    A: Address,
    A::Nonce: Eq + Hash,
{
    results: Arc<Mutex<HashMap<A::Nonce, CreationResolved<A>>>>,
}

impl<A> Clone for CreationResults<A>
where
    A: Address,
    A::Nonce: Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            results: self.results.clone(),
        }
    }
}

impl<A> CreationResults<A>
where
    A: Address,
    A::Nonce: Copy + Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn begin(&self) {
        self.results
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    pub(crate) fn record(&self, result: CreationResolved<A>) {
        self.results
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(result.nonce, result);
    }

    pub(crate) fn resolve(&self, nonce: &A::Nonce) -> Option<CreationResolved<A>> {
        self.results
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(nonce)
            .copied()
    }
}

/// Common installation error selected by one actor-local capability product.
pub(crate) trait BirthInstaller<A: Address> {
    type Error;
}

/// Installs one concrete child at its statically selected birth position.
pub(crate) trait SpawnChild<A, Position, Child, Error>
where
    A: Address,
    Child: Behavior<Protocol: Protocol<Addr = A>>,
{
    fn spawn_child(
        &mut self,
        creation: Create<A, Child>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Affine retirement of actor-local runtime capability tasks.
pub(crate) trait RetireCapabilities {
    type Event;
    type Descendants;

    fn retire(
        self,
    ) -> impl Future<Output = CapabilityRetirement<Self::Event, Self::Descendants>> + Send;
}

/// Begins the result scope for one complete action value.
pub(crate) trait CreationTransaction {
    fn begin_creations(&mut self);
}

/// Actor-local capability product paired with the emitting actor's address.
pub(crate) struct ActionInterpreter<Capabilities> {
    capabilities: Capabilities,
}

impl<Capabilities> ActionInterpreter<Capabilities> {
    pub(crate) const fn new(capabilities: Capabilities) -> Self {
        Self { capabilities }
    }
}

impl<A, Position, Child, Capabilities>
    InstallBirth<Position, Child, (), <Capabilities as BirthInstaller<A>>::Error>
    for ActionInterpreter<Capabilities>
where
    A: Address,
    Child: Behavior<Protocol: Protocol<Addr = A>>,
    Capabilities: BirthInstaller<A>
        + SpawnChild<A, Position, Child, <Capabilities as BirthInstaller<A>>::Error>,
{
    fn install_birth(
        &mut self,
        creation: Create<A, Child>,
    ) -> impl Future<Output = Result<(), <Capabilities as BirthInstaller<A>>::Error>> + Send {
        self.capabilities.spawn_child(creation)
    }
}

impl<B, Capabilities> CommitActions<B> for ActionInterpreter<Capabilities>
where
    B: Behavior<Ph = behavior::Never>,
    B::Sends: Send,
    <B::Birth as BirthMode>::Child: Send,
    BehaviorAddr<B>: Send,
    <BehaviorAddr<B> as Address>::Nonce: Send,
    Capabilities: BirthInstaller<BehaviorAddr<B>>
        + CreationTransaction
        + RetireCapabilities<Event = B::Event>
        + SendInterpreter
        + TerminalReportTransaction
        + Send,
    <Capabilities as BirthInstaller<BehaviorAddr<B>>>::Error:
        Into<<Capabilities as SendInterpreter>::Error>,
    <B::Birth as BirthMode>::Child: DispatchBirth<
            BehaviorAddr<B>,
            ActionInterpreter<Capabilities>,
            (),
            <Capabilities as BirthInstaller<BehaviorAddr<B>>>::Error,
        >,
    B::Sends: behavior::InterpretSends<Capabilities, B::Event, behavior::Here>,
{
    type Error = <Capabilities as SendInterpreter>::Error;
    type Retired = Capabilities::Descendants;

    async fn commit(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        let Actions {
            sends,
            creates,
            become_,
        } = actions;
        let terminal_disposition = match become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        self.capabilities.begin_terminal_reports();
        self.capabilities.begin_creations();
        let result = async {
            for creation in creates {
                creation
                    .child
                    .dispatch_birth(creation.nonce, creation.kind, self)
                    .await
                    .map_err(Into::into)?;
            }
            <B::Sends as behavior::InterpretSends<Capabilities, B::Event, behavior::Here>>::interpret(
                sends,
                &mut self.capabilities,
            )
            .await
        }
        .await;
        let terminal_disposition = match (&result, terminal_disposition) {
            (Ok(()), TerminalReportDisposition::Retain) => TerminalReportDisposition::Retain,
            (Ok(()), TerminalReportDisposition::Discard) | (Err(_), _) => {
                TerminalReportDisposition::Discard
            }
        };
        self.capabilities
            .finish_terminal_reports(terminal_disposition);
        result
    }

    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        self.capabilities.retire().await
    }
}
