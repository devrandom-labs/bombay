//! Ordered, statically dispatched interpretation of complete actor actions.

use std::time::Instant;
use tokio::task::JoinError;

use behavior::{
    ActionSettlement, ActionSettlements, Behavior, BehaviorAddr, BehaviorSettlements,
    CreationSettlements, InterpretCreations, InterpretSends, InterpretationProgress, Never,
    Protocol, SendSettlements, SourceProgress, SourceSettlementCustody, Step,
};

use bombay_engine::ActionsOf;

use crate::address::MailAddr;
use crate::application_runtime::ApplicationCapabilities;
use crate::local::{CapabilityRetirement, CommitActions};
use crate::reports::TerminalReportTransaction;
use crate::termination::TerminalReportDisposition;

pub(crate) type ActionSettlementOf<B> = <B as BehaviorSettlements>::Settlements;
pub(crate) type InterpretedActionSettlement<B> = ActionSettlement<
    <<B as behavior::Behavior>::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements,
    <<B as behavior::Behavior>::Sends as SendSettlements>::Settlements,
    Never,
>;

/// Affine retirement of actor-local runtime capability tasks.
pub(crate) trait RetireCapabilities {
    type Event;
    type Descendants;

    fn next_local_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Self::Event, JoinError>> + Send;

    fn next_deadline(&mut self) -> Option<Instant>;

    fn pop_due(&mut self, now: Instant) -> Option<Self::Event>;

    /// Original capabilities and every acquired retirement lane outlive this attempt.
    fn receive_retirement(
        capabilities: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
    ) -> impl core::future::Future<Output = ()> + Send
    where
        Self: Sized + Send,
        Self::Event: Send,
        Self::Descendants: Send;

    #[cfg(test)]
    fn retire(
        self,
    ) -> impl core::future::Future<Output = CapabilityRetirement<Self::Event, Self::Descendants>> + Send;
}

/// Actor-local capability product paired with the emitting actor's address.
pub(crate) struct ActionInterpreter<Capabilities> {
    capabilities: Option<Capabilities>,
}

impl<Capabilities> ActionInterpreter<Capabilities> {
    pub(crate) const fn new(capabilities: Capabilities) -> Self {
        Self {
            capabilities: Some(capabilities),
        }
    }
}

impl<B, Actor, Spaces, Parent, Bindings, Origins> CommitActions<B>
    for ActionInterpreter<ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>>
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    B: BehaviorSettlements<
            Ph = Never,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        >,
    B::InterpretationCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::InterpretationCustody: Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    BehaviorAddr<B>: Send,
    B::Birth: InterpretCreations<
            BehaviorAddr<B>,
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            behavior::Here,
        >,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody,
        > + Send,
    <B::Sends as SendSettlements>::Settlements: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = <B::Sends as SendSettlements>::SourceCustody,
        > + Send,
    B::Sends: InterpretSends<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            behavior::Here,
        > + Send,
    ActionSettlementOf<B>: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = B::SourceCustody,
        > + Send,
    ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>:
        RetireCapabilities<Event = B::Event> + TerminalReportTransaction + Send,
{
    type Retired = <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants;

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        let become_ = match progress.as_ref() {
            Some(InterpretationProgress::Original(actions)) => &actions.become_,
            Some(InterpretationProgress::Interpreting((_, become_))) => become_,
            Some(InterpretationProgress::Completed(_)) | None => return,
        };
        let terminal_disposition = match become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match *never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        ActionsOf::<B>::interpret::<_, B::Event, behavior::Here>(progress, self.capabilities_mut())
            .await;
        self.capabilities_mut()
            .finish_terminal_reports(terminal_disposition);
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
        >>::prepare_source(progress);
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<
                ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
                B::Event,
            >>::offer_next_to_source(custody, self.capabilities_mut())
            .await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
        >>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        self.capabilities_mut().next_local_event().await
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.capabilities_mut().next_deadline()
    }

    fn pop_due(&mut self, now: Instant) -> Option<B::Event> {
        self.capabilities_mut().pop_due(now)
    }

    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<B::Event, <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants>>,
    ) where
        B::Event: Send,
        <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants: Send,
{
        let Some(owner) = interpreter.as_mut() else {
            return;
        };
        RetireCapabilities::receive_retirement(&mut owner.capabilities, received).await;
        if owner.capabilities.is_none() && received.is_some() {
            drop(interpreter.take());
        }
    }
}

#[cfg(test)]
impl<B, Inner, Activation, Preparation> CommitActions<B>
    for ActionInterpreter<(
        Option<Inner>,
        Activation,
        Preparation,
        Option<CapabilityRetirement<B::Event, Inner::Descendants>>,
    )>
where
    Inner: RetireCapabilities<Event = B::Event> + TerminalReportTransaction + Send,
    Inner::Descendants: Send,
    Activation: Send,
    Preparation: Send,
    B::Event: Send,
    B: BehaviorSettlements<
            Ph = Never,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        >,
    B::InterpretationCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::InterpretationCustody: Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    BehaviorAddr<B>: Send,
    B::Birth: InterpretCreations<BehaviorAddr<B>, Self, B::Event, behavior::Here>,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements: SourceSettlementCustody<
            Self,
            B::Event,
            Custody = <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody,
        > + Send,
    <B::Sends as SendSettlements>::Settlements: SourceSettlementCustody<
            Self,
            B::Event,
            Custody = <B::Sends as SendSettlements>::SourceCustody,
        > + Send,
    B::Sends: InterpretSends<Self, B::Event, behavior::Here> + Send,
    ActionSettlementOf<B>:
        SourceSettlementCustody<Self, B::Event, Custody = B::SourceCustody> + Send,
{
    type Retired = (Inner::Descendants, (Activation, Preparation));

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        let become_ = match progress.as_ref() {
            Some(InterpretationProgress::Original(actions)) => &actions.become_,
            Some(InterpretationProgress::Interpreting((_, become_))) => become_,
            Some(InterpretationProgress::Completed(_)) | None => return,
        };
        let terminal_disposition = match become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match *never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        ActionsOf::<B>::interpret::<_, B::Event, behavior::Here>(progress, self).await;
        self.capabilities_mut()
            .finish_terminal_reports(terminal_disposition);
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::prepare_source(
            progress,
        );
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::offer_next_to_source(custody, self).await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        self.capabilities_mut().next_local_event().await
    }
    fn next_deadline(&mut self) -> Option<Instant> {
        self.capabilities_mut().next_deadline()
    }
    fn pop_due(&mut self, now: Instant) -> Option<B::Event> {
        self.capabilities_mut().pop_due(now)
    }
    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<
            CapabilityRetirement<B::Event, (Inner::Descendants, (Activation, Preparation))>,
        >,
    ) {
        let Some(owner) = interpreter.as_mut() else {
            return;
        };
        RetireCapabilities::receive_retirement(&mut owner.capabilities, received).await;
        if owner.capabilities.is_none() && received.is_some() {
            drop(interpreter.take());
        }
    }
}

impl<Capabilities> ActionInterpreter<Capabilities> {
    /// Loan the exact existing interpreter capabilities to owning-crate law probes.
    pub(crate) fn capabilities_mut(&mut self) -> &mut Capabilities {
        self.capabilities
            .as_mut()
            .expect("live original capabilities remain installed")
    }
}
