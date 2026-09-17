//! Ordered, statically dispatched interpretation of complete actor actions.

use behavior::{
    ActionSettlement, BehaviorAddr, BehaviorSettlements, CreationSettlements, InterpretCreations,
    InterpretSends, Interpretation, Never, SendSettlements, SourceCustody, SourceSettlementCustody,
    Step,
};

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

    fn retire(
        self,
    ) -> impl core::future::Future<Output = CapabilityRetirement<Self::Event, Self::Descendants>> + Send;
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

impl<B, Capabilities> CommitActions<B> for ActionInterpreter<Capabilities>
where
    B: BehaviorSettlements<Ph = Never, Settlements = InterpretedActionSettlement<B>>,
    BehaviorAddr<B>: Send,
    B::Birth: InterpretCreations<BehaviorAddr<B>, Capabilities, B::Event, behavior::Here>,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements: Send,
    B::Sends: InterpretSends<Capabilities, B::Event, behavior::Here> + Send,
    ActionSettlementOf<B>: SourceSettlementCustody<Capabilities, B::Event> + Send,
    Capabilities: RetireCapabilities<Event = B::Event> + TerminalReportTransaction + Send,
{
    type Retired = Capabilities::Descendants;

    async fn commit(
        &mut self,
        actions: bombay_engine::ActionsOf<B>,
    ) -> Interpretation<ActionSettlementOf<B>> {
        self.capabilities.begin_terminal_reports();
        let terminal_disposition = match &actions.become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match *never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        let interpretation = actions
            .interpret::<_, B::Event, behavior::Here>(&mut self.capabilities)
            .await;
        self.capabilities
            .finish_terminal_reports(terminal_disposition);
        interpretation
    }

    async fn offer_next(
        &mut self,
        settlement: ActionSettlementOf<B>,
    ) -> SourceCustody<ActionSettlementOf<B>> {
        settlement
            .offer_next_to_source(&mut self.capabilities)
            .await
    }

    async fn retire(self) -> CapabilityRetirement<B::Event, Self::Retired> {
        self.capabilities.retire().await
    }
}
