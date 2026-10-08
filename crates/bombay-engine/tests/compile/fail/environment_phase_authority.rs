use core::ops::ControlFlow;
use std::convert::Infallible;

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Interpretation, MailAddr, Never,
    NoBirths, SettlementStatus, SourceCustody, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};

struct Definition;

impl Behavior for Definition {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct Active;
struct Prepared;
struct Settlement;

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl Environment<Definition> for Prepared {
    type Active = Active;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = ();
    type RetirementRequest = Never;

    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<Definition>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) {
        if received.is_some() || actions.is_none() {
            return;
        }
        let Some(owner) = environment.take() else {
            return;
        };
        drop(actions.take());
        drop(owner);
        *received = Some(Ok((Active, Interpretation::Complete(Settlement))));
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<Definition>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || environment.is_none() {
            return;
        }
        drop((environment.take(), actions.take()));
        *received = Some(());
    }
}

impl ActiveEnvironment<Definition> for Active {
    type Settlement = Settlement;
    type Residual = ();
    type RetirementRequest = Never;

    async fn next(&mut self) -> ControlFlow<Never, Option<<Definition as Behavior>::Event>> {
        ControlFlow::Continue(None)
    }

    async fn next_source(&mut self) -> ControlFlow<Never, Option<<Definition as Behavior>::Event>> {
        ControlFlow::Continue(None)
    }

    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<Definition>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() || actions.is_none() {
            return;
        }
        drop(actions.take());
        *received = Some(Interpretation::Complete(Settlement));
    }

    async fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        if let Some(settlement) = settlement.take() {
            *received = Some(SourceCustody::Exhausted(settlement));
        }
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        ControlFlow::Continue(())
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<Definition>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<
            ControlFlow<Self::RetirementRequest, Option<<Definition as Behavior>::Event>>,
        >,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || environment.is_none() {
            return;
        }
        // This concrete unit-residual fixture explicitly discharges its supplied values.
        drop((
            environment.take(),
            actions.take(),
            interpretation.take(),
            source.take(),
            source_index.take(),
            ingress.take(),
            settlements.take(),
        ));
        *received = Some(());
    }
}

fn main() {
    let mut prepared = Prepared;
    let _ = prepared.next();

    let mut active = Some(Active);
    let mut actions = Some(Actions::stop());
    let mut received = None;
    let _ = Environment::<Definition>::activate(&mut active, &mut actions, &mut received);
}
