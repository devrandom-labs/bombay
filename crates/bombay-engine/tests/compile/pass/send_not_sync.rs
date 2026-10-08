use core::ops::ControlFlow;
use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Interpretation, MailAddr, Never,
    NoBirths, SettlementStatus, SourceCustody, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};

struct SendNotSync(Cell<u8>);

impl Behavior for SendNotSync {
    type Protocol = behavior::MessageProtocol<MailAddr, Box<u8>>;
    type Event = User<MailAddr, Box<u8>>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.0.set(*event.message);
        Ok(Actions::stop())
    }
}

struct Env {
    event: Option<Box<u8>>,
    local: Rc<Cell<u8>>,
}

struct Settlement;

impl ClassifySettlement for Settlement {
    fn settlement_status(&self) -> SettlementStatus {
        SettlementStatus::Accepted
    }
}

impl ActiveEnvironment<SendNotSync> for Env {
    type Settlement = Settlement;
    type Residual = ();
    type RetirementRequest = Never;

    async fn next(&mut self) -> ControlFlow<Never, Option<<SendNotSync as Behavior>::Event>> {
        ControlFlow::Continue({
            self.local.set(1);
            self.event.take().map(|value| User::new(MailAddr(1), value))
        })
    }

    async fn next_source(
        &mut self,
    ) -> ControlFlow<Never, Option<<SendNotSync as Behavior>::Event>> {
        ControlFlow::Continue(None)
    }

    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<SendNotSync>>,
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
        actions: &mut Option<ActionsOf<SendNotSync>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<
            ControlFlow<Self::RetirementRequest, Option<<SendNotSync as Behavior>::Event>>,
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

impl Environment<SendNotSync> for Env {
    type Active = Self;
    type Settlement = Settlement;
    type Error = Infallible;
    type Residual = ();
    type RetirementRequest = Never;

    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<SendNotSync>>,
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
        *received = Some(Ok((owner, Interpretation::Complete(Settlement))));
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<SendNotSync>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || environment.is_none() {
            return;
        }
        drop((environment.take(), actions.take()));
        *received = Some(());
    }
}

fn main() {
    let mut driver = Some(Driver::new(
        SendNotSync(Cell::new(0)),
        Env {
            event: Some(Box::new(1)),
            local: Rc::new(Cell::new(0)),
        },
    ));
    let mut received = None;
    let execution = Driver::receive_run(&mut driver, &mut received);
    drop(execution);
    drop((driver, received));
}
