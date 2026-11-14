//! Concrete actors for the single-Node consumer; every fold returns typed Actions.
use super::super::counter::{Counter, CounterMessage};
use super::super::ledger::MailLedger;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Births, ChildChoice,
    ChildCreationOutcome, ChildHead, ChildOccurrence, ChildRole, ChildTail, CreateChild,
    CreationEvent, CreationSequence, CreationSettlement, CreationSettlements, Creations,
    DeclaredChildOccurrence, EstablishedActor, EstablishedDelivery, EstablishedRecipient,
    InitializationTurn, ItemSettlement, MessageProtocol, Never, Protocol, SettledItem, Step,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DrainFailure, EntityActivationError, EntityDefinition, EntityId,
};
use bombay::prelude::StopOnShutdown;
use bombay::{ActorRetirement, ActorRetirementReport, MailAddr};
use std::sync::{Arc, Mutex};
use tokio::task::JoinError;

pub(super) type ManagedCounter = StopOnShutdown<Counter>;
type Children = ChildChoice<ManagedCounter, ChildChoice<MailLedger, Never>>;
type ChildCreations = <Births<Children> as CreationSettlements<MailAddr>>::Settlements;
pub(super) type Exports = MessageProtocol<MailAddr, ActorExports>;

pub(super) struct ActorExports {
    pub(super) counter: EstablishedActor<ManagedCounter>,
    pub(super) sibling: EstablishedActor<ManagedCounter>,
    pub(super) ledger: EstablishedActor<MailLedger>,
}

#[derive(Default)]
pub(super) struct Creator {
    pub(super) counters: Vec<EstablishedActor<ManagedCounter>>,
    pub(super) ledger: Option<EstablishedActor<MailLedger>>,
    pub(super) creations: Vec<ChildCreations>,
}

pub(super) struct CountersChild;

impl ChildRole<Creator> for CountersChild {
    type Child = ManagedCounter;
    type Position = ChildHead;
}

impl ChildOccurrence<Creator> for CountersChild {
    type Resolution = DeclaredChildOccurrence;
}

pub(super) struct LedgerChild;

impl ChildRole<Creator> for LedgerChild {
    type Child = MailLedger;
    type Position = ChildTail<ChildHead>;
}

impl ChildOccurrence<Creator> for LedgerChild {
    type Resolution = DeclaredChildOccurrence;
}

impl Protocol for Creator {
    type Addr = MailAddr;
    type Msg = EstablishedRecipient<Exports>;
}

impl BehaviorBase for Creator {
    type Base = Self;

    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for Creator {
    type Protocol = Self;
    type Event = CreationEvent<MailAddr, Children, EstablishedRecipient<Exports>>;
    type Sends = Vec<EstablishedDelivery<Exports>>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<Children>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let counter = sequence.issue().expect("three actual child IDs fit");
        let sibling = sequence.issue().expect("three actual child IDs fit");
        let ledger = sequence.issue().expect("three actual child IDs fit");
        Ok(Actions::create(
            Creations::one(CreateChild::birth(
                counter,
                Children::Head(Counter::new().stop_on_shutdown()),
            ))
            .and(CreateChild::birth(
                sibling,
                Children::Head(Counter::new().stop_on_shutdown()),
            ))
            .and(CreateChild::birth(
                ledger,
                Children::Tail(ChildChoice::Head(MailLedger {
                    entries: Vec::new(),
                })),
            )),
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            CreationEvent::Settlements(settled) => {
                let original = settled.into_settlement();
                if let CreationSettlement::Settled(children) = &original {
                    for child in children {
                        match child {
                            SettledItem::Attempted(ItemSettlement::Accepted(
                                ChildChoice::Head(ChildCreationOutcome::Established(committed)),
                            )) => self.counters.push(committed.actor()),
                            SettledItem::Attempted(ItemSettlement::Accepted(
                                ChildChoice::Tail(ChildChoice::Head(
                                    ChildCreationOutcome::Established(committed),
                                )),
                            )) => self.ledger = Some(committed.actor()),
                            _ => {}
                        }
                    }
                }
                self.creations.push(original);
                Ok(Actions::cont())
            }
            CreationEvent::User(request) => {
                let ([counter, sibling], Some(ledger)) =
                    (self.counters.as_slice(), self.ledger.as_ref())
                else {
                    return Ok(Actions::stop());
                };
                Ok(Actions::new(
                    vec![EstablishedDelivery::new(
                        request.message,
                        ActorExports {
                            counter: counter.clone(),
                            sibling: sibling.clone(),
                            ledger: ledger.clone(),
                        },
                    )],
                    Creations::empty(),
                    Step::Continue,
                ))
            }
        }
    }
}

pub(super) enum CounterReport {
    #[expect(
        dead_code,
        reason = "retain complete activation failures; this successful fixture rejects that entire variant"
    )]
    Activation(
        EntityId<u64>,
        ActivationId,
        EntityActivationError<Never, ManagedCounter, Never, Vec<Never>>,
    ),
    #[expect(
        dead_code,
        reason = "retain complete admission failures; this successful fixture rejects that entire variant"
    )]
    Admission(EntityId<u64>, AdmissionFailure<CounterMessage>),
    #[expect(
        dead_code,
        reason = "retain complete forced-retirement failures; this successful fixture rejects that entire variant"
    )]
    Forced(EntityId<u64>, ActivationId, DrainFailure),
    Retired(
        EntityId<u64>,
        ActivationId,
        Result<ActorRetirement<ManagedCounter, Never, Vec<Never>>, JoinError>,
        ActorRetirementReport,
    ),
}

pub(super) struct Counters {
    pub(super) reports: Arc<Mutex<Vec<CounterReport>>>,
}

#[derive(Clone, Copy)]
pub(super) struct CountersRole;

impl EntityDefinition for Counters {
    type Id = u64;
    type Behavior = ManagedCounter;
    type Hosts = topology::Topology;
    type HydrationError = Never;
    type Terminal = Never;
    type ChildFailures = Vec<Never>;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the owning hydration port transfers the actor only when polled"
    )]
    async fn hydrate(&self, _: EntityId<u64>) -> Result<ManagedCounter, Never> {
        Ok(Counter::new().stop_on_shutdown())
    }

    fn activation_failed(
        &self,
        id: EntityId<u64>,
        activation: ActivationId,
        failure: EntityActivationError<Never, ManagedCounter, Never, Vec<Never>>,
    ) {
        self.reports
            .lock()
            .unwrap()
            .push(CounterReport::Activation(id, activation, failure));
    }

    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<CounterMessage>) {
        self.reports
            .lock()
            .unwrap()
            .push(CounterReport::Admission(id, failure));
    }

    fn forced_retirement(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        self.reports
            .lock()
            .unwrap()
            .push(CounterReport::Forced(*id, activation, failure));
    }

    fn retired(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        retirement: Result<ActorRetirement<ManagedCounter, Never, Vec<Never>>, JoinError>,
        report: ActorRetirementReport,
    ) {
        self.reports
            .lock()
            .unwrap()
            .push(CounterReport::Retired(*id, activation, retirement, report));
    }
}

bombay::topology! {
    pub(super) topology {
        root: Creator { Counter(CountersChild) => counter; MailLedger(LedgerChild) => ledger; }
        counter in counters {}
        ledger {}
        hosts { counters: Counter; }
        families { counter: Counters; }
    }
}
