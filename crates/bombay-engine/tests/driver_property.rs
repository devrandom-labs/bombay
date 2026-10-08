use core::ops::ControlFlow;
use std::collections::VecDeque;

use behavior::{
    Actions, Behavior, BehaviorActed, Births, ClassifySettlement, CreateChild, CreationSequence,
    Creations, EventLayer, Interpretation, MailAddr, Never, SendLayer, SettlementStatus,
    SourceCustody, Step, User,
};
use core::fmt::Debug;

use bombay_engine::{
    ActionsOf, ActiveEnvironment, Completion, Driver, DriverError, Environment, SettlementFailure,
};
use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ActorDecision {
    Continue,
    Stop,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SourceChoice {
    Exhaust,
    Retain,
    Admit(Box<PlannedTurn>),
    AdmitMissing,
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlannedTurn {
    id: u8,
    decision: ActorDecision,
    settlement: SettlementStatus,
    source: SourceChoice,
    first: u8,
    second: u16,
    child: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ActivationChoice {
    Establish,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Script {
    initialization: PlannedTurn,
    activation: ActivationChoice,
    ordinary: Vec<PlannedTurn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Ingress {
    Ordinary(u8),
    Source(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExecutionEvent {
    ActivationFailed,
    Created(u8),
    SentFirst(u8),
    SentSecond(u16),
    Published,
    RequestedOrdinary,
    RequestedSource,
    Offered(u8),
    Retired(Vec<u8>),
    PreparedRetired,
}

struct Directive {
    id: u8,
    settlement: SettlementStatus,
    source: SourceChoice,
    first: u8,
}

struct ScriptBehavior {
    initialization: PlannedTurn,
    creations: CreationSequence,
    delivered_inputs: Vec<Ingress>,
}

impl ScriptBehavior {
    fn actions(&mut self, turn: PlannedTurn) -> BehaviorActed<Self> {
        match turn.decision {
            ActorDecision::Fail => Err("decision failed"),
            ActorDecision::Continue | ActorDecision::Stop => {
                let creates = match turn.child {
                    Some(child) => {
                        let id = self.creations.issue().expect("bounded creation IDs exist");
                        [CreateChild::birth(id, child)].into_iter().collect()
                    }
                    None => Creations::empty(),
                };
                let step = match turn.decision {
                    ActorDecision::Continue => Step::Continue,
                    ActorDecision::Stop => Step::Stop(behavior::Stopped),
                    ActorDecision::Fail => unreachable!("failure returned before actions"),
                };
                Ok(Actions::new(
                    SendLayer::new(
                        vec![Directive {
                            id: turn.id,
                            settlement: turn.settlement,
                            source: turn.source,
                            first: turn.first,
                        }],
                        vec![turn.second],
                    ),
                    creates,
                    step,
                ))
            }
        }
    }
}

impl Behavior for ScriptBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, PlannedTurn>;
    type Event = EventLayer<PlannedTurn, User<MailAddr, PlannedTurn>>;
    type Sends = SendLayer<Vec<Directive>, Vec<u16>>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = Births<u8>;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        self.actions(self.initialization.clone())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let (ingress, turn) = match event {
            EventLayer::Owned(turn) => (Ingress::Source(turn.id), turn),
            EventLayer::Inner(event) => (Ingress::Ordinary(event.message.id), event.message),
        };
        self.delivered_inputs.push(ingress);
        self.actions(turn)
    }
}

struct ScriptEnvironment {
    activation: ActivationChoice,
    ordinary: VecDeque<PlannedTurn>,
    source: VecDeque<PlannedTurn>,
    execution_trace: Vec<ExecutionEvent>,
}

struct ScriptSettlement {
    id: u8,
    status: SettlementStatus,
    source: SourceChoice,
}

impl ClassifySettlement for ScriptSettlement {
    fn settlement_status(&self) -> SettlementStatus {
        self.status
    }
}

impl ScriptEnvironment {
    fn commit(&mut self, actions: ActionsOf<ScriptBehavior>) -> Interpretation<ScriptSettlement> {
        let Actions { sends, creates, .. } = actions;
        for creation in creates {
            let (_, child, _) = creation.into_parts();
            self.execution_trace.push(ExecutionEvent::Created(child));
        }
        let mut first = sends.owned.into_iter();
        let directive = first.next().expect("one first-lane directive");
        let extra = first.next();
        assert!(extra.is_none());
        self.execution_trace
            .push(ExecutionEvent::SentFirst(directive.first));
        self.execution_trace
            .extend(sends.inner.into_iter().map(ExecutionEvent::SentSecond));
        let status = directive.settlement;
        let settlement = ScriptSettlement {
            id: directive.id,
            status,
            source: directive.source,
        };
        match status {
            SettlementStatus::Accepted | SettlementStatus::Rejected => {
                Interpretation::Complete(settlement)
            }
            SettlementStatus::Corrupt => Interpretation::Corrupt(settlement),
        }
    }
}

impl Environment<ScriptBehavior> for ScriptEnvironment {
    type Active = Self;
    type Settlement = ScriptSettlement;
    type Error = &'static str;
    type Residual = (Vec<ExecutionEvent>, Option<ActionsOf<ScriptBehavior>>);
    type RetirementRequest = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<ScriptBehavior>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) {
        if received.is_some() {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        match original.activation {
            ActivationChoice::Establish => {
                let Some(actions) = actions.take() else {
                    return;
                };
                let settlement = original.commit(actions);
                let active = environment
                    .take()
                    .expect("the script environment remains available");
                *received = Some(Ok((active, settlement)));
            }
            ActivationChoice::Fail => {
                original
                    .execution_trace
                    .push(ExecutionEvent::ActivationFailed);
                let original = environment
                    .take()
                    .expect("the rejected script environment remains available");
                *received = Some(Err((
                    "activation failed",
                    (original.execution_trace, actions.take()),
                )));
            }
        }
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<ScriptBehavior>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        original
            .execution_trace
            .push(ExecutionEvent::PreparedRetired);
        let original = environment
            .take()
            .expect("the prepared script environment remains available");
        *received = Some((original.execution_trace, actions.take()));
    }
}

impl ActiveEnvironment<ScriptBehavior> for ScriptEnvironment {
    type Settlement = ScriptSettlement;
    type Residual = (Vec<ExecutionEvent>, Option<ActionsOf<ScriptBehavior>>);
    type RetirementRequest = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> ControlFlow<Never, Option<<ScriptBehavior as Behavior>::Event>> {
        ControlFlow::Continue({
            self.execution_trace.push(ExecutionEvent::RequestedOrdinary);
            self.ordinary
                .pop_front()
                .map(|turn| EventLayer::Inner(User::new(MailAddr(7), turn)))
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next_source(
        &mut self,
    ) -> ControlFlow<Never, Option<<ScriptBehavior as Behavior>::Event>> {
        ControlFlow::Continue({
            self.execution_trace.push(ExecutionEvent::RequestedSource);
            self.source.pop_front().map(EventLayer::Owned)
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<ScriptBehavior>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        if let Some(actions) = actions.take() {
            *received = Some(self.commit(actions));
        }
    }

    async fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(mut settlement) = settlement.take() else {
            return;
        };
        self.execution_trace
            .push(ExecutionEvent::Offered(settlement.id));
        let offered = match std::mem::replace(&mut settlement.source, SourceChoice::Exhaust) {
            SourceChoice::Exhaust => SourceCustody::Exhausted(settlement),
            SourceChoice::Retain => SourceCustody::Retained(settlement),
            SourceChoice::Admit(turn) => {
                self.source.push_back(*turn);
                SourceCustody::Admitted(settlement)
            }
            SourceChoice::AdmitMissing => SourceCustody::Admitted(settlement),
            SourceChoice::Close => SourceCustody::Closed(settlement),
        };
        *received = Some(offered);
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        self.execution_trace.push(ExecutionEvent::Published);
        ControlFlow::Continue(())
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<ScriptBehavior>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<ControlFlow<Never, Option<<ScriptBehavior as Behavior>::Event>>>,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some()
            || interpretation.is_some()
            || source.is_some()
            || source_index.is_some()
            || ingress.is_some()
        {
            return;
        }
        let Some(original) = environment.as_mut() else {
            return;
        };
        let Some(settlements) = settlements.take() else {
            return;
        };
        original.execution_trace.push(ExecutionEvent::Retired(
            settlements
                .into_iter()
                .map(|settlement| settlement.id)
                .collect(),
        ));
        let original = environment
            .take()
            .expect("the retiring script environment remains available");
        *received = Some((original.execution_trace, actions.take()));
    }
}

type Disposition = Result<Completion, DriverError<&'static str, &'static str>>;

fn execute(script: Script) -> (Disposition, Vec<Ingress>, Vec<ExecutionEvent>) {
    let expected_initialization = script.initialization.clone();
    let expected_activation = script.activation.clone();
    let behavior = ScriptBehavior {
        initialization: script.initialization,
        creations: CreationSequence::new(),
        delivered_inputs: Vec::new(),
    };
    let environment = ScriptEnvironment {
        activation: script.activation,
        ordinary: script.ordinary.into(),
        source: VecDeque::new(),
        execution_trace: Vec::new(),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("local runtime builds");
    let retirement = runtime
        .block_on(Driver::new(behavior, environment).run())
        .unwrap_or_else(|driver| {
            drop(driver);
            panic!("the selected script host completes retirement")
        });
    let (trace, uncommitted) = retirement.residual;
    match (
        expected_activation,
        &expected_initialization.decision,
        uncommitted,
    ) {
        (ActivationChoice::Fail, ActorDecision::Continue | ActorDecision::Stop, Some(actions)) => {
            let Actions {
                sends,
                creates,
                become_,
            } = actions;
            let children = creates
                .into_iter()
                .map(|creation| {
                    let (id, child, kind) = creation.into_parts();
                    (id, kind, child)
                })
                .collect::<Vec<_>>();
            let expected_children = expected_initialization
                .child
                .into_iter()
                .collect::<Vec<_>>();
            let actual_children = children
                .iter()
                .map(|(_, _, child)| *child)
                .collect::<Vec<_>>();
            assert_eq!(actual_children, expected_children);
            assert!(
                children
                    .iter()
                    .all(|(_, kind, _)| matches!(kind, behavior::CreationKind::Birth))
            );
            let [directive] = sends.owned.as_slice() else {
                panic!("one original initialization directive")
            };
            assert_eq!(directive.id, expected_initialization.id);
            assert_eq!(directive.settlement, expected_initialization.settlement);
            assert_eq!(directive.source, expected_initialization.source);
            assert_eq!(directive.first, expected_initialization.first);
            assert_eq!(sends.inner, [expected_initialization.second]);
            match expected_initialization.decision {
                ActorDecision::Continue => assert!(matches!(become_, Step::Continue)),
                ActorDecision::Stop => assert!(matches!(become_, Step::Stop(_))),
                ActorDecision::Fail => unreachable!("a failed fold supplies no actions"),
            }
        }
        (ActivationChoice::Establish, _, None)
        | (ActivationChoice::Fail, ActorDecision::Fail, None) => {}
        _ => panic!("only a rejected host activation retains the exact uncommitted initialization"),
    }
    (
        retirement.disposition,
        retirement.behavior.delivered_inputs,
        trace,
    )
}

struct CausalModel {
    execution_trace: Vec<ExecutionEvent>,
    delivered_inputs: Vec<Ingress>,
    pending: Vec<u8>,
}

enum ModelEnd {
    Stopped,
    Failure(DriverError<&'static str, &'static str>),
}

impl CausalModel {
    fn commit(&mut self, turn: &PlannedTurn) {
        if let Some(child) = turn.child {
            self.execution_trace.push(ExecutionEvent::Created(child));
        }
        self.execution_trace
            .push(ExecutionEvent::SentFirst(turn.first));
        self.execution_trace
            .push(ExecutionEvent::SentSecond(turn.second));
        self.pending.insert(0, turn.id);
    }

    fn discharge(&mut self, id: u8) {
        let index = self
            .pending
            .iter()
            .position(|pending| *pending == id)
            .expect("the source settlement remains owned");
        self.pending.remove(index);
    }

    fn offer(&mut self, turn: &PlannedTurn) -> Result<(), ModelEnd> {
        self.execution_trace.push(ExecutionEvent::Offered(turn.id));
        match &turn.source {
            SourceChoice::Exhaust => self.discharge(turn.id),
            SourceChoice::Retain => {}
            SourceChoice::Close => {
                return Err(ModelEnd::Failure(DriverError::Settlement(
                    SettlementFailure::SourceClosed,
                )));
            }
            SourceChoice::AdmitMissing => {
                self.execution_trace.push(ExecutionEvent::RequestedSource);
                return Err(ModelEnd::Failure(DriverError::Settlement(
                    SettlementFailure::SourceClosed,
                )));
            }
            SourceChoice::Admit(source) => {
                self.execution_trace.push(ExecutionEvent::RequestedSource);
                self.decision(source, Ingress::Source(source.id))?;
                self.execution_trace.push(ExecutionEvent::Offered(turn.id));
                self.discharge(turn.id);
            }
        }
        Ok(())
    }

    fn decision(&mut self, turn: &PlannedTurn, ingress: Ingress) -> Result<(), ModelEnd> {
        self.delivered_inputs.push(ingress);
        if matches!(turn.decision, ActorDecision::Fail) {
            return Err(ModelEnd::Failure(DriverError::Behavior("decision failed")));
        }
        self.commit(turn);
        if matches!(turn.settlement, SettlementStatus::Corrupt) {
            return Err(ModelEnd::Failure(DriverError::Settlement(
                SettlementFailure::Corrupt,
            )));
        }
        if matches!(turn.decision, ActorDecision::Stop) {
            return Err(ModelEnd::Stopped);
        }
        self.offer(turn)
    }
}

fn expected(script: &Script) -> (Disposition, Vec<Ingress>, Vec<ExecutionEvent>) {
    let mut model = CausalModel {
        execution_trace: Vec::new(),
        delivered_inputs: Vec::new(),
        pending: Vec::new(),
    };
    let initial = &script.initialization;
    let disposition = if matches!(initial.decision, ActorDecision::Fail) {
        model.execution_trace.push(ExecutionEvent::PreparedRetired);
        Err(DriverError::Behavior("decision failed"))
    } else if matches!(script.activation, ActivationChoice::Fail) {
        model.execution_trace.push(ExecutionEvent::ActivationFailed);
        Err(DriverError::Activation("activation failed"))
    } else {
        model.commit(initial);
        let outcome = match (initial.settlement, &initial.decision) {
            (SettlementStatus::Rejected, _) => Err(ModelEnd::Failure(DriverError::Settlement(
                SettlementFailure::Rejected,
            ))),
            (SettlementStatus::Corrupt, _) => Err(ModelEnd::Failure(DriverError::Settlement(
                SettlementFailure::Corrupt,
            ))),
            (SettlementStatus::Accepted, ActorDecision::Stop) => Err(ModelEnd::Stopped),
            (SettlementStatus::Accepted, ActorDecision::Continue) => {
                if let Err(end) = model.offer(initial) {
                    Err(end)
                } else {
                    model.execution_trace.push(ExecutionEvent::Published);
                    let mut ordinary = script.ordinary.iter();
                    loop {
                        model
                            .execution_trace
                            .push(ExecutionEvent::RequestedOrdinary);
                        let Some(turn) = ordinary.next() else {
                            break Ok(Completion::Exhausted);
                        };
                        match model.decision(turn, Ingress::Ordinary(turn.id)) {
                            Ok(()) => {}
                            Err(end) => break Err(end),
                        }
                    }
                }
            }
            (SettlementStatus::Accepted, ActorDecision::Fail) => {
                unreachable!("initial failure retired the prepared environment")
            }
        };
        model
            .execution_trace
            .push(ExecutionEvent::Retired(model.pending.clone()));
        match outcome {
            Ok(completion) => Ok(completion),
            Err(ModelEnd::Stopped) => Ok(Completion::Stopped),
            Err(ModelEnd::Failure(error)) => Err(error),
        }
    };
    (disposition, model.delivered_inputs, model.execution_trace)
}

fn actor_decision_strategy() -> impl Strategy<Value = ActorDecision> {
    prop_oneof![
        Just(ActorDecision::Continue),
        Just(ActorDecision::Stop),
        Just(ActorDecision::Fail)
    ]
}

fn settlement_strategy() -> impl Strategy<Value = SettlementStatus> {
    prop_oneof![
        Just(SettlementStatus::Accepted),
        Just(SettlementStatus::Rejected),
        Just(SettlementStatus::Corrupt),
    ]
}

fn source_strategy() -> impl Strategy<Value = SourceChoice> {
    prop_oneof![
        Just(SourceChoice::Exhaust),
        Just(SourceChoice::Retain),
        Just(SourceChoice::AdmitMissing),
        Just(SourceChoice::Close),
    ]
}

fn turn_strategy() -> BoxedStrategy<PlannedTurn> {
    let leaf = (
        any::<u8>(),
        actor_decision_strategy(),
        settlement_strategy(),
        source_strategy(),
        any::<u8>(),
        any::<u16>(),
        prop::option::of(any::<u8>()),
    )
        .prop_map(
            |(id, decision, settlement, source, first, second, child)| PlannedTurn {
                id,
                decision,
                settlement,
                source,
                first,
                second,
                child,
            },
        );
    leaf.prop_recursive(2, 32, 4, |inner| {
        (
            any::<u8>(),
            actor_decision_strategy(),
            settlement_strategy(),
            inner.prop_map(|turn| SourceChoice::Admit(Box::new(turn))),
            any::<u8>(),
            any::<u16>(),
            prop::option::of(any::<u8>()),
        )
            .prop_map(|(id, decision, settlement, source, first, second, child)| {
                PlannedTurn {
                    id,
                    decision,
                    settlement,
                    source,
                    first,
                    second,
                    child,
                }
            })
    })
    .boxed()
}

fn script_strategy() -> impl Strategy<Value = Script> {
    (
        turn_strategy(),
        prop_oneof![
            Just(ActivationChoice::Establish),
            Just(ActivationChoice::Fail)
        ],
        prop::collection::vec(turn_strategy(), 0..5),
    )
        .prop_map(|(initialization, activation, ordinary)| Script {
            initialization,
            activation,
            ordinary,
        })
}

fn matrix_turn(
    id: u8,
    decision: ActorDecision,
    settlement: SettlementStatus,
    source: SourceChoice,
) -> PlannedTurn {
    PlannedTurn {
        id,
        decision,
        settlement,
        source,
        first: id.wrapping_add(1),
        second: u16::from(id) + 100,
        child: Some(id),
    }
}

#[test]
fn bounded_phase_matrix_covers_failure_source_and_terminal_edges() {
    let decisions = [
        ActorDecision::Continue,
        ActorDecision::Stop,
        ActorDecision::Fail,
    ];
    let statuses = [
        SettlementStatus::Accepted,
        SettlementStatus::Rejected,
        SettlementStatus::Corrupt,
    ];
    let sources = [
        SourceChoice::Exhaust,
        SourceChoice::Retain,
        SourceChoice::AdmitMissing,
        SourceChoice::Close,
        SourceChoice::Admit(Box::new(matrix_turn(
            3,
            ActorDecision::Continue,
            SettlementStatus::Accepted,
            SourceChoice::Exhaust,
        ))),
    ];
    for decision in &decisions {
        for status in statuses {
            for source in &sources {
                let initial = matrix_turn(1, decision.clone(), status, source.clone());
                let script = Script {
                    initialization: initial,
                    activation: ActivationChoice::Establish,
                    ordinary: vec![matrix_turn(
                        2,
                        ActorDecision::Continue,
                        SettlementStatus::Accepted,
                        SourceChoice::Exhaust,
                    )],
                };
                let (actual_disposition, actual_folded, actual_trace) = execute(script.clone());
                let (expected_disposition, expected_folded, expected_trace) = expected(&script);
                assert_driver_disposition(&actual_disposition, &expected_disposition);
                assert_eq!(actual_folded, expected_folded);
                assert_eq!(actual_trace, expected_trace);

                let ordinary = matrix_turn(4, decision.clone(), status, source.clone());
                let script = Script {
                    initialization: matrix_turn(
                        0,
                        ActorDecision::Continue,
                        SettlementStatus::Accepted,
                        SourceChoice::Exhaust,
                    ),
                    activation: ActivationChoice::Establish,
                    ordinary: vec![
                        ordinary,
                        matrix_turn(
                            5,
                            ActorDecision::Stop,
                            SettlementStatus::Accepted,
                            SourceChoice::Exhaust,
                        ),
                    ],
                };
                let (actual_disposition, actual_folded, actual_trace) = execute(script.clone());
                let (expected_disposition, expected_folded, expected_trace) = expected(&script);
                assert_driver_disposition(&actual_disposition, &expected_disposition);
                assert_eq!(actual_folded, expected_folded);
                assert_eq!(actual_trace, expected_trace);
            }
        }
    }
    for activation in [ActivationChoice::Establish, ActivationChoice::Fail] {
        let script = Script {
            initialization: matrix_turn(
                0,
                ActorDecision::Continue,
                SettlementStatus::Accepted,
                SourceChoice::Exhaust,
            ),
            activation,
            ordinary: Vec::new(),
        };
        let (actual_disposition, actual_folded, actual_trace) = execute(script.clone());
        let (expected_disposition, expected_folded, expected_trace) = expected(&script);
        assert_driver_disposition(&actual_disposition, &expected_disposition);
        assert_eq!(actual_folded, expected_folded);
        assert_eq!(actual_trace, expected_trace);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn typed_scripts_match_the_independent_causal_model(script in script_strategy()) {
        let (actual_disposition, actual_folded, actual_trace) = execute(script.clone());
                let (expected_disposition, expected_folded, expected_trace) = expected(&script);
                assert_driver_disposition(&actual_disposition, &expected_disposition);
                assert_eq!(actual_folded, expected_folded);
                assert_eq!(actual_trace, expected_trace);
    }
}

fn assert_driver_disposition<Request: PartialEq + Debug>(
    actual: &Result<Completion<Request>, DriverError<&'static str, &'static str>>,
    expected: &Result<Completion<Request>, DriverError<&'static str, &'static str>>,
) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
        (Err(DriverError::Behavior(actual)), Err(DriverError::Behavior(expected)))
        | (Err(DriverError::Activation(actual)), Err(DriverError::Activation(expected))) => {
            assert_eq!(actual, expected);
        }
        (Err(DriverError::Settlement(actual)), Err(DriverError::Settlement(expected))) => {
            assert_eq!(actual, expected);
        }
        (
            Err(DriverError::InitializationPanicked(_)),
            Err(DriverError::InitializationPanicked(_)),
        ) => {}
        (actual, expected) => panic!(
            "unexpected disposition in this non-native-panic law: actual={actual:?}, expected={expected:?}"
        ),
    }
}

#[test]
fn rejected_source_continuation_during_initialization_settles_before_publication() {
    let script = Script {
        initialization: PlannedTurn {
            id: 1,
            decision: ActorDecision::Continue,
            settlement: SettlementStatus::Accepted,
            source: SourceChoice::Admit(Box::new(PlannedTurn {
                id: 2,
                decision: ActorDecision::Continue,
                settlement: SettlementStatus::Rejected,
                source: SourceChoice::Exhaust,
                first: 22,
                second: 222,
                child: None,
            })),
            first: 11,
            second: 111,
            child: None,
        },
        activation: ActivationChoice::Establish,
        ordinary: Vec::new(),
    };
    let (actual_disposition, actual_inputs, actual_trace) = execute(script);
    assert_eq!(actual_inputs, [Ingress::Source(2)]);
    assert_eq!(
        actual_trace,
        [
            ExecutionEvent::SentFirst(11),
            ExecutionEvent::SentSecond(111),
            ExecutionEvent::Offered(1),
            ExecutionEvent::RequestedSource,
            ExecutionEvent::SentFirst(22),
            ExecutionEvent::SentSecond(222),
            ExecutionEvent::Offered(2),
            ExecutionEvent::Offered(1),
            ExecutionEvent::Published,
            ExecutionEvent::RequestedOrdinary,
            ExecutionEvent::Retired(Vec::new()),
        ],
        "source continuation disposition={actual_disposition:?}"
    );
    assert_driver_disposition(&actual_disposition, &Ok(Completion::Exhausted));
}

#[test]
fn rejected_source_stop_during_initialization_preserves_stop_and_settlement_order() {
    let script = Script {
        initialization: PlannedTurn {
            id: 1,
            decision: ActorDecision::Continue,
            settlement: SettlementStatus::Accepted,
            source: SourceChoice::Admit(Box::new(PlannedTurn {
                id: 2,
                decision: ActorDecision::Stop,
                settlement: SettlementStatus::Rejected,
                source: SourceChoice::Exhaust,
                first: 22,
                second: 222,
                child: None,
            })),
            first: 11,
            second: 111,
            child: None,
        },
        activation: ActivationChoice::Establish,
        ordinary: Vec::new(),
    };
    let (actual_disposition, actual_inputs, actual_trace) = execute(script);
    assert_eq!(actual_inputs, [Ingress::Source(2)]);
    assert_eq!(
        actual_trace,
        [
            ExecutionEvent::SentFirst(11),
            ExecutionEvent::SentSecond(111),
            ExecutionEvent::Offered(1),
            ExecutionEvent::RequestedSource,
            ExecutionEvent::SentFirst(22),
            ExecutionEvent::SentSecond(222),
            ExecutionEvent::Retired(vec![2, 1]),
        ],
        "source stop disposition={actual_disposition:?}"
    );
    assert_driver_disposition(&actual_disposition, &Ok(Completion::Stopped));
}
