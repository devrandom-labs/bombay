use std::collections::VecDeque;
use std::future::Future;

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Creations, Interpretation, MailAddr,
    Never, NoBirths, SettlementStatus, SourceCustody, Step, User,
};
use bombay_engine::{
    ActionsOf, ActiveEnvironment, Completion, Driver, DriverError, DriverRetirement, Environment,
    SettlementFailure,
};

#[derive(Debug, PartialEq, Eq)]
struct CustodyBehavior {
    value: u64,
    initialization_failure: Option<&'static str>,
    initialization_decision: InitializationDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializationDecision {
    Continue,
    Stop,
}

impl Behavior for CustodyBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        self.value += 1;
        if let Some(error) = self.initialization_failure {
            Err(error)
        } else {
            let decision = match self.initialization_decision {
                InitializationDecision::Continue => Step::Continue,
                InitializationDecision::Stop => Step::Stop(behavior::Stopped),
            };
            Ok(Actions::new(vec![self.value], Creations::empty(), decision))
        }
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.value += event.message;
        match event.message {
            13 => Err("transition"),
            0 => Ok(Actions::new(
                vec![self.value],
                Creations::empty(),
                Step::Stop(behavior::Stopped),
            )),
            _ => Ok(Actions::send(vec![self.value])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResidualPhase {
    Prepared,
    Active,
}

#[derive(Debug, PartialEq, Eq)]
struct Residual {
    phase: ResidualPhase,
    committed: Vec<u64>,
    settlements: Vec<ActionSettlement>,
    remaining_events: Vec<u64>,
    publication: Publication,
    retirements: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publication {
    Withheld,
    Published,
    Repeated,
}

#[derive(Debug, PartialEq, Eq)]
enum ActionSettlement {
    Applied(Vec<u64>),
    Rejected {
        committed: Vec<u64>,
        reason: &'static str,
    },
    Failed {
        committed: Vec<u64>,
        error: &'static str,
    },
}

impl ClassifySettlement for ActionSettlement {
    fn settlement_status(&self) -> SettlementStatus {
        match self {
            Self::Applied(_) => SettlementStatus::Accepted,
            Self::Rejected { .. } => SettlementStatus::Rejected,
            Self::Failed { .. } => SettlementStatus::Corrupt,
        }
    }
}

#[derive(Clone, Copy)]
enum SettlementPlan {
    Accepted,
    Rejected(&'static str),
    Corrupt(&'static str),
}

fn settle_actions(committed: Vec<u64>, plan: SettlementPlan) -> Interpretation<ActionSettlement> {
    match plan {
        SettlementPlan::Accepted => Interpretation::Complete(ActionSettlement::Applied(committed)),
        SettlementPlan::Rejected(reason) => {
            Interpretation::Complete(ActionSettlement::Rejected { committed, reason })
        }
        SettlementPlan::Corrupt(error) => {
            Interpretation::Corrupt(ActionSettlement::Failed { committed, error })
        }
    }
}

struct PreparedEnvironment {
    events: VecDeque<u64>,
    committed: Vec<u64>,
    activation_failure: Option<&'static str>,
    initialization_settlement: SettlementPlan,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
}

struct ActiveCustodyEnvironment {
    events: VecDeque<u64>,
    committed: Vec<u64>,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
    publication: Publication,
}

#[derive(Clone, Copy)]
enum SettlementCustody {
    Exhaust,
    RetainNext,
    Retained,
    AdmitThenRetain,
    FirstSourceAdmitted,
    RetainTransitive,
    OlderResidualAdmitted,
    SecondSourceAdmitted,
    ExhaustRemaining,
}

impl Environment<CustodyBehavior> for PreparedEnvironment {
    type Active = ActiveCustodyEnvironment;
    type Settlement = ActionSettlement;
    type Error = &'static str;
    type Residual = Residual;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(
        mut self,
        actions: ActionsOf<CustodyBehavior>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        let committed = actions.sends;
        self.committed.extend(committed.iter().copied());
        if let Some(error) = self.activation_failure {
            return Err((
                error,
                Residual {
                    phase: ResidualPhase::Prepared,
                    committed: self.committed,
                    settlements: Vec::new(),
                    remaining_events: self.events.into(),
                    publication: Publication::Withheld,
                    retirements: 0,
                },
            ));
        }
        let interpretation = settle_actions(committed, self.initialization_settlement);
        Ok((
            ActiveCustodyEnvironment {
                events: self.events,
                committed: self.committed,
                active_settlement: self.active_settlement,
                settlement_custody: self.settlement_custody,
                publication: Publication::Withheld,
            },
            interpretation,
        ))
    }

    fn retire(self) -> impl Future<Output = Self::Residual> {
        std::future::ready(Residual {
            phase: ResidualPhase::Prepared,
            committed: self.committed,
            settlements: Vec::new(),
            remaining_events: self.events.into(),
            publication: Publication::Withheld,
            retirements: 1,
        })
    }
}

impl ActiveEnvironment<CustodyBehavior> for ActiveCustodyEnvironment {
    type Settlement = ActionSettlement;
    type Residual = Residual;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(&mut self) -> Option<<CustodyBehavior as Behavior>::Event> {
        self.events
            .pop_front()
            .map(|event| User::new(MailAddr(7), event))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next_source(&mut self) -> Option<<CustodyBehavior as Behavior>::Event> {
        let message = match self.settlement_custody {
            SettlementCustody::FirstSourceAdmitted => {
                self.settlement_custody = SettlementCustody::RetainTransitive;
                2
            }
            SettlementCustody::SecondSourceAdmitted => {
                self.settlement_custody = SettlementCustody::ExhaustRemaining;
                4
            }
            _ => panic!("source input was requested without an admitted settlement"),
        };
        Some(User::new(MailAddr(7), message))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn apply(
        &mut self,
        actions: ActionsOf<CustodyBehavior>,
    ) -> Interpretation<Self::Settlement> {
        let committed = actions.sends;
        self.committed.extend(committed.iter().copied());
        settle_actions(committed, self.active_settlement)
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn offer_next(
        &mut self,
        settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        match self.settlement_custody {
            SettlementCustody::Exhaust | SettlementCustody::ExhaustRemaining => {
                SourceCustody::Exhausted(settlement)
            }
            SettlementCustody::RetainNext => {
                self.settlement_custody = SettlementCustody::Retained;
                SourceCustody::Retained(settlement)
            }
            SettlementCustody::Retained => {
                panic!("a retained settlement was offered to its source again")
            }
            SettlementCustody::AdmitThenRetain => {
                self.settlement_custody = SettlementCustody::FirstSourceAdmitted;
                SourceCustody::Admitted(settlement)
            }
            SettlementCustody::RetainTransitive => {
                self.settlement_custody = SettlementCustody::OlderResidualAdmitted;
                SourceCustody::Retained(settlement)
            }
            SettlementCustody::OlderResidualAdmitted => {
                self.settlement_custody = SettlementCustody::SecondSourceAdmitted;
                SourceCustody::Admitted(settlement)
            }
            SettlementCustody::FirstSourceAdmitted | SettlementCustody::SecondSourceAdmitted => {
                panic!("another settlement was offered before the admitted source was read")
            }
        }
    }

    fn publish(&mut self) {
        self.publication = match self.publication {
            Publication::Withheld => Publication::Published,
            Publication::Published | Publication::Repeated => Publication::Repeated,
        };
    }

    fn retire(self, settlements: Vec<Self::Settlement>) -> impl Future<Output = Self::Residual> {
        std::future::ready(Residual {
            phase: ResidualPhase::Active,
            committed: self.committed,
            settlements,
            remaining_events: self.events.into(),
            publication: self.publication,
            retirements: 1,
        })
    }
}

fn driver(
    behavior: CustodyBehavior,
    events: impl IntoIterator<Item = u64>,
    activation_failure: Option<&'static str>,
    initialization_settlement: SettlementPlan,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
) -> Driver<CustodyBehavior, PreparedEnvironment> {
    Driver::new(
        behavior,
        PreparedEnvironment {
            events: events.into_iter().collect(),
            committed: Vec::new(),
            activation_failure,
            initialization_settlement,
            active_settlement,
            settlement_custody,
        },
    )
}

fn assert_retirement(
    retirement: DriverRetirement<
        CustodyBehavior,
        Residual,
        DriverError<&'static str, &'static str>,
    >,
    expected_behavior: CustodyBehavior,
    expected_residual: Residual,
    expected_disposition: Result<Completion, DriverError<&'static str, &'static str>>,
) {
    let expected = DriverRetirement {
        behavior: expected_behavior,
        residual: expected_residual,
        disposition: expected_disposition,
    };
    assert_eq!(retirement, expected);
    drop(retirement);
}

#[tokio::test]
async fn stop_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [3, 0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 8,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![5, 8, 8],
            settlements: vec![ActionSettlement::Applied(vec![8])],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
        },
        Ok(Completion::Stopped),
    );
}

#[tokio::test]
async fn exhaustion_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [2],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![2, 4],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn behavior_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 5,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [13],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 19,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![6],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
        },
        Err(DriverError::Behavior("transition")),
    );
}

#[tokio::test]
async fn initialization_failure_returns_mutated_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 8,
            initialization_failure: Some("initialization"),
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 9,
            initialization_failure: Some("initialization"),
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Prepared,
            committed: vec![],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Withheld,
            retirements: 1,
        },
        Err(DriverError::Behavior("initialization")),
    );
}

#[tokio::test]
async fn activation_failure_returns_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 2,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        Some("activation"),
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 3,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Prepared,
            committed: vec![3],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Withheld,
            retirements: 0,
        },
        Err(DriverError::Activation("activation")),
    );
}

#[tokio::test]
async fn apply_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 10,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [4],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Corrupt("apply"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 15,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![11, 15],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![15],
                error: "apply",
            }],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn retained_source_settlement_reaches_retirement_without_reoffer() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::RetainNext,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Applied(vec![1])],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn retained_transitive_head_does_not_hide_an_older_source_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::AdmitThenRetain,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 7,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1, 3, 7],
            settlements: vec![ActionSettlement::Applied(vec![3])],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn stopping_turn_corruption_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Corrupt("stopping turn"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1, 1],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![1],
                error: "stopping turn",
            }],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn stopping_turn_rejection_preserves_stop_and_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Rejected("stopping turn"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1, 1],
            settlements: vec![ActionSettlement::Rejected {
                committed: vec![1],
                reason: "stopping turn",
            }],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
        },
        Ok(Completion::Stopped),
    );
}

#[tokio::test]
async fn stopping_initialization_rejection_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        [99],
        None,
        SettlementPlan::Rejected("initialization"),
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Rejected {
                committed: vec![1],
                reason: "initialization",
            }],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
        },
        Err(DriverError::Settlement(SettlementFailure::Rejected)),
    );
}

#[tokio::test]
async fn stopping_initialization_corruption_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        [99],
        None,
        SettlementPlan::Corrupt("initialization"),
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        Residual {
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![1],
                error: "initialization",
            }],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}
