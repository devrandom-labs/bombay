use std::collections::VecDeque;
use std::future::Future;

use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, Step, User};
use bombay_engine::{
    ActionsOf, ActiveEnvironment, Completion, Driver, DriverError, DriverRetirement, Environment,
};

#[derive(Debug, PartialEq, Eq)]
struct CustodyBehavior {
    value: u64,
    initialization_failure: Option<&'static str>,
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
            Ok(Actions::send(vec![self.value]))
        }
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.value += event.message;
        match event.message {
            13 => Err("transition"),
            0 => Ok(Actions::new(
                vec![self.value],
                Vec::new(),
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
}

struct PreparedEnvironment {
    events: VecDeque<u64>,
    committed: Vec<u64>,
    activation_failure: Option<&'static str>,
    apply_failure: Option<&'static str>,
}

struct ActiveCustodyEnvironment {
    events: VecDeque<u64>,
    committed: Vec<u64>,
    apply_failure: Option<&'static str>,
}

impl Environment<CustodyBehavior> for PreparedEnvironment {
    type Active = ActiveCustodyEnvironment;
    type Error = &'static str;
    type Residual = Residual;

    async fn activate(
        mut self,
        actions: ActionsOf<CustodyBehavior>,
    ) -> Result<Self::Active, (Self::Error, Self::Residual)> {
        self.committed.extend(actions.sends);
        if let Some(error) = self.activation_failure {
            return Err((
                error,
                Residual {
                    phase: ResidualPhase::Prepared,
                    committed: self.committed,
                },
            ));
        }
        Ok(ActiveCustodyEnvironment {
            events: self.events,
            committed: self.committed,
            apply_failure: self.apply_failure,
        })
    }

    fn retire(self) -> impl Future<Output = Self::Residual> {
        std::future::ready(Residual {
            phase: ResidualPhase::Prepared,
            committed: self.committed,
        })
    }
}

impl ActiveEnvironment<CustodyBehavior> for ActiveCustodyEnvironment {
    type Error = &'static str;
    type Residual = Residual;

    async fn next(&mut self) -> Option<<CustodyBehavior as Behavior>::Event> {
        self.events
            .pop_front()
            .map(|event| User::new(MailAddr(7), event))
    }

    async fn apply(&mut self, actions: ActionsOf<CustodyBehavior>) -> Result<(), Self::Error> {
        self.committed.extend(actions.sends);
        if let Some(error) = self.apply_failure {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn retire(self) -> impl Future<Output = Self::Residual> {
        std::future::ready(Residual {
            phase: ResidualPhase::Active,
            committed: self.committed,
        })
    }
}

fn driver(
    behavior: CustodyBehavior,
    events: impl IntoIterator<Item = u64>,
    activation_failure: Option<&'static str>,
    apply_failure: Option<&'static str>,
) -> Driver<CustodyBehavior, PreparedEnvironment> {
    Driver::new(
        behavior,
        PreparedEnvironment {
            events: events.into_iter().collect(),
            committed: Vec::new(),
            activation_failure,
            apply_failure,
        },
    )
}

fn assert_retirement(
    retirement: DriverRetirement<
        CustodyBehavior,
        Residual,
        DriverError<&'static str, &'static str, &'static str>,
    >,
    value: u64,
    phase: ResidualPhase,
    committed: &[u64],
    expected_disposition: &Result<
        Completion,
        DriverError<&'static str, &'static str, &'static str>,
    >,
) {
    let DriverRetirement {
        behavior,
        residual,
        disposition,
    } = retirement;
    assert_eq!(behavior.value, value);
    assert_eq!(residual.phase, phase);
    assert_eq!(residual.committed, committed);
    assert_eq!(&disposition, expected_disposition);
}

#[tokio::test]
async fn stop_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
        },
        [3, 0, 99],
        None,
        None,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        8,
        ResidualPhase::Active,
        &[5, 8, 8],
        &Ok(Completion::Stopped),
    );
}

#[tokio::test]
async fn exhaustion_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
        },
        [2],
        None,
        None,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        4,
        ResidualPhase::Active,
        &[2, 4],
        &Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn behavior_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 5,
            initialization_failure: None,
        },
        [13],
        None,
        None,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        19,
        ResidualPhase::Active,
        &[6],
        &Err(DriverError::Behavior("transition")),
    );
}

#[tokio::test]
async fn initialization_failure_returns_mutated_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 8,
            initialization_failure: Some("initialization"),
        },
        [],
        None,
        None,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        9,
        ResidualPhase::Prepared,
        &[],
        &Err(DriverError::Behavior("initialization")),
    );
}

#[tokio::test]
async fn activation_failure_returns_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 2,
            initialization_failure: None,
        },
        [],
        Some("activation"),
        None,
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        3,
        ResidualPhase::Prepared,
        &[3],
        &Err(DriverError::Activation("activation")),
    );
}

#[tokio::test]
async fn apply_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 10,
            initialization_failure: None,
        },
        [4],
        None,
        Some("apply"),
    )
    .run()
    .await;

    assert_retirement(
        retirement,
        15,
        ResidualPhase::Active,
        &[11, 15],
        &Err(DriverError::Environment("apply")),
    );
}
