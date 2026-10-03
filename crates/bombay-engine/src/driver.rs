//! Universal direct-Behavior Driver.
//!
//! One closed Behavior and one coherent typed environment cross this boundary.
//! Behavior's own [`Activate`] transition initializes the definition once and
//! yields its [`behavior::Active`] value. The Driver then obtains one event,
//! folds that active Behavior directly once, applies the complete action value
//! once, and only then requests another event. It contains no template,
//! routing, mailbox, scheduling, identity, retry, or machine-topology policy.

use core::ops::ControlFlow;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};

use behavior::{
    Actions, Behavior, ClassifySettlement, Never, SettlementStatus, SourceCustody, Step, Stopped,
};

use crate::{ActiveEnvironment, Environment};

/// The complete action value emitted by one closed Behavior decision.
pub type ActionsOf<B> = Actions<
    behavior::BehaviorAddr<B>,
    <B as Behavior>::Ph,
    <B as Behavior>::Sends,
    <B as Behavior>::Birth,
>;

/// The factual reason one Driver execution completed successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion<Request = Never> {
    /// The Behavior explicitly selected [`Step::Stop`].
    Stopped,
    /// The environment permanently exhausted its event source.
    Exhausted,
    /// The environment requested retirement before another event fold.
    /// This fact neither claims Behavior stop, source exhaustion, nor completed cleanup.
    RetirementRequested(Request),
}

/// Why the Driver could not finish custody of a complete action settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SettlementFailure {
    /// Installed initialization effects were lawfully rejected.
    #[error("installed behavior initialization effects were rejected")]
    Rejected,
    /// Action interpretation corrupted the complete retained settlement.
    #[error("action interpretation corrupted its complete settlement")]
    Corrupt,
    /// Source admission closed while a complete settlement remained in custody.
    #[error("source admission closed with a retained settlement")]
    SourceClosed,
}

/// A failure on either side of the Behavior/environment boundary.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DriverError<B, A> {
    /// The Behavior rejected initialization or an event.
    #[error("behavior transition failed")]
    Behavior(#[source] B),
    /// The synchronous pure initialization fold panicked before host commitment.
    #[error("behavior initialization panicked")]
    InitializationPanicked,
    /// The prepared environment rejected initialization commitment or publication.
    #[error("environment activation failed")]
    Activation(#[source] A),
    /// Complete action-settlement custody could not lawfully continue.
    #[error("action settlement failed")]
    Settlement(#[source] SettlementFailure),
}

enum SettlementTurn<S> {
    Offer(S),
    AwaitSource(S),
    Retained(S),
}

impl<S> SettlementTurn<S> {
    fn into_settlement(self) -> S {
        match self {
            Self::Offer(settlement)
            | Self::AwaitSource(settlement)
            | Self::Retained(settlement) => settlement,
        }
    }
}

fn next_progressing_settlement<S>(
    settlements: &mut VecDeque<SettlementTurn<S>>,
) -> Option<(usize, SettlementTurn<S>)> {
    let index = settlements
        .iter()
        .position(|turn| !matches!(turn, SettlementTurn::Retained(_)))?;
    settlements.remove(index).map(|turn| (index, turn))
}

enum ExecutionPhase {
    Initializing,
    Active,
}

enum ActionDecision {
    Continue,
    Stop,
}

impl ActionDecision {
    fn from_step(step: Step<Never, Stopped>) -> Self {
        match step {
            Step::Continue => Self::Continue,
            Step::Goto(phase) => match phase {},
            Step::Stop(_) => Self::Stop,
        }
    }
}

async fn drive_active<B, E, ActivationError>(
    behavior: &mut B,
    environment: &mut E,
    settlements: &mut VecDeque<SettlementTurn<E::Settlement>>,
) -> Result<Completion<E::RetirementRequest>, DriverError<B::Error, ActivationError>>
where
    B: Behavior<Ph = Never>,
    E: ActiveEnvironment<B>,
{
    let mut phase = ExecutionPhase::Initializing;
    loop {
        let event = match next_progressing_settlement(settlements) {
            Some((index, SettlementTurn::Offer(settlement))) => {
                match environment.offer_next(settlement).await {
                    SourceCustody::Exhausted(_) => {}
                    SourceCustody::Retained(settlement) => {
                        settlements.insert(index, SettlementTurn::Retained(settlement));
                    }
                    SourceCustody::Admitted(settlement) => {
                        settlements.insert(index, SettlementTurn::AwaitSource(settlement));
                    }
                    SourceCustody::Closed(settlement) => {
                        settlements.insert(index, SettlementTurn::Offer(settlement));
                        return Err(DriverError::Settlement(SettlementFailure::SourceClosed));
                    }
                }
                continue;
            }
            Some((index, SettlementTurn::AwaitSource(residual))) => {
                settlements.insert(index, SettlementTurn::Offer(residual));
                match environment.next_source().await {
                    ControlFlow::Continue(Some(event)) => event,
                    ControlFlow::Continue(None) => {
                        return Err(DriverError::Settlement(SettlementFailure::SourceClosed));
                    }
                    ControlFlow::Break(request) => {
                        return Ok(Completion::RetirementRequested(request));
                    }
                }
            }
            Some((_, SettlementTurn::Retained(_))) => {
                unreachable!("retained settlements do not progress through source admission")
            }
            None => match phase {
                ExecutionPhase::Initializing => {
                    environment.publish();
                    phase = ExecutionPhase::Active;
                    continue;
                }
                ExecutionPhase::Active => match environment.next().await {
                    ControlFlow::Continue(Some(event)) => event,
                    ControlFlow::Continue(None) => return Ok(Completion::Exhausted),
                    ControlFlow::Break(request) => {
                        return Ok(Completion::RetirementRequested(request));
                    }
                },
            },
        };

        let actions =
            behavior::delegate_transition(behavior, event).map_err(DriverError::Behavior)?;
        let decision = ActionDecision::from_step(actions.become_);
        let interpretation = environment.apply(actions).await;
        let status = interpretation.settlement_status();
        settlements.push_front(SettlementTurn::Offer(interpretation.into_settlement()));
        match (status, decision) {
            (SettlementStatus::Corrupt, _) => {
                return Err(DriverError::Settlement(SettlementFailure::Corrupt));
            }
            (SettlementStatus::Accepted | SettlementStatus::Rejected, ActionDecision::Stop) => {
                return Ok(Completion::Stopped);
            }
            (SettlementStatus::Accepted | SettlementStatus::Rejected, ActionDecision::Continue) => {
            }
        }
    }
}

/// Exact ownership returned after the Driver's one retirement barrier.
///
/// The final concrete Behavior and environment residual coexist with the
/// factual execution disposition. Neither is reconstructed from the
/// disposition or erased behind a runtime-neutral wrapper.
#[must_use = "final behavior and environment custody must be retained or explicitly discharged"]
#[derive(Debug, PartialEq, Eq)]
pub struct DriverRetirement<B, R, E, Request = Never> {
    /// Final concrete Behavior state after its last attempted fold.
    pub behavior: B,
    /// Exact state returned by prepared or active environment retirement.
    pub residual: R,
    /// Factual completion or failure that selected retirement.
    pub disposition: Result<Completion<Request>, E>,
}

/// An uninitialized, affine Driver execution.
///
/// Construction relies on inference: callers pass the final composed Behavior
/// value directly and never need to name its nested wrapper type.
pub struct Driver<B: Behavior, E> {
    behavior: B,
    environment: E,
}

impl<B: Behavior, E> Driver<B, E> {
    #[must_use]
    pub fn new(behavior: B, environment: E) -> Self {
        Self {
            behavior,
            environment,
        }
    }
}

impl<B, E> Driver<B, E>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    /// Consume and run one complete execution.
    ///
    /// Initialization and every event fold happen exactly once. Each complete
    /// action value crosses the environment boundary exactly once before the
    /// Driver requests another event. Every ordinary return retires the
    /// environment; cancellation only drops owned values and does not claim an
    /// asynchronous retirement completed.
    ///
    /// The returned disposition preserves the exact Behavior error when
    /// initialization or a turn fails, or the exact environment error when
    /// local action commitment fails.
    pub async fn run(
        self,
    ) -> DriverRetirement<B, E::Residual, DriverError<B::Error, E::Error>, E::RetirementRequest>
    {
        let Self {
            behavior,
            environment,
        } = self;
        let mut behavior = behavior;
        // A partially mutated behavior is returned as terminal custody only;
        // it is never resumed after unwinding through its pure fold.
        let initialized =
            match catch_unwind(AssertUnwindSafe(|| behavior::initialize(&mut behavior))) {
                Ok(Ok(initialized)) => initialized,
                Ok(Err(error)) => {
                    let residual = environment.retire().await;
                    return DriverRetirement {
                        behavior,
                        residual,
                        disposition: Err(DriverError::Behavior(error)),
                    };
                }
                Err(_) => {
                    let residual = environment.retire().await;
                    return DriverRetirement {
                        behavior,
                        residual,
                        disposition: Err(DriverError::InitializationPanicked),
                    };
                }
            };
        let initialization_decision = ActionDecision::from_step(initialized.become_);
        let (mut environment, interpretation) = match environment.activate(initialized).await {
            Ok(activated) => activated,
            Err((error, residual)) => {
                return DriverRetirement {
                    behavior,
                    residual,
                    disposition: Err(DriverError::Activation(error)),
                };
            }
        };
        let initialization_status = interpretation.settlement_status();
        let initialization = interpretation.into_settlement();
        let (disposition, settlements) = match (initialization_status, initialization_decision) {
            (SettlementStatus::Rejected, _) => (
                Err(DriverError::Settlement(SettlementFailure::Rejected)),
                vec![initialization],
            ),
            (SettlementStatus::Corrupt, _) => (
                Err(DriverError::Settlement(SettlementFailure::Corrupt)),
                vec![initialization],
            ),
            (SettlementStatus::Accepted, ActionDecision::Stop) => {
                (Ok(Completion::Stopped), vec![initialization])
            }
            (SettlementStatus::Accepted, ActionDecision::Continue) => {
                let mut pending = VecDeque::from([SettlementTurn::Offer(initialization)]);
                let disposition = drive_active(&mut behavior, &mut environment, &mut pending).await;
                let settlements = pending
                    .into_iter()
                    .map(SettlementTurn::into_settlement)
                    .collect();
                (disposition, settlements)
            }
        };
        let residual = environment.retire(settlements).await;
        DriverRetirement {
            behavior,
            residual,
            disposition,
        }
    }
}
