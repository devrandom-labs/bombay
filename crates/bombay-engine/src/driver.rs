//! Universal direct-Behavior Driver.
//!
//! One closed Behavior and one coherent typed environment cross this boundary.
//! Behavior's own [`Activate`] transition initializes the definition once and
//! yields its [`behavior::Active`] value. The Driver then obtains one event,
//! folds that active Behavior directly once, applies the complete action value
//! once, and only then requests another event. It contains no template,
//! routing, mailbox, scheduling, identity, retry, or machine-topology policy.

use behavior::{Actions, Behavior, Never, Step};

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
pub enum Completion {
    /// The Behavior explicitly selected [`Step::Stop`].
    Stopped,
    /// The environment permanently exhausted its event source.
    Exhausted,
}

/// A failure on either side of the Behavior/environment boundary.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DriverError<B, A, E = A> {
    /// The Behavior rejected initialization or an event.
    #[error("behavior transition failed")]
    Behavior(#[source] B),
    /// The prepared environment rejected initialization commitment or publication.
    #[error("environment activation failed")]
    Activation(#[source] A),
    /// The active environment failed while applying a successful decision's actions.
    #[error("environment failed while applying behavior actions")]
    Environment(#[source] E),
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
    /// # Errors
    ///
    /// Returns the exact Behavior error when initialization or a turn fails,
    /// or the exact environment error when local action commitment fails.
    pub async fn run(
        self,
    ) -> Result<
        Completion,
        DriverError<B::Error, E::Error, <E::Active as ActiveEnvironment<B>>::Error>,
    > {
        let Self {
            behavior,
            environment,
        } = self;
        let mut behavior = behavior;
        let initialized = match behavior::initialize(&mut behavior) {
            Ok(initialized) => initialized,
            Err(error) => {
                return Err(DriverError::Behavior(error));
            }
        };
        let stopped = matches!(&initialized.become_, Step::Stop(_));
        let mut environment = environment
            .activate(initialized)
            .await
            .map_err(DriverError::Activation)?;
        let result = if stopped {
            Ok(Completion::Stopped)
        } else {
            loop {
                let Some(event) = environment.next().await else {
                    break Ok(Completion::Exhausted);
                };
                let actions = match behavior::delegate_transition(&mut behavior, event) {
                    Ok(actions) => actions,
                    Err(error) => break Err(DriverError::Behavior(error)),
                };
                let stopped = matches!(&actions.become_, Step::Stop(_));
                match environment.apply(actions).await {
                    Ok(()) if stopped => break Ok(Completion::Stopped),
                    Ok(()) => {}
                    Err(error) => break Err(DriverError::Environment(error)),
                }
            }
        };

        environment.retire().await;
        result
    }
}
