//! The supervising capability: [`Strategy`] restart-set markers,
//! [`Supervising`], and the [`HasSupervising`] loop-participation half —
//! whose supertrait ([`HasWatching`]) IS the composition law.

use core::marker::PhantomData;

use crate::restart::SupervisionStrategy;

use super::{Actor, HasWatching};

/// A restart-set strategy named as a TYPE — the [`Supervising`] plug.
///
/// There is deliberately no default marker: bounded supervision names its
/// strategy by construction (the shipped `OneForOne` trait default is
/// dropped, card #280).
pub trait Strategy: Send + 'static {
    /// The runtime strategy this marker names.
    const STRATEGY: SupervisionStrategy;
}

/// A failed child is rebuilt alone; siblings never observe it.
pub struct OneForOne;

/// A failed child restarts itself and every YOUNGER sibling (ADR-0014).
pub struct RestForOne;

/// A failed child restarts the whole set (ADR-0014).
pub struct OneForAll;

impl Strategy for OneForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForOne;
}

impl Strategy for RestForOne {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::RestForOne;
}

impl Strategy for OneForAll {
    const STRATEGY: SupervisionStrategy = SupervisionStrategy::OneForAll;
}

/// The supervising capability (ADR-0026 stage 3).
///
/// Plugged as a cap-set field, it runs the actor on the three-arm
/// supervised loop — children are registered via the handle's `supervise`
/// verb, rebuilt under their per-child
/// [`RestartConfig`](crate::restart::RestartConfig) (path unchanged) and
/// this set-level strategy. Requires [`Watching`](super::Watching) in the same set
/// (compile-time law — [`HasSupervising`] bounds [`HasWatching`]). Zero
/// runtime state: the strategy rides the type.
pub struct Supervising<SS: Strategy> {
    strategy: PhantomData<SS>,
}

impl<SS: Strategy> Supervising<SS> {
    /// Builds the (stateless) supervising capability.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            strategy: PhantomData,
        }
    }
}

impl<SS: Strategy> Default for Supervising<SS> {
    fn default() -> Self {
        Self::new()
    }
}

/// "This cap set supervises" — the loop-participation half of
/// [`Supervising`].
///
/// The supertrait IS the composition law (ADR-0026 constraint 3):
/// supervising-without-watching is unsatisfiable, so an invalid stack
/// does not compile. Derive-emitted from a `Supervising<SS>` field.
pub trait HasSupervising<A: Actor>: HasWatching<A> {
    /// The declared restart-set strategy.
    type Strat: Strategy;
}

#[cfg(test)]
mod tests {
    use super::{OneForAll, OneForOne, RestForOne, Strategy};
    use crate::restart::SupervisionStrategy;

    /// The three strategy markers name exactly their runtime strategy —
    /// kills value-swap mutants on the `STRATEGY` consts.
    #[test]
    fn strategy_markers_name_their_runtime_strategy() {
        assert_eq!(
            <OneForOne as Strategy>::STRATEGY,
            SupervisionStrategy::OneForOne
        );
        assert_eq!(
            <RestForOne as Strategy>::STRATEGY,
            SupervisionStrategy::RestForOne
        );
        assert_eq!(
            <OneForAll as Strategy>::STRATEGY,
            SupervisionStrategy::OneForAll
        );
    }
}
