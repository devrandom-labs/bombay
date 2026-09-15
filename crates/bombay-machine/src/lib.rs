//! Composable, representable state machines and ordered execution.
//!
//! A reducer is the total function `State × Event → State × Effects`. It does
//! not execute effects, choose a transport, own locks, or prescribe storage.
//! Those omissions are the useful boundary: interpreters may be concurrent,
//! asynchronous, simulated, or model checked without changing the reducer.
//!
//! Entity lifecycle management is one application. Actor supervision,
//! protocol sessions, retry policies, and resource ownership can use the same
//! kernel when their decisions are deterministic and their effects can be
//! represented as data. It is not appropriate for algorithms whose state is
//! intrinsically external or whose correctness depends on hidden I/O.
//!
//! [`Machine`] retains sequential, product, and choice composition as concrete
//! structure. The same value can therefore be executed with [`Machine::step`]
//! and inspected with [`Machine::describe`]. Composition is static and
//! allocation-free: Rust monomorphizes the concrete composition tree.
//!
//! Bombay Entity uses this to keep its executable lifecycle synchronized with
//! topology documentation and reference models. Other Bombay-owned policies
//! may use it only when their decisions are deterministic and representable.
//! It does not replace Bombay's Driver, mailbox, scheduler, or resource
//! ownership: those are interpreters and runtime capabilities, not deterministic
//! machine descriptions.

#![deny(missing_docs)]

#[cfg(test)]
extern crate alloc;

pub mod executor;
mod machine;

pub use machine::{
    Base, Compose, Either, Machine, Product, Routed, Structure, Then, Topology, TopologyError,
    Transition, TriggerId, ValidatedTopology, Vertex, VertexId,
};

/// The value produced by one deterministic reduction.
///
/// A decision must be handled because discarding it also discards its
/// successor state and effect description.
///
/// ```
/// use bombay_machine::Decision;
///
/// let decision = Decision::new(1_u8, ());
/// assert_eq!(decision.state, 1);
/// ```
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// use bombay_machine::Decision;
///
/// Decision { state: 1_u8, effects: () };
/// ```
#[must_use = "a reduction's successor state and effects must be handled"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision<S, F> {
    /// State to install after the reduction.
    pub state: S,
    /// Description of effects for a separate interpreter.
    pub effects: F,
}

impl<S, F> Decision<S, F> {
    /// Pair the next state with its effect description.
    pub const fn new(state: S, effects: F) -> Self {
        Self { state, effects }
    }

    /// Transform only the effect description.
    pub fn map_effects<G>(self, map: impl FnOnce(F) -> G) -> Decision<S, G> {
        Decision::new(self.state, map(self.effects))
    }

    /// Transform only the next state.
    pub fn map_state<T>(self, map: impl FnOnce(S) -> T) -> Decision<T, F> {
        Decision::new(map(self.state), self.effects)
    }

    /// Transform the next state and effect description independently.
    pub fn map<T, G>(
        self,
        state: impl FnOnce(S) -> T,
        effects: impl FnOnce(F) -> G,
    ) -> Decision<T, G> {
        Decision::new(state(self.state), effects(self.effects))
    }
}

/// A deterministic transition algebra.
pub trait Reducer<S, E> {
    /// Effect description returned to an interpreter.
    type Effects;

    /// Reduce one state and event into the next state and effects.
    fn reduce(&self, state: S, event: E) -> Decision<S, Self::Effects>;
}

#[cfg(test)]
mod tests {
    use super::{Decision, Reducer};

    struct Sum;

    impl Reducer<u32, u32> for Sum {
        type Effects = u32;

        fn reduce(&self, state: u32, event: u32) -> Decision<u32, Self::Effects> {
            Decision::new(state + event, event)
        }
    }

    #[test]
    fn reducer_returns_owned_state_and_effects() {
        assert_eq!(Sum.reduce(1, 2), Decision::new(3, 2));
    }
}
