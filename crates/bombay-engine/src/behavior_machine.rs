//! Genuine Behavior-to-Transition adapter.
//!
//! [`BehaviorMachine<B>`] implements [`bombay_transition::Machine`], wrapping a
//! [`Behavior`]'s `transition` method into Transition's affine `step` signature.
//! This adapter is the proof that Behavior composes with Transition's machine
//! algebra; because it is a plain `Machine`, it is executor-agnostic.
//!
//! The production [`crate::Driver`] seats the adapter in
//! `ExclusiveExecutor<BehaviorMachine<B>>` and delegates each event through
//! `ExclusiveExecutor::turn`, which invokes [`Machine::step`] and therefore
//! [`Behavior::transition`]. The `SerializedExecutor` and `LinearizedExecutor`
//! paths are exercised only as non-production evidence in
//! `behavior_machine_tests`. Every production event is processed through
//! Transition's `Machine` trait via the executor.
//!
//! # Topology
//!
//! Topology is optional: use [`BehaviorMachine::for_runtime`] when only
//! execution is needed (no topology). Use [`BehaviorMachine::with_topology`]
//! when model checking or topology validation is desired.

use behavior::{Active, Behavior};
use bombay_transition::{Machine, Structure, ValidatedTopology};

/// Wraps a [`Behavior`] as a [`bombay_transition::Machine`].
///
/// This is the canonical adapter proving Behavior fits Transition's machine
/// algebra. Every call to [`Machine::step`] delegates to
/// [`Behavior::transition`], making this the single point where Behavior
/// enters the Transition composition.
///
pub(crate) struct BehaviorMachine<B: Behavior> {
    behavior: Active<B>,
    topology: Option<ValidatedTopology>,
}

impl<B: Behavior> BehaviorMachine<B> {
    /// Wrap a behavior for runtime execution (no topology).
    ///
    /// Use this constructor when the machine will only be stepped — not
    /// described or model-checked. For topology validation, use
    /// `BehaviorMachine::with_topology` (test-only) instead.
    pub(crate) const fn for_runtime(behavior: Active<B>) -> Self {
        Self {
            behavior,
            topology: None,
        }
    }

    /// Wrap a behavior with its descriptive topology.
    ///
    /// The topology must be pre-validated via
    /// [`Topology::validated`](bombay_transition::Topology::validated).
    #[cfg(test)]
    pub(crate) const fn with_topology(behavior: Active<B>, topology: ValidatedTopology) -> Self {
        Self {
            behavior,
            topology: Some(topology),
        }
    }
}

impl<B: Behavior> Machine for BehaviorMachine<B> {
    type Input = B::Event;
    type Output = behavior::BehaviorActed<B>;

    fn step(mut self, input: Self::Input) -> (Self::Output, Self) {
        let output = self.behavior.transition(input);
        (output, self)
    }

    fn describe<V: Structure>(&self, visitor: &mut V) -> V::Output {
        if let Some(t) = &self.topology {
            visitor.base(t.topology())
        } else {
            // Minimal one-state execution-shell topology for runtime use.
            static RUNTIME_VERTICES: &[bombay_transition::Vertex] = &[bombay_transition::Vertex {
                id: bombay_transition::VertexId(0),
                label: "executing",
            }];
            const RUNTIME_TOPOLOGY: bombay_transition::Topology = bombay_transition::Topology {
                name: "behavior-machine (runtime)",
                initial: bombay_transition::VertexId(0),
                vertices: RUNTIME_VERTICES,
                transitions: &[],
            };
            // Safety: one-vertex topology with no edges is trivially valid.
            visitor.base(RUNTIME_TOPOLOGY)
        }
    }
}
