use std::sync::OnceLock;

use bombay_machine::{
    Base, Reducer, Topology, TopologyError, Transition, TriggerId, ValidatedTopology, Vertex,
    VertexId,
};

use super::{EntitySlot, SlotEffectBatch, SlotEvent, SlotReducer};

/// Stable lifecycle phase identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LifecyclePhase {
    /// No incarnation exists.
    Inactive,
    /// One activation is in flight.
    Activating,
    /// An incarnation admits commands.
    Active,
    /// Admission is closed while processing drains.
    Draining,
    /// Exact termination is awaited.
    Retiring,
}

impl LifecyclePhase {
    const fn id(self) -> VertexId {
        VertexId(self as u8)
    }
}

/// Stable identity of an input relevant to lifecycle topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LifecycleTrigger {
    /// Claim a fresh activation.
    ClaimActivation,
    /// Dispatch a command.
    Dispatch,
    /// Cancel an activation waiter.
    CancelWaiter,
    /// Activation committed.
    ActivationSucceeded,
    /// Activation failed.
    ActivationFailed,
    /// A reserved delivery resolved.
    DeliveryResolved,
    /// Close admission.
    BeginDrain,
    /// Processing fence acknowledged.
    FenceAcknowledged,
    /// Force bounded draining to finish.
    ForceDrain,
    /// Exact incarnation terminated.
    Terminated,
}

impl LifecycleTrigger {
    const fn id(self) -> TriggerId {
        TriggerId(self as u8)
    }
}

/// Authoritative identity of one phase-changing lifecycle edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LifecycleEdge {
    /// Inactive to activating.
    ClaimActivation,
    /// Activating to active.
    ActivationSucceeded,
    /// Activating to inactive.
    ActivationFailed,
    /// Active to draining.
    BeginDrain,
    /// Draining to retiring after the fence.
    FenceAcknowledged,
    /// Draining to retiring after a bounded failure.
    ForceDrain,
    /// Retiring to inactive after exact termination.
    Terminated,
}

impl LifecycleEdge {
    /// Every declared lifecycle edge in deterministic rendering order.
    pub const ALL: [Self; 7] = [
        Self::ClaimActivation,
        Self::ActivationSucceeded,
        Self::ActivationFailed,
        Self::BeginDrain,
        Self::FenceAcknowledged,
        Self::ForceDrain,
        Self::Terminated,
    ];

    /// Return the strongly typed source, trigger, and destination.
    #[must_use]
    pub const fn endpoints(self) -> (LifecyclePhase, LifecycleTrigger, LifecyclePhase) {
        match self {
            Self::ClaimActivation => (
                LifecyclePhase::Inactive,
                LifecycleTrigger::ClaimActivation,
                LifecyclePhase::Activating,
            ),
            Self::ActivationSucceeded => (
                LifecyclePhase::Activating,
                LifecycleTrigger::ActivationSucceeded,
                LifecyclePhase::Active,
            ),
            Self::ActivationFailed => (
                LifecyclePhase::Activating,
                LifecycleTrigger::ActivationFailed,
                LifecyclePhase::Inactive,
            ),
            Self::BeginDrain => (
                LifecyclePhase::Active,
                LifecycleTrigger::BeginDrain,
                LifecyclePhase::Draining,
            ),
            Self::FenceAcknowledged => (
                LifecyclePhase::Draining,
                LifecycleTrigger::FenceAcknowledged,
                LifecyclePhase::Retiring,
            ),
            Self::ForceDrain => (
                LifecyclePhase::Draining,
                LifecycleTrigger::ForceDrain,
                LifecyclePhase::Retiring,
            ),
            Self::Terminated => (
                LifecyclePhase::Retiring,
                LifecycleTrigger::Terminated,
                LifecyclePhase::Inactive,
            ),
        }
    }

    const fn transition(self) -> Transition {
        let (from, trigger, to) = self.endpoints();
        Transition {
            from: from.id(),
            trigger: trigger.id(),
            to: to.id(),
            label: trigger_label(trigger),
        }
    }

    /// Test whether this exact typed edge is declared by the lifecycle topology.
    #[must_use]
    pub fn is_declared(self) -> bool {
        LIFECYCLE_TOPOLOGY.transitions.contains(&self.transition())
    }
}

/// Evidence produced by one lifecycle execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionEvidence {
    /// Execution traversed this exact declared edge.
    Traversed(LifecycleEdge),
    /// Input was handled without changing phase.
    SelfLoop {
        /// Phase retained by the execution.
        phase: LifecyclePhase,
        /// Handled input identity.
        trigger: LifecycleTrigger,
    },
    /// Input was irrelevant, invalid for the phase, or stale.
    Ignored {
        /// Phase retained by the execution.
        phase: LifecyclePhase,
        /// Ignored input identity.
        trigger: LifecycleTrigger,
    },
}

/// Output of one executable lifecycle-machine step.
#[derive(Debug)]
pub struct LifecycleOutput<C, E, L> {
    /// Ordered effects for the runtime interpreter.
    pub effects: SlotEffectBatch<C, E, L>,
    /// Checked phase-transition evidence.
    pub evidence: TransitionEvidence,
    pub(crate) activation_id: Option<super::ActivationId>,
}

const VERTICES: &[Vertex] = &[
    Vertex {
        id: LifecyclePhase::Inactive.id(),
        label: "inactive",
    },
    Vertex {
        id: LifecyclePhase::Activating.id(),
        label: "activating",
    },
    Vertex {
        id: LifecyclePhase::Active.id(),
        label: "active",
    },
    Vertex {
        id: LifecyclePhase::Draining.id(),
        label: "draining",
    },
    Vertex {
        id: LifecyclePhase::Retiring.id(),
        label: "retiring",
    },
];

const TRANSITIONS: &[Transition] = &[
    LifecycleEdge::ClaimActivation.transition(),
    LifecycleEdge::ActivationSucceeded.transition(),
    LifecycleEdge::ActivationFailed.transition(),
    LifecycleEdge::BeginDrain.transition(),
    LifecycleEdge::FenceAcknowledged.transition(),
    LifecycleEdge::ForceDrain.transition(),
    LifecycleEdge::Terminated.transition(),
];

/// Authoritative lifecycle topology.
pub const LIFECYCLE_TOPOLOGY: Topology = Topology {
    name: "entity_lifecycle",
    initial: LifecyclePhase::Inactive.id(),
    vertices: VERTICES,
    transitions: TRANSITIONS,
};

/// Lifecycle-specific structural validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleTopologyError {
    /// Generic topology validation failed.
    #[error("generic topology validation failed")]
    Structure(#[from] TopologyError),
    /// One required lifecycle edge is absent.
    #[error("required lifecycle edge is absent")]
    Missing(LifecycleEdge),
    /// A draining or retiring edge could reopen admission.
    #[error("drain or retirement could reopen admission")]
    ReopensAdmission,
}

/// Validate graph properties and required entity lifecycle invariants.
///
/// # Errors
///
/// Returns the first generic structural or lifecycle-specific defect.
pub fn validate_lifecycle_topology(topology: Topology) -> Result<(), LifecycleTopologyError> {
    topology
        .validate()
        .map_err(LifecycleTopologyError::Structure)?;
    for required in LifecycleEdge::ALL {
        if !topology.transitions.contains(&required.transition()) {
            return Err(LifecycleTopologyError::Missing(required));
        }
    }
    if topology.transitions.iter().any(|edge| {
        let from_closes = edge.from == LifecyclePhase::Draining.id()
            || edge.from == LifecyclePhase::Retiring.id();
        let to_admission =
            edge.to == LifecyclePhase::Activating.id() || edge.to == LifecyclePhase::Active.id();
        from_closes && to_admission
    }) {
        return Err(LifecycleTopologyError::ReopensAdmission);
    }
    Ok(())
}

type TransitionFn<C, E, L> =
    fn(EntitySlot<C, E, L>, SlotEvent<C, E, L>) -> (LifecycleOutput<C, E, L>, EntitySlot<C, E, L>);

/// Representable executable lifecycle machine for one stable entity slot.
pub type LifecycleMachine<C, E, L> =
    Base<EntitySlot<C, E, L>, TransitionFn<C, E, L>, SlotEvent<C, E, L>, LifecycleOutput<C, E, L>>;

/// Construct an inactive entity lifecycle machine.
///
/// # Panics
///
/// Panics if the crate's statically declared lifecycle topology is invalid.
#[must_use]
pub fn lifecycle_machine<C, E: Clone, L>() -> LifecycleMachine<C, E, L> {
    static VALIDATED: OnceLock<ValidatedTopology> = OnceLock::new();
    Base::new(
        EntitySlot::Inactive,
        *VALIDATED.get_or_init(|| {
            LIFECYCLE_TOPOLOGY
                .validated()
                .expect("lifecycle topology is valid")
        }),
        transition::<C, E, L>,
    )
}

fn transition<C, E: Clone, L>(
    state: EntitySlot<C, E, L>,
    event: SlotEvent<C, E, L>,
) -> (LifecycleOutput<C, E, L>, EntitySlot<C, E, L>) {
    let from = state.phase();
    let trigger = event.trigger();
    let handled = state.handles(&event);
    let decision = SlotReducer.reduce(state, event);
    let to = decision.state.phase();
    let evidence = if from == to {
        if handled {
            TransitionEvidence::SelfLoop {
                phase: from,
                trigger,
            }
        } else {
            TransitionEvidence::Ignored {
                phase: from,
                trigger,
            }
        }
    } else {
        let edge = LifecycleEdge::ALL
            .into_iter()
            .find(|edge| edge.endpoints() == (from, trigger, to))
            .expect("every executable phase change must be declared");
        TransitionEvidence::Traversed(edge)
    };
    (
        LifecycleOutput {
            effects: decision.effects,
            evidence,
            activation_id: decision.state.activation_id(),
        },
        decision.state,
    )
}

const fn trigger_label(trigger: LifecycleTrigger) -> &'static str {
    match trigger {
        LifecycleTrigger::ClaimActivation => "claim_activation",
        LifecycleTrigger::Dispatch => "dispatch",
        LifecycleTrigger::CancelWaiter => "cancel_waiter",
        LifecycleTrigger::ActivationSucceeded => "activation_succeeded",
        LifecycleTrigger::ActivationFailed => "activation_failed",
        LifecycleTrigger::DeliveryResolved => "delivery_resolved",
        LifecycleTrigger::BeginDrain => "begin_drain",
        LifecycleTrigger::FenceAcknowledged => "fence_acknowledged",
        LifecycleTrigger::ForceDrain => "force_drain",
        LifecycleTrigger::Terminated => "terminated",
    }
}

#[cfg(test)]
mod tests {
    use core::num::{NonZeroU64, NonZeroUsize};

    use bombay_machine::Machine;

    use super::*;
    use crate::entity::{
        ActivationId, DispatchId, DrainFailure, DrainStage, SlotEffect, SlotEvent,
    };

    fn activation(value: u64) -> ActivationId {
        ActivationId::new(NonZeroU64::new(value).unwrap())
    }

    fn dispatch(value: u64) -> DispatchId {
        DispatchId(NonZeroU64::new(value).unwrap())
    }

    #[test]
    fn topology_is_valid_and_renders_deterministically() {
        validate_lifecycle_topology(LIFECYCLE_TOPOLOGY).unwrap();
        let mut mermaid = String::new();
        LIFECYCLE_TOPOLOGY.write_mermaid(&mut mermaid).unwrap();
        assert_eq!(LIFECYCLE_TOPOLOGY.vertices.len(), 5);
        assert_eq!(LIFECYCLE_TOPOLOGY.transitions.len(), 7);
        assert_eq!(
            mermaid,
            "stateDiagram-v2\n    [*] --> inactive\n    inactive --> activating: claim_activation\n    activating --> active: activation_succeeded\n    activating --> inactive: activation_failed\n    active --> draining: begin_drain\n    draining --> retiring: fence_acknowledged\n    draining --> retiring: force_drain\n    retiring --> inactive: terminated\n"
        );
    }

    #[test]
    fn every_observed_phase_change_carries_declared_edge_evidence() {
        let machine = lifecycle_machine::<u8, u8, u8>();
        let (claim, machine) = machine.step(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 1,
            waiter_limit: NonZeroUsize::MIN,
        });
        assert_eq!(
            claim.evidence,
            TransitionEvidence::Traversed(LifecycleEdge::ClaimActivation)
        );
        let (activated, machine) = machine.step(SlotEvent::ActivationSucceeded {
            activation_id: activation(1),
            endpoint: 2,
            lease: 3,
        });
        assert_eq!(
            activated.evidence,
            TransitionEvidence::Traversed(LifecycleEdge::ActivationSucceeded)
        );
        let (_, machine) = machine.step(SlotEvent::DeliveryResolved {
            activation_id: activation(1),
            failure: None,
        });
        let (drain, machine) = machine.step(SlotEvent::BeginDrain {
            activation_id: activation(1),
        });
        let (fence, machine) = machine.step(SlotEvent::FenceAcknowledged {
            activation_id: activation(1),
        });
        let (terminated, _machine) = machine.step(SlotEvent::Terminated {
            activation_id: activation(1),
        });
        for evidence in [
            claim.evidence,
            activated.evidence,
            drain.evidence,
            fence.evidence,
            terminated.evidence,
        ] {
            let TransitionEvidence::Traversed(edge) = evidence else {
                panic!("phase change lacked edge evidence")
            };
            assert!(edge.is_declared());
        }
    }

    #[test]
    fn stale_and_nonchanging_inputs_are_honestly_classified() {
        let machine = lifecycle_machine::<u8, u8, u8>();
        let (dispatch, machine) = machine.step(SlotEvent::Dispatch {
            dispatch_id: dispatch(1),
            command: 1,
        });
        assert_eq!(
            dispatch.evidence,
            TransitionEvidence::SelfLoop {
                phase: LifecyclePhase::Inactive,
                trigger: LifecycleTrigger::Dispatch
            }
        );
        let (ignored, _) = machine.step(SlotEvent::Terminated {
            activation_id: activation(9),
        });
        assert_eq!(
            ignored.evidence,
            TransitionEvidence::Ignored {
                phase: LifecyclePhase::Inactive,
                trigger: LifecycleTrigger::Terminated
            }
        );
    }

    #[test]
    fn forced_drain_has_its_distinct_declared_edge() {
        let machine = lifecycle_machine::<u8, u8, u8>();
        let (_, machine) = machine.step(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 1,
            waiter_limit: NonZeroUsize::MIN,
        });
        let (_, machine) = machine.step(SlotEvent::ActivationSucceeded {
            activation_id: activation(1),
            endpoint: 2,
            lease: 3,
        });
        let (_, machine) = machine.step(SlotEvent::DeliveryResolved {
            activation_id: activation(1),
            failure: None,
        });
        let (_, machine) = machine.step(SlotEvent::BeginDrain {
            activation_id: activation(1),
        });
        let (forced, _) = machine.step(SlotEvent::ForceDrain {
            activation_id: activation(1),
            failure: DrainFailure {
                stage: DrainStage::FenceAcknowledgement,
                outstanding_reservations: 0,
            },
        });
        assert_eq!(
            forced.evidence,
            TransitionEvidence::Traversed(LifecycleEdge::ForceDrain)
        );
    }

    #[derive(Clone, Copy)]
    enum Input {
        ClaimCurrent,
        Dispatch,
        CancelCurrent,
        CancelStale,
        ActivationCurrent,
        ActivationStale,
        ActivationFailure,
        ActivationFailureStale,
        DeliveryCurrent,
        DeliveryFailedCurrent,
        DeliveryFailedStale,
        BeginDrainCurrent,
        BeginDrainStale,
        FenceCurrent,
        FenceStale,
        ForceCurrent,
        ForceStale,
        TerminatedCurrent,
        TerminatedStale,
    }

    const INPUTS: [Input; 19] = [
        Input::ClaimCurrent,
        Input::Dispatch,
        Input::CancelCurrent,
        Input::CancelStale,
        Input::ActivationCurrent,
        Input::ActivationStale,
        Input::ActivationFailure,
        Input::ActivationFailureStale,
        Input::DeliveryCurrent,
        Input::DeliveryFailedCurrent,
        Input::DeliveryFailedStale,
        Input::BeginDrainCurrent,
        Input::BeginDrainStale,
        Input::FenceCurrent,
        Input::FenceStale,
        Input::ForceCurrent,
        Input::ForceStale,
        Input::TerminatedCurrent,
        Input::TerminatedStale,
    ];

    fn event(input: Input) -> SlotEvent<u8, u8, u8> {
        match input {
            Input::ClaimCurrent => SlotEvent::ClaimActivation {
                activation_id: activation(1),
                dispatch_id: dispatch(1),
                command: 1,
                waiter_limit: NonZeroUsize::new(2).unwrap(),
            },
            Input::Dispatch => SlotEvent::Dispatch {
                dispatch_id: dispatch(2),
                command: 2,
            },
            Input::CancelCurrent => SlotEvent::CancelWaiter {
                activation_id: activation(1),
                dispatch_id: dispatch(1),
            },
            Input::CancelStale => SlotEvent::CancelWaiter {
                activation_id: activation(2),
                dispatch_id: dispatch(1),
            },
            Input::ActivationCurrent => SlotEvent::ActivationSucceeded {
                activation_id: activation(1),
                endpoint: 1,
                lease: 1,
            },
            Input::ActivationStale => SlotEvent::ActivationSucceeded {
                activation_id: activation(2),
                endpoint: 2,
                lease: 2,
            },
            Input::ActivationFailure => SlotEvent::ActivationFailed {
                activation_id: activation(1),
            },
            Input::ActivationFailureStale => SlotEvent::ActivationFailed {
                activation_id: activation(2),
            },
            Input::DeliveryCurrent => SlotEvent::DeliveryResolved {
                activation_id: activation(1),
                failure: None,
            },
            Input::DeliveryFailedCurrent => SlotEvent::DeliveryResolved {
                activation_id: activation(1),
                failure: Some((dispatch(3), 3)),
            },
            Input::DeliveryFailedStale => SlotEvent::DeliveryResolved {
                activation_id: activation(2),
                failure: Some((dispatch(3), 3)),
            },
            Input::BeginDrainCurrent => SlotEvent::BeginDrain {
                activation_id: activation(1),
            },
            Input::BeginDrainStale => SlotEvent::BeginDrain {
                activation_id: activation(2),
            },
            Input::FenceCurrent => SlotEvent::FenceAcknowledged {
                activation_id: activation(1),
            },
            Input::FenceStale => SlotEvent::FenceAcknowledged {
                activation_id: activation(2),
            },
            Input::ForceCurrent => SlotEvent::ForceDrain {
                activation_id: activation(1),
                failure: DrainFailure {
                    stage: DrainStage::FenceAcknowledgement,
                    outstanding_reservations: 0,
                },
            },
            Input::ForceStale => SlotEvent::ForceDrain {
                activation_id: activation(2),
                failure: DrainFailure {
                    stage: DrainStage::FenceAcknowledgement,
                    outstanding_reservations: 0,
                },
            },
            Input::TerminatedCurrent => SlotEvent::Terminated {
                activation_id: activation(1),
            },
            Input::TerminatedStale => SlotEvent::Terminated {
                activation_id: activation(2),
            },
        }
    }

    fn check_trace(trace: &[Input]) {
        let mut machine = lifecycle_machine::<u8, u8, u8>();
        for input in trace {
            let before = machine.state().phase();
            let (output, successor) = machine.step(event(*input));
            let after = successor.state().phase();
            match output.evidence {
                TransitionEvidence::Traversed(edge) => {
                    assert!(edge.is_declared());
                    let (from, _, to) = edge.endpoints();
                    assert_eq!((from, to), (before, after));
                }
                TransitionEvidence::SelfLoop { phase, .. } => {
                    assert_eq!(phase, before);
                    assert_eq!(before, after);
                }
                TransitionEvidence::Ignored { phase, .. } => {
                    assert_eq!(phase, before);
                    assert_eq!(before, after);
                    // An ignored input never changes state; the only effects it
                    // may emit are cleanup: rejections returning owned commands
                    // and retirement of stale incarnation leases.
                    assert!(
                        output.effects.as_slice().iter().all(|effect| matches!(
                            effect,
                            SlotEffect::Reject { .. } | SlotEffect::Retire { .. }
                        )),
                        "ignored input produced non-cleanup effects"
                    );
                }
            }
            machine = successor;
        }
    }

    fn enumerate(prefix: &mut Vec<Input>, remaining: usize) {
        check_trace(prefix);
        if remaining == 0 {
            return;
        }
        for input in INPUTS {
            prefix.push(input);
            enumerate(prefix, remaining - 1);
            prefix.pop();
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the negative fixture deliberately leaks a synthetic 'static topology slice"
    )]
    fn topology_reopening_admission_from_drain_or_retirement_is_rejected() {
        for (from, to) in [
            (LifecyclePhase::Draining, LifecyclePhase::Active),
            (LifecyclePhase::Draining, LifecyclePhase::Activating),
            (LifecyclePhase::Retiring, LifecyclePhase::Active),
            (LifecyclePhase::Retiring, LifecyclePhase::Activating),
        ] {
            let mut transitions = LIFECYCLE_TOPOLOGY.transitions.to_vec();
            transitions.push(Transition {
                from: from.id(),
                trigger: TriggerId(99),
                to: to.id(),
                label: "reopen",
            });
            let topology = Topology {
                transitions: transitions.leak(),
                ..LIFECYCLE_TOPOLOGY
            };
            assert_eq!(
                validate_lifecycle_topology(topology),
                Err(LifecycleTopologyError::ReopensAdmission),
                "{from:?} -> {to:?} must be rejected"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "native exhaustive enumeration covers 137,561 traces; Miri runs each reducer test"
    )]
    fn bounded_event_traces_preserve_topology_evidence_and_structure() {
        enumerate(&mut Vec::new(), 4);
    }
}
