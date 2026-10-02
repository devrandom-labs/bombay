/// Stable phase of one Entity routing slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// Phase-changing result selected by a slot transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LifecycleEdge {
    /// A fresh activation was claimed.
    ClaimActivation,
    /// Activation committed.
    ActivationSucceeded,
    /// Activation failed and its waiters were returned.
    ActivationFailed,
    /// Admission closed for a live incarnation.
    BeginDrain,
    /// The processing fence completed.
    FenceAcknowledged,
    /// Bounded draining forced retirement.
    ForceDrain,
    /// Exact termination removed the slot.
    Terminated,
}

/// Disposition returned by one executable Entity slot transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionEvidence {
    /// Execution changed phase along this edge.
    Traversed(LifecycleEdge),
    /// Input was handled without changing phase.
    SelfLoop,
    /// Input was irrelevant, invalid for the phase, or stale.
    Ignored,
}

#[cfg(test)]
mod tests {
    use core::num::{NonZeroU64, NonZeroUsize};

    use super::super::{DrainProgress, EntitySlot, ReservationCount, SlotEffectBatch};
    use super::TransitionEvidence::{Ignored, SelfLoop, Traversed};
    use super::*;
    use crate::entity::{
        ActivationId, DispatchId, DrainFailure, DrainStage, Refusal, RetirementMode, SlotEffect,
        SlotEvent,
    };

    fn activation(value: u64) -> ActivationId {
        ActivationId::new(NonZeroU64::new(value).unwrap())
    }

    fn dispatch(value: u64) -> DispatchId {
        DispatchId(NonZeroU64::new(value).unwrap())
    }

    #[test]
    fn fence_acknowledgement_before_reserved_delivery_is_ignored() {
        let claimed = EntitySlot::<u8, u8, u8>::Inactive.decide(SlotEvent::ClaimActivation {
            activation_id: activation(1),
            dispatch_id: dispatch(1),
            command: 9,
            waiter_limit: NonZeroUsize::MIN,
        });
        let activated = claimed.state.decide(SlotEvent::ActivationSucceeded {
            activation_id: activation(1),
            endpoint: 2,
            lease: 3,
        });
        let draining = activated.state.decide(SlotEvent::BeginDrain {
            activation_id: activation(1),
        });
        let premature = draining.state.decide(SlotEvent::FenceAcknowledged {
            activation_id: activation(1),
        });

        assert_eq!(premature.state.phase(), LifecyclePhase::Draining);
        assert!(premature.effects.as_slice().is_empty());
        assert_eq!(premature.evidence, TransitionEvidence::Ignored);
    }

    #[derive(Clone, Copy, Debug)]
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

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ExpectedSlot {
        Inactive,
        Activating(Vec<(DispatchId, u8)>),
        Active(usize),
        Draining(Option<NonZeroUsize>),
        Retiring,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum ExpectedAuthority {
        Inactive,
        Activation(ActivationId, NonZeroUsize),
        ActorGeneration(ActivationId, u8, u8),
        Retirement(ActivationId),
    }

    #[derive(Debug, Eq, PartialEq)]
    enum ExpectedEffect {
        Start(ActivationId),
        Deliver(ActivationId, DispatchId, u8, u8),
        Reject(DispatchId, u8, Refusal),
        Fence(ActivationId, u8),
        Retire(ActivationId, u8, RetirementMode),
        Remove(ActivationId),
    }

    fn observed_slot(slot: &EntitySlot<u8, u8, u8>) -> ExpectedSlot {
        match slot {
            EntitySlot::Inactive => ExpectedSlot::Inactive,
            EntitySlot::Activating(state) => ExpectedSlot::Activating(
                state
                    .waiters
                    .iter()
                    .map(|waiter| (waiter.dispatch_id, waiter.command))
                    .collect(),
            ),
            EntitySlot::Active(state) => ExpectedSlot::Active(match state.reservations {
                ReservationCount::Drained => 0,
                ReservationCount::Pending(pending) => pending.get(),
            }),
            EntitySlot::Draining(state) => ExpectedSlot::Draining(match state.progress {
                DrainProgress::Reservations(pending) => Some(pending),
                DrainProgress::FenceAcknowledgement => None,
            }),
            EntitySlot::Retiring { .. } => ExpectedSlot::Retiring,
        }
    }

    fn observed_authority(slot: &EntitySlot<u8, u8, u8>) -> ExpectedAuthority {
        match slot {
            EntitySlot::Inactive => ExpectedAuthority::Inactive,
            EntitySlot::Activating(state) => {
                ExpectedAuthority::Activation(state.activation_id, state.waiter_limit)
            }
            EntitySlot::Active(state) => {
                ExpectedAuthority::ActorGeneration(state.activation_id, state.endpoint, state.lease)
            }
            EntitySlot::Draining(state) => {
                ExpectedAuthority::ActorGeneration(state.activation_id, state.endpoint, state.lease)
            }
            EntitySlot::Retiring { activation_id } => ExpectedAuthority::Retirement(*activation_id),
        }
    }

    fn expected_authority(slot: &ExpectedSlot) -> ExpectedAuthority {
        match slot {
            ExpectedSlot::Inactive => ExpectedAuthority::Inactive,
            ExpectedSlot::Activating(_) => ExpectedAuthority::Activation(
                activation(1),
                NonZeroUsize::new(2).expect("two waiters is nonzero"),
            ),
            ExpectedSlot::Active(_) | ExpectedSlot::Draining(_) => {
                ExpectedAuthority::ActorGeneration(activation(1), 1, 1)
            }
            ExpectedSlot::Retiring => ExpectedAuthority::Retirement(activation(1)),
        }
    }

    fn observed_effects(effects: &SlotEffectBatch<u8, u8, u8>) -> Vec<ExpectedEffect> {
        effects
            .as_slice()
            .iter()
            .map(|effect| match effect {
                SlotEffect::StartActivation { activation_id } => {
                    ExpectedEffect::Start(*activation_id)
                }
                SlotEffect::Deliver {
                    activation_id,
                    dispatch_id,
                    endpoint,
                    command,
                } => ExpectedEffect::Deliver(*activation_id, *dispatch_id, *endpoint, *command),
                SlotEffect::Reject {
                    dispatch_id,
                    command,
                    reason,
                } => ExpectedEffect::Reject(*dispatch_id, *command, *reason),
                SlotEffect::EnqueueFence {
                    activation_id,
                    endpoint,
                } => ExpectedEffect::Fence(*activation_id, *endpoint),
                SlotEffect::Retire {
                    activation_id,
                    lease,
                    retirement,
                } => ExpectedEffect::Retire(*activation_id, *lease, *retirement),
                SlotEffect::Remove { activation_id } => ExpectedEffect::Remove(*activation_id),
            })
            .collect()
    }

    fn forced_retirement() -> RetirementMode {
        RetirementMode::Forced(DrainFailure {
            stage: DrainStage::Retirement,
            outstanding_reservations: 0,
        })
    }

    fn cleanup(input: Input) -> Vec<ExpectedEffect> {
        match input {
            Input::ActivationCurrent => vec![ExpectedEffect::Retire(
                activation(1),
                1,
                forced_retirement(),
            )],
            Input::ActivationStale => vec![ExpectedEffect::Retire(
                activation(2),
                2,
                forced_retirement(),
            )],
            Input::DeliveryFailedCurrent | Input::DeliveryFailedStale => {
                vec![ExpectedEffect::Reject(dispatch(3), 3, Refusal::Unavailable)]
            }
            _ => Vec::new(),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the independent state/input oracle stays one exhaustive transition"
    )]
    fn expected_step(
        state: ExpectedSlot,
        input: Input,
    ) -> (ExpectedSlot, Vec<ExpectedEffect>, TransitionEvidence) {
        match state {
            ExpectedSlot::Inactive => match input {
                Input::ClaimCurrent => (
                    ExpectedSlot::Activating(vec![(dispatch(1), 1)]),
                    vec![ExpectedEffect::Start(activation(1))],
                    Traversed(LifecycleEdge::ClaimActivation),
                ),
                Input::Dispatch => (
                    ExpectedSlot::Inactive,
                    vec![ExpectedEffect::Reject(dispatch(2), 2, Refusal::Unavailable)],
                    SelfLoop,
                ),
                _ => (ExpectedSlot::Inactive, cleanup(input), Ignored),
            },
            ExpectedSlot::Activating(mut waiters) => match input {
                Input::Dispatch if waiters.len() < 2 => {
                    waiters.push((dispatch(2), 2));
                    (ExpectedSlot::Activating(waiters), Vec::new(), SelfLoop)
                }
                Input::Dispatch => (
                    ExpectedSlot::Activating(waiters),
                    vec![ExpectedEffect::Reject(dispatch(2), 2, Refusal::Busy)],
                    SelfLoop,
                ),
                Input::ClaimCurrent => (
                    ExpectedSlot::Activating(waiters),
                    vec![ExpectedEffect::Reject(dispatch(1), 1, Refusal::Busy)],
                    SelfLoop,
                ),
                Input::CancelCurrent => {
                    let prior = waiters.len();
                    waiters.retain(|(dispatch_id, _)| *dispatch_id != dispatch(1));
                    let evidence = if waiters.len() < prior {
                        SelfLoop
                    } else {
                        Ignored
                    };
                    (ExpectedSlot::Activating(waiters), Vec::new(), evidence)
                }
                Input::ActivationCurrent => {
                    let reservations = waiters.len();
                    let effects = waiters
                        .into_iter()
                        .map(|(dispatch_id, command)| {
                            ExpectedEffect::Deliver(activation(1), dispatch_id, 1, command)
                        })
                        .collect();
                    (
                        ExpectedSlot::Active(reservations),
                        effects,
                        Traversed(LifecycleEdge::ActivationSucceeded),
                    )
                }
                Input::ActivationFailure => (
                    ExpectedSlot::Inactive,
                    waiters
                        .into_iter()
                        .map(|(dispatch_id, command)| {
                            ExpectedEffect::Reject(dispatch_id, command, Refusal::Unavailable)
                        })
                        .chain(core::iter::once(ExpectedEffect::Remove(activation(1))))
                        .collect(),
                    Traversed(LifecycleEdge::ActivationFailed),
                ),
                _ => (ExpectedSlot::Activating(waiters), cleanup(input), Ignored),
            },
            ExpectedSlot::Active(reservations) => match input {
                Input::Dispatch => (
                    ExpectedSlot::Active(reservations + 1),
                    vec![ExpectedEffect::Deliver(activation(1), dispatch(2), 1, 2)],
                    SelfLoop,
                ),
                Input::ClaimCurrent => (
                    ExpectedSlot::Active(reservations),
                    vec![ExpectedEffect::Reject(dispatch(1), 1, Refusal::Busy)],
                    SelfLoop,
                ),
                Input::DeliveryCurrent | Input::DeliveryFailedCurrent => {
                    let effects = cleanup(input);
                    match reservations.checked_sub(1) {
                        Some(remaining) => (ExpectedSlot::Active(remaining), effects, SelfLoop),
                        None => (ExpectedSlot::Active(reservations), effects, Ignored),
                    }
                }
                Input::BeginDrainCurrent => match NonZeroUsize::new(reservations) {
                    Some(pending) => (
                        ExpectedSlot::Draining(Some(pending)),
                        Vec::new(),
                        Traversed(LifecycleEdge::BeginDrain),
                    ),
                    None => (
                        ExpectedSlot::Draining(None),
                        vec![ExpectedEffect::Fence(activation(1), 1)],
                        Traversed(LifecycleEdge::BeginDrain),
                    ),
                },
                _ => (ExpectedSlot::Active(reservations), cleanup(input), Ignored),
            },
            ExpectedSlot::Draining(pending) => match input {
                Input::Dispatch => (
                    ExpectedSlot::Draining(pending),
                    vec![ExpectedEffect::Reject(dispatch(2), 2, Refusal::Draining)],
                    SelfLoop,
                ),
                Input::ClaimCurrent => (
                    ExpectedSlot::Draining(pending),
                    vec![ExpectedEffect::Reject(dispatch(1), 1, Refusal::Draining)],
                    SelfLoop,
                ),
                Input::DeliveryCurrent | Input::DeliveryFailedCurrent => {
                    let mut effects = cleanup(input);
                    match pending {
                        Some(count) if count.get() > 1 => (
                            ExpectedSlot::Draining(NonZeroUsize::new(count.get() - 1)),
                            effects,
                            SelfLoop,
                        ),
                        Some(_) => {
                            effects.push(ExpectedEffect::Fence(activation(1), 1));
                            (ExpectedSlot::Draining(None), effects, SelfLoop)
                        }
                        None => (ExpectedSlot::Draining(None), effects, Ignored),
                    }
                }
                Input::FenceCurrent => match pending {
                    Some(count) => (ExpectedSlot::Draining(Some(count)), Vec::new(), Ignored),
                    None => (
                        ExpectedSlot::Retiring,
                        vec![ExpectedEffect::Retire(
                            activation(1),
                            1,
                            RetirementMode::Graceful,
                        )],
                        Traversed(LifecycleEdge::FenceAcknowledged),
                    ),
                },
                Input::ForceCurrent => (
                    ExpectedSlot::Retiring,
                    vec![ExpectedEffect::Retire(
                        activation(1),
                        1,
                        RetirementMode::Forced(DrainFailure {
                            stage: DrainStage::FenceAcknowledgement,
                            outstanding_reservations: 0,
                        }),
                    )],
                    Traversed(LifecycleEdge::ForceDrain),
                ),
                _ => (ExpectedSlot::Draining(pending), cleanup(input), Ignored),
            },
            ExpectedSlot::Retiring => match input {
                Input::Dispatch => (
                    ExpectedSlot::Retiring,
                    vec![ExpectedEffect::Reject(dispatch(2), 2, Refusal::Draining)],
                    SelfLoop,
                ),
                Input::ClaimCurrent => (
                    ExpectedSlot::Retiring,
                    vec![ExpectedEffect::Reject(dispatch(1), 1, Refusal::Draining)],
                    SelfLoop,
                ),
                Input::TerminatedCurrent => (
                    ExpectedSlot::Inactive,
                    vec![ExpectedEffect::Remove(activation(1))],
                    Traversed(LifecycleEdge::Terminated),
                ),
                _ => (ExpectedSlot::Retiring, cleanup(input), Ignored),
            },
        }
    }

    fn check_trace(trace: &[Input]) {
        let mut slot = EntitySlot::<u8, u8, u8>::Inactive;
        let mut expected = ExpectedSlot::Inactive;
        for input in trace {
            let before = slot.phase();
            let output = slot.decide(event(*input));
            let after = output.state.phase();
            let (next, effects, evidence) = expected_step(expected, *input);
            assert_eq!(observed_slot(&output.state), next, "input {input:?}");
            assert_eq!(
                observed_authority(&output.state),
                expected_authority(&next),
                "input {input:?}"
            );
            assert_eq!(
                observed_effects(&output.effects),
                effects,
                "input {input:?}"
            );
            assert_eq!(output.evidence, evidence, "input {input:?}");
            match output.evidence {
                TransitionEvidence::Traversed(_) => {
                    assert_ne!(before, after);
                }
                TransitionEvidence::SelfLoop => {
                    assert_eq!(before, after);
                }
                TransitionEvidence::Ignored => {
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
            expected = next;
            slot = output.state;
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
        ignore = "native exhaustive enumeration covers 137,561 traces; Miri runs each reducer test"
    )]
    fn bounded_event_traces_match_independent_slot_oracle() {
        enumerate(&mut Vec::new(), 4);
    }

    #[test]
    fn independent_oracle_covers_every_edge_and_replayed_stale_facts() {
        check_trace(&[
            Input::ClaimCurrent,
            Input::ActivationCurrent,
            Input::DeliveryCurrent,
            Input::BeginDrainCurrent,
            Input::FenceCurrent,
            Input::TerminatedCurrent,
            Input::TerminatedCurrent,
        ]);
        check_trace(&[
            Input::ClaimCurrent,
            Input::ActivationCurrent,
            Input::BeginDrainCurrent,
            Input::FenceCurrent,
            Input::DeliveryFailedStale,
            Input::ForceCurrent,
            Input::TerminatedCurrent,
        ]);
        check_trace(&[
            Input::ClaimCurrent,
            Input::ActivationFailure,
            Input::ActivationFailure,
            Input::ActivationStale,
            Input::TerminatedStale,
        ]);
        check_trace(&[
            Input::ClaimCurrent,
            Input::Dispatch,
            Input::ActivationCurrent,
            Input::DeliveryFailedCurrent,
            Input::BeginDrainCurrent,
            Input::DeliveryCurrent,
            Input::FenceCurrent,
            Input::TerminatedCurrent,
        ]);
    }

    #[test]
    fn every_lifecycle_edge_has_a_stale_and_replay_inversion() {
        let edge_witnesses: [(&[Input], Option<Input>, Input, LifecycleEdge); 7] = [
            (
                &[],
                None,
                Input::ClaimCurrent,
                LifecycleEdge::ClaimActivation,
            ),
            (
                &[Input::ClaimCurrent],
                Some(Input::ActivationStale),
                Input::ActivationCurrent,
                LifecycleEdge::ActivationSucceeded,
            ),
            (
                &[Input::ClaimCurrent],
                Some(Input::ActivationFailureStale),
                Input::ActivationFailure,
                LifecycleEdge::ActivationFailed,
            ),
            (
                &[Input::ClaimCurrent, Input::ActivationCurrent],
                Some(Input::BeginDrainStale),
                Input::BeginDrainCurrent,
                LifecycleEdge::BeginDrain,
            ),
            (
                &[
                    Input::ClaimCurrent,
                    Input::ActivationCurrent,
                    Input::DeliveryCurrent,
                    Input::BeginDrainCurrent,
                ],
                Some(Input::FenceStale),
                Input::FenceCurrent,
                LifecycleEdge::FenceAcknowledged,
            ),
            (
                &[
                    Input::ClaimCurrent,
                    Input::ActivationCurrent,
                    Input::BeginDrainCurrent,
                ],
                Some(Input::ForceStale),
                Input::ForceCurrent,
                LifecycleEdge::ForceDrain,
            ),
            (
                &[
                    Input::ClaimCurrent,
                    Input::ActivationCurrent,
                    Input::DeliveryCurrent,
                    Input::BeginDrainCurrent,
                    Input::FenceCurrent,
                ],
                Some(Input::TerminatedStale),
                Input::TerminatedCurrent,
                LifecycleEdge::Terminated,
            ),
        ];

        for (prefix, stale, current, edge) in edge_witnesses {
            let mut state = ExpectedSlot::Inactive;
            for input in prefix {
                state = expected_step(state, *input).0;
            }
            for input in INPUTS {
                let mut trace = prefix.to_vec();
                trace.push(input);
                check_trace(&trace);
            }
            let mut trace = prefix.to_vec();
            if let Some(stale) = stale {
                let (unchanged, _, evidence) = expected_step(state.clone(), stale);
                assert_eq!(unchanged, state, "stale input {stale:?}");
                assert_eq!(evidence, Ignored, "stale input {stale:?}");
                trace.push(stale);
            }
            let (successor, _, evidence) = expected_step(state, current);
            assert_eq!(evidence, Traversed(edge), "current input {current:?}");
            let (replayed, _, replay_evidence) = expected_step(successor.clone(), current);
            assert_eq!(replayed, successor, "replayed input {current:?}");
            assert_eq!(
                replay_evidence,
                if matches!(edge, LifecycleEdge::ClaimActivation) {
                    SelfLoop
                } else {
                    Ignored
                },
                "replayed input {current:?}"
            );
            trace.extend([current, current]);
            check_trace(&trace);
        }
    }
}
