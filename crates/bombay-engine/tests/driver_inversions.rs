#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fact {
    Initialize,
    CommitInit,
    Next(u8),
    Fold(u8),
    Commit(u8),
    CommitFailed,
    BehaviorError,
    InputClosed,
    Stop,
    Retire,
}

#[derive(Debug, Clone, Copy)]
enum Inversion {
    InitializeTwice,
    EventBeforeInitializationCommit,
    Prefetch,
    SkipFold,
    DoubleFold,
    DropActions,
    DoubleCommit,
    IgnoreStop,
    RetryCommit,
    WorkAfterBehaviorError,
    WorkAfterInputClosure,
    RetireTwice,
    SynchronousSelfReentry,
}

const EXPECTED: &[Fact] = &[
    Fact::Initialize,
    Fact::CommitInit,
    Fact::Next(1),
    Fact::Fold(1),
    Fact::Commit(1),
    Fact::Next(2),
    Fact::Fold(2),
    Fact::Commit(2),
    Fact::Stop,
    Fact::Retire,
];

fn inverted(inversion: Inversion) -> Vec<Fact> {
    let mut facts = EXPECTED.to_vec();
    match inversion {
        Inversion::InitializeTwice => facts.insert(1, Fact::Initialize),
        Inversion::EventBeforeInitializationCommit => facts.swap(1, 2),
        Inversion::Prefetch => facts.insert(4, Fact::Next(2)),
        Inversion::SkipFold => {
            facts.remove(3);
        }
        Inversion::DoubleFold => facts.insert(4, Fact::Fold(1)),
        Inversion::DropActions => {
            facts.remove(4);
        }
        Inversion::DoubleCommit => facts.insert(5, Fact::Commit(1)),
        Inversion::IgnoreStop => facts.insert(9, Fact::Next(3)),
        Inversion::RetryCommit => {
            facts.splice(5..5, [Fact::CommitFailed, Fact::Commit(1)]);
        }
        Inversion::WorkAfterBehaviorError => {
            facts.splice(
                3..,
                [
                    Fact::Fold(1),
                    Fact::BehaviorError,
                    Fact::Next(2),
                    Fact::Retire,
                ],
            );
        }
        Inversion::WorkAfterInputClosure => {
            facts.splice(9..9, [Fact::InputClosed, Fact::Next(3)]);
        }
        Inversion::RetireTwice => facts.push(Fact::Retire),
        Inversion::SynchronousSelfReentry => {
            facts.splice(4..7, [Fact::Commit(1), Fact::Fold(2), Fact::Next(2)]);
        }
    }
    facts
}

fn causal_oracle(facts: &[Fact]) -> bool {
    facts == EXPECTED
}

fn pending_progress_oracle(source_polls: usize, self_wakes: usize) -> bool {
    source_polls == 1 && self_wakes == 0
}

fn cancellation_oracle(owned_values_dropped: bool, retired: bool, completed: bool) -> bool {
    owned_values_dropped && !retired && !completed
}

fn panic_terminality_oracle(
    panicked: bool,
    later_polls: usize,
    owned_values_dropped: bool,
) -> bool {
    panicked && later_polls == 0 && owned_values_dropped
}

fn completion_classification_oracle(
    stop_is_stopped: bool,
    exhaustion_is_exhausted: bool,
    both_are_success: bool,
) -> bool {
    stop_is_stopped && exhaustion_is_exhausted && both_are_success
}

fn ordinary_retirement_oracle(retirement_attempts: usize) -> bool {
    retirement_attempts == 1
}

fn explicit_stop_oracle(final_actions_committed: bool, later_ingress: bool) -> bool {
    final_actions_committed && !later_ingress
}

fn source_closure_oracle(synthetic_fold: bool, later_poll: bool) -> bool {
    !synthetic_fold && !later_poll
}

fn terminal_fusion_oracle(work_after_terminal_edge: bool) -> bool {
    !work_after_terminal_edge
}

fn initialization_count_oracle(initializations: usize) -> bool {
    initializations == 1
}

fn accepted_fold_count_oracle(accepted_events: usize, folds: usize) -> bool {
    accepted_events == folds
}

fn decision_integrity_oracle(successor_state: u8, committed_action: u8) -> bool {
    successor_state == committed_action
}

fn controlled_failure_oracle(actions_committed: usize, later_work: usize) -> bool {
    actions_committed == 0 && later_work == 0
}

fn universality_oracle(custom_shapes_run: usize, custom_shapes_declared: usize) -> bool {
    custom_shapes_run == custom_shapes_declared
}

fn closed_input_oracle(closed_events: usize, side_channel_events: usize) -> bool {
    closed_events > 0 && side_channel_events == 0
}

fn complete_output_oracle(lanes_received: usize, lanes_emitted: usize) -> bool {
    lanes_received == lanes_emitted
}

fn exclusive_fold_oracle(maximum_active_folds: usize) -> bool {
    maximum_active_folds <= 1
}

fn non_reentrancy_oracle(folds_while_commit_pending: usize) -> bool {
    folds_while_commit_pending == 0
}

fn local_commit_oracle(awaited_external_completion: bool) -> bool {
    !awaited_external_completion
}

fn capability_event_oracle(callback_folds: usize, later_event_folds: usize) -> bool {
    callback_folds == 0 && later_event_folds == 1
}

fn interpretation_count_oracle(successful_decisions: usize, commits: usize) -> bool {
    successful_decisions == commits
}

fn lane_order_oracle(emitted: &[u8], received: &[u8]) -> bool {
    emitted == received
}

fn payload_ownership_oracle(emitted: usize, committed_or_recovered: usize) -> bool {
    emitted == committed_or_recovered
}

fn honest_completion_oracle(claimed_external_delivery: bool) -> bool {
    !claimed_external_delivery
}

fn interpretation_failure_oracle(later_work: usize) -> bool {
    later_work == 0
}

fn committed_prefix_oracle(actual_prefix: usize, reported_prefix: usize) -> bool {
    actual_prefix == reported_prefix
}

fn retry_oracle(commit_attempts: usize) -> bool {
    commit_attempts == 1
}

fn no_rollback_oracle(successor_state: u8, dropped_state: u8) -> bool {
    successor_state == dropped_state
}

fn creation_precedence_oracle(first_send: usize, final_creation: usize) -> bool {
    final_creation < first_send
}

fn creation_result_scope_oracle(committed_nonces: &[u8], observed_nonces: &[u8]) -> bool {
    committed_nonces == observed_nonces
}

fn static_sufficiency_oracle(missing_capabilities_compile: bool) -> bool {
    !missing_capabilities_compile
}

fn environment_substitutability_oracle(concrete_environments_run: usize) -> bool {
    concrete_environments_run >= 2
}

fn exact_error_oracle(behavior_erased: bool, environment_erased: bool) -> bool {
    !behavior_erased && !environment_erased
}

#[test]
fn causal_oracle_kills_every_deliberate_algorithm_inversion() {
    assert!(causal_oracle(EXPECTED));
    for inversion in [
        Inversion::InitializeTwice,
        Inversion::EventBeforeInitializationCommit,
        Inversion::Prefetch,
        Inversion::SkipFold,
        Inversion::DoubleFold,
        Inversion::DropActions,
        Inversion::DoubleCommit,
        Inversion::IgnoreStop,
        Inversion::RetryCommit,
        Inversion::WorkAfterBehaviorError,
        Inversion::WorkAfterInputClosure,
        Inversion::RetireTwice,
        Inversion::SynchronousSelfReentry,
    ] {
        assert!(
            !causal_oracle(&inverted(inversion)),
            "oracle survived {inversion:?}"
        );
    }
}

#[test]
fn pending_progress_oracle_kills_spin_and_self_wake_inversions() {
    assert!(pending_progress_oracle(1, 0));

    // D-SCHED-5 inversion: manufacture a wake despite no dependency progress.
    assert!(!pending_progress_oracle(1, 1));
    // D-SCHED-6 inversion: poll the pending source again in the same wake turn.
    assert!(!pending_progress_oracle(2, 0));
}

#[test]
fn cancellation_oracle_kills_leak_false_retirement_and_false_completion_inversions() {
    assert!(cancellation_oracle(true, false, false));
    assert!(!cancellation_oracle(false, false, false));
    assert!(!cancellation_oracle(true, true, false));
    assert!(!cancellation_oracle(true, false, true));
}

#[test]
fn panic_terminality_oracle_kills_recovery_repoll_and_leak_inversions() {
    assert!(panic_terminality_oracle(true, 0, true));
    assert!(!panic_terminality_oracle(false, 0, true));
    assert!(!panic_terminality_oracle(true, 1, true));
    assert!(!panic_terminality_oracle(true, 0, false));
}

#[test]
fn completion_oracle_kills_collapsed_swapped_and_failure_classifications() {
    assert!(completion_classification_oracle(true, true, true));
    assert!(!completion_classification_oracle(false, true, true));
    assert!(!completion_classification_oracle(true, false, true));
    assert!(!completion_classification_oracle(true, true, false));
}

#[test]
fn ordinary_retirement_oracle_kills_missing_and_duplicate_retirement_inversions() {
    assert!(ordinary_retirement_oracle(1));
    assert!(!ordinary_retirement_oracle(0));
    assert!(!ordinary_retirement_oracle(2));
}

#[test]
fn explicit_stop_oracle_kills_dropped_final_actions_and_later_ingress_inversions() {
    assert!(explicit_stop_oracle(true, false));
    assert!(!explicit_stop_oracle(false, false));
    assert!(!explicit_stop_oracle(true, true));
}

#[test]
fn source_closure_oracle_kills_synthetic_fold_and_repoll_inversions() {
    assert!(source_closure_oracle(false, false));
    assert!(!source_closure_oracle(true, false));
    assert!(!source_closure_oracle(false, true));
}

#[test]
fn terminal_fusion_oracle_kills_every_post_terminal_work_inversion() {
    assert!(terminal_fusion_oracle(false));
    assert!(!terminal_fusion_oracle(true));
}

#[test]
fn initialization_count_oracle_kills_missing_and_duplicate_initialization_inversions() {
    assert!(initialization_count_oracle(1));
    assert!(!initialization_count_oracle(0));
    assert!(!initialization_count_oracle(2));
}

#[test]
fn accepted_fold_count_oracle_kills_missing_and_duplicate_fold_inversions() {
    assert!(accepted_fold_count_oracle(3, 3));
    assert!(!accepted_fold_count_oracle(3, 2));
    assert!(!accepted_fold_count_oracle(3, 4));
}

#[test]
fn decision_integrity_oracle_kills_stale_state_and_stale_action_inversions() {
    assert!(decision_integrity_oracle(5, 5));
    assert!(!decision_integrity_oracle(4, 5));
    assert!(!decision_integrity_oracle(5, 4));
}

#[test]
fn controlled_failure_oracle_kills_fabricated_actions_and_continuation_inversions() {
    assert!(controlled_failure_oracle(0, 0));
    assert!(!controlled_failure_oracle(1, 0));
    assert!(!controlled_failure_oracle(0, 1));
}

#[test]
fn universality_oracle_kills_shape_specific_driver_inversion() {
    assert!(universality_oracle(3, 3));
    assert!(!universality_oracle(2, 3));
}

#[test]
fn closed_input_oracle_kills_driver_owned_side_channel_inversion() {
    assert!(closed_input_oracle(2, 0));
    assert!(!closed_input_oracle(2, 1));
}

#[test]
fn complete_output_oracle_kills_projected_or_dropped_lane_inversion() {
    assert!(complete_output_oracle(3, 3));
    assert!(!complete_output_oracle(2, 3));
}

#[test]
fn initialization_order_oracle_kills_event_before_initial_commit_inversion() {
    assert!(!causal_oracle(&inverted(
        Inversion::EventBeforeInitializationCommit
    )));
}

#[test]
fn commit_order_oracle_kills_next_before_commit_inversion() {
    assert!(!causal_oracle(&inverted(Inversion::Prefetch)));
}

#[test]
fn no_prefetch_oracle_kills_early_second_input_inversion() {
    assert!(!causal_oracle(&inverted(Inversion::Prefetch)));
}

#[test]
fn self_send_oracle_kills_synchronous_reentry_inversion() {
    assert!(!causal_oracle(&inverted(Inversion::SynchronousSelfReentry)));
}

#[test]
fn exclusive_fold_oracle_kills_overlapping_fold_inversion() {
    assert!(exclusive_fold_oracle(1));
    assert!(!exclusive_fold_oracle(2));
}

#[test]
fn non_reentrancy_oracle_kills_fold_during_pending_commit_inversion() {
    assert!(non_reentrancy_oracle(0));
    assert!(!non_reentrancy_oracle(1));
}

#[test]
fn local_commit_oracle_kills_external_completion_wait_inversion() {
    assert!(local_commit_oracle(false));
    assert!(!local_commit_oracle(true));
}

#[test]
fn capability_event_oracle_kills_callback_fold_inversion() {
    assert!(capability_event_oracle(0, 1));
    assert!(!capability_event_oracle(1, 0));
}

#[test]
fn interpretation_count_oracle_kills_dropped_and_duplicate_commit_inversions() {
    assert!(interpretation_count_oracle(4, 4));
    assert!(!interpretation_count_oracle(4, 3));
    assert!(!interpretation_count_oracle(4, 5));
}

#[test]
fn lane_order_oracle_kills_reordering_inversion() {
    assert!(lane_order_oracle(&[1, 2, 3], &[1, 2, 3]));
    assert!(!lane_order_oracle(&[1, 2, 3], &[2, 1, 3]));
}

#[test]
fn payload_ownership_oracle_kills_drop_and_duplication_inversions() {
    assert!(payload_ownership_oracle(4, 4));
    assert!(!payload_ownership_oracle(4, 3));
    assert!(!payload_ownership_oracle(4, 5));
}

#[test]
fn honest_completion_oracle_kills_external_delivery_claim_inversion() {
    assert!(honest_completion_oracle(false));
    assert!(!honest_completion_oracle(true));
}

#[test]
fn interpretation_failure_oracle_kills_later_work_inversion() {
    assert!(interpretation_failure_oracle(0));
    assert!(!interpretation_failure_oracle(1));
}

#[test]
fn committed_prefix_oracle_kills_rollback_and_fabricated_prefix_inversions() {
    assert!(committed_prefix_oracle(1, 1));
    assert!(!committed_prefix_oracle(1, 0));
    assert!(!committed_prefix_oracle(1, 2));
}

#[test]
fn retry_oracle_kills_implicit_retry_inversion() {
    assert!(retry_oracle(1));
    assert!(!retry_oracle(2));
}

#[test]
fn no_rollback_oracle_kills_state_rollback_inversion() {
    assert!(no_rollback_oracle(3, 3));
    assert!(!no_rollback_oracle(3, 0));
}

#[test]
fn creation_precedence_oracle_kills_send_before_creation_inversion() {
    assert!(creation_precedence_oracle(2, 1));
    assert!(!creation_precedence_oracle(0, 1));
}

#[test]
fn creation_result_scope_oracle_kills_cross_action_and_reordering_inversions() {
    assert!(creation_result_scope_oracle(&[7, 8], &[7, 8]));
    assert!(!creation_result_scope_oracle(&[7, 8], &[8, 7]));
    assert!(!creation_result_scope_oracle(&[7, 8], &[6, 7, 8]));
}

#[test]
fn static_sufficiency_oracle_kills_missing_capability_acceptance_inversion() {
    assert!(static_sufficiency_oracle(false));
    assert!(!static_sufficiency_oracle(true));
}

#[test]
fn environment_substitutability_oracle_kills_concrete_environment_coupling_inversion() {
    assert!(environment_substitutability_oracle(2));
    assert!(!environment_substitutability_oracle(1));
}

#[test]
fn exact_error_oracle_kills_behavior_and_environment_erasure_inversions() {
    assert!(exact_error_oracle(false, false));
    assert!(!exact_error_oracle(true, false));
    assert!(!exact_error_oracle(false, true));
}

#[test]
fn source_order_oracle_kills_reordered_environment_event_inversion() {
    assert!(lane_order_oracle(&[1, 2, 3], &[1, 2, 3]));
    assert!(!lane_order_oracle(&[1, 2, 3], &[2, 1, 3]));
}
