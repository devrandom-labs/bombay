use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

#[allow(unused_imports)]
use behavior::{
    Acknowledgements, Barrier, Broadcast, Buffer, Cache, CircuitBreaker, Compose, Configuration,
    ConsistentHash, Correlator, Deadline, Deduplicator, Features, FinalizeOnShutdown, Health,
    KeyedWorkerPool, Latch, Lease, LeastLoaded, Machine, OneShot, OrderGate, Periodic, Presence,
    PriorityQueue, Proxy, PubSub, RateLimiter, Readiness, ReceiveTimeout, Registry, RendezvousHash,
    Resolver, RoundRobin, Router, Sequencer, Stash, StopOnShutdown, Supervisor, Task, Topic, Watch,
    WorkQueue, WorkerPool, Workflow,
};

const ACTORS_EXPORTS: &[&str] = &[
    "Machine",
    "Stash",
    "Task",
    "Watch",
    "FinalizeOnShutdown",
    "StopOnShutdown",
    "Proxy",
    "Supervisor",
    "WorkerPool",
    "KeyedWorkerPool",
    "Router<RoundRobin>",
    "Router<Broadcast>",
    "Router<LeastLoaded>",
    "Router<ConsistentHash>",
    "Router<RendezvousHash>",
    "WorkQueue",
    "PriorityQueue",
    "Buffer",
    "CircuitBreaker",
    "RateLimiter",
    "Correlator",
    "Acknowledgements",
    "Sequencer",
    "OrderGate",
    "Deduplicator",
    "Registry",
    "Resolver",
    "Presence",
    "Topic",
    "PubSub",
    "Deadline",
    "ReceiveTimeout",
    "OneShot",
    "Periodic",
    "Lease",
    "Workflow",
    "Barrier",
    "Latch",
    "Cache",
    "Health",
    "Readiness",
    "Configuration",
    "Features",
];

#[derive(Deserialize)]
struct Manifest {
    schema: u8,
    law_source: String,
    laws: Vec<Law>,
}

#[allow(clippy::struct_field_names)]
#[derive(Deserialize)]
struct Law {
    law: String,
    owner: String,
    positive: String,
    inversion: String,
    killer: String,
    negative: String,
    boundaries: String,
    adversarial: String,
    templates: String,
    command: String,
    status: String,
}

fn audit_obsolete_api(path: &std::path::Path, violations: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(path).expect("read repository") {
        let entry = entry.expect("repository entry");
        let path = entry.path();
        if path
            .file_name()
            .is_some_and(|name| name == "target" || name.to_string_lossy().starts_with('.'))
        {
            continue;
        }
        if path.is_dir() {
            audit_obsolete_api(&path, violations);
        } else if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("rs" | "md")
        ) && std::fs::read_to_string(&path).is_ok_and(|source| {
            source.contains(concat!("Send", "Product"))
                || source.contains(concat!("Inner", "<Path>"))
        }) {
            violations.push(path);
        }
    }
}

fn repository_artifacts() -> Vec<(String, String)> {
    fn visit(root: &std::path::Path, path: &std::path::Path, files: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(path).expect("read repository artifact directory") {
            let entry = entry.expect("repository artifact entry");
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if path.is_dir()
                && (name.starts_with('.')
                    || name == "target"
                    || name == "mutants.out"
                    || name == "mutants.out.old")
            {
                continue;
            }
            if path.is_dir() {
                visit(root, &path, files);
                continue;
            }
            if !matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("rs" | "md" | "toml" | "json" | "yml" | "yaml")
            ) {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .expect("artifact beneath repository")
                .to_string_lossy()
                .into_owned();
            let source = std::fs::read_to_string(&path).expect("text repository artifact");
            files.push((relative, source));
        }
    }

    let root = root();
    let mut files = Vec::new();
    visit(&root, &root, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn repository_artifacts_are_closed(files: &[(String, String)]) -> bool {
    let deleted_paths = [
        "crates/bombay-framework/",
        "crates/bombay/src/mailbox/",
        "crates/bombay/src/routing/",
        "crates/bombay/src/runtime/",
        "crates/bombay/fuzz/",
        "crates/bombay/benches/",
        "crates/bombay/examples/",
    ];
    if files.iter().any(|(path, _)| {
        deleted_paths
            .iter()
            .any(|deleted| path.starts_with(deleted))
    }) {
        return false;
    }

    let obsolete_contracts = [
        "System::spawn",
        "PreparedDriver",
        "RunExit",
        "RunError",
        "RuntimeEffects",
        "runtime_composition",
        "runtime_operations",
        "Prepared ->",
    ];
    files.iter().all(|(path, source)| {
        if path == "docs/open-design-ledger.md"
            || path == "docs/cookbook.md"
            || path == "docs/driver-law.md"
            || path == "crates/bombay-engine/tests/law_manifest.rs"
            || path.starts_with("crates/bombay-engine/tests/compile/")
            || path == "crates/bombay/src/core/incarnation.rs"
        {
            return true;
        }
        !obsolete_contracts
            .iter()
            .any(|obsolete| source.contains(obsolete))
    })
}

fn root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn manifest() -> Manifest {
    let path = root().join("docs/driver-law-manifest.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("read law manifest"))
        .expect("parse law manifest")
}

fn evidence_source(suite: &str) -> Option<std::path::PathBuf> {
    if suite == "behavior_actors" {
        return Some(root().join("tests/behavior_actors_scenarios.rs"));
    }
    let file = match suite {
        "driver_law" => "driver_law.rs",
        "driver_inversions" => "driver_inversions.rs",
        "driver_property" => "driver_property.rs",
        "compile" => "compile.rs",
        "law_manifest" => "law_manifest.rs",
        "incarnation" => {
            return Some(root().join("crates/bombay/src/core").join("incarnation.rs"));
        }
        "local" => {
            return Some(root().join("crates/bombay/src/core").join("local.rs"));
        }
        "generation" => {
            return Some(root().join("crates/bombay/src/core").join("generation.rs"));
        }
        _ => return None,
    };
    Some(root().join("crates/bombay-engine/tests").join(file))
}

fn test_reference_exists(reference: &str) -> bool {
    let Some((suite, test)) = reference.split_once("::") else {
        return false;
    };
    let Some(path) = evidence_source(suite) else {
        return false;
    };
    std::fs::read_to_string(path).is_ok_and(|source| source.contains(&format!("fn {test}(")))
}

fn attributed_actor_tests(rows: &[serde_json::Value]) -> BTreeSet<String> {
    let mut attributed = BTreeSet::new();
    for reference in rows.iter().flat_map(|row| {
        row["positive"]
            .as_str()
            .expect("positive evidence")
            .split(" + ")
    }) {
        assert!(
            attributed.insert(reference.to_owned()),
            "Behavior Actors Driver test attributed more than once: {reference}"
        );
    }
    attributed
}

fn executable_actor_tests() -> BTreeSet<String> {
    let source = std::fs::read_to_string(
        evidence_source("behavior_actors").expect("Behavior Actors evidence source"),
    )
    .expect("read Behavior Actors evidence");
    let mut tests = BTreeSet::new();
    let mut test_attribute = false;
    for line in source.lines() {
        let line = line.trim();
        if line.starts_with("#[tokio::test") {
            test_attribute = true;
        } else if test_attribute && line.starts_with("async fn ") {
            let name = line
                .trim_start_matches("async fn ")
                .split('(')
                .next()
                .expect("test function name");
            assert!(tests.insert(format!("behavior_actors::{name}")));
            test_attribute = false;
        }
    }
    tests
}

fn assert_test_reference_exists(law: &str, reference: &str) {
    assert!(
        test_reference_exists(reference),
        "{law} names stale or unknown executable evidence {reference}"
    );
}

fn assert_adversarial_references_exist(law: &str, references: &str) {
    let mut suite = None;
    for reference in references.split(" + ") {
        let qualified = if reference.contains("::") {
            suite = reference.split_once("::").map(|(suite, _)| suite);
            reference.to_owned()
        } else {
            format!(
                "{}::{reference}",
                suite.unwrap_or_else(|| panic!("{law} starts with unqualified evidence"))
            )
        };
        assert_test_reference_exists(law, &qualified);
    }
}

fn canonical_ids() -> Vec<String> {
    let law = std::fs::read_to_string(root().join("docs/driver-law.md")).expect("read Driver law");
    law.lines()
        .filter_map(|line| {
            let start = line.find("**D-")? + 2;
            let tail = &line[start..];
            let end = tail.find(" —")?;
            Some(tail[..end].to_owned())
        })
        .collect()
}

fn exact_law_rows(canonical: &[String], rows: &[String]) -> bool {
    rows == canonical && rows.iter().collect::<BTreeSet<_>>().len() == rows.len()
}

fn every_law_executed(statuses: &[String]) -> bool {
    statuses.iter().all(|status| status == "passing")
}

fn exact_template_rows(rows: &[&str]) -> bool {
    rows == ACTORS_EXPORTS && rows.iter().collect::<BTreeSet<_>>().len() == rows.len()
}

fn validate_shared_evidence(row: &Law) {
    if matches!(
        row.law.as_str(),
        "D-INC-1" | "D-INC-2" | "D-INC-3" | "D-INC-6"
    ) {
        assert_eq!(
            row.negative,
            "generation::address_collision_rejects_activation_without_replacing_the_live_generation"
        );
        assert_eq!(
            row.boundaries,
            "local::rejected_initial_commit_exposes_no_endpoint_and_closes_anchor"
        );
        assert_eq!(
            row.adversarial,
            "generation::panic_and_cancellation_release_address_before_exact_terminal_publication + observer_failure_cannot_change_or_prevent_terminal_publication"
        );
        assert_eq!(
            row.templates,
            "generation::root_and_child_generations_use_the_same_transactional_activation_path"
        );
        assert_eq!(
            row.command,
            "cargo test -p bombay-rs --lib core::generation::tests"
        );
        return;
    }
    if matches!(row.law.as_str(), "D-INC-4" | "D-INC-5") {
        assert_eq!(
            row.negative,
            "driver_law::controlled_failure_is_terminal_and_commits_no_nonexistent_actions"
        );
        assert_eq!(
            row.boundaries,
            "incarnation::source_exhaustion_preserves_its_exact_successful_cause"
        );
        assert_eq!(
            row.adversarial,
            "incarnation::panic_drops_driver_before_exactly_one_terminal_classification + cancellation_drops_driver_before_exactly_one_terminal_classification"
        );
        assert_eq!(
            row.templates,
            "law_manifest::template_manifest_matches_selected_actors_exports"
        );
        assert_eq!(
            row.command,
            "cargo test -p bombay-rs --lib core::incarnation::tests"
        );
        return;
    }
    assert_eq!(
        row.negative,
        "driver_law::controlled_failure_is_terminal_and_commits_no_nonexistent_actions"
    );
    assert_eq!(
        row.boundaries,
        "driver_property::zero_singleton_limit_and_post_stop_boundaries"
    );
    assert_eq!(
        row.adversarial,
        "driver_law::panic_consumes_the_only_execution_and_cannot_poll_again + cancellation_drops_ownership_without_claiming_async_retirement + commit_failure_preserves_the_factual_committed_prefix"
    );
    assert_eq!(
        row.templates,
        "law_manifest::template_manifest_matches_selected_actors_exports"
    );
    assert_eq!(row.command, "cargo test -p bombay-engine");
}

#[allow(
    clippy::too_many_lines,
    reason = "the reviewed evidence registry is intentionally exhaustive"
)]
fn validate_evidence(row: &Law) {
    for (field, value) in [
        ("owner", &row.owner),
        ("positive", &row.positive),
        ("inversion", &row.inversion),
        ("killer", &row.killer),
        ("negative", &row.negative),
        ("boundaries", &row.boundaries),
        ("adversarial", &row.adversarial),
        ("templates", &row.templates),
        ("command", &row.command),
    ] {
        assert!(!value.trim().is_empty(), "{} has empty {field}", row.law);
    }
    assert!(matches!(
        row.status.as_str(),
        "planned" | "blocked" | "passing"
    ));
    assert_test_reference_exists(&row.law, &row.positive);
    assert_test_reference_exists(&row.law, &row.killer);
    assert_test_reference_exists(&row.law, &row.negative);
    assert_test_reference_exists(&row.law, &row.boundaries);
    assert_test_reference_exists(&row.law, &row.templates);
    assert_adversarial_references_exist(&row.law, &row.adversarial);
    assert!(
        [
            "driver_law::universal_causal_transcript_has_no_prefetch_or_reentrancy",
            "driver_law::complete_move_only_stop_actions_cross_once_before_completion",
            "driver_law::pending_input_is_polled_once_without_busy_wait_or_self_wake",
            "driver_law::cancellation_at_every_await_drops_ownership_without_false_completion_or_retirement",
            "driver_law::initialization_and_turn_panics_consume_the_only_execution_and_cannot_poll_again",
            "driver_law::completion_preserves_stop_and_input_exhaustion_as_success",
            "driver_law::every_ordinary_return_attempts_retirement_exactly_once",
            "driver_law::source_closure_folds_no_synthetic_event_and_retires_once",
            "driver_law::every_ordinary_terminal_edge_is_fused_against_later_work",
            "driver_law::initialization_occurs_once_across_stop_closure_and_failure_boundaries",
            "driver_law::every_accepted_event_is_folded_exactly_once",
            "driver_law::successor_state_and_complete_actions_come_from_the_same_decision",
            "driver_law::controlled_failure_is_terminal_and_commits_no_nonexistent_actions",
            "driver_law::unrelated_custom_behavior_shapes_use_the_same_driver_algorithm",
            "driver_law::driver_accepts_only_the_final_closed_behavior_event_type",
            "driver_law::self_send_reenters_only_as_a_later_ordinary_event",
            "driver_law::at_most_one_behavior_fold_is_active",
            "driver_law::pending_commit_prevents_reentrant_or_later_fold",
            "driver_law::local_commitment_advances_only_through_a_later_capability_event",
            "driver_law::every_successful_decision_is_committed_exactly_once",
            "driver_law::commit_failure_preserves_the_factual_committed_prefix",
            "driver_law::commitment_failure_does_not_roll_back_the_successful_fold",
            "driver_law::environment_preserves_creation_precedence_and_same_action_result_scope",
            "driver_law::one_behavior_is_substitutable_across_distinct_static_environments",
            "driver_law::exact_behavior_and_environment_errors_remain_distinct",
            "compile::driver_surface_conformance",
            "law_manifest::repository_has_one_direct_driver_path_and_no_obsolete_product_api",
            "law_manifest::repository_closure_accounts_for_every_current_driver_artifact",
            "law_manifest::manifest_exactly_matches_canonical_law_index",
            "law_manifest::driver_has_no_observation_control_surface",
            "incarnation::successful_completion_drops_driver_then_retires_once",
            "incarnation::exact_driver_failures_remain_distinct",
            "incarnation::panic_drops_driver_before_exactly_one_terminal_classification",
            "incarnation::core_surface_has_one_driver_run_and_no_split_lifecycle",
            "local::complete_actions_commit_once_and_initialization_precedes_publication",
            "local::rejected_initial_commit_exposes_no_endpoint_and_closes_anchor",
            "generation::root_and_child_generations_use_the_same_transactional_activation_path",
            "generation::replacement_uses_fresh_driver_environment_address_and_observation_generations",
            "generation::panic_and_cancellation_release_address_before_exact_terminal_publication",
            "generation::address_collision_rejects_activation_without_replacing_the_live_generation",
        ]
        .contains(&row.positive.as_str()),
        "{} names unknown positive evidence: {}",
        row.law,
        row.positive
    );
    assert!(
        [
            "driver_inversions::causal_algorithm_mutations",
            "driver_inversions::pending_progress_mutations",
            "driver_inversions::cancellation_ownership_mutations",
            "driver_inversions::panic_terminality_mutations",
            "driver_inversions::collapsed_swapped_or_failure_completion_mutations",
            "driver_inversions::ordinary_retirement_count_mutations",
            "driver_inversions::explicit_stop_mutations",
            "driver_inversions::source_closure_mutations",
            "driver_inversions::terminal_fusion_mutations",
            "driver_inversions::initialization_count_mutations",
            "driver_inversions::accepted_fold_count_mutations",
            "driver_inversions::decision_integrity_mutations",
            "driver_inversions::controlled_failure_mutations",
            "driver_inversions::shape_specific_driver_mutation",
            "driver_inversions::driver_owned_input_side_channel_mutation",
            "driver_inversions::projected_action_output_mutation",
            "driver_inversions::event_before_initial_commit_mutation",
            "driver_inversions::next_before_turn_commit_mutation",
            "driver_inversions::early_second_input_mutation",
            "driver_inversions::synchronous_self_reentry_mutation",
            "driver_inversions::overlapping_fold_mutation",
            "driver_inversions::fold_during_pending_commit_mutation",
            "driver_inversions::external_completion_wait_mutation",
            "driver_inversions::capability_callback_fold_mutation",
            "driver_inversions::interpretation_count_mutations",
            "driver_inversions::lane_reordering_mutation",
            "driver_inversions::payload_drop_or_duplication_mutations",
            "driver_inversions::external_delivery_claim_mutation",
            "driver_inversions::post_interpretation_failure_work_mutation",
            "driver_inversions::fictitious_transaction_mutations",
            "driver_inversions::implicit_retry_mutation",
            "driver_inversions::state_rollback_mutation",
            "driver_inversions::send_before_creation_mutation",
            "driver_inversions::cross_action_creation_result_mutation",
            "driver_inversions::missing_capability_acceptance_mutation",
            "driver_inversions::concrete_environment_coupling_mutation",
            "driver_inversions::typed_error_erasure_mutations",
            "driver_inversions::source_event_reordering_mutation",
            "law_manifest::structural_surface_and_authority_mutations",
            "law_manifest::repository_closure_oracle_kills_stale_path_and_contract_inversions",
            "law_manifest::observation_control_mutations",
            "incarnation::incarnation_terminal_mutations_are_deliberate_semantic_inversions",
            "local::local_activation_inversions_are_deliberate_semantic_mutations",
            "generation::generation_inversions_are_deliberate_semantic_mutations",
        ]
        .contains(&row.inversion.as_str()),
        "{} names unknown inversion: {}",
        row.law,
        row.inversion
    );
    assert!(
        [
            "driver_inversions::causal_oracle_kills_every_deliberate_algorithm_inversion",
            "driver_inversions::pending_progress_oracle_kills_spin_and_self_wake_inversions",
            "driver_inversions::cancellation_oracle_kills_leak_false_retirement_and_false_completion_inversions",
            "driver_inversions::panic_terminality_oracle_kills_recovery_repoll_and_leak_inversions",
            "driver_inversions::completion_oracle_kills_collapsed_swapped_and_failure_classifications",
            "driver_inversions::ordinary_retirement_oracle_kills_missing_and_duplicate_retirement_inversions",
            "driver_inversions::explicit_stop_oracle_kills_dropped_final_actions_and_later_ingress_inversions",
            "driver_inversions::source_closure_oracle_kills_synthetic_fold_and_repoll_inversions",
            "driver_inversions::terminal_fusion_oracle_kills_every_post_terminal_work_inversion",
            "driver_inversions::initialization_count_oracle_kills_missing_and_duplicate_initialization_inversions",
            "driver_inversions::accepted_fold_count_oracle_kills_missing_and_duplicate_fold_inversions",
            "driver_inversions::decision_integrity_oracle_kills_stale_state_and_stale_action_inversions",
            "driver_inversions::controlled_failure_oracle_kills_fabricated_actions_and_continuation_inversions",
            "driver_inversions::universality_oracle_kills_shape_specific_driver_inversion",
            "driver_inversions::closed_input_oracle_kills_driver_owned_side_channel_inversion",
            "driver_inversions::complete_output_oracle_kills_projected_or_dropped_lane_inversion",
            "driver_inversions::initialization_order_oracle_kills_event_before_initial_commit_inversion",
            "driver_inversions::commit_order_oracle_kills_next_before_commit_inversion",
            "driver_inversions::no_prefetch_oracle_kills_early_second_input_inversion",
            "driver_inversions::self_send_oracle_kills_synchronous_reentry_inversion",
            "driver_inversions::exclusive_fold_oracle_kills_overlapping_fold_inversion",
            "driver_inversions::non_reentrancy_oracle_kills_fold_during_pending_commit_inversion",
            "driver_inversions::local_commit_oracle_kills_external_completion_wait_inversion",
            "driver_inversions::capability_event_oracle_kills_callback_fold_inversion",
            "driver_inversions::interpretation_count_oracle_kills_dropped_and_duplicate_commit_inversions",
            "driver_inversions::lane_order_oracle_kills_reordering_inversion",
            "driver_inversions::payload_ownership_oracle_kills_drop_and_duplication_inversions",
            "driver_inversions::honest_completion_oracle_kills_external_delivery_claim_inversion",
            "driver_inversions::interpretation_failure_oracle_kills_later_work_inversion",
            "driver_inversions::committed_prefix_oracle_kills_rollback_and_fabricated_prefix_inversions",
            "driver_inversions::retry_oracle_kills_implicit_retry_inversion",
            "driver_inversions::no_rollback_oracle_kills_state_rollback_inversion",
            "driver_inversions::creation_precedence_oracle_kills_send_before_creation_inversion",
            "driver_inversions::creation_result_scope_oracle_kills_cross_action_and_reordering_inversions",
            "driver_inversions::static_sufficiency_oracle_kills_missing_capability_acceptance_inversion",
            "driver_inversions::environment_substitutability_oracle_kills_concrete_environment_coupling_inversion",
            "driver_inversions::exact_error_oracle_kills_behavior_and_environment_erasure_inversions",
            "driver_inversions::source_order_oracle_kills_reordered_environment_event_inversion",
            "law_manifest::structural_oracle_kills_surface_and_authority_inversions",
            "law_manifest::repository_closure_oracle_kills_stale_path_and_contract_inversions",
            "law_manifest::observation_oracle_kills_control_surface_inversions",
            "incarnation::incarnation_oracles_kill_order_count_and_classification_inversions",
            "local::local_activation_ordering_oracle_kills_every_publication_inversion",
            "generation::generation_ordering_oracle_kills_release_and_identity_inversions",
        ]
        .contains(&row.killer.as_str()),
        "{} names unknown killer: {}",
        row.law,
        row.killer
    );
    validate_shared_evidence(row);
}

#[test]
fn manifest_exactly_matches_canonical_law_index() {
    let manifest = manifest();
    assert_eq!(manifest.schema, 1);
    assert_eq!(manifest.law_source, "docs/driver-law.md");

    let canonical = canonical_ids();
    assert_eq!(
        canonical.len(),
        74,
        "law-count changes require explicit review"
    );
    let rows: Vec<_> = manifest.laws.iter().map(|row| row.law.clone()).collect();
    assert!(
        exact_law_rows(&canonical, &rows),
        "missing, duplicate, renamed, reordered, stale, or unknown law row"
    );

    manifest.laws.iter().for_each(validate_evidence);
}

#[test]
fn manifest_gate_kills_missing_duplicate_renamed_unknown_and_unexecuted_laws() {
    let canonical = canonical_ids();
    assert!(exact_law_rows(&canonical, &canonical));

    let mut missing = canonical.clone();
    missing.pop();
    assert!(!exact_law_rows(&canonical, &missing));

    let mut duplicate = canonical.clone();
    duplicate.push(canonical[0].clone());
    assert!(!exact_law_rows(&canonical, &duplicate));

    let mut renamed = canonical.clone();
    renamed[0].push_str("-RENAMED");
    assert!(!exact_law_rows(&canonical, &renamed));

    let mut unknown = canonical.clone();
    unknown.push("D-UNKNOWN-1".to_owned());
    assert!(!exact_law_rows(&canonical, &unknown));

    let passing = vec!["passing".to_owned(); canonical.len()];
    assert!(every_law_executed(&passing));
    for stale_status in ["planned", "blocked", "unknown"] {
        let mut unexecuted = passing.clone();
        unexecuted[canonical.len() / 2] = stale_status.to_owned();
        assert!(!every_law_executed(&unexecuted));
    }
}

#[test]
fn manifest_gate_kills_stale_renamed_and_unknown_executable_evidence() {
    assert!(test_reference_exists(
        "driver_law::every_accepted_event_is_folded_exactly_once"
    ));
    assert!(!test_reference_exists(
        "driver_law::renamed_event_is_folded_exactly_once"
    ));
    assert!(!test_reference_exists(
        "unknown_suite::every_accepted_event_is_folded_exactly_once"
    ));
    assert!(!test_reference_exists("unqualified_test_name"));
}

#[test]
#[ignore = "explicit completion gate; run with --ignored"]
fn executes_all_manifest_evidence() {
    let manifest = manifest();
    let statuses: Vec<_> = manifest.laws.iter().map(|row| row.status.clone()).collect();
    let not_passing: BTreeMap<_, _> = manifest
        .laws
        .iter()
        .filter(|row| row.status != "passing")
        .map(|row| (row.law.as_str(), row.status.as_str()))
        .collect();
    assert!(
        every_law_executed(&statuses) && not_passing.is_empty(),
        "every law must have executed positive and inversion evidence: {not_passing:?}"
    );
}

#[test]
fn template_manifest_matches_selected_actors_exports() {
    let path = root().join("docs/driver-template-manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read template manifest"))
            .expect("parse template manifest");
    assert_eq!(manifest["schema"], 1);
    assert_eq!(manifest["crate"], "bombay-behavior-actors");
    assert_eq!(manifest["version"], "0.12.0");

    let rows = manifest["templates"].as_array().expect("template rows");
    let row_names: Vec<_> = rows
        .iter()
        .map(|row| row["template"].as_str().expect("template name"))
        .collect();
    assert!(
        exact_template_rows(&row_names),
        "template inventory changed"
    );
    let mut names = BTreeSet::new();
    for row in rows {
        let template = row["template"].as_str().expect("template name");
        assert!(names.insert(template), "duplicate template {template}");
        assert_eq!(
            row["revision"], "40b39b2605416e3b88427e3289c4dac4568c78e0",
            "{template} has stale owner revision"
        );
        for field in [
            "family",
            "revision",
            "domain",
            "events",
            "actions",
            "composition",
            "positive",
            "inversion",
            "killer",
            "command",
            "status",
        ] {
            assert!(
                row[field].as_str().is_some_and(|value| !value.is_empty()),
                "{template} has empty {field}"
            );
        }
        assert!(matches!(
            row["status"].as_str(),
            Some("planned" | "blocked" | "passing")
        ));
        let positive = row["positive"].as_str().expect("positive evidence");
        assert!(
            positive.starts_with("behavior_actors::"),
            "{template} must name concrete Driver execution evidence, not an owner-suite placeholder"
        );
        for reference in positive.split(" + ") {
            assert_test_reference_exists(template, reference);
        }

        assert_eq!(
            row["killer"], "template_manifest_matches_selected_actors_exports",
            "{template} must use the canonical inventory killer"
        );
        assert_test_reference_exists(
            template,
            "law_manifest::template_manifest_matches_selected_actors_exports",
        );
    }
    assert_eq!(
        attributed_actor_tests(rows),
        executable_actor_tests(),
        "every executable Behavior Actors Driver test must be attributed to exactly one manifest inventory"
    );
}

#[test]
fn template_gate_kills_missing_duplicate_renamed_reordered_and_unknown_exports() {
    assert!(exact_template_rows(ACTORS_EXPORTS));

    let mut missing = ACTORS_EXPORTS.to_vec();
    missing.pop();
    assert!(!exact_template_rows(&missing));

    let mut duplicate = ACTORS_EXPORTS.to_vec();
    duplicate.push(ACTORS_EXPORTS[0]);
    assert!(!exact_template_rows(&duplicate));

    let mut renamed = ACTORS_EXPORTS.to_vec();
    renamed[0] = "RenamedCompose";
    assert!(!exact_template_rows(&renamed));

    let mut reordered = ACTORS_EXPORTS.to_vec();
    reordered.swap(0, 1);
    assert!(!exact_template_rows(&reordered));

    let mut unknown = ACTORS_EXPORTS.to_vec();
    unknown.push("UnknownTemplate");
    assert!(!exact_template_rows(&unknown));
}

#[test]
#[ignore = "explicit completion gate; run with --ignored"]
fn executes_all_template_evidence() {
    let path = root().join("docs/driver-template-manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read template manifest"))
            .expect("parse template manifest");
    let incomplete: Vec<_> = manifest["templates"]
        .as_array()
        .expect("template rows")
        .iter()
        .filter(|row| row["status"] != "passing")
        .map(|row| row["template"].as_str().expect("template name"))
        .collect();
    assert!(
        incomplete.is_empty(),
        "every exported template requires executed Driver evidence: {incomplete:?}"
    );
}

#[test]
fn repository_has_one_direct_driver_path_and_no_obsolete_product_api() {
    let root = root();
    let engine_manifest =
        std::fs::read_to_string(root.join("crates/bombay-engine/Cargo.toml")).unwrap();
    assert!(!engine_manifest.contains("bombay-transition"));
    assert!(!engine_manifest.contains("bombay-machine-executor"));
    assert!(
        !root
            .join("crates/bombay-engine/src/behavior_machine.rs")
            .exists()
    );

    let driver = std::fs::read_to_string(root.join("crates/bombay-engine/src/driver.rs")).unwrap();
    assert_eq!(
        driver.matches("behavior.transition(event)").count(),
        1,
        "the production Driver must contain exactly one direct fold site"
    );
    for obsolete in [
        "ExclusiveExecutor",
        "BehaviorMachine",
        "PreparedDriver",
        "RuntimeEffects",
        "from_definition",
        "run_init",
        "run_loop",
        "fn recover",
        "fn reset",
        "fn restart",
        "fn reuse",
        "fn clear_poison",
        "DriverError::Poisoned",
        "behavior::Task",
        "behavior::Supervisor",
        "dyn Any",
        "downcast",
        "type_id",
        "transition(event).await",
        "spawn(",
        "yield_now",
        "    registry:",
        "HashMap<TypeId",
        "B: Behavior<Ph = Never> +",
        "E: ActiveEnvironment<B> +",
        "'static",
        "#[derive(Clone)]\npub struct Driver",
    ] {
        assert!(
            !driver.contains(obsolete),
            "obsolete Driver surface: {obsolete}"
        );
    }
    for forbidden_authority in [
        "    address:",
        "    mailbox:",
        "    router:",
        "    scheduler:",
        "    dispatcher:",
        "    registration:",
        "    generation:",
    ] {
        assert!(
            !driver.contains(forbidden_authority),
            "Driver acquired forbidden authority: {forbidden_authority}"
        );
    }

    let exports = std::fs::read_to_string(root.join("crates/bombay-engine/src/lib.rs")).unwrap();
    assert!(exports.contains("pub use driver::{ActionsOf, Completion, Driver, DriverError};"));
    assert!(exports.contains("ActiveEnvironment"));
    assert!(exports.contains("Environment"));
    assert!(driver.contains("pub enum Completion"));
    assert!(driver.contains("Stopped"));
    assert!(driver.contains("Exhausted"));
    assert!(
        driver.contains("<B as Behavior>::Ph"),
        "ActionsOf<B> must preserve Behavior's own phase algebra"
    );
    assert!(!root.join("crates/bombay-engine/src/run.rs").exists());

    let mut violations = Vec::new();
    audit_obsolete_api(&root, &mut violations);
    assert!(
        violations.is_empty(),
        "obsolete positional product guidance: {violations:?}"
    );
}

#[test]
fn repository_closure_accounts_for_every_current_driver_artifact() {
    assert!(repository_artifacts_are_closed(&repository_artifacts()));
}

#[test]
fn repository_closure_oracle_kills_stale_path_and_contract_inversions() {
    let files = repository_artifacts();
    assert!(repository_artifacts_are_closed(&files));

    for stale_path in [
        "crates/bombay-framework/src/lib.rs",
        "crates/bombay/src/runtime/system.rs",
        "crates/bombay/fuzz/fuzz_targets/runtime_operations.rs",
    ] {
        let mut inverted = files.clone();
        inverted.push((stale_path.to_owned(), "pub struct Legacy;".to_owned()));
        assert!(!repository_artifacts_are_closed(&inverted));
    }

    for stale_contract in [
        "System::spawn",
        "PreparedDriver",
        "RunExit",
        "RuntimeEffects",
        "runtime_composition",
        "Prepared -> Live",
    ] {
        let mut inverted = files.clone();
        inverted.push((
            "docs/current-driver-adapter.md".to_owned(),
            stale_contract.to_owned(),
        ));
        assert!(!repository_artifacts_are_closed(&inverted));
    }
}

fn observation_is_nonsemantic(source: &str) -> bool {
    [
        "observer",
        "observation",
        "trace",
        "metric",
        "diagnostic",
        "callback",
    ]
    .iter()
    .all(|authority| !source.contains(authority))
}

#[test]
fn driver_has_no_observation_control_surface() {
    let source =
        std::fs::read_to_string(root().join("crates/bombay-engine/src/driver.rs")).unwrap();
    assert!(observation_is_nonsemantic(&source));
}

#[test]
fn observation_oracle_kills_control_surface_inversions() {
    let source =
        std::fs::read_to_string(root().join("crates/bombay-engine/src/driver.rs")).unwrap();
    for inversion in [
        "\n    observer: (),",
        "\n    tracing_callback: (),",
        "\n    metrics_select_work: (),",
        "\n    diagnostic_keeps_alive: (),",
    ] {
        assert!(!observation_is_nonsemantic(&format!("{source}{inversion}")));
    }
}

fn structural_driver_oracle(source: &str) -> bool {
    source.matches("behavior.transition(event)").count() == 1
        && source.matches("\n    environment: E,").count() == 1
        && [
            "ExclusiveExecutor",
            "BehaviorMachine",
            "PreparedDriver",
            "RuntimeEffects",
            "from_definition",
            "run_init",
            "run_loop",
            "fn recover",
            "fn reset",
            "fn restart",
            "fn reuse",
            "fn clear_poison",
            "DriverError::Poisoned",
            "behavior::Task",
            "behavior::Supervisor",
            "dyn Any",
            "downcast",
            "type_id",
            "transition(event).await",
            "spawn(",
            "yield_now",
            "    registry:",
            "HashMap<TypeId",
            "B: Behavior<Ph = Never> +",
            "E: ActiveEnvironment<B> +",
            "'static",
            "#[derive(Clone)]\npub struct Driver",
            "    address:",
            "    mailbox:",
            "    router:",
            "    scheduler:",
            "    dispatcher:",
            "    registration:",
            "    generation:",
        ]
        .iter()
        .all(|forbidden| !source.contains(forbidden))
}

#[test]
fn structural_oracle_kills_surface_and_authority_inversions() {
    let source =
        std::fs::read_to_string(root().join("crates/bombay-engine/src/driver.rs")).unwrap();
    assert!(structural_driver_oracle(&source));

    for inversion in [
        "\nstruct BehaviorMachine;",
        "\nstruct PreparedDriver;",
        "\nstruct RuntimeEffects;",
        "\nfn run_init() {}",
        "\nfn run_loop() {}",
        "\nfn recover() {}",
        "\nfn reset() {}",
        "\nfn restart() {}",
        "\nfn reuse() {}",
        "\nfn clear_poison() {}",
        "\ntype TemplateSpecialCase = behavior::Task;",
        "\ntype SupervisionSpecialCase = behavior::Supervisor;",
        "\nfn inspect(_: &dyn Any) {}",
        "\nfn downcast() {}",
        "\nfn type_id() {}",
        "\nfn spawn() {}",
        "\nfn yield_now() {}",
        "\n    registry: (),",
        "\ntype Capabilities = HashMap<TypeId, Box<dyn Any>>;",
        "\nfn extra_behavior_bound<B: Behavior<Ph = Never> + Sync>() {}",
        "\nfn extra_environment_bound<B, E: ActiveEnvironment<B> + Send>() where B: Behavior<Ph = Never> {}",
        "\nfn static_bound<T: 'static>() {}",
        "\n#[derive(Clone)]\npub struct Driver;",
        "\n    address: u64,",
        "\n    mailbox: (),",
        "\n    router: (),",
        "\n    scheduler: (),",
        "\n    dispatcher: (),",
        "\n    registration: (),",
        "\n    generation: u64,",
    ] {
        let mutated = format!("{source}{inversion}");
        assert!(!structural_driver_oracle(&mutated));
    }

    let bypass = source.replacen(
        "behavior.transition(event)",
        "behavior.transition(event); behavior.transition(event)",
        1,
    );
    assert!(!structural_driver_oracle(&bypass));

    let asynchronous_fold = source.replacen(
        "behavior.transition(event)",
        "behavior.transition(event).await",
        1,
    );
    assert!(!structural_driver_oracle(&asynchronous_fold));
}
