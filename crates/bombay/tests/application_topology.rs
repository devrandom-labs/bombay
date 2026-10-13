//! Public topology assembly and complete native custody regressions.

#[path = "../../../examples/worker-pool/src/worker.rs"]
mod search_worker;

#[path = "application_topology/cold_shutdown_outer.rs"]
mod cold_shutdown_outer;
#[path = "application_topology/cold_timer_outer.rs"]
mod cold_timer_outer;
#[path = "application_topology/recursive.rs"]
mod recursive;

#[path = "application_topology/keyed_pool.rs"]
mod keyed_pool;
#[path = "application_topology/pool.rs"]
mod pool;
#[path = "application_topology/supervision.rs"]
mod supervision;

#[path = "application_topology/counter.rs"]
mod counter;

#[test]
fn topology_rejects_missing_capabilities_and_invalid_custody() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/topology_missing_trace.rs");
    cases.compile_fail("tests/compile/fail/topology_missing_readiness.rs");
    cases.compile_fail("tests/compile/fail/topology_forged_constructor.rs");
    cases.compile_fail("tests/compile/fail/topology_swapped_roles.rs");
    cases.compile_fail("tests/compile/fail/topology_duplicate_roles.rs");
    cases.compile_fail("tests/compile/fail/topology_erased_cold_origin.rs");
}

#[test]
fn topology_inference_rejects_forged_roles_and_incomplete_declarations() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/topology_inferred_missing_trace.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_missing_readiness.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_extra_birth.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_inconsistent_child.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_forged_position.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_forged_role.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_wrong_role_parent.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_missing_anchor.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_recursive_anchor.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_cold_root.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_multiple_root.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_unused_role_parent.rs");
    cases.compile_fail("tests/compile/fail/topology_inferred_root_role_parent.rs");
}

#[test]
fn topology_rejects_invalid_entity_class_sources() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/topology_family_missing_class.rs");
    cases.compile_fail("tests/compile/fail/topology_family_duplicate_class.rs");
    cases.compile_fail("tests/compile/fail/topology_family_root_class.rs");
    cases.compile_fail("tests/compile/fail/topology_family_wrong_definition.rs");
    cases.compile_fail("tests/compile/fail/topology_family_missing_effect_host.rs");
}
