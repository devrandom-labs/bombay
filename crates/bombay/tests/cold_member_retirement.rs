//! Inferred cold actor classes preserve occurrence-specific hosting and complete custody.

#[path = "cold_member_retirement/descendants.rs"]
mod descendants;
#[path = "cold_member_retirement/equal_actors.rs"]
mod equal_actors;
#[path = "cold_member_retirement/recursive.rs"]
mod recursive;

#[test]
fn cold_topology_rejects_incomplete_births_hosts_and_roles() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/topology_cold_wrong_role.rs");
    cases.compile_fail("tests/compile/fail/topology_cold_wrong_host.rs");
    cases.compile_fail("tests/compile/fail/topology_cold_extra_birth.rs");
    cases.compile_fail("tests/compile/fail/topology_cold_recursive_owner.rs");
}

#[test]
fn cold_topology_accepts_actual_member_types_without_annotations() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/topology_inferred_cold_classes.rs");
}
