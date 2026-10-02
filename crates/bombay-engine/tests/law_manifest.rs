use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

const BEHAVIOR_REVISION: &str = "804b2bf25325a523884ec49d8a4ae6d2d2b6e9da";

#[derive(Deserialize)]
struct Manifest {
    schema: u8,
    law_source: String,
    behavior: BehaviorSelection,
    gate: EvidenceGate,
    laws: Vec<Law>,
}

#[derive(Deserialize)]
struct BehaviorSelection {
    core: String,
    actors: String,
    macros: String,
    revision: String,
}

#[derive(Deserialize)]
struct EvidenceGate {
    command: String,
    artifact: String,
}

#[allow(clippy::struct_field_names)]
#[derive(Deserialize)]
struct Law {
    law: String,
    owner: String,
    positive: TestEvidence,
    boundary: TestEvidence,
    inversion: InversionEvidence,
}

#[derive(Deserialize)]
struct TestEvidence {
    id: String,
    revision: String,
    claim: String,
    test: String,
    command: String,
}

#[derive(Deserialize)]
struct InversionEvidence {
    id: String,
    revision: String,
    claim: String,
    target: String,
    mutation: String,
    killer: String,
    command: String,
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

fn rust_source_contains(path: &std::path::Path, excluded: Option<&str>, needle: &str) -> bool {
    std::fs::read_dir(path)
        .expect("read Rust source tree")
        .map(|entry| entry.expect("Rust source entry").path())
        .any(|path| {
            if path.is_dir() {
                rust_source_contains(&path, excluded, needle)
            } else {
                path.extension().and_then(|extension| extension.to_str()) == Some("rs")
                    && path.file_name().and_then(|name| name.to_str()) != excluded
                    && std::fs::read_to_string(path).is_ok_and(|source| source.contains(needle))
            }
        })
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
        "crates/bombay/benches/runtime_operations.rs",
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
        "RuntimeEffects",
        "runtime_composition",
        "runtime_operations",
        "Prepared ->",
        "name = \"bombay-framework\"",
    ];
    files.iter().all(|(path, source)| {
        if path == "docs/driver-law.md"
            || path == "crates/bombay-engine/tests/law_manifest.rs"
            || path.starts_with("crates/bombay-engine/tests/compile/")
            || path == "crates/bombay/src/actor_execution.rs"
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
    let file = match suite {
        "driver_law" => "driver_law.rs",
        "source_settlement_order" => "source_settlement_order.rs",
        "terminal_custody" => "terminal_custody.rs",
        "compile" => "compile.rs",
        "law_manifest" => "law_manifest.rs",
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

fn assert_test_reference_exists(law: &str, reference: &str) {
    assert!(
        test_reference_exists(reference),
        "{law} names stale or unknown executable evidence {reference}"
    );
}

fn test_command(reference: &str) -> Option<String> {
    let (suite, test) = reference.split_once("::")?;
    evidence_source(suite)?;
    Some(format!(
        "nix develop -c cargo test --locked -p bombay-engine --test {suite} {test} -- --exact"
    ))
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

fn validate_test_evidence(law: &str, evidence: &TestEvidence) {
    for (field, value) in [
        ("id", &evidence.id),
        ("revision", &evidence.revision),
        ("claim", &evidence.claim),
        ("test", &evidence.test),
        ("command", &evidence.command),
    ] {
        assert!(!value.trim().is_empty(), "{law} has empty {field}");
    }
    assert_test_reference_exists(law, &evidence.test);
    assert_eq!(
        Some(evidence.command.as_str()),
        test_command(&evidence.test).as_deref(),
        "{law} test command does not run its exact reference"
    );
}

fn validate_inversion_evidence(law: &str, evidence: &InversionEvidence) {
    for (field, value) in [
        ("id", &evidence.id),
        ("revision", &evidence.revision),
        ("claim", &evidence.claim),
        ("target", &evidence.target),
        ("mutation", &evidence.mutation),
        ("killer", &evidence.killer),
        ("command", &evidence.command),
    ] {
        assert!(
            !value.trim().is_empty(),
            "{law} has empty inversion {field}"
        );
    }
    assert!(
        root().join(&evidence.target).is_file(),
        "{law} mutation target is absent"
    );
    assert_test_reference_exists(law, &evidence.killer);
    assert_eq!(
        evidence.command,
        format!(
            "nix develop -c bash crates/bombay-engine/tests/driver-law-evidence.sh --law {law}"
        )
    );
    let runner =
        std::fs::read_to_string(root().join("crates/bombay-engine/tests/driver-law-evidence.sh"))
            .expect("read executable evidence runner");
    assert!(
        runner.contains(&format!("{law}:{id}", id = evidence.id)),
        "{law} inversion {} has no executable mutation",
        evidence.id
    );
}

#[test]
fn production_driver_mutation_evidence_has_one_owner() {
    let root = root();
    let mut disconnected_evidence = Vec::new();
    if root
        .join("crates/bombay-engine/tests/driver_inversions.rs")
        .exists()
    {
        disconnected_evidence.push("disconnected Driver inversion suite remains");
    }

    let manifest = std::fs::read_to_string(root.join("docs/driver-law-manifest.json"))
        .expect("read Driver law manifest");
    if manifest.contains("driver_inversions::") {
        disconnected_evidence.push("manifest still names disconnected Driver inversions");
    }

    let flake = std::fs::read_to_string(root.join("flake.nix")).expect("read flake");
    let required_mutation = flake
        .split_once("mutants = craneLib.mkCargoDerivation")
        .and_then(|(_, packages)| packages.split_once("mutants-sweep ="))
        .map(|(required, _)| required)
        .expect("required mutation derivation remains present");
    if !required_mutation.contains("--package bombay-engine") {
        disconnected_evidence.push("required mutation derivation omits bombay-engine");
    }
    let nextest_profiles = std::fs::read_to_string(root.join(".config/nextest.toml"))
        .expect("read Nextest mutation profile");
    if !nextest_profiles.contains("[profile.mutants]")
        || flake.matches("-- --profile mutants").count() != 2
        || flake
            .replace("-- --profile mutants", "")
            .contains("--profile mutants")
    {
        disconnected_evidence.push("mutation derivations lack the declared Nextest profile");
    }

    let baseline = std::fs::read_to_string(root.join("mutants-baseline.json"))
        .expect("read mutation baseline");
    if !baseline.contains("crates/bombay-engine/src/driver.rs::") {
        disconnected_evidence.push("reviewed mutation baseline omits the production Driver");
    }

    assert_eq!(disconnected_evidence, Vec::<&str>::new());
}

#[test]
fn manifest_exactly_matches_canonical_law_index() {
    let manifest = manifest();
    assert_eq!(manifest.schema, 2);
    assert_eq!(manifest.law_source, "docs/driver-law.md");
    assert_eq!(manifest.behavior.core, "0.20.0");
    assert_eq!(manifest.behavior.actors, "0.20.0");
    assert_eq!(manifest.behavior.macros, "0.13.0");
    assert_eq!(manifest.behavior.revision, BEHAVIOR_REVISION);
    assert_eq!(
        manifest.gate.command,
        "nix build path:.#driver-law-evidence --no-link"
    );
    assert_eq!(manifest.gate.artifact, "driver-law-evidence.json");

    let canonical = canonical_ids();
    assert_eq!(
        canonical.len(),
        8,
        "law-count changes require explicit review"
    );
    let rows: Vec<_> = manifest.laws.iter().map(|row| row.law.clone()).collect();
    assert!(
        exact_law_rows(&canonical, &rows),
        "missing, duplicate, renamed, reordered, stale, or unknown law row"
    );

    let mut evidence_ids = BTreeSet::new();
    for row in &manifest.laws {
        assert!(!row.owner.trim().is_empty(), "{} has no owner", row.law);
        validate_test_evidence(&row.law, &row.positive);
        validate_test_evidence(&row.law, &row.boundary);
        validate_inversion_evidence(&row.law, &row.inversion);
        for id in [&row.positive.id, &row.boundary.id, &row.inversion.id] {
            let unique = evidence_ids.insert(id);
            assert!(unique, "{} reuses evidence id {id}", row.law);
        }
        for revision in [
            &row.positive.revision,
            &row.boundary.revision,
            &row.inversion.revision,
        ] {
            assert_eq!(
                revision, BEHAVIOR_REVISION,
                "{} has stale evidence",
                row.law
            );
        }
    }
}

#[test]
fn manifest_gate_kills_missing_duplicate_renamed_and_unknown_laws() {
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
}

#[test]
fn manifest_gate_kills_stale_renamed_and_unknown_executable_evidence() {
    assert!(test_reference_exists(
        "driver_law::initialization_has_exact_disposition_and_trace_across_terminal_boundaries"
    ));
    assert!(!test_reference_exists(
        "driver_law::renamed_event_is_folded_exactly_once"
    ));
    assert!(!test_reference_exists(
        "unknown_suite::initialization_has_exact_disposition_and_trace_across_terminal_boundaries"
    ));
    assert!(!test_reference_exists("unqualified_test_name"));
}

#[test]
fn manifest_evidence_is_revision_bound_and_executable() {
    let path = root().join("docs/driver-law-manifest.json");
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("read Driver law manifest"))
            .expect("parse Driver law manifest");
    let mut defects = Vec::new();
    if manifest["schema"] != 2 {
        defects.push("manifest schema is not executable schema 2".to_owned());
    }
    if manifest["behavior"]["revision"] != BEHAVIOR_REVISION {
        defects.push("manifest is not bound to the selected Behavior revision".to_owned());
    }
    if manifest["gate"]["command"] != "nix build path:.#driver-law-evidence --no-link" {
        defects.push("completion gate is not the pinned executable evidence gate".to_owned());
    }
    if manifest["gate"]["artifact"] != "driver-law-evidence.json" {
        defects.push("completion gate does not emit an actual result artifact".to_owned());
    }

    let mut evidence_ids = BTreeSet::new();
    for law in manifest["laws"].as_array().into_iter().flatten() {
        let law_id = law["law"].as_str().unwrap_or("<missing law>");
        for kind in ["positive", "boundary", "inversion"] {
            let evidence = &law[kind];
            let Some(id) = evidence["id"].as_str() else {
                defects.push(format!("{law_id} has no structured {kind} evidence"));
                continue;
            };
            if !evidence_ids.insert(id.to_owned()) {
                defects.push(format!("{law_id} reuses evidence id {id}"));
            }
            if evidence["revision"] != BEHAVIOR_REVISION {
                defects.push(format!("{law_id} {kind} evidence is not revision-bound"));
            }
            if evidence["claim"].as_str().is_none_or(str::is_empty) {
                defects.push(format!("{law_id} {kind} evidence has no exact claim"));
            }
            if evidence["command"]
                .as_str()
                .is_none_or(|command| !command.starts_with("nix develop -c "))
            {
                defects.push(format!(
                    "{law_id} {kind} evidence has no pinned-Nix command"
                ));
            }
            match kind {
                "positive" | "boundary" if evidence["test"].as_str().is_none() => {
                    defects.push(format!("{law_id} {kind} evidence has no executable test"));
                }
                "inversion"
                    if ["target", "mutation", "killer"]
                        .iter()
                        .any(|field| evidence[*field].as_str().is_none_or(str::is_empty)) =>
                {
                    defects.push(format!(
                        "{law_id} inversion lacks a target, mutation, or killer"
                    ));
                }
                _ => {}
            }
        }
    }

    let source = std::fs::read_to_string(root().join("crates/bombay-engine/tests/law_manifest.rs"))
        .expect("read manifest test source");
    if source.contains("#[ignore = \"explicit completion gate; run with --ignored\"]") {
        defects.push("completion still depends on an ignored status-string test".to_owned());
    }
    assert!(
        defects.is_empty(),
        "Driver-law evidence is not executable and revision-bound:\n{}",
        defects.join("\n")
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the exact locked actor-template catalogue is one boundary contract"
)]
fn engine_does_not_mirror_actor_template_laws() {
    let path = root().join("docs/driver-template-manifest.json");
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(path).expect("read template boundary manifest"),
    )
    .expect("parse template boundary manifest");
    let expected_templates = BTreeSet::from([
        "atomic.dynamic-supervisor",
        "atomic.fifo-pool",
        "atomic.fixed-supervisor",
        "atomic.keyed-pool",
        "atomic.stable-proxy",
        "composition.machine",
        "composition.message-adapter",
        "composition.stash",
        "discovery.presence",
        "discovery.pub-sub",
        "discovery.registry",
        "discovery.resolver",
        "discovery.topic",
        "lifecycle.child-shutdown-plan",
        "lifecycle.finalize-on-shutdown",
        "lifecycle.heterogeneous-shutdown-coordinator",
        "lifecycle.propagate-termination",
        "lifecycle.shutdown-coordinator",
        "lifecycle.stop-on-shutdown",
        "lifecycle.task",
        "lifecycle.termination-monitor",
        "lifecycle.watch",
        "operations.configuration",
        "operations.health",
        "operations.readiness",
        "persistence.cache",
        "routing.acknowledgements",
        "routing.buffer",
        "routing.circuit-breaker",
        "routing.correlator",
        "routing.deduplicator",
        "routing.order-gate",
        "routing.priority-queue",
        "routing.rate-limiter",
        "routing.router",
        "routing.sequencer",
        "routing.work-queue",
        "time.deadline",
        "time.lease",
        "time.one-shot",
        "time.periodic",
        "time.receive-timeout",
        "workflow.barrier",
        "workflow.latch",
        "workflow.workflow",
    ]);
    let expected_capabilities = BTreeSet::from([
        "request.begin-activation",
        "request.cancel-observation",
        "request.customer-delivery",
        "request.diagnostic-action",
        "request.initialize-worker",
        "request.observe-child",
        "request.observe-creation",
        "request.observe-established",
        "request.observe-established-creation",
        "request.observe-peer",
        "request.report-shutdown-plan",
        "request.report-terminal-outcome",
        "request.schedule-after",
        "request.schedule-at",
        "request.shutdown-child",
        "request.shutdown-established",
        "source-action.assign-worker",
        "source-action.prepare-workers",
        "source-action.proxy-operation",
    ]);
    let mut defects = Vec::new();

    if manifest["schema"] != 3 {
        defects.push(format!("expected schema 3, found {}", manifest["schema"]));
    }
    for (field, expected) in [
        ("package", "bombay-behavior-actors"),
        ("version", "0.20.0"),
        ("revision", BEHAVIOR_REVISION),
    ] {
        if manifest["owner"][field] != expected {
            defects.push(format!("owner {field} is not {expected}"));
        }
    }
    if manifest["engine_contract"] != "universal Behavior execution only" {
        defects.push("Engine contract was broadened to actor-template policy".to_owned());
    }

    let templates = manifest["templates"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let template_ids = templates
        .iter()
        .filter_map(|entry| entry["id"].as_str())
        .collect::<BTreeSet<_>>();
    if template_ids != expected_templates || template_ids.len() != templates.len() {
        defects.push(format!(
            "template inventory mismatch: missing {:?}; extra {:?}; duplicate or malformed rows {}",
            expected_templates
                .difference(&template_ids)
                .collect::<Vec<_>>(),
            template_ids
                .difference(&expected_templates)
                .collect::<Vec<_>>(),
            templates.len().saturating_sub(template_ids.len())
        ));
    }
    for template in &templates {
        let id = template["id"].as_str().unwrap_or("<missing id>");
        for field in ["source", "event", "composition"] {
            if template[field].as_str().is_none_or(str::is_empty) {
                defects.push(format!("{id} lacks {field}"));
            }
        }
        for field in ["public", "lanes", "evidence"] {
            if template[field].as_array().is_none_or(Vec::is_empty) {
                defects.push(format!("{id} lacks {field}"));
            }
        }
    }

    let capabilities = manifest["capabilities"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let capability_ids = capabilities
        .iter()
        .filter_map(|entry| entry["id"].as_str())
        .collect::<BTreeSet<_>>();
    if capability_ids != expected_capabilities || capability_ids.len() != capabilities.len() {
        defects.push(format!(
            "capability inventory mismatch: missing {:?}; extra {:?}; duplicate or malformed rows {}",
            expected_capabilities
                .difference(&capability_ids)
                .collect::<Vec<_>>(),
            capability_ids
                .difference(&expected_capabilities)
                .collect::<Vec<_>>(),
            capabilities.len().saturating_sub(capability_ids.len())
        ));
    }
    let bombay_sources = root().join("crates/bombay/src");
    for capability in &capabilities {
        let id = capability["id"].as_str().unwrap_or("<missing id>");
        let public = capability["public"].as_str().unwrap_or_default();
        for field in ["public", "contract", "source"] {
            if capability[field].as_str().is_none_or(str::is_empty) {
                defects.push(format!("{id} lacks {field}"));
            }
        }
        if !capability["emitted_by"].is_array() || !capability["evidence"].is_array() {
            defects.push(format!("{id} lacks emitter or evidence arrays"));
        }
        let implemented =
            rust_source_contains(&bombay_sources, None, &format!("InterpretItem<{public}"));
        let recorded = capability["bombay_interpreter"].as_str();
        let expected = if implemented {
            "implemented"
        } else {
            "missing"
        };
        if recorded != Some(expected) {
            defects.push(format!(
                "{id} records Bombay interpreter {recorded:?}, but source proves {expected}"
            ));
        }
    }

    if manifest["mirrored_templates"]
        .as_array()
        .is_none_or(|rows| !rows.is_empty())
    {
        defects.push("Engine mirrors actor-template policy".to_owned());
    }
    let engine = root().join("crates/bombay-engine");
    let engine_manifest =
        std::fs::read_to_string(engine.join("Cargo.toml")).expect("read Engine Cargo manifest");
    if engine_manifest.contains("bombay-behavior-actors")
        || rust_source_contains(&engine, Some("law_manifest.rs"), "behavior_actors")
    {
        defects.push("Engine depends on or imports Behavior Actors".to_owned());
    }

    assert!(
        defects.is_empty(),
        "actor-template boundary inventory is incomplete or mirrored:\n{}",
        defects.join("\n")
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
        driver
            .matches("behavior::delegate_transition(behavior, event)")
            .count(),
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
        "behavior::delegate_transition(behavior, event).await",
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
    assert!(exports.contains(
        "pub use driver::{ActionsOf, Completion, Driver, DriverError, DriverRetirement, SettlementFailure};"
    ));
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
