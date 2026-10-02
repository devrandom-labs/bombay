//! Verdict tool for the on-demand mutation-testing derivation.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;
use std::process::ExitCode;

use serde::Deserialize;

#[derive(Deserialize)]
struct Report {
    outcomes: Vec<Outcome>,
}

#[derive(Deserialize)]
struct Outcome {
    summary: Summary,
    scenario: Scenario,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum Summary {
    Success,
    CaughtMutant,
    MissedMutant,
    Unviable,
    Timeout,
    Failure,
}

#[derive(Deserialize)]
enum Scenario {
    Baseline,
    Mutant(Mutant),
}

#[derive(Deserialize)]
struct Mutant {
    name: String,
    file: String,
    function: Option<Function>,
}

#[derive(Deserialize)]
struct Function {
    function_name: String,
}

#[derive(Deserialize)]
struct Candidate {
    name: String,
    file: String,
    function: Option<Function>,
}

#[derive(Deserialize)]
struct Baseline {
    floors: BTreeMap<String, usize>,
    known_zero_viable: Vec<String>,
    #[serde(default)]
    equivalent_mutants: Vec<String>,
}

#[derive(Default)]
struct Tally {
    total: usize,
    viable: usize,
    unviable: usize,
    missed: usize,
    timeout: usize,
}

fn key(file: &str, function: Option<&Function>) -> String {
    function.map_or_else(
        || format!("{file}::<module>"),
        |value| format!("{file}::{}", value.function_name),
    )
}

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let text = fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
}

fn tallies(
    report: &Report,
    candidates: &[Candidate],
    equivalent_mutants: &[String],
) -> Result<BTreeMap<String, Tally>, String> {
    let mut baselines = report
        .outcomes
        .iter()
        .filter(|outcome| matches!(outcome.scenario, Scenario::Baseline));
    let Some(baseline) = baselines.next() else {
        return Err("the unmutated baseline is missing".into());
    };
    if baselines.next().is_some() {
        return Err("the unmutated baseline appears more than once".into());
    }
    if baseline.summary != Summary::Success {
        return Err("the unmutated baseline did not pass".into());
    }
    if candidates.is_empty() {
        return Err("no mutants were selected".into());
    }

    let mut expected = BTreeMap::new();
    for candidate in candidates {
        if expected
            .insert(
                candidate.name.as_str(),
                key(&candidate.file, candidate.function.as_ref()),
            )
            .is_some()
        {
            return Err(format!("duplicate candidate: {}", candidate.name));
        }
    }
    let reviewed: BTreeSet<_> = equivalent_mutants.iter().map(String::as_str).collect();
    if reviewed.len() != equivalent_mutants.len() {
        return Err("duplicate reviewed equivalent mutant".into());
    }
    for name in &reviewed {
        if !expected.contains_key(name) {
            return Err(format!("stale reviewed equivalent mutant: {name}"));
        }
    }
    let mut result = BTreeMap::<String, Tally>::new();
    let mut observed = BTreeSet::new();
    for outcome in &report.outcomes {
        let Scenario::Mutant(mutant) = &outcome.scenario else {
            continue;
        };
        let name = mutant.name.as_str();
        let Some(expected_key) = expected.get(name) else {
            return Err(format!("unexpected mutant outcome: {name}"));
        };
        let mutant_key = key(&mutant.file, mutant.function.as_ref());
        if mutant_key != *expected_key {
            return Err(format!("candidate identity changed: {name}"));
        }
        if !observed.insert(name) {
            return Err(format!("duplicate mutant outcome: {name}"));
        }
        if reviewed.contains(name) && outcome.summary != Summary::MissedMutant {
            return Err(format!("reviewed equivalent no longer missed: {name}"));
        }
        let tally = result.entry(mutant_key).or_default();
        tally.total += 1;
        match outcome.summary {
            Summary::CaughtMutant => tally.viable += 1,
            Summary::MissedMutant => {
                tally.viable += 1;
                if !reviewed.contains(name) {
                    tally.missed += 1;
                }
            }
            Summary::Timeout => {
                tally.viable += 1;
                tally.timeout += 1;
            }
            Summary::Unviable => tally.unviable += 1,
            Summary::Success | Summary::Failure => {
                return Err(format!(
                    "invalid mutant outcome {name}: {:?}",
                    outcome.summary
                ));
            }
        }
    }
    for name in expected.keys() {
        if !observed.contains(name) {
            return Err(format!("incomplete candidate results: missing {name}"));
        }
    }
    Ok(result)
}

fn emit_baseline(
    report: &Report,
    candidates: &[Candidate],
    equivalent_mutants: &[String],
) -> Result<String, String> {
    let mut floors = BTreeMap::new();
    let mut known_zero_viable = Vec::new();
    for (key, tally) in tallies(report, candidates, equivalent_mutants)? {
        if tally.missed > 0 || tally.timeout > 0 {
            return Err(format!(
                "cannot seed baseline from survivor or timeout: {key}"
            ));
        }
        if tally.viable == 0 {
            if tally.unviable != tally.total {
                return Err(format!("non-unviable zero-viability outcome: {key}"));
            }
            known_zero_viable.push(key);
        } else {
            floors.insert(key, tally.viable);
        }
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "floors": floors,
        "known_zero_viable": known_zero_viable,
        "equivalent_mutants": equivalent_mutants,
    }))
    .map_err(|error| error.to_string())
}

fn check(output: &Path, baseline_path: &Path) -> Result<(), String> {
    let report: Report = read(&output.join("outcomes.json"))?;
    let candidates: Vec<Candidate> = read(&output.join("mutants.json"))?;
    let baseline: Baseline = read(baseline_path)?;
    let tallies = tallies(&report, &candidates, &baseline.equivalent_mutants)?;
    let expected: BTreeSet<_> = candidates
        .iter()
        .map(|candidate| key(&candidate.file, candidate.function.as_ref()))
        .collect();
    let known_zero: BTreeSet<_> = baseline.known_zero_viable.iter().collect();
    let mut failures = Vec::new();
    if known_zero.len() != baseline.known_zero_viable.len() {
        failures.push("duplicate known-zero-viable entry".to_owned());
    }
    for (key, floor) in &baseline.floors {
        if *floor == 0 {
            failures.push(format!("invalid zero floor: {key}"));
        } else if !expected.contains(key) {
            failures.push(format!("stale floor: {key}"));
        }
    }
    for key in &known_zero {
        if !expected.contains(*key) {
            failures.push(format!("stale known-zero-viable entry: {key}"));
        }
        if baseline.floors.contains_key(*key) {
            failures.push(format!("conflicting baseline entries: {key}"));
        }
    }
    for (key, tally) in &tallies {
        if tally.missed > 0 {
            failures.push(format!("{key}: {} survivor(s)", tally.missed));
        }
        if tally.timeout > 0 {
            failures.push(format!("{key}: {} timeout(s)", tally.timeout));
        }
        if let Some(floor) = baseline.floors.get(key) {
            if tally.viable < *floor {
                failures.push(format!(
                    "{key}: viability collapsed to {} below {floor}",
                    tally.viable
                ));
            }
        } else if known_zero.contains(key) {
            if tally.viable > 0 || tally.unviable != tally.total {
                failures.push(format!(
                    "{key}: known-zero-viable entry gained viable mutants"
                ));
            }
        } else {
            failures.push(format!("unaccounted: {key}"));
        }
    }
    let viable: usize = tallies.values().map(|tally| tally.viable).sum();
    let total: usize = tallies.values().map(|tally| tally.total).sum();
    println!("mutation coverage: {viable} viable / {total} total");
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn run() -> Result<(), String> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    match args.as_slice() {
        [mode, output] if mode == "emit-baseline" => {
            let report = read(&Path::new(output).join("outcomes.json"))?;
            let candidates: Vec<Candidate> = read(&Path::new(output).join("mutants.json"))?;
            println!("{}", emit_baseline(&report, &candidates, &[])?);
            Ok(())
        }
        [mode, output, reviewed_baseline] if mode == "emit-baseline" => {
            let report = read(&Path::new(output).join("outcomes.json"))?;
            let candidates: Vec<Candidate> = read(&Path::new(output).join("mutants.json"))?;
            let reviewed: Baseline = read(Path::new(reviewed_baseline))?;
            println!(
                "{}",
                emit_baseline(&report, &candidates, &reviewed.equivalent_mutants)?
            );
            Ok(())
        }
        [mode, output, baseline] if mode == "check" => {
            check(Path::new(output), Path::new(baseline))
        }
        _ => Err(
            "usage: mutants-gate <emit-baseline OUT [REVIEWED_BASELINE] | check OUT BASELINE>"
                .into(),
        ),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mutants-gate FAIL: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Candidate, check, emit_baseline};
    use std::fs;
    use std::path::{Path, PathBuf};

    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("bombay-mutants-gate-{name}-{}", std::process::id()));
        fs::create_dir_all(&path).expect("create gate scratch directory");
        path
    }

    fn write_report(directory: &Path, baselines: &[&str], mutants: &[&str]) {
        let outcomes: Vec<_> = baselines
            .iter()
            .map(|summary| serde_json::json!({"summary": summary, "scenario": "Baseline"}))
            .chain(mutants.iter().map(|summary| {
                serde_json::json!({
                    "summary": summary,
                    "scenario": {"Mutant": {"name": "a.rs:f:replacement", "file": "a.rs", "function": {"function_name": "f"}}}
                })
            }))
            .collect();
        fs::write(
            directory.join("outcomes.json"),
            serde_json::json!({"outcomes": outcomes}).to_string(),
        )
        .expect("write verdict report");
    }

    fn write_candidate_and_baseline(directory: &Path, floor: usize, known_zero: &[&str]) {
        fs::write(
            directory.join("mutants.json"),
            r#"[{"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}}]"#,
        )
        .expect("write candidate inventory");
        let floors = if floor == 0 {
            serde_json::json!({})
        } else {
            serde_json::json!({"a.rs::f": floor})
        };
        fs::write(
            directory.join("baseline.json"),
            serde_json::json!({
                "floors": floors,
                "known_zero_viable": known_zero
            })
            .to_string(),
        )
        .expect("write baseline expectations");
    }

    #[test]
    fn clean_complete_run_passes_the_ratchet() {
        let directory = scratch("clean");
        fs::write(
            directory.join("outcomes.json"),
            r#"{"outcomes":[
                {"summary":"Success","scenario":"Baseline"},
                {"summary":"CaughtMutant","scenario":{"Mutant":{"name":"a.rs:f:first","file":"a.rs","function":{"function_name":"f"}}}},
                {"summary":"Unviable","scenario":{"Mutant":{"name":"a.rs:f:second","file":"a.rs","function":{"function_name":"f"}}}}
            ]}"#,
        )
        .unwrap();
        fs::write(
            directory.join("mutants.json"),
            r#"[
                {"name":"a.rs:f:first","file":"a.rs","function":{"function_name":"f"}},
                {"name":"a.rs:f:second","file":"a.rs","function":{"function_name":"f"}}
            ]"#,
        )
        .unwrap();
        let baseline = directory.join("baseline.json");
        fs::write(
            &baseline,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[]}"#,
        )
        .unwrap();

        check(&directory, &baseline).expect("complete clean run passes");
    }

    #[test]
    fn survivor_fails_even_when_viability_meets_the_floor() {
        let directory = scratch("survivor");
        fs::write(
            directory.join("outcomes.json"),
            r#"{"outcomes":[
                {"summary":"Success","scenario":"Baseline"},
                {"summary":"MissedMutant","scenario":{"Mutant":{"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}}}}
            ]}"#,
        )
        .unwrap();
        fs::write(
            directory.join("mutants.json"),
            r#"[{"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}}]"#,
        )
        .unwrap();
        let baseline = directory.join("baseline.json");
        fs::write(
            &baseline,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[]}"#,
        )
        .unwrap();

        let rejection = check(&directory, &baseline).unwrap_err();
        assert!(rejection.contains("survivor"));
    }

    #[test]
    fn reviewed_equivalence_applies_only_to_its_exact_surviving_candidate() {
        let directory = scratch("reviewed-equivalence");
        write_report(&directory, &["Success"], &["MissedMutant"]);
        write_candidate_and_baseline(&directory, 1, &[]);
        let baseline_path = directory.join("baseline.json");
        fs::write(
            &baseline_path,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[],"equivalent_mutants":["a.rs:f:replacement"]}"#,
        )
        .expect("write exact reviewed equivalence");
        check(&directory, &baseline_path).expect("the exact reviewed equivalent is accepted");

        fs::write(
            &baseline_path,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[],"equivalent_mutants":["a.rs:f:other"]}"#,
        )
        .expect("write stale reviewed equivalence");
        let stale = check(&directory, &baseline_path).unwrap_err();
        assert!(stale.contains("stale"));

        fs::write(
            &baseline_path,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[],"equivalent_mutants":["a.rs:f:replacement"]}"#,
        )
        .expect("restore exact reviewed equivalence");
        write_report(&directory, &["Success"], &["CaughtMutant"]);
        let no_longer_equivalent = check(&directory, &baseline_path).unwrap_err();
        assert!(no_longer_equivalent.contains("no longer missed"));

        write_report(&directory, &["Success"], &["Timeout"]);
        let timed_out = check(&directory, &baseline_path).unwrap_err();
        assert!(timed_out.contains("no longer missed"));

        write_report(&directory, &["Success"], &["MissedMutant"]);
        let report = super::read(&directory.join("outcomes.json")).unwrap();
        let candidates: Vec<Candidate> = super::read(&directory.join("mutants.json")).unwrap();
        let reviewed = vec!["a.rs:f:replacement".to_owned()];
        let emitted = emit_baseline(&report, &candidates, &reviewed)
            .expect("a complete report can retain the reviewed equivalence");
        assert!(emitted.contains("a.rs:f:replacement"));
        let unreviewed = emit_baseline(&report, &candidates, &[]).unwrap_err();
        assert!(unreviewed.contains("survivor"));

        let duplicate = emit_baseline(
            &report,
            &candidates,
            &[reviewed[0].clone(), reviewed[0].clone()],
        )
        .unwrap_err();
        assert!(duplicate.contains("duplicate"));

        fs::write(
            directory.join("mutants.json"),
            r#"[
                {"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}},
                {"name":"a.rs:f:other","file":"a.rs","function":{"function_name":"f"}}
            ]"#,
        )
        .expect("write the second candidate");
        fs::write(
            directory.join("outcomes.json"),
            r#"{"outcomes":[
                {"summary":"Success","scenario":"Baseline"},
                {"summary":"MissedMutant","scenario":{"Mutant":{"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}}}},
                {"summary":"MissedMutant","scenario":{"Mutant":{"name":"a.rs:f:other","file":"a.rs","function":{"function_name":"f"}}}}
            ]}"#,
        )
        .expect("write two exact candidate verdicts");
        let different_survivor = check(&directory, &baseline_path).unwrap_err();
        assert!(different_survivor.contains("survivor"));
    }

    #[test]
    fn seeding_rejects_a_failed_unmutated_baseline() {
        let report =
            serde_json::from_str(r#"{"outcomes":[{"summary":"Failure","scenario":"Baseline"}]}"#)
                .unwrap();
        let rejection = emit_baseline(&report, &[], &[]).unwrap_err();
        assert!(rejection.contains("did not pass"));
    }

    #[test]
    fn baseline_cardinality_and_every_mutant_summary_fail_closed() {
        let directory = scratch("summary-matrix");
        let baseline_path = directory.join("baseline.json");
        write_candidate_and_baseline(&directory, 0, &["a.rs::f"]);

        for baselines in [
            vec![],
            vec!["Success", "Success"],
            vec!["CaughtMutant"],
            vec!["MissedMutant"],
            vec!["Unviable"],
            vec!["Timeout"],
            vec!["Failure"],
        ] {
            write_report(&directory, &baselines, &["Unviable"]);
            let verdict = check(&directory, &baseline_path);
            assert!(verdict.is_err(), "{baselines:?}");
        }

        for summary in [
            "Success",
            "CaughtMutant",
            "MissedMutant",
            "Timeout",
            "Failure",
        ] {
            write_report(&directory, &["Success"], &[summary]);
            let verdict = check(&directory, &baseline_path);
            assert!(verdict.is_err(), "{summary}");
            let report = super::read(&directory.join("outcomes.json")).unwrap();
            let candidates: Vec<Candidate> = super::read(&directory.join("mutants.json")).unwrap();
            let seeded = emit_baseline(&report, &candidates, &[]);
            if summary == "CaughtMutant" {
                assert!(seeded.is_ok());
            } else {
                assert!(seeded.is_err(), "{summary}");
            }
        }
        write_report(&directory, &["Success"], &["Unviable"]);
        check(&directory, &baseline_path).expect("only unviable may retain zero-viable status");
        let report = super::read(&directory.join("outcomes.json")).unwrap();
        let candidates: Vec<Candidate> = super::read(&directory.join("mutants.json")).unwrap();
        let seeded = emit_baseline(&report, &candidates, &[]);
        assert!(seeded.is_ok());

        write_report(&directory, &["Success"], &["UnrecognizedStatus"]);
        let verdict = check(&directory, &baseline_path);
        assert!(verdict.is_err());
    }

    #[test]
    fn incomplete_results_and_stale_baseline_entries_fail_closed() {
        let directory = scratch("candidate-inventory");
        let baseline_path = directory.join("baseline.json");
        write_candidate_and_baseline(&directory, 0, &["a.rs::f"]);
        fs::write(
            directory.join("mutants.json"),
            r#"[
                {"name":"a.rs:f:replacement","file":"a.rs","function":{"function_name":"f"}},
                {"name":"a.rs:f:missing","file":"a.rs","function":{"function_name":"f"}}
            ]"#,
        )
        .unwrap();
        write_report(&directory, &["Success"], &["Unviable"]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("incomplete"));
        let report = super::read(&directory.join("outcomes.json")).unwrap();
        let candidates: Vec<Candidate> = super::read(&directory.join("mutants.json")).unwrap();
        let rejection = emit_baseline(&report, &candidates, &[]).unwrap_err();
        assert!(rejection.contains("incomplete"));

        write_report(&directory, &["Success"], &["Unviable", "Unviable"]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("duplicate"));

        write_report(&directory, &["Success"], &["Unviable"]);
        write_candidate_and_baseline(&directory, 0, &["stale.rs::f"]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("stale"));

        write_candidate_and_baseline(&directory, 0, &["a.rs::f", "a.rs::f"]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("duplicate"));

        write_candidate_and_baseline(&directory, 1, &[]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("viability collapsed"));

        write_candidate_and_baseline(&directory, 1, &["a.rs::f"]);
        let rejection = check(&directory, &baseline_path).unwrap_err();
        assert!(rejection.contains("conflicting"));
    }
}
