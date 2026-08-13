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
    file: String,
    function: Option<Function>,
}

#[derive(Deserialize)]
struct Function {
    function_name: String,
}

#[derive(Deserialize)]
struct Candidate {
    file: String,
    function: Option<Function>,
}

#[derive(Deserialize)]
struct Baseline {
    floors: BTreeMap<String, usize>,
    known_zero_viable: Vec<String>,
}

#[derive(Default)]
struct Tally {
    total: usize,
    viable: usize,
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

fn tallies(report: &Report) -> BTreeMap<String, Tally> {
    let mut result = BTreeMap::<String, Tally>::new();
    for outcome in &report.outcomes {
        let Scenario::Mutant(mutant) = &outcome.scenario else {
            continue;
        };
        let tally = result
            .entry(key(&mutant.file, mutant.function.as_ref()))
            .or_default();
        tally.total += 1;
        match outcome.summary {
            Summary::CaughtMutant => tally.viable += 1,
            Summary::MissedMutant => {
                tally.viable += 1;
                tally.missed += 1;
            }
            Summary::Timeout => {
                tally.viable += 1;
                tally.timeout += 1;
            }
            Summary::Unviable | Summary::Success | Summary::Failure => {}
        }
    }
    result
}

fn usable(report: &Report) -> Result<(), String> {
    if report.outcomes.iter().any(|outcome| {
        matches!(outcome.scenario, Scenario::Baseline) && outcome.summary != Summary::Success
    }) {
        return Err("the unmutated baseline did not pass".into());
    }
    if !report
        .outcomes
        .iter()
        .any(|outcome| matches!(outcome.scenario, Scenario::Mutant(_)))
    {
        return Err("no mutants were tested".into());
    }
    Ok(())
}

fn emit_baseline(report: &Report) -> Result<String, String> {
    usable(report)?;
    let mut floors = BTreeMap::new();
    let mut known_zero_viable = Vec::new();
    for (key, tally) in tallies(report) {
        if tally.viable == 0 {
            known_zero_viable.push(key);
        } else {
            floors.insert(key, tally.viable);
        }
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "floors": floors,
        "known_zero_viable": known_zero_viable,
    }))
    .map_err(|error| error.to_string())
}

fn check(output: &Path, baseline_path: &Path) -> Result<(), String> {
    let report: Report = read(&output.join("outcomes.json"))?;
    usable(&report)?;
    let candidates: Vec<Candidate> = read(&output.join("mutants.json"))?;
    let baseline: Baseline = read(baseline_path)?;
    let tallies = tallies(&report);
    let mut expected = BTreeMap::<String, usize>::new();
    for candidate in &candidates {
        *expected
            .entry(key(&candidate.file, candidate.function.as_ref()))
            .or_default() += 1;
    }
    let known_zero: BTreeSet<_> = baseline.known_zero_viable.iter().collect();
    let mut failures = Vec::new();
    for (key, count) in &expected {
        let actual = tallies.get(key).map_or(0, |tally| tally.total);
        if actual != *count {
            failures.push(format!("incomplete {key}: {actual}/{count} outcomes"));
        }
    }
    for (key, floor) in &baseline.floors {
        if *floor == 0 {
            failures.push(format!("invalid zero floor: {key}"));
        } else if !expected.contains_key(key) {
            failures.push(format!("stale floor: {key}"));
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
        } else if !known_zero.contains(key) {
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
            println!("{}", emit_baseline(&report)?);
            Ok(())
        }
        [mode, output, baseline] if mode == "check" => {
            check(Path::new(output), Path::new(baseline))
        }
        _ => Err("usage: mutants-gate <emit-baseline OUT | check OUT BASELINE>".into()),
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
    use super::{check, emit_baseline};
    use std::fs;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("bombay-mutants-gate-{name}-{}", std::process::id()));
        fs::create_dir_all(&path).expect("create gate scratch directory");
        path
    }

    #[test]
    fn clean_complete_run_passes_the_ratchet() {
        let directory = scratch("clean");
        fs::write(
            directory.join("outcomes.json"),
            r#"{"outcomes":[
                {"summary":"Success","scenario":"Baseline"},
                {"summary":"CaughtMutant","scenario":{"Mutant":{"file":"a.rs","function":{"function_name":"f"}}}},
                {"summary":"Unviable","scenario":{"Mutant":{"file":"a.rs","function":{"function_name":"f"}}}}
            ]}"#,
        )
        .unwrap();
        fs::write(
            directory.join("mutants.json"),
            r#"[
                {"file":"a.rs","function":{"function_name":"f"}},
                {"file":"a.rs","function":{"function_name":"f"}}
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
                {"summary":"MissedMutant","scenario":{"Mutant":{"file":"a.rs","function":{"function_name":"f"}}}}
            ]}"#,
        )
        .unwrap();
        fs::write(
            directory.join("mutants.json"),
            r#"[{"file":"a.rs","function":{"function_name":"f"}}]"#,
        )
        .unwrap();
        let baseline = directory.join("baseline.json");
        fs::write(
            &baseline,
            r#"{"floors":{"a.rs::f":1},"known_zero_viable":[]}"#,
        )
        .unwrap();

        assert!(
            check(&directory, &baseline)
                .unwrap_err()
                .contains("survivor")
        );
    }

    #[test]
    fn seeding_rejects_a_failed_unmutated_baseline() {
        let report =
            serde_json::from_str(r#"{"outcomes":[{"summary":"Failure","scenario":"Baseline"}]}"#)
                .unwrap();
        assert!(emit_baseline(&report).unwrap_err().contains("did not pass"));
    }
}
