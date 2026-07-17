//! The pure heart of the gate: turn a parsed run + committed baseline into a
//! verdict. No IO here — `main` reads the files and calls `evaluate`.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::{Baseline, Candidate, Report, Scenario, Summary};

/// Per-`file::function` mutation tally.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) total: usize,
    pub(crate) caught: usize,
    pub(crate) unviable: usize,
    pub(crate) missed: usize,
    pub(crate) timeout: usize,
}

impl Tally {
    /// Viable = compiled and ran (caught + missed + timeout); excludes
    /// `Unviable`, which measures nothing about the tests. This is the number
    /// the ratchet floors.
    pub(crate) fn viable(&self) -> usize {
        [self.caught, self.missed, self.timeout].into_iter().sum()
    }
}

/// A reason the gate fails. One variant per failure domain.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    /// A function produced fewer outcomes than it had candidates — the run was
    /// interrupted partway through it (#148's 141/205 shape), checked per
    /// function so a duplicate elsewhere cannot backfill the aggregate.
    MissingOutcomes { key: String, expected: usize, executed: usize },
    /// A mutant survived (a test SHOULD have caught it and did not).
    Survivor { key: String },
    /// A mutant timed out — treated as a failure, per cargo-mutants + #148.
    Timeout { key: String },
    /// A floored function's viable count dropped below its recorded floor.
    Collapse { key: String, floor: usize, viable: usize },
    /// A function the sweep saw is in neither `floors` nor `known_zero_viable`.
    Unaccounted { key: String, viable: usize },
    /// The baseline floors a function the sweep never saw (renamed/deleted): the
    /// floor is stale and silently unenforced. Regenerate the baseline.
    StaleFloor { key: String, floor: usize },
    /// A baseline floor is 0 — an inert no-op floor (an authoring slip).
    InvalidFloor { key: String },
}

/// The full outcome of a gate run: the tally (always) + any failures.
#[derive(Debug)]
pub(crate) struct Verdict {
    pub(crate) tallies: BTreeMap<String, Tally>,
    pub(crate) failures: Vec<Failure>,
}

impl Verdict {
    pub(crate) const fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    pub(crate) fn total_viable(&self) -> usize {
        self.tallies.values().map(Tally::viable).sum()
    }

    pub(crate) fn total_mutants(&self) -> usize {
        self.tallies.values().map(|t| t.total).sum()
    }
}

fn key(file: &str, function: &str) -> String {
    format!("{file}::{function}")
}

/// Group MUTATION outcomes into per-function tallies. Skips the `Baseline`
/// scenario and any stray `Success` summary (only meaningful on the baseline),
/// so `total` always equals the sum of the mutation buckets. Counting is via
/// `.count()`/`.len()` — no manual arithmetic.
fn tally(report: &Report) -> BTreeMap<String, Tally> {
    let mut grouped: BTreeMap<String, Vec<Summary>> = BTreeMap::new();
    for outcome in &report.outcomes {
        let Scenario::Mutant(info) = &outcome.scenario else {
            continue;
        };
        if matches!(outcome.summary, Summary::Success) {
            continue;
        }
        grouped
            .entry(key(&info.file, &info.function.function_name))
            .or_default()
            .push(outcome.summary);
    }
    grouped
        .into_iter()
        .map(|(k, summaries)| {
            let count = |want: Summary| summaries.iter().filter(|&&s| s == want).count();
            let tally = Tally {
                total: summaries.len(),
                caught: count(Summary::CaughtMutant),
                unviable: count(Summary::Unviable),
                missed: count(Summary::MissedMutant),
                timeout: count(Summary::Timeout),
            };
            (k, tally)
        })
        .collect()
}

/// Expected outcome count per `file::function`, from the candidate list.
/// Counted via `.count()` (no manual arithmetic); the candidate list is small.
fn expected_counts(candidates: &[Candidate]) -> BTreeMap<String, usize> {
    let keys: Vec<String> =
        candidates.iter().map(|c| key(&c.file, &c.function.function_name)).collect();
    keys.iter()
        .collect::<BTreeSet<&String>>()
        .into_iter()
        .map(|k| (k.clone(), keys.iter().filter(|x| *x == k).count()))
        .collect()
}

/// Evaluate a run against the baseline. Pure: no IO, deterministic.
pub(crate) fn evaluate(
    report: &Report,
    candidates: &[Candidate],
    baseline: &Baseline,
) -> Verdict {
    let tallies = tally(report);
    let expected = expected_counts(candidates);
    let mut failures = Vec::new();

    let known_zero: BTreeSet<&str> =
        baseline.known_zero_viable.iter().map(String::as_str).collect();

    // 1. Per-function completeness — every candidate must have produced an
    //    outcome. Per key, so a duplicate outcome elsewhere cannot backfill a
    //    genuinely missing one.
    for (k, &want) in &expected {
        let got = tallies.get(k).map_or(0, |t| t.total);
        if got < want {
            failures.push(Failure::MissingOutcomes { key: k.clone(), expected: want, executed: got });
        }
    }

    // 2. Baseline hygiene — a floor for a function the sweep never saw is stale
    //    (silently unenforced); a 0 floor is inert.
    for (k, &floor) in &baseline.floors {
        if floor == 0 {
            failures.push(Failure::InvalidFloor { key: k.clone() });
        }
        if !tallies.contains_key(k) && !expected.contains_key(k) {
            failures.push(Failure::StaleFloor { key: k.clone(), floor });
        }
    }

    // 3. Per-function survivors, timeouts, ratchet, and accounting.
    for (k, t) in &tallies {
        if t.missed > 0 {
            failures.push(Failure::Survivor { key: k.clone() });
        }
        if t.timeout > 0 {
            failures.push(Failure::Timeout { key: k.clone() });
        }
        let viable = t.viable();
        match baseline.floors.get(k) {
            Some(&floor) => {
                if viable < floor {
                    failures.push(Failure::Collapse { key: k.clone(), floor, viable });
                }
            }
            None => {
                if !known_zero.contains(k.as_str()) {
                    failures.push(Failure::Unaccounted { key: k.clone(), viable });
                }
            }
        }
    }

    Verdict { tallies, failures }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FunctionInfo, MutantInfo};

    fn mutant(file: &str, func: &str, summary: Summary) -> crate::model::Outcome {
        crate::model::Outcome {
            summary,
            scenario: Scenario::Mutant(MutantInfo {
                file: file.to_owned(),
                function: FunctionInfo { function_name: func.to_owned() },
            }),
        }
    }

    fn candidate(file: &str, func: &str) -> Candidate {
        Candidate { file: file.to_owned(), function: FunctionInfo { function_name: func.to_owned() } }
    }

    fn baseline(floors: &[(&str, usize)], known_zero: &[&str]) -> Baseline {
        Baseline {
            floors: floors.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
            known_zero_viable: known_zero.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn clean_run_at_floor_passes() {
        let report = Report {
            outcomes: vec![
                mutant("a.rs", "f", Summary::CaughtMutant),
                mutant("a.rs", "f", Summary::CaughtMutant),
                mutant("a.rs", "f", Summary::Unviable),
            ],
        };
        let candidates = vec![candidate("a.rs", "f"), candidate("a.rs", "f"), candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 2)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert!(v.passed(), "failures: {:?}", v.failures);
        assert_eq!(v.total_viable(), 2);
        assert_eq!(v.total_mutants(), 3);
    }

    #[test]
    fn a_survivor_fails() {
        let report = Report { outcomes: vec![mutant("a.rs", "f", Summary::MissedMutant)] };
        let candidates = vec![candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 1)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(v.failures, vec![Failure::Survivor { key: "a.rs::f".to_owned() }]);
    }

    #[test]
    fn a_timeout_fails() {
        let report = Report { outcomes: vec![mutant("a.rs", "f", Summary::Timeout)] };
        let candidates = vec![candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 1)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(v.failures, vec![Failure::Timeout { key: "a.rs::f".to_owned() }]);
    }

    #[test]
    fn an_interrupted_run_reports_missing_outcomes() {
        // 2 candidates enumerated, only 1 outcome recorded (the #148 141/205
        // shape) — attributed to the specific function that vanished.
        let report = Report { outcomes: vec![mutant("a.rs", "f", Summary::CaughtMutant)] };
        let candidates = vec![candidate("a.rs", "f"), candidate("a.rs", "g")];
        let base = baseline(&[("a.rs::f", 1)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(
            v.failures,
            vec![Failure::MissingOutcomes { key: "a.rs::g".to_owned(), expected: 1, executed: 0 }]
        );
    }

    #[test]
    fn viability_collapse_below_floor_fails() {
        let report = Report {
            outcomes: vec![
                mutant("a.rs", "f", Summary::CaughtMutant),
                mutant("a.rs", "f", Summary::Unviable),
            ],
        };
        let candidates = vec![candidate("a.rs", "f"), candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 2)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(
            v.failures,
            vec![Failure::Collapse { key: "a.rs::f".to_owned(), floor: 2, viable: 1 }]
        );
    }

    #[test]
    fn a_new_unaccounted_function_fails() {
        let report = Report { outcomes: vec![mutant("new.rs", "g", Summary::CaughtMutant)] };
        let candidates = vec![candidate("new.rs", "g")];
        let base = baseline(&[], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(
            v.failures,
            vec![Failure::Unaccounted { key: "new.rs::g".to_owned(), viable: 1 }]
        );
    }

    #[test]
    fn a_documented_zero_viable_function_passes() {
        let report = Report {
            outcomes: vec![
                mutant("actor_ref.rs", "with_sender", Summary::Unviable),
                mutant("actor_ref.rs", "with_sender", Summary::Unviable),
            ],
        };
        let candidates = vec![candidate("actor_ref.rs", "with_sender"), candidate("actor_ref.rs", "with_sender")];
        let base = baseline(&[], &["actor_ref.rs::with_sender"]);
        let v = evaluate(&report, &candidates, &base);
        assert!(v.passed(), "failures: {:?}", v.failures);
        assert_eq!(v.total_viable(), 0);
    }

    #[test]
    fn a_stale_floor_fails() {
        // gone.rs::x is floored but appears in neither candidates nor outcomes.
        let report = Report { outcomes: vec![mutant("a.rs", "f", Summary::CaughtMutant)] };
        let candidates = vec![candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 1), ("gone.rs::x", 2)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(
            v.failures,
            vec![Failure::StaleFloor { key: "gone.rs::x".to_owned(), floor: 2 }]
        );
    }

    #[test]
    fn a_zero_floor_is_invalid() {
        let report = Report { outcomes: vec![mutant("a.rs", "f", Summary::CaughtMutant)] };
        let candidates = vec![candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 0)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(v.failures, vec![Failure::InvalidFloor { key: "a.rs::f".to_owned() }]);
    }

    #[test]
    fn multiple_failures_accumulate() {
        // One survivor and one timeout, different functions — both surface.
        let report = Report {
            outcomes: vec![
                mutant("a.rs", "f", Summary::MissedMutant),
                mutant("b.rs", "g", Summary::Timeout),
            ],
        };
        let candidates = vec![candidate("a.rs", "f"), candidate("b.rs", "g")];
        let base = baseline(&[("a.rs::f", 1), ("b.rs::g", 1)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert_eq!(v.failures.len(), 2);
        assert!(v.failures.contains(&Failure::Survivor { key: "a.rs::f".to_owned() }));
        assert!(v.failures.contains(&Failure::Timeout { key: "b.rs::g".to_owned() }));
    }

    #[test]
    fn success_summary_under_a_mutant_scenario_is_ignored() {
        // A stray Success under a Mutant scenario must not inflate `total`.
        let report = Report {
            outcomes: vec![
                mutant("a.rs", "f", Summary::Success),
                mutant("a.rs", "f", Summary::CaughtMutant),
            ],
        };
        let candidates = vec![candidate("a.rs", "f")];
        let base = baseline(&[("a.rs::f", 1)], &[]);
        let v = evaluate(&report, &candidates, &base);
        assert!(v.passed(), "failures: {:?}", v.failures);
        assert_eq!(v.total_mutants(), 1);
        assert_eq!(v.total_viable(), 1);
    }
}
