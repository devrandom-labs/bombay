//! serde types mirroring cargo-mutants 27.0.0 `outcomes.json` / `mutants.json`
//! and this repo's `mutants-baseline.json`. Only the fields the gate needs are
//! modelled; unknown fields are ignored (no `deny_unknown_fields`).

use std::collections::BTreeMap;

use serde::Deserialize;

/// The top of `mutants.out/outcomes.json`.
#[derive(Debug, Deserialize)]
pub(crate) struct Report {
    pub(crate) outcomes: Vec<Outcome>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Outcome {
    pub(crate) summary: Summary,
    pub(crate) scenario: Scenario,
}

/// Externally-tagged: `"CaughtMutant"` etc. deserialize as unit variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum Summary {
    Success,
    CaughtMutant,
    MissedMutant,
    Unviable,
    Timeout,
}

/// Either the bare string `"Baseline"` or `{ "Mutant": { .. } }`.
#[derive(Debug, Deserialize)]
pub(crate) enum Scenario {
    Baseline,
    Mutant(MutantInfo),
}

#[derive(Debug, Deserialize)]
pub(crate) struct MutantInfo {
    pub(crate) file: String,
    pub(crate) function: FunctionInfo,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FunctionInfo {
    pub(crate) function_name: String,
}

/// One element of the top-level `mutants.json` array (candidate list).
#[derive(Debug, Deserialize)]
pub(crate) struct Candidate {
    pub(crate) file: String,
    pub(crate) function: FunctionInfo,
}

/// `mutants-baseline.json`: the committed ratchet.
#[derive(Debug, Deserialize)]
pub(crate) struct Baseline {
    /// `"file::function_name"` -> minimum viable mutant count (>= 1).
    pub(crate) floors: BTreeMap<String, usize>,
    /// `"file::function_name"` documented as structurally 0-viable.
    pub(crate) known_zero_viable: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_mutant_outcome() {
        let json = r#"{
            "summary": "CaughtMutant",
            "scenario": { "Mutant": {
                "file": "bombay-core/src/actor/kind.rs",
                "function": { "function_name": "handle_message" }
            } }
        }"#;
        let outcome: Outcome = serde_json::from_str(json).unwrap();
        assert_eq!(outcome.summary, Summary::CaughtMutant);
        match outcome.scenario {
            Scenario::Mutant(m) => {
                assert_eq!(m.file, "bombay-core/src/actor/kind.rs");
                assert_eq!(m.function.function_name, "handle_message");
            }
            Scenario::Baseline => panic!("expected a Mutant scenario"),
        }
    }

    #[test]
    fn parses_the_baseline_scenario_string() {
        let json = r#"{ "summary": "Success", "scenario": "Baseline" }"#;
        let outcome: Outcome = serde_json::from_str(json).unwrap();
        assert_eq!(outcome.summary, Summary::Success);
        assert!(matches!(outcome.scenario, Scenario::Baseline));
    }

    #[test]
    fn parses_a_candidate() {
        let json = r#"[{
            "file": "bombay-core/src/mailbox.rs",
            "function": { "function_name": "recv" }
        }]"#;
        let candidates: Vec<Candidate> = serde_json::from_str(json).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].function.function_name, "recv");
    }
}
