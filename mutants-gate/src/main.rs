//! CI tool: turns a cargo-mutants run into a pass/fail verdict that cannot be
//! vacuously green. See docs/adr/0006-mutation-viable-ratchet.md.
//!
//! Usage:
//!   mutants-gate check <mutants.out-dir> <baseline.json>
//!   mutants-gate emit-baseline <mutants.out-dir>

#![allow(
    clippy::redundant_pub_crate,
    reason = "multi-module binary crate: pub(crate) documents the crate-internal API surface across sibling modules; unreachable_pub is deferred workspace-wide (root Cargo.toml), so pub(crate) is the intent-revealing choice over bare pub"
)]
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "mutants-gate is a CLI tool: stdout carries the ratio report, stderr carries failure reasons"
)]

mod gate;
mod model;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use model::{Baseline, Candidate, Report};

#[derive(Debug, thiserror::Error)]
enum GateError {
    #[error("usage: mutants-gate <check <out-dir> <baseline.json> | emit-baseline <out-dir>>")]
    Usage,
    #[error("reading {path}: {source}")]
    Io { path: PathBuf, #[source] source: std::io::Error },
    #[error("parsing {path}: {source}")]
    Parse { path: PathBuf, #[source] source: serde_json::Error },
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, GateError> {
    let raw = std::fs::read_to_string(path).map_err(|source| GateError::Io { path: path.to_owned(), source })?;
    serde_json::from_str(&raw).map_err(|source| GateError::Parse { path: path.to_owned(), source })
}

fn run() -> Result<bool, GateError> {
    let mut args = std::env::args().skip(1);
    let Some(mode) = args.next() else { return Err(GateError::Usage) };
    match mode.as_str() {
        "emit-baseline" => {
            let Some(dir_arg) = args.next() else { return Err(GateError::Usage) };
            let out_dir = Path::new(&dir_arg);
            let report: Report = read_json(&out_dir.join("outcomes.json"))?;
            println!("{}", gate::emit_baseline(&report));
            Ok(true)
        }
        "check" => {
            let (Some(dir_arg), Some(baseline_path)) = (args.next(), args.next()) else {
                return Err(GateError::Usage);
            };
            let out_dir = Path::new(&dir_arg);
            let report: Report = read_json(&out_dir.join("outcomes.json"))?;
            let candidates: Vec<Candidate> = read_json(&out_dir.join("mutants.json"))?;
            let baseline: Baseline = read_json(Path::new(&baseline_path))?;
            let verdict = gate::evaluate(&report, &candidates, &baseline);
            print!("{}", gate::render_report(&verdict));
            for failure in &verdict.failures {
                eprintln!("mutants-gate FAIL: {failure:?}");
            }
            Ok(verdict.passed())
        }
        _ => Err(GateError::Usage),
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(err) => {
            eprintln!("mutants-gate: {err}");
            ExitCode::from(2)
        }
    }
}
