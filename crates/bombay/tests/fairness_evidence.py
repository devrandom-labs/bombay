"""Replay selector defects against the native fairness regressions inside Nix."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import shutil
import tempfile


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--profile", choices=("debug", "release"), default="debug")
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--suite", choices=("all", "public"), default="all")
parser.add_argument("--baseline-revision", help="Original revision to replay against the public witness.")
arguments = parser.parse_args()
if "IN_NIX_SHELL" not in os.environ:
    raise SystemExit("Run through nix develop -c python3.")
root = Path(__file__).resolve().parents[3]
# Mutate only an isolated copy, never the caller's live sources.
workspace = tempfile.TemporaryDirectory(prefix="bombay-fairness-")
checkout = Path(workspace.name)
tracked = subprocess.check_output(
    ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
).decode().split("\0")
for relative in set(tracked) - {""}:
    source = root / relative
    if source.is_file():
        destination = checkout / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
environment = checkout / "crates/bombay/src/local/environment.rs"
capabilities = checkout / "crates/bombay/src/local/effects/mod.rs"
observation = checkout / "crates/bombay/src/local/effects/observation.rs"
cargo_environment = os.environ.copy()
cargo_environment.setdefault("CARGO_TARGET_DIR", str(root / "target"))
originals = {path: path.read_bytes() for path in (environment, capabilities, observation)}
arguments.output.mkdir(parents=True, exist_ok=True)
command = ["cargo", "test", "--locked", "-p", "bombay-rs"]
if arguments.profile == "release":
    command.append("--release")
records = []


def execute(name, test, expectation, target=None):
    selection = ["--lib"] if target is None else ["--test", target]
    invocation = command + selection + [test]
    completed = subprocess.run(invocation, cwd=checkout, env=cargo_environment, capture_output=True, text=True, timeout=600)
    transcript = completed.stdout + completed.stderr
    (arguments.output / f"{name}.log").write_text(transcript)
    if expectation == "pass":
        accepted = completed.returncode == 0 and re.search(r"test result: ok\. [1-9][0-9]* passed;", transcript) is not None
    else:
        accepted = (completed.returncode == 101 and "test result: FAILED." in transcript
                    and f"{test} ... FAILED" in transcript)
    records.append({"name": name, "command": invocation, "expected": expectation,
                    "exit": completed.returncode, "accepted": accepted})
    print(f"{name}: {'accepted' if accepted else 'FAILED'}", flush=True)
    if not accepted:
        raise RuntimeError(f"Unexpected result; inspect {arguments.output / (name + '.log')}")


mutations = [
    ("child_stop_overtakes_report", observation, "pub(super) async fn next(&mut self)",
     "TerminationSource::Child(_) => TerminationEvent::Child(event)", "TerminationSource::Child(_) => TerminationEvent::Peer(event)",
     "local::effects::fairness::fairness_child_stop_follows_admitted_reports"),
    ("closed_child_fact_lost", observation, "pub(super) fn into_local",
     "Err(ControlClosed(event)) => Some(event)", "Err(ControlClosed(_event)) => None",
     "local::effects::fairness::fairness_closed_child_destination_retains_the_original_fact"),
    ("settlement_resets_service", environment, "async fn next_source(&mut self)",
     "self.next_settlement_source = match source", "self.next_event_source = EventSource::Local; self.next_settlement_source = match source",
     "local::environment::tests::fairness_settlement_cannot_reset_ordinary_service"),
    ("user_starvation", environment, "pub(crate) fn prepare(",
     "config.with_aging_cap(1)", "config",
     "local::environment::tests::fairness_standard_mailbox_bounds_user_wait_under_control_flood"),
    ("mailbox_priority", environment, "async fn next(&mut self)",
     "self.next_event_source.order()", "EventSource::Mailbox.order()",
     "local::environment::tests::fairness_all_ready_and_single_source_floods_have_bounded_service"),
    ("observation_priority", capabilities, "async fn next_local_event(&mut self)",
     "match self.next_local_source", "match LocalSource::Observation",
     "local::effects::fairness::fairness_ready_observations_and_startup_results_alternate_without_loss"),
    ("source_priority", environment, "async fn next_source(&mut self)",
     "self.next_settlement_source.order()", "SettlementSource::Control.order()",
     "local::environment::tests::fairness_source_settlement_rotates_without_admitting_users_or_timers"),
    ("mailbox_budget", environment, "async fn next(&mut self)",
     "coop::cooperative(self.inbox.recv())", "self.inbox.recv()",
     "local::environment::tests::fairness_mailbox_flood_yields_to_the_executor"),
    ("observation_budget", capabilities, "async fn next_local_event(&mut self)",
     "coop::cooperative(self.observations.next())", "self.observations.next()",
     "local::effects::fairness::fairness_observation_flood_yields_to_the_executor"),
    ("source_budget", environment, "async fn next_source(&mut self)",
     "coop::cooperative(self.inbox.recv_source())", "self.inbox.recv_source()",
     "local::environment::tests::fairness_source_settlement_rotates_without_admitting_users_or_timers"),
]
try:
    if arguments.suite == "all":
        execute("baseline", "fairness_", "pass")
        for name, path, anchor, before, after, test in mutations:
            source = originals[path].decode()
            start = source.index(anchor)
            position = source.index(before, start)
            changed = source[:position] + after + source[position + len(before):]
            path.write_text(changed)
            try:
                execute(name, test, "fail")
            finally:
                path.write_bytes(originals[path])
        execute("restored", "fairness_", "pass")
    public_test = "actual_actor_bounds_both_sources_and_returns_every_original_payload"
    execute("public_baseline", public_test, "pass", "event_fairness")
    for priority in ("Mailbox", "Timer"):
        source = originals[environment].decode()
        start = source.index("async fn next(&mut self)")
        position = source.index("self.next_event_source.order()", start)
        environment.write_text(source[:position] + f"EventSource::{priority}.order()" + source[position + len("self.next_event_source.order()"):])
        try:
            execute(f"public_{priority.lower()}_priority", public_test, "fail", "event_fairness")
        finally:
            environment.write_bytes(originals[environment])
    if arguments.baseline_revision:
        for path in originals:
            relative = str(path.relative_to(checkout))
            path.write_bytes(subprocess.check_output(["git", "show", f"{arguments.baseline_revision}:{relative}"], cwd=root))
        try:
            execute("public_original_revision", public_test, "fail", "event_fairness")
        finally:
            for path, original in originals.items():
                path.write_bytes(original)
    execute("public_restored", public_test, "pass", "event_fairness")
finally:
    for path, original in originals.items():
        path.write_bytes(original)
    receipt = {
        "profile": arguments.profile,
        "baseline_revision": arguments.baseline_revision,
        "sources": {str(path.relative_to(checkout)): hashlib.sha256(original).hexdigest()
                    for path, original in originals.items()},
        "records": records,
    }
    (arguments.output / "results.json").write_text(json.dumps(receipt, indent=2) + "\n")
    workspace.cleanup()
