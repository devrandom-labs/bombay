#!/usr/bin/env python3
"""Reproduce exact child-owned preparation retirement in a disposable workspace."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

if not (os.environ.get("IN_NIX_SHELL") or os.environ.get("NIX_BUILD_TOP")):
    raise SystemExit("Child preparation evidence requires the pinned Nix shell")

arguments = argparse.ArgumentParser()
arguments.add_argument("--output", type=Path, required=True)
output_directory = arguments.parse_args().output.resolve()
repository = Path(__file__).resolve().parents[3]
if output_directory.is_relative_to(repository):
    raise SystemExit("Evidence output must be outside the caller's checkout")
output_directory.mkdir(parents=True, exist_ok=True)
if any(output_directory.iterdir()):
    raise SystemExit("Evidence output must be empty; previous receipts are preserved")

with tempfile.TemporaryDirectory(prefix="bombay-child-preparation-") as directory:
    workspace = Path(directory) / "source"
    shutil.copytree(repository, workspace, copy_function=shutil.copy,
                    ignore=shutil.ignore_patterns(".git", ".direnv", ".serena", "target",
                                                 "mutants.out", "mutants.out.old"))
    target = Path(os.environ.get("CARGO_TARGET_DIR", str(Path(directory) / "target"))).resolve()
    if target.is_relative_to(repository):
        raise SystemExit("Build artifacts must be outside the caller's checkout")
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2",
               CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0",
               CARGO_PROFILE_TEST_DEBUG="0", CARGO_PROFILE_RELEASE_DEBUG="0")
    launch = workspace / "crates/bombay/src/launch.rs"
    test = workspace / "crates/bombay/tests/fixed_supervisor_recovery.rs"
    lock = workspace / "Cargo.lock"
    if any(not path.resolve().is_relative_to(workspace) for path in [launch, test, lock]):
        raise SystemExit("All mutated and conserved source paths must belong to the snapshot")
    original, source, locked = launch.read_bytes(), test.read_bytes(), lock.read_bytes()
    cut = b"    if let Some(residual) = residual {\n        residual.receive_activation_tasks().await;\n    }\n"
    if original.count(cut) != 1:
        raise SystemExit("Expected exactly one existing actor-owned task settlement")
    mutant = original.replace(cut, b"", 1)
    original_hashes = {
        str(path.relative_to(workspace)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in [launch, test, lock, workspace / "Cargo.toml",
                     workspace / "crates/bombay/Cargo.toml"]
    }
    (output_directory / "original-source.json").write_text(json.dumps(original_hashes, indent=2) + "\n")
    records = []
    receipt = {"original_sources": original_hashes, "records": records}
    killer = "joined_child_retirement_waits_for_owned_preparation_while_parent_and_sibling_progress"
    try:
        for phase, runtime, expected in [("healthy", original, 0), ("inverse", mutant, 101),
                                         ("restored", original, 0)]:
            launch.write_bytes(runtime)
            for profile in ["debug", "release"]:
                argv = ["cargo", "test", "--manifest-path", str(workspace / "Cargo.toml"),
                        "-p", "bombay-rs", "--test", "fixed_supervisor_recovery", "--locked"]
                if profile == "release":
                    argv += ["--release"]
                argv += [killer, "--", "--exact", "--nocapture"]
                log = output_directory / f"{phase}-{profile}.log"
                with log.open("w") as transcript:
                    completed = subprocess.run(argv, cwd=workspace, env=env, stdout=transcript,
                                               stderr=subprocess.STDOUT)
                text = log.read_text()
                named = f"test {killer} ... {'FAILED' if phase == 'inverse' else 'ok'}\n"
                oracle = any(message in text for message in [
                    "joined child report precedes no unfinished owned preparation",
                    "joined child report stays absent across parent and sibling progress"])
                semantic = completed.returncode == 101 and "0 passed; 1 failed;" in text and oracle
                semantic = semantic and not any(message in text for message in
                                               ["resumed after completion", "called after complete"])
                verified = semantic if phase == "inverse" else completed.returncode == 0 and "1 passed; 0 failed;" in text
                binary = re.search(r"Running tests/fixed_supervisor_recovery\.rs \(([^\n]+)\)", text)
                binary_path = Path(binary.group(1)) if binary else None
                if binary_path is not None and not binary_path.is_absolute():
                    binary_path = workspace / binary_path
                binary_hash = hashlib.sha256(binary_path.read_bytes()).hexdigest() \
                    if binary_path is not None and binary_path.is_file() else None
                records.append({
                    "phase": phase, "profile": profile, "argv": argv,
                    "cwd": str(workspace), "exit": completed.returncode,
                    "verified": verified and named in text and binary_hash is not None,
                    "post_native_oracle": oracle,
                    "projection_guard": "terminal projection follows every original capability task join" in text,
                    "log": log.name, "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
                    "binary": str(binary_path) if binary_path is not None else None,
                    "binary_sha256": binary_hash,
                    "runtime_sha256": hashlib.sha256(launch.read_bytes()).hexdigest(),
                    "test_sha256": hashlib.sha256(test.read_bytes()).hexdigest(),
                    "lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest(),
                })
                receipt = {"environment": {key: env[key] for key in ["CARGO_TARGET_DIR",
                           "CARGO_BUILD_JOBS", "CARGO_INCREMENTAL", "CARGO_PROFILE_DEV_DEBUG",
                           "CARGO_PROFILE_TEST_DEBUG", "CARGO_PROFILE_RELEASE_DEBUG"]},
                           "original_sources": original_hashes, "records": records}
                (output_directory / "child-preparation-retirement.json").write_text(json.dumps(receipt, indent=2) + "\n")
                print(phase, profile, completed.returncode, records[-1]["verified"], flush=True)
                if test.read_bytes() != source or lock.read_bytes() != locked:
                    raise SystemExit("The original test or selected lock changed in the snapshot")
                if completed.returncode != expected or not records[-1]["verified"]:
                    print(text, flush=True)
                    raise SystemExit("Unexpected result; no child-retirement law credit")
    finally:
        launch.write_bytes(original)
        receipt["snapshot_restored"] = launch.read_bytes() == original
        receipt["caller_sources_unchanged"] = all(hashlib.sha256((repository / path).read_bytes()).hexdigest()
                                                 == digest for path, digest in original_hashes.items())
        (output_directory / "child-preparation-retirement.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if launch.read_bytes() != original or not receipt["caller_sources_unchanged"]:
        raise SystemExit("Snapshot restoration or caller source conservation failed")
    print("Snapshot runtime byte-exact restored; caller checkout unchanged", flush=True)
