#!/usr/bin/env python3
"""Check supported compiler diagnostics without changing the caller's checkout."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hashes(root, names):
    return {name: digest(root / name) if (root / name).is_file() else None for name in names}


def source_names(root):
    return sorted(set(name for name in subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root).decode().split("\0") if name))


def snapshot_names(root):
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def diagnostic_coverage(transcript, manifest):
    cases = re.findall(
        r"^test (.+?\.rs)"
        r"(?: \[(should pass|should fail to compile)\])? \.\.\. ok$",
        transcript, re.MULTILINE)
    paths = [path for path, _ in cases]
    suites = [name for name in manifest["suites"]
              if transcript.count(f"test {name} ... ok\n") == 1]
    summaries = re.findall(
        r"test result: ok\. (\d+) passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;",
        transcript)
    modes = all(mode == "should pass" if "/pass/" in path
                else mode in ("", "should fail to compile") for path, mode in cases)
    complete = len(paths) == len(set(paths)) == len(manifest["cases"]) and \
        set(paths) == set(manifest["cases"]) and modes and \
        suites == manifest["suites"] and \
        sorted(map(int, summaries)) == sorted(manifest["targets"].values())
    return {"verified_cases": sorted(paths), "verified_suites": suites,
            "case_modes_valid": modes, "coverage_complete": complete}


def main():
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument("--cargo", default="cargo")
    options = arguments.parse_args()
    if not (os.environ.get("IN_NIX_SHELL") or os.environ.get("NIX_BUILD_TOP")):
        raise SystemExit("Diagnostics require the pinned Nix shell")
    if "TRYBUILD" in os.environ:
        raise SystemExit("Unset TRYBUILD: expected diagnostics are immutable during verification")
    repository = Path(__file__).resolve().parents[1]
    output = options.output.resolve()
    if output.is_relative_to(repository):
        raise SystemExit("Evidence output must be outside the caller's checkout")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit("Evidence output must be empty; previous receipts are preserved")
    rustc = subprocess.check_output([os.environ.get("RUSTC", "rustc"), "-vV"], text=True)
    cargo = subprocess.check_output([options.cargo, "--version"], text=True).strip()
    identity = dict(line.split(": ", 1) for line in rustc.splitlines()[1:])
    diagnostics = repository / "crates/bombay/tests/compile/diagnostics"
    manifest = json.loads((diagnostics / "compilers.json").read_text())
    selected = [name for name, compiler in manifest["compilers"].items()
                if compiler == {"release": identity["release"],
                                "commit": identity["commit-hash"], "cargo": cargo}]
    if len(selected) != 1:
        raise SystemExit("Unreviewed compiler; no diagnostic expectation is selected")
    compiler = selected[0]
    snapshots = manifest["snapshots"]
    patch = diagnostics / "minimum.patch"
    if digest(patch) != manifest["patch_sha256"]:
        raise SystemExit("The reviewed diagnostic patch changed")
    for name, expected in snapshots.items():
        path = Path(name)
        if path.parent != Path("crates/bombay/tests/compile/fail") or path.suffix != ".stderr":
            raise SystemExit("Expected diagnostics must name existing compile-fail snapshots")
        if digest(repository / path) != expected["canonical_sha256"]:
            raise SystemExit(f"Canonical expectation changed: {name}")
        if digest((repository / path).with_suffix(".rs")) != expected["fixture_sha256"]:
            raise SystemExit(f"The reviewed invalid program changed: {name}")
    names = source_names(repository)
    for name in names:
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or (repository / path).is_symlink():
            raise SystemExit(f"Source must be an owned ordinary file: {name}")
    before = hashes(repository, names)
    receipt = {"compiler": compiler, "rustc": rustc, "cargo": cargo,
               "caller_before": before, "commands": []}
    status = 1
    try:
        with tempfile.TemporaryDirectory(prefix="bombay-diagnostics-") as directory:
            workspace = Path(directory) / "source"
            for name in names:
                destination = workspace / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(repository / name, destination)
            if hashes(workspace, snapshot_names(workspace)) != before:
                raise SystemExit("Source copy differs from the caller")
            expected = dict(before)
            if compiler == "msrv":
                for operation in [["--check"], []]:
                    subprocess.run(["git", "apply", *operation, str(patch)],
                                   cwd=workspace, check=True)
                expected.update({name: value["minimum_sha256"]
                                 for name, value in snapshots.items()})
            if hashes(workspace, snapshot_names(workspace)) != expected:
                raise SystemExit("Only the reviewed expected diagnostics may change")
            receipt["snapshot_before_tests"] = expected
            env = dict(os.environ, CARGO_TARGET_DIR=str(Path(directory) / "target"),
                       CARGO_BUILD_JOBS="2", CARGO_INCREMENTAL="0",
                       CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
                       CARGO_PROFILE_RELEASE_DEBUG="0", CARGO_TERM_COLOR="never")
            argv = [options.cargo, "test", "--locked", "-p", "bombay-rs", "--features",
                    "axum", "--no-fail-fast"]
            for target in manifest["targets"]:
                argv.extend(["--test", target])
            log = output / "diagnostics.log"
            with log.open("w") as transcript:
                completed = subprocess.run(argv, cwd=workspace, env=env, stdout=transcript,
                                           stderr=subprocess.STDOUT)
            receipt["commands"].append({"argv": argv, "exit": completed.returncode,
                                        "log": log.name, "log_sha256": digest(log)})
            transcript = log.read_text()
            receipt.update(diagnostic_coverage(transcript, manifest))
            receipt["snapshot_after_tests"] = hashes(workspace, snapshot_names(workspace))
            if receipt["snapshot_after_tests"] != expected:
                raise SystemExit("Verification changed the source or expected diagnostics")
            if completed.returncode == 0 and not receipt["coverage_complete"]:
                raise SystemExit("Successful exit without the complete named diagnostic corpus")
        status = completed.returncode
    finally:
        receipt["caller_after"] = hashes(repository, source_names(repository))
        receipt["caller_unchanged"] = receipt["caller_after"] == before
        if not receipt["caller_unchanged"]:
            status = 1
        receipt["exit"] = status
        (output / "diagnostics.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if not receipt["caller_unchanged"]:
        raise SystemExit("Caller source conservation failed")
    raise SystemExit(status)


if __name__ == "__main__":
    main()
