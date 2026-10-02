#!/usr/bin/env python3
"""Enforce line floors for runtime owners with independent executable laws."""

import json
import sys
from pathlib import Path


OWNER_FLOORS = {
    "crates/bombay-engine/src/driver.rs": 90,
    "crates/bombay/src/actor_execution.rs": 93,
    "crates/bombay/src/actor_outcome.rs": 90,
    "crates/bombay/src/observe/mod.rs": 90,
}


def verify_owner_coverage(report_path: Path) -> int:
    coverage_report = json.loads(report_path.read_text())
    coverage_files = [
        coverage_file
        for coverage_run in coverage_report["data"]
        for coverage_file in coverage_run["files"]
    ]
    floor_failures = []
    for owner, minimum_percent in OWNER_FLOORS.items():
        owner_reports = [
            coverage_file
            for coverage_file in coverage_files
            if coverage_file["filename"].replace("\\", "/").endswith("/" + owner)
        ]
        if len(owner_reports) != 1:
            floor_failures.append(
                f"{owner}: expected one owner report, found {len(owner_reports)}"
            )
            continue
        line_totals = owner_reports[0]["summary"]["lines"]
        covered, count = line_totals["covered"], line_totals["count"]
        if count <= 0 or covered > count:
            floor_failures.append(f"{owner}: invalid line accounting {covered}/{count}")
            continue
        print(f"{owner}: {covered}/{count} lines, floor {minimum_percent}%")
        if 100 * covered < minimum_percent * count:
            floor_failures.append(f"{owner}: line coverage below {minimum_percent}%")
    for failure in floor_failures:
        print(failure, file=sys.stderr)
    return int(bool(floor_failures))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: verify-owner-coverage.py COVERAGE_JSON")
    raise SystemExit(verify_owner_coverage(Path(sys.argv[1])))
