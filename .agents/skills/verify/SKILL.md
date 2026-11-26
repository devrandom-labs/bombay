---
name: verify
description: Prove a candidate or release actually works - evidence labelled EXECUTED, OBSERVED or REASONED and bound to the exact head or release, feature Y/N plus applicable UI/UX/performance/data quality verdicts, before/after benchmarks on known workloads, and separate local, PR, deployed and authenticated-UI claims. Use before saying something works or cannot be reproduced, before readiness or delivery, or when asked to test an app.
---

# Verify

See the real app or endpoint before claiming it works or cannot be reproduced. Proof effort follows the actual change: a button colour gets a focused check, a new feature gets its full quality proof.

**Context.** Under a Patstack lead, verify against the task's acceptance and spec version and record evidence on the task. Invoked directly to test something, stay within the user's request: no ticket, no new environment or data permission.

## Evidence labels

- **EXECUTED**: assertions ran and their result is recorded.
- **OBSERVED**: actual behavior seen in the real app/endpoint.
- **REASONED**: source or model assessment, including any review.

Keep separate claims for local, PR/CI, deployed and real authenticated UI. Each claim names the head, build or release it covers. Unreadable content makes a result partial. Required but unverified is **not verified**: never an invented Yes and never a claimed failure.

## 1. Bind the target

Record repo/worktree, head/base, build and environment. For deployed proof, read the release identity before and after the run; a mixed-release observation invalidates it. Re-read the live PR head/base and required checks before any readiness claim; evidence for a superseded head is stale.

## 2. Choose the checks

| Change | Checks |
|---|---|
| Tiny UI edit | Requested appearance; affected contrast/focus/hover/disabled, responsive and interaction states; required repo checks. No benchmark or quality matrix. |
| Bug fix | The reported case (red before, green after), relevant counterexamples, affected regressions. |
| New feature / improvement | **Feature works? Y/N** and **Meets the project quality standard? Y/N**, each applicable dimension (UI, UX, performance, data, security) with target, evidence, result and limitation. N/A needs a concrete reason. |

Targets come from the spec. Missing targets are derived before acceptance from project evidence, comparable healthy flows and primary standards; read [quality](../patstack/references/quality.md) for the order of evidence, example targets and the API scorecard. Do not retune a target after seeing the result.

Use existing project tests and verification recipes. For a genuinely missing or stale recipe use [create-test-recipes](../create-test-recipes/SKILL.md). For login or a test persona follow [access](../patstack/references/access.md): matching origin/environment, approved least-privilege persona, secret-safe autofill/broker path; never print, persist or screenshot credentials, tokens, cookies or signed URLs. Host choice for browser/native proof follows [project context](../patstack/references/project-context.md).

Inside T3, use the [native browser/device routes](../patstack/references/coordination.md#t3-code-tools): `preview_status` → `preview_open` when needed → snapshots and focused interactions; mobile uses `device_list` → `device_open` and the returned launcher/context. Follow actual tool availability, tab ownership and documented fallback rules; a closed preview alone is not a blocker.

## 3. Measure, when performance or quality is claimed

1. Freeze metric, workload, dataset, tolerance/noise rule and pass condition before implementation.
2. Capture BEFORE on that protocol (a new feature uses absolute targets; never invent a speedup over a nonexistent feature).
3. Capture AFTER under the same request mix, dataset, concurrency, identity, build/environment and cache state. Repeat or alternate runs enough for the claim; a single run is an estimate.
4. Validate the measurement: the work ran inside the measured region, failures are counted, the limiting resource is known, the component result connects to the end-to-end result.
5. Report actual before → after values, sample counts and limits. UI/UX ratings are labelled agent judgments, not human-usability evidence.

## 4. Shared test targets

Before changing a shared test target, check the active work and actual release, coordinate who deploys/tests which candidate, and prefer an isolated preview when concurrent work would invalidate proof. Check release identity before and after testing; foreign or stale-release evidence is inconclusive. This requires no registry reservation, lease or eligible agent. Keep the coordination period short, preserve other sessions' work and use only task-owned fixtures. Real environment/data approval still applies; never silently evict another deployment. Keep the target/build and evidence on the existing task reference.

Tiny edits and bounded critical fixes skip integration-candidate ceremony but still check current head, required CI and the target release.

## 5. Report

Show a compact evidence card: functional Y/N, each applicable dimension's target → result, evidence links (raw artifacts, run IDs, SHAs, environment), labels and limitations. A failure goes to [implement's repair](../implement/SKILL.md#repair) with its layer (PR or live), failed criterion and exact candidate/release. A stale or foreign release is inconclusive, not a code failure. Production is never touched without explicit authority; say whether it was.
