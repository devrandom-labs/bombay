---
name: implement
description: Build or repair the candidate for a designated development task - proportionate route for tiny edits, bounded fixes, critical incidents and new features, parallel slices under one lead, and the single repair loop for failures found at PR or live layers. Use once the brief is sufficient to build, or when a review, CI or live check failed and the candidate needs a fix.
---

# Implement

Produce the smallest complete candidate and its relevant tests, then hand it to [review](../review/SKILL.md) and [verify](../verify/SKILL.md). Apply [ponytail](../ponytail/SKILL.md) at the session's level.

**Context.** Under a Patstack lead, reuse its ticket, brief/spec version, mode and current candidate. Invoked directly for a concrete change with no owner, apply Patstack's entry (ownership, mode) first. Agree file/worktree responsibilities in the brief ([coordination](../patstack/references/coordination.md)); preserve others' edits. No registry claim or eligible actor is required.

## Choose the route from the actual change

| Change | Route | Verification effort |
|---|---|---|
| Tiny cosmetic/layout/copy edit | Direct in the current context. No spec, task graph, subtickets or Council. Seconds to minutes when the environment is ready. | Focused visual check and affected responsive/interaction/accessibility states; existing required checks. |
| Bounded behavior fix | Tight reproduction (red first), smallest complete fix at the root cause, focused tests. | Reported case, relevant counterexamples and affected regressions. |
| New feature / substantial improvement | Build from the [spec](../to-spec/SKILL.md); quality work is part of the slice. Subtickets/agents only where useful. | Feature Y/N plus applicable UI, UX, performance and data standards; improvements also prove their claimed gain. |

Critical urgency overlays these routes; read [critical path](references/critical-path.md) for triage, timeboxes and iteration control. It never turns a tiny edit into a larger process.

Classify risk by behavior and blast radius, not the file or product name. A cosmetic edit on an auth page is still cosmetic; a change to auth/permission behavior, an important API contract or substantial customer-facing behavior is not. If tooling, startup or CI delay dominates a small task, surface that cost for retro instead of treating it as inevitable.

## Build

1. Read the code the change touches and list every place it must reach (callers, tests, fixtures, config, exports).
2. Write or update the test at the agreed seam first where that is practical; prefer the highest existing seam.
3. Make the change. Run typecheck/lint and single affected test files as you go; run the affected suite once at the end.
4. Commit to the task branch/worktree. Record head, what ran (EXECUTED) and what was only read (REASONED).
5. Hand to [review](../review/SKILL.md) with the review level chosen there, then [verify](../verify/SKILL.md).

## Parallel slices

Split only independently verifiable work that benefits from parallel execution ([to-tickets](../to-tickets/SKILL.md)). Only the lead starts children; one child layer. Each child gets its own branch/worktree and the brief from [coordination](../patstack/references/coordination.md#delegate-and-communicate): task, accessible spec version, owned scope, actual provider/model/runtime/reasoning, allowed actions, dependencies, acceptance, proof and stop conditions. Children return results and objections; they start no agents or top-level chats.

Work in parallel until a shared resource is actually needed. For larger work, the lead freezes an integration candidate (head/base, workflow/spec versions, evidence, PRs) and one integration owner merges slices, rerunning the checks the merge invalidates. Clean up child worktrees the task owns.

## Repair

Every failure, at any layer, comes back here. The lead owns it until the agreed endpoint is verified. Record: layer (PR or live), failed criterion, exact candidate/release, evidence, owner, next action.

| Failure point | Return route | Exit condition |
|---|---|---|
| Local/PR acceptance, review, CI or preview | Diagnose → smallest repair → incremental review at the selected level → affected acceptance/quality checks → update the same open PR → [babysit-pr](../babysit-pr/SKILL.md) | Required checks and evidence cover the current candidate; no unresolved blocker. |
| Live test on the expected deployed release | Stop completion → capture release/failure → authorized rollback or fix-forward → repair PR → review/CI/preview → test deployment/sync → live retest | Intended result and applicable quality targets pass on the verified delivered release. |
| Live target changed or belongs to another task | Mark proof stale/inconclusive → reconcile target owner and current release → schedule a valid retest | Proof is attributable to the intended candidate. Never patch code based only on someone else's deployment. |

- A rollback restores an approved known-good state when permitted; it does not prove the feature delivered. A merged/closed PR stays history: fix-forward or revert uses a new linked PR on the same task. Never reopen a merged PR as a repair mechanism. Reverify downstream syncs after a release changes. Rollback/fix-forward needs existing environment/data permission; never assume a migration is reversible or overwrite another task's release.
- PR and live layers share one cumulative record of attempts and elapsed time. Switching layer, PR, model, release or chat does not reset it. Stale/foreign-release proof consumes time but is not a code-repair attempt.
- **Three failed normal cycles is a maximum, not a quota.** A repeated finding without new evidence, an exhausted timebox or a refuted approach triggers deep diagnosis earlier. Never run an identical pass to reach three.
- **Deep diagnosis**, same clock and history: rebuild the reproduction, inspect actual runtime/test behavior, challenge both the implementation hypothesis and the AI workflow, change the approach from new evidence, fix the root cause and verify it. Then record the technical root cause and the workflow/detection failure that let it persist (input to [retro](../retro/SKILL.md)). It is not abandonment, a new task or an unlimited extension.
- Reassess the approach as a unit rather than stacking speculative patches. Block only on agreed acceptance, applicable standards and material safety/regression risk; record non-blocking improvements as follow-ups.
- With no viable progress, surface the specific blocker and the best verified recovery option promptly; ask Joel only for the actual missing decision or authority. Missing required proof keeps the task open; a genuine external blocker ends in an explicit handoff, never a completion claim.
