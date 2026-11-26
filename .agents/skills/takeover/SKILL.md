---
name: takeover
description: Take over existing work from another session, lead or person - read its spec, PR, handoff and later corrections, verify current ownership and state, preserve settled decisions and original credit, and rejoin at the earliest valid phase. Use for continue this task, pick up this PR or branch, or resume someone else's work.
---

# Takeover

Takeover is an entry route into the same workflow, not a restart. A handoff describes context; it does not establish correctness.

**Context.** Resume the existing task and ticket number through [coordination](../patstack/references/coordination.md); a named ticket is never a new root. Takeover grants no permission to overwrite active work, force-push, merge, close a PR or deploy.

## 1. Check ownership first

- Read the Issue/brief, actual session/PR state and later steering. Check whether someone is still working on the same files, coordinate the handoff and preserve their work. No registry owner, lease or recover operation is required.
- Inspect the worktree for uncommitted or unpushed work before writing. Preserve it.

## 2. Rebuild context from sources

Read, in order: the spec/Issue and its version, later user corrections, PR or branch and current head/base, review findings and their responses, child runs, deployed release if any, and the handoff/checkpoint ([task state](../patstack/references/task-state.md)). A stale checkpoint is a starting reference, not live proof.

## 3. Keep what is settled

- Reuse the established goal, accepted spec, mode, approvals already given and settled decisions. Clarify only real gaps or later changes.
- Keep the cumulative repair count and elapsed time; a new lead does not reset them.
- Preserve original authorship: keep commits, add co-author credit where you build on their work, and link the original PR/branch/thread.

## 4. Verify, then rejoin

Independently verify the behavior and evidence the remaining work depends on. Then continue at the earliest incomplete valid step: plan ([to-spec](../to-spec/SKILL.md)), build ([implement](../implement/SKILL.md)), [review](../review/SKILL.md), [verify](../verify/SKILL.md) or the PR lifecycle ([babysit-pr](../babysit-pr/SKILL.md)). Do not rebuild the spec or repeat valid completed work because the lead changed. Re-establish any native PR watch under the new owner.

If the work turns out obsolete or its base has moved materially, switch to [modernize](../modernize/SKILL.md).
