---
name: modernize
description: Assess an old PR, long-lived branch or abandoned work for current relevance before reviving it - does the problem still exist and does the change fit today's base, APIs and architecture - then continue it, identify a needed rework decision, or recommend closure with evidence. Use for stale PRs and branches, not for active work owned by someone else.
---

# Modernize

Relevance first. A conflict-free rebase is not proof the change is still useful.

**Context.** Under a Patstack lead, this is the entry route for old work into the same task. Invoked directly, assess and report; reviving the change is development work and follows Patstack's entry. Modernize grants no permission to force-push, close a PR or change the product goal.

## 1. Is the problem still real?

Reproduce the original problem on the current base or deployed release, or show from current code that it remains. Check whether another change already fixed or superseded it (search merged PRs, the Issue, the code path).

## 2. Does the change still fit?

Compare against the current base: APIs, architecture, conventions, dependencies, tests and the project's current procedures. Note what the PR touches that has since moved.

## 3. Decide

| Finding | Action |
|---|---|
| Still useful, fits with small updates | Bring the continuation up to date under the project's Git permissions (rebase/merge as the repo prefers; no force-push without authority), rerun affected checks, then rejoin [review](../review/SKILL.md) and [babysit-pr](../babysit-pr/SKILL.md) at the task's level and mode. |
| Goal still useful, needs material rework | Identify the changed work and the decision it needs. Do not quietly expand scope; record it on the task, and ask Joel only if the goal or product behavior changes. |
| Superseded or obsolete | Present the evidence and recommend closure. Actual closure or a changed product goal needs the relevant user authority. |

Preserve the original author's credit when continuing. The revived work inherits the task's review level, evidence requirements, repair limits and delivery mode.
