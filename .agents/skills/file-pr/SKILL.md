---
name: file-pr
description: File or update a concise problem-first pull request for the current candidate, link it to its Issue, task and T3 thread, and record the reviewed head, review level and evidence. Use when a candidate is ready for PR review or an existing open PR needs updating after repair; continue with babysit-pr.
---

# File PR

**Context.** Under a Patstack lead, the PR belongs to the existing task; record its URL there. Invoked directly, open or update the PR the user asked for; that is a development delivery step, so apply Patstack's entry first if no task owns the work. Opening a PR publishes to GitHub: confirm the target repo/base is the intended one.

## Before filing

- Reuse the open PR when it is still the correct continuation; push to it instead of opening another. Never reopen a merged PR; a fix-forward or revert gets a new PR linked to the same task.
- Check current head/base, that required local checks ran on this head, and that the branch contains only this task's changes.

## Body: problem first

```markdown
## Problem
What is broken or missing, for whom. Link the Issue/spec version.
## Change
The smallest complete change and why this approach; notable things deliberately not done.
## Verification
Evidence card: what ran (EXECUTED), what was seen (OBSERVED), what is reasoning (REASONED), for which head/environment.
Feature works? Y/N. Quality standard? Y/N per applicable dimension (new features/improvements).
## Review
Level (R1/R2/R3) with one-line reason; reviewer model/session where independent.
## Risk and rollback
Blast radius, migrations, rollback route, retained human gates.
```

Keep it short; link raw artifacts instead of pasting them. No secrets, private device details or vault output.

## After filing

1. Link the PR to its Issue (avoid auto-closing a parent whose acceptance needs live verification) and, when exposed in T3, call `link_pull_request` immediately with the PR URL (each stack layer separately). A CLI-created PR is not automatically linked. Before handoff, inspect `list_thread_pull_requests` and link any missing PR from this work.
2. Record the PR URL and head on the task.
3. Continue with [babysit-pr](../babysit-pr/SKILL.md). Only the PR-owning lead watches it.

Model review reports are not GitHub human approvals. Never bypass branch protection or impersonate an approver.
