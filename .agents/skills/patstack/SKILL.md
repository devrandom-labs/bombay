---
name: patstack
description: Use for any request to change code, configuration, data or another development deliverable (fix, add, deploy, migrate, release, sync), when Full Send or Full Stop is invoked, or to continue an existing implementation, PR or delivery task. Not for questions, discussion, research or non-development writing such as emails. Coordinates the lead, mode and which Patstack modules run; not needed for unticketed discussion, research or a standalone module request.
---

# Patstack

Workflow version: **1.2.0**. BIG TASK is the lead agent, not a separate role or skill. One lead owns the requested outcome through verified handoff, using the smallest complete path. Patstack chooses mode, route and modules; each module owns its procedure. Modules return results to the lead and never invoke Patstack again.

A concrete request to change code, configuration or an agreed deliverable designates a task; the literal word TASK is not required. Discussion, research and casual testing stay outside development intake until such a request.

## Enter or resume

Read the [Labcast project context](../../project.md). Bind the actual workspace, project procedures and existing task. For a designated task, load [coordination](references/coordination.md) to reuse or allocate its reference number and share its brief. Session starts create no development ticket; numbering imposes no eligibility, ownership or execution gate. Preserve its accepted spec and scope. Joel's latest explicit steering supersedes earlier constraints within retained authority gates. For project procedures, spec distribution or host choices, read [project context](references/project-context.md). On long work or resume, read [task state](references/task-state.md); reconcile actual PR/build/release state before continuing. Work owned by someone else or stale work enters through [takeover](../takeover/SKILL.md) or [modernize](../modernize/SKILL.md).

## Choose the mode

Mode controls Joel's involvement and the endpoint. Size, urgency and review depth (R1/R2/R3) are separate choices; R3 does not by itself mean Full Stop.

- **[Full Send](../full-send/SKILL.md)** is the default: the lead owns routine decisions and continues through PR review, required CI/tests, the established authorized test deployment and real release proof.
- **[Full Stop](../full-stop/SKILL.md)**: Joel decides meaningful product/approach choices; the endpoint is a ready unmerged PR.
- At intake, a very large/important task with meaningful sensitive auth/permissions, an important shared API or broad customer-facing impact can be classified Full Stop; explain the actual impact. A cosmetic edit in such a repo does not qualify by filename alone.
- Joel's explicit Full Send for the task stays in effect within its scope and retained gates. Once selected, ordinary uncertainty or two viable technical options never switch the mode. A verified new high-impact dependency during execution raises review depth and gets one human checkpoint for that decision; unaffected work continues.

Research, check prior tasks and test assumptions where useful, then make and record the best supported in-scope choice. Significant new visual direction is presented as browser-viewable options for Joel's approval; an explicit requested colour/layout change is already directed.

## Route the work

| Task shape | Modules, in order | Notes |
|---|---|---|
| Tiny edit (copy, colour, small layout) | [ponytail](../ponytail/SKILL.md) → [implement](../implement/SKILL.md) direct → [review](../review/SKILL.md) R1 → [verify](../verify/SKILL.md) focused → file-pr/babysit-pr where the repo delivers through PRs → endpoint | No spec, subtickets, Council or benchmark matrix. Required repo checks still run. |
| Bounded bug fix | implement (reproduce first) → review R1/R2 by risk → verify → [file-pr](../file-pr/SKILL.md) → [babysit-pr](../babysit-pr/SKILL.md) → endpoint | Trust-boundary or data changes take at least R2. |
| Critical incident | implement critical path → review R2 (R3 if broad) in parallel → verify → file-pr → babysit-pr → endpoint → retro | Triage/route within ~5 min; no long spec first. |
| New feature / substantial change | [to-spec](../to-spec/SKILL.md) → [to-tickets](../to-tickets/SKILL.md) only for real parallel slices → implement → review (R3 via [council](../council/SKILL.md)) → verify incl. quality → file-pr → babysit-pr → endpoint | Quality proof is part of the work, not later polish. |
| Someone else's work | takeover → earliest valid phase above | Reuse valid spec and credit. |
| Old PR / stale branch | modernize → continue, rework decision or closure recommendation | Relevance before rebasing. |
| Failure at PR or live layer | implement Repair → review incremental → verify → babysit-pr | Same history and clock. |
| Choice only Joel can make | [grilling](../grilling/SKILL.md) | Facts are never his question. |
| Missing/stale test recipe | [create-test-recipes](../create-test-recipes/SKILL.md) | Not for working coverage. |
| Shipped artifact/runtime analysis | [reverse-engineer-anything](../reverse-engineer-anything/SKILL.md) | Not for ordinary source reading. |
| Endpoint reached | [retro](../retro/SKILL.md), proportionate | [workflow-eval](../workflow-eval/SKILL.md) only when a workflow change needs it. |

Load a module when its step starts; do not preload every module. Take the earliest valid row when shapes overlap, and re-route if risk, scope or evidence changes.

When running in T3 Code, use its available native tools for the operation; read the [T3 tool routing](references/coordination.md#t3-code-tools) when choosing transport. This is the existing workflow using its host, not another skill or permission grant.

## Common references

Load only when the step needs them: [coordination](references/coordination.md) (reference numbers, native delegation, delivery authority), [task state](references/task-state.md) (long runs/resume), [project context](references/project-context.md) (project packs, hosts), [access](references/access.md) (BW Locker/login), [quality](references/quality.md) (targets and evidence), [sources](references/sources.md) (provenance and module map).

## Invariants the lead keeps

- One lead and one child layer. Children receive the brief in [coordination](references/coordination.md), preserve others' edits and start no agents or top-level chats. Small work stays direct.
- Evidence is labelled EXECUTED, OBSERVED or REASONED and bound to the exact head/build/release. Model agreement is not execution proof. Missing required proof is not completion.
- Determine applicable delivery surfaces from the task and project configuration; missing access is a blocker, not absence. If no remote, PR, CI or deployment applies, the committed and re-verified head is the endpoint: report it as done. Otherwise complete every applicable delivery requirement, and report genuinely absent surfaces as not applicable.
- Author repair is complete when every finding is fixed or answered with evidence on the committed, re-tested head. When requested, mark the PR ready for review yourself and request required re-review. This author action grants no approval, merge or deployment authority; final readiness still requires current checks and the selected review.
- Full Send grants the established test target only. Production, destructive data/Git actions and explicitly retained human gates need their actual authority.
- Failures at PR or live layers use one repair flow with cumulative attempts and time; three failed normal cycles or earlier stagnation trigger deeper diagnosis and workflow RCA, never automatic abandonment or a new task.
- Native wakeups handle waits; checkpoints preserve work rather than ending it. Complete only after the actual endpoint, evidence, proportionate retro and a final comment. Keep the outcome and evidence on the existing GitHub task and PR.
- Modules invoked without a Patstack task run inside the user's request only: they create no ticket and grant no permission. Standalone retro, research, review and evaluation stay unticketed unless Joel designates development.
