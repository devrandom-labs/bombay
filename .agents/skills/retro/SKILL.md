---
name: retro
description: Proportionate retrospective on a finished task or a session - find real friction in navigation, checks, review, tools and information access, and promote only evidence-backed, tested technical workflow fixes with rollback, while preference changes go to Joel. Use at a task's endpoint or when asked for a retro on a session.
---

# Retro

Retro improves the agent's environment for future runs. It proposes from evidence; it does not re-litigate the task.

**Context.** At a Patstack endpoint the lead runs it before `complete`, without a separate child. Invoked directly on a session, it stays unticketed unless Joel designates development; promoting a change into shared skills is then a designated change and follows the contract below.

## Size it

- **Tiny task:** 2-4 lines in the closing comment: what cost time or went wrong and one concrete environment improvement, or "Retro: no findings". No extra agent or report.
- **Larger task (spec, children, PR, repair loop):** the steps below; ranked candidates go in the task comment and final reply.
- **After a critical recovery:** the full retro runs once restoration is verified, not on the critical path.

## 1. Read primary sources

The session transcript(s) the user names (default: current), task events, PR/review history and repair records, including any deep-diagnosis workflow RCA from [implement](../implement/SKILL.md#repair). Use existing GitHub and native session measurements when available. Report missing
measurements honestly and verify causal claims against the underlying evidence.

## 2. Find candidates

- **Navigation:** time lost finding files or facts; a pointer or index would fix it.
- **Automated checks:** a mistake a lint/type/test/filesystem check could catch. Read the repo's own check scripts and CI first: an existing check that is unwired or silently broken is the finding. A repo with no guardrail at all is itself a finding.
- **Review standards:** a mechanical violation (banned API, import shape, file location) gets a deterministic check rather than a rule; reserve written standards for real judgment calls. The reviewer, not the implementer, enforces standards.
- **Steering files:** instructions that change no behavior, belong in a check, or bloat always-loaded context.
- **Tool economy:** expensive or token-heavy tool use that a cheaper path would replace.
- **Information access:** a crucial fact was unavailable (logs, read-only service access).
- **Workflow:** process that delayed a small task, a loop that stagnated, a gate that did not catch the defect.

Rank by severity and evidence.

## 3. Promote or propose

Joel grants retro authority for a significant, evidence-backed **technical** improvement to the installed workflow/skills. Before promoting, retain:

1. the observed failure or measurable friction and its causal explanation;
2. a reproducible failing example or baseline;
3. the proposed change and proof it fixes/improves that case;
4. relevant non-regression checks ([workflow-eval](../workflow-eval/SKILL.md) for material workflow changes; a mechanical fix needs only proportionate checks);
5. a version and rollback record.

- Mechanical guardrails, corrected navigation, broken-tool repairs and demonstrated workflow defects can qualify. New naming, visual style, product tradeoffs or changed goals are **preferences**: propose them to Joel.
- Changes to shared decision rules, permissions handling, acceptance or orchestration get at least R2 ([review](../review/SKILL.md)) and take effect on the next pinned run. A mechanical repair does not need a council.
- Pin the workflow version per run. A recovery change during an active task is recorded and its affected proof rerun; it never retroactively relaxes acceptance or authority.
- Retro cannot grant itself deployment, data, service or production authority, and cannot lower acceptance to make a result pass. Model agreement alone is not evidence.

Report what was promoted (evidence, test, rollback, version), what is proposed, and what was not checked.
