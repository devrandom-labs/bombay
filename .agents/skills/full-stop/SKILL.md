---
name: full-stop
description: Full Stop mode for a designated development task - Joel stays involved at meaningful product or approach decisions, and the work ends at an evidenced, ready, unmerged PR handed over with Done/Verified/Unresolved/Next. Use when Joel invokes Full Stop or Patstack classifies a high-impact task Full Stop at intake.
---

# Full Stop

Full Stop is hands-on for decisions that matter, not slower for everything else. Size, urgency and review depth are chosen separately; a critical task can use the fast path and still keep Full Stop checkpoints.

**Context.** Runs under a Patstack lead with an owned task. Invoked directly for development with no owner yet, apply Patstack's entry (ownership, project binding) first and continue here.

## What Joel decides

Meaningful product and approach choices: user-visible behavior or copy, product rules (who may do what), significant visual direction, data/permission tradeoffs, irreversible or broad-impact design choices. Use [grilling](../grilling/SKILL.md) for these.

The lead still resolves observable facts and ordinary implementation details autonomously: tooling, file layout, test persona already established by the project, equivalent technical options, parallelism.

## Human checkpoint form

One message per decision point, batching everything currently open: the decision, evidence, recommendation, consequences of each option and the work that continues meanwhile. Never a vague "please confirm". Continue independent work while waiting; record the pending decision on the task.

## Endpoint

1. Build, review and verify as the route requires ([implement](../implement/SKILL.md), [review](../review/SKILL.md), [verify](../verify/SKILL.md)).
2. [File the PR](../file-pr/SKILL.md) and [babysit](../babysit-pr/SKILL.md) it until required checks and the selected review cover the current head.
3. Leave the PR ready and unmerged. Do not merge, deploy or claim delivery; Full Stop supplies no test-delivery grant.
4. Hand off Done / Verified / Unresolved / Next with PR link, reviewed head, review level and evidence labels.

A deployment-bound task keeps its real waiting lifecycle after handoff; never fake delivery to complete it. Retro runs proportionately once the Full Stop endpoint is reached.
