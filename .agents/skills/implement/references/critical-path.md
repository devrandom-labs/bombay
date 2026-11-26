# Critical path

Load for urgent work: an outage, blocked users or a broken release. Urgency is classified separately from size/risk and from Full Send/Full Stop; a critical Full Stop task keeps its human checkpoints for sensitive decisions.

## Steps

1. **Triage and choose a recovery route immediately.** Use the existing task/incident and a short working contract: impact, affected environment, expected behavior, reproduction, authorized target, success check. Prefer the smallest safe fix, or an existing authorized mitigation/rollback when that restores the requested behavior sooner. Do not wait for a long spec, new design exploration or a task graph for a bounded fix.
2. **Keep the active team small.** For a bounded fix that needs R2, the lead works with one independent reviewer. Use the [review procedure](../../review/SKILL.md#r2) to select the reviewer. Investigate separable facts, run reproduction/regression checks and assess blast radius in parallel when practical. Mandatory security and target-environment checks still apply. Review scope, not role names, justifies extra expertise.
3. **Prove the critical behavior.** Reproduce, make the smallest complete fix, run focused acceptance, counterexample and affected-regression checks. Use real browser/endpoint proof when the failure requires it. Reuse still-valid evidence; redo only what the new head invalidates. Acceptance and code verdicts stay explicit even when one reviewer reports both.
4. **Deliver within the existing grant.** Full Send proceeds through the project's test delivery and live check; Full Stop keeps its checkpoints. An actual production incident uses its explicitly authorized recovery target. Urgency itself grants no production access.
5. **Retro after recovery.** Causal investigation needed for the fix stays on the critical path; the longer workflow retro, preventive improvements and unrelated polish follow verified restoration. If restoration is part of a larger feature, the parent stays open until the rest of its acceptance is met.

Review depth follows the same selection rule: a bounded urgent fix can use R2; a broad high-impact incident can require R3 with reviewers in parallel. Do not run both R2 and a Council on an unchanged candidate when one correctly scoped R3 covers the requirement.

## Time and iteration control

- The lead sets and announces a timebox/next-result target at intake from severity, the known reproduction, project constraints and tools. Joel need not estimate debugging work.
- Initial response targets: a triage/recovery-route decision within 5 minutes; a first tested fix or safe mitigation candidate within 15 minutes where the problem permits. These are upper response targets, not promises that every incident resolves in that time, and not waiting periods; a known tiny fix proceeds immediately.
- Keep elapsed wall time, time to first safe mitigation/candidate and time to verified delivery visible. Separate active work from queue/CI/environment waits and human-decision waits, using existing task/run/PR timestamps rather than manual reporting.
- Repair limits and deep diagnosis follow [implement's repair loop](../SKILL.md#repair) under the same elapsed clock.

The fast path ends in verified recovery/delivery evidence, not "works on my machine".
