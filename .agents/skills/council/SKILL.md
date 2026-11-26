---
name: council
description: R3 council. Two fresh sessions judge independently, check each other's findings against the evidence, then report a joint verdict and scores for each round. Use for broad or high-impact changes, irreversible effects, unresolved material disagreement, or when asked for a council on a candidate, plan or technical decision.
---

# Council

Use two reviewers. For Labcast, use the models and commands in [Models](../../project.md#models). The default elsewhere is one reviewer from the Opus family and one from the Sol family. Each reviewer can use several sessions with a narrow scope. Use executed checks to prove behavior.

**Context.** Under a Patstack lead this is the R3 level of [review](../review/SKILL.md) for the task's exact candidate. Invoked directly on a plan or technical decision, run it within the user's request; it creates no ticket. Choices only Joel can make go to [grilling](../grilling/SKILL.md), not to a council vote.

## Setup

- The lead starts two fresh sessions that wrote none of the change. Use the project commands where defined and the native route in [coordination](../patstack/references/coordination.md#delegate-and-communicate) elsewhere. If a required model is unavailable, follow the [review procedure](../review/SKILL.md#independence-rules).
- Same brief to both: task/spec version, relevant context, exact candidate (head/diff or document version), evidence access, and the questions that decide readiness. No author summary, no other model's view.
- Add narrowly targeted expertise (for example a security pass on a trust boundary) only when the change warrants it. Use one of the selected model families.

## Round 1: independent judgments

Each session returns, without seeing the other:

- Acceptance verdict and code/approach verdict, kept separate.
- A score per verdict with a one-line basis (a score communicates judgment; it does not decide readiness).
- Findings: claim, severity, evidence/reproduction, suggested fix.

## Round 2: cross-examination

Give each session the other's findings. For every disputed or blocking claim, record:

| Claim | Severity | Evidence / reproduction | Response | Resolution |
|---|---|---|---|---|

Resolve with evidence: run the reproduction, read the code path, execute the check. A claim that neither side can support with evidence is labelled REASONED and cannot by itself block or clear readiness.

## Joint assessment

Report per-model scores by round, resolved findings, unresolved objections with owner and next action, and the two verdicts. Required acceptance checks and unresolved blocking findings decide readiness; two high scores cannot offset a failed permission test or missing behavior.

## After repair

Blocking findings go to [implement's repair](../implement/SKILL.md#repair). A fresh non-author assessment covers the changed diff. Repeat a full council only when the repair changes high-impact behavior or leaves a material objection unresolved; otherwise retain the valid prior judgments. Review repairs follow the same three-cycle/early-diagnosis limit. Do not stack an R2 pass and a council on an unchanged candidate.
