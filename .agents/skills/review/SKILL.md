---
name: review
description: Choose and run the right review depth for a candidate change - R1 focused author check, R2 fresh independent review, or R3 two-model council - with separate acceptance and code verdicts and security scrutiny at trust boundaries. Use before PR readiness, after a repair, or when asked to review a diff. Review never replaces executed tests.
---

# Review

**Context.** Under a Patstack lead, review the task's exact candidate against its spec version and record the level on the task/PR. Invoked directly on a diff, review it within the user's request; no ticket is created, and your own reading is REASONED evidence.

## Select the level

The lead chooses the lowest sufficient level from the actual change, uncertainty and blast radius, without asking Joel, subject to project-required review and human gates.

| Level | Who and what | Typical fit |
|---|---|---|
| **R1 Focused** | Author checks the diff and targeted result. No separate reviewer, Council, score or loop. Label it author/self-review. | Trivial cosmetic/layout, copy or mechanical edits with understood local effects and no behavior/security/data change. |
| **R2 Independent** | One fresh session that wrote none of the change checks acceptance and implementation quality. For Labcast steps, use the reviewer selected by the [review rule](../../../AGENTS.md#review) and [Models](../../project.md#models). The default elsewhere is a reviewer from the opposite model family. | Bounded behavior fixes, ordinary features, local API/data logic. **Minimum** for auth/permissions, credentials, shell handling user input, untrusted files, authenticated API calls, production databases, data integrity, destructive operations or important API contracts, with independent security scrutiny of that boundary. |
| **R3 Council** | Two fresh reviewers form independent judgments. Each then checks the other's findings against the evidence. See [Council](../council/SKILL.md). | Broad/high-impact auth or API changes, migrations/irreversible effects, cross-project/shared-system impact, substantial customer-facing change, unresolved material uncertainty or disagreement. |

- Select provisionally at intake, confirm against the actual diff, raise it if new risk appears. A legitimate scope reduction can lower it with a stated reason; rejected findings, time pressure or an unavailable reviewer cannot.
- Record level, short reason and reviewed revision, e.g. `R1 - CSS-only button colour change; no behavior, data or auth changes.`
- An unticketed research/discussion question needs no review level. Every level keeps the applicable quality standard, required repo checks and delivery proof.

## R1

Read the whole diff against the request. Check the targeted result and, for UI, the intended appearance and affected responsive/interaction/accessibility states. Fix what you find; no review loop.

## R2

1. The lead starts a fresh reviewer session that wrote none of the change. For Labcast steps, follow [AGENTS.md](../../../AGENTS.md#review) and use the model and command in [Models](../../project.md#models). Elsewhere, default to the opposite model family and use the native route in [coordination](../patstack/references/coordination.md#delegate-and-communicate). A child asks its lead to start the review. Use the selected model; do not substitute silently.
2. Brief it with the task, relevant context, exact head/diff and evidence access. Do not include the author's persuasive summary or any prior score.
3. It returns two distinct verdicts plus findings:
   - **Acceptance:** does the requested behavior work, including counterexamples?
   - **Code:** is this implementation correct, maintainable and safe in this system?
   - Each finding: claim, severity, evidence or reproduction, suggested fix.
4. Blocking findings go to [implement's repair](../implement/SKILL.md#repair). A fresh non-author assessment covers the incremental diff; the author cannot carry an earlier independent approval forward.

## Independence rules

- The author is never the independent reviewer of its own work; independence belongs to the session and its contribution, not just the model name.
- In projects that use opposite-family R2, if both model families contributed, have each family review the other's contributions. Use a fresh non-author session to check how the parts work together. In Labcast, one fresh session selected by [Models](../../project.md#models) reviews the whole step.
- Model verdicts are not GitHub human approvals and not execution proof. Agreement between models is not a test result.
- If a required reviewer model is unavailable, follow the project rules. For Labcast, see [AGENTS.md](../../../AGENTS.md#review) and [Models](../../project.md#models). When the project leaves the response to the lead, continue reversible work and try a verified alternate host that can run the same model. Keep the review level. If the model stays unavailable, mark the PR as pending review and set one next attempt within the task's timebox. Hand off at expiry.
- Technical disagreement is resolved with evidence, not by asking Joel; [grilling](../grilling/SKILL.md) is only for choices that are his.
