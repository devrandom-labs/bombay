---
name: full-send
description: Full Send mode for a designated development task - the lead owns routine decisions and continues hands-off through PR review, required CI/tests, the established authorized test deployment and real release verification. Use when Joel invokes Full Send or as the default Patstack mode. Grants no production access.
---

# Full Send

Full Send decides Joel's involvement and the endpoint, not task size or review depth. A tiny edit in Full Send stays tiny; a risky change still gets R2/R3.

**Context.** Runs under a Patstack lead with an owned task. Invoked directly for development with no owner yet, apply Patstack's entry (ownership, project binding) first and continue here.

## Own the answer

1. Use the brief, project conventions and prior relevant tasks/decisions.
2. Resolve unresolved facts through source, the actual runtime, primary research or a cheap discriminating experiment, whichever fits. These are options, not a mandatory research ceremony.
3. If ordinary uncertainty remains, choose the safest useful, reversible in-scope default. Record the assumption and validate the outcome. Prefer an existing approved product/design pattern when taste is unsettled.
4. Do not ask Joel about routine architecture, technical targets, equivalent implementation options or facts the lead can establish.

Proceeding under uncertainty means a defensible action that is then checked. It never means calling missing proof a pass, bypassing permissions, guessing the target environment or ignoring a failed security/acceptance check. Failures enter [implement's repair loop](../implement/SKILL.md#repair); an exhausted ordinary approach calls for a new hypothesis, not abandonment or another interview.

## Gates Full Send keeps

- Significant new visual direction: browser-viewable alternatives for Joel's approval.
- Preference changes to the shared workflow involve Joel.
- Production, destructive/out-of-scope actions and stricter project gates keep their actual permissions. Reuse approvals already given.
- Vault unlock/MFA that no available authorized mechanism can complete is an access dependency ([access](../patstack/references/access.md)), not a product question.
- A verified new high-impact dependency (sensitive auth/permissions, important shared API, broad customer impact) gets one human checkpoint for that decision. Ordinary uncertainty does not qualify. Continue unaffected work while it waits.

## Test-delivery authority

Joel assigning a concrete task in Full Send (explicit or default) authorizes that task's established, unambiguous test target and its repair revisions. Record the real task grant, target and applicable head in the Issue/PR/brief, preserving the project's real approval mechanism and [coordination's delivery rules](../patstack/references/coordination.md#delivery-and-authority).

Authority record: `Patstack 1.2.0 Full Send (explicit|default)` naming the actual mode basis, established target and retained project gates in the task record. This is evidence of the real grant, not an actor-role or ticket-registry approval. Quote the real assignment with its native reference; never relabel a default as an explicit stage instruction. Full Stop and ordinary research do not supply this grant. An auto-deploying merge is a deployment. Production needs explicit task-specific authority.

## Endpoint

Continue until all hold: acceptance met on the intended test release, required review and CI cover the current head, required project sync verified, live proof bound to the actual release ([verify](../verify/SKILL.md)), then proportionate [retro](../retro/SKILL.md) and a final comment. A checkpoint, child completion, PR opening or third ordinary repair failure is not completion.

Stop short only for a retained human gate, a genuine unavailable prerequisite or an applicable execution limit. Then hand off Done / Verified / Unresolved / Next, naming the exact decision or prerequisite, and keep the task in its real waiting lifecycle rather than faking delivery.
