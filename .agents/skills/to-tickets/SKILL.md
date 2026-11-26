---
name: to-tickets
description: Break an agreed spec into tracer-bullet vertical slices with explicit blocking edges, each independently verifiable with one owner, exact scope and acceptance, published as GitHub sub-issues with native blocking links or in the shared task brief. Use when a spec needs parallel or staged delivery; skip it when one lead can finish the work directly.
---

# To tickets

Split only work that is independently verifiable and benefits from parallel or staged execution. One lead finishing a coherent change directly is the default; slicing is a cost.

**Context.** Under a Patstack lead, slices become children of the existing task/Issue; the lead remains responsible for integration and the full outcome. Invoked directly, draft the breakdown for the user and publish only where they asked.

## 1. Read the source

Use the spec/Issue (full body and comments) and the code it touches. Look for prefactoring that makes the change easy; do it first.

## 2. Draft vertical slices

- Each slice cuts a narrow but complete path through every layer it needs (schema, API, UI, tests), and is demoable or verifiable alone.
- Each fits one fresh context window and one owner.
- Quality proof for a feature slice belongs inside that slice, not in a later "polish" ticket.
- Give each slice its **blocking edges**: the slices that must finish first. A slice with no blockers can start now. Work the frontier: every slice whose blockers are done.

**Wide refactors** (one mechanical change fanning across the codebase, e.g. renaming a column or retyping a shared symbol) use expand-contract instead: expand (add the new form beside the old), migrate callers in batches sized by blast radius, each blocked by the expand and green on its own, then contract (remove the old form) blocked by every batch. If batches cannot stay green alone, give them a shared integration branch and a final integrate-and-verify slice; green is promised only there.

## 3. Settle the breakdown

List each slice: title, blocked by, what it delivers end to end, acceptance, owner/model, scope/worktree. In Full Send the lead settles granularity itself. Present it for approval when Joel invoked this directly or the task is Full Stop and the split reflects a product choice.

## 4. Publish

- **GitHub:** one sub-issue per slice under the parent Issue ([sub-issues](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/adding-sub-issues)), created blockers-first, using native [blocking relationships](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-issue-dependencies). Reuse the repo's issue types/labels. Do not close or rewrite the parent.
- **Task brief:** give each independent slice its deliverable, acceptance, files/worktree and dependencies; share the parent ticket reference. Native subagents can start directly with that brief, without registry registration, claims or frozen model assignments ([coordination](../patstack/references/coordination.md)). Preserve agreed test seams.
- Use GitHub Projects for delivery visibility where useful; execution context stays in the shared brief/native run. One authority per field, no second editable status board.
- Reviews and QA are checks on existing slices, not new development issues.
- When the parent's acceptance includes release/live verification, avoid auto-closing it on merge; an intermediate slice may close while the parent stays open.

Avoid file paths and code snippets in slices; they go stale. Exception: a prototype snippet that states a decision more precisely than prose.
