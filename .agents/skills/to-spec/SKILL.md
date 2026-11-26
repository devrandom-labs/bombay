---
name: to-spec
description: Synthesize an agreed brief into a versioned spec - problem, goal and non-goals, acceptance, quality targets, test seams and risks - kept in a GitHub Issue or an accessible versioned artifact outside the application repo. Use for a new feature or larger change before implementation; a tiny explicit request is its own brief and needs no spec.
---

# To spec

Synthesize what is already known; do not interview. Facts come from the repo, tools and prior tasks. Only a genuinely unsettled choice that is Joel's goes to [grilling](../grilling/SKILL.md).

**Context.** Under a Patstack lead, reuse an accepted spec instead of writing a new one; this skill fills what is missing and versions changes. Invoked directly, write the spec the user asked for; it creates no task and no implementation authority.

## When a spec is needed

- Tiny explicit request: the request is the brief. No spec file, task graph or interview.
- Bounded fix: a short working contract on the task (expected behavior, reproduction, success check) is enough.
- New feature or larger change: write the spec below.

## Write it

1. Explore the affected code. Use the project's domain vocabulary and respect ADRs in the area.
2. Choose test seams: prefer existing ones, as high as possible, as few as possible. In Full Stop, confirm a new seam with Joel only if it changes product behavior; otherwise decide.
3. Derive missing quality targets before implementation from project evidence ([quality](../patstack/references/quality.md)). New features include their UI/UX/performance/data proof as acceptance items. Freeze the measurement protocol now so BEFORE can be captured.
4. Preserve user-selected behavior and any approved visual direction. Significant new visual direction is a browser-options approval point, not something a spec decides.
5. Fill the template; keep it as short as the work allows.

```markdown
## Problem
What the user faces, from their perspective.
## Goal / non-goals
## Acceptance
Numbered, verifiable criteria, including counterexamples and permission/data assertions.
## Quality contract
Per applicable dimension: target, source, test method. N/A with reason.
## Decisions
Modules and interfaces touched, API/schema contracts, settled choices and recorded assumptions. No file paths or code, except a prototype snippet that states a decision more precisely than prose (say so).
## Testing
Seams, prior art, what a good test asserts (external behavior).
## Delivery and risks
Intended test target, mode, review level, rollback, known risks.
## Out of scope
```

## Store and distribute

- GitHub-backed work: the canonical spec is the parent Issue body. It is not a repo file, so it needs no commit and no `.gitignore` entry. Add no per-task spec file to the application repo; project-required intake records, ADRs or requested docs remain real deliverables.
- Deliberately local-only work: one shared, versioned artifact outside the codebase.
- Give every worker/reviewer the reference and exact version. If an agent or host cannot read it, send the content or an accessible copy with the version attached; a path the recipient cannot open is not a handoff. On change, bump the version, send it to affected agents and get acknowledgment. No independently editable forks.
- Keep drafts and caches outside the application repo; clean them once the canonical version and evidence are retained. After completion the Issue keeps the spec, PR links, evidence and retro as history.
