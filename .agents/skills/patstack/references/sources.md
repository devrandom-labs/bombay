# Design sources and module map

## Modules (workflow 1.2.0)

Patstack routes; each module owns one procedure. Common references stay in `patstack/references/`: coordination, task-state, project-context, access, quality, sources.

| Module | Owns | Main sources |
|---|---|---|
| patstack | Entry/resume, mode choice, routing, invariants | Patstack 1.0 entrypoint and decisions |
| full-send / full-stop | Mode contract, gates, test-delivery authority, endpoint | Patstack 1.0 decisions/testing-delivery; tstack Full Send/Full Stop |
| ponytail | Smallest complete change, levels, measured optimization | Ponytail 5.3.2 (MIT, vendored with provenance) |
| to-spec / to-tickets | Spec synthesis/storage, vertical slices, blocking edges | Patstack 1.0 planning/project-context; Matt Pocock to-spec/to-tickets |
| implement | Proportionate build, parallel slices, critical path, single repair loop | Patstack 1.0 build-review/testing-delivery; Matt Pocock implement/implement-spec |
| review / council | R1/R2/R3 selection, independence, two-model council | Patstack 1.0 build-review; tstack Council; Zeroshot review loop |
| verify | Evidence labels, quality verdicts, benchmarks, shared targets | Patstack 1.0 testing-delivery/quality |
| file-pr / babysit-pr | Problem-first PR, native watch, readiness | Patstack 1.0 testing-delivery; tstack File PR/Babysit PR |
| takeover / modernize | Entry routes for existing or stale work | Patstack 1.0 testing-delivery; tstack Takeover/Modernize |
| retro / workflow-eval | Proportionate retro and promotion contract; blinded evaluation | Patstack 1.0 retro; Matt Pocock retro; pstack eval playbook |
| grilling | Human-only decision interview | Earlier local grilling (Matt Pocock origin), sanitized |
| create-test-recipes | Project verification packs | Poteto pstack, MIT ([provenance](../../create-test-recipes/PROVENANCE.md)) |
| reverse-engineer-anything | REA artifact/runtime analysis | Existing REA skill, unchanged |

Native frontend-design and computer-use remain provider-owned dependencies; this package does not redistribute their caches.

## What was borrowed

| Source | Keep | Adapt |
|---|---|---|
| [Matt Pocock: to-spec](https://github.com/mattpocock/skills/blob/main/skills/engineering/to-spec/SKILL.md), [to-tickets](https://github.com/mattpocock/skills/blob/main/skills/engineering/to-tickets/SKILL.md), [implement-spec](https://github.com/mattpocock/skills/blob/main/skills/engineering/implement-spec/SKILL.md) | Synthesize an agreed spec; verifiable slices and blocking edges; expand-contract for wide refactors; fresh worker contexts and integration ownership. | Proportional specs and tests. Preserve an existing accepted brief. No interviews or approval steps for routine implementation details. PR structure fits the work. |
| [tstack Council](https://github.com/t3dotgg/tstack/blob/main/skills/council/SKILL.md), [Full Send](https://github.com/t3dotgg/tstack/blob/main/skills/full-send/SKILL.md), [Full Stop](https://github.com/t3dotgg/tstack/blob/main/skills/full-stop/SKILL.md), [File PR](https://github.com/t3dotgg/tstack/blob/main/skills/file-pr/SKILL.md), [Babysit PR](https://github.com/t3dotgg/tstack/blob/main/skills/babysit-pr/SKILL.md), [Takeover](https://github.com/t3dotgg/tstack/blob/main/skills/takeover/SKILL.md), [Modernize](https://github.com/t3dotgg/tstack/blob/main/skills/modernize/SKILL.md) | T3-owned delegation and PR wakeups; problem-first PRs; per-model scores by review round; explicit stopping modes. | Two model families. Independent first judgments, bounded repair, evidence-backed objections and project-specific authority. Verify the latest head before readiness or merge. |
| [Zeroshot review loop](https://github.com/the-open-engine/zeroshot/blob/main/docs/guides/review-loop.md) | Separate acceptance and code judgments, parallel review, repair feedback, fresh review sessions, explicit terminal failure. | Borrow the graph and bounded transitions; running the Zeroshot engine is not a prerequisite. |
| [Ponytail](https://github.com/DietrichGebert/ponytail/blob/main/skills/ponytail/SKILL.md) | Reuse existing code and platform capabilities; smallest complete change; retain validation, security, accessibility and necessary checks. | Vendored as the ponytail module with MIT license and provenance; the same economy shapes Patstack itself. |
| [Matt Pocock retro](https://github.com/mattpocock/skills/blob/main/skills/engineering/retro/SKILL.md) | Diagnose actual friction; prefer automated checks for mechanical mistakes. | Joel's evidence-backed autonomous technical improvement grant; preferences and product choices still involve him. |
| [pstack eval playbook](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/eval.md), [benchmark checklist](https://github.com/cursor/plugins/blob/main/pstack/skills/benchmark-checklist/SKILL.md) | Blinded comparison of pinned variants; measurement validity. | Bounded, on-demand or explicitly scheduled; never a step of ordinary tasks. |

Upstream files other than the vendored Ponytail and create-test-recipes sources were inspected as sources, not installed or executed; module text is a Patstack rewrite. An implementation that vendors further files should pin and verify revisions and preserve attribution.
