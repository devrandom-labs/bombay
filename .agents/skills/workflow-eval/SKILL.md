---
name: workflow-eval
description: Repeatable, bounded, blinded evaluation of pinned skill or workflow versions on representative scenarios - isolated workspaces, hidden variant labels for the judge, measured quality, completion, rework, time and cost. Use on demand to validate a material workflow/skill change or a retro promotion, or from an explicitly authorized schedule; not a step of ordinary development tasks.
---

# Workflow eval

Answer "is variant B better than A on the work we actually do?" with outcomes, not self-reported compliance. Adapted from [pstack's blinded evaluation playbook](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/eval.md).

**Context.** Runs on demand, after a meaningful candidate change, or from a schedule Joel explicitly configured. It is unticketed unless Joel designates it as development work. It creates no recurring job, deploys nothing, touches no production or real customer data, and changes no preferences.

## 1. Define the run

- **Variants:** each pinned by commit/hash of the skill files and the model/runtime used. Same models across variants unless the model is what is being compared.
- **Scenarios:** representative tasks with known acceptance; start from [scenarios](references/scenarios.md) and add ones that reproduce the failure under study. Reuse existing scenarios; do not build a second task-status system.
- **Metrics:** acceptance pass, quality verdicts, rework/repair cycles, unnecessary ceremony, elapsed time, tokens/cost where available, permission or gate violations (any violation fails the run).
- **Budget:** a cap on runs, time and spend, stated up front. Stop at the cap and report partial results as partial.

## 2. Execute

1. One isolated workspace per scenario × variant (temporary checkout or fixture copy); no shared state between runs and nothing written into real repos.
2. Same prompt, fixtures, tools and conditions per variant; alternate or randomize order.
3. Retain actual outcomes: diffs, test output, transcripts/traces, timings.
4. Repeat where variance matters; report sample counts.

## 3. Judge blind

- A fresh judge session that ran none of the variants sees outcomes with variant and model labels stripped, scored against a rubric fixed before the run.
- Executed acceptance results outrank judge scores. Judge scores are labelled judgments.
- Report per scenario and aggregate: metric per variant, sample count, spread, and where variants differ beyond noise.

## 4. Use the result

Results feed [retro](../retro/SKILL.md) or a promotion decision as evidence. A mechanical fix needs proportionate checks, not the whole suite. Record the variants, scenario set version, raw artifact location and limitations so the run can be repeated.
