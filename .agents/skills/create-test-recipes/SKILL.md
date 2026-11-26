---
name: create-test-recipes
description: Use when a project needs repeatable verification recipes, existing recipes are stale, or agents keep rediscovering how to test an app, API, CLI or user journey.
license: MIT
---

# Create test recipes

Create or maintain a project-owned verification skill that a fresh agent can run. Ground its commands, assertions and evidence in the real project. Reuse existing harnesses; generate only the missing pieces. This generator supports testing, not deployment or task orchestration.

## Discover the project

Inspect the actual workspace, project instructions, test/deployment contracts, commands and existing verification pack. Identify requested journeys, supported surfaces, fixture ownership, auth prerequisites, target environment and available tools. Observe facts before asking Joel; ask only about unresolved product choices or missing authority.

Confirm that startup reaches a usable state and that tests address the intended build. For short-lived CLIs, check the binary/version and invoke the real command; no server or PTY is required unless the interface needs one. Preserve the project's test seam and production boundaries. Report a broken prerequisite rather than repairing application code outside scope. Use approved synthetic/task-owned data; never reset or reseed a shared dataset to make a recipe runnable.

## Choose one project home

Maintain the existing project pack/verification directory when present. Otherwise use a project-local `.agents/skills/verify-<app>/`, unless project instructions specify another home. Keep recipes outside global skill discovery. Link through the project's existing index or instructions within the authorized scope so Claude and Codex can find the same source; do not duplicate client-specific trees. Keep device addresses, credentials and private access details in their existing private configuration, referenced by name only.

## Generate a runnable contract

Write `SKILL.md` with frontmatter naming the app, surface and verification triggers. Cover the applicable sections below; use N/A with a reason when a step is absent, such as server startup for a simple CLI.

| Section | Required content |
|---|---|
| Launch | Real commands, prerequisites, readiness condition and owned resources; an existing live target needs a read-only readiness check instead of local startup. |
| Doctor | Read-only checks for the intended environment/build/release, service readiness and required test identity; never expose credentials. |
| Run | Exact entry commands/locators and assertions, plus links to the requested feature recipes and existing tests. |
| Evidence | Retained artifact location, executed steps, expected/actual result, head/base/build/release and limitations appropriate to the check. |
| Cleanup | Put scratch and evidence inside the authorized write scope, checking temporary-directory defaults. Stop only owned processes and remove only owned temporary fixtures; preserve evidence, including after failed runs. |

Add a small feature index and one recipe per requested coherent journey. Each records entry point, fixture/persona prerequisites, actions, exact expected behavior and side effects, relevant negative cases, quality targets and last verified revision/environment (content hashes when there is no VCS). Prefer real UI/API/CLI entry points over internal setters. Existing scripts remain the implementation source; link rather than copy them. Add an executable helper only when it removes genuinely repeated or fragile work, and show its invocation. Assertions must fail when behavior or prerequisites are wrong; never weaken a failing contract to fit the output.

For performance/API recipes, read [quality checks](references/quality-checks.md). Load only relevant journeys; a button-colour change does not need a service benchmark.

## Execute and classify

Run every new or materially changed recipe in the requested delivery scope through readiness, actions, assertions, evidence capture and cleanup. Confirm evidence survives cleanup, including failure paths. Check assertions against known-good input and a known failure/safe counterexample where practical; do not mutate a live app to manufacture either. A comparator self-test validates its checks, not the application's unobserved success path. Report any missing positive/negative control as a validity limit.

Report **recipe validity: VALID / BROKEN / UNTESTED**, with the executed scope and limits, separately from **application outcome: PASS / FAIL / BLOCKED / NOT RUN**. A faithful recipe can correctly detect an application FAIL. Unavailable prerequisites are BLOCKED; only executed successful assertions are PASS. Distinguish a broken helper from an application defect. List coverage explicitly; one passing journey cannot validate unrun journeys.

Keep PR/local and live results separate. Bind live proof to the actual release and check for drift across the run. A local or stand-in result never establishes a deployed pass. Return failures with criterion, candidate/release and evidence to the existing lead's repair flow; generation grants no merge, deploy, rollback or live-data authority.

## Maintain and hand back

Update affected recipes in place as behavior changes. Preserve unrelated entries, valid proof and original attribution. Report the generated path, discovery link (or why it is missing), runnable entry command, recipe/outcome statuses, evidence links and exact remaining gaps. Suggest no cron job or new global workflow by default.

Adapted from Poteto's pstack generator; source revision and license are in [PROVENANCE.md](PROVENANCE.md) and [LICENSE](LICENSE).
