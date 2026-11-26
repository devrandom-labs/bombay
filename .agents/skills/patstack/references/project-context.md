# Bind the actual project

Resolve the workspace, project pack, environment, applicable permissions and host capabilities. Preserve project procedures; load detailed host routing only when host/tool choice matters.

## Specs, issues and PRs

Spec storage and distribution follow [to-spec](../../to-spec/SKILL.md); sub-issues, blocking links and Projects follow [to-tickets](../../to-tickets/SKILL.md); PR linking and branch protection follow [file-pr](../../file-pr/SKILL.md). Patstack adds no per-task spec file to the application repository.

## Project workflows are part of the task contract

Before work, resolve the actual repo/workspace and applicable project pack, instructions, auth conventions, CI, branch rules, test/deploy target and required follow-through. Use available tools and repo evidence rather than asking Joel for facts already present. Bind every worker to that context and its owned workspace. Verify described hooks and tools are actually configured; documentation alone does not prove execution.

Assess size and impact separately. Shared auth/session or permission behavior, important API contracts and broad effects on customer-facing production products are strong Full Stop signals even when the diff is small. A live production target never follows merely from working on the source of a production app. BIG TASK uses product inventories, system maps, dependency evidence and existing rules to identify affected products; ask Joel only where business criticality or a product decision remains ambiguous. A newly discovered high-impact dependency during implementation triggers reassessment of the affected work.

Read [Labcast project context](../../../project.md) and only the owner records
and specialist skills needed by the actual task.

Some project packs define post-merge synchronization to another branch or hosted builder, with drift checks and project-specific exclusions. Preserve those constraints. If the established path includes a PR hook, verify its actual result for the delivered revision; if that step has not run, the affected Full Send delivery is not complete. The procedure alone does not prove that a live hook ran for this revision.

Project workflows can include real human gates, such as intake decisions or new infrastructure; Full Send should reuse settled answers and approved project settings, not silently remove those gates. Report stale or conflicting procedure guidance against the actual project contract rather than copying it blindly.

## Host capabilities

Inspect relevant tools and host capabilities when needed. Use the project's Nix
environment and existing approved hosts. Keep credentials and host details in
their existing private configuration. Record only the tested environment and
limitations in task evidence.

Route by the task's needs and current capability/resource availability:

| Requirement | Eligible host |
|---|---|
| macOS APIs, Xcode, Apple-platform builds, native Mac interaction | macOS host with the required tools verified |
| Linux behavior, service/build work and Linux browser automation | Linux host when the needed stack is available |
| Native Windows behavior, Windows desktop/Steam/game/hardware tests | Windows host, verified in that actual environment |
| GPU workloads | A host with detected suitable hardware plus the required working driver/toolchain; presence of a GPU alone is insufficient |
| Frontend/browser testing | Any host/runtime that provides the required real browser/Computer Use proof; use the actual target OS when behavior depends on it |
| Mobile | Host exposing the relevant verified simulator/emulator/physical-device capability; report which was actually tested |

Check capability and project/spec access before dispatch. Use fresh reachability/tool verification for the chosen host instead of treating an old inventory as live status. Reserve shared GUI/device resources and avoid other users/agents' active contexts. Same-model/tool availability can differ by host and runtime. A working SSH/T3 ping does not establish Computer Use, an installed Docker CLI does not establish a running daemon, and a configured environment does not establish a deployed app.

### Devbox browser capability

Use the existing approved browser and matching automation runtime through Nix.
[Playwright's browser guidance](https://playwright.dev/docs/browsers) describes version-matched binaries and Linux dependencies.

Headless Chromium on Linux can provide interaction assertions, screenshots, traces and relevant performance measurements. Record that actual platform/browser; native Windows/macOS behavior still needs its own target environment. Keep before/after benchmarking on equivalent conditions. Interactive visual-direction approval should use a view Joel can actually inspect.

Use isolated task-owned browser sessions, profiles, fixtures and artifact directories. Do not reuse or close another session's browser. Keep a verified launch recipe with the existing project test instructions. A synthetic browser smoke test establishes infrastructure readiness; it does not establish that a project feature or production behavior passes. Native Computer Use capability is recorded separately from Playwright automation.
