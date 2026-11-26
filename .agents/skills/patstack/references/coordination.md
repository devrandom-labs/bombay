# Ticket references and native coordination

Use the existing repository-qualified GitHub issue reference, such as
`labcast/labcast-server#308`. The lead and its workers share that reference.
GitHub owns task status, assignments and dependencies. Reviews and session starts
do not create tickets. Follow the [project context](../../../project.md).

## Enter or resume

Read the actual request, accepted spec/Issue and later steering; bind the project, environment and current worktree. Use the number once, not a new ticket per phase or agent. Keep the goal, scope, mode, acceptance, head/build/release, evidence, participants, repair history and next action in the existing Issue/brief/native task record ([continuity](task-state.md)). No second execution database is introduced. Discussion, research and casual testing stay unticketed unless designated as work.

Shared files and releases are coordinated through the brief and current participants. Check for active work and uncommitted changes; preserve it, agree who integrates overlapping edits and use isolated worktrees when useful. This is practical collaboration, not a registry lock or permission check. Resume at the earliest valid phase and preserve credit. A new session or different machine never resets attempts, grants authority or changes the ticket's identity.

## Delegate and communicate

One lead coordinates one child layer. Choose actual available models/tools for the task; project review requirements determine the required model families. Small work stays direct. A child gets the ticket reference, accessible spec/version, project/worktree, files, actual model/runtime/tools, allowed actions, dependencies, acceptance, proof and stop conditions. Children preserve others' work and do not start agents or top-level chats. Review independence covers the actual contributions; model agreement is not execution proof.

Use the host's native delegation and run events under its current tool contract. Inspect ambiguous dispatch outcomes before retrying and preserve returned IDs/idempotency keys; queued is not delivered. Share context through native results and existing task artifacts, with no secrets/private browser records. Existing peer/Claude bridge tools remain transport where applicable, never task or deployment authority.

## T3 Code tools

Follow the live injected T3 contract and exposed schemas; availability and policies vary by runtime. Relevant deferred tools must be discovered before calling a capability absent. Prefer native subagents for same-provider work when the current contract permits; use `delegate_task` for cross-provider/background/isolated or explicitly delegated work as specified by the host. Use live model capabilities when required. Each new independent delegated review is a new task; fix small findings directly when the host says so. Do not turn a child backing thread into a fresh review round. Follow a running delegate's returned continuation instructions; do not invent polling or new top-level chats.

- PRs: link each created/taken-over/pushed PR immediately using exposed `link_pull_request`, including stack layers. Handle current findings first. When required work waits on checks/reviews, the lead calls `watch_pull_request` and ends the turn; native events wake it. Children do not watch. Unwatch before handback. CLI-created PRs still need linking.
- Browser work: follow the live browser contract. When requested/allowed, start with `preview_status`, then `preview_open` if needed; use snapshots and focused interactions. Respect human/agent tab ownership. Parallel browser agents use separate tabs and their returned IDs. Follow the documented fallback threshold and retain the actual error. Do not use preview tools for own edits when the host prohibits it.
- Devices: use exposed `device_list`/`device_open` and the exact returned launcher/context for native proof. A model name does not establish device access.
- Visuals: use `html_preview` then `html_render` when the current host requests inline charts/flows; exported assets belong in the requested repo/file. Do not invent a phase/progress-bar setter.
- Secrets and schedules: use private secret-input tools when required; never chat credentials. Persistent schedules/top-level conversations are created only when requested. None grants deployment or external-message authority.

## Delivery and authority

A ticket number, agent role or tool call grants no permission. Preserve actual project AGENTS/DEPLOY rules, explicit task grants, native client approvals, branch protection and data/production boundaries. Full Send covers the task's established, unambiguous authorized test target and repair revisions; Full Stop ends at an evidenced ready unmerged PR. Auto-deploying merges are deployments. Production needs its actual explicit authority.

Record the real assignment, mode basis, target and applicable head/release in the Issue/PR or existing brief. Quote genuine approval when required.

Finish when the requested endpoint, required review/checks, actual release proof and proportionate Retro are complete. A real missing acceptance check, capability or approval remains a real blocker; report Done / Verified / Unresolved / Next without claiming missing proof.
