# Stay on task across long runs

Use the existing ticket reference, Issue/brief, shared spec and native PR/run IDs. Do not create a second status database, duplicate editable spec or new ticket at each phase. This contract describes information to retain in the existing Issue/brief/native continuation record; it is not a new runtime engine.

Keep the compact record current:

- Goal and acceptance: canonical spec/Issue reference and version; current user constraints, mode and scope.
- Progress: completed criteria with evidence, remaining criteria and current phase.
- Candidate: repo/worktree, head/base, tested build/release and relevant artifact references.
- Coordination: lead, active child files/run IDs, PR/watch lead and who is currently testing a shared target. These are collaboration notes, not locks, leases or eligibility rules.
- Current action: next concrete action, current hypothesis or precise wait/blocker, native wake mechanism and any retained human decision.
- Continuity: cumulative attempts and elapsed time, workflow version and last checkpoint.

Checkpoint meaningful milestones, before handoff/context compaction and at least every 15 minutes during long active work. Use existing timestamps/results rather than narrating every command. Each child returns its acceptance/evidence and unresolved findings against the same spec. New suggestions outside scope remain follow-ups unless needed for the requested outcome.

After compaction, interruption or takeover: read the record and later user changes; verify who owns work, inspect current child runs, PR head/base/checks and deployed release; continue the earliest incomplete valid step. Inspect uncertain dispatch outcomes before retrying, so resume does not create duplicate agents, PRs or deployments. Later explicit user steering supersedes recorded constraints. Bump the spec version, re-brief or stop affected children, and invalidate evidence for a superseded candidate. A stale checkpoint is a starting reference, not live proof.

Full Send continues until acceptance plus testable live proof, a retained human gate, a genuine unavailable prerequisite or an applicable execution limit. A checkpoint, subagent completion, PR opening or third ordinary repair failure is not completion. Stagnation triggers deeper diagnosis with a changed hypothesis and the same history. Run-scoped budgets/timeboxes are visible; do not reset them by starting a new chat.

For CI/review waits, use the native PR watcher and end only the current turn so its event can wake the designated lead. Retain the same reference/context and exact next action. For other waits, use available native run/device/service events; if no durable wake mechanism exists, report that exact liveness gap instead of pretending an idle chat will restart itself. Do not create an unrequested recurring job.

Human handoff says Done / Verified / Unresolved / Next and names the decision or prerequisite. Full Stop ends in the ready unmerged PR handoff. Neither mode marks missing required proof as complete. Retro closes the loop proportionally once the endpoint is actually achieved.
