# Wart log — card #218 (job-queue compositional example)

Every friction point hit while building the example lands here IMMEDIATELY,
then gets triaged into an M1 GitHub issue at the next phase boundary. An entry
is closed only when it carries its issue number. Severity: `blocker` (spine
cannot express the app) / `boilerplate` / `paper-cut`.

| # | Severity | Wart | Issue |
|---|----------|------|-------|
| 1 | boilerplate | Hand-written `Mailboxed` + `Actor` impls for every actor; only `Msg` has a derive, and it must be named as `bombay_macros::Msg` (dev-dep, no re-export through `bombay`). ~40 lines of ceremony per trivial actor. | #243 |
| 2 | boilerplate | A supervisor cannot observe a child rebuild: `on_link_died` never fires for supervised children (`actor/kind.rs` consumes the notice) and no hook delivers the fresh `ActorRef`/`ActorId` — the factory closure is the only seam, forcing the factory-`try_send`-to-self `WorkerReplaced` pattern with a mandatory `WeakActorRef` capture (strong capture = self-cycle, actor never ref-count-stops). | #244 |
| 3 | paper-cut | Roster/rebuild bookkeeping (`WorkerReplaced`) travels through the ordinary bounded mailbox and competes with user backlog — a full mailbox silently loses the roster update. Evidence for #225 (control-signal lane). | folded into #244 (the rebuild-observation seam; #225 was evidence only — control lane carries no user messages) |
| 4 | boilerplate | ~~A supervisor stopping `Normal` does NOT stop its remaining children (only the escalation path calls `stop_surviving_children`; the supervised lifecycle in `spawn.rs` runs no child sweep). A drained app must explicitly `stop_child` each worker and self-signal `FinishStop` to tear down.~~ Resolved by #245: supervisor exit now sweeps children (cancel → bounded join → abort), so `FinishStop` and the manual `stop_child` loop are removed (ADR-0019). | #245 |
