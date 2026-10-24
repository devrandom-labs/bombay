# Recovery after invalid requests

Status: active regression preparation within Static remote actors R09
(ID-10/AUTH-03). This record owns the narrow liveness conflict; the parent
remote PRD and literal acceptance row retain every broader requirement.
Source baselineb19ca648cec41a9f971b6a74e4fcbdec13990568. This is not main:
PR397 is delivered as9ea5defe and PR398 as e50f6e1c; PR399–403 have
separate exact-head gates. Neither is new recovery evidence.

## Problem and retained contract

Literal R09 and the resource table require eventual valid progress after
invalid-ID pressure stops under declared fair-capacity assumptions. Current
Export deliberately reserves an append-only lifetime Request row before any
provider invocation. Every acquired refusal counts; once rows reach the
configured maximum, later native Queries are discharged without verification.
Existing quota tests correctly establish that selected lifetime law. They use
an always-refusing policy or repeat one canonical request, so they cannot
establish invalid-then-valid recovery. No accepted fair-capacity assumption
currently authorizes permanent suppression by invalid traffic.

Preserve exact original causes and typed dispositions, bounded memory/work,
R12 replay denial, R25 diagnostic conservation and normal Node ownership.
Do not reset/reopen the Node, silently delete refusal facts, manufacture a
provider result for an unverified query or weaken the literal recovery promise.
No production repair or new retention/discharge policy is selected yet.

## Prerequisites

Fresh review034c2ff2 and input ledger5696c4a3 verify current b19 before edits;
root independently rechecks every listed input. Complete selected Behavior
instructions760/ec0ad868 and relevant APIs/docs/tests are freshly read.

| Owner | Exact selection and retained role |
| --- | --- |
| Behavior/Actors |0.29.0/55dbac2d; macros0.16.0. Pure Counter Actions and existing shutdown template. |
| Address |0.3.0/9f058dc0. Certify actual actor recipient; numeric origin is not authentication. |
| Communication |0.1.6/963067e0. Distinguish admission from processing; retain actual reply/closure. |
| Observe |Local0e63b34d. Existing independent native outcomes and notifications; no new observer. |
| Timers |0.2.0/9369d1f6. Unchanged generation owner; watchdog is not progress evidence. |
| Entity/Driver |Local b19. Empty families and sole causal fold/retirement composition. |
| Native |SDK1.10.10/c34eb350, config1.10.2, TLS1.10.4, protocol1.10.1, Tokio1.53.3; explicit TLS only. |

Root lock83f01ca3, no owning patch override. Current quota/peer/wire/protocol,
Node/Export/provider custody and native Query/FINAL contracts are verified.
There is no prerequisite blocker to this finite failing-law comparison.
Compiler fitness is unexecuted; production remains blocked until the intended
failure and an explicit resolution of the lifetime/recovery policy conflict.
Reuse standard Results and the already selected Tokio oneshot; no dependency
selection, competing actor/runtime, clock or test framework is needed.

## Smallest failing law and change record

Use the same live public Counter Node/export for two actual native Queries.
First send an unregistered proof pair through the existing deterministic
provider; after its real native EOF, stop invalid traffic and submit a
separately registered permitted IncrementAndReply. Compare retained_requests1
with matched2; both reserve two queued/native query slots and the same fixed
TLS profile. EOF is not proof that a local native-query count permit retired.
No positive is credited from an always-denying provider or reopened export.

After both actual Query streams finish, an existing oneshot wakes Work to
perform an independent local Counter Read, then normal shutdown. Work moves
its actual shutdown Result through a second oneshot to the private peer;
only after joined Node close does that peer close. Retain exact native replies,
all row/provider/admission/publication causes and root/notification outcomes.
The matched2 must execute the valid increment and read1. Under1, source
predicts one invalid row, one provider invocation, no increment and local0.
Only the last independent local Read1 oracle supplies intended failure credit;
all source-specific row/cause, native EOF and cleanup controls precede it.
A compiler/setup/watchdog failure supplies no semantic credit.

Reuse existing node_exports/quota.rs, peer.rs, wire/protocol, real Counter,
Application, Node, Export, Tokio oneshot and standard Result products. The
private quota peer accepts a finite pair instead of repeating one input, and
its existing shutdown hook awaits the actual result. Original quota controls
supply two exact input copies and wrap the same shutdown in an async closure.
No new test controller, public interface, runtime, dependency or framework.

Expected scope: one new owning recovery.rs, its consumer registration, the
existing private peer and quota caller, this record, a parent PRD pointer and
two backlog pointers; eight paths. Expected production+0/-0/net0, roughly
300–400 authored/modified test lines, public types+0/-0. Numerical budgets are
removed. Measure complete tracked/untracked deltas at each checkpoint; do not
mix unrelated audit defects or legacy retirement into this law.

## Acceptance and remaining work

Review the new test twice, including independent counterexamples; first
execute matched and constrained cases in both debug and optimized builds
through pinned Nix. Preserve actual selected source, profile, native cleanup,
intended failing oracle and exact input identities. Only that original
failure may select a concrete recovery policy and smallest repair. Record
ordinary-Rust alternatives, owners and API consequences before production.
Then repaired paired positives, owning quota/consumer regressions, intended
counterfactual/restoration, appropriate full checks, source minimization,
focused PR, required new-head CI and protected main delivery remain required.
This one law does not establish complete flood/CPU/cache/delegation bounds,
R12/R14/R25 or the full remote PRD; three whole-codebase audits remain active.

## Original execution and policy blocker

Reviewed sourcee7b2e03d becomes formattedfe2094d1, with private peerbfd23bc1
and quota1bccc649. Two source reviews resolve the unexecuted initial proposal's
temporary native-result borrow and incomplete local drain before application.
Reviewf3b116fd qualifies the amended source;80a2cff8 confirms formatting only.
No compiler/setup failure is credited as a regression.

Actual original debug and optimized commands each select exactly two cases:
matched2 passes, constrained1 fails ONLY the last local Read oracle at376,
actual User(MailAddr0,0) versus required1. Both exit101 with1passed/1failed,
zero ignored and43 filtered; runtime0.04s/0.03s, compilation16.41s/1m02s.
The invalid row, exact failed verification and inputs, truthful capacity
disposition, matched native increment431/reply proof and all real cleanup
controls run before that oracle. Ten selected input hashes match before and
after each profile. Raw logs2ae4f062/9e511496 and independent paired review
fc5a67c1 retain the original source epoch; no production change is present.

The pinned-shell commands are `cargo test --locked --offline -p bombay-rs
--all-features --test application_clients node_exports::consumer::recovery::
-- --nocapture --test-threads=1`, with `--release` for the optimized run.
This establishes the narrow lifetime/recovery conflict, not an accepted repair.
Reopen only the needed retention/discharge policy. Required bounded custody,
original facts, replay and private SDK ownership remain mandatory.

Complete current working tree against b19, including the untracked381-line
test: eight paths; production+0/-0/net0, tests+399/-9/net390, records+148/
-0/net148; public types+0/-0. Full main-relative inherited source is separate.

The shared Git `session-archives/2026-10-21/request-recovery-original-20261024.tar.gz`
preserves the original failing pair, sources, prerequisite/input manifests and
reviews before any repair. It qualifies no repaired source or CI delivery.
