# Local event fairness

Status: merged through [PR #366](https://github.com/devrandom-labs/bombay/pull/366).
Merge: `b652cd703f5e06b72d0b535d9e0c531081fda8c8` on 2026-10-15 UTC.
Selected scope: PERF-07 local event-selection prerequisite, named FAIR1.
Blocked by: none after the source verification below. Unblocks: no full
backlog requirement; overload, latency measurement and broadcaster policies
remain independent PERF-07 work.

## Selected contract and research

This is deliberate Bombay scheduling policy, not an actor-model timing law.
Owner cancellation is checked first at each acquisition. Ordinary acquisition
rotates mailbox → local results → due timers, starting after the last acquired
source and skipping pending sources. A continuously poll-ready source is
acquired within three ordinary acquisitions (at most two others), assuming the
actor continues acquiring events. Pending work registers a wakeup and parks.
The bound measures acquisitions, not milliseconds or completed actor work.

Local results alternate termination observations and startup-task results, so
each continuously ready family is selected within two local selections. A
selected child termination transfers to the existing control FIFO after its
already-admitted parent reports; peer observations return locally. Child facts
therefore wait for that finite control prefix before Behavior delivery, and
later controls cannot overtake them. Control-only source settlement alternates
control mailbox and local results, excluding user input and timers as required
by D-SETTLE-1. Ordinary and settlement service retain separate cursors, so settlement cannot
reset an ordinary entitlement. Cursor state survives cancellation of a pending
receiving future.
Selection loss cannot discard an acquired fact. A child observation may be
transferred exactly once into the existing control FIFO before local receiving
returns Pending; cancellation then leaves that exact queued event intact.
Original startup tasks remain owned by
ActivationTasks across selection loss; an error retains the original JoinError.

Communication owns control-first priority with optional aging. Bombay selects
its existing aging cap of one in the standard local environment, so a ready
user waits behind at most one control at ordinary mailbox acquisitions.
Control-only settlement remains separate and excludes users. With three ready
outer families this gives at most six ordinary acquisitions for each mailbox
lane. Observation registration order and timer deadline
order remain intact; earlier finite registrations cannot be overtaken by newly
appended observations. No global executor or individual-task millisecond bound
is claimed.

Tokio's [select documentation](https://docs.rs/tokio/1.53.1/tokio/macro.select.html)
explains random initial polling and biased caller responsibility. Removing
`biased` cannot prove the deterministic bound. Its JoinSet receiving contract
is cancellation-safe; tasks live outside the borrowed joining future.
The [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/naming.html)
were checked: no public interface changes are needed. Ordinary private enums
and explicit polling suffice; no wrapper, trait, generic scheduler or new macro.

Slow execution is a separate contract: pure folds stay short, expensive work
belongs in independent workers returning typed results, and accepting work does
not mean completing it. This patch does not detach existing action commitment.
Broadcaster camera replacement, commentary expiry, replay/speech concurrency,
explicit overload rejection, and separate queue-age/timer-lateness/actor-turn/
worker-duration metrics belong in the application, which is absent here.

## Locked-contract verification at fairness delivery

The delivered fairness source selected registry packages without workspace
patches. This table retains that execution's exact versions. The later
[Communication0.1.6 adoption](static-remote-actors.md#communication-capacity-dependency-2026-10-15-utc)
has separate integration gates; it does not rewrite the earlier witnesses.

| Owner | Version / archive revision | Inspected contract and witnesses |
| --- | --- | --- |
| Behavior and Actors | 0.25.0 / 2bb4510077bfa5aed82e08d25453c146b3184285 | Complete revision AGENTS.md; public algebra, source_settlement_admission, runtime_contracts, timer_action_settlement; pure typed Actions and source custody remain unchanged. |
| Behavior Macros | 0.14.1 / locked archive | No macro changes or new authoring seam. |
| Address | 0.3.0 / 9f058dc03d1239e1ef5c3147a893b134a1e74a83 | lib.rs and reservations tests; publication/lease retirement unchanged. |
| Communication | 0.1.5 / 24ad2b9db9f5fefd08d6d2441f84c804b4c84338 | Config, Consumer::recv/recv_control, cancellation and strict-default edge tests; existing aging cap supplies Bombay’s standard lane policy. |
| Observe | workspace-private selected source | ObservationFuture::poll, completion registration, cancellation/wakeup tests; TerminationObservations retains pending futures and removes only the returned observation. |
| Timers | 0.2.0 / 9369d1f6c7fcda91e7de5bf3c19e6769f92145bd | TimerQueue schedule/next_deadline/pop_due and semantics tests; existing generation/order ownership retained. |
| Tokio | 1.53.1 | Select cancellation safety and JoinSet; original task handles remain outside selection futures. |

The original fairness proof above used Behavior/Actors0.25.0. The developer
experience integration selects registry0.26.0/macros0.14.2 without patches, plus
Bombay0.1.2 and the paired SDK1.10.9 locks. Its separate combined-source evidence
belongs in [the coordination record](developer-experience.md); original0.25
results do not certify the integrated source.

Bombay owns selection in ActiveLocalEnvironment and ApplicationCapabilities.
Engine's causal Driver, initialization, complete action interpretation and
retirement retain their existing owners. Entity uses the same environment;
its user-lane fence and admission conversion are preserved.

## Pre-edit change record

Blocker: fixed mailbox-first selection can indefinitely postpone timers and
local results; observation-first selection can postpone startup results.
Smallest native regression: change the existing jointly-ready acquisition
campaign to require observation and timer before the next mailbox item.
Add sustained readiness, timer storm, source-only contention, delayed startup,
idle wakeup, cancellation and exact result-retention witnesses.

Expected files: environment.rs, effects/mod.rs, owning fairness test files,
this PRD, backlog README/status, runtime capability documentation. Expected
production delta: +150 / -45 / net +105; zero public types added or removed.
Reuse Communication Consumer, LocalTimers, TerminationObservations,
ActivationTasks, ActionInterpreter and the current retirement path. Delete
fixed ordering, without adding another runtime or public scheduling interface.

Model checkpoint: existing lifecycle states unchanged. Add only a private
three-way next-source preference and private two-way local preference. These
retain the next service entitlement required by the fairness law; they are
not lifecycle history. No effect/event/error alternatives or public spellings
change. Exhaustive matches and standard pinned futures implement selection.

Initial measurement: production +0/-0/net0; tests +0/-0/net0;
public API +0/-0 types. Verification and final measurements follow below.

## Regression checkpoint

The original production source fails both native outer regressions: the full
trace drains mailbox messages before Observation and Timer. Source inspection
also identifies a cooperative-budget edge: Communication and Observe do not
consume Tokio budget, while Sleep and JoinSet do. Wrap the two former receiving
futures in Tokio's existing cooperative adapter so exhausted budget cannot
let them bypass a ready cooperative sibling indefinitely. Sustained tests
exceed one Tokio budget. This adds no scheduling owner or public type.

## Research comparison (2026-10-14)

The user's research requirement reopened algorithm selection before production
edits. The objective is deterministic bounded *event acquisitions* at existing
safe scheduling points. Equal CPU time, weighted service, deadline feasibility,
and fair completion of model requests are different objectives.

| Candidate | What it establishes | Required information/mechanism | Disposition for this seam |
| --- | --- | --- | --- |
| Fixed priority | Priority overtaking only | Ordered ready sources | Rejected by the native flood trace. |
| Random select | Probabilistic opportunities | Random starting source | No finite deterministic overtaking bound. |
| Cyclic service, one event per visit | At most N−1 other acquisitions for a continuously ready source | Persistent next-source preference | Best fit for three fixed source families, subject to executable cancellation and cooperative-budget evidence. |
| Weighted/interleaved round-robin | Configured unequal opportunities | Explicit weights and a changed bound | No supplied source-weight policy; equal weights reduce to cyclic service. |
| Deficit round-robin | Approximate weighted resource sharing for variable-cost work | Quantums and charged service size | Valuable worker-admission candidate; no truthful cost is available before a Bombay event executes. Unit costs reduce to cyclic service. |
| Start-time fair queueing / EEVDF | Accounted service fairness; EEVDF also orders eligible work by virtual deadlines | Service accounting, weights, eligibility and executable quanta | Different contract. Post-event elapsed time conflates I/O waiting and CPU; an unbounded atomic turn still blocks everyone. |
| Earliest real deadline | Urgency under a selected schedulability model | Work deadlines, execution bounds and admission assumptions | A timer's due instant is not the completion deadline of every event. Strict deadline priority can starve other sources. |
| Controlled preemption | Interrupt long execution independently of voluntary yielding | Compiler, OS or hardware/runtime support and safety rules | Research-relevant for worker execution, not a small safe change to native Rust Behavior folds. |
| Prediction-based LLM fairness | Balance model-serving throughput and response objectives | Token/resource predictions and backend batch ownership | Belongs to the model service; does not select Bombay lifecycle events. |

Primary-source findings, with applicability limits:

1. Hähnle and Henrio, [Provably Fair Cooperative Scheduling](https://arxiv.org/pdf/2312.16977v1),
   Programming 2024, §§3.3 and 7, distinguish fairness at quiescent scheduling
   points from arbitrary program states. Their fairness theorem assumes recurring
   quiescent states, and queue rotation does not establish strong fairness for
   arbitrarily flickering guards. **Inference for Bombay:** formulate the promise
   at acquisition boundaries after required settlement, require continued
   progress, and promise bounded service only for continuously eligible sources.
   The paper does not itself prove this implementation or a millisecond bound.
2. Shreedhar and Varghese, [Efficient Fair Queuing Using Deficit Round-Robin](https://web.stanford.edu/class/ee384x/EE384X/papers/DRR.pdf),
   IEEE/ACM TON 1996, provides constant-work approximate fair queueing with
   variable service sizes. [Tabatabaee and Le Boudec's 2021 analysis](https://arxiv.org/abs/2106.01034)
   sharpens DRR service/delay bounds, including interference assumptions.
   **Inference:** deficit accounting is appropriate when the scarce resource
   and work charge are known; it adds no benefit for equal one-event credits.
3. The [Linux EEVDF documentation](https://docs.kernel.org/scheduler/sched-eevdf.html)
   describes service lag, eligibility, virtual deadlines and slice-based
   preemption. **Inference:** virtual deadlines are not wall-clock deadlines;
   copying that policy into a non-preemptible actor cannot provide its CPU
   scheduling benefits without supplying the missing accounting and mechanism.
4. Škoudlil, Sojka and Hanzálek, [ECRTS 2025](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ECRTS.2025.1),
   studies asynchronous Rust ROS 2 execution and evaluates explicit callback/
   thread mapping and priorities for time-critical work. **Inference:** an
   event selector alone is insufficient for response-time claims; execution
   placement and schedulability assumptions must also be controlled.
5. Guo et al., [NSDI 2025](https://www.usenix.org/conference/nsdi25/presentation/guo),
   investigates user-interrupt preemption and implements Aspen in kernel-bypass
   and Go runtimes. Xie et al., [PLOS 2025](https://nikos.vasilak.is/p/cocoon:plos:2025.pdf),
   sketches constrained hybrid cooperative/preemptive execution and explicitly
   describes its implementation as in progress. Hu et al., [SBB, OSDI 2026](https://www.usenix.org/conference/osdi26/presentation/hu-kang),
   separates request preemption, CPU allocation and multicore load balancing.
   **Inference:** these newer mechanisms operate below or beside this selector;
   adopting them requires a separate executor/platform project and evidence.
6. [Equinox, 2025 preprint](https://arxiv.org/abs/2508.16646),
   combines model-serving fairness objectives with predicted resource use.
   **Inference:** relevant to broadcaster model workers, but not a proven
   replacement for deterministic delivery of required control/results.
7. [Tokio 1.53.1 runtime contract](https://docs.rs/tokio/1.53.1/tokio/runtime/index.html)
   conditions eventual task scheduling on bounded task counts and bounded poll
   duration; it does not promise equal scheduling order. The official
   [cooperative adapter](https://docs.rs/tokio/latest/tokio/task/coop/fn.cooperative.html)
   accounts for non-Tokio futures. The locked source has the same adapter.
   **Inference:** Bombay must participate in the host budget. A custom always-ready
   receiver can otherwise keep winning after budget-aware sources return Pending.

## Selected design and proof obligations

Select hierarchical cyclic service plus host cooperative-budget participation. This is an application of the research
to the required service unit, not a claim that round-robin is a new algorithm
or universally the best scheduler. No paper justifies inventing weights,
cost estimates, deadline priorities or preemption inside Bombay's pure fold.

Hierarchy is intentional: mailbox and timers each get one third of acquisitions
when all three families are ready; the two local-result families split the
remaining third. Flattening all four would change those family shares. There
is no requirement for equal share among every individual observation or task.
Observation registration and timer deadline order remain their owners' policy.

Proof sketch: for N fixed source families, maintain the next starting position.
After a winner, advance past that winner, not merely past the previous starting
position. A continuously ready source cannot be skipped on inspection. Each
other winner before it strictly reduces the cyclic distance to it; therefore
there are at most N−1 other winners. Skipped pending sources receive no credit
and idle polling does not advance the cursor. Applying the same proof inside
the local selection gives the two-choice bound, including child transfers into
control. Child delivery then observes its finite FIFO prefix; it has no
queue-length-independent six-acquisition promise. This proof
requires honest readiness: budget exhaustion must park the entire selection
rather than let noncooperative sources bypass cooperative ones.

Action settlement is an eligibility restriction, not another ordinary source:
next_source admits only control/results and must obey the two-source bound. Its
separate cursor cannot reduce an ordinary source’s accumulated entitlement.
No fairness requirement can authorize a user event to cross D-SETTLE-1.
Pending operations retain their owners; returning one result ends polling
before another result can be consumed. Ready owner cancellation terminates
selection before these ordinary rules apply.

Validation compares exact native event traces, checks sustained readiness past
one executor budget, pending cancellation, delayed original tasks and idle
notifications. Tests must reject fixed priority and missing budget participation.
Queue age, timer lateness, actor-turn duration and worker duration remain distinct
measurements; there is no numeric latency/SLO claim without application load,
service bounds, capacity and host scheduling evidence.

## Tokio replacement comparison

The user authorizes any internal algorithm and permits a Tokio migration if it
simplifies the result. Public interface and its exact semantics remain the
compatibility target. Locked Tokio source was inspected for mpsc, oneshot,
watch, Sleep, JoinSet and cooperative budgeting. A replacement mailbox still
needs two lanes, the existing control aging policy, affine admission closure,
exact drain/rejected-payload recovery and user-lane end markers. A oneshot has
one receiver; watch provides change observation rather than the current shared
terminal-publication contract. Neither removes the need to arbitrate sources.
TimerQueue still owns replacement generations and stable deadline ordering.

The official cooperative adapter explicitly gives third-party receivers the
same budget participation as Tokio mpsc. Select that adapter for custom
mailbox and observation receiving; keep existing Tokio Sleep, JoinSet and
owner-cancellation oneshot. This achieves the relevant scheduling integration
without replacing already-correct payload/lifetime contracts. No allocation,
throughput or latency superiority over a wholesale migration is claimed.
A future replacement must win a measured contract-preserving comparison, not
merely use one package throughout.

Focused debug checkpoint: eight tests pass, including 256 ready events per
family and 512-event individual floods. Original native source fails the two
outer and one local alternation oracle; delayed-task custody is a passing
control. Initial full-debug compilation ran out of disk; package build artifacts
were cleaned and checks now disable debug symbols and incremental compilation
while preserving debug assertions. Optimized checks remain separate.

## Implementation checkpoint

Eight focused tests pass in both debug and optimized builds. Complete working
tree (including untracked files):

```text
production: +144 / -42 / net +102
tests:      +469 / -45 / net +424
public API: +0 types / -0 types
files: 7
```

The retained model has the same lifecycle states and two new private preference
sums (3 and 2 alternatives), one three-arm order match and one two-arm local
arbitration match. No public protocol, runtime owner, generic trait or module
is added to production. Each preference stores the next service entitlement.
Disposition: pass; original fixed priority is deleted and complete native
payload traces pass. This is new scheduling capability, not code reduction.

First test review maps the native traces to bounded service, exact FIFO
custody, source-only eligibility, idle notifications and original task survival.
Second review checks vacuity: campaigns exceed Tokio's budget, finite floods
still assert the bound before queues empty, all original payloads are compared,
and consumed task/observation/timer results are polled again to exclude replay.
All mutations/actions are outside assertions. Intentional source inversions
remain the final evidence gate. Review identified mailbox-wakeup and pending
source-cancellation coverage to strengthen before acceptance.

Expanded strict Clippy with `-D clippy::let_underscore_must_use` finds existing
`let _ = invoke` in application/http.rs (both unchanged). This is not a fairness
regression and no unrelated production cleanup is included. Standard required
Clippy and scoped extra-lint checks are recorded separately.

The strengthened mailbox-wakeup test initially cancelled its receiving future
before sending, then incorrectly required that cancelled registration to wake.
The workspace run rejected that test. The corrected witness keeps the receiver
alive through notification, cancels it before acquisition, and checks exact
payload retention when owner cancellation wins. This was a test-oracle defect,
not a production change; it was resolved in the second review.

## Standard mailbox fairness — additional blocker

The first source-selection stage passes debug/optimized controls; its replay
campaign kills original source priority and missing host-budget participation.
The user's interface-only constraint prompted tracing ordinary construction:
Application execution, child creation and Entity all choose Config::new without
aging, and ordinary application users cannot supply Config. The earlier text
about callers opting into aging therefore cannot deliver ordinary fairness.

Smallest new native regression: queue 256 controls and 256 users through the
existing prepared/active environment and require alternating complete payloads.
The old standard environment drains controls first. Use Communication's existing
with_aging_cap(1) at the one private LocalEnvironment mailbox construction, which
preserves capacity and applies to roots, children and Entity without new API.
No caller in the repository currently supplies a custom aging cap. This is an
explicit Bombay policy change: during ordinary acquisition a waiting user may
follow one control before a later queued ShutdownRequested; owner cancellation
still wins first, and next_source still excludes users entirely. Existing
admission closure and required settlement/lifecycle ordering do not change.

Expected additional production delta: one changed construction plus its law
comment (under +5 net), no public types/files. New test and inversion stay in
the current owning targets. The selected Communication source's recv_control
explicitly leaves aging untouched and never consumes a user, confirming that
this change cannot bypass action settlement. Original source failure must be
recorded before retaining the production change.

The observation-budget inversion survived the initial alternation test because
startup joins themselves consumed budget. That oracle did not prove an
observation-only flood yields. A separate 512-observation single-poll witness
now covers that counterexample; it compares every original observation after
resumption. Surviving mutants are not credited as evidence.

## Settlement interleaving review

Second review found a counterexample to sharing the ordinary cursor with
control-only settlement. Repeated control settlement can reset ordinary
service to Local before each next call and indefinitely postpone a due timer.
The smallest native regression interleaves ordinary acquisition with one
control-only acquisition and requires the timer within three ordinary calls.
Before the production correction, establish that it fails.

Retain separate entitlements for the distinct eligibility sets: the existing
three-source ordinary cursor and a private two-source settlement cursor. This
adds one field and a two-alternative private sum, removes the unreachable timer
settlement arm, and changes no public type or causal eligibility. Expected
additional production delta under +25 net; same files and owners.

The interleaving regression fails on the shared-cursor implementation with an
observed Mailbox, Observation, Observation trace; the due Timer is absent. The
corrected trace is Mailbox, Observation, Timer while all admitted controls still
settle. The inversion campaign separately reintroduces settlement resetting
ordinary preference, and requires this exact native witness to fail.

Checkpoint after the interleaving correction (all tracked and untracked files):

```text
production: +161 / -42 / net +119
tests:      +737 / -46 / net +691
public API: +0 types / -0 types
files: 8
```

Production retains three private preference sums: ordinary service (three
alternatives), source settlement (two), and local results (two). Each stores a
different service entitlement proved necessary by an independent counterexample;
none duplicates lifecycle state or adds an owner. Separate concrete polling
keeps the eligibility sets exhaustive and removes the unreachable timer arm
from settlement. A shared generic selector would add machinery without deleting
a semantic owner or public caller burden, so none is introduced.

The repository-wide source/document/example/test/research search found no
remaining ordinary-selector priority promise or references to the renamed
starvation witnesses. Retained biased selects in production are the two
rotating local arms; other matches occur in test controllers. Driver law already
assigns scheduling to the incarnation/executor owners and requires no change.
No public example, benchmark, macro expansion or re-export changes are needed.

The two test reviews are manual in this session. Review one maps the full native
traces to source bounds, mailbox lane bounds, independent settlement entitlement,
original task/payload custody, cancellation, and host yielding. Review two
checks counterexamples and vacuity: each finite flood exceeds the executor
budget, service is checked before depletion, the idle receiver stays registered
until notification, and exact repeated reads exclude replay. The surviving
observation-budget mutation and shared-cursor counterexample above were resolved
before acceptance; no surviving inversion is credited as a pass.

## Child result ordering — integration blocker

Full workspace verification rejects the initial fair selector in the existing
fifo_pool_recovery held-source and source-panic witnesses. ChildStopped can
overtake an already-admitted worker completion on the parent's control lane;
the pool correctly applies its stop-first interruption policy, and the original
completion is no longer delivered as a successful job result. Nextest also
rejects source_refusal_custody for the same ordering investigation. These are
acceptance failures, not baseline failures or tests to weaken.

The smallest existing end-to-end regression is
shutdown_while_worker_source_is_held_avoids_replacement. Preserve the distinction
between peer observations and child lifecycle facts until their delivery policy
is chosen. Route ChildStopped through the existing parent control FIFO, after
its already-admitted completion reports. Keep peer observations local. This
expresses the causal edge without draining a potentially endless mailbox ahead
of every local event. Acceptance remains distinct from Behavior completion.

Fresh source check: selected Communication ControlSender::send linearizes
control publications in ticket order; Consumer pops that order. Bombay's
ReportToParent and assignment-completion interpreters publish into that same
parent control capability before child termination is published. The selected
Behavior Actors pool explicitly distinguishes completion-first from stop-first
assignment outcomes; fairness may not erase that causal dependency.

Expected additional files: effects/observation.rs (one production owner and its
own tests), existing effects/fairness.rs. Expected additional production delta
under +70 net; zero public types. A private TerminationEvent sum retains the
provenance that the former generic E return erased before routing. Its child
case transfers to the existing control FIFO; if that FIFO has closed, recover
the exact event into local receiving rather than erase it. No new queue, task,
mailbox, runtime, or dependency patch is introduced.

The corrected causal routing passes all six existing fifo_pool_recovery and
source_refusal_custody integration tests in debug mode, without changing their
oracles. Added owning witnesses check that polling and cancelling local
receiving leaves the report followed by the exact ChildStopped in control,
with no duplicate; closed control recovers the original move-only allocation.
The new provenance sum is visible only to its owning effects module.

Pre-correction full workspace/Nextest results are retained as failures: 330
library tests passed, Nextest ran 678/700 with 675 passing and three causal-order
failures. Standard workspace Clippy and formatting passed. The extra
let_underscore_must_use lint also reports unchanged application/execution.rs222
(in addition to the HTTP cases noted earlier). Nix flake verification was
interrupted once the integration failures required a new production revision;
it is not credited as passing. Final checks must use the corrected revision.

## Review checkpoint after causal correction

Complete tracked/untracked change, before submission for CI:

```text
production: +192 / -44 / net +148
tests:      +849 / -52 / net +797
public API: +0 types / -0 types
files: 9
```

The fourth private sum preserves Peer versus Child provenance until delivery.
It implements the demonstrated FIFO transformation using the existing control
capability; it adds no mailbox or public wrapper. The other three sums retain
independent next-source entitlements. The child integration regressions and the
new direct control trace independently establish why that distinction belongs
in this owner. Debug focused suite: 13/13 passing. Corrected full gates and
optimized defect replay remain in progress; this is not merged or distilled.

Historical reproduction used the research runner retained in Git history.
Debug symbols and incremental artifacts were disabled to fit local storage;
debug assertions remained enabled. Git preserves the exact retired commands.

That runner copied tracked and untracked project sources into an isolated
temporary checkout, verified a passing control, restored one concrete defect
at a time, required the named test to fail (a compilation failure was rejected),
and verified the restored suite. JSON receipts retain source hashes, exact
commands and exits. It did not mutate the live checkout. Its retirement and
the current native test commands are recorded below.

## Adversarial test audit requested by the user

The user challenged whether the tests were being tailored to the implementation.
The audit identified a material gap: direct ActiveLocalEnvironment and capability
receiving tests establish source contracts but do not alone prove execution
through the actual Driver/actor loop. The pool ordering failures demonstrate
why that distinction matters. No existing pool or result-custody expectation
was changed to accommodate those failures.

Added public-API event_fairness integration evidence. A current-thread host
synchronously admits Arm followed by every original mailbox payload before
yielding to the actor. The pure Arm fold requests already-due timers through
typed Actions; it performs no I/O or clock read. After complete action
settlement, both finite backlogs remain continuously ready until depleted.
The actual returned actor retains the full observable service trace, every
original move-only allocation, and every exact timer identity/generation.

The independent oracle measures positions and gaps in that completed trace,
not the selector's cursor or an expected rotation string. It checks both
mailbox and timer bounds for balanced load, mailbox flood, and timer flood,
and exact complete payload traces after retirement. Timeouts only guard test
hangs; eventual completion alone cannot satisfy the bound.

Debug and optimized replay against the exact original production revision
406df116dd081593e1099a1f3301d2fb91bfc4fc fails because the ready timer misses its
first service bound. Independently restoring mailbox-first and timer-first
priority each fails the corresponding source bound; restoring the candidate
passes. The original three changed production files are restored together, so
this is not merely a hand-picked approximation of the old implementation.

The direct-source tests retain narrower claims: all three outer sources and
both local sources, settlement eligibility, pending cancellation, exact task
custody and cooperative yielding. They complement the public actor trace; no
single test is presented as proving all runtime or elapsed-time fairness.
Finite backlogs assert the bound while ready, not after depletion. The two
old mailbox-priority characterization tests changed because their former
assertions described the defect explicitly requested for removal. Their
original production failures were recorded before the fix.

Git history preserves the exact retired commands for the independent public
witness and original-source replay; their receipts and source epoch remain
recorded here.

Both reviews of the public witness are complete. The first checked the causal
setup: synchronous admissions on a current-thread host precede the first actor
turn, and typed timer actions settle before the measured trace starts. The
second checked counterexamples: the oracle accepts any schedule satisfying the
bound, demands all original payloads and generations, and fails on the actual
old production sources and on either fixed priority. It does not infer fairness
from a timeout or require a particular rotation string. These were manual
reviews; no independent reviewer is claimed.

## Historical verification after the test audit

All commands use `nix develop -c`, with `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0` and `CARGO_INCREMENTAL=0` for local storage limits.
Debug assertions remain enabled. Results below cover the causal correction and
the public actor witness, rather than the rejected initial implementation.

| Verification | Result |
| --- | --- |
| Focused library fairness suite | 13 passed in debug and optimized builds. |
| Ten owning-source defect inversions | Each named regression fails in both profiles; restored controls pass. Compilation failure is not accepted as a killed inversion. |
| Public actor integration | Balanced readiness, mailbox flood and timer flood pass in both profiles with complete original payload custody. |
| Public original-source replay | Exact original three production files fail the timer bound in both profiles; mailbox-first and timer-first inversions also fail the intended source bound. Restored controls pass. |
| Existing fifo_pool_recovery and source_refusal_custody | All six pass in both profiles, with their original assertions unchanged. |
| `cargo build --locked --workspace` | Passed. |
| `cargo nextest run --locked --workspace` | 703 passed, one skipped; Nextest additionally flags the unchanged Engine cancellation witness as leaky. This is not a clean leak report. |
| `cargo clippy --locked --workspace --all-targets -- -D warnings` | Passed. |
| `cargo fmt --all -- --check` | Passed. |
| Additional required mutation/unused-result lints | The additional unused-result denial rejects unchanged application/execution.rs:222; earlier HTTP findings are also retained above. Standard Clippy passing does not waive those findings. |
| `cargo test --locked --workspace` | Two full runs stop at the unchanged HTTP staging witness. Passing Nextest does not erase either failure. |
| `cargo test --locked --workspace --no-fail-fast` | Passed all targets and documentation tests. This run was requested to execute targets after the earlier HTTP failure; its pass does not erase the intermittent failures. |

The HTTP failure was investigated with the complete seven-test Axum suite,
using `cargo test --locked -p bombay-rs --features axum --test axum` for 20 runs
on each revision. The exact original revision fails twice and the candidate
once, each at `HTTP work and original cleanup return together`. An earlier
20-per-revision campaign running only that test did not reproduce it. This
establishes a pre-existing intermittent failure; it does not establish that the
patch leaves its probability unchanged. No assertion, timeout or retry policy
was weakened. Comparison logs and receipts are in
`/tmp/bombay-http-suite-comparison/` and `/tmp/bombay-http-comparison/`.

Historical submission was [draft PR #366](https://github.com/devrandom-labs/bombay/pull/366).
At production revision `73f53ed`, native TLS release, CodeQL and cargo-deny pass;
Nix flake verification is still running. Native TLS debug fails on a scheduled
query with closed reply receiving and a recorded publication timeout. The
[original-revision CI](https://github.com/devrandom-labs/bombay/actions/runs/38609035631)
also fails native TLS on closed reply receiving, but in different campaign
cases. That is evidence of an existing failure family, not proof that every
new failure is unrelated. The late-exit `-9` records are expected negative
controls, not CI failures. At that checkpoint these gates remained unresolved;
the PR was not ready to merge and FAIR1 remained active.

Detailed local receipts are in `/tmp/bombay-fairness-causal-{debug,release}/`
and `/tmp/bombay-fairness-public-{debug,release}/`. They retain exact commands,
source hashes, exits and individual failure transcripts. The historical runner
is retained in Git history at `crates/bombay/tests/fairness_evidence.py`;
it is not a current test or CI entrypoint.

Complete tracked and untracked scope relative to `406df116` after this audit:

```text
production: +192 / -44 / net +148
tests:      +1086 / -52 / net +1034
public API: +0 types / -0 types
files: 10
```

The audit adds one public-API integration test and extends defect replay; it
does not add further production code. Broadcaster overload and latency policy,
HTTP lifecycle repair and native transport investigation remain separate work.

## Delivery integration — 2026-10-14

The user explicitly retains this fairness contribution as important delivery
work. Coordinator isolated branch fix/event-fairness-sdk-adoption starts from
submitted6f272ec7; the author worktree remains untouched. It integrates actual
mainc1d5b2bd through91efeb9, inheriting only release versions/locks/changelogs and
the release record. No fairness source/test policy is changed by integration.

The existing native debug failure still selects SDK1.10.8/TLS1.10.2. Controlled
TLS correction PR12 merged1ea4bd16 after reviewed passing CI; exact-main
run38631426153 and independent auditd7e7365a pass. Guarded five-package publication
run38638728105 succeeded; independent allnine registry archive review passes. Neither source-path probes nor publication intent are
registry-only SDK adoption proof.

Before edits, under standing recommendation delegation select the same narrow
SDK adoption as the static-remote coordinator: change only the two SDK manifest
pins to published1.10.9 and replace five controlled package records in each lock;
conserve all394/319 unrelated complete records. Retain Config1.10.2/protocol1.10.1,
features, all fairness code and original tests. The private publish=false fixture
uses the separately reviewed path-only Bombay dependency to survive future local
version bumps; its other pins stay exact. Four additional existing manifest/lock
paths bring this contribution from ten to fourteen paths, production Rust/API0
added by the coordinator. No scheduling/default-resource policy is reopened.
Combined local verification, exact-head CI and final independent review precede
normal guarded PR366 merge. Historical failures and all full remote obligations
remain visible; no PR is marked delivered from this integration or an open run.

Independent source review finds one observational-assertion violation in the
public actor-loop witness: termination.unwrap() consumes an acquired result
inside assert_eq before cleanup/native receipts are acquired. Before editing
select the minimal test-only correction: bind that actual result outside the
assertion, then compare it by borrow after cleanup/notifications/native result
acquisition. No scheduler policy, expectation or producer changes. Expected
one existing test path, at most+2 net test lines, production/API0. This retains
the native trace law and avoids an early assertion skipping known cleanup.

Current-document reconciliation before adoption: acceptance.md still calls the
old1.10.8 checkpoint current and the corrected owning minimum pending. Update
that document to identify the old checkpoint as historical and retain actual
SDK publication/registry proof beside open combined-PR gates. One additional
document path brings hosting/adoption to eleven paths and fairness/adoption to
fifteen; production Rust/public types remain0. This documentation scope is
selected under standing recommendation delegation before editing.

Fresh registry adoption prerequisite passes independent consumer and all-four
owning audits: twelve actual consumer runs plus112 exact Query/Config cases.
Review556699b5 retains source/archive/lock/profile/corpus custody and limitations.
Both branches now have the same four shared manifest/lock edits; five selected
package records per lock and all394/319 unrelated records conserved. Cargo
locked/offline metadata validates the four restored original Windows edges in
the root lock. Combined focused/build/lint/native gates remain unexecuted here;
no feature or PR is marked merged by this dependency adoption.

Fresh adoption controls pass through pinned Nix in both debug and optimized:
13 owning fairness tests, one actual public actor, five FIFO recovery cases and
one source-refusal custody case perprofile. Logs and actual commands are retained
in fairness-delivery-review/adopted-focused-receipt.json. Existing34 source-cut
executions remain earlier evidence; no new cuts or copied test binaries claimed.
Final ownership/lock reviewaa427163 passes. Full combined checks and CI remain
required before merge. The coordinator will include this reviewed contribution
in hosting for parallel CI; native fairness delivery stays owned by PR366.

### HTTP fixture binding correction — selection before retained edit

Combined remote workspace run exits101 at the existing HTTP staging/retry
witness; retain its raw failure. Source review proves the fixture discards its
listener, then treats the derived ephemeral port as reserved. Current API
promises original request/input custody, not reservation after disposal.

Smallest concrete failing witness retains an actual bound listener and reverts
only the retry argument to its occupied address: exact named test fails in debug
and optimized. Separate diagnostics observe native StagingRejected/AddrInUse.
The retained candidate reuses original requested127.0.0.1:0 and keeps the returned
listener through retry. All original actor/router/scope/native/notification
assertions remain byte-identical; no additional bind, retry loop or deadline.
Existing ApplicationScope, HTTP execution and Tokio listener ownership suffice.

Expected retained edit: axum.rs+5/-3/net2 testlines; production Rust0/API0. One
additional path gives fairness16 and combined20, authorized under standing
recommendation delegation before this edit. Paired inversions/controls and
full seven-test Axum suites pass both profiles with explicitremote,axum. Two
source/semantic and execution/vacuity reviews7db05f1e/63eb940a pass.

Earlier20 diagnostics plus first occupied-port invocation ran ZERO tests due
to missingaxum; they receive no runtime credit. All raw failures are preserved.
The initial uncontrolled full-workspace failure's phase remains unclassified;
this corrects a separately proven fixture race, not all HTTP lifecycle behavior.
Combined source checks and fresh updated-head CI remain required before merge.

Retained sourceceed8bd1 passes exact paired controls and both seven-test Axum
suites. Minimal inverse0bac14b7 changes only the retry argument and fails the
intended completion oracle in both profiles; source reversal restores every
original assertion. Combined no-fail-fast remote run is in progress; earlier
failed full run remains recorded, not converted to a pass. New submitted heads
must rerun required CI; prior26b/24c native pairs pass independently but do not
qualify the changed head. Current retained HTTP delta is test+5/-3/net2;
production/API0 added, combined20paths/fairness16.

## Reviewed delivery

The accepted FAIR1 scope was distilled and merged through PR366 at
2026-10-15T00:00:40Z. All six checks passed on submitted
`cec6240a44d8ac90fe70cfc90f87612136c61d54`, including
[required CI38650631535](https://github.com/devrandom-labs/bombay/actions/runs/38650631535).
The merge tree is exactly the reviewed submitted tree
`52364b3709ef6f370bbf67e8c7d5fbf28429c5ce`; no direct main push or bypass was used.

Independent source review `a4d780a3` confirms the retained private service
entitlements reuse existing runtime owners, preserve child control FIFO and
original facts, and add no public API. Exact-head native review `d7f0a83b`
and Nix review `df31b358` pass. Nix actually executes 703 passing tests with
one ignored test in each optimized-workspace and debug-coverage campaign,
including all 13 owning fairness cases and the public actor witness. Driver
2048 and four Observe 1024 fuzz campaigns complete. Conditional Miri steps
are skipped and receive no credit. Native late-exit uncertainty stays explicit.

The corrected combined remote workspace run passes 732 tests with four ignored;
the original failed run remains retained. Both complete seven-test Axum suites
pass; the staging case's one-argument inversion fails in both profiles, as
recorded separately above.
The original uncontrolled HTTP failure remains unclassified.

As merged against `c1d5b2b`, the complete 16-path contribution retains production
Rust +192/-44/net148, test source +1092/-55/net1037 and public types +0/-0.
Delivery-record edits are documentation only. PERF-07 overload and elapsed-time
latency policy, hosting PR365 and full remote acceptance are separate scopes;
this merge does not complete them.

## Research runner retirement — selection before deletion, 2026-10-23

The unused `crates/bombay/tests/fairness_evidence.py` reproducer is selected
for direct deletion. Its only callers were the retired historical commands;
neither the flake, workflows nor another executable invokes it. Do not translate
it into a second runner. Expected source delta: production +0/-0, tests
+0/-144, public types +0/-0; this document records the disposition separately.
No scheduler, actor, native test, dependency, CI command or gate changes.

The existing 13 owning tests remain: eight in `local/environment.rs` and five
in `local/effects/fairness.rs`. The separate public `event_fairness` target
retains the actual actor trace and every original payload. Current native
controls must run through the pinned shell:

```sh
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --lib fairness_
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --lib fairness_
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --test event_fairness actual_actor_bounds_both_sources_and_returns_every_original_payload -- --exact
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --test event_fairness actual_actor_bounds_both_sources_and_returns_every_original_payload -- --exact
```

Expected selections are 13 library tests and one public test per profile.
These are commands selected for retirement verification, not new passing
receipts. Execution results must be recorded before accepting the deletion.

The historical negative receipts above remain evidence for their recorded
source and dependency epochs. The deleted reproducer remains available from
Git; deletion does not claim a new execution of those mutations on today's
dependency graph. Its ten owning cuts cover child-stop provenance, closed-child
fact custody, independent settlement selection, mailbox aging, mailbox/local/
settlement priorities, and cooperative mailbox/observation/settlement polls.
Its two public cuts force mailbox or timer priority. The optional original
three-file replay used revision `406df116`; that historical source is not a
current dependency-compatible replacement.

No fairness mutation campaign was wired into current CI. Deletion retires its
manual reproducer; it does not claim that the separate actor-execution shell
runner executes these cuts. The merged FAIR1 semantics and their historical
inversions remain unchanged. Full overload and elapsed-time policy stay open.

### Fresh retirement selection (2026-10-24)

This direct deletion is reselected on actual main277e6dfe, separately from
the remote pressure implementation. The earlier outside proposal/review at
68d73354 with root lockaf4406db is historical; current root lock83f01ca3
selects patched Tokio1.53.3. Behavior/Actors0.29 at55dbac2d, Macros0.16,
Address0.3/9f058dc0, Communication0.1.6/963067e0, Timers0.2/9369d1f6 and
local Observe remain the selected contracts, with no patch override or owner
changes. Full selected Behavior instructions and relevant owner APIs/tests/docs
are verified for this stage. No dependency or ownership blocker remains.

The smallest scope remains the144-line unused manual reproducer, this owning
PRD's historical/current-command distinction and the reciprocal status record.
Expected production+0/-0/net0, tests+0/-144/net-144, publicAPI+0types/-0types,
three existing paths. No Rust body, Nix gate, manifest, lock or workflow changes.
Reuse Cargo and the existing native tests directly; no replacement tool.
Keep historical inversions at their recorded epochs; do not imply a fresh
ten-plus-two mutation campaign, actor-execution substitution or full PERF-07.

Before deletion, actual public controls run on3e1a5ab7 through the sole pinned
Nix shell. Their seven-input ledger rehashes successfully after switching to
main277e6dfe: all fairness production/test owners and current lock are exact.
The recent complete library425/425 runs on3e1a5ab7 include all13 named fairness
cases in each profile. That head adds only an unrelated private Client test
and feature records to main; it changes no production or fairness source.
Check those named cases and exact source equivalence rather than duplicate
an already passing native suite. Public controls must select exactly one
named case per profile. Record actual exits, independent reviews, complete
delta and required new-head CI/main delivery after this selection.

### Direct retirement — actual controls and containment

The unused144-line reproducer is deleted directly. No executable, flake gate,
workflow or other source invoked it; the only project references now explain
its historical Git location and this retirement selection. The four former
Python commands in two fenced blocks are removed. Historical receipts and the exact script
remain available through Git; this change adds no replacement controller.

The current full library runs on3e1a5ab7 pass425 tests/profile without filtering
or ignored tests. Their raw logs independently contain all13 distinct fairness
cases (eight environment, five effects). The actual commands are the full
library selections recorded by the private remote-pressure stage, rather than
new narrow13-case invocations. Comparing that head with main277e6dfe confirms
every production/fairness source, manifest, lock and selected owner is exact;
only an unrelated private Client test and feature records differ.

The actual public `event_fairness` commands above execute exactly the named
one-passed case in debug and optimized, exit0 each,0failed/0ignored/0filtered,
0.01/0.00seconds. Outer script exit0 and the seven original source checks pass.
After deletion, all six retained input checks still pass. Current native
results therefore qualify the retained fairness owners on lock83/Tokio1.53.3.
The old ten-plus-two source inversions are not rerun or relabelled. Deleting
an unused tool adds no semantic law needing a new inverse; the unchanged laws
retain their meaningful original-defect evidence at its documented scope.

Complete current scope is three existing paths: production+0/-0/net0,
tests+0/-144/net-144, publicAPI+0types/-0types, plus this existing PRD and status
record. No Rust body, dependency, manifest, Nix filter/gate, workflow, module,
aggregate state or branch changes. Source review and minimization prefer direct
deletion over a translated framework. Existing FAIR1 delivery remains merged;
this maintenance slice still requires its own reviewed PR, required CI and
actual main merge. Full PERF-07 and remote acceptance remain open.

Fresh independent source/outcome review3684be7c and final-record amendment
fc8108f3 pass both review rounds with no scoped veto. Root review6474967c
concurs. The amendment resolves the inherited advisory status and accurately
counts four retired Python commands in two fenced blocks. Exact original,
retained-input and actual public/library receipts are preserved outside under
`/tmp/bombay-prd-resumption/fairness-runner-retirement-20261024`.

Qualified archive `fairness-retirement-qualified-20261024.tar.gz`, SHA256
`2185580eeee5f965fca3324932eeff0932bd1fc65f4a25ef9b0cef6c7a93c087`,
is in the common Git session archive for2026-10-21. Fresh extraction passes
all24 payload checks and base277 plus the actual patch reconstructs both
records, the unchanged public test and Python absence. The actual3e1 library
control source is retained separately. This later pointer is outside the
capture; the archive is not a compiler closure or CI/main delivery certificate.

### Integration after provider delivery

PR394 has now delivered as actual mainb45c7566 after all six checks passed on
ed72df92; verified/merge trees match20b77b91. This retirement integrates that
main before its own PR submission. Only the shared status record conflicts:
retain the delivered provider/R10 records, current fairness disposition and
accurate historical source scopes. Main's provider UI fixtures and acceptance
records are preserved; no retirement Rust, dependency or Nix change appears.
The six retained fairness inputs rehash unchanged. The complete retirement
delta against new main remains the same three paths, verification0/-144 and
production/API0. Existing native receipts retain their original3e1/277 scope;
source equivalence qualifies the unchanged fairness owners without relabelling
those runs as execution of this new integration head. Fresh required CI and
actual retirement delivery remain pending.
