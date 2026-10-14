# Local event fairness

Status: active verification. Branch: `fix/event-fairness`, based on current `main`.
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
No losing branch may consume an event. Original startup tasks remain owned by
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

## Fresh locked-contract verification

Cargo.lock selects registry packages, with no workspace patch entries:

| Owner | Version / archive revision | Inspected contract and witnesses |
| --- | --- | --- |
| Behavior and Actors | 0.25.0 / 2bb4510077bfa5aed82e08d25453c146b3184285 | Complete revision AGENTS.md; public algebra, source_settlement_admission, runtime_contracts, timer_action_settlement; pure typed Actions and source custody remain unchanged. |
| Behavior Macros | 0.14.1 / locked archive | No macro changes or new authoring seam. |
| Address | 0.3.0 / 9f058dc03d1239e1ef5c3147a893b134a1e74a83 | lib.rs and reservations tests; publication/lease retirement unchanged. |
| Communication | 0.1.5 / 24ad2b9db9f5fefd08d6d2441f84c804b4c84338 | Config, Consumer::recv/recv_control, cancellation and strict-default edge tests; existing aging cap supplies Bombay’s standard lane policy. |
| Observe | workspace-private selected source | ObservationFuture::poll, completion registration, cancellation/wakeup tests; TerminationObservations retains pending futures and removes only the returned observation. |
| Timers | 0.2.0 / 9369d1f6c7fcda91e7de5bf3c19e6769f92145bd | TimerQueue schedule/next_deadline/pop_due and semantics tests; existing generation/order ownership retained. |
| Tokio | 1.53.1 | Select cancellation safety and JoinSet; original task handles remain outside selection futures. |

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

Reproduce the evidence through the pinned shell (debug symbols and incremental
artifacts are disabled to fit local storage; debug assertions remain enabled):

```sh
nix develop -c env CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 python3 crates/bombay/tests/fairness_evidence.py --profile debug --output /tmp/bombay-fairness-debug
nix develop -c env CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 python3 crates/bombay/tests/fairness_evidence.py --profile release --output /tmp/bombay-fairness-release
```

The runner copies tracked and untracked project sources into an isolated
temporary checkout, verifies a passing control, restores one concrete defect
at a time, requires the named test to fail (a compilation failure is rejected),
and verifies the restored suite. JSON receipts retain source hashes, exact
commands and exits. It never mutates the live checkout.
