# Observation outcome disposal

Status: distilled; local acceptance and independent minimization pass.
Source baseline cbaa62a56aceecd71abb46f5ffbbcd6ebd1dcdfc. PR397 has
since merged as9ea5defe; its delivered tree is a separate source cut. PR402 and
the remaining remote deliveries are tracked separately; their inherited source
is not a semantic dependency of this defect. This repair is not delivered yet.

## Pre-edit contract and failing-regression plan

An unobserved completed outcome can enter Observe's existing slot pool. On
reuse, reset currently calls its destructor before clearing OUTCOME_VALID.
If destruction panics, the last pooled Arc unwinds through Slot::drop and may
destroy the same outcome again. This is a source-derived hypothesis until the
original implementation fails the focused regression. No normal actor-runtime
remote failure has been observed: ordinary execution uses unkeyed pairs.

The retained keyed ObservationSpace contract requires safe generation reuse
(runtime-capability-interfaces.md552–627). Keep that owner. LAW-05, LAW-06 and
LAW-18 supply related generation, conservation and lifetime obligations; this
one regression does not close those complete cross-stack properties.

Fresh selected prerequisites are verified before editing: root lock83f01ca3,
Behavior/Actors0.29 at55dbac2d, macros0.16, Address0.3/9f058dc0,
Communication0.1.6/963067e0, Timers0.2/9369d1f6 and local Observe40fb7d44.
triomphe0.1.16 owns last-Arc destruction; parking_lot0.12.5 owns ordinary locks;
Loom0.7.2 models the existing synchronization. There are no patch overrides or
new dependencies. Fresh neighbor source/API/docs/test reviewad7385ee and its
19-input ledger17c3e775 have no unresolved contract or consumer blocker. Root
reads the complete selected Behavior instructions and Observe implementation.

The regression uses the real public namespace: complete one original Subject,
retire it without an observer, catch the next same-key registration's disposal
panic, then dispose a subsequent registration and the namespace. A private
probe holds only a borrowed AtomicUsize and panics on its first destruction;
it owns no resource that could be double-freed by the original defect. Keep
the subsequent registration's typed Result through cleanup. Require count0
before reuse, the actual destructor panic, successful later registration and
counts(1,1) after reuse/namespace disposal. Source predicts(2,2); compiler,
setup, timeout or zero-selection failure supplies no regression credit.
Author reviews03627587 and independentbe6db51c qualify this source-only probe.

Before production edits, execute that exact law in debug and optimized through
the sole pinned Nix runner. Root selects all features to retain the current
build context. Freeze the actual original failure and inputs. Production repair
is unselected until both intended count failures occur. Reuse the existing
validity word, outcome destructor, pool, Arc and namespace; introduce no new
state/type/observer/runtime, error policy, generation system or macro.

Expected scope: existing observe/mod.rs and external_tests/pool.rs, this PRD,
backlog status and its README link. Production estimate at most10 lines before
actual measurement, public types+0/-0; one private probe and one owning test.
Numerical budgets are removed; record complete tracked/untracked deltas and
keep unrelated audit/refactor findings separate. No Python or new tooling.

Acceptance requires original intended debug/optimized failure; repaired exact
positives; owning pool/panic/shared/affine regression coverage; exact original-
ordering inverse and restoration; relevant existing Loom/fuzz/Miri checks;
formatting and strict Clippy. Review the test and actual outcomes twice,
including an independent review, then minimize and record the complete delta.
Required new-head CI, focused PR and normal reviewed main merge remain delivery
gates. Broader remote acceptance and the three ongoing body audits remain open.

## Original defect and repair selection

The real original40fb7d44 with formatted owning test69e95d9a fails in both
profiles: exactly one selected case,0passed/1failed/426filtered, exit101.
The first user destructor panic at pool.rs474 is caught as intended; the
only uncaught failure is the final507 disposal-count oracle, actual(2,2)
versus required(1,1), after the replacement Result and namespace cleanup.
Debug/optimized compilation is40.09seconds/3m26s; execution0.00seconds each.
Logs f0125701/650e91c0 and independent paired review46d41626 preserve the
actual epoch. All four selected input hashes match. No production fix was
present during either baseline run.

Select one existing-owner change: clear only OUTCOME_VALID with a Relaxed
store immediately before its owned outcome destructor. A pooled slot has no
live observation/publication access, so this claims disposal before user Drop
can unwind. A retiring Subject reference can briefly remain; the selected Arc
Release decrement/final Acquire fence publishes the claim to its final drop.
Keep the existing final Release pending-state store and waiter-drain order.
The same Slot::drop then cannot acquire disposal a second time. Clarify the
private unsafe precondition and existing public destructor-panic behavior.
Outside candidatefb63b2ab changes8production lines and removes6, net+2.
Review4aed049d requires comment-only marker/exclusivity refinements before
retention; no new unsafe operation, public type, signature, branch or owner.

Aggregate checkpoint: the same completion/waiter/validity word, three existing
flags, published/taken/pooled ownership and generation source remain. No state,
alternative, arrival history, semantic Boolean, structural caller syntax or
second transition authority is added. The existing no-live-access invariant supplies
exclusive disposal; the change moves its claim before external destructor
execution. Preserve the original panic instead of manufacturing a report.

The paired original run already supplies the original-defect counterfactual.
Compare it with the frozen repair and fresh repaired results; do not relabel
the original logs as a later mutation run. An additional mutation replay is
needed only if subsequent semantic changes introduce a different causal gap.

Refined outside candidate0e63b34d includes those ownership-marker comments:
production+20/-18/net2, one executable store, zero new unsafe operations or
interfaces. Comment precision exceeds the initial authored-line estimate;
numerical budgets remain removed. Source tests+51/-0; before production apply
the four-path tree has production0, tests51, records with the full prerecord.

## Repaired execution

Actual source0e63b34d and the same test69e95d9a pass the exact focused command
in both profiles:1passed/0failed/0ignored/426filtered,0.00seconds each,
exit0. The intended user-destructor panic is still caught; original outcomes
are disposed once and subsequent registration/namespace cleanup succeeds.
Debug/optimized compilation is39.80seconds/3m24s. Logs4d1ff014/2d983dbf
remain separate from the original paired failures. All four inputs match.

Fresh full owning library tests then pass427/0failed/0ignored/0filtered in
both profiles. Strict workspace/all-target/all-feature Clippy passes in19.62s;
formatting passes. Logs5d2ffe57/f40ab7c6 preserve the complete named test sets,
including the new regression and all existing Observe pool/panic/shared/affine
controls. Required new-head CI/review/main delivery is still separate.

The existing isolated Observe Loom suite passes28/0 with preemption bound3
in an optimized build (logf0b0dcb0). This executes the existing gate's command
through the pinned shell; it is not a claim that its Nix derivation was built.
Focused Miri passes with actual seeds0,1,2,3, one selected case and347 filtered
per seed, exit0 (log21bf7dc2). Its default-feature selection is deliberate;
the all-feature library results above retain their separate427-case scope.

The existing bounded Observe fuzz script passes all four targets: ops,
future_ops, promotion_ops and waker_ops,1024 runs each, seed20261001,
maximum input512 bytes, AddressSanitizer (log774dbe4e, exit0). Its retained
run.JvEORhwV environment, target logs and final corpora identify the actual
campaign. These general owner checks do not claim to generate the new
destructor panic; the exact regression and original paired failures do that.
All selected inputs match before and after the broader checks and fuzz run.
The fuzz crate is a separate workspace: its existing command resolves50
packages under generated lock6940b983, including triomphe0.1.17 and
parking_lot0.12.5. Retain that lock with the campaign. This is additional
standalone-source coverage, not the root's locked0.1.16 dependency closure;
the debug/optimized, Loom and Miri results use the root lock above.

Exact commands, executed through pinned Nix shells (temporary target-directory
overrides retain separate build products), are:

```sh
nix develop -c cargo fmt --all -- --check
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --lib observe::external_tests::pool::pooled_outcome_disposal_panic_does_not_repeat_destruction -- --exact --nocapture
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --lib observe::external_tests::pool::pooled_outcome_disposal_panic_does_not_repeat_destruction -- --exact --nocapture
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --lib
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --lib
nix develop -c cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
nix develop -c env RUSTFLAGS='--cfg loom' LOOM_MAX_PREEMPTIONS=3 cargo test --locked --offline -p observe-tests --lib --release
nix develop .#miri -c env MIRIFLAGS=-Zmiri-many-seeds=0..4 cargo miri test --locked --offline -p bombay-rs --lib observe::external_tests::pool::pooled_outcome_disposal_panic_does_not_repeat_destruction -- --exact --nocapture
nix develop .#fuzz -c bash crates/observe-fuzz/verify-observation-operations.sh
```

The stable commands ran within one already-entered pinned shell, rather than
starting a new shell for every invocation. Preserve the original and repaired
source identities with their separate focused results; no additional mutant
execution or complete local `nix flake check` is claimed.

## Minimization and delivery record

Author review0b3d4b8f and independent two-pass revieweefaaddc agree: one
pre-destructor claim is necessary and sufficient for this reproduced law.
The existing nonpanicking probe cannot test destructor unwinding; retain the
borrowed-counter probe with its owning pool tests. No new abstraction,
dependency, public type, unsafe operation or alternative path is retained.
The remaining source changes correct the ownership proof and document the
existing propagated panic. This is a correctness repair with net-positive
production growth, not code reduction.

Complete focused working tree against cbaa: five changed paths, no untracked
project source; production+20/-18/net2, tests+51/-0/net51, records+208/
-1/net207, public types+0/-0. The isolated fuzz lock and
campaign are ignored run artifacts, retained in the evidence capsule rather
than added as product tooling. The separate full diff against actual
main9ea5defe includes the inherited remote PR stack and is retained alongside
this focused census; it is not attributed to this repair.

Durable evidence: the shared Git directory's
`session-archives/2026-10-21/observation-disposal-qualified-20261024.tar.gz`
retains original/repair sources, prerecord, exact logs/exits, selected inputs,
standalone fuzz lock/corpora, both reviews and the final five-path patch.
Its manifest permits byte verification; recovery requires the named base.
It is not a self-contained compiler or dependency closure. The focused PR,
required exact-head CI, normal protected main merge and merge record remain
pending. The remote PRD and three complete-codebase audits remain open.
