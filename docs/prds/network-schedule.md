# Incoming request schedule

Current integration checkpoint: the qualified four-path contribution is now
on reviewed parent256ad8b0. All nine archived source/lock hashes match after
integration. Shipping owners, request/declaration code, the actual Counter
and all three locks retain the qualified inputs; only existing record/index
files gain delivery reconciliation. No earlier run is presented as execution
of the new combined commit. Required submitted CI and merge remain open.

Status: active; the first fixed incoming schedule is under verification within
Static remote actors R24. Original baseline is merged main
`937fecc3f7b7bc5efe5dd7519e1bf223af76c350`. This contribution does not implement
the complete network simulator, generated campaigns, restart or full R24.
The user's complete R01–R25/AUTH1/NET1 goal remains unchanged.

## Fresh source fitness and exact selected owners

At the pre-edit baseline, Cargo.lock SHA256 is
`83f01ca3ff82f49f485cb017d14279a654fafc2dfb592f4818b9b726686a3578`;
flake.lock is `13ea961730a74f4bc222ac4a7d879e1b5cd64e4fc820949211e01f8e3df10dc1`.
Root/crate manifests have no patches. Registry VCS records select
Behavior/Actors 0.29.0 at `55dbac2d1068b29907498b3a7fa32b3080e0cd30`, macros 0.16.0,
Address 0.3.0 at `9f058dc03d1239e1ef5c3147a893b134a1e74a83`, Communication 0.1.6 at
`963067e0e6bdef15549088966cde354fd820be7e` and Timers 0.2.0 at
`9369d1f6c7fcda91e7de5bf3c19e6769f92145bd`. The selected Behavior revision's
complete instructions were read in this session. The exact public algebra,
shutdown composition, opaque address/recipient ownership, mailbox admission/
closure and timer generation contracts and their relevant owning tests were
inspected. Current local Observe includes PR403's disposal correction.

The concrete seam was freshly checked against current source and tests:
private Export::request owns actual verification, canonical receipt lookup,
initial permission, concrete command preparation, final permission,
nonwaiting mailbox admission and complete native/refusal custody. Existing
Exports::preflight binds a real Application actor recipient to ActorInterface,
control and lifecycle without requiring native declaration or a Session.
The existing private custody test constructs Request/Custody with query=None.
The owning poll_verification helper records actual Ready before disposing the
producer; the schedule will reuse it rather than invent completed facts.
Normal Application and the existing Counter source execute all domain folds.

Pre-edit request/query/declaration SHA256 prefixes are respectively
`f96bd6a4`, `007abdca` and `5dae0887`. Their existing tests and the normative
runtime/module/Driver contracts were read for this exact seam. No dependency
contract or consumer gap blocks authoring this internal fixed witness. The
coordinator's separate Loom advisory correction and minimum-compiler work
retain their own source/integration gates; this baseline is not relabelled as
those corrected locks and cannot supply their delivery proof.

## Selected fixed law before authoring

An independent, closed schedule supplies original protected Increment pairs,
logical time and revocation. Its fixed externally issued request sequences
are distinct except for the intentional identical repeat. Authority lives in
one request-indexed grant map; time advances monotonically. The trace is:

1. Deliver a currently permitted Increment; interpret its actual native send.
2. Deliver the identical protected pair again. It must reuse the first actual
   canonical key and perform no second command construction or native send.
3. Hold another known pair in its actual verification future, observe Pending,
   advance logical time past its grant deadline and release it. It refuses
   Expired before construction/admission.
4. Hold a different known pair, revoke only its own grant, then release it.
   It refuses Revoked; other grants retain their exact independent windows.
5. Deliver a request whose initial permission is valid. Its existing command
   constructor consumes the explicitly scheduled later clock event, outside
   every Behavior fold. The final permission check must refuse Expired before
   the actual native attempt. Preserve its constructed command and canonical
   key even though admission was not attempted.
6. Deliver a genuinely fresh independent grant with a window beyond that new
   time. It admits. A separate ordinary local FIFO Counter Read must return 2.
   Perform normal shutdown and acquire the real native retirement, report,
   Work cleanup and notification results before any semantic success oracle.

The primary independent post-cleanup oracle is the actual Counter value 2,
before detailed row/trace assertions. Expected policy calls, constructor events,
grant changes and delivery/release order are written directly from this fixed
law, not calculated from implementation outcomes. Compare all original bytes,
proofs, allocations, actual key equalities, completed provider results,
retained commands, native attempts, refusal causes and producer panic slots.
query=None and empty publications establish no native request reception,
callback bound, reply publication, transport, TLS, Client or Node claim.

## Pre-edit scope, source cuts and model checkpoint

Add one private schedule test beside the existing query-owned custody test,
registered under cfg(test). Reuse the existing export-tests Counter module by
widening only that private test module to its parent; do not copy or re-expand
the Behavior source in a second module. Use plain closed schedule/event/grant
sums, standard VecDeque/BTreeMap/Cell storage and existing oneshot verification
gates. No custom executor, generic simulator framework, runtime hook, new
production abstraction, type erasure or private implementation clone.

Expected scope: new schedule.rs, cfg(test) registration in query.rs,
one existing private test-module visibility change in tests.rs, and this PRD.
Expected production +0/-0/net0, tests roughly +450/-1 and public types +0/-0.
These are estimates, not budgets. Production actor states, subordinate protocol
alternatives, transition branches, modules and public spellings remain exactly
unchanged. New sums belong only to the fixed test input and independent trace;
one grant map owns current authority without arrival-history state. No new
actor/Behavior transition authority or timer service appears. Disposition:
source fitness passes; actual implementation/review/execution follow below.

First intended source cut suppresses only the owning canonical lookup while
preserving typed iteration. The repeat then admits again and the primary
post-cleanup Counter Read must be 3 rather than 2. Restore exact healthy source
and rerun controls before the next cut.

Second cut deletes only the existing drop of initial permission immediately
before final revalidation and that final current_request match. The existing
attempt and drop then reuse the initial permission, requiring no invented
generic guard or compiler workaround. The constructor-expired command admits;
its later reply policy cannot undo that actual mutation. The same independent
post-cleanup Read must be 3. A compiler/setup/timeout failure earns no law credit.
Restore exact production/test sources and rerun healthy controls in both builds.

Before accepting either cut, source review maps the test to the documented
law and independent native outcome. Independent second review checks vacuity,
missing outcomes and whether the original omissions could escape its oracle.
Run all Rust through pinned Nix, serialize shared target builds with the
coordinator, then perform required current-source integration and CI separately.
Full R24, exhaustive/generated schedules, minimized seed replay, crash/restart,
native callback/Client seams and routine/long scheduled campaigns remain open;
durable-commit and cryptographic obligations remain with their external owners.

## Author and independent source review

The first source review maps each scheduled input to the real request
interpreter, complete typed row and independent post-cleanup Counter outcome.
Current-request and current-reply observations remain distinct. The original
revoked grant and both actual verifier-release Results are retained separately;
neither is replaced by a boolean or fabricated completion. The same pinned
request future survives its observed Pending poll and is completed through
the owning poll_verification helper. Every Counter fold remains unchanged.

Independent two-pass `review_bytes` source/vacuity review passes this bounded
law before execution. Its ledger review adds an explicit equality of original
and row counts before zip, preventing silent truncation. The fixed proof's
sequence-to-byte setup uses checked standard conversion. The final formatted
test is larger than the 450-line estimate; actual census is retained below,
not called cleanup or production reduction. No source/state/API change was
invented from a compiler diagnostic. The complete fixed expected trace does
not claim the remaining simulator or native transport seams.

At this source checkpoint: four paths; production +0/-0/net0, tests +620/-1,
public types +0/-0. This includes four cfg(test) registration lines and one
private test-module visibility replacement, not new production state. Actual
Rust execution, both intended cuts and byte-exact restoration remain pending.

Final source review replaces an unconditional held label plus separate pending
ledger with the actual Poll result recorded once in the complete trace. The
expected trace requires Pending for both controlled verifiers. Independent
review rereads this amendment and the complete row/original length equality;
source/vacuity disposition remains pass before execution. The formatted test
is 615 lines, SHA256
`3c945c2029279c1cb5ff412a09fc8dfd10614f631e2e36676ef0b17ad1fd71b2`.
Only ordinary pinned rustfmt has executed at this checkpoint; no compilation,
runtime result, inversion or full R24 acceptance is yet credited.

## Verified lock integration before first compilation

The coordinator selects verified `899b457257721eec5349c975119a5afa01ce2c17`
before any schedule compilation. The branch fast-forwards from 937fecc3;
the four contribution paths remain unchanged. This updates Loom to 0.7.3 and
removes its unmaintained pinning dependency through the owning locked graph.
Root lock is now
`3a4c840ce3db066cd6d2d802c9243df1f34fbeba619ba3a145c530cb90e7266a`;
isolated transport lock is
`6f6f3caf99875f99e1c55c79f18486b8ef36f03c1745ef1e07cf90ed0b690ec0`.
Behavior and the five other selected owners, source, fixed schedule, test
registration and public interfaces remain unchanged; both frozen query and
test files compare byte-identically. Original eight-input guards pass before
integration and their old lock epoch is preserved separately. Refresh the
guards for these actual new locks before the first compiler invocation.
No earlier runtime result is relabelled: none has executed for this schedule.
The coordinator retains advisory PR delivery and complete integration gates.

## Initial compiler veto and unchanged owning seam

The first exact debug command exits 101 before execution. Both E0599 messages
come from the generic serde_json::from_slice function item not satisfying the
existing higher-ranked Decode Fn input. They are not an inaccessible request
method or missing Application contract. Preserve initial source 3c945c20 and
`/tmp/bombay-network-schedule-healthy-debug-initial.log`; no runtime or inversion
credit follows. Select the existing owning-consumer spelling instead:
`|bytes: &[u8]| serde_json::from_slice::<Increment>(bytes)`. This capture-free
closure implements the already selected exact decoding operation; it introduces
no callback contract, type, bound, adapter, visibility or production change.
Independent source review must confirm that equivalence before acceptance.

Independent `review_bytes` readback accepts the exact one-line closure
refinement: same JSON parse operation and existing constructor port, with no
new semantic surface. Refined test SHA256 is
`5fa5d5076d8c900e5ad963313165256cb2f102c728dfef676dd4167c7063dc49`;
the earlier function-item source remains separately preserved. Actual exact
debug execution now passes 1/0 with 429 filtered after 48.17s compilation and
0.01s runtime. Both actual Pending observations, both held refusals, E's
construction/final-check distinction, fresh progress, original typed rows,
native retirement and the entire independent expected trace execute. The
primary post-cleanup local FIFO Read is 2. Refreshed nine-input guards match
before the separate optimized control; no optimized or inverse result is yet
credited at this checkpoint.

The same refined source also passes exact optimized 1/0 with 429 filtered
after 3m49s compilation and 0.00s runtime. This completes the paired positive
control before any source cut. Independent review accepts both prepared cuts:
canonical lookup adds only take(0), SHA256
`5f4946c34d82673258a77bae7a275fbfa037894636c0ee5c36f8d8cb1f837d5f`;
final revalidation deletes only the initial-permission drop and final match,
SHA256 `9626f2e6732bc6bf718b8133cd3e634953dbde61484d71ee608c9d38ccb953d0`.
Healthy query remains `dfaae0a0`. Neither cut introduces a new guard or
compiler workaround. Actual cut executions and restored controls follow;
prepared source alone supplies no inversion credit.

## First canonical cut and transient linker failure

The exact canonical-only cut runs in debug and fails only the first
post-cleanup oracle: actual User3 versus required User2 at schedule.rs438,
one failed leaf/429 filtered (0.04s), exit 101. Later row/trace assertions
remain unexecuted and receive no negative credit. Its optimized attempt also
exits 101 but fails during linking because the Nix 1.99 sysroot's named
libtest/libstd/libcore rlibs are reported missing. It executes zero tests and
provides no optimized law credit. Raw failures remain in
`/tmp/bombay-network-schedule-canonical-{debug,release}.log`.

The command restores healthy query byte-for-byte and all nine source/lock
guards pass. Subsequent read-only inspection finds the cited symlink targets
present; nix-store --verify-path succeeds for both the exact combined 1.99
toolchain and its rust-std component. The original transient cause is unproved;
no repair, package/toolchain change or source workaround is selected. Rerun
the exact optimized cut on these verified current inputs, keeping its new
result distinct from the original failed link and earlier debug execution.

The unchanged optimized canonical cut retry actually reaches the intended
post-cleanup User3-versus2 oracle, 1 failed/429 filtered, exit 101 (0.00s),
after 3m47s compilation. Both old-source canonical negatives are now real;
the original linker failure remains separately uncredited. Exact healthy
query restoration and all nine original input checks pass afterward.

Before subsequent controls, the assertion audit identifies one late encoding
matches that binds an owned Box inside assert. Select borrowing &encoding
instead, so the assertion observes its existing value without transferring
ownership. Independent review accepts this one-character correction; predicate,
expected bytes and the complete prefix through the primary Read oracle are
unchanged. Earlier positives/canonical negatives retain exact source 5fa5d507
and are not relabelled as final-body execution. Run final-source healthy and
canonical controls before the final-permission cut; do not rely only on prefix
equivalence when actual final-source execution remains available.

The final borrowed candidate is 615 lines, SHA256
`ee2203a626673cac00848986f392f724425bbdd24da4b847ee120b491fb3e7eb`.
Its queued paired positive command remains inside pinned Nix entry before
Cargo starts. Nix explicitly reports its store SQLite database busy. A
read-only sample locates the client in RemoteStore::addCAToStore and the daemon
connection read; a concurrent Determinate-owned automatic garbage collection
is observed, without proving the daemon-side cause. Preserve the raw shell
log and client sample. No runtime credit, package replacement, process restart
or host-tool bypass follows from this environment wait. Healthy query and all
nine input guards remain intact while the original command is live.

The original pinned command subsequently enters Cargo without intervention.
The final borrowed source passes debug 1/0 (429 filtered, 0.01s runtime,
2m01 compilation) and optimized 1/0 (0.00s runtime, 4m10 compilation).
All detailed rows and the complete expected trace execute after actual native
cleanup and the primary Read2. All nine frozen inputs pass afterward. These
results belong to ee2203a6; the Nix wait remains separate environment evidence.

The final-source canonical-only cut also reaches the intended primary Read3
versus Read2 oracle in both profiles, each exit 101 and one failed leaf with
429 filtered. Debug compiles in 56.81s and executes in 0.04s; optimized compiles
in 3m58 and executes in 0.00s. These are ee2203a6 outcomes rather than prefix
equivalence claims. The complete nine cut inputs pass unchanged, healthy query
restores byte-for-byte, and the nine healthy input guards pass before the next
paired restored controls. Later row assertions remain unexecuted in each
negative and receive no inversion credit.

Exact restored controls after canonical omission pass debug and optimized,
each 1/0 with 429 filtered; compilation 44.73s/3m52, runtime 0.01s/0.00s.
All nine healthy inputs pass before the separately reviewed final-permission
cut. Shared disk headroom is 1.6GiB; execute only these focused controls in the
existing target, with no broad build or new target.

The final-permission-only cut also fails at the same first post-cleanup
Read3-versus2 oracle in debug and optimized, each exit 101 with one failed leaf
and 429 filtered. Compilation takes 44.85s/3m45 and runtime 0.04s/0.00s.
Canonical lookup remains healthy; the sole source difference retains initial
permission through the constructor instead of revalidating it afterward.
The negative supplies no later-row credit. All nine cut inputs pass, then
healthy query restores exactly and all nine healthy guards pass. The final
healthy pair remains required before acceptance.

The final restored source passes both profiles, each 1/0 with 429 filtered;
debug compilation/runtime is 41.85s/0.01s and optimized 3m45/0.00s. All detailed
rows and the complete expected trace execute after real Application cleanup.
Pinned workspace fmt check exits 0. All nine frozen source/lock guards pass
afterward. No mutant remains; the query is dfaae0a0 and test is ee2203a6.

## Frozen pre-lint scope and exact commands

The frozen pre-lint contribution contains four paths: production +0/-0/net0,
tests +620/-1/net619, public types +0/-0, plus this focused PRD. The private
Counter module visibility permits reuse of the same nominal owning source;
it adds no shipping mechanism or API. Source, outcome and minimization review
retain the plain fixed schedule rather than a new simulator abstraction.

All exact leaf executions use these pinned commands, with Cargo target set to
the coordinator's existing target, build jobs 2, incremental 0 and dev/test/
release debug info 0. Each source epoch, cut input manifest, actual exit and
raw log remains separate in the session evidence.

```sh
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --lib remote::export::query::schedule::scheduled_duplicates_and_authority_changes_preserve_native_admission -- --exact --nocapture
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --lib remote::export::query::schedule::scheduled_duplicates_and_authority_changes_preserve_native_admission -- --exact --nocapture
nix develop -c cargo fmt --all -- --check
```

Final independent review_bytes reads the complete retained source, PRD, raw
outcomes, restorations and nine-input manifests. Its source/outcome/vacuity/
minimization review passes for this bounded slice; no source veto or further
edit is requested. Shipping production/API delta remains zero.

This qualifies only the fixed incoming law and its two omission sensitivities.
The coordinator retains broader integration, Clippy, submitted-head CI and
delivery. The full R24 simulator, generated campaigns and minimized replay,
restart, native query/callback, completion consumer, Client and transport
witnesses remain open. No minimum-compiler or full R24 credit follows from
these current-compiler leaf results. No commit or push is made by this author.

## Strict Clippy source veto and final spelling

The first strict private-unit Clippy invocation reaches actual Cargo after
materializing the existing pinned rust-analyzer dependencies. It exits 101
with exactly one manual_let_else at schedule.rs307. All nine frozen inputs
pass afterward. Preserve its raw log/exit and the already independently
verified frozen source capsule; its passing law outcomes remain ee2203a6.

The coordinator and independent reviewer accept only the ordinary let-else
spelling of the same Deliver|Hold selection, binding and divergent panic.
No event, policy, expected outcome, lint exception, bound, public interface
or production source changes. After formatted source readback, rerun strict
Clippy, healthy debug/optimized, each already reviewed omission in both
profiles with exact restorations, and final healthy debug/optimized. The
coordinator permits byte-exact restoration guards between these two cuts
without another intervening healthy pair. No earlier log is relabelled.

The formatted let-else source is 614 lines, SHA256
`fa52c802b7f248a741c889c776ed4c8433388251086d6e7b0dcc59558555e4a6`.
Independent exact readback confirms only that ordinary spelling changes.
Its strict Clippy exits 0 in 14.86s. Healthy debug/optimized each pass 1/0
with 429 filtered; compilation is 46.99s/3m59, runtime 0.01s/0.00s.
Both omissions then independently fail only the first post-cleanup Read3
versus 2 oracle at the new line 437, each exit 101/one failed leaf. Canonical
compiles 45.68s/3m46 and permission 43.85s/3m52; negative runtime is
0.04s/0.00s for each pair. All cut and exact healthy restoration guards pass.
Later negative row/trace assertions remain unexecuted and uncredited.

The final restored debug also passes 1/0 (45.33s/0.01s). Its optimized
attempt exits 101 during linking, executing zero tests: xcrun warns that the
pinned SDK cannot be found, then cc reports the exact pinned 1.99 sysroot
rlibs missing. The queued fmt check does not execute. Preserve raw warning,
linker output and exit separately; final optimized-restoration credit remains
open. The command restores healthy query and all nine fa52c802 input guards
pass before releasing the shared target to the coordinator.

Read-only inspection then confirms the exact combined Rust, rust-std and
apple-sdk14.4 store paths cited by that invocation are absent; nix-store
--verify-path exits 1 because the combined path is invalid. No alternate SDK,
compiler, source workaround, host Rust command or service mutation is chosen.
The coordinator owns cache relocation and exact pinned-closure restoration
before another invocation. Test/source fitness and earlier actual outcomes
remain separate from this environment failure. Current retained test delta
is +619/-1/net618, production/API zero; full R24 and delivery remain open.

## Exact environment restoration and final retained evidence

The coordinator moves the existing Cargo cache by filesystem metadata to
`/tmp/bombay-remote-20261111-target` before retry. No new cache remains under
the earlier PR worktree. The ordinary supported Nix profile
`/tmp/bombay-network-20261111-shell` records this branch's actual baseline 899b4572
environment. Entry exits 0 and reports the original 1.99/b940 compiler and
Cargo 1.99, exact combined 45c8 sysroot and SDK 0p9q (14.4). Requisites include that
combined compiler, std lb011 and SDK; the roots query names this temporary
profile. Content verification of all three exact paths exits 0. This retains
the selected closure without a project/global configuration, toolchain or
service change; it does not assume the coordinator's newer environment is
identical. Raw profile entry, closure, roots and verify records are preserved.

Using only that own profile and relocated target, the unchanged fa52c802
final optimized control actually passes 1/0 with 429 filtered after 3m46
compilation and 0.00s execution. The full original rows and expected trace
execute after native cleanup and Read2. The separately queued pinned fmt
check now executes and exits 0. All nine source/lock guards pass afterward.
The earlier failed final link remains separate and earns no runtime credit.

Final complete source census: four paths; production +0/-0/net0,
tests +619/-1/net618, public types +0/-0, plus this focused PRD. No mutant,
production edit, lint suppression, new abstraction or changed expected value
remains. Current source is the reviewed 614-line let-else candidate; earlier
source epochs remain frozen evidence. Strict Clippy, healthy controls, both
omissions and exact restorations now have their own actual current-source
results. Broader integration, minimum compiler, submitted-head CI and delivery
remain coordinator-owned. The full R24 obligations remain open as above.

## Integration pre-edit record — 2026-11-11

Integrate the independently qualified four-path contribution on reviewed
parent256ad8b0. The source/results capsule723f7c95 remains immutable; its
baseline899 environment and earlier epochs are not relabelled. The parent
contains the current Loom correction, obsolete research retirement, ordinary
minimum compiler gate and R01 contribution. Preserve all nine guarded runtime,
actor and lock inputs byte for byte after integration. No further production,
test expectation, dependency or public interface change is selected.

Reconcile actual PR408/407 delivery and worktree cleanup, R01's later retained
minimum-remote execution and this fixed incoming witness in the existing
owning PRDs and backlog status. Expected scope is the same three test-owned
paths, this record and three existing record/index files; production/API0.
At this pre-edit checkpoint main is14729ceb and PR409/410 remain open;
later delivery reconciliation above retains their actual subsequent states.
Record final source equality, complete census, independent integration review
and required submitted-head CI before delivery; enable normal auto-merge under
the user's instruction. Delete this worktree after its merge and active use.

### Integrated source conservation

The ordinary fast-forward to256ad8b0 preserves the complete three-path test
patch and all nine qualified input hashes. Pinned Behavior/Actors0.29.0,
Address0.3.0, Communication0.1.6, local Observe, Timers0.2.0 and their public
contracts remain those inspected above. Current Loom0.7.3, native SDK1.10.10
and Tokio1.53.3 remain selected. No dependency, consumer or ownership blocker
is found for this test-only integration. Earlier locks/compiler/profile
epochs keep their original meanings; fresh CI covers the combined commit.

Final local source/outcome/custody review passes by author, independent
reviewer and coordinator. The final source/results capsule is
`session-archives/2026-11-11/network-schedule-qualified-20261111.tar.gz`
in common Git, SHA256
`723f7c95a10df1b04ed601a5b2fa6209aa38e6b896d3833ae9dac98a13bb58f7`;
all133 payload hashes independently pass. Its exact four-path patch SHA256 is
`c84eb27aaeb424e3ad712e7f675f956700520c4bfc8fa429c0357979fb8361e6`.
The earlier pre-lint archive remains separate and unchanged.

The retained comparison is one fixed incoming schedule, with real actor
processing and complete owned facts. It adds no public abstraction or second
runtime and does not stand in for Client/native Query, finite exhaustive or
generated campaigns, minimized replay, volatile restart, scheduled network
campaigns, complete R24, AUTH1, NET1 or final whole-feature distillation.
Complete integrated census and independent record review follow below.

### Final integration review and containment

Independent integration review confirms all nine hashes and the exact
three-path source patch, retained r72 execution, actual delivery receipts and
clean worktree removals. Its archive-location finding is resolved with the
complete common-Git prefix. No shipping code, public type, dependency, compiler
expectation or Nix/workflow configuration changes in this contribution.

The complete tracked/untracked delta against256ad8b0 is eight paths:
production +0/-0/net0; tests +619/-1/net618; public types +0/-0;
records +588/-19/net569; total +1207/-20/net1187. This is verification and
delivery-record growth. Required submitted CI, reviewed merge and worktree
removal remain open; complete R24 and all other incomplete rows stay open.
