# Explicit remote protocols

Current checkpoint: R01 tests and the retained minimum-remote gate qualify
on submitted `256ad8b028a94be77686cb544ee13b837a5f22be`. Independent final
source/outcome reviews pass; required Linux CI and delivery remain pending.
Earlier checkpoints below retain their own source epochs and pending states.

## Retained minimum-remote execution — 2026-11-11

The actual retained Darwin check executes successfully on the clean submitted
source256ad8b0/treecb15a4f9. Fresh evaluation and actual build agree on
`/nix/store/r72mivx9df7m8f4iqxwypvyzhnslp9z0-bombay-workspace-test-0.1.0.drv`.
This differs from the earlier uncommitted-source0kx evaluation because the
final source/records changed; neither the earlier evaluation nor temporary
probe is relabelled as this execution.

```sh
nix build .#checks.aarch64-darwin.bombay-minimum-remote --no-link -L --max-jobs 1 --cores 2
```

Actual Rust1.96.0/ac68faa20 and Cargo1.96.0/30a34c682 execute the full
remote_surface target:3 outer/11 compiler cases pass,0 ignored/filtered outer
tests. The original invalid programs and current/minimum expectations remain
unchanged; independent readback matches all eight actual immutable test-source
inputs and the separate expectation patch to256ad8b0. No case filter, parser,
alternate runner, source substitution or new runtime law is introduced.

Raw log SHA256 is
`61a2c5c47ad46f7a0ea9de13337ee9eb7802ca587d859ed12b45799ee8ac639d`;
fresh evaluation SHA256 is
`bde080527fadcf5d53ffa12e91ba8f7acd04392b49931514b62420ae5f138efb`.
Both logs and actual exit0 are in common Git's
`session-archives/2026-11-11/r01-integration/`. Independent source/output and
derivation reviews pass. The output's ordinary target archive is not claimed
as a recovered executable or complete transitive compiler closure.

PR408/407/409 are now delivered; PR410 CI and merge remain required. This
later evidence record changes no test, compiler snapshot, Nix configuration,
dependency or public interface. Full R01 delivery and other rows remain open.

## Minimum-compiler integration — pre-edit record

Integrate reviewed compiler002d2545 after the qualified R01 source, preserving
all earlier source/results epochs. Its preceding Loom/retirement/compiler PRs
must deliver first. The user now explicitly requests automatic merges for all
our PRs; preserve required checks, reviews and dependency order.

Actual Rust1.96 probing proves both new denials already match their canonical
1.99 diagnostics. Two older absent-verifier fixtures require independently
reviewed compiler-specific rendering alternatives, with the same original
invalid programs and all their intended errors retained. Their matched full
remote_surface replay passes three outer/eleven compiler cases. This is an
isolated probe, not a retained gate or submitted-head CI result.

Before retaining the extension, select one ordinary Fenix/Crane minimum-remote
Cargo check over the complete existing remote_surface target. Reuse existing
minimum toolchain/builders, one package/feature selector and dependency artifacts.
Extend only the existing minimum.patch and its recorded hashes with those two
actual alternatives; canonical current snapshots and invalid programs stay
unchanged. The original default/axum136 corpus stays unchanged. Expected new
scope is those three configuration records and this feature's evidence/index
records; production Rust and public API remain +0/-0. No parser or runner.

Status: active verification within Static remote actors R01. Baseline is
merged main `937fecc3f7b7bc5efe5dd7519e1bf223af76c350` (PR405/406 delivered).
Full R01, AUTH1, NET1 and the complete remote PRD remain open until their
literal obligations, current execution and delivery evidence are complete.

## Exact missing witnesses and existing evidence

The actual two-program public application already executes in
`examples/remote-counter/tests/processes.rs` through
`separate_programs_increment_read_and_stop_through_real_tls`. The sender's
complete pure Actions are checked by
`replies_emit_the_complete_ordered_request_and_terminal_result_actions` in
that example's `tests/protocol.rs`. The actual receiving Counter and its
complete increment/read Actions are checked by the owning `counter` target's
`correlated_increments_publish_after_the_actual_mutation`. Both applications
reuse the same Counter source; no alternate actor or Engine host is evidence.

The current `remote_surface` target has compiling Client/Export consumers
and rejects a Client whose completion is `u64` instead of the actual Caller's
`Reply`. Its unrelated file-service interpreter denial does not prove absence
of the remote interpreter. No current fixture attempts export with generic
serialization alone. The live public actor-read trace also needs an explicit
unrelated ordinary local protocol that remains usable without Serde or any
network declaration.

## Fresh selected contracts and ownership

Before authoring, Cargo.lock and both manifests select registry dependencies
without patch overrides. Their downloaded `.cargo_vcs_info.json` records
agree with the following exact source revisions. The complete selected
Behavior revision's 760-line AGENTS.md was read from its Git object.

| Owner | Selection and inspected contract |
| --- | --- |
| Behavior/Actors | 0.29.0 / `55dbac2d1068b29907498b3a7fa32b3080e0cd30`; public Behavior/Actions/EstablishedDelivery algebra, actual Counter/Caller and StopOnShutdown implementation; the existing pure Counter and Caller tests. Macros remain 0.16.0. |
| Address | 0.3.0 / `9f058dc03d1239e1ef5c3147a893b134a1e74a83`; opaque resolved authority, reservation/publication contract and reservations tests. Existing actual certified local recipients are reused. |
| Communication | 0.1.6 / `963067e0e6bdef15549088966cde354fd820be7e`; typed Full/Closed/original payload and closing/draining contract, current mailbox witnesses. |
| Observe | selected local `937fecc3`, including delivered PR403; existing completion publication and disposal contract. Node joining and current owning observation tests remain unchanged. |
| Timers | 0.2.0 / `9369d1f6c7fcda91e7de5bf3c19e6769f92145bd`; queue authority, generation-safe token and replacement/order tests. Existing Reader deadline is unchanged. |
| Remote | selected local Node, Client, Export and private Exports interpreter; typed declarations and original-input rejection, verified before decode/admission. Native SDK remains 1.10.10, Tokio 1.53.3. |

Application owns the ordinary local endpoint and its closing/draining.
Node owns all private network interpretation; a serialization bound alone
cannot grant the private Exports contract. The Client owns remote request
interpretation; a raw local Requests endpoint alone does not provide it.
An ordinary local-only Node remains valid: the denial preserves the explicit
exports declaration seam, rather than rejecting a program that requests no
network operation. No dependency contract or protocol consumer is unresolved
for these tests. No production or public interface change is proposed.

## Pre-edit change record

Add two private compile-fail fixtures to the existing `remote_surface` target:
pass the actual raw Requests endpoint to the existing `Node::exports` seam
without Client interpretation; separately pass a generic Serialize-only value.
Both must fail for the missing private Exports interpreter contract, with
their existing fully configured Client/Export consumers still compiling.
Keep the current typed-completion denial unchanged. Capture actual compiler
diagnostics; do not manufacture expected errors or credit unrelated failures.

Extend only the existing
`actor_read_uses_the_node_owned_outgoing_contract` public integration. In its
receiving Node binding, allocate an ordinary local MailLedger endpoint, send
its non-Serde LedgerCommand::Append with a move-only original buffer, receive
that command and its actual closed tail. Preserve admission, sender identity,
exact bytes and buffer allocation beside the existing priming facts. Assert
those acquired originals only after both Node cleanups. No Ledger codec,
provider, key or export is supplied. This proves local protocol independence,
not another actor's domain processing. The existing remote trace is retained.

Expected touched owners: two new compile-fail Rust files and their actual
stderr diagnostics, one existing compile registration, one existing outbound
integration and this PRD. Any required compiler manifest entry belongs to its
existing owner. Expected production +0/-0/net0, tests roughly +140/-0 and
public types +0/-0; no dependencies, new actor, protocol, helper, controller,
generic framework or production abstraction. Counts are estimates, not budgets.

Author review first checks each test against the literal law and independent
observable result. Independent second review checks counterexamples, source
epochs, vacuity and missing outcomes before execution is accepted. Run the
new compiler denials and matched pass controls in debug and optimized builds
through pinned Nix. Run the amended public trace in both profiles. A source
cut that changes the same moved local payload to a separately allocated copy
must fail the independent original-allocation oracle after cleanup; restore
exact source and rerun controls. No timeout or setup error earns law credit.
Retain complete source hashes and actual command results in this record.

At this pre-edit checkpoint production +0/-0/net0, tests +0/-0/net0 and
public types +0/-0. No execution or full-row closure is yet claimed. Required
CI, final minimization and normal reviewed PR delivery remain separate gates.

## Author source review before execution

The new denial fixtures retain the actual public Node declaration and real
Counter/Caller protocols. Their `run` expressions require private Exports
interpretation; neither merely references a nonexistent API. The generic
Serialize bound is intentionally insufficient. The raw Requests endpoint
preserves the exact local request lane while omitting only its Client owner.
Existing compiling Client/Export consumers and the unchanged completion denial
remain registered. No compiler output has yet been accepted or predicted as
an actual diagnostic.

The existing public actor-read journey now acquires the ordinary non-Serde
MailLedger local endpoint, actual send/receive/closed-tail and move-only buffer
identity during the receiving Node's binding. Its sole explicit remote Export
remains Counter Read. All existing remote checks are retained. The new local
oracles run after both Node outcomes have joined; original bytes alone cannot
hide a replacement allocation. No assertion performs a required send, receive,
shutdown or other mutation. This endpoint proves a local communication
capability, not execution of another MailLedger Behavior.

Initial author disposition: pass for independent review, not runtime acceptance.
Independent review and actual compiler/runtime controls remain pending.

Independent two-pass source review by `review_bytes` accepts these exact
fixtures and the retained local extension before execution. Both new denials
must reach the missing Exports contract; root/lifetime/setup errors receive
no acceptance credit. Its counterexample review requires allocating the local
copy while the original is still owned, preventing allocator address reuse.
The amendment adds assertions and preserves every earlier remote oracle.
Disposition: source pass; actual execution, diagnostic readback and independent
outcome review remain pending.

## Actual compiler and public controls

Pinned current Rust is 1.99.0 (`b940084d7`). Initial trybuild execution passes
both existing matched Client/Export consumers and all three existing policy/
completion denials. Both new fixtures reach only E0277: the raw local
ExternalActor Requests port and the generic Serialize-only Command lack the
private Exports interpreter required by the real Node::run. The outer harness
exits 101 because the two new diagnostics have no expected snapshots yet.
This is actual static-denial capture, not a failed runtime law.

An explicit fixture rustfmt check finds wrapping in the new Requests fixture;
the ordinary workspace fmt check does not visit standalone trybuild sources.
After pinned rustfmt, fresh execution again reaches those same two intended
contracts with matched controls passing. Only these two actual diagnostics
are copied from trybuild's wip directory; no existing snapshot is overwritten.
Both initial capture logs and the initial formatting failure retain their
own source scope. Final diagnostics are not relabelled as the initial source.

The full remote_surface target now passes 3 outer tests in each profile,
with 3 compiling and 8 intended-denial inner fixtures. Trybuild invokes its
normal compile fixtures; running the outer harness optimized is not a claim
that its inner static-denial consumers execute optimized actor semantics.
The amended exact public actor_read leaf separately passes 1/0 with 49 filtered
in debug (0.03s) and optimized (0.51s). Original source/lock guards pass all
six selected inputs before the separately planned allocation-copy inversion.

Logs are `/tmp/bombay-remote-contracts-denials-initial.log`,
`/tmp/bombay-remote-contracts-denials-final-capture.log`,
`/tmp/bombay-remote-contracts-surface-{debug,release}.log` and
`/tmp/bombay-remote-contracts-local-{debug,release}.log` with separate exit files.
Final healthy delivery source SHA256 is
`e3ea4a9caac9d28bbc851d005563ad25af87bfac1015e5f150c36232699ea615`.
Final raw-port and serialization diagnostics are respectively
`e5f1b504a63271d44e8b4f7f61825a546309716f3c2745791b2ae104fa0a1457`
and `ce9bb0225192ad05ac62cb7501359dc3a20b4f44f0e0c1ee7434171988762088`.
Actual counterexample sensitivity, exact restoration, final owning checks and
independent outcome review remain required before accepting this extension.

## Actual counterexample, restoration and final owning checks

The sole source cut replaces only the local Append argument with
`local_original.to_vec().into_boxed_slice()`. The copy allocation occurs while
the original buffer remains owned. Its SHA256 is
`9b68a3c0f20d5d51b13aca74d254ea96b8ea1cfaf34e669476455db1cd31c8ab`.
Both debug and optimized executions exit 101 with exactly one failed leaf,
49 filtered, zero ignored: only the pointer-identity comparison at line 391
fails after the joined Node cleanups. Earlier local admission, tail, sender
and byte comparisons pass. Later remote/custody assertions remain unexecuted
in the negatives and receive no negative proof credit. These are deliberate
test-owned custody cuts, not evidence of a shipping production defect.

Restore the exact healthy file before subsequent commands; cmp succeeds and
all six original source/lock hashes match. Full owning application_clients
then passes 50/0/0/0 in debug (1.89s) and optimized (2.21s). Workspace formatting
and both standalone fixture formatting checks pass. Strict workspace,
all-target, all-feature Clippy passes in 34.49s. The final eight-input manifest
also includes both actual compiler snapshots. No cut remains in source.

```sh
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --test remote_surface
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --test remote_surface
nix develop -c cargo test --locked --offline -p bombay-rs --all-features --test application_clients
nix develop -c cargo test --locked --offline --release -p bombay-rs --all-features --test application_clients
nix develop -c cargo fmt --all -- --check
nix develop -c rustfmt --edition 2024 --check crates/bombay/tests/compile/fail/remote_client_requires_interpretation.rs crates/bombay/tests/compile/fail/remote_export_requires_declaration.rs
nix develop -c cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
```

Focused healthy and inverse runs select
`outbound::delivery::actor_read_uses_the_node_owned_outgoing_contract`
with `-- --exact --nocapture`. Commands reuse the coordinator's Cargo target
with incremental compilation and debug symbols disabled; debug assertions
remain enabled in debug, and optimized execution is separate. Raw logs and
exit files are `/tmp/bombay-remote-contracts-copy-{debug,release}.*` and
`/tmp/bombay-remote-contracts-restored-{debug,release}.*`. Clippy and formatting
retain their own `/tmp/bombay-remote-contracts-*` logs and exit files.

## Final containment and acceptance boundary

The same public Node/Client/Export owners and pure actors are retained. The
local MailLedger endpoint has no codec, provider, key or declaration and uses
its existing normal lifetime. The two compiler fixtures prove actual missing
interpretation rather than requiring all local-only Nodes to become remote.
The existing `node_outcome` local-only Node control remains valid and separate.
No new public type, production code, dependency, actor, protocol, wrapper,
registry, macro, runtime hook, controller or application-entrypoint test
machinery is added. Every existing remote integration assertion is retained.

Complete tracked/untracked census against 937fecc3: seven paths;
production +0/-0/net0, tests +297/-0/net297 (including 193 actual diagnostic
lines), public types +0/-0. This is verification growth. The remaining changed
path is this owning PRD; its final line count is recorded at handoff.
Independent final outcome/minimization review passes as recorded below.
Integration on current main, required submitted-head CI and normal reviewed
PR delivery remain pending.
The full R01 record still needs its existing real two-program application,
complete sender/receiver Actions, local-only and example gates at their exact
source scopes. This one two-Node test does not replace two OS processes or
close the full remote PRD. No new minimum-compiler run is claimed here.

Independent `review_bytes` final two-pass review reads both complete actual
diagnostics, all eight guarded inputs, paired healthy/inverse/restored logs
and final formatting/Clippy results. It confirms only intended missing Exports
errors, byte-exact restoration and both post-cleanup pointer failures with
their later assertions uncredited. Source/outcome/vacuity/minimization
disposition: pass for this focused contribution. No production, public API
or owning contract change is needed. Current minimum-compiler qualification,
complete R01 acceptance, required head CI and actual delivery remain separate.

## Current integration and literal R01 evidence

The selected parent includes the reviewed Loom0.7.3 correction, obsolete
transport retirement and ordinary minimum-compiler gate. PR408, PR407 and
PR409 must deliver in that order before this contribution. No shipping Rust,
public Counter source, dependency selection or owning API changes here.
The fresh six-owner verification above remains exact for this source.

| Literal R01 obligation | Owning evidence |
| --- | --- |
| Two actual public Applications exchange an explicitly exported concrete command/reply | `examples/remote-counter/tests/processes.rs::separate_programs_increment_read_and_stop_through_real_tls` runs separate public programs with isolated spaces through actual TLS, Node, Client and Export. |
| Sender's complete Actions contains the request | The same package's `tests/protocol.rs::replies_emit_the_complete_ordered_request_and_terminal_result_actions` checks complete ordered request and terminal-result Actions. |
| Receiver's permitted domain effect | Owning `counter` tests compare complete initialization/increment Actions and `correlated_increments_publish_after_the_actual_mutation` observes the actual Counter's processed mutation, correlated reply and read. |
| Unrelated ordinary local protocol stays unexported | The amended existing public actor-read trace sends and receives the unrelated non-Serde MailLedger command, its original allocation and closed tail without an Export declaration. |
| Missing interpreter, wrong completion and serialization-only export are denied | Complete `remote_surface` target retains compiling Client/Export controls and the typed-completion denial, and adds actual missing-Exports denials for raw Requests and generic Serialize-only inputs. A bare local-only Node remains valid. |

On the current Loom0.7.3 source, the complete `remote_surface` target passes
three outer tests/eleven compiler cases in both profiles. Full
`application_clients` passes50/0/0/0 in each profile. The unchanged owning
`counter` target passes5/0/0/0 in each profile. Its logs are
`/tmp/bombay-r01-counter-{debug,release}.log`; remote controls are
`/tmp/bombay-r01-loom073-{surface,clients}-{debug,release}.log`.
Earlier copy inversions and byte-exact restorations retain their qualified
source and lock epochs; no old negative is presented as a new-lock execution.

The unchanged public Counter package was also executed on this same resolved
graph in both profiles: eight process parents and three pure protocol cases
pass. Four intentionally isolated child registrations are invoked by those
parents; they are not four additional parent passes. The zero-case main
target earns no credit. Source equality with the current parent is checked,
rather than relabelling these earlier runs as new combined-head CI. Logs are
`/tmp/bombay-loom-073-counter-{debug,release}.log`.

## Minimum remote compilation

The retained `bombay-minimum-remote` Nix check reuses the existing Fenix1.96
toolchain and Crane builder, with ordinary all-feature bombay-rs dependency
artifacts. It executes the whole `remote_surface` target. Existing default/
Axum136 diagnostic cases, minimum28-target selector and current1.99 snapshots
remain unchanged. The existing expectation patch gains only two actual1.96
renderings for the older absent-verifier fixtures; all intended invalid
programs and their errors remain. No alternate rendering is needed for either
new R01 denial. All25 patched expectation hashes and canonical/fixture hashes
match independent application/readback.

Actual isolated Nix execution of the same full remote target on Rust1.96.0
(`ac68faa20`) and Cargo1.96.0 (`30a34c682`) passes three outer/eleven compiler
cases with no ignored or filtered outer test. Raw log
`/tmp/bombay-minimum-remote-surface-build.log` has SHA256
`9f5f57812facf28c627691982129f48c295836823fc1f9ebd067c05c31f1c8b1`.
The first probe failed only the two older rendering differences; that failure
is retained separately. No invalid program is accepted to obtain a pass.

The permanent combined-source gate evaluates successfully through pinned Nix
to `/nix/store/0kx9marqq52bcn8sy6sk702g6l0s75y1-bombay-workspace-test-0.1.0.drv`.
That evaluation is not execution. The passing isolated probe is actual local
compiler evidence; retained-gate execution on submitted-head CI is still
required. No redundant broad local build is started while the shared disk
has limited space. Neither static compiler result claims Linux runtime,
cryptographic identity, other acceptance rows or full remote completion.

The qualified original test source/results archive is
`session-archives/2026-11-11/remote-protocols-qualified-20261111.tar.gz`
in common Git, SHA256
`b01c6570a8f47184a8d64a13b5f0855ed16d94e6ea8933362289b017d4e2214e`.
The separate minimum-remote archive has SHA256
`a7a8897ecca5afaddb03336d6650ee7def2ae53d699d05cd7a00dc745c98e1d3`.
These preserve source and actual results, without a transitive compiler or
recovered-executable closure claim. Complete final census, independent
combined-source review, required CI and merge evidence follow separately.

## Integrated containment checkpoint

Against immutable parent002d2545, the complete tracked/untracked delta is
twelve paths: production +0/-0/net0; product tests +297/-0/net297; Nix and
compiler expectations +203/-1/net202; public types +0/-0. Records account for
the remaining lines. This is additional verification, not production reduction.
All shipping Rust, examples, manifests and locks remain byte-identical to the
parent. No project-owned Python file is present in the combined source.

Diff whitespace checks pass outside minimum.patch. Its three new whitespace-
only context lines are valid unified-diff context and follow the existing
patch format; retain them. Valid patch application and all25 hash checks
validate that file; pinned Nix formatting covers flake.nix. The original
failures, source cuts and their paired restorations remain unchanged.

Current runtime logs and retained-gate evaluation are sealed in common Git's
`session-archives/2026-11-11/r01-integration/logs.tar.gz`, SHA256
`4aca46e9c7e24cec6963fbbf8833dbb5b1f111194d837f3c5800d9045010bd5f`,
with a separate manifest of every raw log hash. Source identities, actual
outcomes, final source review and retained-gate CI remain distinct evidence.

Independent combined-source review passes. The minimum compiler's25 canonical
and invalid-fixture hashes match before isolated patch application; all25
minimum hashes match afterward. Default compiler/cases/suites,28 targets and
ignored-child metadata equal parent002d2545. The remote selector has no case
filter. Independent raw-log readback confirms all current counts above and
verifies the current integration archive hash. A separate reviewer accepts the
complete literal map, source epochs, containment and pending-delivery wording.
No further production or public interface is justified by these witnesses.

Final submission census: twelve paths; production +0/-0/net0; product tests
+297/-0/net297; Nix/expectations +203/-1/net202; public types +0/-0.
Records +387/-0; complete contribution +887/-1/net886 against002d2545.
This is readiness for exact-head CI, not a passing retained CI gate or merge.
