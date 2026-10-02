# Behavior Actors 0.19.0: proxy diagnostic parent ingress gap

Historical ARC-010 probe captured against Behavior Core/Actors 0.19.0.
The current `Cargo.lock` selects 0.20.0; its integration evidence is recorded
under ARC-010 and TEST-025 in `docs/open-design-ledger.md`. References to the
"selected" archive below mean the 0.19.0 snapshot used for this probe.
This report does not select a current supervisor policy.

## Reproduction

At capture, `Cargo.lock` selected `bombay-behavior` and
`bombay-behavior-actors` 0.19.0.
Both archives identify owner commit
`e5c703e966eba4d2a15fe2c129594f57ae2270fc` in
`.cargo_vcs_info.json`. `bombay-behavior-macros` 0.13.0 identifies
`3f08364ef3c6d84bb4c27d3d7c0dea9721a628b8`. The complete selected
`AGENTS.md` is identical across those commits. The concrete Bombay
application probe is `fixed-supervisor-runtime.rs` beside this report.

`StableProxy<Worker, Plan>::Sends` has two distinct parent-report lanes:
`ReportToParent<ProxyOutcome<Worker, Plan>>` and
`ReportToParent<ProxyDiagnostic<Worker, Plan>>`. The latter is emitted for
unexpected worker start, stop, initialization, activation, and shutdown
inputs, retaining the original owned input and phase.

An owned child's runtime report sink requires the parent's event type to
implement the selected Behavior contract:

```rust
EventIngress<
    Births<StableProxy<Worker, Plan>>,
    ChildReport<ProxyDiagnostic<Worker, Plan>>,
>
```

Both `FixedSupervisorEvent` and `DynamicSupervisorEvent` implement that
ingress for `ChildReport<ProxyOutcome<Worker, Plan>>`, but neither implements
it for `ProxyDiagnostic`. The fixed-supervisor probe therefore fails with an
E0277 bound on the diagnostic report lane after Bombay's own child-launch
bound and parent birth source are aligned. The same source contract blocks
dynamic-supervisor integration. Bombay cannot implement `EventIngress` for
the external event types under Rust's coherence rules.

The attached `behavior-actors-parent-ingress-witness.patch` adds one
compile-only test to the selected Behavior Actors
`tests/proxy_command_recovery.rs` fixture. In an isolated checkout of the
selected Core/Actors archive revision, this exact pinned command fails with
two E0277 diagnostics, one for each supervisor:

```sh
nix develop -c cargo test --locked -p bombay-behavior-actors \
  --test proxy_command_recovery \
  supervisors_accept_their_stable_proxy_diagnostic_report --quiet
```

## Owner decision required

Behavior Actors needs a typed event ingress and a deliberate consumer for the
complete `ChildReport<ProxyDiagnostic<Worker, Plan>>` in each supervisor.
`FixedDiagnostic::UnexpectedInput` already preserves a complete
`FixedSupervisorEvent`, and fixed supervision already has an explicit
`DiagnosticDisposition`. Dynamic supervision has a separate
`DynamicDiagnostic` product and disposition. Those existing owners may inform
the design, but Bombay must not infer whether a proxy diagnostic changes a
roster, is routed, or terminates a supervisor.

Acceptance evidence should include a compile-pass parent report sink for
both supervisors; existing stable-proxy source tests for every
`ProxyDiagnostic` variant and supervisor pure-fold tests that retain one
complete report with its exact phase, child occurrence, and owned unexpected
input; a selected diagnostic disposition or typed rejection; and an executable
Bombay application that runs the actual supervisor policy and returns the
complete terminal tree. No diagnostic may disappear through a no-op route or
be reclassified as a successful `ProxyOutcome`.

## Isolated owner-side candidate

`behavior-actors-proxy-diagnostic-owner.patch` is an unsubmitted patch against
the selected Core/Actors archive commit. It adds the missing ingress,
injection, and recovery mappings to each supervisor's existing event sum.
Fixed supervision returns the full report through its existing
`FixedDiagnostic::UnexpectedInput` and selected diagnostic disposition.
Dynamic supervision emits a new variant of its existing `DynamicDiagnostic`
product through its existing disposition. Both pure-fold tests assert the
complete report and unrelated effect lanes; deliberately dropping the report
fails each test. The patch adds no new public type and does not change
Bombay's lock or the owner checkout.

The three focused owner test binaries pass in debug (30 dynamic, 99 fixed,
54 proxy) and optimized builds. The complete selected Behavior Actors crate
test suite, full owner workspace test suite, and strict workspace all-target
Clippy pass in the isolated checkout.
`git apply --reverse --check` confirms the artifact exactly describes that
checkout's candidate source. The isolated checkout also has local branch
`codex/proxy-diagnostic-parent-ingress` at `e55c2cb`; it has not been pushed
or submitted for review.

An isolated copy of Bombay was patched to use that owner checkout. The
corrected `fixed-supervisor-runtime.rs` probe compiles and passes in debug and
optimized builds through Bombay's pinned Nix shell. Its explicit four-space
application hosts the supervisor command, worker, status, and capability
protocols. The trace proves worker activation, the supervisor's own accepted
`FixedCommand::Shutdown`, normal root termination, and exact stopped proxy
and worker descendants. It checks root and child origins. The outer
`StopOnShutdown` wrapper supplies the `App` root ingress required for launch;
the test sends the typed supervisor command, so the supervisor's drain policy
performs the shutdown. Strict Clippy passes for the isolated integration test.
The copied Cargo patch and lock are confined to
`/tmp/bombay-arc010-integration`; neither changes the selected Bombay build.
With the current local FIFO adapter and public example copied into that
isolated checkout, its full Bombay workspace test suite and strict all-target
Clippy also pass against the owner candidate.

The selected Bombay build also now passes a separate executable temporary
FIFO pool regression: one job is admitted and completed, and
`WaitForActorGraph` retires its worker after the exact shutdown resolution.
This isolated supervisor integration still does not exercise an unexpected
proxy diagnostic or restart budget. The owner still needs to
review the supervisor diagnostic policy, publish a selected immutable
revision, and run its full repository gates before Bombay can adopt it.
