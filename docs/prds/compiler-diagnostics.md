# Compiler diagnostics

Status: active. This focused maintenance stage replaces the two unused Python
diagnostic programs with the repository's existing Fenix, Crane, Cargo and
trybuild owners. Full remote acceptance remains separate.

## Law and pre-edit record (2026-11-11)

The supported minimum compiler must execute the same actual valid and invalid
Rust programs as the selected current compiler. Invalid programs must fail for
their reviewed diagnostics; an unexecuted target or overwritten expectation is
not proof. The repository checkout's Rust programs, canonical snapshots and
reviewed minimum-compiler patch must remain unchanged.

Baseline: main `937fecc3`, after PR405 and PR406 delivery. Cargo declares minimum
Rust 1.96; rust-toolchain.toml pins current Rust 1.99.0. The existing diagnostic
manifest identifies both exact compiler releases, 28 targets, 32 suites and 136
compile cases. All 23 recorded canonical snapshot and fixture hashes match the
baseline. `git apply --check` accepts the existing minimum.patch. These are
source checks, not compiler execution.

The prior configuration has no required minimum-compiler Nix check. Its manual
Python program copies source, chooses reviewed stderr changes, invokes Cargo,
parses its transcript and hashes the source again. A second Python program tests
that transcript parser. Neither program is used by the current flake or workflow.
The developer-experience PRD still directs contributors to invoke them.

Reuse normal Nix immutable inputs and disposable build source, Fenix's supported
toolchain selection and Crane's compiler override, dependency build and Cargo
test derivations. Cargo/trybuild own execution and diagnostic comparison. Read
the existing compiler manifest for the recorded target selection and compiler
identity; assert its minimum release agrees with Cargo's owning rust-version.
Apply only the unchanged reviewed minimum.patch inside that compiler's Nix test
derivation. Keep the current pinned workspace gate and all fixture registrations.
Do not port the transcript parser or its synthetic transcript tests.

Inspect before editing: Trive's repository-owned Nix environment, CESR's
Fenix/Crane source filtering and checks, and mnesis-bombay's Fenix override and
trybuild stderr inclusion. Those projects use the same existing package builders;
none supplies another diagnostic runner that Bombay should adopt.

Selected dependencies are unchanged: registry Behavior/Actors 0.29.0 at
`55dbac2d1068b29907498b3a7fa32b3080e0cd30`, macros 0.16.0, Address 0.3.0,
Communication 0.1.6 and Timers 0.2.0, plus local Observe/Engine. Their complete
selected Behavior instructions were read. This stage changes no primitive,
runtime interpreter, protocol, behavior or public interface. Compiler tests
remain in their existing owning crates. No Cargo or flake lock change is needed.

Expected complete scope: flake.nix, the two deleted Python programs, the current
developer-experience command guidance and this record. Production Rust +0/-0,
product tests +0/-0, public types +0/-0. Forecast verification configuration
about +60 lines, obsolete verification programs -297 lines. Numerical budgets
were explicitly removed by the user; measurements and minimization remain.

## Verification and failure controls

Before acceptance, evaluate the new required check and run its actual Nix build.
Inspect actual compiler identities and every selected trybuild case in the
resulting log. Compare those names with the unchanged manifest, and establish
that the current pinned gate still executes its owning compile tests. Retain raw
outputs outside the source checkout. Normal Nix patch application must reject a
changed expectation; a minimum-compiler version disagreement must fail evaluation.
Run a focused invalid-program control with its expected error removed to prove
that trybuild's actual compiler/diagnostic check fails; restore the unchanged
source before healthy execution. No product test or expected value is weakened.

This is verification maintenance, not a new actor law. No runtime regression,
network acceptance, performance claim or whole-PRD completion follows from it.
Independent source/outcome reviews, final complete tracked/untracked census,
updated-head CI and reviewed PR delivery remain required.

## Original locked qualification

The actual Darwin Nix build on `937fecc3` with root lock `83f01ca3` and Loom
0.7.2 exits zero. Fenix supplies Rust 1.96.0 at
`ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96` and Cargo 1.96.0 at `30a34c682`.
The normal Crane test phase runs optimized Cargo against all 28 recorded
targets: 114 outer passes, one expected child registration exercised by its
passing parent, all 32 named compiler suites and exactly the 136 recorded
compile-case names. A direct comparison of actual names with the unchanged
manifest succeeds. Inner compile cases are not added to outer test counts.

An actual Engine-only counterfactual deletes just the first expected E0599
line inside the disposable Nix build. The owning compile suite executes five
cases: four pass and only no_lifecycle_controls has the intended diagnostic
mismatch; Cargo exits 101 and Nix exits one. A fresh matched Engine-only control
without that deletion executes the same five cases and exits zero. Canonical
programs, snapshots and the minimum patch remain unchanged in the checkout.
Changing Cargo's minimum to 1.97 separately causes the intended Nix version
agreement assertion to fail; canonical Cargo is restored before compilation.

Earlier vendor-only runs and an inverse with the wrong intermediate phase
override were cancelled before any test execution. They receive no runtime or
negative credit. The corrected counterfactual uses Crane's final checkPhase
with its original pre/post hooks, rather than an already-expanded argument.

The 17-payload capsule `compiler-diagnostics-loom-0.7.2-20261111.tar.gz`, SHA256
`7ab67040ecd77a3f8a64aed8fe071b70164b626bec78bfc2104a3f88858851a2`, retains
the exact source proposal, original lock, reviewed expectations, actual
program/counterfactual expectation, derivation identity and raw outcomes.
Fresh extraction verifies every payload. It is not a compiler closure.

These results retain their original source scope. Local integration of verified
`899b4572` selects Loom 0.7.3 and removes the unmaintained pin-utils dependency;
its exact root lock is `3a4c840ce3db066cd6d2d802c9243df1f34fbeba619ba3a145c530cb90e7266a`.
The unchanged minimum gate requires its own fresh execution on that graph.
No old result is relabelled as the corrected dependency's qualification or PR408
delivery. The initial gate deliberately preserves default/Axum compiler scope;
separate minimum-compiler remote probes cannot receive credit from it.

## Corrected lock qualification

Fresh execution on `899b4572` and root lock `3a4c840c` exits zero through the
ordinary minimum-compiler Nix derivation. Rust/Cargo identities still match;
all 28 targets, 114 outer passes, the expected parent-owned child and all 32
named suites execute. The complete sorted set of 136 actual compile cases
again matches the unchanged manifest. The raw build log SHA256 is
`e8263d9f7906671f7f4a66a62b4bfd31ce6f8d57dfbdb9ba107e7aa35e030cd2`;
its case-name set is `f52906e077a22357b345cb1c65fbd0f5f055738ff36687a306a372028182690a`.
The original inverse/restoration retains its explicitly older graph scope.

```sh
nix build .#checks.aarch64-darwin.bombay-minimum-compiler --no-link -L --max-jobs 1 --cores 2
```

The two Python programs are deleted with no parser or synthetic transcript
suite replacement. Product Rust, its tests, canonical compiler snapshots,
reviewed expectation patch and compiler manifest remain unchanged. This adds
one ordinary required Nix check; no production or public type is introduced.
Current-compiler workspace and remote gates retain their existing owners.
Nix formatting and whitespace checks pass. Updated-head full CI, independent
final review and actual PR delivery remain required before recording a merge.

## Containment

Against the integrated prerequisite `899b4572`, the complete tracked/untracked
contribution has five paths: flake.nix, the two deleted diagnostic programs,
the developer-experience guidance and this record. There are no untracked
project artifacts. The record and existing guidance account for 163
added/eight removed documentation lines; the full contribution is 206
added/308 removed lines. The prerequisite's two locks and two records remain
separate committed inputs, not additional compiler production changes.

```text
production Rust: +0 / -0 / net 0
product tests:   +0 / -0 / net 0
parser tests:    +0 / -112 / net -112
verification:   +43 / -188 / net -145
public API:     +0 types / -0 types
```

The retained composition uses the existing Fenix/Crane builders, one shared
package/feature selector and the owning compiler manifest. The deleted parser's
self-tests retire with their owner; no actor or public compiler test is removed.
