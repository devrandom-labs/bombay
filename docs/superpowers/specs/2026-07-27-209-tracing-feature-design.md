# Card #209 — `tracing` feature: actor-lifecycle spans/events

**Date:** 2026-07-27 · **Card:** [#209](https://github.com/devrandom-labs/bombay/issues/209) · **Status:** approved

## Problem

The core has no observability. Three `eprintln!` placeholders sit in the hot
lifecycle path (`crates/core/src/actor/spawn.rs` — `log_on_stop_outcome` ×2,
`log_on_stop_abandoned`), each carrying `#[expect(clippy::print_stderr)]`. They
surface real defects (an `on_stop` that errors / panics / outlives its grace)
but unstructured, unfilterable, stderr-only. #66 defines the feature *layout*;
this card is the `tracing` *implementation*.

## Bar

axum-grade library instrumentation: bombay emits `tracing` spans/events with
structured fields; the subscriber — fmt logs, `tracing-opentelemetry`,
tokio-console — is always downstream's choice. Bombay never installs a
subscriber (tracing docs: "libraries should not call `set_global_default()`").

## Decisions (with alternatives considered)

### D1 — tracing-direct, no observer seam

Three architectures were weighed:

1. **tracing-direct** (chosen) — `tracing` *is* the hook seam; its `Subscriber`
   trait is the observer pattern, and otel/fmt/console attach downstream with
   zero bombay code. Disabled callsites cost one static-atomic interest check
   (tracing's callsite-registry design).
2. **Own `ActorObserver` trait** (Elixir `:telemetry` style) — reinvents
   tracing's dispatch minus the ecosystem: no free otel bridge, no `RUST_LOG`
   filtering, and no span *context* (scoped enter/exit across polls), which is
   the hard part. Rust prior art for this shape is near-zero; hyper/h2/tokio/
   kameo all emit tracing directly.
3. **`log` facade** — events only, no spans, no trace trees. Dead.

Downstream apps that want `log` output enable the `tracing/log` feature in
their own tree (cargo feature unification reaches bombay's `tracing` dep);
bombay does nothing.

### D2 — feature gating: single cfg surface in `trace.rs` (amended in execution)

As designed, this section said per-site `#[cfg(feature = "tracing")]`,
kameo-style. What shipped is the opposite concentration: **all** cfg lives in
`crates/core/src/trace.rs`, which holds two mirrored `imp` halves — the real
implementation under `#[cfg(feature = "tracing")]`, and a no-op half with
identical signatures (`Span`/`SendContext` as ZSTs, `const fn` stubs) under
`#[cfg(not(feature = "tracing"))]` — re-exported so every call site is
cfg-free. Per-site cfg was rejected during execution: the envelope field
(`SendContext` riding `Signal::Message`) and `instrument`'s `impl Future`
return force cfg-carrying *types* regardless, so per-site gating would have
smeared the same conditionals across `mailbox`/`spawn`/`kind`/`watch` instead
of concentrating them in one module. Mirrored halves keep exactly one surface
to audit, and the off half is verified wholesale by the `bombay-tracing-off`
gate check (D9).

### D3 — default-ON

`default = ["tracing"]`. The card said default-off; amended by decision
2026-07-27 (record on the card at close). Matches axum and kameo
(`default = ["macros", "tracing"]`). With no subscriber installed the cost per
event is one atomic load + branch; users opt out via
`default-features = false`, which compiles every site out.

### D4 — minimal dep: no proc macro

```toml
# workspace root
tracing = { version = "0.1", default-features = false, features = ["std"] }
# crates/core
tracing = { workspace = true, optional = true }

[features]
default = ["tracing"]
tracing = ["dep:tracing"]
```

`default-features = false` drops the `attributes` feature (the `#[instrument]`
proc macro and its syn/quote subtree) — all spans here are manual, so the
macro buys nothing. No `tokio/tracing` either: that feeds tokio-console,
which is the separate `console` card per #66.

### D5 — span model

- **One `actor.lifecycle` span per actor** — created at spawn with fields
  `actor.name = A::name()`, `actor.id`, and `stop.reason = field::Empty`.
  The run-loop task is wrapped `task.instrument(span)`: per-poll enter/exit,
  never an `enter()` guard across `.await` (tracing docs: holding the guard
  across await points produces "incorrect traces").
- **`follows_from`, not parent, for the spawn site.** kameo implicitly parents
  the lifecycle span to the spawner's current span; but the actor outlives its
  spawner and parent-child implies containment. tracing docs: follows_from
  spans "may close even if subsequent spans that follow from it are still
  open", modeling "async task spawning" — exactly spawn. The lifecycle span is
  a root that `follows_from` the spawn-site span; otel renders it as a span
  link.
- **`stop.reason` recorded at stop** via `span.record()` — the whole actor
  lifetime becomes one otel span carrying its outcome as an attribute.
  (tracing constraint: late-recorded fields must be declared `field::Empty`
  up front.)

### D6 — caller-span propagation (per-message `actor.handle` span)

The send path captures `Span::current()` (docs: zero-cost when no subscriber /
untracked) into a `#[cfg(feature = "tracing")]` field on the mailbox envelope.
The loop creates an `actor.handle` span per message with **explicit parent =
the captured caller span**; when the caller span is disabled/none, the span
parents to the current (lifecycle) span. Cross-actor otel traces stitch: a
sender inside an HTTP request span produces a handler span in the same trace
tree — this is the whole "nice otel" property.

Envelope footprint (amended in execution): the envelope field is
`Option<Box<Span>>` — **one word per slot**, niched on `None`. The original
"a disabled `Span`, a few words, no alloc" inline claim broke the #114
slot-size tripwires: an inline `Span` pushed `Signal<Probe>` to 56 bytes.
Boxing restores the slot to 24 bytes + the one context word and preserves the
#207 zero-alloc tell: the box is allocated **only** when the sender is inside
an *enabled* span — with no subscriber installed (or the feature off, where
`SendContext` is a ZST) the field is `None` and the send path allocates
nothing. The one small allocation per traced send is the unavoidable price of
context propagation in any architecture.

### D7 — events and levels

| lifecycle point | level | key fields |
|---|---|---|
| spawned | `trace` | (on lifecycle span) |
| `on_start` ok | `trace` | |
| `on_start` err | `error` | `?err` |
| message handled | `actor.handle` span | msg type name |
| handler panic (`on_panic` path) | `error` | `?reason` |
| `on_stop` ok | `trace` | `%reason` |
| `on_stop` err / panic / **abandoned** | `error` | `%reason`, `?err` |
| restart scheduled | `warn` | `restart.attempt`, `restart.delay_ms`, policy |
| restart give-up (`RestartLimitExceeded`) | `error` | `restart.attempt` |
| death-watch notice delivered | `trace` | `%reason`, `cleanup` |

Field discipline (card bullet): `actor.name` / `actor.id` / `reason` are
structured fields via `%`/`?` sigils — never `format!`-ed into the message.
Dotted field names follow kameo/otel convention.

### D8 — retire the `eprintln!`s

`log_on_stop_outcome` / `log_on_stop_abandoned` emit `error!` events when the
feature is on and compile to nothing when off; both
`#[expect(clippy::print_stderr)]` markers are deleted. An `on_stop` failure is
silent only when the user explicitly opted out of the default feature.

### D9 — zero-cost-off verification

A gate check builds `--no-default-features` and asserts `tracing` is absent
from `cargo tree` for the production crate. This is the card's "events compile
out" bullet made mechanical. (Reminder: `nix flake check` sources the git
tree — new files must be `git add`ed or the check passes vacuously.)

### D10 — downstream levers, documented not built

- `tracing`'s `release_max_level_*` features let downstream statically strip
  levels at *their* compile time — mechanism ours, policy theirs.
- README gains a short "observability" section: enable a subscriber, otel via
  `tracing-opentelemetry`, opt-out via `default-features = false`.

## Testing (TDD, per the 4 categories)

Capturing subscriber as a small `tracing-subscriber` dev-dep layer in
`test_support` (feature-gated with `test-support` + `tracing`):

- **Sequence/Protocol:** spawn→handle→stop emits lifecycle span with
  `stop.reason` recorded; event order matches lifecycle order.
- **Defensive boundary:** `on_stop` err / panic / abandoned each emit exactly
  one `error!` event carrying `actor.name` + `reason` fields (the eprintln
  replacements can actually fail their assertions).
- **Propagation invariant:** handler-side `actor.handle` span's parent id ==
  the span id current at send time; absent caller span → parents to lifecycle.
- **Restart:** scheduled restart emits `warn` with exact `restart.attempt` /
  `restart.delay_ms` (seeded RNG seam makes delays exact); give-up emits
  `error`.
- **Off-build:** D9's gate check; plus the crate compiles warning-free with
  `--no-default-features`.

Mutants: new functions get `mutants-baseline.json` entries per the baseline
workflow.

## Out of scope (per card)

`console` (tokio-console), `otel` re-export feature, `metrics` — separate
features per #66, own cards at first concrete need.

## Execution deltas

Recorded at close; where a delta touches a decision above, that section is
amended in place (D2, D6).

- **D3 confirmed:** shipped `default = ["tracing"]` in `crates/core/Cargo.toml`.
- **D2 reversed:** cfg concentrated in `trace.rs` mirrored on/off halves; call
  sites cfg-free (see the amended D2).
- **`actor.id` uses the `?` sigil (Debug):** `ActorId` deliberately has no
  `Display` — it is a pure name (ADR-0015) — so the field records its `Debug`
  form, as do `child.id` and `watcher.id`.
- **Envelope span is boxed:** `Option<Box<Span>>`, one word per slot, allocated
  only under an enabled span (see the amended D6).
- **D7 field names as shipped:** the scheduled-restart `warn!` carries
  `restart.attempt`, `restart.delay` (a `Duration`, Debug-formatted — not
  `delay_ms`), and `child.id`; the give-up `error!` carries `restart.rebuilds`
  (the lifetime budget counter), not `restart.attempt`.
- **Death-notice emission covers every delivery edge:** `trace::death_notice`
  fires from the teardown guard (`watch.rs`) *and* from
  `MailboxReceiver::reject_queued_watchers` (`mailbox.rs`), so watchers whose
  `Watch` was still queued at kill / receiver-drop / startup-failure are traced
  too.
