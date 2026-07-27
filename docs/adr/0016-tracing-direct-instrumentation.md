# ADR-0016 — Observability: direct `tracing` instrumentation, no observer seam

**Status:** Accepted (2026-07-27) — decided under card #209 (design record:
[`docs/superpowers/specs/2026-07-27-209-tracing-feature-design.md`](../superpowers/specs/2026-07-27-209-tracing-feature-design.md))

## Context

The core had no observability: three `eprintln!` placeholders sat in the
teardown path (`log_on_stop_outcome` ×2, `log_on_stop_abandoned`), each
surfacing a real defect (an `on_stop` that errors / panics / outlives its
grace) but unstructured, unfilterable, and stderr-only. The bar is
axum-grade library instrumentation: structured spans/events a downstream app
can route to fmt logs, OpenTelemetry, or tokio-console — while bombay itself
never installs a subscriber (tracing docs: libraries must not call
`set_global_default()`) and an opted-out build pays nothing.

## Options considered

- **A — direct `tracing` instrumentation** *(chosen).* `tracing` **is** the
  hook seam: its `Subscriber` trait is the observer pattern, so otel
  (`tracing-opentelemetry`), fmt, and console attach downstream with zero
  bombay code, and `RUST_LOG` filtering comes free. A disabled callsite costs
  one static-atomic interest check (tracing's callsite-registry design).
  hyper, h2, tokio, and the kameo oracle all emit tracing directly.
- **B — own `ActorObserver` trait** (Elixir `:telemetry` style). Rejected:
  reinvents tracing's dispatch minus the ecosystem — no free otel bridge, no
  filtering, and no span *context* (scoped enter/exit across polls), which is
  the hard part. Rust prior art for this shape is near-zero.
- **C — `log` facade.** Rejected: events only — no spans, no trace trees, so
  the cross-actor stitching that motivates the card is unbuildable on it.
- **Envelope sub-choice — inline `Span` vs boxed.** An inline `Span` field on
  `Signal::Message` broke the #114 slot-size tripwires (`Signal<Probe>` grew
  to 56 bytes): every mailbox slot of every actor would carry a full `Span`.
  Rejected for `Option<Box<Span>>`.

## Decision

1. **tracing-direct.** Bombay emits `tracing` spans/events with structured
   fields and never installs a subscriber; consumers attach downstream.
2. **Default-on feature**, matching axum and kameo: `default = ["tracing"]`,
   with a minimal dep (`default-features = false, features = ["std"]` — no
   `attributes` proc macro; all spans are manual).
3. **Single cfg surface.** All `#[cfg(feature = "tracing")]` lives in
   `crates/core/src/trace.rs` as two mirrored `imp` halves (real impl / no-op
   ZSTs and `const fn` stubs with identical signatures); every call site is
   cfg-free. Per-site cfg was rejected: the envelope field and `instrument`'s
   `impl Future` return force cfg-carrying types anyway, so gating per site
   would smear conditionals across four modules instead of one.
4. **`follows_from` spawn linkage, not parent.** The root `actor.lifecycle`
   span (`parent: None`) links `follows_from` the spawn-site span: the actor
   outlives its spawner, and parent-child implies containment. Otel renders
   the link as a span link.
5. **Boxed envelope propagation.** The sender's span rides `Signal::Message`
   as `SendContext { caller: Option<Box<Span>> }` — one word per slot (niched
   on `None`), boxed only when the sender is inside an *enabled* span, so an
   unobserved send allocates nothing (the #207 zero-alloc tell holds).
6. **Zero-cost off, enforced mechanically.** The `bombay-tracing-off` flake
   check builds `--no-default-features` and fails if the `tracing` crate
   appears in the resolved normal-dep graph.

## Consequences

- otel/fmt/console integration is downstream configuration, not bombay code;
  a subscriber sees the lifecycle span (with `stop.reason` recorded at
  teardown), per-message `actor.handle` spans parented to the caller's span
  (cross-actor traces stitch into one tree), and error-level events for every
  lifecycle failure.
- Feature on with no subscriber: one static-atomic interest check per call
  site, zero allocations per send. Feature off: everything compiles out and
  the dependency disappears — proven by the gate, not asserted.
- Every mailbox slot pays one word for trace context while the feature is on,
  even for apps that never install a subscriber — the price of default-on,
  pinned by the `mailbox.rs` slot-size tripwires so it can never silently
  grow.
- `trace.rs`'s mirrored halves must stay signature-identical; the off half is
  compile-checked only by the `bombay-tracing-off` gate (the default test
  suite never sees it).
- `metrics`, `console` (tokio-console), and an `otel` re-export remain
  separate features per #66, on their own cards.
