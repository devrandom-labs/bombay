# Bombay test-coverage specification (card #74)

> **Phase 1 deliverable: scenarios only, no wiring.** This tree captures *every* test
> scenario for the surviving kameo local core as executable specifications, with each
> scenario's invariant made explicit. It does **not** contain step definitions or any
> binding to the system-under-test yet. Wiring is a deliberately separate later phase
> (see *Phased plan*).

## Why Gherkin / `cucumber-rs`

We capture scenarios as [Gherkin](https://cucumber.io/docs/gherkin/) `.feature` files,
to be run later by [`cucumber-rs`](https://github.com/cucumber-rs/cucumber) (async-first,
tokio-native — a fit for an async actor framework).

The decision is driven by the phasing the work requires:

| Need | Gherkin / cucumber-rs | `rstest` / plain `#[test]` |
|---|---|---|
| Write scenarios with **zero** implementation | ✅ a `.feature` file is pure text | ❌ the scenario *is* a function body |
| Separate "what we guarantee" from "how we check it" | ✅ feature vs. step definitions | ❌ fused in one place |
| Force the invariant into the open | ✅ you cannot write a `Then` without naming the observable guarantee | ⚠️ optional |
| Survive the M7 de-handroll | ✅ scenarios are implementation-agnostic | ❌ tied to current API |
| Audit the 4 cross-cutting categories | ✅ scenario tags | ⚠️ by convention only |

**Honest trade-off:** `cucumber-rs` is a heavier dev-dependency than `rstest`, and
step-regex matching adds maintenance cost *at wiring time*. For *scenario capture* (this
phase) that cost is zero — feature files are plain text and pull in no dependency until
we wire. When we wire, individual pure helpers (e.g. the `console` `tui` formatters,
`ActorId` byte round-trips) may be cheaper to bind via `rstest` parameterisation behind a
single Gherkin `Scenario Outline`; that is a wiring-phase decision, not a scenario-phase
one.

## The 4 cross-cutting categories (tags)

Per `CLAUDE.md` rule 7, these come first and every scenario carries exactly one as a tag:

- `@sequence` — multi-step interactions on the same object (protocol order), not ops in isolation.
- `@lifecycle` — create / close / corrupt / reopen; for actors: spawn / stop / kill / panic / restart.
- `@boundary` — defensive: feed each unit inputs that violate its upstream crate's guarantees.
- `@linearizability` — concurrent readers + writers with consistency assertions (real overlap).

Supplementary tags:

- `@bug:<n>` — scenario reproduces a known/suspected defect; **must fail today** (never a green test documenting a bug).
- `@unstable-clock` / `@timing` — depends on wall-clock; needs `tokio::time` pause/advance when wired.
- `@core` / `@actors` / `@console` — owning crate.

## Layout

```
tests/features/
  core/      actor_lifecycle, actor_ref, actor_id, mailbox, message, reply,
             request_ask, request_tell, registry, links, supervision, error
  actors/    broker, pubsub, message_bus, message_queue, pool, scheduler
  console/   poller, tui, server_wire
docs/testing/
  README.md      (this file: methodology + gap report + phased plan)
  invariants.md  (the behavioural invariants every feature encodes)
```

## Verified coverage gap (2026-06, grounded in source)

Existing tests counted directly from the tree (`#[test]`/`#[tokio::test]`):

| Section | Tested today | Notes |
|---|---|---|
| core `actor_ref` | 2 | panic/deadlock only — no ask/tell/refcount |
| core `id` | 4 | eq + hash; not generate()/bytes |
| core `reply` | 3 | `ForwardedReply` ctors only |
| core `request/ask` | 8 | + `request/tell` 6 |
| core `supervision` | 29 | best-covered module |
| core `mailbox` `message` `registry` `links` `error` `actor`/`spawn`/`kind` | **0** each | |
| `actors` (all 7 modules) | **0** | broker, pubsub, message_bus, message_queue, pool, scheduler, lib |
| `console` crate (poller, tui) | **0** | |
| in-tree `src/console` + `tests/console.rs` | 6 integ. (happy path) | no error injection |

Totals reconcile with the card: **52 unit + 8 integration** in core; **0** in `actors`; **0** in the `console` crate.

### Confirmed defect (drives a `@bug` scenario)

`actors/src/message_queue.rs:707` — `Pattern::new(&binding.routing_key).unwrap()` in the
Topic `BasicPublish` arm. The `QueueBind` handler (`:591-642`) validates queue existence,
exchange existence, duplicate bindings, and the Headers `x-match` value, **but never
validates that a Topic `routing_key` is a compilable glob**. A malformed key (e.g.
`"[unclosed"`) is accepted at bind and panics the actor run-loop at publish. The
`message_queue.feature` file carries a `@bug` scenario that reproduces the panic and a
companion scenario asserting the *desired* bind-time rejection.

## Phased plan

1. **This phase — enumerate every scenario.** All `.feature` files below, tagged, with
   invariants explicit. No step definitions, no `World`, no SUT binding, no `Cargo.toml`
   change. Scenarios are the spec.
2. **Next phase — a second kind of scenarios.** Property-based / model-based scenarios
   (proptest oracles, linearizability models) layered on top of the example-based ones
   here; plus any scenarios surfaced while reviewing this set.
3. **Final phase — wire them.** Add `cucumber` as a dev-dependency, implement the `World`
   and step definitions per crate, run under `nix flake check`. Only here do scenarios
   start exercising real code.

This ordering is what unblocks re-tightening the god-level clippy bar safely: refactors
under lint pressure become safe once these scenarios are wired and green.
