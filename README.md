# bombay

**Bombay** — a Zenoh-native fork of the [kameo](https://github.com/tqwewe/kameo) actor framework: event-sourced, identity-bearing, dataspace-native actors. It replaces kameo's libp2p `remote` layer with a thin layer over a Zenoh `Session`, and adds an adapter that maps [nexus](https://github.com/devrandom-labs/nexus) event-sourced aggregates onto actors (actor = single-writer consistency boundary).

## Bombay + Zenoh + nexus (the co-design)

Bombay sits between two systems and is designed *with* both:

- **Zenoh** owns **transport / addressing / discovery** — actors are addressable in the dataspace by key-expr; `ask` = query/reply, `tell` = put, death-watch = a liveliness token per actor.
- **nexus** owns **consistency / persistence / ordering / replay** — the event log, single-writer-per-aggregate via optimistic concurrency (`Version`), at-least-once + idempotent/exactly-once replay, the forever-driver subscription cursor.
- **Bombay** is the **adapter** that maps nexus aggregates onto Zenoh-addressed actors and builds the runtime nexus ships only primitives for (the loop, dispatch, cursor, lifecycle, supervision, command/projection/saga runners).

Because we control both nexus and bombay, a Zenoh limitation is rarely a Bombay limitation: **for any Zenoh gap, ask whether nexus already covers it, and if not, patch whichever layer gives the best result.** (E.g. Zenoh's per-message "reliability" is only a link-selection marker — real delivery comes from nexus's event log + replay; Zenoh has no in-SDK storage — nexus *is* the store.)

## Status

**Planning / M0 (pre-flight).** No production Rust has landed yet. The M0 de-risking is done:
- **#62 walking-skeleton spike — GO.** `ask`/`tell`/liveliness/health validated over Zenoh 1.9.0; sub-ms messaging; crash-detection 2 ms vs partition-detection ~10 s (lease); single-writer must come from nexus, not Zenoh addressing.
- **#64 feature matrix** — Zenoh core vs zenoh-ext vs zenoh-pico mapped, with stability gates and per-card caveats.

Next: **#63 fork strategy** (gate into M1, the actual fork of kameo).

## Reference docs

Distilled, AI-referenceable knowledge lives under [`docs/`](docs/). **Read the relevant doc before working a card:**

- [`docs/zenoh/capabilities.md`](docs/zenoh/capabilities.md) — Zenoh capability matrix (core/ext/pico), stability gates, and the per-card "gap → nexus/bombay coverage" flags. Read before any card touching Zenoh.

The work is GitHub-project-cards-driven with TDD; see [`CLAUDE.md`](CLAUDE.md) for the working method, milestones, and engineering rules.

## Local setup

Enable the pre-commit hook once per clone — it enforces the README-with-every-commit discipline (and will also run `nix flake check` once the Nix harness lands, #60):

```bash
git config core.hooksPath .githooks
```
