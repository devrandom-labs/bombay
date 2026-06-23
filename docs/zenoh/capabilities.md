# Zenoh capability reference (read before any card touching Zenoh)

Distilled from the M0 source dig + walking-skeleton spike. **Full detail and measurements live on the issues — this is the actionable summary.**
- Spike findings (latencies, crash-vs-partition, uniqueness): **issue #62**
- Full feature matrix (every cell sourced, incl. pico): **issue #64**

> **How to use this:** before implementing a card that touches Zenoh, check the matrix for *availability + stability gate*, then the card-flags table for *whether the gap is real or covered by nexus*. Do not assume a Zenoh feature exists, is stable, or behaves as named — verify here first, and against the crate source if unsure.

## Pinned versions
`zenoh = 1.9.0` · `zenoh-ext = 1.9.0` · `zenoh-pico = 1.9.0` (latest 1.x; the cards' "1.7.2" is superseded — we use latest).

## The co-design principle (why a Zenoh gap is often a non-problem)

**Zenoh owns transport / addressing / discovery. nexus owns consistency / persistence / ordering / replay.** Bombay is the adapter between them, and **we control both nexus and bombay** — so for every Zenoh limitation, the question is not "can Zenoh do it?" but "**does nexus already cover it, and if not, which layer do we patch for the best result?**" Most gaps below are squarely nexus's domain. (Verify nexus claims against the integration contract, issue #59.)

## Availability matrix

**Legend:** ✅ core-stable · 🅤 unstable-gated (`zenoh/unstable`) · ⚙️ cargo-feature (non-default) · 🔌 plugin (out-of-SDK) · ❌ absent.

| Feature | core | ext | pico | Card | Stability |
|---|---|---|---|---|---|
| ask (queryable+get) / tell (put+sub) | ✅ | — | ✅ | #4/#2 | stable |
| liveliness death-watch | ✅ | — | ✅ | #8 | stable |
| QoS priority (8 lanes) + congestion (Block/Drop) | ✅ | — | n/v | #17 | stable (not on replies) |
| per-message reliability | 🅤 | — | n/v | #18 | **unstable** |
| mailbox: Fifo / Ring(keep-latest) | ✅ | — | n/a | #19 | stable |
| selectors / parameters | ✅ | — | ✅ | #21 | stable (`_time` 🅤) |
| attachments / encoding | ✅ | — | ✅ | #22/#23 | stable |
| typed serialization (`z_serialize`) | ❌ | ✅ | n/a | #23 | stable, **no derive** |
| HLC timestamps (uhlc) | ✅ | — | ✅ | #24 | stable, **off by default** |
| storages / persistence | ❌ | 🔌 | ❌ | #12 | plugin only |
| advanced pub/sub (history/recovery/heartbeat/sample-miss) | ❌ | 🅤 | ⚙️off | #25 | **unstable** |
| fetch-current-on-join | ❌ | 🅤 | ⚙️off | #26 | unstable (old API deprecated → `AdvancedSubscriber.history()`) |
| group membership / leader | ❌ | 🅤 | n/a | #11/#13 | **unstable** |
| SHM zero-copy | ⚙️🅤 | — | ❌ | #28 | feature + unstable |
| transport diversity | ✅ 7 default | — | TCP/UDP | #29 | Serial/Vsock/UnixPipe opt-in |
| zero-config clustering (multicast+gossip) | ✅ | — | client+peer | #30 | stable (TTL=1 LAN) |
| `replier_id` (who answered) | 🅤 | — | n/a | #2 | **unstable — not needed**, use `Sample::key_expr()` |

`n/v` = not verified for pico; `n/a` = not applicable.

## Card-actionable flags (Zenoh gap → coverage)

These are the cards whose naive premise is wrong, and how the bombay+nexus plan covers each. **Read the relevant row before starting the card.**

| Card | The trap | Coverage / what to actually do |
|---|---|---|
| **#12 storages** | There is **no in-SDK storage API**; `zenoh-plugin-storage-manager` is a deployable dylib. | **nexus is the event store** — aggregate state never needs Zenoh storages. Zenoh storages, if ever used, are only for read-model caching, which are nexus projections (#69) anyway. |
| **#18 reliability** | The flag is a **link-selection marker — no retransmission**. "BestEffort vs Reliable" is not delivery semantics. | **nexus owns delivery:** event log + at-least-once + idempotent/exactly-once replay; the subscription cursor re-reads from the log (forever-driver, never `None`). Wire retransmission is moot. If a *live* recovery channel is wanted, use #25 advanced pub/sub (unstable) — but it's belt-and-suspenders over the log. |
| **#23 serialization** | `z_serialize`/`z_deserialize` is **zenoh-ext, not core, and has no derive macro**. | **nexus events already serialize** (their own codec); Bombay carries them as **opaque Zenoh payloads** + an `Encoding` hint. Don't reach for zenoh-ext serialization for domain types. |
| **#24 HLC** | `uhlc` timestamps exist but **timestamping defaults OFF** for peers/clients. | **nexus metadata (#71: correlation/causation/trace/HLC)** is the source of causal ordering; if Zenoh-side timestamps are wanted, enable `timestamping/enabled` explicitly. Don't assume samples carry a timestamp. |
| **#26 fetch-on-join** | The named `QueryingSubscriber` is **deprecated**. | Use `AdvancedSubscriber.history()` (unstable). Or note nexus replay already gives state-on-join via the cursor. |
| **single-writer** (cross-cutting) | Zenoh **does not enforce one-queryable-per-key**; duplicates are silently consolidated (see #62). | **nexus optimistic concurrency on `Version`** is the guarantee; Bombay's `ask_one` `Ambiguous` is only a diagnostic. Addressing-uniqueness is best-effort routing (#3), not a safety mechanism. |

## The cross-cutting decision for #66 (crate/feature architecture)

A large fraction of M3–M4 is **`unstable`-gated**: reliability (#18), time-range (#21), SHM (#28), and **all of zenoh-ext** — advanced pub/sub (#25), fetch-on-join (#26), group (#11/#13). The "no `unstable`" stance adopted for identity (#62) is **incompatible** with those cards.

➡️ **#66 must consciously decide:** enable `zenoh/unstable` (behind a Bombay feature flag, pinning `zenoh = "=1.9.0"` and watching for churn across 1.x) — recommended — or forgo those cards. Where a feature is unstable *and* nexus already covers the need (e.g. #18, #25 recovery), prefer the nexus path and keep `unstable` off that path.
