# ADR-0009 — Registry: papaya directly, no trait seam

**Status:** Accepted (2026-07-18) — decided under card #119

## Context

Card #119's finalized design picked `papaya` as the local name→actor map
(read-heavy lock-free lookups, atomic per-key `compute` for register-once,
no guard-across-`.await` hazard class). The #122 adversarial review added a
note that the registry should sit "behind the `Registry` trait seam" so that

1. a **deterministic impl** could serve loom/DST (review point #8 called
   `papaya` "loom-opaque"), and
2. a single-threaded `Rc`+`HashMap` impl could serve the former-M6 client
   tier (review point #9).

Both premises were re-examined at implementation time.

## Options considered

- **A — `Registry` trait + deterministic test impl** (the card note).
  Rejected on evidence:
  - The DST lane is **MIRI, not loom** (ADR-0005) — loom was already ruled
    out for this codebase because flume ships no loom instrumentation, and
    the same holds for papaya. "loom-opaque" stopped being a cost the moment
    the lane became MIRI.
  - papaya **is MIRI-transparent**: papaya 0.2.4 + seize 0.5.1 interpret
    green under the sweep's exact flags (`-Zmiri-strict-provenance`, pinned
    `nightly-2026-06-15`) — all 17 registry tests pass, including the
    scoped-thread races, ~9 s total. This was verified empirically before
    deciding (papaya's own CI uses `-Zmiri-permissive-provenance`, so the
    stricter flag could not be assumed).
  - A deterministic impl would carry the reimplementation trap: the registry
    logic *is* thin composition over the map primitive, so a second map impl
    under test would shift MIRI coverage off the production interleavings —
    the exact "green lane over the wrong surface" failure #164/#165 exist to
    prevent.
  - The client-tier consumer lives in private sibling repos, not here — no
    second concrete use exists in this workspace (shared rule: add the
    abstraction at the second use).
- **B — concrete `Registry` struct over `papaya`** *(chosen)*. The full
  production type is visible to every lane (unit, MIRI sweep, MIRI
  many-seeds, mutation); the seam can be introduced compatibly later if a
  second in-repo impl ever materializes.

## Decision

`bombay-core/src/registry.rs` implements `Registry` as a concrete struct
over `papaya::HashMap<Cow<'static, str>, Box<dyn ErasedEntry>>`. The only
erasure is the value-side `ErasedEntry` (a private trait over
`WeakActorRef<A>`: liveness probe + `Any` downcast), which is the kameo
`HashMap<ActorId, Link>` shape the #122 review prescribed. Semantics:
register-once decided atomically inside `papaya::HashMap::compute`; dead
entries (channel closed) read as absent on every path and their names are
reclaimed atomically by `register`.

The three scoped-thread race tests are added to `miri.yml`'s many-seeds leg:
plain `std::thread::scope` threads are OS threads under MIRI, so the
tasks>workers probe caveat (a tokio work-stealing artifact) cannot apply.

## Consequences

- MIRI explores papaya's *real* atomics under our tests — no fidelity gap
  between what is model-checked and what ships.
- The registry's correctness under provenance rules now rides our own lane,
  not just papaya's (more permissive) upstream CI; a papaya upgrade that
  regresses strict provenance fails our scheduled sweep loudly.
- No trait indirection on the lookup hot path.
- If a sibling repo ever needs a different registry impl *through this
  crate*, that is the second concrete use — introduce the seam then, as its
  own card.
