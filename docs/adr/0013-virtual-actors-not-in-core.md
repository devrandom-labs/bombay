# ADR-0013: Virtual-actor (lazy reactivation) lifecycle is not in the core

**Status:** accepted · card #196 (restart & supervision, #120 slice 2)

## Context

While designing #196's restart model, one surveyed system answers failure
completely differently from every supervision-tree design. Orleans' **virtual
actors**: *"Actors are purely logical entities that always exist, virtually. An
actor cannot be explicitly created nor destroyed, and its virtual existence is
unaffected by the failure of a server that executes it."* There is no
supervision tree; the runtime deactivates an idle or failed grain and
re-activates it on the next message, reloading state from storage.

For a **nexus-backed aggregate addressed by a stable identity** this is
arguably a better fit than an eager supervisor: the aggregate is defined by its
event log, not by a live task, so "rebuild on next message from the log" is the
natural recovery, and it composes with the local name registry (#119) and the
identity-first `ActorId` (#121).

The question is whether the bombay **core** should express lazy reactivation —
e.g. a `RestartPolicy::OnDemand` that keeps the child spec and rebuilds on the
next message rather than eagerly on death.

## Decision

**No. Lazy reactivation stays out of `bombay-core`.** #196 ships eager restart
only (`Permanent` / `Transient` / `Never`, rebuild on death).

The core is deliberately transport- and domain-agnostic (CLAUDE.md): the nexus
runner, KERI, and the mobile SDK sit *on top* of it. Virtual-actor semantics are
a **domain** concern — they need a mailbox that outlives its actor, a registry
entry whose actor is absent, and a state source (the event log) the core knows
nothing about. Baking `OnDemand` into the core would pull registry and
persistence coupling into a layer that must not have them.

Lazy reactivation belongs to the `bombay-nexus` layer, built on the primitives
#196 does ship: the erased restart factory (a nexus child's factory rehydrates
from the event log instead of taking plain `Args`), the loop-owned child table,
and the death edge. Recorded here so the branch is not silently re-derived as an
`OnDemand` policy variant in a future core card.

## Consequences

- `RestartPolicy` has exactly three variants; no `OnDemand`. A supervised child
  is rebuilt eagerly or not at all.
- The factory closure is the seam: `bombay-nexus` supplies a factory that
  rehydrates from the log, and the same supervised loop drives it — no core
  change needed to support the virtual-actor layer on top.
- If a future core card is tempted to add lazy reactivation, this ADR is the
  standing decision that it is the sibling layer's job, not the core's.
