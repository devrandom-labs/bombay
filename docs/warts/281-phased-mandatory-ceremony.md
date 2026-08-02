# Wart (card #281): `Phased` charges for machinery a policy never uses

Surfaced by the job-queue walking skeleton's phased `Worker`: its gate is
all-`Deliver` (a draining worker must refuse loudly, so nothing is ever
deferred), yet `PhasePolicy` still demands `stash_capacity` — the Worker
writes a ceremonial `Capacity::new(NonZeroUsize::MIN)` for a stash that
can never hold a message. Symmetrically, a machine that declares no
deadline anywhere (`phase_deadline` always `None`) must still write
`on_phase_timeout`.

The required-items rule is deliberate (the #196 no-default precedent: a
declared deadline with a defaulted reaction is a silent pair) — the wart
is that the requirement is not CONDITIONAL on the declaration. Candidate
shapes for the fix: split the optional seats into composable policy
sub-traits, or let the #243 derive / the actor-builder arc (#286) infer
"never defers" / "no deadlines" from the declaration table and absorb the
dead items. Fix card: #290.

**FIXED (card #290, ADR-0028).** `PhasePolicy` declares its seats as
plugged strategy types: `type Deferral = NoDefer` (no token, no stash —
the gate's verdict type `Disposition<Never>` cannot even spell `Defer`;
the stash type is a ZST) or `Bounded<SP>` over the reused `StashPolicy`;
`type Timeout = NoTimeout` or a `DeadlinePolicy<ByPhase<Self>>` seat
(one context-generic trait now serves `Deadlined` and `Phased`). The
Worker's `Capacity::MIN` ceremony is deleted; its machine reads
`Deferral = NoDefer` + a `DrainGrace` seat. The silent-pair law survives
structurally in both directions.
