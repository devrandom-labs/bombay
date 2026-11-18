# Rejected DevX hypotheses

## Definition identity in native activation errors (2026-11-18)

Disposition: **reopen**. Start the next hypothesis from signed67131997.

The candidate made `EntityActivationError<D>` nominally specific to an
`EntityDefinition`. It derived hydration, actor and descendant payload types
from that definition and named the complete joined result with a transparent
`EntityRetirement<D>` alias. This was an interface policy, not an actor-model
law. Four callback phases and all original runtime values remained unchanged.

The policy failed existing composition. In
`crates/bombay/tests/completed_application_receiving.rs`, both
`RetiringAccounts::activation_failed` and `AccountFamily::activation_failed`
forward the exact owned error directly to `Accounts::activation_failed`.
Their identity and payload types are equal. Changing only the definition
parameter blocks these transfers without preserving any additional required
fact or authority. Rebuilding the variants or replacing those consumers would
hide the model defect. The actual equal-payload E0308 static witness proves
that restriction; it does not establish a need for the restriction.

Focused current/minimum debug/release executions, loss controls and restored
controls passed on source a3231260. Their narrow review missed these delegated
consumers. Author inventory and independent review then found the falsifier,
overturning the narrow no-veto conclusion before any mechanical migration.
The13-path caller candidate remains unapplied. All production/caller Rust is
restored exactly; the two invented-policy static files are removed. Exact
source, archive, raw outcomes, candidates and reviews remain preserved in the
existing handoff. Reopening receipt:2d3a831c76050ab3e6818b87fbc0311e3e5e99e6b35e6df0c0ddb86f01d1289d.

Before/after rollback: EntitySlot retains its five states; every subordinate
alternative retains the same owned values. Entity modules10, match-arm tokens192
and existing actor/runtime branches are unchanged. Physical production lines
return4869->4847; public declaration tokens return93->92 under the exact
recorded regex. The error retains four alternatives and returns from one
nominal definition parameter to its three original payload parameters. No
new runtime state, arrival history, repeated cause, cardinality assumption,
semantic boolean, erased value or nested transition authority survives.

The next hypothesis must preserve the owning native payload equation and
original same-payload delegation. Record that complete consumer law and a
regression before production; do not carry forward this nominal restriction,
convert rejected ownership, add a callback or invent a bound from diagnostics.
Recheck DX03/17/22/26, D-RETIRE-1/D-TERM-1 and the locked Behavior instructions.
A transparent result spelling is a research option, not an accepted model.

## Definition-derived activation alias (2026-11-18)

Disposition: **reopen** for replacing the canonical error name while retaining
inferred variant paths. The real Accounts caller temporarily used a transparent
alias over the unchanged three-payload native error and complete joined result.
Unlike the rejected nominal representation, these aliases preserve equal-payload
identity. Pinned current Cargo still refused101 with one E0284 at the unchanged
`EntityActivationError::ResidentCapacity` match: it cannot infer a definition
from those associated payload projections. No library production was changed.
The exact original caller was restored, all730 inputs/modes matched, and its
original check passed0. Actual probe/restoration records remain under `aliases`
in the existing handoff. No explicit-definition variant path, extra bound or
wrapper is accepted as a repair. The joined-result alias needs a separate
focused hypothesis; it uses ordinary `Ok`/`Err` and adds no error identity.
