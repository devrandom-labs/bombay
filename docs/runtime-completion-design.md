# Bombay completion design navigation

Status: superseded planning draft, consolidated on 2026-09-29.

The earlier long-form proposal mixed a feature inventory, proposed API syntax,
and a transport-pluggability experiment. The subsequent product direction
selects **Zenoh as the sole production networking substrate**, with typed
identity integration and a deterministic network test host. There is no
requirement to implement interchangeable production transports.

Use these documents instead:

- [Completion requirements and PRD groups](prd-backlog/README.md)
- [Selected versions and source evidence](prd-backlog/evidence.md)
- [Zenoh, node AIDs and application experience](prd-backlog/zenoh.md)
- [Local runtime completion](prd-backlog/local-runtime.md)
- [Networking requirements](prd-backlog/networking.md)
- [Identity and placement](prd-backlog/identity-and-placement.md)
- [Durability and operations](prd-backlog/durability-and-operations.md)
- [Failure rules and properties](prd-backlog/failure-contracts.md)

The former illustrative `.transport(...).identity(...).distribution(...)`
syntax was never an implemented API. Public composition remains subject to
ordinary-Rust experiments and executable evidence.

Behavior Actors owns the existing supervisor/pool policies; Bombay now
interprets all 19 inventoried actor-owned requests, while broader policy
verification is recorded in [the backlog status index](prd-backlog/status.md)
and each selected feature PRD. Mnesis owns persistence,
mnesis-bombay owns its execution
integration, and Selo will supply KERI identity downstream using that stack.
Node identity, actor identity and hosting authority remain distinct.

This short page preserves existing links without maintaining a competing
completion specification. The PRD backlog and its status index select work;
each feature PRD records implementation eligibility and acceptance evidence.
