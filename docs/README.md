# Bombay documentation

Start with the executable [counter](../examples/counter), then the
[application topology](../examples/application-topology) example. Source and
tests establish the API at the selected lock; prose does not certify behavior.

| Need | Owner |
| --- | --- |
| Existing application usage and source witnesses | [API guide](user-facing-api.md) |
| Planned simplification and paired Behavior PRD | [Developer experience](prds/developer-experience.md) |
| Causal runtime and effect interpretation | [Driver law](driver-law.md) |
| Execution, cancellation and result custody | [Execution contract](prds/execution-ownership.md) |
| Typed capability integration | [Capability interfaces](runtime-capability-interfaces.md) |
| Requirement inventory and selected work | [Backlog](prd-backlog/README.md), [status](prd-backlog/status.md) |

The public API audit, module map, template manifests and backlog evidence are
reference inventories. Verify their entries against code before relying on
them. Historical statuses and test results describe their recorded revisions;
they are not a fresh certification. Product targets such as networking and
durability remain requirements until their owning source and tests exist.

Keep API-local contracts in rustdoc, examples executable, cross-component laws
compact, and unresolved work in one owning PRD. Delete superseded implementation
chronology after preserving its unique obligations and immutable evidence.
Code alone cannot explain every deliberate policy or rejected alternative;
retain that rationale once, alongside the responsible contract.
