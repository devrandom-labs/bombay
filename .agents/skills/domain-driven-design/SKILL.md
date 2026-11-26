---
name: domain-driven-design
description: Model business rules with shared domain language, bounded contexts, aggregates and explicit consistency boundaries. Use for domain discovery, context mapping, aggregate design, or reviewing domain behavior and ports in this server. Keep implementation evidence in the existing project workflow.
---

# Domain-Driven Design

## Project use

Start with current server documents in docs/ and [decision records](../../../docs/adr)
when present. Use confirmed rules
and distinguish proposals from decisions. Ask the founder only for missing
business rules. Existing code is evidence of behavior, not approval of a model.
Keep each fact in its owning document; do not create a second glossary or context
catalogue. Follow the owning PRD's verification plan for implementation.
For Server, [Loco](../loco/SKILL.md) overrides generic layer prescriptions below: models
own validation, business rules and persistence. Do not create service, repository
or store layers or duplicate domain/persistence types.

Labcast already selects current-state CRUD with Loco/SeaORM and direct live streaming.
Use the current ADR index and owning architecture contracts before general pattern
guidance below. Domain modeling does not reopen persistence, worker placement or
the selected SQL intent and Pub/Sub saved-work boundary.

For each operation, identify the actor, current authority, invariant, state
transition, transaction owner and observable failure. Server is a simple backend:
one authorization check per request, last write wins, and a failed write is an
ordinary error ([framework writes](../../../docs/arch/server/framework.md#writes)).
Check stale workers against confirmed architecture and public interface contracts. When those contracts do not exist,
make the gaps explicit and establish them with the owner before implementation.
A pattern must preserve confirmed contracts before it can simplify the design.

## When to Apply DDD

DDD addresses domain complexity (business rules, language, boundaries), not technical complexity (scaling, performance). Apply selectively.

| Domain Type (Cynefin) | DDD Investment | Approach |
|------------------------|----------------|----------|
| Clear/Simple | None -- use CRUD | Standard patterns, no modeling overhead |
| Complicated | Tactical only | Expert analysis, pragmatic patterns suffice |
| Complex | Full strategic + tactical | Iterative modeling, bounded contexts, continuous refinement |
| Chaotic | Stabilize first | Emergency patches, then apply DDD incrementally |

### Signs You Need DDD
- Domain experts and developers frequently misunderstand each other
- Business rules are complex, interconnected, frequently changing
- Multiple teams work on the same codebase with conflicting models
- System has grown into a "big ball of mud"
- Business logic scattered across services, controllers, database procedures

### Signs DDD Is Overkill
- Simple CRUD without complex business rules
- Technical complexity outweighs domain complexity
- Small team, small codebase, single bounded context
- For simple microservices, anemic domain model is acceptable (not an anti-pattern)

## Strategic DDD

### Subdomain Classification

| Criterion | Core | Supporting | Generic |
|-----------|------|------------|---------|
| Competitive advantage | Yes | No | No |
| Unique to organization | Yes | Partially | No |
| Build or buy | Build | Build (simplified) | Buy/integrate |
| DDD investment | Full strategic + tactical | Pragmatic tactical | Minimal/none |

### Bounded Contexts

A bounded context is the boundary within which a particular domain model is defined and applicable. Different from subdomains: a bounded context is a software boundary; a subdomain is a problem-space concept. They should align but are not the same.

**Discovery techniques**:
- **Language divergence**: same term means different things to different groups -- you have crossed a boundary
- **Organizational structure**: different departments often correspond to different contexts
- **Consistency requirements**: where true invariants must be maintained transactionally
- **EventStorming**: Big Picture workshops surface boundaries through hotspots and swimlane divergences
- **Domain Storytelling**: actors, work objects, activity arrows reveal natural boundaries

**Common mistakes**:
1. Confusing contexts with subdomains (software boundary vs. problem-space concept)
2. Assuming one microservice = one bounded context (Evans calls this an "oversimplification")
3. Making contexts too large (big ball of mud with conflicting models)
4. Making contexts too small (excessive cross-context communication overhead)

### Context Mapping Patterns

Nine patterns organized by team relationship type.

| Pattern | Relationship | When to Use |
|---------|-------------|-------------|
| Partnership | Mutually dependent | Teams must deliver together; true reciprocal dependency |
| Shared Kernel | Shared model subset | Close coordination around common concepts. Keep small |
| Customer-Supplier | Upstream-downstream | Asymmetric dependencies requiring structured negotiation |
| Conformist | Downstream adopts upstream | Integration simplicity outweighs design freedom |
| Anti-Corruption Layer | Isolating translation | Protecting domain from poor upstream models; legacy integration |
| Open Host Service | Standardized API | Supporting multiple downstream teams |
| Published Language | Shared schema/format | Standardized formats for inter-context translation (iCalendar, vCard) |
| Separate Ways | No connection | Contexts have no meaningful interdependencies |
| Big Ball of Mud | Recognition only | Demarcate and quarantine legacy/poorly-structured systems |

**Relationship categories**: Mutually Dependent (Partnership) | Upstream/Downstream (Customer-Supplier, Conformist, ACL) | Free (Separate Ways)

**Evolution**: context maps evolve during refactoring. A Conformist relationship can be refactored to Customer-Supplier with an ACL. Draw context maps before writing code.

### Ubiquitous Language

A common, rigorous vocabulary shared between developers and domain experts, scoped per bounded context.

**Rules**:
1. Never use generic technical terms when a domain term exists
2. If developers and domain experts use different words for the same concept, resolve the conflict
3. When the model becomes hard to express in code, the language needs refinement
4. The same word may mean different things in different contexts -- this is expected

Code must reflect the language: class names, method names, variable names use domain terms. When the language changes, the code changes.

## Tactical DDD

### Aggregates and consistency

1. **Model true invariants in consistency boundaries**: only include elements that must be consistent within the same transaction
2. **Keep aggregates focused**: include the state needed to protect the invariant. Do not choose one aggregate per noun or bounded context.
3. **Reference other aggregates by identity where appropriate**: a typed ID avoids exposing another aggregate's internals. It does not decide transaction scope.
4. **Choose consistency per operation**: use eventual consistency only when the business rule permits delay and its failure behavior is explicit. One aggregate per transaction is a design preference, not a universal limit. Preserve the owning workflow's atomic authority checks and writes. If several aggregates must change together, inspect the boundary and state the transaction contract; do not split the write merely to satisfy a pattern.

**Common mistakes**: including objects for compositional convenience | "god aggregates" | direct object references | requiring immediate consistency where eventual suffices

### Entities vs. Value Objects

| Criterion | Entity | Value Object |
|-----------|--------|--------------|
| Identity matters | Yes (tracked over time) | No (interchangeable if same values) |
| Mutability | Mutable (state changes) | Immutable (replace, do not modify) |
| Equality | By identity (ID) | By attribute values (structural) |
| Examples | Customer, Order, Account | Money, Address, DateRange, Email |

**Self-validating value objects**: the only way for a value object to exist is to be valid. Validate in constructor; reject invalid input. Eliminates scattered validation logic.

**Context sensitivity**: whether something is Entity or Value Object depends on the bounded context. Address is a VO in e-commerce but an Entity in utility billing.

### Domain Events

Represent something that happened. Named in past tense: `OrderPlaced`, `UserRegistered`, `PaymentProcessed`.

- **Domain events** record business facts within a context. Pure decisions and replay do not perform external effects. An event does not require a mediator library or a message broker.
- **Integration events** carry committed facts across a boundary when asynchronous delivery fits its contract. State delivery, ordering, deduplication and recovery guarantees. If publication must survive a crash after commit, use a verified durable handoff. Synchronous context calls remain valid when their transaction and failure contracts require them.

### Repositories

Use narrow persistence ports where workflows need them. An aggregate can be a useful
load/save boundary; it does not require its own repository trait or library.
Keep SQL and provider types in adapters. The workflow owns the transaction across
all required reads and writes. Do not make each repository commit independently.

- **Collection-oriented**: add/remove/find operations, repository tracks changes (Unit of Work)
- **Persistence-oriented**: explicit save/load through the owning transaction

### Domain Services

Stateless operations spanning multiple aggregates. Named using ubiquitous language. Operate on domain types only.

Danger: overuse strips entities of behavior, creating anemic domain models.

| Aspect | Domain Service | Application Service |
|--------|---------------|-------------------|
| Contains | Domain logic | Orchestration logic |
| Dependencies | Domain types only | Domain objects + infrastructure ports |
| Layer | Domain model | Application |
| Example | TransferFunds, CalculateDiscount | PlaceOrderHandler, RegisterUserUseCase |

## Integration Patterns

### DDD + Hexagonal Architecture

Keep domain types and rules in core without HTTP, SQL or provider SDK dependencies.
Application workflows coordinate domain operations and transactions. Inbound ports
expose use cases; outbound ports express needed external capabilities; adapters
implement those contracts and translate foreign models. In Rust, keep state private
and validate construction, reconstruction and transitions. Add traits and separate
representations only for a real boundary. A context does not imply a separate crate,
service or database.

### Current-state persistence

For Labcast, model invariants around the selected CRUD state and SeaORM transaction
owners. Reads enforce business access. Durable background delivery follows the
accepted SQL intent and Pub/Sub contract; live observations follow the Studio streaming contract.
Do not introduce another persistence architecture through generic DDD guidance.

### DDD + Microservices

Bounded contexts provide decomposition tool, but the mapping is not one-to-one. A bounded context may contain multiple services; a microservice may handle a focused subdomain within a larger context.

**Decomposition criteria**: subdomain boundaries (primary) | single responsibility | common closure | team autonomy | loose coupling

**Cross-context operations**: retain an explicit shared transaction where the selected
store and invariant support it. Across independent systems, specify partial failure,
uncertainty and recovery. Consider a saga only when compensation is valid; compensation
does not restore atomicity or undo every external effect.

## Anti-Patterns

| Anti-Pattern | Signal | Fix |
|-------------|--------|-----|
| Anemic Domain Model | Entities = data only, logic in services | Move business rules into entities/aggregates |
| God Aggregate | Too many entities/VOs, concurrency contention | Split by true invariant boundaries |
| Domain Logic in App Layer | Workflows redefine business invariants | Put domain decisions in core; keep coordination and transaction ownership in workflows |
| Primitive Obsession | Raw values erase an invariant or identity distinction | Use validated value types where they preserve a meaningful rule |
| Missing Boundaries | Single unified model for entire system | Identify bounded contexts via language divergence |
| Events as RPCs | An event is used as an implicit request requiring a reply | Use an explicit command contract; preserve required event delivery and failure reporting |
| Database-Driven Design | Entities mirror tables | Model domain first, map to persistence second |

## EventStorming Quick Reference

Three levels: **Big Picture** (business scope) | **Process Modeling** (workflow) |
**Design Level** (domain rules and boundaries). Use the smallest discussion needed
with the founder and relevant owners; workshop size and separate artifacts are optional.

Key artifacts: Domain Events (orange, past tense) | Commands (blue, imperative) | Aggregates (yellow, consistency boundaries) | Policies (lilac, automation rules) | Read Models (green, decision info) | External Systems (pink) | Hotspots (red, problems/questions).

Start with chaotic event exploration, enforce timeline, add commands/actors, identify aggregates/policies, mark hotspots. Iterate between divergent and convergent phases.
