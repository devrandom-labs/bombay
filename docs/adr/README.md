# Architecture Decision Records (ADRs)

Durable records of **why** we picked what — the decision, the options we weighed,
the evidence, and the consequences we accepted. An ADR is written when a choice
is (a) hard to reverse, (b) shapes downstream cards, or (c) would otherwise be
re-litigated from memory. It captures the *reasoning at decision time*, not a
running status.

Distilled, still-true knowledge lives in [`docs/`](../); ADRs are the audit trail
behind it. If an ADR is later overturned, add a new ADR that supersedes it and
flip the old one's status to `Superseded by ADR-NNNN` — never rewrite history.

## Format

Each ADR is `NNNN-kebab-title.md` with: **Status** · **Context** · **Options
considered** (with the evidence — benchmarks, primary sources) · **Decision** ·
**Consequences**. Keep it to the essentials; link to code/benches/cards.

Status is one of: `Proposed` · `Accepted` · `Superseded by ADR-NNNN` · `Rejected`.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-mailbox-channel-primitive.md) | Mailbox channel primitive | Accepted |
| [0002](0002-reply-channel-primitive.md) | Reply channel primitive | Accepted |
