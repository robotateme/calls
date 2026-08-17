# ADR

ADR records architecture decisions that should not be lost in code or README.

Each ADR answers:

- what problem was solved;
- what decision was made;
- why this option was chosen;
- what consequences and tradeoffs it has.

ADR does not replace current documentation. Current behavior is described in:

- [solution.md](../solution.md);
- [architecture.md](../architecture.md);
- [kafka-contracts.md](../kafka-contracts.md);
- [production.md](../production.md).

## When to Add ADR

Add an ADR when a decision changes:

- service boundary;
- Kafka contract;
- state machine;
- persistence/reliability model;
- dependency direction;
- operator reservation rules;
- metrics and production behavior.

A new ADR is useful when the decision cannot be safely inferred from code or
README.

## Format

Use:

```text
# ADR-000N: title

Status: Accepted

## Context
## Decision
## Consequences
## Alternatives
```

Keep ADR practical. Do not write generic architecture theory.

## Current ADR

- [ADR-0001: Calls Owns Only Local Operator Reservation](0001-local-operator-reservation.md)
- [ADR-0002: Workers Claim Rows with Row Locks and SKIP LOCKED](0002-skip-locked-for-workers.md)
- [ADR-0003: Kafka Key Is Always external_call_id](0003-kafka-message-key-external-call-id.md)
- [ADR-0004: /metrics Renders Cached Metrics Only](0004-metrics-scrape-from-cache.md)
- [ADR-0005: Internal Architecture Is Split into Layers](0005-internal-architecture-boundaries.md)
- [ADR-0006: Kafka Is the Authoritative Service Channel](0006-kafka-as-authoritative-service-channel.md)
