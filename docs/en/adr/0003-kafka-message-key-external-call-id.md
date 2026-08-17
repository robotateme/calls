# ADR-0003: Kafka Key Is Always external_call_id

Status: Accepted

## Context

All events for one call must be processed in order. Kafka ordering is guaranteed
inside one partition, and partitioning is driven by the message key.

Calls has a stable business key:

```text
external_call_id
```

## Decision

Kafka key for every message related to one call must be `external_call_id`.

If Kafka key exists and differs from payload `external_call_id`, the message is
a contract violation and goes to DLQ.

## Consequences

- Events for one call stay ordered in one Kafka partition.
- Consumers can reason about late facts and retry attempts safely.
- Producers must not use operator id, phone, topic-local ids, or random UUID as
  key for call events.
- DLQ growth from key mismatch is an upstream contract incident.

## Alternatives

- Use random key.
- Use operator id.
- Use no key.

These were rejected because they break per-call ordering.
