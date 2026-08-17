# ADR-0006: Kafka Is the Authoritative Service Channel

Status: Accepted

## Context

Calls processes service events around inbound calls. These events are produced by
Telephony or a gateway, not by user HTTP requests.

The system needs:

- durable ingress;
- per-call ordering;
- consumer groups;
- replay/recovery;
- explicit DLQ for bad messages;
- outbox-based command publishing.

## Decision

Kafka is the authoritative ingress and egress service channel.

Inbound calls arrive on `incoming-calls`.
Telephony facts arrive on `telephony.facts`.
Calls commands are published to `telephony.commands` from `telephony_outbox`.

HTTP surface remains limited to `/metrics`.

Kafka key for call messages is `external_call_id`; see [ADR-0003](0003-kafka-message-key-external-call-id.md).

## Consequences

- Calls is backend-only and has no user-facing frontend.
- Kafka contracts are part of the public service contract.
- Bad messages go to DLQ instead of being guessed.
- Redis queue remains internal implementation detail.
- Operators run Kafka consumers and outbox publishers as production processes.

## Rejected

- HTTP as primary ingress.
- Redis queue as inter-service contract.
- Direct Telephony calls from handlers.
- Publishing commands outside `telephony_outbox`.
