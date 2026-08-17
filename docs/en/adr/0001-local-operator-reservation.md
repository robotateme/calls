# ADR-0001: Calls Owns Only Local Operator Reservation

Status: Accepted

## Context

Calls needs to prevent two concurrent jobs from assigning the same operator to
different calls.

At the same time, Calls is not the owner of real operator availability. Real
availability comes from an external read model or service. Calls only needs a
short reservation while it asks Telephony to connect a specific client and
operator.

## Decision

Calls stores only local reservation fields:

```text
operators.reserved_call_id
operators.reserved_at
```

Calls may reserve an operator for one call and release that reservation.

Calls must not set `available=true` after `connected`. Real availability is
owned outside Calls.

## Consequences

- Reservation is short-lived and local to Calls.
- Stale reservations are released by
  `calls:operator-reservations:release-expired`.
- After `connected`, Calls releases reservation and stops owning the flow.
- Operator availability bugs must be fixed in the availability owner, not by
  making Calls invent availability state.

## Alternatives

- Store full operator availability in Calls.
- Ask Telephony synchronously for every assignment.
- Use distributed locks outside PostgreSQL.

These options were rejected because they either blur service ownership or make
the hot path harder to operate.
