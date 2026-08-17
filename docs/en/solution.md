# Solution

This document describes how Calls processes a call.

## Main Points

- Inbound calls come from Kafka, not HTTP.
- One call is identified by `external_call_id`.
- Kafka key for one call is also `external_call_id`.
- Calls owns the flow only until `connected`.
- Every Telephony command is first written to `telephony_outbox`.

Kafka contracts: [kafka-contracts.md](kafka-contracts.md).
Accepted decisions: [adr/README.md](adr/README.md).

## Responsibility Boundary

Calls owns a short part of the process:

1. Receive an inbound call.
2. Find the client.
3. Find and reserve an operator.
4. Ask Telephony to connect client and operator.
5. Receive the fact that connection succeeded or failed.

Calls does not manage the conversation after `connected`. If a hangup or drop
arrives after connection, it does not change the Calls business status.

## Call Path

1. Kafka sends `incoming_call_registered`.
2. Calls creates a row in `calls` with status `new`.
3. If the same `external_call_id` already exists, no new row is created.
4. Calls enqueues an internal processing job.
5. The handler looks up client and operator.
6. If an operator is found:
   - Calls creates a reservation;
   - moves the call to `assignment_requested`;
   - writes `call_assignment_requested` to `telephony_outbox`.
7. If no operator is available:
   - call policy chooses retry or final outcome;
   - Calls writes `operator_search_retry_scheduled` or
     `operator_search_exhausted`.
8. Publisher sends outbox commands to Kafka.
9. Telephony sends facts:
   - `operator_dialing`;
   - `bridge_established`;
   - `operator_no_answer`;
   - `operator_leg_dropped`;
   - `hangup`.
10. On `connected`, Calls releases the reservation and finishes its work.

The key reliability rule: status change and Telephony command are persisted in
one DB transaction. If the DB commit succeeds, the command is not lost. If the DB
rolls back, the command must not be published.

## Statuses

- `new` - the call was just recorded.
- `waiting` - waiting for the next operator search attempt.
- `assignment_requested` - an operator is reserved and Telephony has or will get
  the command.
- `operator_dialing` - Telephony is dialing the operator.
- `connected` - client and operator are bridged.
- `missed` - connection failed.
- `callback_missed` - connection failed and callback is required.
- `hangup_on_retry` - client hung up while Calls was waiting for retry.

Transitions:

- `new/waiting -> assignment_requested` - operator found;
- `assignment_requested -> operator_dialing` - Telephony dials operator;
- `assignment_requested/operator_dialing -> connected` - bridge established;
- `assignment_requested/operator_dialing -> waiting` - attempt failed, retry left;
- `assignment_requested/operator_dialing -> missed|callback_missed|hangup_on_retry`
  - attempts exhausted;
- `new/waiting/assignment_requested/operator_dialing -> missed|callback_missed|hangup_on_retry`
  - client hung up before connection.

`operator_dialing` is the correct status and Kafka fact name.

After `connected`, Calls does not model hangup, drop, or conversation state.

Late events:

- a fact for an old attempt does not change the current attempt;
- a fact after a final status does not reopen the call;
- a fact after `connected` does not move the call to `missed`.

## Retry

Inbound message parameters:

- `operator_search_max_attempts`;
- `operator_search_retry_delay_seconds`;
- `operator_search_hangup_policy`.

No operator is a normal outcome, not an exception.

Redis retry may add minimum delay, jitter, and cap. The Kafka command still
contains the original business delay.

Example: the inbound message allows 3 attempts. If the first attempt finds no
operator, Calls moves the call to `waiting` and schedules retry. If the third
attempt still finds no operator, Calls sets the final status from policy.

## Outbox

Telephony command and status change are written in one DB transaction.

Outbox commands:

- `call_assignment_requested`;
- `call_assignment_canceled`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`.

Publisher:

- claims `pending`;
- sets `processing` and increments `attempts`;
- publishes to Kafka;
- sets `published` or `failed`;
- stale `processing` records are moved back to `pending`.

In PostgreSQL, outbox claim is atomic through `UPDATE ... RETURNING`: one
operation locks due records, moves them to `processing`, increments `attempts`,
and returns the updated state to the publisher. SQLite tests and non-PostgreSQL
drivers use the portable fallback with a separate read after claim.

Repeated publishing is safe by `idempotency_key`.

Outbox prevents a bad failure mode: Calls changes DB state and the process dies
before sending the Kafka command. With outbox, the command remains in DB and can
be published later.

## DLQ

`HandleKafkaCallFactHandler` validates:

- JSON;
- `type`;
- `schema_version`;
- `external_call_id`;
- Kafka key.

DLQ receives:

- invalid JSON;
- invalid payload;
- unknown `type`;
- unsupported `schema_version`;
- Kafka key mismatch;
- handler failure.

DLQ is stored in `dead_letter_messages`. Duplicates are deduplicated by
`message_hash`.

DLQ is not an inbox. An inbox is needed only if the source stops guaranteeing
fact uniqueness.

DLQ does not fix messages automatically. It shows what arrived, why it was
rejected, and what upstream producer or code path needs fixing.

## Further Reading

- Exact Kafka format: [kafka-contracts.md](kafka-contracts.md).
- Why Kafka, outbox, locks, reservations, and layers were chosen:
  [adr/README.md](adr/README.md).
- Production processes, metrics, and rollback: [production.md](production.md).
- Load scenarios and reports: [load-testing.md](load-testing.md).
