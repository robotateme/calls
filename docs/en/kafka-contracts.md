# Kafka

Kafka is the main service channel between Calls and Telephony.

For Calls, a Kafka message is either an external fact or an outgoing command. If
the message format does not match this contract, Calls must not guess its
meaning. The message goes to DLQ.

Kafka is used for:

- inbound calls;
- Telephony facts;
- Calls commands to Telephony.

Redis queue is internal to Calls. It is not a service-to-service contract.

Why Kafka is the main channel: [ADR-0006](adr/0006-kafka-as-authoritative-service-channel.md).

## Key

For every message of one call:

```text
key = external_call_id
```

This keeps events for one call ordered inside one partition.

If key exists and does not match `payload.external_call_id`, the message goes to
DLQ.

Example:

- good: key `asterisk-linkedid-6001`, payload
  `external_call_id=asterisk-linkedid-6001`;
- bad: key `operator-15`, payload `external_call_id=asterisk-linkedid-6001`.

Kafka key rule: [ADR-0003](adr/0003-kafka-message-key-external-call-id.md).

## Topics

| Topic | Producer | Consumer |
|---|---|---|
| `incoming-calls` | Telephony/AMI Gateway | Calls |
| `telephony.facts` | Telephony | Calls |
| `telephony.commands` | Calls | Telephony |
| `*.DLQ` | Calls | ops/manual |

Settings:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

`rdkafka` mode requires `php-rdkafka`.

## Commands: Calls -> Telephony

Commands are published only from `telephony_outbox`.

Calls does not call Telephony directly. First a command is persisted in DB, then
the publisher sends it to Kafka.

Envelope:

```json
{
  "schema_version": 1,
  "command_id": "uuid",
  "idempotency_key": "external_call_id:type:attempt",
  "type": "call_assignment_requested",
  "external_call_id": "asterisk-linkedid-6001",
  "payload": {}
}
```

Fields:

| Field | Meaning |
|---|---|
| `schema_version` | contract version |
| `command_id` | command id from outbox |
| `idempotency_key` | Telephony deduplication key |
| `type` | command type |
| `external_call_id` | call id and Kafka key |
| `payload` | command body |

Console producer sends:

```text
external_call_id<TAB>json_payload
```

### `call_assignment_requested`

Calls found and reserved an operator.

Idempotency key:

```text
external_call_id:call_assignment_requested:attempt
```

Payload:

```json
{
  "external_call_id": "asterisk-linkedid-6001",
  "operator_id": 15,
  "assignment_attempt": 1
}
```

### `call_assignment_canceled`

Calls cancels assignment after hangup or timeout.

Idempotency key:

```text
external_call_id:call_assignment_canceled:attempt
```

Payload:

```json
{
  "external_call_id": "asterisk-linkedid-6001",
  "operator_id": 15,
  "assignment_attempt": 1,
  "reason": "call_hung_up"
}
```

`reason`:

- `call_hung_up`;
- `operator_assignment_timeout`.

### `operator_search_retry_scheduled`

No operator was found or assignment failed, but attempts remain.

Idempotency key:

```text
external_call_id:operator_search_retry_scheduled:attempt
```

Payload:

```json
{
  "external_call_id": "asterisk-linkedid-6001",
  "attempt": 1,
  "retry_delay_seconds": 20
}
```

`retry_delay_seconds` is the business delay. Redis may add technical jitter, but
Kafka receives this value.

### `operator_search_exhausted`

Operator search attempts are exhausted.

Idempotency key:

```text
external_call_id:operator_search_exhausted:attempt
```

Payload:

```json
{
  "external_call_id": "asterisk-linkedid-6001",
  "attempt": 3,
  "final_status": "missed"
}
```

`final_status`:

- `missed`;
- `callback_missed`;
- `hangup_on_retry`.

## Facts: Telephony -> Calls

Consumer receives:

- source;
- topic;
- partition/offset;
- Kafka key;
- trace id;
- raw JSON.

Supported contract version: `schema_version=1`.

For `incoming-calls`, payload can be sent without an envelope. For
`telephony.facts`, an envelope with `type` is required.

Fact rule: Telephony reports what already happened. Calls must not treat a fact
as successful if it belongs to another operator or an old assignment attempt.

Single-message check:

```bash
php artisan calls:kafka:handle-message incoming-calls '{"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}' --key=asterisk-linkedid-1001
```

JSONL check:

```bash
printf '%s\n' '{"topic":"incoming-calls","partition":0,"offset":1,"key":"asterisk-linkedid-1001","payload":{"schema_version":1,"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}}' \
  | php artisan calls:kafka:consume incoming-calls --limit=1
```

### `incoming_call_registered`

Payload:

```json
{
  "schema_version": 1,
  "external_call_id": "asterisk-linkedid-1001",
  "phone": "+15550001001",
  "kafka_message_id": "incoming-calls-0-1001",
  "operator_search_max_attempts": 5,
  "operator_search_retry_delay_seconds": 12,
  "operator_search_hangup_policy": "hangup_on_retry"
}
```

Handler: `RegisterIncomingCallFromKafkaCommand`.

### `operator_dialing`

Telephony is dialing the operator. Internal Calls status is also
`operator_dialing`.

Payload:

```json
{
  "schema_version": 1,
  "type": "operator_dialing",
  "payload": {
    "external_call_id": "asterisk-linkedid-4001",
    "operator_id": 15,
    "assignment_attempt": 1
  }
}
```

Handler: `MarkOperatorDialingFromKafkaCommand`.

### `bridge_established`

Client and operator are connected.

Payload:

```json
{
  "schema_version": 1,
  "type": "bridge_established",
  "payload": {
    "external_call_id": "asterisk-linkedid-4002",
    "operator_id": 15,
    "assignment_attempt": 1
  }
}
```

Handler: `MarkCallBridgeEstablishedFromKafkaCommand`.

### `operator_no_answer`

Operator did not answer.

Payload:

```json
{
  "schema_version": 1,
  "type": "operator_no_answer",
  "payload": {
    "external_call_id": "asterisk-linkedid-4003",
    "operator_id": 15,
    "assignment_attempt": 1
  }
}
```

Handler: `MarkOperatorNoAnswerFromKafkaCommand`.

### `operator_leg_dropped`

Operator leg dropped.

Payload:

```json
{
  "schema_version": 1,
  "type": "operator_leg_dropped",
  "payload": {
    "external_call_id": "asterisk-linkedid-4004",
    "operator_id": 15,
    "assignment_attempt": 1
  }
}
```

Handler: `MarkOperatorLegDroppedFromKafkaCommand`.

Before `connected`, this is a failed assignment. After `connected`, it is a
no-op for Calls.

### `hangup`

Client hung up.

Payload:

```json
{
  "schema_version": 1,
  "type": "hangup",
  "payload": {
    "external_call_id": "asterisk-linkedid-4005"
  }
}
```

Handler: `MarkCallHungUpFromKafkaCommand`.

Before `connected`, Calls closes the call by policy. After `connected`, Calls
does not manage the conversation.

## Deduplication

Commands:

- `telephony_outbox.idempotency_key` is unique;
- publisher may retry sending;
- Telephony must deduplicate by `idempotency_key`.

Facts:

- inbound calls are deduplicated by `external_call_id`;
- assignment facts apply only when `operator_id` and `assignment_attempt` match;
- old facts from another attempt become no-op;
- if the source starts sending duplicate facts, an inbox is needed.

## DLQ

DLQ receives messages that cannot be safely applied:

- invalid JSON or payload;
- unknown `type`;
- unsupported `schema_version`;
- missing `external_call_id`;
- Kafka key is not equal to `external_call_id`;
- handler failure.

Current DLQ is the `dead_letter_messages` table with `message_hash`. It can be
replaced by Kafka DLQ topics later without changing the application contract.

What to do with DLQ:

1. Inspect `reason`.
2. Find the producer or handler that caused it.
3. Fix the format or code.
4. Run `calls:dead-letter:replay --dry-run` if the record should be applied.
5. Replay manually or mark the record resolved.

Do not silently replay everything from DLQ. First understand why the message was
there.

Manual replay command:

```bash
php artisan calls:dead-letter:replay --dry-run --id=123
php artisan calls:dead-letter:replay --id=123 --note="fixed handler"
```

Replay attempts are stored in `dead_letter_replay_attempts`. Successful replay
sets `resolved_at`; failed replay keeps the record unresolved.

## Not Implemented

- fact inbox;
- schema registry;
- dedicated Kafka DLQ topics.
