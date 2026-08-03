# Kafka Contracts

Kafka - основная межсервисная шина Calls. Она используется для входящих facts от
Telephony/AMI Gateway и исходящих commands в Telephony.

## Почему Kafka

- durable log и replay после сбоев;
- ordering внутри partition по `external_call_id`;
- независимые consumer groups для Calls, Telephony, audit/read models;
- backpressure через consumer lag;
- отсутствие синхронного HTTP side effect-а внутри DB transaction Calls.

Redis queue остаётся внутренней очередью jobs Calls и не считается
межсервисным контрактом.

## Message key

Все сообщения одного звонка используют один key:

```text
key = external_call_id
```

Иначе `hangup`, `operator_dialing`, `bridge_established` и no-answer facts могут
прийти в Calls out-of-order.

Решение зафиксировано в
[ADR-0003](adr/0003-kafka-message-key-external-call-id.md).

## Topics

| Topic | Direction | Producer | Consumer |
|---|---|---|---|
| `incoming-calls` | fact | Telephony/AMI Gateway | Calls |
| `telephony.facts` | fact | Telephony | Calls |
| `telephony.commands` | command | Calls outbox publisher | Telephony |
| `*.DLQ` | dead letter | Calls consumers | ops/manual recovery |

Topic names настраиваются через env/config. Production Kafka adapters включаются
через:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

`rdkafka` adapters требуют PHP extension `php-rdkafka` и падают fail-fast, если
расширение не установлено.

## Commands: Calls -> Telephony

Commands публикуются только из `telephony_outbox`. Envelope:

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

Обязательные поля:

| Field | Meaning |
|---|---|
| `schema_version` | версия контракта |
| `command_id` | технический id outbox-команды |
| `idempotency_key` | ключ дедупликации side effect-а |
| `type` | тип команды |
| `external_call_id` | business key и Kafka key |
| `payload` | данные команды |

Текущий console producer отправляет keyed message в формате:

```text
external_call_id<TAB>json_payload
```

### `call_assignment_requested`

Когда Calls нашёл и зарезервировал оператора.

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

Когда call сброшен или assignment timeout случился после публикации assignment.

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

Reasons:

- `call_hung_up`;
- `operator_assignment_timeout`.

### `operator_search_retry_scheduled`

Когда оператор не найден или назначение не состоялось, но попытки ещё есть.

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

`retry_delay_seconds` - бизнес-delay из policy. Redis queue может добавить
операционный min delay/jitter/cap, но наружу в Kafka уходит исходное правило.

### `operator_search_exhausted`

Когда попытки поиска оператора исчерпаны.

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

Consumer boundary реализован через `KafkaConsumer` port и
`HandleKafkaCallFactHandler`.

Consumer получает:

- source;
- topic;
- partition/offset;
- Kafka key;
- trace id;
- raw JSON payload.

Поддерживается `schema_version=1`. Для `incoming-calls` flat payload без версии
трактуется как version 1. Для multi-type topics, например `telephony.facts`,
версия должна быть в envelope.

Локальная проверка одного сообщения:

```bash
php artisan calls:kafka:handle-message incoming-calls '{"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}' --key=asterisk-linkedid-1001
```

JSONL smoke loop:

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

Maps to `RegisterIncomingCallFromKafkaCommand`.

### `operator_dialing`

External Telephony fact. Internally Calls maps it to `operator_dialing`.

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

Maps to `MarkOperatorDialingFromKafkaCommand`.

### `bridge_established`

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

Maps to `MarkCallBridgeEstablishedFromKafkaCommand`.

### `operator_no_answer`

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

Maps to `MarkOperatorNoAnswerFromKafkaCommand`.

### `operator_leg_dropped`

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

Maps to `MarkOperatorLegDroppedFromKafkaCommand`. До `connected` это неудачное
назначение. После `connected` для Calls это no-op.

### `hangup`

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

Maps to `MarkCallHungUpFromKafkaCommand`. До `connected` Calls закрывает call по
hangup policy. После `connected` Calls не моделирует разговор.

## Idempotency

Commands:

- `telephony_outbox.idempotency_key` уникален;
- publisher может повторить Kafka publish после stale processing requeue;
- Telephony должна дедуплицировать commands по `idempotency_key`.

Facts:

- Calls дедуплицирует incoming call по `external_call_id`;
- facts назначения применяются только при совпадении `operator_id` и
  `assignment_attempt` с текущим assignment;
- поздние facts по старой попытке становятся no-op;
- если источник перестанет гарантировать уникальность facts, нужен inbox по
  event id/message id.

## DLQ

Record уходит в DLQ, когда consumer не может безопасно применить Kafka message:

- invalid JSON/payload;
- unknown `type`;
- unsupported `schema_version`;
- missing/mismatched `external_call_id`;
- handler failure.

Текущая DLQ - локальная таблица `dead_letter_messages` с `message_hash` для
идемпотентной записи. В production adapter можно заменить на Kafka DLQ topic без
изменения application contract.

## Не реализовано сейчас

- inbox/event store для facts;
- schema registry;
- автоматический replay из DLQ;
- отдельные Kafka DLQ topics.
