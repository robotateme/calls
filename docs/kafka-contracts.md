# Kafka

Kafka - главный канал между Calls и Telephony.

Используется для:

- входящих звонков;
- фактов от Telephony;
- команд Calls в Telephony.

Redis queue - только внутренняя очередь Calls. Это не межсервисный договор.

## Key

Для всех сообщений одного звонка:

```text
key = external_call_id
```

Так Kafka держит порядок событий одного звонка внутри partition.

Если key есть и он не равен `payload.external_call_id`, сообщение идёт в DLQ.

См. [ADR-0003](adr/0003-kafka-message-key-external-call-id.md).

## Topics

| Topic | Кто пишет | Кто читает |
|---|---|---|
| `incoming-calls` | Telephony/AMI Gateway | Calls |
| `telephony.facts` | Telephony | Calls |
| `telephony.commands` | Calls | Telephony |
| `*.DLQ` | Calls | ops/manual |

Настройки:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

Для `rdkafka` нужен `php-rdkafka`.

## Commands: Calls -> Telephony

Commands публикуются только из `telephony_outbox`.

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

Поля:

| Field | Что это |
|---|---|
| `schema_version` | версия |
| `command_id` | id команды в outbox |
| `idempotency_key` | ключ дедупликации в Telephony |
| `type` | тип команды |
| `external_call_id` | id звонка и Kafka key |
| `payload` | тело команды |

Console producer отправляет:

```text
external_call_id<TAB>json_payload
```

### `call_assignment_requested`

Calls нашёл оператора и поставил бронь.

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

Calls отменяет назначение после hangup или timeout.

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

Оператор не найден или назначение сорвалось, но попытки ещё есть.

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

`retry_delay_seconds` - бизнес-задержка. Redis может добавить технический jitter,
но в Kafka уходит это значение.

### `operator_search_exhausted`

Попытки поиска закончились.

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

Consumer получает:

- source;
- topic;
- partition/offset;
- Kafka key;
- trace id;
- raw JSON.

Поддерживается `schema_version=1`.

Для `incoming-calls` можно передать payload без envelope. Для `telephony.facts`
нужен envelope с `type`.

Проверка одного сообщения:

```bash
php artisan calls:kafka:handle-message incoming-calls '{"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}' --key=asterisk-linkedid-1001
```

JSONL-проверка:

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

Telephony дозванивается до оператора. Внутренний статус Calls тоже
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

Клиент и оператор соединены.

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

Оператор не ответил.

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

Операторский leg оборвался.

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

До `connected` это неудачное назначение. После `connected` для Calls это no-op.

### `hangup`

Клиент повесил трубку.

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

До `connected` Calls закрывает звонок по policy. После `connected` Calls не
ведёт разговор.

## Дедупликация

Commands:

- `telephony_outbox.idempotency_key` уникален;
- publisher может повторить отправку;
- Telephony должна дедуплицировать по `idempotency_key`.

Facts:

- входящий звонок дедуплицируется по `external_call_id`;
- факты назначения применяются только при совпадении `operator_id` и
  `assignment_attempt`;
- старые факты по другой попытке становятся no-op;
- если источник начнёт присылать дубли facts, нужен inbox.

## DLQ

В DLQ идут сообщения, которые нельзя безопасно применить:

- битый JSON или payload;
- неизвестный `type`;
- неподдержанный `schema_version`;
- нет `external_call_id`;
- Kafka key не равен `external_call_id`;
- handler упал.

Сейчас DLQ - таблица `dead_letter_messages` с `message_hash`. Позже её можно
заменить на Kafka DLQ topic без смены application-контракта.

## Не сделано

- inbox для facts;
- schema registry;
- автоматическая повторная обработка из DLQ;
- отдельные Kafka DLQ topics.
