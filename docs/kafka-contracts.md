# Kafka Contracts

## Почему Kafka

Kafka выбрана как основная межсервисная шина для call-flow, потому что здесь нужен
не RPC, а поток фактов и команд с повторяемой доставкой.

Что Kafka даёт в этой задаче:

| Требование | Почему это важно |
|---|---|
| Durable log | Telephony facts и команды можно перечитать при сбое consumer-а или deploy-е |
| Consumer groups | Calls, Telephony, audit/analytics и будущие read models могут читать свои потоки независимо |
| Ordering внутри partition | События одного call должны обрабатываться в предсказуемом порядке |
| Partitioning by key | Нагрузку можно горизонтально масштабировать, сохранив порядок внутри одного `external_call_id` |
| Replay | Можно восстановить read model или переобработать факты после исправления consumer-а |
| Слабая связанность сервисов | Calls не ждёт HTTP-ответ Telephony в своей транзакции |
| Backpressure | Consumer lag виден и управляется, в отличие от скрытой синхронной деградации HTTP |

Почему не HTTP:

- HTTP не даёт durable log и replay;
- синхронный HTTP связывает latency Calls и Telephony;
- retry HTTP-запроса может повторить side effect без нормальной идемпотентности;
- при деградации Telephony Calls начнёт держать workers/connections.

Почему не Redis queue как межсервисная шина:

- Redis queue используется локально для jobs Calls;
- Redis queue не является контрактом событий между сервисами;
- replay, partition ordering и независимые consumer groups для нескольких сервисов лучше выражаются Kafka.

Почему не shared database:

- shared DB стирает границы сервисов;
- сложно независимо масштабировать и деплоить владельцев данных;
- трудно контролировать ownership и совместимость схем;
- факты Telephony и команды Calls должны быть контрактом сообщений, а не чужими update-ами в таблицы.

## Общее правило ключа

Все Kafka-сообщения, относящиеся к конкретному звонку, используют:

```text
key = external_call_id
```

Это обязательно для:

- входящего события о звонке;
- фактов Telephony по звонку;
- исходящих команд Calls в Telephony.

Причина: Kafka гарантирует порядок только внутри partition. Если разные события
одного call получат разные keys, `hangup`, `operator_ringing`, `connected` и
`operator_no_answer` могут обрабатываться out-of-order.

## Topics

Текущие и целевые topics:

| Topic | Направление | Producer | Consumer | Статус |
|---|---|---|---|---|
| `incoming-calls` | fact | Telephony/AMI Gateway | Calls | consumer boundary реализован, native adapter доступен через `KAFKA_CONSUMER_ADAPTER=rdkafka` |
| `telephony.facts` | fact | Telephony | Calls | consumer boundary реализован, native adapter доступен через `KAFKA_CONSUMER_ADAPTER=rdkafka` |
| `telephony.commands` | command | Calls outbox publisher | Telephony | реализован publisher |
| `*.DLQ` | dead letter | Calls consumers | ops/manual recovery | контракт подготовлен через локальную DLQ |

Имена topics настраиваются через env/config. Сейчас явно используется:

- `KAFKA_TELEPHONY_COMMANDS_TOPIC`;
- `KAFKA_BROKERS`.

`KAFKA_INCOMING_CALLS_TOPIC` есть в `.env.example` как целевое имя для consumer-а,
production adapter включается через `KAFKA_CONSUMER_ADAPTER=rdkafka`.

## Envelope

Исходящие команды Calls в Telephony публикуются в envelope:

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

| Поле | Назначение |
|---|---|
| `schema_version` | версия контракта сообщения |
| `command_id` | технический id конкретной outbox-команды |
| `idempotency_key` | бизнес-идемпотентность команды |
| `type` | тип команды |
| `external_call_id` | correlation id звонка и Kafka key |
| `payload` | данные команды |

Publisher обязан отправлять Kafka message как keyed record:

```text
external_call_id<TAB>json_payload
```

Текущий `KafkaConsoleTelephonyCommandPublisher` использует:

```text
--property parse.key=true
--property key.separator=\t
```

Это покрыто тестом `KafkaConsoleTelephonyCommandPublisherTest`.

## Команды Calls -> Telephony

### `call_assignment_requested`

Когда Calls нашёл оператора и зарезервировал его локально.

Key:

```text
external_call_id
```

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

Когда call сброшен или assignment timeout случился после публикации
`call_assignment_requested`.

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

Reasons сейчас:

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

Важно: `retry_delay_seconds` - бизнес-delay из policy. Redis queue может применить
операционный jitter/backpressure к фактическому job delay, но наружу в Kafka уходит
исходное правило.

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

## Facts Telephony -> Calls

Consumer boundary уже реализован через `KafkaConsumer` port и
`HandleKafkaCallFactHandler`. Локальный adapter для smoke/test читает JSONL из
stdin. Production adapter `RdkafkaKafkaConsumer` включается через
`KAFKA_CONSUMER_ADAPTER=rdkafka` и требует установленное PHP-расширение
`php-rdkafka`.

Все facts используют:

```text
key = external_call_id
```

Consumer mapping/validation слой уже есть: `HandleKafkaCallFactHandler`.
Настоящий Kafka transport пока не подключён, но любой consumer adapter должен
передавать в handler topic, partition, offset, key, trace id и raw payload.

Для локальной проверки одного сообщения есть команда:

```bash
php artisan calls:kafka:handle-message incoming-calls '{"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}' --key=asterisk-linkedid-1001
```

Для локального smoke loop без native Kafka client есть JSONL adapter:

```bash
printf '%s\n' '{"topic":"incoming-calls","partition":0,"offset":1,"key":"asterisk-linkedid-1001","payload":{"schema_version":1,"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}}' \
  | php artisan calls:kafka:consume incoming-calls --limit=1
```

Это не production Kafka consumer. Для production нужно включить native adapter:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_AUTO_OFFSET_RESET=earliest
```

Native adapter сохраняет тот же вызов `HandleKafkaCallFactHandler`.

Consumer принимает два формата:

- flat payload, если topic сам задаёт тип события, например `incoming-calls`;
- envelope `{ "type": "...", "payload": {...} }`, если в topic несколько типов.

Поддерживаемая версия Kafka facts сейчас только `schema_version=1`.
Для старого flat payload в `incoming-calls` отсутствие `schema_version` трактуется
как версия `1`. Для multi-type topics, например `telephony.facts`,
`schema_version` должен быть в envelope.

Если версия не поддержана, record уходит в DLQ с reason
`unsupported_schema_version`.

### `incoming_call_registered`

Целевой входящий факт от Telephony/AMI Gateway.

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

Маппится в `RegisterIncomingCallFromKafkaCommand`.

### `operator_ringing`

Payload:

```json
{
  "schema_version": 1,
  "type": "operator_ringing",
  "payload": {
    "external_call_id": "asterisk-linkedid-4001",
    "operator_id": 15,
    "assignment_attempt": 1
  }
}
```

Маппится в `MarkOperatorRingingFromKafkaCommand`.

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

Маппится в `MarkCallBridgeEstablishedFromKafkaCommand`.

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

Маппится в `MarkOperatorNoAnswerFromKafkaCommand`.

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

Маппится в `MarkOperatorLegDroppedFromKafkaCommand`.

До `connected` это неудачное назначение. После `connected` для Calls это no-op.

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

Маппится в `MarkCallHungUpFromKafkaCommand`.

До `connected` Calls закрывает call по hangup policy. После `connected` Calls не
моделирует разговор.

## Идемпотентность

Commands:

- `telephony_outbox.idempotency_key` уникален;
- publisher может повторить Kafka publish после stale processing requeue;
- Telephony должна дедуплицировать команды по `idempotency_key`.

Facts:

- сейчас предполагается, что Kafka facts уникальны на стороне источника;
- Calls дополнительно дедуплицирует входящий call по `external_call_id`;
- если гарантия уникальности facts исчезнет, нужен inbox по event id/message id.

Late facts:

- commands содержат `assignment_attempt`;
- facts назначения тоже должны содержать `assignment_attempt`;
- Calls применяет факт только если `operator_id` и `assignment_attempt` совпадают с текущим assignment;
- поздние facts по старой попытке становятся no-op.

## DLQ

Для consumer boundary заведён порт `Application\Shared\Ports\DeadLetterQueue`.

Сообщение уходит в DLQ, когда consumer не может безопасно применить Kafka record:

- payload невалиден или не проходит schema/contract validation;
- неизвестный `type`;
- не получается собрать application command;
- handler стабильно падает после исчерпания consumer retry;
- нарушен обязательный key `external_call_id`.

Текущая реализация DLQ - локальная таблица `dead_letter_messages`.

Поля записи:

| Поле | Назначение |
|---|---|
| `source` | имя consumer-а или adapter-а |
| `topic` | Kafka topic исходного сообщения |
| `message_partition`, `message_offset` | позиция Kafka record, если известна |
| `message_key` | Kafka key |
| `trace_id` | сквозная трассировка обработки |
| `reason` | короткая причина отправки в DLQ |
| `raw_payload` | исходный payload без потери данных |
| `decoded_payload` | распарсенный payload, если он был доступен |
| `message_hash` | идемпотентность записи в локальную DLQ |
| `resolved_at`, `resolution_note` | отметка ручного разбора |

`message_hash` защищает от дублей при повторном падении того же consumer-а.

`trace_id` хранится в DLQ-записи для поиска в логах/tracing. В metric tags он не
попадает, потому что это высокая кардинальность.

В production это можно заменить adapter-ом, который пишет в Kafka DLQ topic
например `incoming-calls.DLQ` или `telephony.facts.DLQ`. Application-контракт при
этом не меняется.

DLQ не заменяет inbox. Inbox нужен, если источник перестанет гарантировать
уникальность facts или если Calls должен хранить каждый применённый event id.

## Partitioning

Минимальное правило:

```text
partition key = external_call_id
```

Если в будущем появятся tenant/region constraints, partition key можно расширять
только так, чтобы все события одного `external_call_id` всё равно попадали в одну
partition. Например, `tenant_id:external_call_id`.

Нельзя использовать:

- random UUID как Kafka key для facts одного call;
- `operator_id` как key для call facts;
- пустой key.

## Что не реализовано сейчас

- inbox/event store для facts;
- schema registry;
- Kafka DLQ topics.

Эти элементы добавляются по мере необходимости, не меняя текущую границу:
application commands и ports уже отделены от transport adapter-а.
