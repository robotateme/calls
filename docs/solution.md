# Решение

Документ фиксирует текущее поведение Calls: как звонок попадает в сервис, как
выбирается оператор, что пишется в outbox и где заканчивается ответственность
сервиса.

## Контекст

Основной входящий поток - Kafka. HTTP не используется ни для входящего звонка,
ни для команд в Telephony.

Причины выбора Kafka:

- нужен durable log фактов и команд;
- события одного звонка должны идти в порядке внутри partition по
  `external_call_id`;
- consumer groups дают независимых читателей: Calls, Telephony, audit/read
  models;
- replay нужен для восстановления после сбоев;
- Calls не должен держать HTTP-запрос к Telephony в своей transaction.

Kafka contracts описаны в [kafka-contracts.md](kafka-contracts.md).
Архитектурные решения по reservation, конкурентному claim, Kafka key и
Prometheus scrape path зафиксированы в [adr/README.md](adr/README.md).

## Что реализовано

- Регистрация incoming call из Kafka через `RegisterIncomingCallHandler`.
- Дедупликация по уникальному `external_call_id`.
- Тонкий queue adapter `ProcessIncomingCallJob`.
- Один use case `ProcessIncomingCallHandler` для первого поиска оператора и
  retry.
- Domain state machine в `Domain\Calls\Call`.
- CQRS-порты для calls, clients, operators и telephony outbox.
- Repository-порты возвращают Domain/VO, не Eloquent/DTO/scalars.
- Локальная reservation оператора через `operators.reserved_call_id`.
- Исходящие Telephony commands через transactional `telephony_outbox`.
- Publisher outbox records в Kafka.
- Kafka consumer boundary: JSONL adapter для smoke/test и `rdkafka` adapter для
  production.
- DLQ для poison Kafka records.
- Recovery-команды для stale outbox records и expired reservations.
- Metrics port и Prometheus endpoint `/metrics`.

## Flow звонка

1. Kafka fact регистрирует call с `external_call_id`, phone и retry policy.
2. Calls ставит `ProcessIncomingCallJob`.
3. Handler ищет клиента и доступного оператора.
4. Если оператор найден, Calls резервирует его, переводит call в
   `assignment_requested` и пишет `call_assignment_requested` в outbox.
5. Если оператора нет, Domain решает retry или final status. Handler пишет
   `operator_search_retry_scheduled` или `operator_search_exhausted`.
6. Telephony получает command из Kafka и публикует facts обратно.
7. Facts двигают state machine: dialing, connected, retry/final, hangup.
8. При `connected` Calls освобождает локальную reservation и больше не ведёт
   разговор.

## Машина состояний

Статусы:

- `new`;
- `waiting`;
- `assignment_requested`;
- `operator_dialing`;
- `connected`;
- `missed`;
- `callback_missed`;
- `hangup_on_retry`.

Переходы:

- `new/waiting -> assignment_requested` при найденном операторе;
- `assignment_requested -> operator_dialing` по Telephony fact
  `operator_dialing`;
- `assignment_requested/operator_dialing -> connected` по
  `bridge_established`;
- `assignment_requested/operator_dialing -> waiting|final` по no-answer/drop;
- `new/waiting/assignment_requested/operator_dialing -> final` по hangup policy.

`operator_dialing` - внешний Kafka fact. Внутренний статус называется
`operator_dialing`, потому что Telephony дозванивается до оператора, а не
оператор звонит клиенту.

После `connected` события hangup/drop/no-answer для Calls становятся no-op или
техническим audit/log. Жизненный цикл разговора принадлежит Telephony,
SIP/call-client или отдельному сервису доступности операторов.

## Retry и outbox

Поиск оператора:

- max attempts, retry delay и hangup policy приходят во входящем Kafka fact;
- отсутствие оператора не является exception;
- Domain outcome решает: retry или final;
- Redis retry delay получает min delay, jitter и cap, чтобы не создавать retry
  storm.

Outbox:

- command пишется в той же DB transaction, где меняется call;
- publisher claim-ит `pending` records и переводит их в `processing`;
- stale `processing` records возвращаются в `pending`;
- повторная публикация безопасна через `idempotency_key`.

Команды outbox:

- `call_assignment_requested`;
- `call_assignment_canceled`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`.

## DLQ и consumer mapping

`HandleKafkaCallFactHandler` получает raw Kafka record, валидирует JSON,
`type`, `schema_version`, `external_call_id` и Kafka key.

Record уходит в DLQ при:

- invalid JSON/payload;
- unknown type;
- unsupported schema version;
- contract violation;
- handler failure.

DLQ хранится в `dead_letter_messages` и защищена `message_hash` от дублей. Это
не inbox: inbox нужен отдельно, если источник перестанет гарантировать
уникальность Kafka facts.

## Проблемы исходного решения

Критические:

- несколько workers могли выбрать одного оператора;
- изменение БД и внешний side effect не были атомарны;
- retry job мог повторить внешний side effect;
- статус `assigned` выглядел как успешный финал без подтверждения Telephony.

Важные:

- отсутствие оператора было exception вместо бизнес-исхода;
- job смешивал queue adapter, persistence, allocation и integration;
- не было state machine для ожидания, дозвона, соединения и неудачного
  назначения;
- доступность оператора и reservation были смешаны.

Отложено:

- inbox/event-id store, пока источник гарантирует уникальность facts;
- schema registry;
- выделение clients/operators в отдельные сервисы;
- автоматический replay из DLQ;
- сценарии разговора после `connected`.

## Тесты

Покрыты ключевые сценарии:

- регистрация и дедупликация incoming call;
- выбор оператора и запись outbox command;
- отсутствие оператора, retry и exhausted outcome;
- AFK/зарезервированные operators не участвуют в allocation;
- late job после final status становится no-op;
- Telephony facts `operator_dialing`, `bridge_established`,
  `operator_no_answer`, `operator_leg_dropped`, `hangup`;
- cancel assignment при hangup/timeout;
- stale outbox requeue;
- expired reservation compensation;
- DLQ list/resolve/prune;
- metrics snapshot;
- architecture boundary tests.

## Предположения и риски

Предположения:

- Telephony даёт стабильный `external_call_id`.
- Kafka key для call facts равен `external_call_id`.
- Facts уникальны на стороне источника.
- Telephony дедуплицирует commands по `idempotency_key`.
- Shared DB для clients/operators временно допустима как read model.

Риски:

- нестабильный `external_call_id` ломает correlation;
- дубли facts без inbox могут повторно двигать state machine;
- поздние facts должны оставаться idempotent/no-op по текущему статусу,
  `operator_id` и `assignment_attempt`;
- рост DLQ означает сломанный contract, deploy или upstream producer;
- при выделении operator-service нужен отдельный контракт reservation/release;
- если после `connected` начать менять Calls-статус, сервис начнёт владеть чужой
  предметной областью.

## Масштабирование

Ожидаемые bottleneck-и:

- DB locks при allocation;
- рост `calls` и `telephony_outbox`;
- Redis delayed jobs и retry storms;
- Kafka consumer/publisher lag;
- рост unresolved DLQ;
- синхронное логирование.

Увеличение workers помогает, пока bottleneck в application CPU/IO и queue depth.
Оно перестаёт помогать, когда workers конкурируют за горячие rows
operators/outbox, DB упирается в lock wait/write throughput, Redis получает retry
wave или Kafka/Telephony становится медленнее producers.

Уже сделано:

- индексы под allocation, retry scans, outbox claim и assignment lookup;
- `FOR UPDATE SKIP LOCKED` для locked call/outbox batches на PostgreSQL/MySQL;
- row lock при operator allocation;
- jitter/backpressure для `calls-retry`;
- stale outbox requeue;
- expired reservation cleanup;
- metrics snapshot по calls/outbox/reservations/queues/DLQ.

Дальше:

1. Проверить Kafka partitioning по `external_call_id` на реальной нагрузке.
2. Ввести inbox, если facts перестанут быть уникальными.
3. Перевести clients/operators в read models или отдельный Dispatch/Routing
   service.
4. Архивировать или партиционировать завершённые calls и published outbox.
5. Нагрузочно прогнать no-operators, массовый no-answer, hangup до соединения,
   late facts after connected и Telephony lag.
