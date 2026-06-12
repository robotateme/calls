# Решение

## Контекст

Основной входящий поток для звонков - Kafka. HTTP не используется ни для входящего звонка, ни для команд в Telephony.

Почему Kafka:

- нужен durable log фактов и команд, а не синхронный RPC;
- события одного звонка должны сохранять порядок внутри partition по `external_call_id`;
- consumer groups позволяют независимо подключать Calls, Telephony, audit и будущие read models;
- replay нужен для восстановления read models и переобработки после сбоев;
- Calls не должен держать HTTP-запрос к Telephony в своей транзакции.

Текущая модель:

- `external_call_id` - стабильный идентификатор звонка из Telephony/AMI;
- `external_call_id` уникален в `calls`;
- Kafka-события считаются уникальными на стороне источника;
- `kafka_message_id` хранится как технические audit-данные;
- правила повторов поиска оператора приходят от Telephony через Kafka;
- локальные `clients` и `operators` пока остаются shared database;
- границы к clients/operators заведены через порты.
- Kafka contracts описаны в [`kafka-contracts.md`](kafka-contracts.md).

Calls-сервис не моделирует SIP, внутреннее устройство соединения и таймеры дозвона оператору. Он хранит жизненный цикл звонка до успешного соединения, исполняет правила и применяет факты, пришедшие от Telephony.

## Реализованная стадия

- `RegisterIncomingCallHandler` регистрирует входящее Kafka-событие и дедуплицирует call по `external_call_id`.
- `ProcessIncomingCallJob` - тонкий adapter Redis-очереди.
- `ProcessIncomingCallHandler` выполняет поиск оператора и не отправляет команды в Telephony напрямую.
- Application-слой разделён по предметным областям: `Calls`, `Clients`, `Operators`, `Telephony`.
- CQRS-порты разделяют read и write роли: call lookup отдельно от write-side state machine, client lookup отдельно, telephony outbox writer отдельно от reader.
- Eloquent `casts()` убраны: raw DB scalars собираются в domain value objects (`CallId`, `ClientId`, `OperatorId`, attempts/retry delay, `Timestamp`) в infrastructure mapper-е.
- Repositories возвращают доменные объекты/модели/VO, не DTO, не Query result, не Eloquent records: `Call`, `ClientId`, `OperatorReservation`, `TelephonyOutboxMessage`.
- `CallReadRepository` возвращает `?Call`, а не scalar id.
- `CallWriteRepository::createIncomingFromKafka()` возвращает созданный `Call`, а не `int`.
- Скалярные id используются только на границе конкретного side effect-а: queue payload, domain event payload, outbox payload.
- Все исходящие команды в Telephony пишутся в `telephony_outbox`.
- `PublishTelephonyOutboxHandler` забирает ожидающие outbox-записи и передаёт их в `TelephonyCommandPublisher`.
- Artisan-команда `calls:telephony-outbox:publish` запускает одну итерацию публикации.
- `TelephonyCommandPublisher` реализован через adapter консольного Kafka producer-а.
- Сброс до `connected` отменяет ожидающий `call_assignment_requested`; если назначение уже опубликовано, создаётся `call_assignment_canceled`.
- Просроченная reservation оператора компенсируется отдельной командой: call уходит в retry или финальный статус, reservation освобождается, в Telephony outbox пишутся нужные команды.
- Горячие выборки allocation/outbox claim используют `SKIP LOCKED` на PostgreSQL/MySQL и составные индексы.
- Redis retry queue применяет `min_delay`, jitter и max cap к фактической задержке job, чтобы не создавать retry storm.
- Outbox claim фиксирует `processing_started_at`; зависшие `processing` записи возвращаются в `pending` командой requeue.
- Kafka poison messages можно записывать через `DeadLetterQueue`; текущий adapter хранит их в `dead_letter_messages` с идемпотентным `message_hash`.
- `HandleKafkaCallFactHandler` валидирует raw Kafka facts, маппит их в application commands и отправляет невалидные records в DLQ.
- Найденный оператор переводит call в `assignment_requested`, не в финальный `connected`.
- События Telephony двигают машину состояний: оператору звонят, соединение установлено, оператор не ответил, плечо оборвалось, звонок сброшен.
- “Оператор не ответил” и обрыв до соединения освобождают оператора и запускают следующую попытку или финальный статус по правилу.
- `connected` завершает ответственность Calls: сервис очищает локальную reservation оператора и дальше не моделирует разговор.
- Обрыв после соединения, завершение клиентом или завершение оператором не меняют бизнес-статус call в Calls.

## Машина состояний

Статусы:

- `new` - звонок зарегистрирован;
- `waiting` - ждём следующую попытку поиска оператора;
- `assignment_requested` - оператор зарезервирован через `operators.reserved_call_id`, команда назначения записана в outbox;
- `operator_ringing` - Telephony начала дозвон оператору;
- `connected` - Telephony установила соединение клиента и оператора; для Calls это успешный терминал;
- `missed`, `callback_missed`, `hangup_on_retry` - финальные статусы из policy.

Переходы:

- `new/waiting -> assignment_requested` при найденном доступном операторе, который не AFK;
- `assignment_requested -> operator_ringing` по событию Telephony;
- `assignment_requested/operator_ringing -> connected` по факту установленного соединения; Calls очищает локальную `operators.reserved_call_id`;
- `assignment_requested/operator_ringing -> waiting|final` по событию “оператор не ответил” или обрыву до соединения;
- `new/waiting/assignment_requested/operator_ringing -> final` по правилу сброса звонка.

После `connected` Calls не закрывает call по событию сброса и не ведёт жизненный цикл разговора. Эти факты принадлежат Telephony, SIP/call-client или отдельному сервису доступности операторов. Если такие события всё же приходят в Calls, они должны быть no-op для машины состояний или техническим audit/log.

Решения state machine находятся в `Domain\Calls\Call`:

- поиск без оператора возвращает domain outcome: retry или exhausted;
- успешный поиск возвращает assignment requested outcome;
- неудачное назначение возвращает retry/exhausted outcome;
- handlers больше не решают через `CallStatus`, остались только orchestration, persistence, outbox, queue.

## Outbox

`telephony_outbox` фиксирует команды:

- `call_assignment_requested`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`;
- `call_assignment_canceled`.

Команда пишется в той же DB transaction, где меняется `calls`.

Жизненный цикл доставки:

- `pending` - команда готова к публикации;
- `processing` - publisher claim-нул запись и записал `processing_started_at`;
- `published` - transport adapter успешно принял команду;
- `failed` - исчерпаны попытки публикации.

`attempts`, `available_at`, `processing_started_at`, `published_at`, `canceled_at`, `cancel_reason`, `last_error` хранят retry/cancel/processing state доставки.

Publisher забирает только `pending` records с `canceled_at is null`.

Если publisher умер после claim, record остаётся в `processing`. Команда `calls:telephony-outbox:requeue-stale` возвращает такие records в `pending`, если `processing_started_at` старше `TELEPHONY_OUTBOX_PROCESSING_TIMEOUT_SECONDS`. Повторная публикация безопасна за счёт `idempotency_key`.

Текущий `TelephonyCommandPublisher` - `KafkaConsoleTelephonyCommandPublisher`. Он вызывает настраиваемый console binary `KAFKA_CONSOLE_PRODUCER_BINARY` и отправляет keyed message в Kafka.

Если в runtime-образе нет `kafka-console-producer.sh`, нужно передать wrapper/binary через `KAFKA_CONSOLE_PRODUCER_BINARY` или заменить binding на native Kafka client adapter. Application layer при этом не меняется.

Idempotency key:

- assignment: `external_call_id + call_assignment_requested + attempt`;
- assignment cancel: `external_call_id + call_assignment_canceled + attempt`;
- retry scheduled: `external_call_id + operator_search_retry_scheduled + attempt`;
- exhausted: `external_call_id + operator_search_exhausted + attempt`.

Kafka message key для publisher-а: `external_call_id`.

## DLQ

DLQ нужна для будущих Kafka consumers, чтобы плохое сообщение не ломало consumer
group бесконечно и не терялось.

Порт: `Application\Shared\Ports\DeadLetterQueue`.

Текущая реализация: `Infrastructure\Shared\Kafka\EloquentDeadLetterQueue`.

Записываем:

- источник consumer-а;
- topic, partition, offset;
- Kafka key;
- trace id;
- короткую причину;
- raw payload;
- decoded payload, если он доступен.

Примеры причин:

- `invalid_payload`;
- `unknown_type`;
- `missing_external_call_id`;
- `handler_failed`;
- `contract_violation`.

Это не inbox. Inbox понадобится отдельно, если гарантия уникальности Kafka facts
исчезнет или если нужно хранить каждый успешно применённый event id.

При записи DLQ публикуется counter `dead_letter.recorded` с tags
`source/topic/reason/result`. `trace_id` в metric tags не добавляется, чтобы не
создавать высокую кардинальность.

## Kafka consumer mapping

Production native Kafka consumer подключается конфигом, boundary обработки Kafka
records уже есть.

`HandleKafkaCallFactHandler` принимает:

- source;
- topic;
- partition/offset;
- Kafka key;
- trace id;
- raw JSON payload.

Handler:

- валидирует JSON;
- определяет `type`;
- проверяет `schema_version`;
- проверяет `external_call_id` и соответствие Kafka key;
- маппит сообщение в существующие application commands;
- пишет DLQ при `invalid_json`, `invalid_payload`, `missing_external_call_id`, `unknown_type`, `unsupported_schema_version`, `contract_violation`, `handler_failed`;
- пишет consumer metrics.

Поддерживается только `schema_version=1`. Для старого flat payload в
`incoming-calls` отсутствие версии трактуется как `1`; для `telephony.facts`
версия должна быть в envelope.

Команда для локальной проверки:

```bash
php artisan calls:kafka:handle-message incoming-calls '{"external_call_id":"asterisk-linkedid-1001","phone":"+15550001001"}' --key=asterisk-linkedid-1001
```

Команда `calls:kafka:consume` уже заведена и зависит от порта `KafkaConsumer`.
Default binding - `JsonLinesKafkaConsumer`, который читает JSONL records из stdin.
Это локальный smoke/test adapter.

Production binding включается через:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
```

`RdkafkaKafkaConsumer` соблюдает правило: offset commit только после успешного
применения сообщения или записи в DLQ. Если `php-rdkafka` не установлен, adapter
падает fail-fast при старте обработки.

## Ответственность сервисов

Calls:

- регистрирует call;
- хранит машину состояний;
- запрашивает reservation оператора через `Application\Operators`;
- пишет команды в outbox;
- публикует outbox через отдельный handler доставки;
- применяет факты от Telephony;
- очищает локальную reservation оператора при неудачном назначении;
- очищает локальную reservation оператора при `connected`;
- исполняет правила повторов и сброса.

Telephony:

- звонит оператору;
- устанавливает соединение клиента и оператора;
- определяет, что оператору звонят, соединение установлено, оператор не ответил, плечо оборвалось или звонок сброшен;
- публикует факты в Kafka;
- владеет жизненным циклом разговора после `connected`.

Operator Availability / call-client:

- владеет фактическими `available` и `afk`;
- публикует изменения доступности оператора;
- не зависит от локальной reservation Calls.

Clients:

- предоставляет read-side lookup клиента по телефону;
- не владеет call lifecycle;
- в будущем заменяется сервисным adapter-ом или Kafka read model.

Policy/admin вне Calls:

- задаёт количество попыток, задержку повтора и правило сброса;
- задаёт дальнейшее поведение разговора вне Calls, если оно нужно бизнесу.

Подробная таблица ownership с пометкой реализованных и гипотетических сервисов находится в [`architecture.md`](architecture.md#зоны-ответственности-сервисов).

## Проблемы исходного решения

### Критические

1. Гонка выбора оператора: несколько workers могли выбрать одного оператора.
2. Нет атомарности между изменением БД и внешним side effect-ом.
3. Повтор Job мог повторить внешний side effect.
4. `assigned` использовался как ложный финальный статус до подтверждения Telephony.

### Важные

5. `No available operators` был exception, хотя это бизнес-состояние.
6. Job смешивал queue adapter, persistence, allocation и integration.
7. Не было машины состояний для ожидания, дозвона оператору, успешного соединения и неудачного назначения.
8. Shared DB по clients/operators не была изолирована портами.
9. Доступность оператора и бронь на назначение были смешаны в одном `available`.

### Было бы хорошо сделать

10. Нужен отдельный inbox/event-id store, если уникальность Kafka-событий перестанет быть гарантией.

## Тесты

Покрыто:

- Kafka registration создаёт call, сохраняет правило, публикует domain event и ставит job;
- повторная регистрация по `external_call_id` не создаёт дубль;
- найденный оператор переводит call в `assignment_requested` и создаёт outbox command;
- AFK operators не участвуют в выборе оператора;
- оператор с активной `reserved_call_id` не участвует в выборе оператора;
- отсутствие оператора переводит call в `waiting`, пишет outbox retry scheduled и ставит `calls-retry`;
- исчерпанные попытки пишут outbox exhausted и закрывают call по правилу;
- событие “оператору звонят” переводит `assignment_requested -> operator_ringing`;
- факт установленного соединения переводит `operator_ringing -> connected`;
- событие “оператор не ответил” освобождает оператора и запускает повтор или финализацию по правилу;
- обрыв до соединения ведёт себя как неудачное назначение;
- connected освобождает локальную reservation оператора;
- обрыв после соединения не меняет бизнес-статус Calls;
- просроченная operator reservation освобождается через compensation flow, а не прямым update;
- сброс закрывает `new/waiting/assignment_requested/operator_ringing` по правилу;
- сброс отменяет ожидающий assignment outbox или создаёт cancel command для уже опубликованного assignment;
- outbox publisher публикует due records, помечает `published`, откладывает retry или ставит terminal `failed`;
- native `rdkafka` adapters fail-fast падают, если расширение `php-rdkafka` не установлено;
- DLQ adapter идемпотентно пишет poison message и не создаёт дубль при повторной записи;
- DLQ records можно посмотреть, отметить resolved и удалить resolved после retention;
- metrics snapshot пишет `dead_letter.depth` только по unresolved DLQ records;
- Kafka console publisher формирует keyed Kafka message и прокидывает idempotency envelope;
- boundary-тесты защищают `Domain` и `Application` от Laravel/Infrastructure зависимостей;
- архитектурный тест запрещает repository-портам возвращать scalar/DTO/query-result вместо `Domain`.

## Что не делаем сейчас

- Не строим автоматический replay из DLQ.
- Не поднимаем Kafka DLQ topics вместо локальной DLQ-таблицы.
- Не подключаем schema registry.
- Не вводим inbox, пока источник гарантирует уникальность Kafka facts.
- Не выделяем clients/operators в отдельные сервисы.
- Не строим admin rule-builder внутри Calls.
- Не решаем сценарии после `connected` внутри Calls.
- Не смешиваем retry поиска оператора и retry доставки outbox-команды.

## Предположения

- Telephony выдаёт стабильный `external_call_id` для всего жизненного цикла звонка.
- Kafka key для call facts совпадает с `external_call_id`, чтобы порядок событий одного звонка сохранялся внутри partition.
- Источник Kafka facts не шлёт дубли с разными offset/key для одного и того же бизнес-события; если это изменится, нужен inbox.
- Telephony умеет идемпотентно принять команды по `idempotency_key`.
- Локальные `clients` и `operators` пока допустимы как shared DB/read model, но не считаются окончательной границей сервиса.
- После `connected` Calls больше не владеет разговором, SIP-состояниями и доступностью оператора.

## Риски

- Если `external_call_id` нестабилен, ломается correlation.
- Если Kafka uniqueness нарушится, нужен inbox.
- Если Telephony присылает поздние события, они должны быть idempotent/no-op по текущему статусу, operator_id и attempt.
- Если DLQ начнёт расти, это не операционная норма, а сигнал сломанного контракта, deploy-а consumer-а или несовместимой схемы.
- Если operator release останется локальной shared DB операцией, при выделении operator-service нужен отдельный контракт release/reservation.
- Если события после `connected` будут ошибочно использоваться как бизнес-состояния Calls, сервис начнёт владеть чужой предметной областью: разговором, SIP-состояниями и доступностью оператора.
- Если call-client/availability read model запаздывает, оператор может выглядеть доступным сразу после `connected`; это нужно закрывать SLA/lag metrics для потока доступности или переносом allocation в отдельный dispatch-service.

## Масштабирование

Bottleneck-и:

- DB locks при allocation;
- рост `calls` и `telephony_outbox`;
- Redis delayed jobs и retry storms;
- Kafka consumer lag;
- рост unresolved DLQ при несовместимых payload/schema;
- Kafka publisher lag;
- задержка обработки Telephony facts;
- синхронное логирование.

Простое увеличение количества workers поможет, пока bottleneck находится в CPU/IO
application-процессов и Redis queue depth. Оно перестанет помогать, когда workers
начнут конкурировать за одни и те же горячие строки operators/outbox, когда DB
упрётся в lock wait/connection pool/write throughput, когда Redis delayed jobs
создадут retry wave, или когда Telephony/Kafka publisher станет медленнее
производителей.

Если оставить legacy HTTP-интеграцию с Telephony, её лимиты будут отдельным
узким местом: connection pool, timeout-и, rate limits, повтор side effect-а при
retry, backpressure на queue workers и сложная идемпотентность. В текущем
решении этот риск снят transactional outbox-ом и асинхронной Kafka-доставкой,
но сам outbox publisher всё равно нужно масштабировать и мониторить отдельно.

Что уже сделано:

- составные индексы под operator allocation, due outbox claim, assignment lookup и retry scans;
- `FOR UPDATE SKIP LOCKED` для allocation и outbox claim на PostgreSQL/MySQL;
- отдельная команда `calls:operator-reservations:release-expired` для зависших reservation;
- компенсация timeout-а назначения через обычную state machine и Telephony outbox.
- jitter/backpressure для `calls-retry`: `OPERATOR_SEARCH_RETRY_MIN_DELAY_SECONDS`, `OPERATOR_SEARCH_RETRY_JITTER_SECONDS`, `OPERATOR_SEARCH_RETRY_MAX_DELAY_SECONDS`.
- requeue stale `telephony_outbox.processing`: `processing_started_at`, `TELEPHONY_OUTBOX_PROCESSING_TIMEOUT_SECONDS`, `calls:telephony-outbox:requeue-stale`.
- Laravel scheduler каждую минуту запускает outbox publish, stale outbox requeue и expired reservation cleanup с `withoutOverlapping`.
- metrics port и log adapter: counters/timings на hot paths, gauges snapshot по calls/outbox/reservations/queues.
- DLQ port, локальная таблица и операционные команды для poison messages.
- `rdkafka` consumer/producer adapters включаются через env без изменения application layer.

Оставшийся план:

1. Kafka partition key - `external_call_id`.
2. Ввести inbox, если Telephony event uniqueness не гарантирована.
3. Перевести clients/operators в Kafka read models или отдельный dispatch-service.
4. Архивировать/партиционировать завершённые calls и published outbox.
5. Нагрузочно проверить: нет операторов, массовый no-answer, dropped leg до соединения, поздние события после connected, Telephony lag.
