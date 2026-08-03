# Архитектура

Calls построен как hexagonal Laravel-сервис: бизнес-решения живут в Domain и
Application, Laravel/БД/Kafka/Redis остаются adapter-ами.

## Слои

- `src/Domain` - framework-free domain objects, value objects и domain events.
- `src/Application` - use cases, commands и ports.
- `src/Infrastructure` - Laravel/database/queue/Kafka adapters.
- `app` - Laravel wiring: jobs, providers, console/HTTP entrypoints.

Границы:

- `Domain` не импортирует Laravel, `app`, `Application`, `Infrastructure`.
- `Application` зависит от `Domain` и application ports, но не от Laravel и
  Infrastructure.
- `Infrastructure` реализует application ports и может использовать Laravel.
- Runtime config (`config()`, `env()`) держится в `app` или `Infrastructure`.

Архитектурные ограничения проверяются тестом
`tests/Unit/ArchitectureBoundaryTest.php`.

## Области Application

- `Application\Calls` - lifecycle звонка до `connected`, state machine и команды
  обработки.
- `Application\Clients` - read-side lookup клиента по телефону.
- `Application\Operators` - локальная reservation оператора на время назначения.
- `Application\Telephony` - outbox commands и delivery ports.
- `Application\Shared` - transaction, queue, event bus, Kafka consumer, metrics,
  DLQ.

## Правила Repository

Repository-порты для Application возвращают только:

- domain aggregate/model/value object;
- `null`;
- `void`;
- `list<Domain\...>`.

Они не возвращают Eloquent models, Laravel collections, raw arrays, DTO,
query-result classes, `stdClass` или scalar ids. Если application-слою нужен id,
repository возвращает VO (`CallId`, `ClientId`, `OperatorId`) или domain model.

Eloquent models считаются persistence records. Raw DB scalars собираются в domain
VO внутри `Infrastructure\*\Persistence\*Mapper`.

## Flow звонка

1. Kafka fact регистрирует входящий звонок через
   `RegisterIncomingCallHandler`.
2. `ProcessIncomingCallJob` делегирует обработку в
   `ProcessIncomingCallHandler`.
3. Handler в одной transaction меняет call, резервирует оператора и пишет
   Telephony command в `telephony_outbox`.
4. Outbox publisher отправляет command в Kafka.
5. Telephony возвращает facts через Kafka.
6. `HandleKafkaCallFactHandler` валидирует record, маппит его в application
   command или пишет poison record в DLQ.

Детальный call flow описан в [solution.md](solution.md), Kafka payloads - в
[kafka-contracts.md](kafka-contracts.md).

## Машина состояний

Статусы:

- `new` - call зарегистрирован;
- `waiting` - ждём следующую попытку поиска оператора;
- `assignment_requested` - оператор зарезервирован, command записан в outbox;
- `operator_dialing` - Telephony дозванивается до оператора;
- `connected` - соединение установлено, ответственность Calls завершена;
- `missed`, `callback_missed`, `hangup_on_retry` - финальные статусы policy.

Основные переходы:

- `new/waiting -> assignment_requested` при найденном операторе;
- `assignment_requested -> operator_dialing` по Kafka fact `operator_dialing`;
- `assignment_requested/operator_dialing -> connected` по
  `bridge_established`;
- `assignment_requested/operator_dialing -> waiting|final` по no-answer/drop;
- `new/waiting/assignment_requested/operator_dialing -> final` по hangup policy.

После `connected` Calls не моделирует разговор, hangup, SIP state и дальнейшую
доступность оператора. Поздние facts после `connected` не меняют бизнес-статус
call.

Диаграммы: [diagrams.md](diagrams.md).

## Telephony outbox

Исходящие команды в Telephony не отправляются напрямую из use case. Они пишутся
в `telephony_outbox` в той же DB transaction, где меняется state machine.

Команды:

- `call_assignment_requested`;
- `call_assignment_canceled`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`.

Delivery lifecycle:

- `pending`;
- `processing`;
- `published`;
- `failed`.

Publisher claim-ит due records с row lock/`SKIP LOCKED`, пишет
`processing_started_at`, отправляет Kafka message и помечает результат.
`calls:telephony-outbox:requeue-stale` возвращает зависшие `processing` records
в `pending`. Повторная публикация безопасна через `idempotency_key`.

## Reservation оператора

Calls владеет только краткой локальной reservation:

- `operators.reserved_call_id`;
- `operators.reserved_at`.

Фактическая доступность `available/afk` считается внешней read model. Calls не
выставляет `available=true` при освобождении reservation, потому что после
`connected` оператор может быть занят разговором.

Allocation выбирает только операторов с `available=true`, `afk=false` и
`reserved_call_id is null`. Release очищает reservation только если
`reserved_call_id` совпадает с текущим call id.

## Надежность

- Kafka ingress повторяемый; handlers идемпотентны по явному business key.
- `external_call_id` уникален в `calls`.
- Critical state change + Telephony command фиксируются одной DB transaction.
- Retry поиска оператора имеет attempts, delay, jitter, cap и final outcome.
- Poison Kafka records идут в `dead_letter_messages`.
- Late facts по старой попытке становятся no-op, если `operator_id` или
  `assignment_attempt` не совпадают с текущим assignment.
- Перед side effect-ом handler повторно проверяет, что aggregate processable.

## Ownership

| Компонент | Ответственность | Не делает |
|---|---|---|
| Calls | Регистрирует call, хранит state machine до `connected`, выбирает/резервирует оператора, пишет Telephony commands, применяет Telephony facts | Не звонит оператору сам, не управляет SIP и разговором после `connected` |
| Telephony outbox publisher | Доставляет records из `telephony_outbox` в Kafka | Не принимает бизнес-решения |
| Redis queue | Внутренняя очередь jobs Calls | Не является межсервисной шиной |
| DLQ | Хранит poison Kafka records для ручного разбора | Не заменяет inbox |
| Telephony | Звонит оператору, соединяет клиента и оператора, публикует facts | Не выбирает оператора и не хранит lifecycle Calls |
| Operator Availability | Владеет фактической доступностью оператора | Не хранит call state machine |
| Clients | Даёт lookup клиента | Не участвует в назначении оператора |

## Runtime процессы

Production processes перечислены в [production.md](production.md). Минимально
нужны scheduler, workers `calls`/`calls-retry`, Kafka consumers и outbox
publisher.
