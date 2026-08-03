# Архитектура

Calls разделён на слои. Бизнес-правила не должны зависеть от Laravel.

## Слои

- `src/Domain` - статусы, правила звонка, простые объекты-значения.
- `src/Application` - обработчики команд и интерфейсы к внешнему миру.
- `src/Infrastructure` - БД, Redis, Kafka, метрики.
- `app` - Laravel jobs, providers, HTTP и console wiring.

Правила:

- `Domain` не импортирует Laravel, `app`, `Application`, `Infrastructure`.
- `Application` не зависит от Laravel и конкретной БД/Kafka.
- `Infrastructure` может использовать Laravel и реализует интерфейсы
  `Application`.
- `config()` и `env()` остаются в `app` или `Infrastructure`.

Это проверяет `tests/Unit/ArchitectureBoundaryTest.php`.

## Области

- `Application\Calls` - звонок до `connected`.
- `Application\Clients` - поиск клиента по телефону.
- `Application\Operators` - локальная бронь оператора.
- `Application\Telephony` - команды в `telephony_outbox`.
- `Application\Shared` - транзакции, очередь, Kafka, DLQ, метрики.

## Репозитории

Интерфейсы репозиториев в `Application` возвращают только:

- доменный объект или объект-значение;
- `null`;
- `void`;
- список доменных объектов.

Они не возвращают Eloquent, Laravel collections, raw arrays, DTO, `stdClass` или
голые ids.

Eloquent и строки БД - только формат хранения. В доменные объекты они собираются внутри
`Infrastructure\*\Persistence\*Mapper`.

## Звонок

1. Kafka-сообщение создаёт `call`.
2. `ProcessIncomingCallJob` вызывает `ProcessIncomingCallHandler`.
3. Обработчик в транзакции ищет клиента, ищет оператора, меняет статус и пишет
   команду в `telephony_outbox`.
4. Publisher отправляет команду из `telephony_outbox` в Kafka.
5. Telephony присылает факты обратно в Kafka.
6. `HandleKafkaCallFactHandler` проверяет сообщение и вызывает нужный обработчик.
7. Если сообщение плохое, оно попадает в `dead_letter_messages`.

Подробности: [solution.md](solution.md) и [kafka-contracts.md](kafka-contracts.md).

## Статусы

- `new` - звонок записан.
- `waiting` - ждём следующую попытку поиска оператора.
- `assignment_requested` - оператор забронирован, команда записана.
- `operator_dialing` - Telephony звонит оператору.
- `connected` - клиент и оператор соединены.
- `missed`, `callback_missed`, `hangup_on_retry` - финал без соединения.

Переходы:

- `new/waiting -> assignment_requested`;
- `assignment_requested -> operator_dialing`;
- `assignment_requested/operator_dialing -> connected`;
- `assignment_requested/operator_dialing -> waiting|final`;
- `new/waiting/assignment_requested/operator_dialing -> final`.

После `connected` Calls больше не меняет бизнес-статус звонка. Разговор и
доступность оператора после соединения принадлежат другим системам.

## Telephony outbox

Calls не отправляет команды напрямую. Он пишет их в `telephony_outbox` в той же
транзакции, где меняет `calls`.

Типы команд:

- `call_assignment_requested`;
- `call_assignment_canceled`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`.

Статусы outbox:

- `pending`;
- `processing`;
- `published`;
- `failed`.

Publisher забирает готовые записи с row lock/`SKIP LOCKED`, отправляет в Kafka и
пишет результат. Зависшие `processing` записи возвращаются командой
`calls:telephony-outbox:requeue-stale`.

Повторная отправка безопасна, потому что есть `idempotency_key`.

## Оператор

Calls хранит только локальную бронь:

- `operators.reserved_call_id`;
- `operators.reserved_at`.

`available` и `afk` приходят извне. Calls не ставит `available=true`, когда
снимает бронь: после `connected` оператор может быть занят разговором.

Оператор подходит, если:

- `available=true`;
- `afk=false`;
- `reserved_call_id is null`.

Бронь снимается только если `reserved_call_id` равен текущему `call_id`.

## Надёжность

- Повторное Kafka-сообщение не должно ломать состояние.
- `external_call_id` уникален.
- Важная смена статуса и команда Telephony пишутся в одной транзакции.
- Retry поиска оператора имеет лимит, задержку, jitter и финальный статус.
- Плохие Kafka-сообщения идут в `dead_letter_messages`.
- Старые факты по другой попытке становятся no-op.
- Перед внешним действием handler заново проверяет состояние звонка.

## Кто за что отвечает

| Часть | Делает | Не делает |
|---|---|---|
| Calls | Ведёт звонок до `connected`, выбирает оператора, пишет команды Telephony | Не управляет разговором после `connected` |
| Outbox publisher | Отправляет `telephony_outbox` в Kafka | Не принимает бизнес-решения |
| Redis queue | Запускает внутренние jobs | Не заменяет Kafka |
| DLQ | Хранит плохие Kafka-сообщения | Не чинит их автоматически |
| Telephony | Звонит оператору и соединяет разговор | Не выбирает оператора |
| Operator Availability | Знает реальную доступность оператора | Не хранит статус звонка Calls |
| Clients | Даёт клиента по телефону | Не участвует в назначении |

## Процессы

Нужны scheduler, queue workers, Kafka consumers и outbox publisher. Команды
перечислены в [production.md](production.md).

## ADR

Решения записаны в [adr/README.md](adr/README.md):

- локальная бронь оператора;
- row locks и `SKIP LOCKED`;
- Kafka key = `external_call_id`;
- `/metrics` читает готовые метрики.
