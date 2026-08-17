# Domain Model и Infrastructure Mapping

Этот документ описывает текущую доменную модель Calls и то, как она
восстанавливается из PostgreSQL. Это не общий DDD-гайд, а правила именно этого
сервиса.

## Aggregate Root `Call`

В домене Calls главный агрегат - `Domain\Calls\Call`.

```text
Call
├── Entity
└── Aggregate Root
```

`Call` является Entity, потому что имеет собственную идентичность:
`Domain\Calls\CallId`.

`Call` является Aggregate Root, потому что через него контролируется
согласованность жизненного цикла звонка: назначение оператора, попытки поиска,
переходы в `operator_dialing`, `connected` и финальные статусы.

Короткая формула:

> `Call` - Entity по своей природе и Aggregate Root по своей роли.

Каждый Aggregate Root является Entity, но не каждая Entity является Aggregate
Root. В этом проекте не нужен отдельный `CallEntity` и отдельный
`CallAggregate`: это была бы искусственная разница, которой нет в модели.

## Состав агрегата

Текущий `Call` состоит из доменных типов и ссылок на внешние сущности:

```text
Call - Aggregate Root / Entity
│
├── CallId
├── ExternalCallId
├── PhoneNumber
├── CallStatus
├── ClientId?
├── OperatorId?
├── OperatorSearchAttempts
├── OperatorSearchMaxAttempts
├── OperatorSearchRetryDelay
├── CallHangupPolicy
├── nextOperatorSearchAt?
├── assignmentRequestedAt?
├── operatorDialingAt?
├── connectedAt?
├── createdAt
└── правила изменения состояния
```

Реальные классы:

- `Domain\Calls\Call`
- `Domain\Calls\CallId`
- `Domain\Calls\ExternalCallId`
- `Domain\Calls\PhoneNumber`
- `Domain\Calls\CallStatus`
- `Domain\Clients\ClientId`
- `Domain\Operators\OperatorId`
- `Domain\Calls\OperatorSearchAttempts`
- `Domain\Calls\OperatorSearchMaxAttempts`
- `Domain\Calls\OperatorSearchRetryDelay`
- `Domain\Calls\CallHangupPolicy`
- `Domain\Shared\Timestamp`

`kafka_message_id` хранится в таблице `calls` как ingress/idempotency metadata
регистрации входящего Kafka-сообщения. Сейчас это не часть доменного объекта
`Call`.

## Модель создания входящего звонка

Создание нового звонка не начинается с полноценного `Call`: до INSERT у него ещё
нет `CallId`, потому что id выдаёт база.

Для этого есть явная application-модель:

```text
Application\Calls\IncomingCallRegistration
```

Она содержит данные регистрации:

```text
ExternalCallId
PhoneNumber
kafkaMessageId
OperatorSearchMaxAttempts
OperatorSearchRetryDelay
CallHangupPolicy
initialStatus() → CallStatus::New
```

Именно эта модель задаёт creation semantics нового входящего звонка: начальный
статус `new`. `EloquentCallMapper` не выбирает `CallStatus::New` сам, а только
переводит уже подготовленную модель регистрации в persistence representation для
INSERT.

Поток создания:

```text
RegisterIncomingCallHandler
    ↓
IncomingCallRegistration
    ↓
CallWriteRepository::createIncoming(...)
    ↓
EloquentCallMapper::toIncomingInsertData(...)
    ↓
calls INSERT
    ↓
EloquentCallMapper::toDomain(...)
    ↓
Call::restore(...)
```

Так `kafka_message_id` остаётся metadata регистрации, а доменный `Call` после
сохранения восстанавливается уже с настоящим `CallId`.

## Граница агрегата

`Client` и `Operator` не являются внутренними Entity агрегата `Call`. `Call` не
владеет их жизненным циклом и не хранит внутри себя Eloquent-модели клиента или
оператора.

Связь выглядит так:

```text
Call
 │
 ├── ClientId ──────→ Client
 │
 └── OperatorId ────→ Operator / reservation
```

Внутри `Call` есть только `ClientId?` и `OperatorId?`. Это ссылки на внешние
объекты, а не вложенные persistence-модели.

Причины:

- клиент ищется по телефону и живёт в своей таблице `clients`;
- фактическая доступность оператора приходит извне;
- Calls владеет только короткой локальной бронью в `operators.reserved_call_id`
  и `operators.reserved_at`;
- reservation оператора имеет отдельные persistence и concurrency concerns:
  `SELECT ... FOR UPDATE SKIP LOCKED`, release по TTL, release после
  `connected`.

Поэтому reservation не превращается во внутреннюю Entity агрегата `Call`.
Concurrency и lifecycle reservation держит `EloquentOperatorReservationRepository`.

## Ответственность `Call`

Изменение бизнес-состояния звонка должно идти через поведение `Call`, а не через
прямое изменение колонок таблицы `calls`.

Текущие методы доменной модели:

- `attachClient()` - привязать найденного клиента или оставить `null`;
- `recordSuccessfulOperatorSearchAttempt()` - зафиксировать успешный поиск
  оператора и перейти к `assignment_requested`;
- `recordFailedOperatorSearchAttempt()` - зафиксировать неуспешный поиск и
  выбрать retry или финальный исход;
- `markOperatorDialing()` - принять факт `operator_dialing`;
- `markConnected()` - принять факт установленного моста;
- `failPendingOperatorAssignment()` - обработать неуспешную попытку дозвона
  оператору;
- `markHungUp()` - обработать hangup до соединения.

`Call` проверяет допустимость переходов и сохраняет инварианты звонка. Например:

- поздний факт по старой попытке не должен менять текущую попытку;
- факт после финального статуса не открывает звонок заново;
- факт после `connected` не переводит звонок в `missed`;
- retry возможен только пока не исчерпаны `OperatorSearchMaxAttempts`.

Application handler оркестрирует транзакцию, repository, outbox, queue, logger и
metrics. Но решение, можно ли перевести конкретный звонок в новый бизнес-статус,
остаётся в `Domain\Calls\Call`.

## Persistence -> Domain Restoration

В PostgreSQL доменная модель хранится как набор persistence primitives:

```text
integer
string
timestamp
nullable values
...
```

После чтения из БД эти значения надо вернуть на язык Domain:

```text
PostgreSQL
    ↓
Eloquent / DB representation
    ↓
Infrastructure Repository
    ↓
Infrastructure Mapper
    ↓
Value Objects / Enum
    ↓
Call::restore(...)
    ↓
Call Aggregate Root
```

Примеры преобразования:

```text
calls.id
    → CallId

calls.external_call_id
    → ExternalCallId

calls.phone
    → PhoneNumber

calls.status
    → CallStatus

calls.client_id
    → ClientId?

calls.operator_id
    → OperatorId?

calls.operator_search_attempts
    → OperatorSearchAttempts

calls.operator_search_max_attempts
    → OperatorSearchMaxAttempts

calls.operator_search_retry_delay_seconds
    → OperatorSearchRetryDelay

calls.operator_search_hangup_policy
    → CallHangupPolicy

timestamp columns
    → Timestamp?
```

`Infrastructure\Calls\Persistence\EloquentCallMapper::toDomain()` читает raw
значения Eloquent record и вызывает `Call::restore(...)`. Если обязательный
`created_at` отсутствует, mapper падает с ошибкой corrupted persistence data,
а не придумывает историческое время через `Timestamp::now()`.

## Роль Mapper

Простое правило:

> Repository получает и сохраняет данные. Mapper переводит persistence
> representation на язык Domain и обратно. Domain определяет, что эти данные
> означают и какие правила для них действуют.

Mapper знает соответствие:

```text
DB column/value
        ↕
Domain representation
```

Примеры:

```text
operator_id
    ↕
OperatorId

status
    ↕
CallStatus

attempts
    ↕
TelephonyOutboxMessage.attempts
```

Mapper не должен решать:

- можно ли сейчас назначить оператора;
- можно ли перевести звонок в `connected`;
- исчерпаны ли попытки поиска;
- какой следующий state допустим;
- когда claim outbox должен увеличить `attempts`;
- когда запись должна перейти в `processing`, `pending`, `published` или
  `failed`.

Эти решения принадлежат Domain/Application и repository lifecycle.

Точная формула:

> Repository достаёт данные. Mapper переводит их на язык Domain. Domain
> определяет их смысл и обеспечивает инварианты.

Не надо говорить, что Infrastructure "возвращает инварианты". Infrastructure
восстанавливает Domain-модель из сохранённых данных, а Domain снова получает
свои типы и применяет собственные правила.

## Repository / Mapper / Domain

```text
Repository
│
├── queries
├── transactions
├── locking
├── persistence lifecycle
└── получение/сохранение данных


Mapper
│
├── persistence primitives → Domain types
├── persistence record → Aggregate
└── Domain state → persistence representation


Domain
│
├── identity
├── business meaning
├── state transitions
└── invariants
```

Коротко:

```text
Repository ≠ Mapper
Mapper ≠ Domain
Domain ≠ Persistence Model
```

## Eloquent Model и Domain Model

`App\Models\Call` - Eloquent Model. Он описывает, как состояние хранится в
таблице `calls`: fillable-поля, связи `client()` и `operator()`, persistence
lifecycle Laravel.

`Domain\Calls\Call` - Domain Model и Aggregate Root. Он описывает, что это
состояние значит для звонка и какие переходы допустимы.

Главное различие:

> Eloquent Model описывает, как состояние хранится. `Call` описывает, что это
> состояние означает для бизнеса.

Это не критика Active Record. Это граница текущей архитектуры Calls: Laravel
остаётся adapter-ом, а бизнес-правила остаются в Domain.

## Текущие mapper-ы

`Infrastructure\Calls\Persistence\EloquentCallMapper`

- `toDomain()` восстанавливает `Call` из `App\Models\Call`;
- `toIncomingInsertData()` переводит `IncomingCallRegistration` в данные для
  INSERT;
- `toUpdateData()` переводит изменённый `Call` в данные для UPDATE;
- не выбирает начальный статус нового звонка;
- не управляет `updated_at`, транзакциями, locks и lifecycle.

`Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxMapper`

- переводит DB record из `telephony_outbox` в `TelephonyOutboxMessage`;
- не увеличивает `attempts`;
- не решает retry/failure outcome.

`Infrastructure\Operators\Persistence\EloquentOperatorReservationMapper`

- переводит `App\Models\Operator` в `OperatorReservation`;
- не выбирает оператора и не управляет locks.

`Infrastructure\Clients\Persistence\EloquentClientMapper`

- переводит raw id клиента в `ClientId`;
- оставлен как маленькая, но явная persistence boundary между `clients.id` и
  доменным `ClientId`.

Соответствующие repository:

- `EloquentCallRepository` делает запросы по `calls`, lock для обработки,
  создание входящего звонка из `IncomingCallRegistration` и сохранение
  изменённого агрегата;
- `EloquentTelephonyOutboxRepository` claim-ит due records, увеличивает
  `attempts`, меняет `status`, requeue-ит stale processing records и фиксирует
  publish/failure lifecycle. В PostgreSQL publish claim использует
  `UPDATE ... RETURNING`, чтобы вернуть post-claim состояние без повторного
  SELECT; остальные драйверы используют portable fallback;
- `EloquentOperatorReservationRepository` выбирает доступного оператора под
  lock, ставит и снимает reservation;
- `EloquentClientReadRepository` ищет клиента по телефону.

## Диаграмма

PlantUML-диаграмма границы агрегата лежит в
[diagrams/06-call-aggregate-boundary.puml](diagrams/06-call-aggregate-boundary.puml).

Она показывает, что `Client` и `Operator` находятся за пределами aggregate
boundary `Call`, а внутри агрегата есть только ссылки `ClientId` и `OperatorId`.
