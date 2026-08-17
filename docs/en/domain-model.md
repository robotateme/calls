# Domain Model and Infrastructure Mapping

This document describes the current Calls domain model and how it is restored
from PostgreSQL. It is not a generic DDD guide.

## Aggregate Root `Call`

The main aggregate in Calls is `Domain\Calls\Call`.

```text
Call
├── Entity
└── Aggregate Root
```

`Call` is an Entity because it has identity: `Domain\Calls\CallId`.

`Call` is an Aggregate Root because it controls consistency of the call
lifecycle: operator assignment, search attempts, transitions to
`operator_dialing`, `connected`, and final statuses.

Short formula:

> `Call` is an Entity by nature and an Aggregate Root by role.

Every Aggregate Root is an Entity, but not every Entity is an Aggregate Root. In
this project there is no separate `CallEntity` and `CallAggregate`; that would be
an artificial split.

## Aggregate Contents

Current `Call` contains domain types and references to external entities:

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
└── state-change rules
```

Real classes:

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

`kafka_message_id` is stored in `calls` as ingress/idempotency metadata for
incoming Kafka registration. It is not part of the domain `Call` object.

## Incoming Call Creation Model

A new call does not start as a complete `Call`: before INSERT it has no `CallId`,
because the database assigns the id.

Creation uses an explicit application model:

```text
Application\Calls\IncomingCallRegistration
```

It contains:

```text
ExternalCallId
PhoneNumber
kafkaMessageId
OperatorSearchMaxAttempts
OperatorSearchRetryDelay
CallHangupPolicy
initialStatus() → CallStatus::New
```

This model owns creation semantics for an inbound call: initial status `new`.
`EloquentCallMapper` does not choose `CallStatus::New`; it maps the prepared
registration model to persistence data for INSERT.

Creation flow:

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

This keeps `kafka_message_id` as registration metadata, while saved `Call` is
restored with a real `CallId`.

## Aggregate Boundary

`Client` and `Operator` are not internal Entities of the `Call` aggregate. `Call`
does not own their lifecycle and does not hold Eloquent models inside.

Relationship:

```text
Call
 │
 ├── ClientId ──────→ Client
 │
 └── OperatorId ────→ Operator / reservation
```

Inside `Call` there are only `ClientId?` and `OperatorId?`: references to
external objects, not nested persistence models.

Reasons:

- client is looked up by phone and lives in `clients`;
- real operator availability comes from outside;
- Calls owns only a short local reservation in `operators.reserved_call_id` and
  `operators.reserved_at`;
- reservation has separate persistence and concurrency concerns:
  `SELECT ... FOR UPDATE SKIP LOCKED`, TTL release, release after `connected`.

Reservation is therefore not modeled as an internal Entity of `Call`.
`EloquentOperatorReservationRepository` owns its concurrency and lifecycle.

## `Call` Responsibility

Business state changes must go through `Call` behavior, not direct updates of
`calls` columns.

Current domain methods:

- `attachClient()` - attach found client or keep `null`;
- `recordSuccessfulOperatorSearchAttempt()` - record successful operator search
  and move to `assignment_requested`;
- `recordFailedOperatorSearchAttempt()` - record failed search and choose retry
  or final outcome;
- `markOperatorDialing()` - accept `operator_dialing`;
- `markConnected()` - accept bridge-established fact;
- `failPendingOperatorAssignment()` - handle failed operator attempt;
- `markHungUp()` - handle hangup before connection.

`Call` validates transitions and keeps call invariants:

- old attempt facts do not change the current attempt;
- facts after final status do not reopen a call;
- facts after `connected` do not move the call to `missed`;
- retry is possible only while attempts remain.

Application handlers orchestrate transaction, repository, outbox, queue, logger,
and metrics. The decision whether a call can move to another business status
stays in `Domain\Calls\Call`.

## Persistence -> Domain Restoration

In PostgreSQL, the domain model is stored as persistence primitives:

```text
integer
string
timestamp
nullable values
...
```

After reading from DB, those values must be restored into Domain language:

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

Examples:

```text
calls.id → CallId
calls.external_call_id → ExternalCallId
calls.phone → PhoneNumber
calls.status → CallStatus
calls.client_id → ClientId?
calls.operator_id → OperatorId?
calls.operator_search_attempts → OperatorSearchAttempts
calls.operator_search_max_attempts → OperatorSearchMaxAttempts
calls.operator_search_retry_delay_seconds → OperatorSearchRetryDelay
calls.operator_search_hangup_policy → CallHangupPolicy
timestamp columns → Timestamp?
```

`Infrastructure\Calls\Persistence\EloquentCallMapper::toDomain()` reads raw
Eloquent values and calls `Call::restore(...)`. If required `created_at` is
missing, mapping fails as corrupted persistence data instead of inventing
historical time with `Timestamp::now()`.

## Mapper Role

Rule:

> Repository obtains and saves data. Mapper translates persistence
> representation to Domain language and back. Domain defines what the data means
> and which rules apply.

Mapper knows:

```text
DB column/value
        ↕
Domain representation
```

Examples:

```text
operator_id ↕ OperatorId
status ↕ CallStatus
attempts ↕ TelephonyOutboxMessage.attempts
```

Mapper must not decide:

- whether an operator can be assigned now;
- whether the call can move to `connected`;
- whether search attempts are exhausted;
- which next state is valid;
- when outbox claim increments `attempts`;
- when a row moves to `processing`, `pending`, `published`, or `failed`.

Those decisions belong to Domain/Application and repository lifecycle.

Accurate formula:

> Repository fetches data. Mapper translates it to Domain language. Domain
> defines meaning and enforces invariants.

Infrastructure does not "return invariants". It restores the Domain model from
saved data, and Domain receives its own types again.

## Repository / Mapper / Domain

```text
Repository
│
├── queries
├── transactions
├── locking
├── persistence lifecycle
└── data read/write


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

Short:

```text
Repository ≠ Mapper
Mapper ≠ Domain
Domain ≠ Persistence Model
```

## Eloquent Model and Domain Model

`App\Models\Call` is an Eloquent Model. It describes how state is stored in
`calls`: fillable fields, `client()` and `operator()` relations, Laravel
persistence lifecycle.

`Domain\Calls\Call` is a Domain Model and Aggregate Root. It describes what the
state means for the call and which transitions are allowed.

Main difference:

> Eloquent Model describes how state is stored. `Call` describes what this state
> means for the business.

This is not criticism of Active Record. It is the boundary chosen by Calls:
Laravel stays an adapter, business rules stay in Domain.

## Current Mappers

`Infrastructure\Calls\Persistence\EloquentCallMapper`

- `toDomain()` restores `Call` from `App\Models\Call`;
- `toIncomingInsertData()` maps `IncomingCallRegistration` to INSERT data;
- `toUpdateData()` maps changed `Call` to UPDATE data;
- does not choose initial status for a new call;
- does not manage `updated_at`, transactions, locks, or lifecycle.

`Infrastructure\Telephony\Outbox\EloquentTelephonyOutboxMapper`

- maps `telephony_outbox` DB record to `TelephonyOutboxMessage`;
- does not increment `attempts`;
- does not decide retry/failure outcome.

`Infrastructure\Operators\Persistence\EloquentOperatorReservationMapper`

- maps `App\Models\Operator` to `OperatorReservation`;
- does not choose operator and does not manage locks.

`Infrastructure\Clients\Persistence\EloquentClientMapper`

- maps raw client id to `ClientId`;
- remains as a small explicit boundary between `clients.id` and domain `ClientId`.

Repositories:

- `EloquentCallRepository` queries `calls`, locks rows for processing, creates
  inbound calls from `IncomingCallRegistration`, and saves changed aggregates;
- `EloquentTelephonyOutboxRepository` claims due records, increments `attempts`,
  changes `status`, requeues stale processing records, and records
  publish/failure lifecycle. PostgreSQL publish claim uses
  `UPDATE ... RETURNING` to return post-claim state without a second SELECT;
  other drivers use the portable fallback;
- `EloquentOperatorReservationRepository` selects available operator under lock,
  creates and releases reservation;
- `EloquentClientReadRepository` finds client by phone.

## Diagram

PlantUML aggregate boundary diagram:
[diagrams/06-call-aggregate-boundary.puml](diagrams/06-call-aggregate-boundary.puml).

It shows that `Client` and `Operator` are outside the `Call` aggregate boundary,
while the aggregate contains only `ClientId` and `OperatorId` references.
