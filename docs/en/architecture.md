# Architecture

Calls is split into layers. Business rules must not depend on Laravel.

The point is simple: a call rule must be understandable without a database,
queue, or HTTP. Laravel starts code, reads config, writes to the database, and
wires parts together.

## Layers

- `src/Domain` - statuses, call rules, value objects.
- `src/Application` - command handlers and ports to the outside world.
- `src/Infrastructure` - database, Redis, Kafka, metrics.
- `app` - Laravel jobs, providers, HTTP and console wiring.

Rules:

- `Domain` does not import Laravel, `app`, `Application`, or `Infrastructure`.
- `Application` does not depend on Laravel or concrete database/Kafka adapters.
- `Infrastructure` may use Laravel and implements `Application` interfaces.
- `config()` and `env()` stay in `app` or `Infrastructure`.

If a rule can be described in business terms, it must not live in a controller,
job, Eloquent model, or config file. If code talks to DB, Kafka, Redis, or
Laravel, it is not `Domain`.

`tests/Unit/ArchitectureBoundaryTest.php` checks this boundary.

## Areas

- `Application\Calls` - call flow until `connected`.
- `Application\Clients` - client lookup by phone.
- `Application\Operators` - local operator reservation.
- `Application\Telephony` - commands written to `telephony_outbox`.
- `Application\Shared` - transactions, queue, Kafka, DLQ, metrics.

## Repositories

Repository interfaces in `Application` return only:

- a domain object or value object;
- `null`;
- `void`;
- lists of domain objects.

They do not return Eloquent models, Laravel collections, raw arrays, DTOs,
`stdClass`, or scalar ids.

Eloquent models and DB rows are persistence representation only. They are mapped
into domain objects inside `Infrastructure\*\Persistence\*Mapper`.

Reason: an `Application` handler should work with a call, operator, or id it can
understand, not with the way Laravel stores a row.

Detailed notes about `Call` as Aggregate Root, aggregate boundary, and
Repository/Mapper/Domain responsibilities:
[domain-model.md](domain-model.md).

## Where Rules Live

- Call path, statuses, retry, outbox, and DLQ: [solution.md](solution.md).
- Aggregate Root `Call` and persistence mapping: [domain-model.md](domain-model.md).
- Kafka topics, payload, and key: [kafka-contracts.md](kafka-contracts.md).
- Layering, Kafka, locks, reservations, and metrics snapshots:
  [adr/README.md](adr/README.md).
- Production processes: [production.md](production.md).

## Responsibilities

| Part | Does | Does Not Do |
|---|---|---|
| Calls | Runs the call until `connected`, selects an operator, writes Telephony commands | Does not manage the conversation after `connected` |
| Outbox publisher | Publishes `telephony_outbox` to Kafka | Does not make business decisions |
| Redis queue | Runs internal jobs | Does not replace Kafka |
| DLQ | Stores bad Kafka messages | Does not fix them automatically |
| Telephony | Calls the operator and bridges the conversation | Does not select the operator |
| Operator Availability | Knows real operator availability | Does not store Calls call status |
| Clients | Provides a client by phone | Does not participate in assignment |
