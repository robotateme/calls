# ADR-0005: Internal Architecture Is Split into Layers

Status: Accepted

## Context

Calls has business rules that should stay understandable without Laravel,
PostgreSQL, Redis, or Kafka:

- call state transitions;
- retry and exhausted outcomes;
- late event handling;
- operator assignment policy;
- outbox and DLQ orchestration.

If these rules are mixed into Eloquent models, jobs, controllers, or config, the
system becomes hard to test and easy to break.

## Decision

Use the current layered layout:

```text
src/Domain
src/Application
src/Infrastructure
app
```

Dependency direction:

```text
Infrastructure -> Application -> Domain
app -> Application/Infrastructure
```

Rules:

- `Domain` has no Laravel, `app`, `Application`, or `Infrastructure` imports.
- `Application` depends on `Domain` and its own ports.
- `Infrastructure` implements ports and may use Laravel.
- `app` wires framework entrypoints to application use cases.

Architecture tests enforce these rules.

## Consequences

- Business behavior is testable without framework adapters.
- Repository ports return domain objects/value objects, not Eloquent or raw rows.
- Runtime config stays in `app`/`Infrastructure`.
- Jobs, console commands, controllers, and Kafka consumers stay thin.
- New code must choose the layer by responsibility, not convenience.

## Rejected

- Put business rules in Eloquent models.
- Let Application depend on concrete DB/Kafka adapters.
- Add generic mapper frameworks or extra layers without need.
- Bypass outbox with direct Telephony calls.
