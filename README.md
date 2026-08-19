# Calls

[Русская версия](README.ru.md)

Calls is a Laravel service that processes an inbound call until a client and an
operator are connected.

The service has no user-facing UI. HTTP is limited to `/metrics`.

In short: Calls receives an inbound-call fact, finds a client, reserves an
operator, asks Telephony to connect them, and waits for Telephony facts. Once the
call reaches `connected`, Calls releases its local reservation and stops owning
the call flow.

## What It Does

- Reads inbound calls from Kafka.
- Deduplicates by `external_call_id`.
- Looks up a client by phone number.
- Finds an operator and creates a short local reservation.
- Writes Telephony commands to `telephony_outbox`.
- Reads Telephony facts from Kafka.
- Releases the reservation after `connected`.

What it does not do:

- it does not accept calls over HTTP;
- it does not call Telephony directly;
- it does not manage SIP or the conversation after `connected`;
- it does not decide whether an operator is available after connection.

## Reading Order

1. [Solution](docs/en/solution.md) - the full call path in plain language.
2. [Kafka](docs/en/kafka-contracts.md) - incoming and outgoing messages.
3. [Architecture](docs/en/architecture.md) - layers and dependency rules.
4. [Domain Model](docs/en/domain-model.md) - `Call` aggregate root and
   persistence mapping.
5. [ADR](docs/en/adr/README.md) - why Kafka, layers, reservations, locks, and
   metrics snapshots were chosen.
6. [Observability](docs/en/observability.md) - Prometheus, Grafana, metrics
   flow, and smoke checks.
7. [Production](docs/en/production.md) - required long-running processes.

## Documents

- [Architecture](docs/en/architecture.md)
- [Domain Model and Infrastructure Mapping](docs/en/domain-model.md)
- [Solution](docs/en/solution.md)
- [Kafka](docs/en/kafka-contracts.md)
- [ADR](docs/en/adr/README.md)
- [Diagrams](docs/en/diagrams.md)
- [Observability](docs/en/observability.md)
- [Production](docs/en/production.md)
- [Load Testing](docs/en/load-testing.md)

## Main Rules

- Kafka is the authoritative ingress.
- `external_call_id` is the business key of a call.
- Kafka key for all events of one call must be `external_call_id`.
- `operator_dialing` is the current external fact and internal Calls status.
- Operator reservation is stored in `operators.reserved_call_id` and
  `operators.reserved_at`.
- Telephony commands always go through `telephony_outbox`.
- `/metrics` renders cached metrics only and does not run heavy SQL.
- Invalid Kafka messages go to `dead_letter_messages`.
- Facts after `connected` do not move the Calls state machine backwards.

## Tables

- `calls` - call state until `connected`;
- `clients` - client lookup by phone number;
- `operators` - external availability plus Calls local reservation;
- `telephony_outbox` - commands for Telephony;
- `dead_letter_messages` - Kafka messages that cannot be safely applied.

Statuses and transitions are described in
[docs/en/solution.md](docs/en/solution.md).

## Local Run

Docker services:

- PostgreSQL: `pgsql:5432`
- Redis: `redis:6379`
- Kafka: `kafka:9092` in Docker, `localhost:9094` from host
- Kafka UI: `http://localhost:8081`
- Prometheus: `http://localhost:9090`
- AlertManager: `http://localhost:9093`
- Grafana: `http://localhost:3000`

```bash
cp .env.example .env
./vendor/bin/sail up -d
./vendor/bin/sail artisan key:generate
./vendor/bin/sail artisan migrate
```

Checks:

```bash
./vendor/bin/sail test
./vendor/bin/sail composer phpstan
```

PostgreSQL integration tests run as part of `php artisan test` when a database
from `PG_INTEGRATION_*` is available. CI starts PostgreSQL as a service. Locally:

```bash
./vendor/bin/sail up -d pgsql
PG_INTEGRATION_HOST=127.0.0.1 PG_INTEGRATION_DATABASE=testing php artisan test
```

## Make

Common commands:

```bash
make up
make migrate
make queue
make queue-retry
make schedule
make outbox-publish
make outbox-requeue-stale
make release-expired-reservations
make metrics-snapshot
make prometheus-ready
make prometheus-targets
make prometheus-query QUERY='up{job="calls"}'
make prometheus-rules
make prometheus-alerts
make prometheus-smoke
make alertmanager
make alertmanager-ready
make alertmanager-alerts
make grafana
make grafana-ready
make kafka-consume TOPIC=incoming-calls
make load-jsonl COUNT=1000
make dead-letter-list
make dead-letter-prune
make validate
```

## Kafka

Local mode:

```env
KAFKA_CONSUMER_ADAPTER=jsonl
KAFKA_PRODUCER_ADAPTER=console
```

`rdkafka` mode:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

The runtime image must contain `php-rdkafka` before `rdkafka` mode is enabled.

## Metrics, Prometheus, AlertManager, and Grafana

Endpoint:

```text
GET /metrics
```

Local Prometheus scrapes:

```text
http://laravel.test/metrics
```

Local Grafana opens at:

```text
http://localhost:3000
```

Default local login is `admin` / `admin`. Grafana is provisioned with the
Prometheus datasource and the `Calls Overview` dashboard.

Local AlertManager opens at:

```text
http://localhost:9093
```

Check required series:

```bash
make prometheus-smoke
```

Full observability flow, Grafana provisioning, metric list, and PromQL smoke
queries: [docs/en/observability.md](docs/en/observability.md).
