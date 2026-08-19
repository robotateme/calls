# Production

## Processes

Production must run separate long-lived processes:

| Process | Command | Scaling |
|---|---|---|
| Scheduler | `php artisan schedule:work` | 1 per deployment |
| Calls queue | `php artisan queue:work redis --queue=calls --tries=1 --timeout=0` | many |
| Calls retry queue | `php artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0` | many |
| Incoming calls consumer | `php artisan calls:kafka:consume-daemon incoming-calls --group=calls-incoming --source=incoming-calls-consumer --limit=1000 --timeout-ms=1000` | by partition count |
| Telephony facts consumer | `php artisan calls:kafka:consume-daemon telephony.facts --group=calls-telephony-facts --source=telephony-facts-consumer --limit=1000 --timeout-ms=1000` | by partition count |
| Outbox publisher | `php artisan calls:telephony-outbox:publish-daemon --limit=100 --interval=1` | many |

Scheduler runs:

- `calls:telephony-outbox:publish`;
- `calls:telephony-outbox:requeue-stale`;
- `calls:operator-reservations:release-expired`;
- `calls:metrics:snapshot`;
- `calls:dead-letter:prune-resolved`.

If outbox grows faster than scheduler can publish it, run additional standalone
outbox publishers.

Production process managers must send `SIGTERM` and wait for graceful shutdown.
The bundled supervisor config uses `stopasgroup=true`, `killasgroup=true`,
`stopsignal=TERM`, and `stopwaitsecs=30`. Kafka consumer and outbox publisher
daemon commands stop after the current consume/publish iteration completes.
Laravel queue workers handle `SIGTERM`; use `php artisan queue:restart` during
deployments instead of relying on `--stop-when-empty` for long-lived workers.

Important: one HTTP process is not enough. Without scheduler, metrics and stale
reservations are not updated. Without queue workers, calls are not processed.
Without Kafka consumers, Calls does not see new calls or Telephony facts. Without
outbox publisher, commands remain in DB and do not reach Telephony.

## Kafka

Local mode:

```env
KAFKA_CONSUMER_ADAPTER=jsonl
KAFKA_PRODUCER_ADAPTER=console
```

Production:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

`rdkafka` requires PHP extension `php-rdkafka`. If it is missing, the service
must fail when the Kafka adapter starts.

## Docker Image

Build:

```bash
docker build -f docker/production/Dockerfile -t calls:production .
```

The image contains:

- PHP 8.4 CLI;
- `pdo_pgsql`, `pcntl`, `sockets`, `opcache`;
- `redis`;
- `rdkafka`;
- `supervisor`.

Default command uses `docker/production/supervisord.conf`.

In Kubernetes, prefer one process per container and use commands from the table
above.

## Database and Redis

PostgreSQL:

- apply migrations before workers/consumers;
- monitor slow queries and lock wait;
- monitor size of `calls`, `telephony_outbox`, `dead_letter_messages`;
- archive completed calls and published outbox for large volumes.

Redis:

- queues `calls` and `calls-retry` must be separate;
- monitor queue depth and latency;
- when operators are unavailable, watch retry queue growth.

## DLQ

Commands:

```bash
php artisan calls:dead-letter:list
php artisan calls:dead-letter:list --reason=invalid_payload
php artisan calls:dead-letter:resolve 123 --note="fixed upstream schema"
php artisan calls:dead-letter:replay --dry-run --id=123
php artisan calls:dead-letter:replay --id=123 --note="fixed handler"
php artisan calls:dead-letter:prune-resolved --older-than-days=30
```

DLQ growth is a message, deployment, or upstream producer problem. It is not a
normal task backlog.

When DLQ grows:

1. Inspect new `reason` values.
2. Inspect example payload.
3. Check last deployment and producer.
4. Use `calls:dead-letter:replay --dry-run` before replaying.
5. Replay only after the failure is understood.
6. Do not delete records manually before the failure is understood.

Replay is manual and audited. Successful replay marks the DLQ record resolved.
Failed replay leaves it unresolved and records the attempt in
`dead_letter_replay_attempts`. Reasons such as `invalid_json`,
`invalid_payload`, `missing_external_call_id`, `unknown_type`,
`unsupported_schema_version`, and `contract_violation` are blocked unless
`--force` is passed. Use `--force` only after inspecting the payload and producer
contract.

## Metrics

Endpoint:

```text
GET /metrics
```

Production must not expose `/metrics` to the public internet. Restrict it at the
network level to Prometheus IPs or a private monitoring network. Optional Basic
Auth can be enabled as a second layer:

```env
METRICS_BASIC_AUTH_USER=prometheus
METRICS_BASIC_AUTH_PASSWORD=change-me
```

If one of these values is set, both must be set and Prometheus must scrape with
matching credentials.

`/metrics` only renders prepared values. It does not run `COUNT`, `MIN`, or
grouping over PostgreSQL. Those queries are done by:

```bash
php artisan calls:metrics:snapshot
```

See [ADR-0004](adr/0004-metrics-scrape-from-cache.md).

Calls metrics:

- `calls_received_total`;
- `calls_deduplicated_total`;
- `call_transitions_total{from,to}`;
- `operator_reservation_attempts_total{result}`;
- `telephony_outbox_publish_total{result}`;
- `dead_letter_messages_total{reason}`;
- `dead_letter_records_total{source,topic,reason,result}`;
- `kafka_consumer_dlq_total{source,topic,reason}`;
- `kafka_consumer_failures_total{source,topic,reason}`;
- `calls_current{status}`;
- `operators_reserved_current`;
- `telephony_outbox_current{status}`;
- `dead_letter_current`;
- `oldest_waiting_call_age_seconds`;
- `oldest_outbox_message_age_seconds`.

External monitoring should also cover:

- Kafka consumer lag;
- Kafka produce/fetch latency;
- PostgreSQL slow queries and lock wait;
- Redis queue depth/latency;
- PHP worker restarts and memory;
- DLQ depth by reason;
- outbox depth by status.

PromQL smoke after deployment:

```promql
up{job="calls"}
rate(calls_received_total[5m])
rate(calls_deduplicated_total[5m])
sum by (from, to) (rate(call_transitions_total[5m]))
sum by (result) (rate(operator_reservation_attempts_total[5m]))
sum by (result) (rate(telephony_outbox_publish_total[5m]))
sum by (reason) (rate(dead_letter_messages_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_dlq_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_failures_total[5m]))
sum by (status) (calls_current)
operators_reserved_current
sum by (status) (telephony_outbox_current)
dead_letter_current
oldest_waiting_call_age_seconds
oldest_outbox_message_age_seconds
```

Grafana is local/provisioned as a dashboard layer over Prometheus:

- local URL: `http://localhost:3000`;
- default local login: `admin` / `admin`;
- datasource: `Prometheus` -> `http://prometheus:9090`;
- dashboard: `Calls Overview`.

Grafana must not query PostgreSQL working tables directly for these service
metrics. Calls metric values are still produced by handlers and
`calls:metrics:snapshot`, scraped by Prometheus from `/metrics`, and then read by
Grafana from Prometheus.

The local Grafana dashboard also includes external Prometheus series for:

- Kafka consumer group lag from `kafka-exporter`;
- Redis queue depth from the Calls snapshot metric `queue_depth`;
- Redis memory from `redis-exporter`;
- container CPU and memory from cAdvisor.

Production must provide equivalent Kafka, Redis, and container metrics through
the environment monitoring stack. The local Compose exporters are a development
baseline, not a production deployment requirement.

Detailed local observability setup: [observability.md](observability.md).

If `up{job="calls"}` is 0, Prometheus cannot scrape `/metrics`. If gauges do not
change, check `calls:metrics:snapshot` and scheduler. If counters do not grow
on test events, check handlers and Redis queue.

## Alerting

Local Prometheus sends alerts to AlertManager:

```text
http://alertmanager:9093
```

Local AlertManager UI:

```text
http://localhost:9093
```

Alert rules live in:

```text
docker/prometheus/rules/calls-alerts.yml
```

Current rules cover:

- `/metrics` scrape failure;
- missing snapshot gauges;
- unresolved or newly increasing DLQ;
- Kafka messages sent to DLQ by source/topic/reason;
- Kafka consumer failures by source/topic/reason;
- high Kafka consumer group lag;
- high Redis queue depth for `calls` and `calls-retry`;
- high Kafka/DLQ processing error rate;
- external exporter scrape failures;
- failed outbox records;
- high pending outbox backlog;
- old pending outbox records;
- calls waiting too long for an operator.

Current rules do not cover explicit Kafka consumer/outbox publisher heartbeat,
Kafka broker produce/fetch latency, or Redis/container resource thresholds. Add
application heartbeat metrics, exporter-backed rules, or managed monitoring
alerts before paging on those signals.

The local AlertManager receiver is intentionally a no-op. Production must route
alerts to the real incident channel for the environment.

## Deploy

Order:

1. Migrations.
2. App image.
3. Scheduler.
4. Queue workers.
5. Kafka consumers.
6. Outbox publishers.

Rollback:

- stop consumers;
- stop outbox publishers;
- roll back app image;
- do not manually delete outbox/DLQ;
- inspect `telephony_outbox.failed`, `dead_letter_messages`, Kafka lag.

Reason: first stop input and outbound publishing so broken code stops creating
new commands. Then roll back the image and inspect unfinished messages.
