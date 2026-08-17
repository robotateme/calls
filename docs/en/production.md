# Production

## Processes

Production must run separate long-lived processes:

| Process | Command | Scaling |
|---|---|---|
| Scheduler | `php artisan schedule:work` | 1 per deployment |
| Calls queue | `php artisan queue:work redis --queue=calls --tries=1 --timeout=0` | many |
| Calls retry queue | `php artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0` | many |
| Incoming calls consumer | `php artisan calls:kafka:consume incoming-calls --group=calls-incoming --source=incoming-calls-consumer --limit=1000 --timeout-ms=1000` | by partition count |
| Telephony facts consumer | `php artisan calls:kafka:consume telephony.facts --group=calls-telephony-facts --source=telephony-facts-consumer --limit=1000 --timeout-ms=1000` | by partition count |
| Outbox publisher | `php artisan calls:telephony-outbox:publish --limit=100` | many |

Scheduler runs:

- `calls:telephony-outbox:publish`;
- `calls:telephony-outbox:requeue-stale`;
- `calls:operator-reservations:release-expired`;
- `calls:metrics:snapshot`;
- `calls:dead-letter:prune-resolved`.

If outbox grows faster than scheduler can publish it, run additional standalone
outbox publishers.

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
php artisan calls:dead-letter:prune-resolved --older-than-days=30
```

DLQ growth is a message, deployment, or upstream producer problem. It is not a
normal task backlog.

When DLQ grows:

1. Inspect new `reason` values.
2. Inspect example payload.
3. Check last deployment and producer.
4. Do not delete records manually before the failure is understood.

## Metrics

Endpoint:

```text
GET /metrics
```

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
sum by (status) (calls_current)
operators_reserved_current
sum by (status) (telephony_outbox_current)
dead_letter_current
oldest_waiting_call_age_seconds
oldest_outbox_message_age_seconds
```

If `up{job="calls"}` is 0, Prometheus cannot scrape `/metrics`. If gauges do not
change, check `calls:metrics:snapshot` and scheduler. If counters do not grow
on test events, check handlers and Redis queue.

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
