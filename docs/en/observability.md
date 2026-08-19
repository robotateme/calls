# Observability

Calls exposes service metrics through `/metrics`. Prometheus scrapes that
endpoint, evaluates alert rules, and sends alerts to AlertManager. Grafana reads
Prometheus and shows dashboards.

Grafana and AlertManager are not part of the call-processing path. If either is
down, calls still register, retry, write outbox commands, and process Telephony
facts.

## Responsibility Boundary

| Component | Does | Does Not Do |
|---|---|---|
| Calls handlers | Increment counters and record business events | Do not render dashboards |
| `calls:metrics:snapshot` | Calculates DB aggregation gauges on schedule | Does not publish Kafka commands |
| `/metrics` | Renders cached Prometheus series | Does not run heavy SQL |
| Prometheus | Scrapes `/metrics`, stores time series, evaluates alert rules | Does not query PostgreSQL directly |
| AlertManager | Receives alerts from Prometheus | Does not decide business state or repair data |
| Grafana | Reads Prometheus and renders dashboards | Does not query Calls working tables |

The key rule is still ADR-0004: `/metrics` renders cached values only.

## Local Stack

Docker Compose services:

- `prometheus` - `http://localhost:9090`
- `alertmanager` - `http://localhost:9093`
- `grafana` - `http://localhost:3000`

Grafana local login:

```text
admin / admin
```

These credentials are for local development only. Override them with
`GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` when needed.

## Data Flow

```text
Handlers and snapshot job
    -> PrometheusMetricsStore
    -> GET /metrics
    -> Prometheus
    -> AlertManager
    -> Grafana
```

Heavy PostgreSQL queries belong in:

```bash
php artisan calls:metrics:snapshot
```

They do not belong in `MetricsController`, Prometheus scrape path, or Grafana
datasource queries.

## Provisioning

Prometheus files:

- `docker/prometheus/prometheus.yml`
- `docker/prometheus/rules/calls-alerts.yml`

AlertManager file:

- `docker/alertmanager/alertmanager.yml`

Grafana provisioning files:

- `docker/grafana/provisioning/datasources/prometheus.yml`
- `docker/grafana/provisioning/dashboards/calls.yml`
- `docker/grafana/dashboards/calls-overview.json`

Provisioned datasource:

```text
Prometheus -> http://prometheus:9090
```

Provisioned dashboard:

```text
Calls Overview
```

The dashboard uses the existing Calls metrics documented in
[production.md](production.md).

## Alert Rules

Local Prometheus loads Calls alert rules from
`docker/prometheus/rules/calls-alerts.yml`.

Current rules:

- `CallsMetricsTargetDown`: Prometheus cannot scrape `/metrics`.
- `CallsMetricsSeriesMissing`: snapshot gauges such as `calls_current` are
  absent.
- `CallsDeadLettersPresent`: unresolved DLQ records exist.
- `CallsDeadLettersIncreasing`: new DLQ records appeared in the last 5 minutes.
- `CallsKafkaMessagesSentToDlq`: Kafka messages were rejected into DLQ by
  source, topic, and reason.
- `CallsKafkaConsumerFailures`: Kafka consumer boundary failed by source, topic,
  and reason.
- `CallsOutboxFailedRecordsPresent`: failed outbox records exist.
- `CallsOutboxPendingHigh`: pending outbox backlog is above the local threshold.
- `CallsOutboxOldestPendingTooOld`: oldest pending outbox record is older than
  the local threshold.
- `CallsWaitingTooLong`: oldest waiting call is older than the local threshold.

Thresholds in local rules are starting values, not production SLOs. Tune them
per environment before using them for paging.

Not covered by current Calls metrics yet:

- Redis queue depth;
- Kafka consumer heartbeat;
- container CPU and memory.

Those need Redis/Kafka/container exporters or new application metrics.

## Local Commands

```bash
make up
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
```

Open dashboard:

```text
http://localhost:3000/d/calls-overview/calls-overview
```

## Smoke Checks

Prometheus readiness:

```bash
make prometheus-ready
```

Prometheus sees Calls:

```bash
make prometheus-query QUERY='up{job="calls"}'
```

Grafana health:

```bash
make grafana-ready
```

AlertManager readiness:

```bash
make alertmanager-ready
```

Loaded Prometheus alert rules:

```bash
make prometheus-rules
```

Required Calls series:

```bash
make prometheus-smoke
```

## Troubleshooting

If `up{job="calls"}` is `0`, Prometheus cannot scrape Calls. Check that
`laravel.test` is running and `/metrics` responds.

If Grafana starts but the dashboard is missing, check provisioning logs:

```bash
docker compose logs grafana
```

If alerts are not visible, check:

- Prometheus loaded `calls.rules`;
- AlertManager is ready;
- Prometheus can reach `alertmanager:9093` inside Docker.

If gauges do not change, check:

- scheduler is running;
- `calls:metrics:snapshot` succeeds;
- Prometheus scrape target is healthy.

If counters do not grow during test events, check handlers, Redis queue workers,
and Kafka consumers.

Kafka/DLQ failure counters:

```promql
sum by (source, topic, reason) (rate(kafka_consumer_dlq_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_failures_total[5m]))
sum by (source, topic, reason, result) (rate(dead_letter_records_total[5m]))
```

If Grafana cannot query Prometheus, check that the datasource URL inside Docker
is `http://prometheus:9090`, not `localhost:9090`.

## Do Not

- Do not add SQL aggregation to `/metrics`.
- Do not point Grafana at PostgreSQL working tables for Calls service metrics.
- Do not treat Grafana as a source of truth for business state.
- Do not treat AlertManager as a repair or replay mechanism.
- Do not put production credentials in docs or Compose defaults.
