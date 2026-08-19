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
- `kafka-exporter` - Kafka consumer group lag metrics for Prometheus
- `redis-exporter` - Redis runtime metrics for Prometheus
- `cadvisor` - container CPU and memory metrics for Prometheus

Grafana local login:

```text
admin / admin
```

These credentials are for local development only. Override them with
`GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` when needed.

## Metrics Access

Production must restrict `/metrics` at the network level so only Prometheus can
reach it, for example through a private service, security group, ingress
allowlist, or Kubernetes `ClusterIP`.

Calls also supports Basic Auth and an optional app-level IP allowlist as a
second layer:

```env
METRICS_BASIC_AUTH_USER=prometheus
METRICS_BASIC_AUTH_PASSWORD=change-me
METRICS_ALLOWED_IPS=10.0.0.0/8,192.168.10.20
```

If either variable is configured, both must be present and match the Prometheus
scrape request. `METRICS_ALLOWED_IPS` accepts comma-separated exact IPs or CIDR
ranges. The local `.env.example` enables Basic Auth with development credentials
and the local Prometheus config scrapes with the same credentials. Production
must replace those values.

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
[production.md](production.md) and local exporter metrics:

- Kafka consumer lag: `kafka_consumergroup_lag`;
- Redis queue depth from Calls snapshot: `queue_depth`;
- Redis memory: `redis_memory_used_bytes`;
- container CPU: `container_cpu_usage_seconds_total`;
- container memory: `container_memory_working_set_bytes`.

Production can use different exporters or managed monitoring, but it must expose
equivalent Prometheus series before relying on these dashboard panels.

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
- `CallsKafkaConsumerLagHigh`: Kafka consumer group lag is above the local
  threshold.
- `CallsQueueDepthHigh`: Redis queue depth for `calls` or `calls-retry` is
  above the local threshold.
- `CallsProcessingErrorRateHigh`: Kafka DLQ and consumer failure rate is above
  the local percentage threshold.
- `CallsExternalMetricsTargetDown`: Prometheus cannot scrape one of the local
  exporter targets used by Grafana.
- `CallsOutboxFailedRecordsPresent`: failed outbox records exist.
- `CallsOutboxPendingHigh`: pending outbox backlog is above the local threshold.
- `CallsOutboxOldestPendingTooOld`: oldest pending outbox record is older than
  the local threshold.
- `CallsWaitingTooLong`: oldest waiting call is older than the local threshold.

Thresholds in local rules are starting values, not production SLOs. Tune them
per environment before using them for paging.

The local stack covers Kafka lag, Redis queue depth, Redis memory, and container
CPU/memory in Grafana. It still does not emit explicit heartbeat metrics from
Kafka consumers or the outbox publisher, and it does not cover Kafka broker
produce/fetch latency as explicit service SLOs. Add application heartbeat
metrics, Kafka broker metrics, or managed Kafka monitoring before paging on
those signals.

The local AlertManager receiver is intentionally a no-op. Production must mount
an environment-specific AlertManager configuration with a real incident receiver
such as PagerDuty, Opsgenie, or a dedicated on-call webhook.

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

If external panels are empty, check Prometheus scrape targets for `kafka`,
`redis`, and `containers`. Those panels depend on local exporters, not on the
Calls `/metrics` endpoint.

If counters do not grow during test events, check handlers, Redis queue workers,
and Kafka consumers.

Kafka/DLQ failure counters:

```promql
sum by (source, topic, reason) (rate(kafka_consumer_dlq_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_failures_total[5m]))
sum by (source, topic, reason, result) (rate(dead_letter_records_total[5m]))
```

External dashboard queries:

```promql
sum by (consumergroup, topic) (kafka_consumergroup_lag)
sum by (queue) (queue_depth)
redis_memory_used_bytes
sum by (name) (rate(container_cpu_usage_seconds_total{image!=""}[5m]))
sum by (name) (container_memory_working_set_bytes{image!=""})
```

If Grafana cannot query Prometheus, check that the datasource URL inside Docker
is `http://prometheus:9090`, not `localhost:9090`.

## Do Not

- Do not add SQL aggregation to `/metrics`.
- Do not point Grafana at PostgreSQL working tables for Calls service metrics.
- Do not treat Grafana as a source of truth for business state.
- Do not treat AlertManager as a repair or replay mechanism.
- Do not put production credentials in docs or Compose defaults.
