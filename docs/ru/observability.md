# Observability

Calls отдаёт service-метрики через `/metrics`. Prometheus читает этот endpoint,
вычисляет alert rules и отправляет alerts в AlertManager. Grafana читает
Prometheus и показывает dashboards.

Grafana и AlertManager не участвуют в обработке звонка. Если один из них
недоступен, Calls всё ещё регистрирует звонки, делает retry, пишет команды в
outbox и обрабатывает facts от Telephony.

## Граница ответственности

| Компонент | Делает | Не делает |
|---|---|---|
| Calls handlers | Увеличивают counters и пишут business events | Не рисуют dashboards |
| `calls:metrics:snapshot` | Считает DB aggregation gauges по расписанию | Не публикует Kafka commands |
| `/metrics` | Отдаёт готовые Prometheus series | Не делает тяжёлый SQL |
| Prometheus | Читает `/metrics`, хранит time series, вычисляет alert rules | Не ходит напрямую в PostgreSQL |
| AlertManager | Получает alerts от Prometheus | Не решает business state и не чинит данные |
| Grafana | Читает Prometheus и рисует dashboards | Не ходит в рабочие таблицы Calls |

Главное правило остаётся из ADR-0004: `/metrics` отдаёт только готовые значения.

## Локальный стек

Docker Compose services:

- `prometheus` - `http://localhost:9090`
- `alertmanager` - `http://localhost:9093`
- `grafana` - `http://localhost:3000`

Локальный логин Grafana:

```text
admin / admin
```

Это credentials только для локальной разработки. При необходимости переопределяй
их через `GRAFANA_ADMIN_USER` и `GRAFANA_ADMIN_PASSWORD`.

## Поток данных

```text
Handlers and snapshot job
    -> PrometheusMetricsStore
    -> GET /metrics
    -> Prometheus
    -> AlertManager
    -> Grafana
```

Тяжёлые PostgreSQL-запросы должны жить здесь:

```bash
php artisan calls:metrics:snapshot
```

Их нельзя переносить в `MetricsController`, Prometheus scrape path или Grafana
datasource queries.

## Provisioning

Файлы Prometheus:

- `docker/prometheus/prometheus.yml`
- `docker/prometheus/rules/calls-alerts.yml`

Файл AlertManager:

- `docker/alertmanager/alertmanager.yml`

Файлы provisioning Grafana:

- `docker/grafana/provisioning/datasources/prometheus.yml`
- `docker/grafana/provisioning/dashboards/calls.yml`
- `docker/grafana/dashboards/calls-overview.json`

Datasource:

```text
Prometheus -> http://prometheus:9090
```

Dashboard:

```text
Calls Overview
```

Dashboard использует существующие метрики Calls из [production.md](production.md).

## Alert rules

Локальный Prometheus читает Calls alert rules из
`docker/prometheus/rules/calls-alerts.yml`.

Текущие rules:

- `CallsMetricsTargetDown`: Prometheus не может прочитать `/metrics`.
- `CallsMetricsSeriesMissing`: snapshot gauges вроде `calls_current`
  отсутствуют.
- `CallsDeadLettersPresent`: есть unresolved DLQ records.
- `CallsDeadLettersIncreasing`: за последние 5 минут появились новые DLQ
  records.
- `CallsKafkaMessagesSentToDlq`: Kafka-сообщения были отклонены в DLQ по
  source, topic и reason.
- `CallsKafkaConsumerFailures`: Kafka consumer boundary упал по source, topic и
  reason.
- `CallsOutboxFailedRecordsPresent`: есть failed outbox records.
- `CallsOutboxPendingHigh`: pending outbox backlog выше локального порога.
- `CallsOutboxOldestPendingTooOld`: самый старый pending outbox record старше
  локального порога.
- `CallsWaitingTooLong`: самый старый waiting call старше локального порога.

Локальные thresholds - стартовые значения, а не production SLO. Перед paging в
production их надо настроить под окружение.

Текущие метрики Calls пока не покрывают:

- Redis queue depth;
- Kafka consumer heartbeat;
- CPU и memory контейнеров.

Для этого нужны Redis/Kafka/container exporters или новые application metrics.

## Локальные команды

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

Открыть dashboard:

```text
http://localhost:3000/d/calls-overview/calls-overview
```

## Smoke checks

Prometheus readiness:

```bash
make prometheus-ready
```

Prometheus видит Calls:

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

Загруженные Prometheus alert rules:

```bash
make prometheus-rules
```

Обязательные Calls series:

```bash
make prometheus-smoke
```

## Troubleshooting

Если `up{job="calls"}` равен `0`, Prometheus не может прочитать Calls.
Проверьте, что `laravel.test` запущен и `/metrics` отвечает.

Если Grafana стартует, но dashboard не появился, проверьте provisioning logs:

```bash
docker compose logs grafana
```

Если alerts не видны, проверьте:

- Prometheus загрузил `calls.rules`;
- AlertManager ready;
- Prometheus достучался до `alertmanager:9093` внутри Docker.

Если gauges не меняются, проверьте:

- работает ли scheduler;
- успешно ли проходит `calls:metrics:snapshot`;
- healthy ли scrape target в Prometheus.

Если counters не растут на тестовых событиях, проверьте handlers, Redis queue
workers и Kafka consumers.

Kafka/DLQ failure counters:

```promql
sum by (source, topic, reason) (rate(kafka_consumer_dlq_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_failures_total[5m]))
sum by (source, topic, reason, result) (rate(dead_letter_records_total[5m]))
```

Если Grafana не может читать Prometheus, проверьте, что datasource URL внутри
Docker - `http://prometheus:9090`, а не `localhost:9090`.

## Не делать

- Не добавлять SQL aggregation в `/metrics`.
- Не направлять Grafana в рабочие таблицы PostgreSQL за service-метриками Calls.
- Не считать Grafana source of truth для business state.
- Не считать AlertManager механизмом repair или replay.
- Не класть production credentials в docs или Compose defaults.
