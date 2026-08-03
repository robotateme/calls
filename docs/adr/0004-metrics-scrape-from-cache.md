# ADR-0004: /metrics отдаёт кешированный snapshot

Status: Accepted

## Context

Prometheus часто scrapes `/metrics`. Если каждый scrape выполняет `COUNT`,
`MIN`, grouping, backlog и age queries по рабочим PostgreSQL tables, мониторинг
сам создаст нагрузку на БД и может ухудшить hot path обработки звонков.

При этом операционные gauges всё равно нужны:

- calls by status;
- outbox depth;
- oldest pending outbox age;
- active reservations;
- DLQ depth;
- queue depth.

## Decision

HTTP endpoint `/metrics` не ходит в рабочие PostgreSQL tables. Он только
рендерит cached Prometheus series из `PrometheusMetricsStore`.

Тяжёлые агрегации выполняет controlled job:

```bash
php artisan calls:metrics:snapshot
```

Scheduler запускает snapshot отдельно от scrape path. Если новая метрика требует
`COUNT`, `MIN`, grouping или вычисления возраста records, она добавляется в
snapshot flow, а не в controller `/metrics`.

## Consequences

- Частота Prometheus scrape не масштабирует нагрузку на PostgreSQL.
- Свежесть gauge-метрик зависит от частоты `calls:metrics:snapshot`.
- При падении scheduler gauges могут устареть, поэтому надо мониторить сам
  scheduler/process health.
- Counters/timings пишутся runtime-кодом в metrics store и доступны без
  PostgreSQL aggregation на scrape.

## Alternatives

- Делать SQL aggregation прямо в `/metrics`. Отклонено: monitoring становится
  источником нагрузки на рабочую БД.
- Собирать все gauges только внешним exporter-ом. Возможный вариант позже, но
  текущий сервис уже имеет нужный snapshot command и Prometheus renderer.
- Не отдавать backlog/depth gauges. Отклонено: без них хуже видны outbox/DLQ,
  retry storm и stuck reservations.
