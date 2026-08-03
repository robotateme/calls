# ADR-0004: /metrics отдаёт только кешированный snapshot

Status: Accepted

## Проблема

Prometheus часто вызывает `/metrics`.

Если каждый scrape будет делать тяжёлые SQL-запросы в PostgreSQL, мониторинг сам
начнёт грузить рабочую БД.

Опасные запросы для scrape path:

- `COUNT`;
- `MIN`;
- grouping;
- backlog queries;
- age queries по рабочим таблицам.

При этом операционные gauge-метрики нужны:

- calls by status;
- outbox depth;
- oldest pending outbox age;
- active reservations;
- DLQ depth;
- queue depth.

## Решение

`/metrics` не ходит в рабочие PostgreSQL tables.

HTTP endpoint только читает уже готовые Prometheus series из
`PrometheusMetricsStore` и отдаёт их наружу.

Тяжёлые агрегации делает отдельная controlled job:

```bash
php artisan calls:metrics:snapshot
```

Scheduler запускает этот command отдельно от Prometheus scrape.

## Как добавлять новые метрики

Если новая метрика требует SQL aggregation, её нельзя добавлять в
`MetricsController`.

Её надо добавлять в snapshot flow:

```bash
php artisan calls:metrics:snapshot
```

Простое правило:

- runtime counters/timings можно писать сразу в metrics store;
- тяжёлые gauges по БД считаются snapshot command-ом;
- `/metrics` только рендерит готовый кеш.

## Что нельзя делать

- Нельзя выполнять `COUNT`, `MIN`, grouping или age queries внутри `/metrics`.
- Нельзя превращать Prometheus scrape в нагрузочный тест PostgreSQL.
- Нельзя считать stale gauges нормой: если scheduler умер, это отдельная
  operational problem.

## Минусы

- Gauge-метрики не real-time. Они свежие настолько, насколько часто работает
  `calls:metrics:snapshot`.
- Нужно мониторить scheduler/process health.
- При падении snapshot job `/metrics` продолжит отдавать старые значения.

## Что отклонили

- SQL aggregation прямо в `/metrics`: monitoring станет источником нагрузки на
  рабочую БД.
- Только внешний exporter: возможно позже, но сейчас в сервисе уже есть snapshot
  command и Prometheus renderer.
- Не отдавать backlog/depth gauges: без них хуже видно outbox/DLQ, retry storm и
  stuck reservations.
