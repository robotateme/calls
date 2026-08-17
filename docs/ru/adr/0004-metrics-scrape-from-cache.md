# ADR-0004: /metrics отдаёт только готовые метрики

Status: Accepted

## Проблема

Prometheus часто вызывает `/metrics`.

Если каждый вызов будет делать тяжёлые SQL-запросы, мониторинг начнёт грузить
рабочую БД.

Нельзя делать внутри `/metrics`:

- `COUNT`;
- `MIN`;
- grouping;
- поиск backlog;
- расчёт возраста строк.

## Решение

`/metrics` не ходит в рабочие таблицы PostgreSQL.

Он только читает готовые значения из `PrometheusMetricsStore`.

Тяжёлые значения считает команда:

```bash
php artisan calls:metrics:snapshot
```

Scheduler запускает её отдельно.

Что это значит на практике: Prometheus может часто читать `/metrics`, и каждый
такой запрос остаётся дешёвым. Если нужны дорогие числа из БД, их заранее считает
отдельная команда.

## Новые метрики

Если метрика требует SQL aggregation, добавляйте её в
`calls:metrics:snapshot`, не в `MetricsController`.

Правило:

- counters и timings можно писать сразу;
- gauges по БД считает snapshot;
- `/metrics` только отдаёт готовый текст.

Пример: `calls_received_total` можно увеличить прямо в обработчике входящего
звонка. А `calls_current{status}` надо считать в `calls:metrics:snapshot`,
потому что это запрос по таблице `calls`.

## Нельзя

- Делать `COUNT`, `MIN`, grouping или age queries в `/metrics`.
- Создавать нагрузку на PostgreSQL каждым scrape.
- Игнорировать старые gauges, если scheduler упал.

## Минусы

- Gauges не real-time.
- Нужно следить за scheduler.
- Если snapshot job упала, `/metrics` отдаст старые значения.

## Отклонили

- SQL прямо в `/metrics`.
- Только внешний exporter.
- Не отдавать depth/backlog gauges.
