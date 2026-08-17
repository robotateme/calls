# ADR-0004: /metrics Renders Cached Metrics Only

Status: Accepted

## Context

Prometheus scrapes `/metrics` frequently. The endpoint must be cheap and
predictable.

Operational metrics need values such as:

- current calls by status;
- outbox depth by status;
- oldest pending/waiting age;
- DLQ depth.

Those values require SQL aggregation over working PostgreSQL tables.

## Decision

`/metrics` renders cached Prometheus series only.

Heavy SQL aggregation is done by scheduled command:

```bash
php artisan calls:metrics:snapshot
```

The scheduler must run this command regularly.

## Consequences

- Prometheus scrape path stays cheap.
- Heavy SQL has controlled cadence.
- Stale metrics mean scheduler/snapshot problem, not HTTP endpoint problem.
- New DB aggregation metrics belong in `calls:metrics:snapshot`, not in
  `MetricsController`.

## Alternatives

- Run SQL aggregation directly from `/metrics`.
- Emit only logs and rely on log-based metrics.
- Let Prometheus query the database.

These were rejected because they make scrape path expensive or move operational
logic to the wrong place.
