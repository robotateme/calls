# Load Testing

## Quick Local Check

JSONL mode checks the consumer without a real Kafka client:

```bash
php tools/load/generate-incoming-calls-jsonl.php 1000 local \
  | php artisan calls:kafka:consume incoming-calls --limit=1000 --timeout-ms=5000
```

After the run:

```bash
php artisan calls:metrics:snapshot
php artisan calls:dead-letter:list --limit=20
```

A successful quick run means messages were read, jobs processed, outbox did not
stall, and DLQ did not grow. It does not prove that real Kafka is configured,
because JSONL bypasses the broker.

## Profiles

| Profile | What It Runs | Purpose |
|---|---|---|
| `smoke` | 100 calls, 1 worker, 1 outbox publisher, 150 operators | quick end-to-end check |
| `stress-large` | 250k calls, 16 workers, 4 publishers, 300k operators | find machine limit |
| `soak-large` | 3 hours, 50 rps, 8 workers, 2 publishers, 750k operators | long-running stability |
| `custom` | env variables | manual tuning |

Run:

```bash
LOAD_PROFILE=smoke bash tools/load/run-jsonl-load.sh
LOAD_PROFILE=stress-large bash tools/load/run-jsonl-load.sh
LOAD_PROFILE=soak-large bash tools/load/run-jsonl-load.sh
```

Via Sail:

```bash
make load-smoke
make load-stress-large
make load-soak-large
```

Custom:

```bash
LOAD_PROFILE=custom \
LOAD_MODE=stress \
LOAD_COUNT=10000 \
LOAD_WORKERS=4 \
LOAD_RETRY_WORKERS=1 \
LOAD_OUTBOX_PUBLISHERS=1 \
LOAD_FAKE_TELEPHONY_PRODUCER=1 \
bash tools/load/run-jsonl-load.sh
```

Custom via Sail:

```bash
./vendor/bin/sail bash -lc 'LOAD_PROFILE=custom LOAD_MODE=stress LOAD_COUNT=10000 bash tools/load/run-jsonl-load.sh'
```

Time-limited run:

```bash
LOAD_MODE=soak \
LOAD_DURATION_SECONDS=900 \
LOAD_RATE_PER_SECOND=100 \
bash tools/load/run-jsonl-load.sh
```

## Script Behavior

- Prepares operators through `tools/load/prepare-dataset.php`.
- Starts `calls` and `calls-retry` workers.
- Starts fake Telephony outbox publisher.
- Generates JSONL and feeds `calls:kafka:consume`.
- Writes reports to `storage/load-reports/<prefix>`.
- For large runs, writes `snapshot-*.json` and `progress.env`.
- Fails if queue/outbox backlog or unresolved DLQ remains.

If the script fails, start with `storage/load-reports/<prefix>`. Usually the
important parts are remaining jobs, remaining outbox messages, and DLQ records.

## GitHub Actions

There are two GitHub Actions workflows:

- `CI` - Composer validate, Pint, PHPStan, PHPUnit.
- `Load Test` - manual `smoke`, `stress-large`, `soak-large`, `custom`.

`Load Test` uses PostgreSQL and Redis service containers, JSONL consumer, and
fake Telephony producer. It checks code but does not replace a run through real
Kafka.

Use a self-hosted runner for long `soak-large` runs.

## Production Load

Production validation needs a real Kafka producer or replay from a staging
topic.

Scenarios:

| Scenario | Watch |
|---|---|
| Many inbound calls | Kafka lag, insert latency, Redis queue depth |
| No operators | retry queue, outbox retry commands |
| Many `operator_no_answer` | reservation release, retry/final |
| `hangup` before connection | assignment cancel, reservation release |
| Late facts after `connected` | no-op |
| Telephony lag | old reservations, outbox cancel |
| Bad `schema_version` | DLQ |

Minimum report:

- p50/p95/p99 latency;
- Kafka consumer lag;
- Redis queue depth;
- PostgreSQL slow queries/lock wait;
- `telephony_outbox_current{status}`;
- `dead_letter_current`;
- worker CPU/memory;
- log error rate.

How to read the result:

- growing Kafka lag - consumers cannot read fast enough;
- growing Redis queue depth - workers cannot process calls fast enough;
- growing `telephony_outbox_current{status="pending"}` - publishers cannot send
  commands fast enough;
- growing `dead_letter_current` - bad messages arrive or handler fails;
- growing `oldest_waiting_call_age_seconds` - calls wait too long for operator.

## Staging Go-Live Validation

Before production traffic, staging must pass checks that use real Kafka, not
only JSONL:

- successful incoming call path until `connected`;
- duplicate `external_call_id` handling;
- unavailable operator retry/final outcome;
- timeout or no-answer fact handling;
- alert firing for Kafka lag, DLQ growth, queue depth, and target down;
- graceful shutdown while Kafka consumer or outbox publisher is processing;
- `calls:dead-letter:replay --dry-run` and one replay with test data;
- external `/metrics` access denied from outside the monitoring network.

Keep the report with deployment notes. Production go-live should not depend on
local Compose exporters; staging must use the same monitoring path as the target
environment or an equivalent managed stack.
