# Нагрузка

## Быстрая локальная проверка

JSONL-режим проверяет consumer без настоящего Kafka client:

```bash
php tools/load/generate-incoming-calls-jsonl.php 1000 local \
  | php artisan calls:kafka:consume incoming-calls --limit=1000 --timeout-ms=5000
```

После прогона:

```bash
php artisan calls:metrics:snapshot
php artisan calls:dead-letter:list --limit=20
```

## Готовые профили

| Профиль | Что запускает | Для чего |
|---|---|---|
| `smoke` | 100 звонков, 1 worker, 1 outbox publisher, 150 операторов | быстро проверить весь путь |
| `stress-large` | 250k звонков, 16 workers, 4 publishers, 300k операторов | найти предел машины |
| `soak-large` | 3 часа, 50 rps, 8 workers, 2 publishers, 750k операторов | проверить долгую работу |
| `custom` | env-переменные | ручная настройка |

Запуск:

```bash
LOAD_PROFILE=smoke bash tools/load/run-jsonl-load.sh
LOAD_PROFILE=stress-large bash tools/load/run-jsonl-load.sh
LOAD_PROFILE=soak-large bash tools/load/run-jsonl-load.sh
```

Через Sail:

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

Custom через Sail:

```bash
./vendor/bin/sail bash -lc 'LOAD_PROFILE=custom LOAD_MODE=stress LOAD_COUNT=10000 bash tools/load/run-jsonl-load.sh'
```

Ограничение по времени:

```bash
LOAD_MODE=soak \
LOAD_DURATION_SECONDS=900 \
LOAD_RATE_PER_SECOND=100 \
bash tools/load/run-jsonl-load.sh
```

## Что делает скрипт

- Готовит операторов через `tools/load/prepare-dataset.php`.
- Запускает workers `calls` и `calls-retry`.
- Запускает fake Telephony outbox publisher.
- Генерирует JSONL и отдаёт в `calls:kafka:consume`.
- Пишет отчёты в `storage/load-reports/<prefix>`.
- Для больших прогонов пишет `snapshot-*.json` и `progress.env`.
- Завершается с ошибкой, если остались queue/outbox backlog или unresolved DLQ.

## GitHub Actions

Есть две GitHub Actions проверки:

- `CI` - Composer validate, Pint, PHPStan, PHPUnit.
- `Load Test` - ручной запуск `smoke`, `stress-large`, `soak-large`, `custom`.

`Load Test` использует PostgreSQL и Redis service containers, JSONL consumer и
fake Telephony producer. Это проверяет код, но не заменяет повторную обработку
через реальную Kafka.

Для длинного `soak-large` лучше self-hosted runner.

## Production-нагрузка

Для production нужен настоящий Kafka producer или повторная обработка staging
topic.

Сценарии:

| Сценарий | Что смотреть |
|---|---|
| Много входящих звонков | Kafka lag, insert latency, Redis queue depth |
| Нет операторов | retry queue, outbox retry commands |
| Много `operator_no_answer` | снятие брони, retry/final |
| `hangup` до соединения | cancel assignment, снятие брони |
| Поздние facts после `connected` | no-op |
| Telephony lag | старые брони, outbox cancel |
| Битый `schema_version` | DLQ |

Минимум для отчёта:

- p50/p95/p99 latency;
- Kafka consumer lag;
- Redis queue depth;
- PostgreSQL slow queries/lock wait;
- `telephony_outbox_current{status}`;
- `dead_letter_current`;
- CPU/memory workers;
- error rate в логах.
