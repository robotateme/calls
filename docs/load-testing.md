# Load Testing

## Local JSONL Smoke

JSONL smoke проверяет consumer boundary без настоящего Kafka client:

```bash
php tools/load/generate-incoming-calls-jsonl.php 1000 local \
  | php artisan calls:kafka:consume incoming-calls --limit=1000 --timeout-ms=5000
```

После прогона проверить:

```bash
php artisan calls:metrics:snapshot
php artisan calls:dead-letter:list --limit=20
```

## Local Stress/Soak Runner

Повторяемый runner для CI и локальных проверок поддерживает большие профили:

| Профиль | Параметры по умолчанию | Назначение |
|---|---|---|
| `smoke` | 100 incoming calls, 1 `calls` worker, 1 outbox publisher, 150 operators | Быстро проверить сам runner, queue/outbox drain и отчетность |
| `stress-large` | 250k incoming calls, no producer throttle, 16 `calls` workers, 4 outbox publishers, 300k operators | Найти throughput ceiling, DB/Redis/outbox bottleneck и backlog under burst |
| `soak-large` | 3 часа, 50 rps, 8 `calls` workers, 2 outbox publishers, 750k operators | Проверить стабильность, утечки, рост latency/backlog/DLQ на длинном прогоне |
| `custom` | env-переменные ниже | Ручная настройка под машину или эксперимент |

Smoke-проверка runner'а:

```bash
LOAD_PROFILE=smoke bash tools/load/run-jsonl-load.sh
```

Большой stress локально:

```bash
LOAD_PROFILE=stress-large bash tools/load/run-jsonl-load.sh
```

Большой soak локально:

```bash
LOAD_PROFILE=soak-large bash tools/load/run-jsonl-load.sh
```

Через Sail:

```bash
make load-smoke
make load-stress-large
make load-soak-large
```

Custom-пример:

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

Если нужен custom-запуск через Sail, переменные нужно передавать внутрь контейнера:

```bash
./vendor/bin/sail bash -lc 'LOAD_PROFILE=custom LOAD_MODE=stress LOAD_COUNT=10000 bash tools/load/run-jsonl-load.sh'
```

Soak-профиль ограничивается временем:

```bash
LOAD_MODE=soak \
LOAD_DURATION_SECONDS=900 \
LOAD_RATE_PER_SECOND=100 \
bash tools/load/run-jsonl-load.sh
```

Runner:

- подготавливает load dataset с операторами через `tools/load/prepare-dataset.php`;
- запускает workers для `calls`, `calls-retry` и fake Telephony outbox publisher;
- генерирует JSONL batches и отдаёт их в `calls:kafka:consume`;
- пишет отчёты в `storage/load-reports/<prefix>`;
- во время больших профилей пишет периодические `snapshot-*.json` и `progress.env`;
- завершает прогон ошибкой, если остался queue/outbox backlog или появились unresolved DLQ records.

## GitHub Actions

В CI есть два workflow:

- `CI` — быстрый validation на PR/push: Composer validate, Pint, PHPStan, PHPUnit, frontend build.
- `Load Test` — ручной `workflow_dispatch` для `smoke`, `stress-large`, `soak-large` и `custom`, без деплоя.

`Load Test` использует PostgreSQL и Redis service containers, JSONL consumer adapter и fake Telephony producer. Это не заменяет production Kafka replay, но стабильно проверяет consumer/queue/PostgreSQL/outbox контур в GitHub runner.

Для длительного `soak-large` лучше выбирать `self-hosted` runner, если нужен прогон дольше лимитов GitHub-hosted окружения или более мощная машина. В workflow можно менять `timeout_minutes`, batch size, workers, outbox publishers и custom rate.

## Production Kafka Load

Для production-нагрузки нужен реальный Kafka producer или replay из staging topic.

Обязательные сценарии:

| Сценарий | Что проверяем |
|---|---|
| Массовые входящие звонки | consumer lag, insert latency, Redis queue depth |
| Нет доступных операторов | retry storm, `calls-retry`, outbox retry scheduled |
| Массовый `operator_no_answer` | release reservation, retry/final policy |
| Массовый `hangup` до соединения | cancel assignment, release reservation |
| Поздние facts после `connected` | no-op state machine |
| Telephony lag | stale reservation cleanup, outbox cancel flow |
| Некорректная schema version | DLQ depth and alerting |

Минимальные метрики для отчёта:

- p50/p95/p99 processing latency;
- Kafka consumer lag;
- Redis queue depth;
- PostgreSQL lock wait;
- `telephony_outbox.depth` by status;
- `dead_letter.depth` by reason;
- CPU/memory per worker;
- error rate по logs.
