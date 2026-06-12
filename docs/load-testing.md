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
