# Production

## Обязательные процессы

В production сервису нужны отдельные long-running процессы:

| Процесс | Команда | Масштабирование |
|---|---|---|
| Scheduler | `php artisan schedule:work` | 1 replica на deployment |
| Calls queue | `php artisan queue:work redis --queue=calls --tries=1 --timeout=0` | горизонтально |
| Calls retry queue | `php artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0` | горизонтально |
| Incoming calls consumer | `php artisan calls:kafka:consume incoming-calls --group=calls-incoming --source=incoming-calls-consumer --limit=1000 --timeout-ms=1000` | по partition count |
| Telephony facts consumer | `php artisan calls:kafka:consume telephony.facts --group=calls-telephony-facts --source=telephony-facts-consumer --limit=1000 --timeout-ms=1000` | по partition count |
| Outbox publisher | `php artisan calls:telephony-outbox:publish --limit=100` | можно несколько replicas, claim защищён lock/skip locked |

Scheduler уже запускает:

- `calls:telephony-outbox:publish`;
- `calls:telephony-outbox:requeue-stale`;
- `calls:operator-reservations:release-expired`;
- `calls:metrics:snapshot`;
- `calls:dead-letter:prune-resolved`.

Если outbox throughput высокий, `calls:telephony-outbox:publish` можно вынести в
отдельный горизонтально масштабируемый process, оставив scheduler как страховку.

## Kafka adapters

Local/default:

```env
KAFKA_CONSUMER_ADAPTER=jsonl
KAFKA_PRODUCER_ADAPTER=console
```

Production:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

Runtime image должен содержать PHP extension `php-rdkafka`. Если расширение не
установлено, `rdkafka` adapters падают fail-fast.

## Runtime Image

В репозитории есть production Dockerfile:

```bash
docker build -f docker/production/Dockerfile -t calls:production .
```

Он устанавливает:

- PHP 8.4 CLI;
- `pdo_pgsql`, `pcntl`, `sockets`, `opcache`;
- `redis`;
- `rdkafka`;
- `supervisor`.

Default `CMD` запускает `supervisord` с конфигом
`docker/production/supervisord.conf`.

Для Kubernetes предпочтительнее запускать один process на container и override-ить
command из таблицы выше. Supervisor-конфиг оставлен как готовый single-container
вариант для VM/Compose.

## БД и Redis

PostgreSQL:

- применять migrations до запуска workers/consumers;
- следить за lock wait, slow queries, размером `calls`, `telephony_outbox`, `dead_letter_messages`;
- для больших объёмов включить архивирование или partitioning завершённых calls/outbox.

Redis:

- использовать отдельные очереди `calls` и `calls-retry`;
- мониторить queue depth и latency;
- retry storm сглаживается jitter/cap, но при деградации операторов нужно смотреть backlog.

## DLQ

Команды:

```bash
php artisan calls:dead-letter:list
php artisan calls:dead-letter:list --reason=invalid_payload
php artisan calls:dead-letter:resolve 123 --note="fixed upstream schema"
php artisan calls:dead-letter:prune-resolved --older-than-days=30
```

DLQ рост означает сломанный contract, несовместимый deploy или ошибку upstream
producer-а. Это не нормальный backlog.

## Метрики

Application пишет counters/timings/gauges через `Metrics` port.

Обязательные внешние метрики:

- Kafka consumer lag по consumer group;
- Kafka broker produce/fetch latency;
- PostgreSQL lock wait и slow queries;
- Redis queue depth/latency;
- PHP worker restarts и memory;
- DLQ depth by reason;
- outbox pending/processing/failed depth.

## Rollout

Порядок deploy-а:

1. Migrations.
2. Новая версия app.
3. Scheduler.
4. Queue workers.
5. Kafka consumers.
6. Outbox publisher replicas.

Rollback:

- остановить consumers;
- остановить outbox publisher replicas;
- откатить app image;
- не удалять DLQ/outbox records вручную;
- проверить `telephony_outbox.failed`, `dead_letter_messages`, Kafka lag.
