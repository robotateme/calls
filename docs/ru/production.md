# Production

## Процессы

В production должны быть запущены отдельные процессы:

| Процесс | Команда | Масштабирование |
|---|---|---|
| Scheduler | `php artisan schedule:work` | 1 на deployment |
| Calls queue | `php artisan queue:work redis --queue=calls --tries=1 --timeout=0` | можно несколько |
| Calls retry queue | `php artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0` | можно несколько |
| Incoming calls consumer | `php artisan calls:kafka:consume incoming-calls --group=calls-incoming --source=incoming-calls-consumer --limit=1000 --timeout-ms=1000` | по числу partitions |
| Telephony facts consumer | `php artisan calls:kafka:consume telephony.facts --group=calls-telephony-facts --source=telephony-facts-consumer --limit=1000 --timeout-ms=1000` | по числу partitions |
| Outbox publisher | `php artisan calls:telephony-outbox:publish --limit=100` | можно несколько |

Scheduler запускает:

- `calls:telephony-outbox:publish`;
- `calls:telephony-outbox:requeue-stale`;
- `calls:operator-reservations:release-expired`;
- `calls:metrics:snapshot`;
- `calls:dead-letter:prune-resolved`.

Если outbox растёт быстрее, чем scheduler успевает публиковать, запускайте
несколько отдельных outbox publishers.

Важно: одного HTTP-процесса недостаточно. Без scheduler не обновятся метрики и не
почистятся старые брони. Без queue workers звонки не будут обрабатываться. Без
Kafka consumers Calls не увидит новые звонки и факты Telephony. Без outbox
publisher команды останутся в БД и не уйдут в Telephony.

## Kafka

Локально:

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

Для `rdkafka` нужен PHP extension `php-rdkafka`. Если его нет, сервис должен
упасть при старте Kafka-адаптера.

## Docker-образ

Собрать:

```bash
docker build -f docker/production/Dockerfile -t calls:production .
```

В образе есть:

- PHP 8.4 CLI;
- `pdo_pgsql`, `pcntl`, `sockets`, `opcache`;
- `redis`;
- `rdkafka`;
- `supervisor`.

По умолчанию запускается `docker/production/supervisord.conf`.

В Kubernetes лучше запускать один процесс на контейнер и задавать команду из
таблицы выше.

## БД и Redis

PostgreSQL:

- применить migrations до workers/consumers;
- следить за slow queries и lock wait;
- следить за размером `calls`, `telephony_outbox`, `dead_letter_messages`;
- для больших объёмов архивировать завершённые звонки и published outbox.

Redis:

- очереди `calls` и `calls-retry` должны быть раздельными;
- следить за queue depth и latency;
- при проблемах с операторами смотреть рост retry queue.

## DLQ

Команды:

```bash
php artisan calls:dead-letter:list
php artisan calls:dead-letter:list --reason=invalid_payload
php artisan calls:dead-letter:resolve 123 --note="fixed upstream schema"
php artisan calls:dead-letter:prune-resolved --older-than-days=30
```

Рост DLQ - это проблема сообщения, deploy-а или upstream producer-а. Это не
нормальная очередь задач.

Если DLQ растёт:

1. Сначала смотрите новые `reason`.
2. Потом смотрите пример payload.
3. Потом проверяйте последний deploy и producer-а.
4. Не удаляйте записи вручную, пока не понятно, что произошло.

## Метрики

Endpoint:

```text
GET /metrics
```

`/metrics` только отдаёт готовые значения. Он не делает `COUNT`, `MIN` и
grouping по PostgreSQL. Такие запросы делает:

```bash
php artisan calls:metrics:snapshot
```

См. [ADR-0004](adr/0004-metrics-scrape-from-cache.md).

Метрики Calls:

- `calls_received_total`;
- `calls_deduplicated_total`;
- `call_transitions_total{from,to}`;
- `operator_reservation_attempts_total{result}`;
- `telephony_outbox_publish_total{result}`;
- `dead_letter_messages_total{reason}`;
- `dead_letter_records_total{source,topic,reason,result}`;
- `kafka_consumer_dlq_total{source,topic,reason}`;
- `kafka_consumer_failures_total{source,topic,reason}`;
- `calls_current{status}`;
- `operators_reserved_current`;
- `telephony_outbox_current{status}`;
- `dead_letter_current`;
- `oldest_waiting_call_age_seconds`;
- `oldest_outbox_message_age_seconds`.

Снаружи также нужны:

- Kafka consumer lag;
- Kafka produce/fetch latency;
- PostgreSQL slow queries и lock wait;
- Redis queue depth/latency;
- PHP worker restarts и memory;
- DLQ depth by reason;
- outbox depth by status.

PromQL smoke после deploy-а:

```promql
up{job="calls"}
rate(calls_received_total[5m])
rate(calls_deduplicated_total[5m])
sum by (from, to) (rate(call_transitions_total[5m]))
sum by (result) (rate(operator_reservation_attempts_total[5m]))
sum by (result) (rate(telephony_outbox_publish_total[5m]))
sum by (reason) (rate(dead_letter_messages_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_dlq_total[5m]))
sum by (source, topic, reason) (rate(kafka_consumer_failures_total[5m]))
sum by (status) (calls_current)
operators_reserved_current
sum by (status) (telephony_outbox_current)
dead_letter_current
oldest_waiting_call_age_seconds
oldest_outbox_message_age_seconds
```

Grafana локально настроена как dashboard-слой поверх Prometheus:

- локальный URL: `http://localhost:3000`;
- локальный логин по умолчанию: `admin` / `admin`;
- datasource: `Prometheus` -> `http://prometheus:9090`;
- dashboard: `Calls Overview`.

Grafana не должна ходить напрямую в рабочие таблицы PostgreSQL за этими
service-метриками. Значения метрик Calls по-прежнему пишут handlers и
`calls:metrics:snapshot`, Prometheus читает их из `/metrics`, а Grafana читает
их из Prometheus.

Локальный Grafana dashboard также включает внешние Prometheus series:

- Kafka consumer group lag из `kafka-exporter`;
- Redis queue depth из Calls snapshot metric `queue_depth`;
- Redis memory из `redis-exporter`;
- container CPU и memory из cAdvisor.

Production должен отдавать эквивалентные Kafka, Redis и container metrics через
monitoring stack окружения. Локальные Compose exporters - development baseline,
а не требование к production deployment.

Подробная настройка локального observability:
[observability.md](observability.md).

Если `up{job="calls"}` равен 0, Prometheus не видит `/metrics`. Если gauges не
меняются, проверьте `calls:metrics:snapshot` и scheduler. Если counters не
растут при тестовых событиях, проверьте обработчики и Redis queue.

## Alerting

Локальный Prometheus отправляет alerts в AlertManager:

```text
http://alertmanager:9093
```

Локальный UI AlertManager:

```text
http://localhost:9093
```

Alert rules лежат здесь:

```text
docker/prometheus/rules/calls-alerts.yml
```

Текущие rules покрывают:

- отказ scrape `/metrics`;
- отсутствие snapshot gauges;
- unresolved или растущий DLQ;
- Kafka-сообщения, отправленные в DLQ, по source/topic/reason;
- отказы Kafka consumer по source/topic/reason;
- failed outbox records;
- высокий pending outbox backlog;
- старые pending outbox records;
- звонки, которые слишком долго ждут оператора.

Текущие rules не покрывают Kafka consumer heartbeat, Kafka broker produce/fetch
latency и Redis/container resource thresholds. Для этих сигналов нужны
exporter-backed rules или managed monitoring alerts.

Локальный receiver AlertManager намеренно no-op. В production alerts надо
направить в реальный incident channel окружения.

## Deploy

Порядок:

1. Migrations.
2. App image.
3. Scheduler.
4. Queue workers.
5. Kafka consumers.
6. Outbox publishers.

Откат:

- остановить consumers;
- остановить outbox publishers;
- откатить app image;
- не удалять outbox/DLQ вручную;
- проверить `telephony_outbox.failed`, `dead_letter_messages`, Kafka lag.

Зачем такой порядок: сначала останавливаем вход и отправку наружу, чтобы не
продолжать писать новые команды старым или сломанным кодом. Потом откатываем
образ и проверяем, какие сообщения остались незавершёнными.
