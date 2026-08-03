# Calls

Laravel-сервис обработки входящих звонков до успешного соединения клиента и
оператора.

Проект намеренно backend-only: нет пользовательского frontend-а, HTML/UI surface,
Vite/Tailwind toolchain и браузерных assets. HTTP surface ограничен служебным
endpoint-ом `/metrics`.

## Что делает сервис

- Принимает входящие call facts из Kafka.
- Дедуплицирует звонки по `external_call_id`.
- Ищет клиента по номеру телефона.
- Выбирает и кратко резервирует доступного оператора.
- Пишет команды в Telephony через transactional outbox.
- Применяет Kafka facts от Telephony и двигает state machine.
- После `connected` освобождает локальную reservation и завершает свою
  ответственность.

Что Calls не делает:

- не принимает входящий звонок через HTTP;
- не вызывает Telephony напрямую по HTTP;
- не управляет SIP и разговором после `connected`;
- не владеет фактической доступностью оператора, кроме локальной reservation.

## Ключевые документы

- [Архитектура](docs/architecture.md) - слои, границы и ownership.
- [Решение](docs/solution.md) - call flow, state machine, retry/outbox/DLQ.
- [Kafka contracts](docs/kafka-contracts.md) - topics, payloads, idempotency.
- [ADR](docs/adr/README.md) - принятые архитектурные решения.
- [Диаграммы](docs/diagrams.md) - PlantUML/PNG flow diagrams.
- [Production](docs/production.md) - процессы, rollout, rollback.
- [Load testing](docs/load-testing.md) - smoke/stress/soak проверки.

## Текущая модель

Authoritative ingress - Kafka. `external_call_id` является стабильным business
key звонка и Kafka message key для всех событий одного call.

Локальные таблицы:

- `calls` - state machine звонка до `connected`;
- `clients` - текущий shared DB lookup клиента;
- `operators` - текущая read model доступности и локальная reservation;
- `telephony_outbox` - исходящие команды в Telephony;
- `dead_letter_messages` - poison Kafka records.

Внутренние статусы call:

- `new`
- `waiting`
- `assignment_requested`
- `operator_dialing`
- `connected`
- `missed`
- `callback_missed`
- `hangup_on_retry`

Внешний Kafka fact `operator_dialing` остается контрактом Telephony и внутри
Calls маппится в статус `operator_dialing`.

## Локальный запуск

Окружение подготовлено на Laravel Sail:

- PostgreSQL: `pgsql:5432`
- Redis: `redis:6379`
- Kafka: `kafka:9092` внутри Docker, `localhost:9094` с хоста
- Kafka UI: `http://localhost:8081`
- Prometheus: `http://localhost:9090`

```bash
cp .env.example .env
./vendor/bin/sail up -d
./vendor/bin/sail artisan key:generate
./vendor/bin/sail artisan migrate
./vendor/bin/sail test
./vendor/bin/sail composer phpstan
```

Основные команды доступны через `make`:

```bash
make up
make migrate
make queue
make queue-retry
make schedule
make outbox-publish
make outbox-requeue-stale
make release-expired-reservations
make metrics-snapshot
make kafka-consume TOPIC=incoming-calls
make load-jsonl COUNT=1000
make dead-letter-list
make dead-letter-prune
make validate
```

## Проверки

```bash
./vendor/bin/pint --dirty --test
composer phpstan
php artisan test
composer validate --strict --no-check-publish
```

## Kafka adapters

Локально по умолчанию используются JSONL consumer и console producer.

Production adapters включаются через env:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

Для `rdkafka` adapter-ов в runtime-образе нужно PHP-расширение
`php-rdkafka`. Если расширения нет, adapter падает fail-fast.

## Metrics

Prometheus endpoint:

```text
GET /metrics
```

Локальный Docker Compose поднимает Prometheus и scrapes Calls внутри сети Docker:

```text
http://laravel.test/metrics
```

UI Prometheus доступен на `http://localhost:9090`.

Базовый application contract:

- `calls_received_total`
- `calls_deduplicated_total`
- `call_transitions_total{from,to}`
- `operator_reservation_attempts_total{result}`
- `telephony_outbox_publish_total{result}`
- `dead_letter_messages_total{reason}`
- `calls_current{status}`
- `operators_reserved_current`
- `telephony_outbox_current{status}`
- `dead_letter_current`
- `oldest_waiting_call_age_seconds`
- `oldest_outbox_message_age_seconds`

В production также должны мониториться Kafka lag, Redis queue depth, PostgreSQL
lock wait/slow queries, outbox depth и DLQ depth.
