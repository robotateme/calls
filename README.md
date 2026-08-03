# Calls

Laravel-сервис, который обрабатывает входящий звонок до соединения клиента с
оператором.

Сервис без пользовательского UI. HTTP нужен только для `/metrics`.

## Что делает

- Читает входящие звонки из Kafka.
- Не создаёт дубль, если `external_call_id` уже есть.
- Ищет клиента по телефону.
- Ищет оператора и ставит короткую локальную бронь.
- Пишет команды для Telephony в `telephony_outbox`.
- Читает ответы Telephony из Kafka.
- После `connected` снимает бронь и больше не ведёт звонок.

Что не делает:

- не принимает звонки через HTTP;
- не звонит в Telephony напрямую;
- не управляет SIP и разговором после `connected`;
- не решает, доступен ли оператор после соединения.

## Документы

- [Архитектура](docs/architecture.md)
- [Решение](docs/solution.md)
- [Kafka](docs/kafka-contracts.md)
- [ADR](docs/adr/README.md)
- [Диаграммы](docs/diagrams.md)
- [Production](docs/production.md)
- [Нагрузка](docs/load-testing.md)

## Основные правила

- Главный вход - Kafka.
- `external_call_id` - ключ звонка.
- Kafka key для событий одного звонка всегда равен `external_call_id`.
- Статус `operator_dialing` используется и снаружи, и внутри Calls.
- Бронь оператора хранится в `operators.reserved_call_id` и
  `operators.reserved_at`.
- Команды для Telephony всегда идут через `telephony_outbox`.
- `/metrics` отдаёт готовые метрики и не делает тяжёлые SQL-запросы.

## Таблицы

- `calls` - звонок и его статус до `connected`;
- `clients` - поиск клиента по телефону;
- `operators` - внешняя доступность плюс локальная бронь Calls;
- `telephony_outbox` - команды для Telephony;
- `dead_letter_messages` - плохие Kafka-сообщения.

Статусы звонка:

- `new`
- `waiting`
- `assignment_requested`
- `operator_dialing`
- `connected`
- `missed`
- `callback_missed`
- `hangup_on_retry`

## Локальный запуск

Сервисы в Docker:

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
```

Проверки:

```bash
./vendor/bin/sail test
./vendor/bin/sail composer phpstan
```

## Make

Частые команды:

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
make prometheus-ready
make prometheus-targets
make prometheus-query QUERY='up{job="calls"}'
make prometheus-smoke
make kafka-consume TOPIC=incoming-calls
make load-jsonl COUNT=1000
make dead-letter-list
make dead-letter-prune
make validate
```

## Kafka

Локально:

```env
KAFKA_CONSUMER_ADAPTER=jsonl
KAFKA_PRODUCER_ADAPTER=console
```

Для `rdkafka`:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

Рабочий Docker-образ должен содержать `php-rdkafka`. Без расширения `rdkafka`-режим
падает сразу.

## Метрики

Endpoint:

```text
GET /metrics
```

Локальный Prometheus читает:

```text
http://laravel.test/metrics
```

Минимальный набор метрик Calls:

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

Дополнительно снаружи нужно следить за Kafka lag, Redis queue depth,
PostgreSQL slow queries/lock wait, outbox depth и DLQ depth.
