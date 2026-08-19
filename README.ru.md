# Calls

[English version](README.md)

Laravel-сервис, который обрабатывает входящий звонок до соединения клиента с
оператором.

Сервис без пользовательского UI. HTTP нужен только для `/metrics`.

Если коротко: Calls берёт факт входящего звонка, пытается найти оператора,
просит Telephony соединить клиента с оператором и ждёт ответ от Telephony. Как
только клиент и оператор соединены, Calls заканчивает свою часть работы.

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

## Как читать проект

Если нужно понять поведение звонка, читайте в таком порядке:

1. [Решение](docs/ru/solution.md) - весь путь звонка простым текстом.
2. [Kafka](docs/ru/kafka-contracts.md) - какие сообщения приходят и уходят.
3. [Архитектура](docs/ru/architecture.md) - где лежит код и что нельзя смешивать.
4. [Domain Model](docs/ru/domain-model.md) - что такое `Call` aggregate root и как
   mapper-ы восстанавливают доменную модель из БД.
5. [ADR](docs/ru/adr/README.md) - почему выбраны Kafka, слои, бронь, locks и
   snapshot метрик.
6. [Observability](docs/ru/observability.md) - Prometheus, Grafana, поток метрик
   и smoke checks.
7. [Production](docs/ru/production.md) - какие процессы должны работать постоянно.

## Документы

- [Архитектура](docs/ru/architecture.md)
- [Domain Model и Infrastructure Mapping](docs/ru/domain-model.md)
- [Решение](docs/ru/solution.md)
- [Kafka](docs/ru/kafka-contracts.md)
- [ADR](docs/ru/adr/README.md)
- [Диаграммы](docs/ru/diagrams.md)
- [Observability](docs/ru/observability.md)
- [Production](docs/ru/production.md)
- [Нагрузка](docs/ru/load-testing.md)

## Основные правила

- Главный вход - Kafka.
- `external_call_id` - главный id звонка. По нему Calls понимает, что это тот же
  самый звонок, а не новый.
- Kafka key для событий одного звонка всегда равен `external_call_id`.
- Статус `operator_dialing` используется и снаружи, и внутри Calls.
- Бронь оператора хранится в `operators.reserved_call_id` и
  `operators.reserved_at`.
- Команды для Telephony всегда идут через `telephony_outbox`.
- `/metrics` отдаёт готовые метрики и не делает тяжёлые SQL-запросы.
- Плохие Kafka-сообщения не применяются к звонку. Они пишутся в
  `dead_letter_messages`.
- После `connected` Calls не откатывает звонок назад по поздним фактам.

## Таблицы

- `calls` - звонок и его статус до `connected`;
- `clients` - поиск клиента по телефону;
- `operators` - внешняя доступность плюс локальная бронь Calls;
- `telephony_outbox` - команды для Telephony;
- `dead_letter_messages` - плохие Kafka-сообщения.

Статусы и переходы описаны в [docs/ru/solution.md](docs/ru/solution.md).

## Локальный запуск

Сервисы в Docker:

- PostgreSQL: `pgsql:5432`
- Redis: `redis:6379`
- Kafka: `kafka:9092` внутри Docker, `localhost:9094` с хоста
- Kafka UI: `http://localhost:8081`
- Prometheus: `http://localhost:9090`
- AlertManager: `http://localhost:9093`
- Grafana: `http://localhost:3000`

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

PostgreSQL integration tests запускаются обычным `php artisan test`, если
доступна БД из `PG_INTEGRATION_*`. В CI PostgreSQL поднимается отдельным service.
Локально можно поднять `pgsql` через Sail и указать тестовую БД:

```bash
./vendor/bin/sail up -d pgsql
PG_INTEGRATION_HOST=127.0.0.1 PG_INTEGRATION_DATABASE=testing php artisan test
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
make prometheus-rules
make prometheus-alerts
make prometheus-smoke
make alertmanager
make alertmanager-ready
make alertmanager-alerts
make grafana
make grafana-ready
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

## Метрики, Prometheus, AlertManager и Grafana

Endpoint:

```text
GET /metrics
```

Локальный Prometheus читает:

```text
http://laravel.test/metrics
```

Локальная Grafana открывается здесь:

```text
http://localhost:3000
```

Логин по умолчанию для локального запуска: `admin` / `admin`. Grafana уже
получает Prometheus datasource и dashboard `Calls Overview`.

Локальный AlertManager открывается здесь:

```text
http://localhost:9093
```

Проверить обязательные series:

```bash
make prometheus-smoke
```

Полный observability flow, Grafana provisioning, список метрик и PromQL smoke
queries: [docs/ru/observability.md](docs/ru/observability.md).
