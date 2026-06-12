# Calls

Laravel-сервис обработки входящих звонков до момента успешного соединения клиента и оператора.

## Исходное задание

Входящий звонок создаёт запись в таблице `calls`, после чего в очередь Redis отправляется `ProcessIncomingCallJob`.

Job должен:

- найти клиента по номеру телефона;
- выбрать доступного оператора;
- назначить звонок оператору;
- отправить событие в телефонию;
- записать лог;
- при ошибке повториться.

Система работает в production под нагрузкой. Обработка звонков выполняется несколькими воркерами параллельно.

### Фрагмент кода

```php
class ProcessIncomingCallJob implements ShouldQueue
{
   public $tries = 5;

   private $callId;

   public function __construct($callId)
   {
       $this->callId = $callId;
   }

   public function handle()
   {
       $call = Call::find($this->callId);

       if (!$call) {
           return;
       }

       if ($call->status === 'new') {
           $client = Client::where('phone', $call->phone)->first();

           if ($client) {
               $call->client_id = $client->id;
           }

           $operator = Operator::where('available', true)
               ->orderBy('last_call_at')
               ->first();

           if (!$operator) {
               throw new \Exception('No available operators');
           }

           $operator->available = false;
           $operator->save();

           $call->operator_id = $operator->id;
           $call->status = 'assigned';
           $call->save();

           // HTTP-запрос во внешнюю телефонию для назначения звонка оператору.
           // Гарантии внешней системы неизвестны.
           app(TelephonyClient::class)->sendCallAssigned($call->id, $operator->id);

           Log::info('Call assigned', [
               'call_id' => $call->id,
               'operator_id' => $operator->id,
           ]);
       }
   }
}
```

### Задачи

Найдите 7-10 проблем в решении.

Предложите варианты исправлений.

Разделите проблемы по критичности:

- Критические
- Важные
- Было бы хорошо сделать

Опишите, какие тесты вы бы добавили первыми.

Что вы бы не стали делать прямо сейчас?

Если поведение внешней системы, очереди, телефонии или legacy-кода не описано явно, укажите свои предположения. Отдельно опишите риски и опасения, которые возникают из-за этой неопределённости.

### Вопрос про масштабирование

Представьте, что через полгода нагрузка выросла в 10-50 раз: больше входящих звонков, больше операторов, больше параллельных workers, больше событий во внешнюю телефонию.

Опишите план масштабирования решения.

Нужно раскрыть:

- какие bottleneck-и вы ожидаете в текущей реализации;
- что даст простое увеличение количества workers и где оно перестанет помогать;
- какие лимиты могут возникнуть в Redis, БД, HTTP-интеграции с телефонией и логировании.

## Документация

- Архитектура и границы слоёв: [`docs/architecture.md`](docs/architecture.md)
- Разбор исходного задания и принятое решение: [`docs/solution.md`](docs/solution.md)
- Kafka contracts и объяснение выбора Kafka: [`docs/kafka-contracts.md`](docs/kafka-contracts.md)
- Диаграммы flow и state machine: [`docs/diagrams.md`](docs/diagrams.md)
- Production runbook: [`docs/production.md`](docs/production.md)
- Load testing: [`docs/load-testing.md`](docs/load-testing.md)

## Текущая реализация

Коротко:

- Входящий звонок приходит только через Kafka и маппится в `RegisterIncomingCallHandler`.
- Runtime Kafka transport ещё не подключён; mapper одного Kafka record реализован в `HandleKafkaCallFactHandler` и доступен через `calls:kafka:handle-message`.
- `calls:kafka:consume` подключён через порт `KafkaConsumer`; JSONL adapter используется для smoke/test, `rdkafka` adapter включается через env.
- Дедупликация: уникальный `external_call_id`.
- Текущее хранение: локальные shared DB таблицы `calls`, `clients`, `operators`.
- Будущая граница: клиенты и операторы за портами, позже service adapters или Kafka read models.
- CQRS-порты: read/write контракты разделены для Calls, Telephony outbox, Clients и Operator reservation.
- Repository-контракты возвращают только Domain: aggregate/model/VO или список Domain-объектов. Не DTO, не Query result, не Eloquent.
- Вход из очереди: тонкий `ProcessIncomingCallJob`.
- Поиск оператора: один handler для входящего звонка и повторных попыток.
- Бронь оператора: Calls использует `operators.reserved_call_id`, а `available/afk` считаются внешней read model.
- Машина состояний: `new`, `waiting`, `assignment_requested`, `operator_ringing`, `connected`, финальные missed-статусы.
- Правила повторов: max attempts, delay и hangup policy приходят от Telephony/Kafka.
- Финальные статусы: `missed`, `callback_missed`, `hangup_on_retry`.
- Доставка в Telephony: transactional `telephony_outbox` + `calls:telephony-outbox:publish`; доступны console и `rdkafka` producer adapters.
- DLQ: порт `DeadLetterQueue`, локальная таблица `dead_letter_messages`, команды list/resolve/prune.
- Highload-защита: составные индексы под hot queries, `SKIP LOCKED` для allocation/outbox claim на PostgreSQL/MySQL, cleanup просроченных operator reservations, jitter/backpressure для retry queue, requeue зависших `telephony_outbox.processing`, operational metrics.

Что сервис не делает:

- не принимает HTTP-входящий звонок;
- не вызывает Telephony по HTTP;
- не моделирует SIP/разговор после `connected`;
- не управляет фактической доступностью оператора, кроме локальной краткой reservation.

## Качество

Проект ориентирован на PHP 8.4, PHPStan/Larastan level 8 и feature-тесты ключевого call flow.

```bash
./vendor/bin/pint --dirty --test
composer phpstan
php artisan test
composer validate --strict --no-check-publish
```

## Local Development

Локальное окружение подготовлено на Laravel Sail:

- PostgreSQL: `pgsql:5432`
- Redis: `redis:6379`
- Kafka: `kafka:9092` внутри Docker, `localhost:9094` с хоста
- Kafka UI: `http://localhost:8081`

```bash
cp .env.example .env
./vendor/bin/sail up -d
./vendor/bin/sail artisan key:generate
./vendor/bin/sail artisan migrate
./vendor/bin/sail test
./vendor/bin/sail composer phpstan
```

Основные команды также доступны через `make`:

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

Production Kafka adapters:

```env
KAFKA_CONSUMER_ADAPTER=rdkafka
KAFKA_PRODUCER_ADAPTER=rdkafka
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PRODUCER_FLUSH_TIMEOUT_MS=10000
```

Для этих adapter-ов в runtime-образе должно быть установлено PHP-расширение
`php-rdkafka`. Если расширения нет, adapter падает fail-fast.

В production должен быть запущен Laravel scheduler:

```bash
php artisan schedule:work
```

Альтернатива для cron:

```cron
* * * * * cd /path/to/app && php artisan schedule:run >> /dev/null 2>&1
```
