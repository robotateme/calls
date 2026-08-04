# Архитектура

Calls разделён на слои. Бизнес-правила не должны зависеть от Laravel.

Смысл простой: правило звонка должно быть понятно без базы, очереди и HTTP.
Laravel только запускает код, читает config, пишет в БД и связывает части между
собой.

## Слои

- `src/Domain` - статусы, правила звонка, простые объекты-значения.
- `src/Application` - обработчики команд и интерфейсы к внешнему миру.
- `src/Infrastructure` - БД, Redis, Kafka, метрики.
- `app` - Laravel jobs, providers, HTTP и console wiring.

Правила:

- `Domain` не импортирует Laravel, `app`, `Application`, `Infrastructure`.
- `Application` не зависит от Laravel и конкретной БД/Kafka.
- `Infrastructure` может использовать Laravel и реализует интерфейсы
  `Application`.
- `config()` и `env()` остаются в `app` или `Infrastructure`.

Если правило можно объяснить словами бизнеса, оно не должно жить в controller,
job, Eloquent model или config-файле. Если код говорит с БД, Kafka, Redis или
Laravel, это уже не `Domain`.

Это проверяет `tests/Unit/ArchitectureBoundaryTest.php`.

## Области

- `Application\Calls` - звонок до `connected`.
- `Application\Clients` - поиск клиента по телефону.
- `Application\Operators` - локальная бронь оператора.
- `Application\Telephony` - команды в `telephony_outbox`.
- `Application\Shared` - транзакции, очередь, Kafka, DLQ, метрики.

## Репозитории

Интерфейсы репозиториев в `Application` возвращают только:

- доменный объект или объект-значение;
- `null`;
- `void`;
- список доменных объектов.

Они не возвращают Eloquent, Laravel collections, raw arrays, DTO, `stdClass` или
голые ids.

Eloquent и строки БД - только формат хранения. В доменные объекты они собираются
внутри `Infrastructure\*\Persistence\*Mapper`.

Причина: обработчик в `Application` должен работать с понятным звонком,
оператором или id, а не знать, как именно Laravel хранит строку в таблице.

## Где искать правила

- Путь звонка, статусы, retry, outbox и DLQ: [solution.md](solution.md).
- Kafka topics, payload и key: [kafka-contracts.md](kafka-contracts.md).
- Почему выбраны слои, Kafka, locks, бронь и snapshot метрик:
  [adr/README.md](adr/README.md).
- Production-процессы: [production.md](production.md).

## Кто за что отвечает

| Часть | Делает | Не делает |
|---|---|---|
| Calls | Ведёт звонок до `connected`, выбирает оператора, пишет команды Telephony | Не управляет разговором после `connected` |
| Outbox publisher | Отправляет `telephony_outbox` в Kafka | Не принимает бизнес-решения |
| Redis queue | Запускает внутренние jobs | Не заменяет Kafka |
| DLQ | Хранит плохие Kafka-сообщения | Не чинит их автоматически |
| Telephony | Звонит оператору и соединяет разговор | Не выбирает оператора |
| Operator Availability | Знает реальную доступность оператора | Не хранит статус звонка Calls |
| Clients | Даёт клиента по телефону | Не участвует в назначении |
