# Решение

Этот документ описывает, как Calls обрабатывает звонок.

## Главное

- Входящие звонки приходят из Kafka, не по HTTP.
- Один звонок определяется `external_call_id`.
- Для одного звонка Kafka key тоже равен `external_call_id`.
- Calls отвечает только до `connected`.
- Все команды для Telephony сначала пишутся в `telephony_outbox`.

Подробные договоры Kafka: [kafka-contracts.md](kafka-contracts.md).
Принятые решения: [adr/README.md](adr/README.md).

## Граница ответственности

Calls отвечает за короткий кусок процесса:

1. Получить входящий звонок.
2. Найти клиента.
3. Найти и забронировать оператора.
4. Попросить Telephony соединить клиента с оператором.
5. Принять факт, что соединение получилось или не получилось.

Calls не ведёт разговор после `connected`. Если после соединения пришёл hangup
или drop, это уже не меняет бизнес-статус звонка в Calls.

## Путь звонка

1. Kafka присылает `incoming_call_registered`.
2. Calls создаёт строку в `calls` со статусом `new`.
3. Если такой `external_call_id` уже есть, новая строка не создаётся. Это дубль.
4. Calls ставит job на обработку.
5. Обработчик ищет клиента и оператора.
6. Если оператор найден:
   - Calls ставит бронь;
   - переводит звонок в `assignment_requested`;
   - пишет `call_assignment_requested` в `telephony_outbox`.
7. Если оператора нет:
   - правило звонка решает повтор или финал;
   - Calls пишет `operator_search_retry_scheduled` или
     `operator_search_exhausted`.
8. Publisher отправляет outbox-команды в Kafka.
9. Telephony присылает факты:
   - `operator_dialing`;
   - `bridge_established`;
   - `operator_no_answer`;
   - `operator_leg_dropped`;
   - `hangup`.
10. При `connected` Calls снимает бронь и заканчивает работу по звонку.

Главная идея: смена статуса и команда для Telephony фиксируются вместе. Если БД
сохранилась, команда не потеряна. Если БД откатилась, команда не должна уйти.

## Статусы

- `new` - звонок только записали.
- `waiting` - ждём следующую попытку поиска оператора.
- `assignment_requested` - оператор уже забронирован, Telephony получила или скоро
  получит команду.
- `operator_dialing` - Telephony звонит оператору.
- `connected` - клиент и оператор соединены.
- `missed` - соединения не получилось.
- `callback_missed` - соединения не получилось, дальше нужен callback.
- `hangup_on_retry` - клиент повесил трубку, пока Calls ждал повтор.

Переходы:

- `new/waiting -> assignment_requested` - оператор найден;
- `assignment_requested -> operator_dialing` - Telephony дозванивается;
- `assignment_requested/operator_dialing -> connected` - мост установлен;
- `assignment_requested/operator_dialing -> waiting` - попытка не удалась, ещё
  можно повторить;
- `assignment_requested/operator_dialing -> missed|callback_missed|hangup_on_retry`
  - попытки закончились;
- `new/waiting/assignment_requested/operator_dialing -> missed|callback_missed|hangup_on_retry`
  - клиент повесил трубку до соединения.

`operator_dialing` - правильное имя статуса и Kafka fact. Старые варианты не
используются.

После `connected` Calls не моделирует hangup, drop и разговор.

Поздние события:

- факт по старой попытке не меняет текущую попытку;
- факт после финального статуса не открывает звонок заново;
- факт после `connected` не переводит звонок в `missed`.

## Retry

Параметры приходят во входящем сообщении:

- `operator_search_max_attempts`;
- `operator_search_retry_delay_seconds`;
- `operator_search_hangup_policy`.

Нет оператора - это обычный исход, не exception.

Redis retry может добавить min delay, jitter и cap. В Kafka-команду всё равно
пишется исходная бизнес-задержка.

Пример: входящее сообщение разрешает 3 попытки. Если на первой попытке оператора
нет, Calls ставит `waiting` и планирует повтор. Если после третьей попытки
оператора всё ещё нет, Calls ставит финальный статус из policy.

## Outbox

Команда для Telephony и смена статуса пишутся в одной транзакции БД.

Outbox-команды:

- `call_assignment_requested`;
- `call_assignment_canceled`;
- `operator_search_retry_scheduled`;
- `operator_search_exhausted`.

Publisher:

- забирает `pending`;
- ставит `processing`;
- отправляет в Kafka;
- ставит `published` или `failed`;
- старые `processing` возвращаются в `pending`.

Повторная отправка безопасна по `idempotency_key`.

Outbox нужен не для красоты. Без него возможна плохая ситуация: Calls поменял
статус в БД, но процесс умер до отправки команды в Kafka. Тогда звонок завис бы
внутри Calls, а Telephony ничего бы не знала.

## DLQ

`HandleKafkaCallFactHandler` проверяет:

- JSON;
- `type`;
- `schema_version`;
- `external_call_id`;
- Kafka key.

В DLQ идут:

- битый JSON;
- неправильный payload;
- неизвестный `type`;
- неподдержанный `schema_version`;
- mismatch Kafka key и `external_call_id`;
- ошибка handler-а.

DLQ хранится в `dead_letter_messages`. Дубли режутся по `message_hash`.

DLQ - не inbox. Inbox понадобится, если источник перестанет гарантировать
уникальность фактов.

DLQ не чинит сообщение сама. Это место, где оператор или разработчик видит:
какое сообщение пришло, почему оно не принято и что надо исправить у producer-а
или в коде.

## Где дальше читать

- Точный формат Kafka-сообщений: [kafka-contracts.md](kafka-contracts.md).
- Почему выбраны Kafka, outbox, locks, бронь и слои: [adr/README.md](adr/README.md).
- Production-процессы, метрики и rollback: [production.md](production.md).
- Нагрузочные сценарии и отчёты: [load-testing.md](load-testing.md).
