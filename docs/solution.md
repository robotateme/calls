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

## Что уже есть

- Регистрация звонка из Kafka.
- Дедупликация по `external_call_id`.
- Очередь `ProcessIncomingCallJob`.
- Обработчик `ProcessIncomingCallHandler`.
- Правила звонка в `Domain\Calls\Call`.
- Локальная бронь оператора.
- `telephony_outbox`.
- JSONL consumer для локальных проверок.
- `rdkafka` consumer/producer для production.
- DLQ в `dead_letter_messages`.
- Cleanup-команды для outbox, броней и DLQ.
- `/metrics` и snapshot метрик.

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

## Что было исправлено

- Два workers могли выбрать одного оператора.
- БД менялась отдельно от внешнего действия.
- Retry мог повторить внешнее действие.
- `assigned` выглядел как финал без подтверждения Telephony.
- Отсутствие оператора было exception.
- Доступность оператора и бронь были смешаны.

## Тесты

Покрыто:

- регистрация и дубль входящего звонка;
- выбор оператора;
- отсутствие оператора и retry;
- финал после исчерпания попыток;
- AFK и уже забронированные операторы;
- поздние jobs после финала;
- факты Telephony;
- cancel assignment;
- requeue зависшего outbox;
- снятие старых броней;
- DLQ list/resolve/prune;
- snapshot метрик;
- границы слоёв.

## Риски

- Если `external_call_id` нестабилен, дедупликация и порядок ломаются.
- Если Kafka key не равен `external_call_id`, порядок одного звонка ломается.
- Если facts начнут приходить дублями без гарантии уникальности, нужен inbox.
- Рост DLQ значит проблему контракта, deploy-а или upstream producer-а.
- Если Calls начнёт менять статус после `connected`, он выйдет за свою зону
  ответственности.

## Нагрузка

Узкие места:

- locks в БД при выборе оператора;
- рост `calls` и `telephony_outbox`;
- много delayed jobs в Redis;
- Kafka lag;
- рост DLQ;
- синхронные логи.

Уже сделано:

- индексы под рабочие запросы;
- `FOR UPDATE SKIP LOCKED` там, где workers забирают batch;
- row lock при брони оператора;
- jitter для `calls-retry`;
- requeue зависшего outbox;
- cleanup старых броней;
- snapshot метрик.

Дальше:

1. Проверить Kafka partitions по `external_call_id` на реальной нагрузке.
2. Добавить inbox, если facts перестанут быть уникальными.
3. Вынести clients/operators в отдельные таблицы чтения или отдельный
   routing-service.
4. Архивировать завершённые `calls` и `published` outbox.
5. Прогнать сценарии: нет операторов, массовый no-answer, hangup до соединения,
   поздние facts после `connected`, lag Telephony.
