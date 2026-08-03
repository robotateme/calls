# ADR-0002: Workers забирают строки через row locks и SKIP LOCKED

Status: Accepted

## Проблема

Calls работает параллельно:

- queue workers выбирают операторов;
- outbox publishers забирают pending commands;
- recovery jobs возвращают stale records и снимают expired reservations.

Несколько процессов могут одновременно смотреть в одни и те же таблицы:

- `operators`;
- `telephony_outbox`;
- calls/retry выборки.

Без lock один record могут забрать два workers. Без `SKIP LOCKED` workers могут
стоять друг за другом и ждать lock, хотя рядом есть другие готовые строки.

## Решение

Там, где worker забирает работу из общей таблицы, используется row lock.

Для PostgreSQL/MySQL используем:

```sql
FOR UPDATE SKIP LOCKED
```

Это правило действует для:

- выборки batch calls для recovery/retry;
- claim pending/due records в `telephony_outbox`;
- requeue stale outbox records;
- выбора оператора внутри transaction.

Для SQLite в тестах остаётся обычный Laravel lock fallback.

## Как должен работать worker

Worker делает так:

- берёт маленький batch;
- лочит выбранные строки;
- сразу помечает их как claimed/processing или ставит бронь;
- коммитит transaction;
- дальше работает только со своими claimed records.

Главное правило: сначала явно забрать строку себе, потом делать side effect.

## Зачем нужен SKIP LOCKED

Если строку уже держит другой worker, текущий worker её пропускает.

Так несколько workers могут обрабатывать разные строки параллельно, а не стоять
в очереди за одной залоченной строкой.

## Что нельзя делать

- Нельзя сначала прочитать pending records, а потом отдельно решать, кто их
  обработает.
- Нельзя делать side effect до claim/lock.
- Нельзя держать большую transaction во время внешнего вызова.
- Нельзя считать, что один worker навсегда решает проблему гонок.

## Индексы

Под hot queries нужны индексы:

- allocation операторов;
- due outbox records;
- stale outbox records;
- retry/recovery scans.

Без индексов `SKIP LOCKED` не спасёт от медленных запросов.

## Минусы

- Нужно следить за PostgreSQL lock wait и slow queries.
- Выбор конкретного оператора всё ещё может ждать lock этой строки.
- Если allocation станет bottleneck-ом, следующий шаг - явный
  `FOR UPDATE SKIP LOCKED` для operator claim или PostgreSQL
  `UPDATE ... RETURNING`.

## Что отклонили

- Один worker на всю обработку: проще, но плохо масштабируется.
- Advisory locks: сложнее сопровождать и хуже видно в коде.
- Только `UPDATE ... RETURNING`: хорошо для PostgreSQL-only, но текущему коду
  нужен совместимый fallback для тестов и MySQL.
