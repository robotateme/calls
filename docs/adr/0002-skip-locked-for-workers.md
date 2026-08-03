# ADR-0002: Row locks и SKIP LOCKED для конкурентных workers

Status: Accepted

## Context

Calls работает несколькими workers:

- queue workers выбирают операторов;
- outbox publishers claim-ят due records;
- recovery jobs возвращают stale records и освобождают expired reservations.

Горячие таблицы `operators` и `telephony_outbox` могут обрабатываться
параллельно. Без row locks один record может быть выбран несколькими workers.
Без `SKIP LOCKED` workers могут ждать друг друга и терять throughput на lock wait.

## Decision

Для конкурентного claim/selection используются row locks:

- call lookup/recovery batches используют `FOR UPDATE SKIP LOCKED` для
  PostgreSQL/MySQL;
- outbox claim/requeue использует `FOR UPDATE SKIP LOCKED` для PostgreSQL/MySQL;
- operator allocation берёт выбранного оператора внутри transaction с row lock;
- SQLite/test fallback использует обычный lock режим Laravel.

Workers берут ограниченный batch, меняют статус/бронь в той же transaction и
дальше работают только с уже claimed records.

## Consequences

- Несколько workers могут идти параллельно, не выбирая один и тот же outbox
  record.
- Залоченные records пропускаются, а не блокируют весь batch.
- Нужны индексы под hot queries: allocation, due outbox, stale outbox и retry
  scans.
- Operator allocation всё ещё может ждать lock конкретной выбранной строки. Если
  это станет bottleneck-ом, следующий шаг - перевести operator claim на явный
  `FOR UPDATE SKIP LOCKED` или атомарный `UPDATE ... RETURNING` для PostgreSQL.
- При росте нагрузки всё равно надо мониторить PostgreSQL lock wait, slow
  queries и размер рабочих таблиц.

## Alternatives

- Один worker на allocation/outbox. Отклонено: простой вариант, но плохо
  масштабируется.
- Advisory locks. Отклонено для текущего slice: сложнее операционно и хуже
  читается, чем row-level claim в тех же таблицах.
- Claim через `UPDATE ... RETURNING`. Возможный вариант для PostgreSQL-only
  реализации, но текущий код сохраняет совместимый fallback для тестов и MySQL.
