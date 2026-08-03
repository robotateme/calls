# ADR-0003: Kafka message key равен external_call_id

Status: Accepted

## Context

Calls получает facts по одному звонку из Kafka и публикует commands в Telephony.
Для одного call порядок событий критичен:

- incoming call должен быть зарегистрирован до Telephony facts;
- `operator_dialing` должен относиться к текущему assignment attempt;
- `bridge_established`, no-answer/drop и hangup не должны приходить в Calls в
  случайном порядке между partitions.

Стабильный business key звонка - `external_call_id`.

## Decision

Все Kafka messages одного звонка используют:

```text
key = external_call_id
```

Это касается:

- incoming facts;
- Telephony facts;
- outgoing Telephony commands из outbox.

Consumer проверяет message key, если он передан: key должен совпадать с
`payload.external_call_id`. Несовпадение считается contract violation и уходит в
DLQ.

## Consequences

- Kafka сохраняет порядок событий одного call внутри partition.
- Replay и audit проще связывать по одному business key.
- Consumer groups могут масштабироваться по partitions, но один call не должен
  размазываться по разным partitions.
- Если producer не ставит key или ставит другой key, это upstream contract bug.

## Alternatives

- Key по `operator_id`. Отклонено: события одного call могут попасть в разные
  partitions при смене оператора или retry.
- Key по `command_id`/`message_id`. Отклонено: это технические ids, они ломают
  ordering одного business flow.
- Без key. Отклонено: Kafka не гарантирует порядок одного call между partitions.
