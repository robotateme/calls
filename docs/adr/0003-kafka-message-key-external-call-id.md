# ADR-0003: Kafka key всегда равен external_call_id

Status: Accepted

## Проблема

Для одного звонка порядок сообщений важен.

Нельзя, чтобы `operator_dialing`, `bridge_established`, no-answer и hangup одного
звонка разъехались по разным Kafka partitions.

Стабильный ключ звонка - `external_call_id`.

## Решение

Для всех Kafka messages одного звонка:

```text
key = external_call_id
```

Это касается:

- incoming facts;
- Telephony facts;
- outgoing Telephony commands.

Что это значит на практике: всё, что относится к одному звонку, попадает в одну
очередь порядка внутри Kafka. Calls не получает `hangup` раньше старого
`operator_dialing` только из-за того, что producer выбрал другой key.

## Проверка

Если Kafka key передан, consumer проверяет:

```text
message key == payload.external_call_id
```

Если не совпало, сообщение идёт в DLQ.

Calls не исправляет key сам. Несовпадение key и payload означает, что producer
нарушил договор, а порядок событий уже нельзя считать надёжным.

## Почему

Kafka держит порядок внутри partition. Один key даёт одну partition для одного
звонка.

`external_call_id` подходит, потому что это id звонка, а не id отдельного
сообщения.

## Нельзя

- Использовать `operator_id` как Kafka key.
- Использовать `command_id` или `message_id` как key для событий звонка.
- Отправлять события звонка без key.
- Обрабатывать mismatch key/payload как нормальное событие.

## Минусы

- Producer обязан ставить `external_call_id`.
- Ошибка producer-а станет DLQ record.
- Один звонок остаётся внутри одной partition.

## Отклонили

- Key по `operator_id`.
- Key по `command_id`/`message_id`.
- Сообщения без key.
