# ADR-0003: Kafka key всегда равен external_call_id

Status: Accepted

## Проблема

Calls получает и отправляет Kafka messages по звонку.

Для одного звонка порядок важен:

- сначала надо зарегистрировать incoming call;
- потом можно применять Telephony facts;
- `operator_dialing` должен относиться к текущей попытке назначения;
- retry/no-answer/drop/bridge events не должны перемешиваться между Kafka
  partitions.

Стабильный business key звонка - `external_call_id`.

## Решение

Для всех Kafka messages одного звонка:

```text
key = external_call_id
```

Это относится к:

- incoming facts;
- Telephony facts;
- outgoing Telephony commands из outbox.

## Проверка consumer-а

Если Kafka message key передан, consumer проверяет:

```text
message key == payload.external_call_id
```

Если значения разные, это contract violation.

Такое сообщение нельзя молча обрабатывать. Его надо отправить в DLQ.

## Почему это важно

Kafka сохраняет порядок сообщений внутри одной partition. Чтобы события одного
call попали в одну partition, у них должен быть один key.

`external_call_id` подходит, потому что это business key звонка, а не технический
id отдельного сообщения.

## Что нельзя делать

- Нельзя использовать `operator_id` как Kafka key.
- Нельзя использовать `command_id` или `message_id` как Kafka key для call flow.
- Нельзя отправлять сообщения без key, если они относятся к call flow.
- Нельзя обрабатывать mismatch key/payload как нормальное событие.

## Минусы

- Producer обязан знать и ставить `external_call_id`.
- Ошибка producer-а с неправильным key станет DLQ record.
- Масштабирование идёт по partitions, но один call остаётся внутри одной
  partition.

## Что отклонили

- Key по `operator_id`: при retry или смене оператора события одного call могут
  попасть в разные partitions.
- Key по `command_id`/`message_id`: это технические ids, они ломают ordering
  одного business flow.
- Без key: Kafka не гарантирует порядок одного call между partitions.
