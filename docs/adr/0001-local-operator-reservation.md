# ADR-0001: Локальная reservation оператора

Status: Accepted

## Context

Calls должен назначить входящий call одному оператору при параллельной работе
нескольких queue workers.

Фактическая доступность оператора не принадлежит Calls. Поля `available` и `afk`
считаются внешней read model. Calls отвечает только за lifecycle звонка до
`connected`.

Если использовать только `available=false`, сервис начнёт владеть чужой
предметной областью: занятостью оператора, разговором после соединения и
дальнейшей доступностью.

## Decision

Calls хранит краткую локальную reservation:

- `operators.reserved_call_id`;
- `operators.reserved_at`.

Allocation выбирает оператора только если:

- `available=true`;
- `afk=false`;
- `reserved_call_id is null`.

Release очищает reservation только для того call, который её держит. После
`connected` Calls освобождает `reserved_call_id`, но не выставляет
`available=true`.

## Consequences

- Два workers не должны назначить одного оператора одному или разным calls.
- Calls не решает, свободен ли оператор после `connected`.
- Зависшие reservation надо чистить отдельным compensation flow:
  `calls:operator-reservations:release-expired`.
- При выделении operator-service нужен отдельный контракт reservation/release
  или read model, но state machine Calls не должна переехать в availability.

## Alternatives

- Менять `operators.available` внутри Calls. Отклонено: это смешивает локальную
  бронь и фактическую доступность.
- Не хранить reservation и полагаться только на Telephony. Отклонено: workers
  смогут одновременно отправить assignment одному оператору.
- Перенести allocation в отдельный dispatch-service. Это возможное целевое
  направление, но сейчас не требуется для текущего slice.
