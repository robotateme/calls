# Architecture Decision Records

ADR фиксируют решения, которые влияют на архитектуру, runtime-поведение или
межсервисные контракты Calls.

Формат записи:

- Status;
- Context;
- Decision;
- Consequences;
- Alternatives.

## Accepted

- [ADR-0001: Локальная reservation оператора](0001-local-operator-reservation.md)
- [ADR-0002: Row locks и SKIP LOCKED для конкурентных workers](0002-skip-locked-for-workers.md)
- [ADR-0003: Kafka message key равен external_call_id](0003-kafka-message-key-external-call-id.md)
- [ADR-0004: /metrics отдаёт кешированный snapshot](0004-metrics-scrape-from-cache.md)
