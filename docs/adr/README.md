# Architecture Decision Records

ADR - это список решений, которые нельзя случайно "улучшить" в коде.

Каждый ADR отвечает на простые вопросы:

- какую проблему решаем;
- какое правило приняли;
- как это должно работать в коде;
- что нельзя делать;
- какие минусы приняли осознанно.

## Accepted

- [ADR-0001: Calls держит только локальную бронь оператора](0001-local-operator-reservation.md)
- [ADR-0002: Workers забирают строки через row locks и SKIP LOCKED](0002-skip-locked-for-workers.md)
- [ADR-0003: Kafka key всегда равен external_call_id](0003-kafka-message-key-external-call-id.md)
- [ADR-0004: /metrics отдаёт только кешированный snapshot](0004-metrics-scrape-from-cache.md)
