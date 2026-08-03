# ADR

ADR - это решения, которые нельзя случайно поменять в коде.

Каждый файл отвечает:

- какая была проблема;
- какое правило приняли;
- как это должно работать;
- что нельзя делать;
- какие минусы приняли.

## Accepted

- [ADR-0001: Calls держит только локальную бронь оператора](0001-local-operator-reservation.md)
- [ADR-0002: Workers забирают строки через row locks и SKIP LOCKED](0002-skip-locked-for-workers.md)
- [ADR-0003: Kafka key всегда равен external_call_id](0003-kafka-message-key-external-call-id.md)
- [ADR-0004: /metrics отдаёт только готовые метрики](0004-metrics-scrape-from-cache.md)
