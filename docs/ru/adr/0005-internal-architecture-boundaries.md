# ADR-0005: Внутренняя архитектура разделена на слои

Status: Accepted

## Проблема

Calls работает с несколькими вещами сразу:

- Kafka;
- очереди Laravel;
- PostgreSQL;
- Redis;
- outbox;
- DLQ;
- метрики Prometheus;
- правила звонка и выбора оператора.

Если всё это смешать в jobs, controllers, Eloquent models и console commands,
бизнес-правила начнут зависеть от Laravel и деталей хранения. Тогда любое
изменение Kafka, БД или очереди будет легко ломать обработку звонка.

Нам нужно, чтобы правило звонка было видно отдельно от транспорта и БД.

## Решение

Код разделён на слои:

- `src/Domain` - правила предметной области;
- `src/Application` - use cases, обработчики и ports;
- `src/Infrastructure` - адаптеры Laravel, БД, Kafka и Redis;
- `app` - Laravel entrypoints и wiring.

Зависимости идут внутрь:

```text
app -> Application -> Domain
Infrastructure -> Application -> Domain
```

`Domain` не знает про Laravel, БД, Kafka, Redis, HTTP и queue.

`Application` знает про `Domain` и свои ports, но не знает про Eloquent,
Laravel collections, Kafka clients или Redis clients.

`Infrastructure` реализует ports и маппит строки хранения в доменные объекты.

`app` связывает Laravel entrypoints с application-обработчиками.

## Почему так

Обработка звонка содержит бизнес-решения:

- можно ли менять статус;
- что делать при no-answer;
- когда повтор, а когда финал;
- можно ли применить позднее событие;
- когда снять бронь оператора;
- какую команду записать в outbox.

Эти правила должны проверяться тестами без поднятия Kafka и без знания SQL.

Laravel остаётся важным, но он не должен становиться местом, где спрятано
бизнес-решение. Job, controller, console command и Kafka consumer должны только
принять вход, собрать command и вызвать обработчик.

## Правила

- Бизнес-правила звонка живут в `Domain`.
- Оркестрация use case живёт в `Application`.
- Runtime config (`config()`, `env()`) живёт в `app` или `Infrastructure`.
- Eloquent models не возвращаются из repository ports.
- Repository ports возвращают доменные объекты, value objects, `null`, `void` или
  списки доменных объектов.
- Mapping из строк БД в доменные объекты делается в `Infrastructure`.
- Транзакции открывает application handler через port.
- Адаптеры Kafka, Redis, БД, метрик, DLQ и outbox живут в `Infrastructure`.
- Laravel jobs, controllers, console commands и providers остаются тонкими.

## Последствия

Плюсы:

- обработку звонка легче читать и тестировать;
- поздние события и retry rules не размазаны по framework-коду;
- Kafka или DB adapter можно менять без переписывания доменных правил;
- architecture boundary test может ловить неправильные зависимости.

Минусы:

- нужно писать mapping между Eloquent и доменными объектами;
- простая правка иногда требует больше файлов;
- нельзя быстро вернуть Eloquent model из repository, даже если так короче;
- нужно следить, чтобы новые handlers не превращались в controllers с бизнесом.

## Нельзя

- Импортировать Laravel в `src/Domain`.
- Возвращать Eloquent model из application repository port.
- Читать `config()` внутри domain object.
- Прятать смену статуса в job, controller или Eloquent observer.
- Делать Kafka consumer источником бизнес-правил.
- Обходить outbox прямым вызовом Telephony adapter.

## Отклонили

- Оставить обычный Laravel MVC для всего flow.
- Держать state machine в Eloquent model.
- Держать всю обработку в queue jobs.
- Сделать Kafka consumer главным местом бизнес-логики.
- Вынести всё в отдельные микросервисы сейчас.
