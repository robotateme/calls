# Диаграммы

PlantUML-исходники лежат в [diagrams](diagrams/).

- [Регистрация входящего звонка](diagrams/01-incoming-call-registration.puml)
- [Поиск оператора](diagrams/02-operator-search.puml)
- [События от телефонии](diagrams/03-telephony-events.puml)
- [Публикация outbox](diagrams/04-telephony-outbox-publisher.puml)
- [Статусы звонка](diagrams/05-call-state-machine.puml)
- [Граница агрегата Call](diagrams/06-call-aggregate-boundary.puml)

PNG-превью: [diagrams/README.md](diagrams/README.md).

Перегенерировать:

```bash
plantuml -tpng docs/ru/diagrams/*.puml
```
