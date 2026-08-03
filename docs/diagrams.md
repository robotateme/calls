# Диаграммы

Исходники PlantUML и PNG лежат в [docs/diagrams](diagrams/).

## Список

- [Регистрация входящего звонка](diagrams/01-incoming-call-registration.puml)
- [Поиск оператора](diagrams/02-operator-search.puml)
- [События от телефонии](diagrams/03-telephony-events.puml)
- [Публикация telephony outbox](diagrams/04-telephony-outbox-publisher.puml)
- [Машина состояний звонка](diagrams/05-call-state-machine.puml)

PNG-превью собраны в [docs/diagrams/README.md](diagrams/README.md).

## Перегенерация

```bash
plantuml -tpng docs/diagrams/*.puml
```
