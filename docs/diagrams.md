# Диаграммы

Исходники PlantUML и PNG лежат в [`docs/diagrams`](./diagrams).

Перегенерация:

```bash
plantuml -tpng docs/diagrams/*.puml
```

## Регистрация входящего звонка

![Регистрация входящего звонка](./diagrams/01-incoming-call-registration.png)

## Поиск оператора

![Поиск оператора](./diagrams/02-operator-search.png)

## События от телефонии

![События от телефонии](./diagrams/03-telephony-events.png)

## Публикация outbox

![Публикация telephony outbox](./diagrams/04-telephony-outbox-publisher.png)

## Машина состояний

![Машина состояний звонка](./diagrams/05-call-state-machine.png)
