# Диаграммы

Исходники PlantUML и PNG лежат рядом.

Перегенерация:

```bash
plantuml -tpng docs/diagrams/*.puml
```

## Превью

![Регистрация входящего звонка](./01-incoming-call-registration.png)

![Поиск оператора](./02-operator-search.png)

![События от телефонии](./03-telephony-events.png)

![Публикация telephony outbox](./04-telephony-outbox-publisher.png)

![Машина состояний звонка](./05-call-state-machine.png)
