# Диаграммы

Исходники PlantUML и PNG:

- `01-incoming-call-registration.puml` / `01-incoming-call-registration.png`
- `02-operator-search.puml` / `02-operator-search.png`
- `03-telephony-events.puml` / `03-telephony-events.png`
- `04-telephony-outbox-publisher.puml` / `04-telephony-outbox-publisher.png`
- `05-call-state-machine.puml` / `05-call-state-machine.png`

## Превью

### Регистрация входящего звонка

![Регистрация входящего звонка](./01-incoming-call-registration.png)

### Поиск оператора

![Поиск оператора](./02-operator-search.png)

### События от телефонии

![События от телефонии](./03-telephony-events.png)

### Публикация outbox

![Публикация telephony outbox](./04-telephony-outbox-publisher.png)

### Машина состояний

![Машина состояний звонка](./05-call-state-machine.png)

Перегенерация:

```bash
plantuml -tpng docs/diagrams/*.puml
```
