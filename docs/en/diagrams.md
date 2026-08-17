# Diagrams

PlantUML sources are in [diagrams](diagrams/).

- [Inbound call registration](diagrams/01-incoming-call-registration.puml)
- [Operator search](diagrams/02-operator-search.puml)
- [Telephony events](diagrams/03-telephony-events.puml)
- [Outbox publishing](diagrams/04-telephony-outbox-publisher.puml)
- [Call statuses](diagrams/05-call-state-machine.puml)
- [Call aggregate boundary](diagrams/06-call-aggregate-boundary.puml)

PNG preview: [diagrams/README.md](diagrams/README.md).

Regenerate:

```bash
plantuml -tpng docs/en/diagrams/*.puml
```
