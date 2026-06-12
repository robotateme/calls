SAIL := ./vendor/bin/sail
ARTISAN := php artisan
COMPOSER := composer
TEST_ENV := APP_ENV=testing APP_MAINTENANCE_DRIVER=file BCRYPT_ROUNDS=4 LOG_CHANNEL=null BROADCAST_CONNECTION=null CACHE_STORE=array DB_CONNECTION=sqlite DB_DATABASE=:memory: DB_URL= MAIL_MAILER=array QUEUE_CONNECTION=sync SESSION_DRIVER=array PULSE_ENABLED=false TELESCOPE_ENABLED=false NIGHTWATCH_ENABLED=false

.DEFAULT_GOAL := help

.PHONY: help up down restart status logs shell composer artisan migrate fresh test phpstan pint validate validate-local queue queue-retry schedule outbox-publish outbox-requeue-stale release-expired-reservations metrics-snapshot kafka-consume load-jsonl load-smoke load-stress-large load-soak-large production-build dead-letter-list dead-letter-prune kafka-topics kafka-ui

help:
	@printf "%s\n" "Available targets:"
	@printf "  %-18s %s\n" "up" "Start Sail services"
	@printf "  %-18s %s\n" "down" "Stop Sail services"
	@printf "  %-18s %s\n" "restart" "Restart Sail services"
	@printf "  %-18s %s\n" "status" "Show Compose service status"
	@printf "  %-18s %s\n" "logs" "Follow service logs, optionally SERVICE=name"
	@printf "  %-18s %s\n" "shell" "Open a shell in laravel.test"
	@printf "  %-18s %s\n" "composer" "Run Composer in Sail, pass CMD='install'"
	@printf "  %-18s %s\n" "artisan" "Run Artisan in Sail, pass CMD='about'"
	@printf "  %-18s %s\n" "migrate" "Run migrations in Sail"
	@printf "  %-18s %s\n" "fresh" "Refresh database in Sail"
	@printf "  %-18s %s\n" "test" "Run PHPUnit locally"
	@printf "  %-18s %s\n" "phpstan" "Run PHPStan locally"
	@printf "  %-18s %s\n" "pint" "Run Pint locally"
	@printf "  %-18s %s\n" "validate" "Run local validation suite"
	@printf "  %-18s %s\n" "queue" "Run calls queue worker in Sail"
	@printf "  %-18s %s\n" "queue-retry" "Run calls retry queue worker in Sail"
	@printf "  %-18s %s\n" "schedule" "Run Laravel scheduler worker in Sail"
	@printf "  %-18s %s\n" "outbox-publish" "Publish Telephony outbox once in Sail"
	@printf "  %-18s %s\n" "outbox-requeue-stale" "Requeue stale Telephony outbox processing records once in Sail"
	@printf "  %-18s %s\n" "release-expired-reservations" "Release expired operator reservations once in Sail"
	@printf "  %-18s %s\n" "metrics-snapshot" "Record operational metrics snapshot once in Sail"
	@printf "  %-18s %s\n" "kafka-consume" "Consume Kafka JSONL records from stdin in Sail, pass TOPIC=name"
	@printf "  %-18s %s\n" "load-jsonl" "Generate incoming-call JSONL and consume locally, pass COUNT=1000"
	@printf "  %-18s %s\n" "load-smoke" "Run smoke JSONL load profile in Sail"
	@printf "  %-18s %s\n" "load-stress-large" "Run large stress JSONL profile in Sail"
	@printf "  %-18s %s\n" "load-soak-large" "Run large soak JSONL profile in Sail"
	@printf "  %-18s %s\n" "production-build" "Build production image locally"
	@printf "  %-18s %s\n" "dead-letter-list" "List unresolved DLQ records in Sail"
	@printf "  %-18s %s\n" "dead-letter-prune" "Prune resolved DLQ records in Sail"
	@printf "  %-18s %s\n" "kafka-topics" "List Kafka topics"
	@printf "  %-18s %s\n" "kafka-ui" "Print Kafka UI URL"

up:
	$(SAIL) up -d

down:
	$(SAIL) down

restart:
	$(SAIL) down
	$(SAIL) up -d

status:
	docker compose ps

logs:
	docker compose logs -f $(SERVICE)

shell:
	$(SAIL) shell

composer:
	$(SAIL) composer $(CMD)

artisan:
	$(SAIL) artisan $(CMD)

migrate:
	$(SAIL) artisan migrate

fresh:
	$(SAIL) artisan migrate:fresh --seed

test:
	$(TEST_ENV) $(ARTISAN) test

phpstan:
	$(COMPOSER) phpstan

pint:
	./vendor/bin/pint --dirty

validate: validate-local

validate-local:
	docker compose config --quiet
	./vendor/bin/pint --dirty --test
	$(COMPOSER) phpstan
	$(TEST_ENV) $(ARTISAN) test
	$(COMPOSER) validate --strict --no-check-publish

queue:
	$(SAIL) artisan queue:work redis --queue=calls --tries=1 --timeout=0

queue-retry:
	$(SAIL) artisan queue:work redis --queue=calls-retry --tries=1 --timeout=0

schedule:
	$(SAIL) artisan schedule:work

outbox-publish:
	$(SAIL) artisan calls:telephony-outbox:publish

outbox-requeue-stale:
	$(SAIL) artisan calls:telephony-outbox:requeue-stale

release-expired-reservations:
	$(SAIL) artisan calls:operator-reservations:release-expired

metrics-snapshot:
	$(SAIL) artisan calls:metrics:snapshot

kafka-consume:
	$(SAIL) artisan calls:kafka:consume $(TOPIC)

load-jsonl:
	php tools/load/generate-incoming-calls-jsonl.php $${COUNT:-1000} local | $(ARTISAN) calls:kafka:consume incoming-calls --limit=$${COUNT:-1000} --timeout-ms=5000

load-smoke:
	$(SAIL) bash -lc 'LOAD_PROFILE=smoke bash tools/load/run-jsonl-load.sh'

load-stress-large:
	$(SAIL) bash -lc 'LOAD_PROFILE=stress-large bash tools/load/run-jsonl-load.sh'

load-soak-large:
	$(SAIL) bash -lc 'LOAD_PROFILE=soak-large bash tools/load/run-jsonl-load.sh'

production-build:
	docker build -f docker/production/Dockerfile -t calls:production .

dead-letter-list:
	$(SAIL) artisan calls:dead-letter:list

dead-letter-prune:
	$(SAIL) artisan calls:dead-letter:prune-resolved

kafka-topics:
	docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

kafka-ui:
	@printf "%s\n" "Kafka UI: http://localhost:$${FORWARD_KAFKA_UI_PORT:-8081}"
