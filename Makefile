.PHONY: generate up down logs clean

COMPOSE_FILE=Compose.yml
GENERATOR_SCRIPT=scripts/generate-compose.py

generate:
	@python3 $(GENERATOR_SCRIPT)

up: generate $(COMPOSE_FILE)
	docker compose -f $(COMPOSE_FILE) up -d --build --remove-orphans

down:
	docker compose -f $(COMPOSE_FILE) stop -t 1
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

clean:
	rm -f $(COMPOSE_FILE)
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	docker image prune -f
