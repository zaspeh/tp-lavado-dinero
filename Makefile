.PHONY: generate up down logs clean small_test medium_test chaos real_chaos catastrophe

COMPOSE_FILE=Compose.yml
GENERATOR_SCRIPT=scripts/generate-compose.py
TEST_SCRIPT=scripts/verify_outputs.py
CHAOS_SCRIPT=scripts/chaos_monkey.py
HYPERVISOR_CONTAINER ?= fault_hypervisor_2
CLIENT ?= 1

generate:
	@python3 $(GENERATOR_SCRIPT)

up: generate $(COMPOSE_FILE)
	docker compose -f $(COMPOSE_FILE) up -d --build --remove-orphans

down:
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

clean:
	rm -f $(COMPOSE_FILE)
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	docker image prune -f

medium_test:
	@python3 $(TEST_SCRIPT) --expected-dir expected_outputs/expected_hi_medium --client $(CLIENT)

small_test:
	@python3 $(TEST_SCRIPT) --expected-dir expected_outputs/expected_hi_small --client $(CLIENT)

chaos:
	@python3 $(CHAOS_SCRIPT) $(if $(INTERVAL),--interval $(INTERVAL)) $(if $(TARGET),--target $(TARGET))

real_chaos:
	@python3 $(CHAOS_SCRIPT) --hypervisor-container $(HYPERVISOR_CONTAINER) $(if $(INTERVAL),--interval $(INTERVAL)) $(if $(TARGET),--target $(TARGET)) --kill

catastrophe:
	docker exec $(HYPERVISOR_CONTAINER) sh -c 'docker kill $$(docker ps -q)'
