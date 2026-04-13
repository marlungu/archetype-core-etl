.PHONY: help setup up down restart logs ps test lint typecheck generate demo clean
.DEFAULT_GOAL := help

# Print all available targets
help:
	@printf "\n\033[1marchetype-core-etl\033[0m — available targets:\n\n"
	@printf "  \033[36msetup\033[0m       Generate .env from template with auto-generated secrets\n"
	@printf "  \033[36mup\033[0m          Start all services, wait for healthy, seed LocalStack\n"
	@printf "  \033[36mdown\033[0m        Stop all services\n"
	@printf "  \033[36mrestart\033[0m     Stop and restart all services\n"
	@printf "  \033[36mdemo\033[0m        Full setup + start (runs setup then up)\n"
	@printf "  \033[36mtest\033[0m        Install dev dependencies and run pytest\n"
	@printf "  \033[36mlint\033[0m        Run ruff linter\n"
	@printf "  \033[36mtypecheck\033[0m   Run mypy type checker\n"
	@printf "  \033[36mgenerate\033[0m    Generate 1000 synthetic test records\n"
	@printf "  \033[36mlogs\033[0m        Tail service logs\n"
	@printf "  \033[36mps\033[0m          Show running services\n"
	@printf "  \033[36mclean\033[0m       Remove all containers, volumes, and .env\n"
	@printf "\n"

# Generate .env from template
setup:
	@bash scripts/setup-local.sh

# Start all services and seed data
up:
	@docker compose up -d
	@printf "Waiting for Airflow init to complete"
	@for i in $$(seq 1 24); do \
		if docker compose ps airflow-init 2>/dev/null | grep -q "Exited (0)"; then \
			break; \
		fi; \
		printf "."; \
		sleep 5; \
	done
	@echo ""
	@printf "Waiting for Airflow webserver"
	@for i in $$(seq 1 24); do \
		if docker compose exec -T airflow-webserver curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then \
			break; \
		fi; \
		printf "."; \
		sleep 5; \
	done
	@echo ""
	@bash scripts/init-localstack.sh
	@echo ""
	@echo "========================================="
	@echo "  archetype-core-etl is ready!"
	@echo ""
	@echo "  Airflow UI:  http://localhost:8080"
	@echo "  Username:    admin"
	@echo "  Password:    admin"
	@echo "========================================="

# Stop all services
down:
	@docker compose down

# Restart all services
restart:
	@$(MAKE) down
	@$(MAKE) up

# Tail service logs
logs:
	@docker compose logs -f

# Show running services
ps:
	@docker compose ps

# Run tests
test:
	@pip install -e ".[dev]" --quiet
	@pytest

# Run linter
lint:
	@ruff check src/ tests/ dags/

# Run type checker
typecheck:
	@pip install -e ".[dev]" --quiet
	@mypy src/

# Generate synthetic data
generate:
	@python3 scripts/generate_data.py --records 1000 --seed 42

# Full demo: setup + up
demo:
	@$(MAKE) setup
	@$(MAKE) up

# Full cleanup
clean:
	@docker compose down -v --remove-orphans 2>/dev/null || true
	@rm -f .env
	@echo "Cleaned up: volumes removed, .env deleted."
