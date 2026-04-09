.PHONY: up down build seed test lint typecheck dashboard flow dbt-build dbt-docs clean

up:
	docker compose up -d --build
	@echo "Postgres: localhost:5432 | Prefect: http://localhost:4200 | Streamlit: http://localhost:8501"

down:
	docker compose down -v

build:
	docker compose build

seed:
	docker compose exec prefect-worker python -m orchestration.flows --run-once

test:
	docker compose exec prefect-worker pytest -ra
	docker compose exec prefect-worker bash -lc "cd dbt_project && dbt build --target test"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy --strict ingest transform orchestration

dashboard:
	docker compose up -d streamlit
	@open http://localhost:8501 2>/dev/null || true

flow:
	docker compose exec prefect-worker python -m orchestration.flows --run-once

dbt-build:
	docker compose exec prefect-worker bash -lc "cd dbt_project && dbt deps && dbt build --target dev"

dbt-docs:
	docker compose exec prefect-worker bash -lc "cd dbt_project && dbt docs generate && dbt docs serve --port 8080"

clean:
	docker compose down -v
	rm -rf .pytest_cache .ruff_cache .mypy_cache **/__pycache__
