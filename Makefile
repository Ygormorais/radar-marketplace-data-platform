.PHONY: install lint typecheck test contract generate dbt-parse pbir-validate fabric-validate infra-up infra-down check

install:
	python -m pip install -e ".[dev]"

lint:
	python -m ruff check .
	python -m ruff format --check .

typecheck:
	python -m mypy

test:
	python -m pytest --cov=radar --cov-report=term-missing

contract:
	python -m pytest -m contract

dbt-parse:
	dbt parse --project-dir dbt --profiles-dir dbt --no-partial-parse

pbir-validate:
	npx --no-install powerbi-report-author validate powerbi/Radar.pbip --format text

fabric-validate:
	python scripts/deploy_fabric.py --workspace-id 00000000-0000-0000-0000-000000000000 --dry-run

generate:
	python scripts/generate_events.py --order-count 1000 --output data/generated/delivery-events.jsonl
	python scripts/generate_clickstream.py --user-count 1000 --output data/generated/clickstream-events.jsonl

infra-up:
	docker compose -f infrastructure/docker/compose.yml up -d --wait

infra-down:
	docker compose -f infrastructure/docker/compose.yml down

check: lint typecheck test dbt-parse pbir-validate fabric-validate
