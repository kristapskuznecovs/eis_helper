PACKAGE=app_template
BACKEND_DIR=backend
WEB_DIR=apps/web

.PHONY: bootstrap sync dev api web worker up down test test-backend test-web test-cov lint format fix typecheck migrate makemigrations seed hooks

bootstrap:
	python3 scripts/bootstrap.py $(NAME)

sync:
	cd $(BACKEND_DIR) && uv sync --extra dev

dev:
	cd $(BACKEND_DIR) && uv run uvicorn $(PACKAGE).main:app --app-dir src --reload --port 8000

api:
	cd $(BACKEND_DIR) && uv run uvicorn $(PACKAGE).main:app --app-dir src --reload --port 8000

web:
	cd $(WEB_DIR) && npm run dev

worker:
	cd $(BACKEND_DIR) && uv run env PYTHONPATH=src python -m $(PACKAGE).worker

up:
	docker compose -f infra/docker/docker-compose.yml up -d

down:
	docker compose -f infra/docker/docker-compose.yml down

test: test-backend test-web

test-backend:
	cd $(BACKEND_DIR) && uv run pytest

test-web:
	cd $(WEB_DIR) && npm test

test-cov:
	cd $(BACKEND_DIR) && uv run pytest --cov=$(PACKAGE) --cov-report=term-missing --cov-report=html

lint:
	cd $(BACKEND_DIR) && uv run ruff check .
	cd $(WEB_DIR) && npm run lint

format:
	cd $(BACKEND_DIR) && uv run ruff format .

fix:
	cd $(BACKEND_DIR) && uv run ruff check . --fix
	cd $(BACKEND_DIR) && uv run ruff format .

typecheck:
	cd $(BACKEND_DIR) && uv run pyright
	cd $(WEB_DIR) && npm run typecheck

migrate:
	cd $(BACKEND_DIR) && uv run alembic upgrade head

makemigrations:
	cd $(BACKEND_DIR) && uv run alembic revision --autogenerate -m "describe-change"

seed:
	cd $(BACKEND_DIR) && uv run python ../database/seeds/seed.py

hooks:
	cd $(BACKEND_DIR) && uv run pre-commit install
