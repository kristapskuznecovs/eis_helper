# Backend

FastAPI modular monolith starter.

## First run

```bash
uv sync --extra dev
uv run alembic upgrade head
uv run uvicorn app_template.main:app --app-dir src --reload --port 8000
```

## Rules

- routes stay thin
- services own business logic
- tasks delegate to services
- modules never import provider SDKs directly
- modules never import Celery directly
- modules never import a concrete storage backend directly
- request logging is structured and correlated by `request_id`
- errors return a standardized JSON envelope

## Common commands

```bash
uv run uvicorn app_template.main:app --app-dir src --reload --port 8000
uv run alembic upgrade head
uv run pytest --cov=app_template --cov-report=term-missing
uv run pyright
```

## Logging and errors

The backend template includes:

- structured JSON logs via `structlog`
- request middleware that attaches `X-Request-ID`
- user context binding after auth resolution
- task context binding for background work
- standardized error handlers for HTTP, validation, application, and unexpected errors

## Queue and storage

The template now includes:

- `shared.jobs.queue` as the only public queue interface for modules
- inline and Celery job backends
- `shared.storage.service` as the only public storage interface for modules
- local and S3/MinIO-compatible storage backends

## Migrations

The template ships with an initial Alembic migration for:

- `users`
- `documents`

Create future revisions with:

```bash
uv run alembic revision --autogenerate -m "describe-change"
```
