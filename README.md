# Modular Monolith Template

Opinionated starter template for:

- `apps/web` with Vite + React + Tailwind
- `backend` with FastAPI
- PostgreSQL + SQLAlchemy ORM + Alembic
- optional async workers behind a queue abstraction
- AI-assisted OCR / extraction workflows without provider lock-in

## Why this template exists

This template is designed to stay usable after the first month. It avoids the common failure modes:

- flat `frontend/` roots that break when you add a second app
- generic `services/` dumping grounds
- business logic inside route handlers
- provider-specific AI code spread across modules
- direct Celery coupling in domain code
- runtime files mixed with checked-in database assets

## Quick start

1. Copy this template to a new project directory.
2. Run the bootstrap script:

```bash
make bootstrap NAME=my_project
```

3. Copy `.env.example` to `.env`.
4. Run:

```bash
cd backend && uv sync --extra dev && cd ..
make up
make migrate
make dev
```

`uv sync` will also generate `backend/uv.lock`, which should be committed in real projects.

## Included implementation details

This template already includes:

- a bootstrap rename script in `scripts/bootstrap.py`
- an initial Alembic migration for `users` and `documents`
- a storage factory so modules do not import a concrete storage backend directly
- CI workflow for Ruff, Pyright, pytest, and frontend checks
- runbooks for AI pipelines and adding new modules
- request-scoped structured logging with `X-Request-ID`
- a standardized JSON error envelope
- swappable queue backends behind `shared.jobs.queue`
- swappable local/S3 storage behind `shared.storage.service`
- MinIO in the workers Docker profile for local S3-compatible testing

## Python quality tools

The backend workflow uses:

- `uv` for dependency management
- Ruff for linting and formatting
- Pyright for type checking
- `pytest` + coverage for tests
- `pre-commit` to enforce checks before commit

Common commands:

```bash
make lint
make format
make fix
make typecheck
make test-cov
```

## Observability contract

The backend includes structured request logging and standardized API errors.

- every request gets `X-Request-ID`
- logs are correlated by `request_id`
- authenticated requests can bind `user_id` into log context
- async tasks bind their own `request_id`
- errors return a stable JSON envelope with `error.code`, `error.message`, and `error.request_id`

Install hooks once per clone:

```bash
make hooks
```

For optional workers:

```bash
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.workers.yml --profile workers up -d
```

## Repo shape

```text
apps/web        Vite frontend
backend         FastAPI backend
database        checked-in SQL/bootstrap assets
data            gitignored runtime files
infra/docker    containers and compose files
docs            ADRs and runbooks
```

Read [ARCHITECTURE.md](./ARCHITECTURE.md) before adding code.
