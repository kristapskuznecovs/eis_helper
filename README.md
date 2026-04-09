# EIS Helper

Latvian public procurement intelligence platform built on top of [EIS](https://www.eis.gov.lv) (Elektroniskā iepirkumu sistēma) — Latvia's national procurement portal.

## What it does

- **Tender search** — find open public tenders by keyword, CPV code, planning region, contract value, deadline, and procedure type
- **AI chat** — conversational search assistant (GPT-4o-mini) that asks one question at a time and builds filters from natural language
- **Analytics dashboards** — top winners/buyers by awarded value, regional distribution, win rates, multi-lot stats, buyer concentration
- **Company intelligence** — bid history, win rates, close-loss analysis, CPV sector specializations
- **CKAN data sync** — pulls procurement announcements, results, participants, amendments, purchase orders, and deliveries from [data.gov.lv](https://data.gov.lv)

Primary focus is construction procurement (CPV prefix `45`). Supports Latvian and English.

## Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 15, React 19, TypeScript, Tailwind CSS, next-intl |
| Backend | FastAPI, SQLAlchemy 2.0, Alembic, Pydantic, Uvicorn |
| Primary DB | PostgreSQL (users, auth, procurements) |
| Analytics DB | SQLite (read-only, checked in at `database/`) |
| AI | OpenAI SDK (gpt-4o-mini) |
| Storage | Local or S3/MinIO |
| Jobs | Inline (default) or Celery + Redis |
| Packaging | uv |

## Quick start

```bash
cp .env.example .env          # fill in secrets
cd backend && uv sync --extra dev && cd ..
make up                       # start PostgreSQL and MinIO via Docker
make migrate                  # run Alembic migrations
make dev                      # start backend (:8000) and frontend (:8080)
```

Install git hooks once per clone:

```bash
make hooks
```

## Repo layout

```
apps/web        Next.js frontend
backend         FastAPI backend
database        checked-in SQLite analytics database
data            gitignored runtime files (uploads, OCR output, exports)
infra/docker    Docker Compose files
docs            ADRs and runbooks
scripts         bootstrap and utility scripts
```

## Backend modules

| Module | Responsibility |
|---|---|
| `auth` | Login, registration, JWT token issuance |
| `users` | User profiles |
| `documents` | File upload and storage |
| `chat` | AI-powered tender search, company CPV profiling |
| `extraction` | CKAN sync pipeline, procurement classification, analytics queries |

## Common commands

```bash
make lint          # Ruff check + frontend lint
make format        # Ruff format
make fix           # Ruff --fix + format
make typecheck     # Pyright + tsc --noEmit
make test          # pytest + frontend tests
make test-cov      # pytest with coverage report
make migrate       # alembic upgrade head
make makemigrations MSG="description"   # alembic revision --autogenerate
```

## Environment variables

Key variables to set in `.env`:

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `JWT_SECRET_KEY` | Must be changed from the default |
| `OPENAI_API_KEY` | Required for chat and procurement classification |
| `STORAGE_BACKEND` | `local` (default) or `s3` |
| `JOBS_BACKEND` | `inline` (default) or `celery` |
| `REDIS_URL` | Required only when `JOBS_BACKEND=celery` |

## Optional workers

```bash
docker compose -f infra/docker/docker-compose.yml -f infra/docker/docker-compose.workers.yml --profile workers up -d
```

Read [ARCHITECTURE.md](./ARCHITECTURE.md) before adding code.
