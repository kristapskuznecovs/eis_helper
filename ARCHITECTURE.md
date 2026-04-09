# Architecture Rules

This repository is a modular monolith with:

- Next.js frontend in `apps/web`
- FastAPI backend in `backend`
- PostgreSQL + SQLAlchemy ORM + Alembic
- optional async workers
- AI/document processing behind provider abstractions

## Hard rules

- No business logic in route handlers.
- No provider-specific AI code in modules.
- No direct Celery imports in modules.
- No generic top-level `services/` outside modules.
- No runtime file writes outside `data/`.
- No schema changes without an Alembic migration.
- Route handlers and tasks never query the DB directly.
- `shared/` is infrastructure only, not domain logic.
- Modules may only contain files they actually need.
- Every request and task must be traceable by `request_id`.

## Module rules

- `router.py` is transport only.
- `service.py` owns business logic.
- `tasks.py` delegates immediately to `service.py`.
- `repository.py` is optional and only added when query complexity justifies it.
- Minimum module is `__init__.py` plus one real file.

## Data rules

- `database/` is checked in.
- `data/` is gitignored.
- Uploaded files, OCR output, and exports are runtime data, not source code.

## Backend seams

- Auth mechanics live only in `shared/auth/`.
- AI provider code lives only in `shared/ai/`.
- Queue implementation details live only in `shared/jobs/`.
- Storage implementation details live only in `shared/storage/`.
- Modules must request storage through a shared storage factory, not import a concrete backend directly.

## Queue

- Modules call `shared.jobs.queue` only and never import Celery directly.
- `JOBS_BACKEND=inline` runs jobs in-process and requires no Redis.
- `JOBS_BACKEND=celery` enables the Celery-backed job adapter.
- Switching queue backends must require only config changes, not module rewrites.

## Storage

- Modules call `shared.storage.service` only and never import storage backends directly.
- `STORAGE_BACKEND=local` writes to `data/uploads/`.
- `STORAGE_BACKEND=s3` works with AWS S3 or MinIO.
- MinIO is available in the workers compose profile for local S3-compatible testing.

## Auth

- Base template auth is email/password plus JWT access token.
- Password minimum is 8 characters with at least one uppercase letter and one digit.
- Refresh-token rotation is an optional extension, not part of the base template.
- Auth validation must have explicit test coverage.

## Logging and errors

- Every HTTP response must include `X-Request-ID`.
- Every log line must include the active `request_id`.
- Auth resolution may bind `user_id` into logging context.
- Tasks must clear and bind fresh context before work starts.
- API errors must use a standardized JSON error envelope.

## Internationalization

- Default locale is `lv`; additional supported locales are defined centrally in shared config.
- Backend locale is resolved once per request from `X-Locale`, then optional `lang`, then `Accept-Language`, then fallback.
- Locale is bound to request context; services must not thread locale through function signatures.
- Services and domain code must not emit user-facing strings directly when a stable translation key is possible.
- Backend translation happens at the API boundary and error-handler layer.
- Missing translations fall back in this order: requested locale, default locale, translation key.
- Frontend routes are locale-prefixed (`/lv/...`, `/en/...`) and locale switching must preserve the current route when possible.
