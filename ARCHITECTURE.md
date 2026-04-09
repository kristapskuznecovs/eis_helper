# Architecture

This is a modular monolith with:

- Next.js 15 frontend in `apps/web`
- FastAPI backend in `backend`
- PostgreSQL as the primary database (users, auth, procurements)
- SQLite as a read-only analytics database (`database/eis_procurement_records.sqlite`)
- OpenAI for procurement classification and conversational search
- Optional async workers via Celery + Redis

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

- `database/` is checked in (read-only SQLite for analytics).
- `data/` is gitignored (uploads, OCR output, exports).
- Uploaded files, OCR output, and exports are runtime data, not source code.
- The SQLite analytics database is populated by the extraction pipeline, not by migrations.

## Backend seams

- Auth mechanics live only in `shared/auth/`.
- AI provider code lives only in `shared/ai/`.
- Queue implementation details live only in `shared/jobs/`.
- Storage implementation details live only in `shared/storage/`.
- Modules must request storage through the shared storage factory, not import a concrete backend directly.

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

- Auth is email/password plus JWT access token.
- Password minimum is 8 characters with at least one uppercase letter and one digit.
- Refresh-token rotation is an optional extension, not part of the base.
- Auth validation must have explicit test coverage.

## Extraction pipeline

The extraction module (`modules/extraction/`) is the largest module and owns the full data lifecycle:

1. **CKAN sync** (`fetch_ckan_raw.py`) — pulls procurement datasets from data.gov.lv into PostgreSQL raw tables
2. **Classification** (`fetch_metadata.py`) — calls OpenAI to classify procurements (construction vs other, work type, asset scale)
3. **Analytics build** (`eis_analytics.py`) — query layer over the SQLite database, used by dashboard endpoints
4. **Storage** (`collector_storage.py`) — writes classified records into SQLite and PostgreSQL

The SQLite file at `database/eis_procurement_records.sqlite` is the source for all analytics dashboards. It is populated offline by the pipeline and committed when updated. It is never written to by request handlers.

## Internationalization

- Default locale is `lv`; English (`en`) is also supported.
- Backend locale is resolved once per request from `X-Locale`, then optional `lang`, then `Accept-Language`, then fallback to `lv`.
- Locale is bound to request context; services must not thread locale through function signatures.
- Services and domain code must not emit user-facing strings directly when a stable translation key is possible.
- Backend translation happens at the API boundary and error-handler layer.
- Missing translations fall back in this order: requested locale → default locale → translation key.
- Frontend routes are locale-prefixed (`/lv/...`, `/en/...`) and locale switching must preserve the current route when possible.

## Logging and errors

- Every HTTP response must include `X-Request-ID`.
- Every log line must include the active `request_id`.
- Auth resolution may bind `user_id` into logging context.
- Tasks must clear and bind fresh context before work starts.
- API errors must use the standardized JSON error envelope: `{ "error": { "code", "message", "request_id" } }`.

## Domain conventions

- Procurement statuses are Latvian strings from EIS. Open statuses: `Izsludināts`, `Pieteikumi/piedāvājumi atvērti`. The chat assistant must only surface open tenders.
- Planning regions are derived from the `delivery_location` field and mapped to one of six regions: Rīga, Vidzeme, Kurzeme, Zemgale, Latgale, Pierīga.
- Company names are normalized by stripping Latvian legal designations (`SIA`, `AS`, `PSIA`, etc.) before matching.
- CPV prefix `45` covers construction. The system is designed to be extended to other CPV sectors without structural changes.
