# Adding A Module

## Smallest valid module

Every module must contain:

- `__init__.py`
- at least one real file such as `router.py`, `service.py`, `schemas.py`, or `models.py`

Do not create empty files for symmetry.

## Usual module shape

Use only the files the module actually needs:

- `router.py` for HTTP transport
- `schemas.py` for request/response models
- `models.py` for ORM models
- `service.py` for business logic
- `tasks.py` for async entrypoints
- `repository.py` only when query complexity justifies it

## Rules

- route handlers never query the DB directly
- tasks never own business logic
- provider-specific AI code stays out of modules
- concrete storage backends stay out of modules

## Process

1. Create `backend/src/<package>/modules/<module_name>/`.
2. Add only the needed files.
3. Register the router in `main.py` if the module exposes HTTP endpoints.
4. Add models to `shared/db/models.py` if the module owns tables.
5. Create an Alembic migration for schema changes.
