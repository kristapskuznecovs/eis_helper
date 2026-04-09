# Docker Workflow

## Base stack

The base compose file starts PostgreSQL only:

```bash
docker compose -f infra/docker/docker-compose.yml up -d
```

Use this when you want the simplest local setup while running the API and frontend directly on your machine.

## Worker profile

Enable Redis and the worker service with:

```bash
docker compose \
  -f infra/docker/docker-compose.yml \
  -f infra/docker/docker-compose.workers.yml \
  --profile workers up -d
```

Use this when OCR, extraction, or other background work should run through the queue layer.

## Notes

- `database/init/` is mounted into the Postgres container for first-boot SQL.
- runtime files belong under `data/`, not in container-specific temp locations.
- the template keeps workers optional so projects can start simple and scale later.
