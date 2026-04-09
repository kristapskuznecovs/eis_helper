# Architecture Decision Record (ADR)

Current as of 2026-04-09.

## ADR-001: Modular pipeline scripts
- Date: 2026-03-06
- Decision: Use separate runnable scripts in `pipelines/`; no central orchestrator.
- Why: Each stage can run independently and is easier to test/debug.
- Trade-off: Users run multiple commands manually.

## ADR-002: Shared `lib/` modules
- Date: 2026-03-06
- Decision: Move common logic to `lib/` (`collector_*`, `utils`).
- Why: Remove duplicated logic and reduce drift.
- Trade-off: More imports and file navigation.

## ADR-003: Hybrid-first classification
- Date: 2026-03-06
- Decision: Keep heuristic mode; allow `hybrid`/`openai` for uncertain cases.
- Why: Balance cost, speed, and extraction quality.
- Trade-off: Maintain both rule-based and LLM paths.

## ADR-004: SQLite as primary persistence
- Date: 2026-03-23 (updated from file-based)
- Decision: Persist all pipeline state in `database/eis_procurement_records.sqlite`.
- Why: Enables cross-pipeline queries, dashboard aggregations, and company identity joins.
- Trade-off: Requires schema migrations when adding columns (handled via `ALTER TABLE` in `collector_storage.py`).

## ADR-005: Canonical company index
- Date: 2026-03-23
- Decision: Build a `companies` table via `pipelines/build_company_index.py` using reg number anchoring + fuzzy matching.
- Why: Same company appears under many name variants in raw EIS data; reliable winner statistics require a canonical identity.
- Trade-off: Must re-run build script after new procurement data is ingested. 15 suspect reg numbers flagged as LLM extraction errors.

## ADR-006: Corporate groups via UR open data
- Date: 2026-03-23
- Decision: Detect sister companies via Latvia's UR open data (shared registered address, shared beneficial owner).
- Why: Holding structures split contract wins across multiple legal entities; groups reveal true concentration.
- Trade-off: UR data has rate limits; cached in `ur_cache` table. Manual verification still needed for borderline cases.

## ADR-008: JSONL pipeline outputs are disposable intermediates
- Date: 2026-03-24
- Decision: Treat `data/**/*.jsonl` files as ephemeral pipeline artefacts, not as source of truth. SQLite is canonical. Test and partial-run files are deleted when no longer needed.
- Why: JSONL files accumulate quickly (test batches, partial runs, intermediate steps) and become misleading. The database holds the complete, deduplicated, queryable state.
- Trade-off: Full re-extraction is needed if the database is lost. Mitigated by keeping `data/construction_2023_2025/reports/` (the downloaded source documents).

## ADR-007: EIS data portal as cross-check source
- Date: 2026-03-23
- Decision: Use `data.gov.lv` CKAN API directly for ad-hoc queries (company history, pre-2023 data, CPV verification).
- Why: Local DB covers only 2023–2025; data portal has full history from 2018. Also useful when local CPV filtering misses contracts with wrong codes in EIS.
- Notes: Results table has no CPV — must join with announcements table on `Iepirkuma_ID`. Both tables use Excel CSV escaping (`="value"`). See `docs/DATA_SOURCES.md`.

## ADR-009: Migrate from standalone scripts to FastAPI modular monolith
- Date: 2026-04-01
- Decision: Move all pipeline logic into a FastAPI backend following a modular monolith pattern (router → service → repository). Retire the standalone `pipelines/` scripts approach from ADR-001.
- Why: Needed a web UI, authenticated API, and shared infrastructure (logging, error handling, job queue, storage). A monolith avoids premature service splitting while keeping modules independently testable.
- Trade-off: The extraction module is significantly larger than other modules and contains some pipeline-style scripts that don't fit cleanly into the router/service pattern. Accepted as a known exception.

## ADR-010: SQLite retained as read-only analytics database alongside PostgreSQL
- Date: 2026-04-01
- Decision: Keep the SQLite file (`database/eis_procurement_records.sqlite`) as the source for all analytics dashboards. PostgreSQL is used for users, auth, and raw CKAN sync tables. The two databases serve different roles and are not merged.
- Why: SQLite analytics DB is populated offline by the extraction pipeline and committed to the repo. This makes the dashboard work without running the full sync pipeline. PostgreSQL handles transactional data that must be consistent and multi-user.
- Trade-off: Two databases to reason about. Queries cannot join across them. The SQLite file must be manually updated and committed after each pipeline run.

## ADR-011: GPT-4o-mini as the default AI model
- Date: 2026-04-01
- Decision: Default `AI_MODEL=gpt-4o-mini` for both procurement classification and the chat assistant.
- Why: Sufficient quality for structured extraction tasks (CPV classification, work type, asset scale) and conversational search at significantly lower cost than GPT-4o. Model is configurable via `AI_MODEL` env var.
- Trade-off: Occasionally misclassifies ambiguous procurement titles. Mitigated by the hybrid heuristic fallback (ADR-003).

## ADR-012: Swappable job queue and storage backends
- Date: 2026-04-01
- Decision: Abstract the job queue behind `shared.jobs.queue` (inline vs Celery) and file storage behind `shared.storage.service` (local vs S3/MinIO). Modules never import concrete backends.
- Why: Local development must work without Redis or S3. Production can switch to Celery + S3 with only config changes, no module rewrites.
- Trade-off: One extra indirection layer. Inline job backend runs tasks synchronously in the request cycle, which blocks responses for long-running pipeline steps.

## ADR-013: Locale-prefixed frontend routes with server-side resolution
- Date: 2026-04-01
- Decision: All frontend routes are prefixed with locale (`/lv/...`, `/en/...`). Locale is resolved server-side in Next.js middleware from cookie → `X-Locale` header → `Accept-Language` → fallback `lv`. Backend resolves locale per-request from the same header chain.
- Why: Enables shareable locale-specific URLs and avoids client-side locale flicker. Keeps locale resolution logic consistent between frontend middleware and backend request context.
- Trade-off: All internal links must be locale-aware. Locale switching must rewrite the path prefix, not just set a cookie.

## ADR-014: Dual analytics routers (SQLite and PostgreSQL)
- Date: 2026-04-09
- Decision: Maintain two separate analytics routers — `router.py` (SQLite-backed) and `router_pg.py` (PostgreSQL-backed). Frontend has both `/dashboard` and `/dashboard-pg` pages.
- Why: PostgreSQL analytics are under active development. SQLite dashboard is stable and covers the committed dataset. Running both in parallel allows comparison and gradual migration without breaking the existing dashboard.
- Trade-off: Duplicate code in the two routers. `router_pg.py` has a known type error (`PostgresAnalyticsRepository` not assignable to `AnalyticsRepository` base class) that should be resolved by defining a proper shared interface.
