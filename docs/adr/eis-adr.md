# Architecture Decision Record (ADR)

Current as of 2026-03-24.

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
