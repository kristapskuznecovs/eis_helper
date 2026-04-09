#!/usr/bin/env python3
"""Build the companies canonical index for reliable winner statistics.

Phases:
  1. Collect all (winner_name, reg_no) pairs from procurement_records
  2. Detect suspect reg numbers (LLM bleed-across errors)
  3. Anchor companies by valid registration numbers
  4. Build fuzzy lookup index
  5. Fuzzy-match names without reg numbers
  6. Update winner_company_id on procurement_records
  7. Print summary

Usage:
    PYTHONPATH=. python3 pipelines/build_company_index.py
    PYTHONPATH=. python3 pipelines/build_company_index.py --threshold 85 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from .collector_companies import (
    FUZZY_MATCH_THRESHOLD,
    build_canonical_name,
    fuzzy_score,
    is_plausible_company_name,
    normalize_for_matching,
    normalize_reg_no,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("database/eis_procurement_records.sqlite")

# A reg_no group is suspect if avg pairwise fuzzy similarity of core names < this
SUSPECT_SIMILARITY_THRESHOLD = 50
# Only check suspect heuristic for groups larger than this
SUSPECT_MIN_GROUP_SIZE = 4


def _create_companies_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name   TEXT NOT NULL,
            registration_no  TEXT UNIQUE,
            source           TEXT NOT NULL,
            match_confidence REAL,
            aliases_json     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_companies_reg_no ON companies(registration_no)
            WHERE registration_no IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_companies_canonical ON companies(canonical_name);
    """)
    # Add winner_company_id to procurement_records if missing
    existing = {row[1] for row in conn.execute("PRAGMA table_info(procurement_records)")}
    if "winner_company_id" not in existing:
        conn.execute(
            "ALTER TABLE procurement_records ADD COLUMN winner_company_id INTEGER REFERENCES companies(id)"
        )
    conn.commit()


def _is_suspect_group(names: set[str], threshold: int = SUSPECT_SIMILARITY_THRESHOLD) -> bool:
    """Return True if the name group looks like an LLM extraction error."""
    if len(names) < SUSPECT_MIN_GROUP_SIZE:
        return False
    cores = [normalize_for_matching(n) for n in names]
    pairs = list(combinations(cores, 2))
    if not pairs:
        return False
    avg_sim = sum(fuzzy_score(a, b) for a, b in pairs) / len(pairs)
    return avg_sim < threshold


def _upsert_company(
    conn: sqlite3.Connection,
    canonical_name: str,
    registration_no: str | None,
    source: str,
    match_confidence: float | None,
    aliases: list[str],
    dry_run: bool,
) -> int:
    """Insert or update a company row. Returns the row id."""
    aliases_json = json.dumps(sorted(set(aliases)), ensure_ascii=False)
    if dry_run:
        # Return a fake id for dry-run bookkeeping
        return -1

    if registration_no:
        conn.execute(
            """
            INSERT INTO companies (canonical_name, registration_no, source, match_confidence, aliases_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(registration_no) DO UPDATE SET
                canonical_name   = excluded.canonical_name,
                source           = excluded.source,
                match_confidence = excluded.match_confidence,
                aliases_json     = excluded.aliases_json
            """,
            (canonical_name, registration_no, source, match_confidence, aliases_json),
        )
        row = conn.execute(
            "SELECT id FROM companies WHERE registration_no = ?", (registration_no,)
        ).fetchone()
    else:
        conn.execute(
            """
            INSERT INTO companies (canonical_name, registration_no, source, match_confidence, aliases_json)
            VALUES (?, NULL, ?, ?, ?)
            """,
            (canonical_name, source, match_confidence, aliases_json),
        )
        row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return row[0]


def _add_alias_to_company(
    conn: sqlite3.Connection, company_id: int, new_alias: str, dry_run: bool
) -> None:
    if dry_run:
        return
    row = conn.execute(
        "SELECT aliases_json FROM companies WHERE id = ?", (company_id,)
    ).fetchone()
    if not row:
        return
    aliases = json.loads(row[0])
    if new_alias not in aliases:
        aliases.append(new_alias)
        conn.execute(
            "UPDATE companies SET aliases_json = ? WHERE id = ?",
            (json.dumps(sorted(aliases), ensure_ascii=False), company_id),
        )


def build_index(db_path: Path, threshold: int, dry_run: bool) -> None:
    if dry_run:
        log.info("DRY RUN — no writes will be made")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ── Phase 0: ensure schema ────────────────────────────────────────────────
    if not dry_run:
        _create_companies_table(conn)
    else:
        log.info("Phase 0: skipping schema creation (dry run)")

    # ── Phase 1: collect names ────────────────────────────────────────────────
    log.info("Phase 1: collecting winner names...")
    rows = conn.execute(
        "SELECT procurement_winner, procurement_winner_registration_no "
        "FROM procurement_records WHERE procurement_winner IS NOT NULL"
    ).fetchall()

    reg_to_names: dict[str, set[str]] = defaultdict(set)
    no_reg_names: list[str] = []

    for row in rows:
        name = row["procurement_winner"].strip()
        if not name:
            continue
        reg = normalize_reg_no(row["procurement_winner_registration_no"])
        if reg:
            reg_to_names[reg].add(name)
        else:
            no_reg_names.append(name)

    log.info(
        "  %d reg-anchored groups, %d names without reg number",
        len(reg_to_names),
        len(no_reg_names),
    )

    # ── Phase 2: detect suspect reg numbers ───────────────────────────────────
    log.info("Phase 2: detecting suspect reg numbers...")
    valid_reg_groups: dict[str, set[str]] = {}
    suspect_reg_groups: dict[str, set[str]] = {}

    for reg, names in reg_to_names.items():
        if _is_suspect_group(names):
            suspect_reg_groups[reg] = names
            log.warning(
                "  SUSPECT reg_no %s → %d names: %s",
                reg,
                len(names),
                ", ".join(sorted(names)[:5]),
            )
        else:
            valid_reg_groups[reg] = names

    log.info(
        "  %d valid groups, %d suspect groups",
        len(valid_reg_groups),
        len(suspect_reg_groups),
    )

    # ── Phase 3: anchor by registration number ────────────────────────────────
    log.info("Phase 3: anchoring companies by registration number...")
    reg_to_id: dict[str, int] = {}

    for reg, names in valid_reg_groups.items():
        canonical = build_canonical_name(sorted(names))
        company_id = _upsert_company(
            conn, canonical, reg, "reg_no", None, sorted(names), dry_run
        )
        reg_to_id[reg] = company_id
        if dry_run:
            log.info("  [dry] reg_no %s → %r (%d aliases)", reg, canonical, len(names))

    for _reg, names in suspect_reg_groups.items():
        # Each name becomes its own standalone entry (no merging)
        for name in names:
            canonical = build_canonical_name([name])
            _upsert_company(conn, canonical, None, "reg_no_suspect", None, [name], dry_run)

    if not dry_run:
        conn.commit()
    log.info("  %d companies anchored by reg number", len(reg_to_id))

    # ── Phase 4: build fuzzy lookup index ─────────────────────────────────────
    log.info("Phase 4: building fuzzy lookup index...")
    if not dry_run:
        known_rows = conn.execute(
            "SELECT id, canonical_name, aliases_json FROM companies"
        ).fetchall()
    else:
        known_rows = []

    # known: list of (id, canonical_name, norm_key, set_of_alias_norms)
    known: list[tuple[int, str, str, set[str]]] = []
    for r in known_rows:
        aliases = json.loads(r["aliases_json"])
        alias_norms = {normalize_for_matching(a) for a in aliases}
        norm_key = normalize_for_matching(r["canonical_name"])
        known.append((r["id"], r["canonical_name"], norm_key, alias_norms))

    # ── Phase 5: fuzzy-match names without reg numbers ────────────────────────
    log.info("Phase 5: fuzzy-matching %d names without reg numbers...", len(no_reg_names))
    matched = 0
    inserted = 0
    skipped = 0

    # Deduplicate no_reg_names while preserving all distinct strings
    seen_no_reg: set[str] = set()
    unique_no_reg: list[str] = []
    for n in no_reg_names:
        if n not in seen_no_reg:
            seen_no_reg.add(n)
            unique_no_reg.append(n)

    for name in unique_no_reg:
        norm_key = normalize_for_matching(name)
        if len(norm_key) < 4:
            skipped += 1
            continue

        # Check if this name already exists as an alias in known
        already_known = any(norm_key in entry[3] for entry in known)
        if already_known:
            skipped += 1
            continue

        # Find best match
        best_id: int | None = None
        best_score: float = 0.0
        best_name: str = ""
        for cid, cname, cnorm, _alias_norms in known:
            score = fuzzy_score(norm_key, cnorm)
            if score > best_score:
                best_score = score
                best_id = cid
                best_name = cname

        if best_score >= threshold and best_id is not None:
            # Add as alias to existing company
            _add_alias_to_company(conn, best_id, name, dry_run)
            # Update alias_norms in known
            for i, entry in enumerate(known):
                if entry[0] == best_id:
                    updated_norms = entry[3] | {norm_key}
                    known[i] = (entry[0], entry[1], entry[2], updated_norms)
                    break
            matched += 1
            if dry_run:
                log.info(
                    "  [dry] fuzzy match (%.0f): %r → %r", best_score, name, best_name
                )
        else:
            if not is_plausible_company_name(name):
                skipped += 1
                continue
            # Insert as new standalone/fuzzy entry
            source = "fuzzy_match" if best_score > 0 else "standalone"
            confidence = best_score if best_score > 0 else None
            company_id = _upsert_company(
                conn, build_canonical_name([name]), None, source, confidence, [name], dry_run
            )
            if not dry_run:
                alias_norms: set[str] = {norm_key}
                known.append((company_id, name, norm_key, alias_norms))
            inserted += 1

    if not dry_run:
        conn.commit()
    log.info(
        "  matched=%d  inserted=%d  skipped=%d", matched, inserted, skipped
    )

    # ── Phase 6: update winner_company_id on procurement_records ─────────────
    log.info("Phase 6: updating winner_company_id on procurement_records...")

    if not dry_run:
        # Reload company data fresh after all inserts
        company_rows = conn.execute(
            "SELECT id, registration_no, aliases_json FROM companies"
        ).fetchall()
        reg_to_id_final: dict[str, int] = {}
        alias_norm_to_id: dict[str, int] = {}
        for cr in company_rows:
            if cr["registration_no"]:
                reg_to_id_final[cr["registration_no"]] = cr["id"]
            aliases = json.loads(cr["aliases_json"])
            for alias in aliases:
                nk = normalize_for_matching(alias)
                if nk and nk not in alias_norm_to_id:
                    alias_norm_to_id[nk] = cr["id"]

        records = conn.execute(
            "SELECT procurement_record_key, procurement_winner, procurement_winner_registration_no "
            "FROM procurement_records WHERE procurement_winner IS NOT NULL"
        ).fetchall()

        updates: list[tuple[int, str]] = []
        unresolved = 0
        for rec in records:
            reg = normalize_reg_no(rec["procurement_winner_registration_no"])
            company_id = None
            if reg:
                company_id = reg_to_id_final.get(reg)
            if company_id is None:
                nk = normalize_for_matching(rec["procurement_winner"])
                company_id = alias_norm_to_id.get(nk)
            if company_id is not None:
                updates.append((company_id, rec["procurement_record_key"]))
            else:
                unresolved += 1

        conn.executemany(
            "UPDATE procurement_records SET winner_company_id = ? WHERE procurement_record_key = ?",
            updates,
        )
        conn.commit()
        log.info(
            "  updated %d records, %d unresolved", len(updates), unresolved
        )
    else:
        log.info("  [dry] skipping winner_company_id updates")

    # ── Phase 7: summary ──────────────────────────────────────────────────────
    log.info("Phase 7: summary")
    if not dry_run:
        source_counts = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM companies GROUP BY source"
        ).fetchall()
        total_companies = sum(r["cnt"] for r in source_counts)
        log.info("  Total companies: %d", total_companies)
        for r in source_counts:
            log.info("    %-20s %d", r["source"], r["cnt"])

        with_id, total_w = conn.execute(
            "SELECT SUM(CASE WHEN winner_company_id IS NOT NULL THEN 1 END), "
            "SUM(CASE WHEN procurement_winner IS NOT NULL THEN 1 END) "
            "FROM procurement_records"
        ).fetchone()
        log.info(
            "  winner_company_id coverage: %d / %d (%.0f%%)",
            with_id or 0,
            total_w or 0,
            100.0 * (with_id or 0) / max(total_w or 1, 1),
        )

        top_aliases = conn.execute(
            "SELECT canonical_name, json_array_length(aliases_json) as n "
            "FROM companies ORDER BY n DESC LIMIT 10"
        ).fetchall()
        log.info("  Top 10 by alias count:")
        for r in top_aliases:
            log.info("    %3d  %s", r["n"], r["canonical_name"])

        if suspect_reg_groups:
            log.warning("  Suspect reg numbers (%d):", len(suspect_reg_groups))
            for reg in sorted(suspect_reg_groups):
                log.warning("    %s", reg)

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build company canonical index")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=FUZZY_MATCH_THRESHOLD,
        help="Fuzzy match threshold (0-100, default %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing to DB",
    )
    args = parser.parse_args()
    build_index(args.db_path, args.threshold, args.dry_run)


if __name__ == "__main__":
    main()
