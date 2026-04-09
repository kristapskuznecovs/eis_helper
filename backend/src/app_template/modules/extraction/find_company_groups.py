#!/usr/bin/env python3
"""Find and assign corporate groups among companies with similar names.

Data sources (all free, no auth):
  - UR CKAN API  — address per company by reg number
  - UR beneficial_owners.csv — UBO names (downloaded once, queried locally)

Strategy:
  1. Find candidate pairs with fuzzy score 65-87 (similar names, different reg numbers)
  2. Fetch address from UR CKAN API for each unique reg number (cached in DB)
  3. Download beneficial_owners.csv once and load into memory
  4. Group by: same address > same UBO > same municipality+NACE (suspected)
  5. Write company_groups rows and update companies.group_id

Usage:
    PYTHONPATH=. python3 pipelines/find_company_groups.py
    PYTHONPATH=. python3 pipelines/find_company_groups.py --dry-run --min-wins 2
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sqlite3
import time
import unicodedata
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from .collector_companies import fuzzy_score, normalize_for_matching

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("database/eis_procurement_records.sqlite")

UR_CKAN_URL = (
    "https://data.gov.lv/dati/api/3/action/datastore_search"
    "?resource_id=25e80bf3-f107-4ab4-89ef-251b5b9374e9"
    "&filters={{\"regcode\":{reg}}}"
)
UR_UBO_URL = (
    "https://data.gov.lv/dati/dataset/b7848ab9-7886-4df0-8bc6-70052a8d9e1a"
    "/resource/20a9b26d-d056-4dbb-ae18-9ff23c87bdee/download/beneficial_owners.csv"
)

FETCH_DELAY = 0.3   # UR API is fast and official — no need for long delays
FUZZY_LOW = 65
FUZZY_HIGH = 87


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class CompanyInfo:
    reg: str
    name: str = ""
    address: str = ""
    address_norm: str = ""
    municipality: str = ""
    error: str = ""


# ── Normalisation helpers ─────────────────────────────────────────────────────

def _norm_address(addr: str) -> str:
    addr = unicodedata.normalize("NFKD", addr.lower())
    addr = "".join(ch for ch in addr if not unicodedata.combining(ch))
    addr = addr.replace(".", " ").replace(",", " ")
    # strip postal codes like LV-4701
    import re
    addr = re.sub(r"\blv-?\d{4}\b", "", addr)
    return re.sub(r"\s+", " ", addr).strip()


def _municipality(address: str) -> str:
    """First component of UR address string (city or district)."""
    parts = [p.strip() for p in address.split(",") if p.strip()]
    return parts[0] if parts else ""


# ── UR CKAN API ───────────────────────────────────────────────────────────────

def fetch_ur_company(reg: str) -> CompanyInfo:
    info = CompanyInfo(reg=reg)
    url = UR_CKAN_URL.format(reg=reg)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read())
        records = data.get("result", {}).get("records", [])
        if not records:
            info.error = "not found"
            return info
        r = records[0]
        info.name = r.get("name", "")
        info.address = r.get("address", "")
        info.address_norm = _norm_address(info.address)
        info.municipality = _municipality(info.address)
    except Exception as e:
        info.error = str(e)
    return info


# ── Beneficial owners CSV ─────────────────────────────────────────────────────

def download_ubo_map() -> Dict[str, List[str]]:
    """Download beneficial_owners.csv and return {reg -> [full_name, ...]}."""
    log.info("Downloading beneficial_owners.csv from data.gov.lv...")
    req = urllib.request.Request(UR_UBO_URL, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", errors="replace")
    ubo_map: Dict[str, List[str]] = defaultdict(list)
    reader = csv.DictReader(io.StringIO(raw), delimiter=";")
    for row in reader:
        reg = str(row.get("legal_entity_registration_number", "")).strip()
        forename = row.get("forename", "").strip()
        surname = row.get("surname", "").strip()
        if reg and (forename or surname):
            ubo_map[reg].append(f"{forename} {surname}".strip())
    log.info("  Loaded UBO data for %d companies", len(ubo_map))
    return dict(ubo_map)


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _ensure_cache(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ur_cache (
            reg          TEXT PRIMARY KEY,
            name         TEXT,
            address      TEXT,
            address_norm TEXT,
            municipality TEXT,
            error        TEXT
        )
    """)
    conn.commit()


def _load_cache(conn: sqlite3.Connection) -> Dict[str, CompanyInfo]:
    cache: Dict[str, CompanyInfo] = {}
    try:
        for row in conn.execute("SELECT * FROM ur_cache"):
            info = CompanyInfo(
                reg=row["reg"], name=row["name"] or "", address=row["address"] or "",
                address_norm=row["address_norm"] or "", municipality=row["municipality"] or "",
                error=row["error"] or "",
            )
            cache[row["reg"]] = info
    except sqlite3.OperationalError:
        pass
    return cache


def _save_cache(conn: sqlite3.Connection, info: CompanyInfo) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO ur_cache (reg, name, address, address_norm, municipality, error) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (info.reg, info.name, info.address, info.address_norm, info.municipality, info.error),
    )
    conn.commit()


# ── Candidate pairs ───────────────────────────────────────────────────────────

def find_candidate_pairs(
    conn: sqlite3.Connection, min_wins: int
) -> List[Tuple[float, int, int, str, str, str, str, int, int]]:
    rows = conn.execute("""
        SELECT c.id, c.canonical_name, c.registration_no,
               COUNT(r.procurement_record_key) as wins
        FROM companies c
        JOIN procurement_records r ON r.winner_company_id = c.id
        WHERE c.source = 'reg_no'
        GROUP BY c.id
        HAVING wins >= ?
        ORDER BY wins DESC
    """, (min_wins,)).fetchall()

    companies = [
        (r["id"], r["canonical_name"], r["registration_no"],
         normalize_for_matching(r["canonical_name"]), r["wins"])
        for r in rows
    ]
    log.info("Finding candidate pairs among %d companies (min_wins=%d)...",
             len(companies), min_wins)

    pairs = []
    for i in range(len(companies)):
        for j in range(i + 1, len(companies)):
            id_a, name_a, reg_a, norm_a, wins_a = companies[i]
            id_b, name_b, reg_b, norm_b, wins_b = companies[j]
            if len(norm_a) < 4 or len(norm_b) < 4:
                continue
            score = fuzzy_score(norm_a, norm_b)
            if FUZZY_LOW <= score <= FUZZY_HIGH:
                pairs.append((score, id_a, id_b, name_a, reg_a, name_b, reg_b, wins_a, wins_b))

    pairs.sort(reverse=True)
    log.info("Found %d candidate pairs (score %d-%d)", len(pairs), FUZZY_LOW, FUZZY_HIGH)
    return pairs


# ── Group source determination ────────────────────────────────────────────────

def group_source(
    reg_a: str, reg_b: str,
    cache: Dict[str, CompanyInfo],
    ubo_map: Dict[str, List[str]],
) -> Optional[Tuple[str, str]]:
    """Return (source, evidence_note) or None if no grouping signal."""
    info_a = cache.get(reg_a)
    info_b = cache.get(reg_b)

    # 1. Same address
    if info_a and info_b and info_a.address_norm and info_b.address_norm:
        if info_a.address_norm == info_b.address_norm:
            return ("same_address", f"{info_a.address}")

    # 2. Common beneficial owner
    ubos_a = set(ubo_map.get(reg_a, []))
    ubos_b = set(ubo_map.get(reg_b, []))
    common = ubos_a & ubos_b
    if common:
        return ("same_owner", f"Common UBO(s): {', '.join(sorted(common))}")

    # 3. Same municipality (first address component)
    if info_a and info_b and info_a.municipality and info_b.municipality:
        muni_a = _norm_address(info_a.municipality)
        muni_b = _norm_address(info_b.municipality)
        if muni_a == muni_b and muni_a:
            return ("suspected", f"Same municipality: {info_a.municipality}")

    return None


# ── Union-find ────────────────────────────────────────────────────────────────

class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[int, int] = {}
        self.edge_source: Dict[Tuple[int, int], Tuple[str, str]] = {}

    def find(self, x: int) -> int:
        self.parent.setdefault(x, x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x: int, y: int, source: str, note: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[rx] = ry
        key = (min(x, y), max(x, y))
        # Keep strongest source
        priority = {"same_address": 3, "same_owner": 2, "suspected": 1}
        existing = self.edge_source.get(key)
        if not existing or priority.get(source, 0) > priority.get(existing[0], 0):
            self.edge_source[key] = (source, note)

    def groups(self) -> Dict[int, List[int]]:
        result: Dict[int, List[int]] = {}
        for x in self.parent:
            root = self.find(x)
            result.setdefault(root, []).append(x)
        return {k: sorted(v) for k, v in result.items() if len(v) > 1}

    def best_source_for_group(self, member_ids: List[int]) -> Tuple[str, str]:
        """Return the strongest (source, note) among all edges in this group."""
        priority = {"same_address": 3, "same_owner": 2, "suspected": 1}
        best = ("suspected", "")
        for i in range(len(member_ids)):
            for j in range(i + 1, len(member_ids)):
                key = (min(member_ids[i], member_ids[j]), max(member_ids[i], member_ids[j]))
                edge = self.edge_source.get(key)
                if edge and priority.get(edge[0], 0) > priority.get(best[0], 0):
                    best = edge
        return best


# ── Main ──────────────────────────────────────────────────────────────────────

def run(db_path: Path, min_wins: int, dry_run: bool) -> None:
    if dry_run:
        log.info("DRY RUN — no DB writes")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if not dry_run:
        _ensure_cache(conn)

    pairs = find_candidate_pairs(conn, min_wins)

    # Unique reg numbers needed
    needed: Set[str] = set()
    for _, id_a, id_b, name_a, reg_a, name_b, reg_b, wins_a, wins_b in pairs:
        needed.add(reg_a)
        needed.add(reg_b)

    # Load existing cache (lursoft_cache or ur_cache)
    cache = _load_cache(conn)

    # Migrate any existing lursoft_cache entries
    try:
        lc_rows = conn.execute(
            "SELECT reg, name, address, address_norm, municipality, error FROM lursoft_cache"
        ).fetchall()
        for row in lc_rows:
            if row["reg"] not in cache:
                info = CompanyInfo(
                    reg=row["reg"], name=row["name"] or "", address=row["address"] or "",
                    address_norm=row["address_norm"] or "", municipality=row["municipality"] or "",
                    error=row["error"] or "",
                )
                cache[row["reg"]] = info
        if lc_rows:
            log.info("Migrated %d entries from lursoft_cache", len(lc_rows))
    except sqlite3.OperationalError:
        pass

    # Fetch missing from UR CKAN API
    to_fetch = sorted(needed - set(cache.keys()))
    log.info("Fetching %d companies from UR CKAN API...", len(to_fetch))
    for i, reg in enumerate(to_fetch):
        info = fetch_ur_company(reg)
        if info.error:
            log.warning("  [%d/%d] %s — %s", i + 1, len(to_fetch), reg, info.error)
        else:
            log.info("  [%d/%d] %s | %s", i + 1, len(to_fetch), reg, info.address)
        cache[reg] = info
        if not dry_run:
            _save_cache(conn, info)
        time.sleep(FETCH_DELAY)

    # Download UBO map
    ubo_map = download_ubo_map()

    # Save UBO data for our companies to DB
    if not dry_run:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS company_ubos (
                    reg   TEXT NOT NULL,
                    name  TEXT NOT NULL,
                    PRIMARY KEY (reg, name)
                )
            """)
            our_regs = {r for r in needed if r in ubo_map}
            for reg in our_regs:
                for ubo_name in ubo_map[reg]:
                    conn.execute(
                        "INSERT OR IGNORE INTO company_ubos (reg, name) VALUES (?, ?)",
                        (reg, ubo_name)
                    )
            conn.commit()
        except Exception as e:
            log.warning("Could not save UBO data: %s", e)

    # Build groups using union-find
    uf = UnionFind()
    for score, id_a, id_b, name_a, reg_a, name_b, reg_b, wins_a, wins_b in pairs:
        result = group_source(reg_a, reg_b, cache, ubo_map)
        if result:
            source, note = result
            uf.union(id_a, id_b, source, note)

    groups = uf.groups()
    log.info("\nFound %d groups from %d pairs", len(groups), len(pairs))

    # Existing confirmed groups (same_owner from prior runs) — don't overwrite
    confirmed_group_ids: Set[int] = set(
        row["id"] for row in conn.execute(
            "SELECT id FROM company_groups WHERE source = 'same_owner'"
        )
    )
    existing_company_groups: Dict[int, int] = {
        row["id"]: row["group_id"]
        for row in conn.execute(
            "SELECT id, group_id FROM companies WHERE group_id IS NOT NULL"
        )
    }

    created = 0
    for root, member_ids in sorted(groups.items(), key=lambda x: -len(x[1])):
        member_rows = [
            conn.execute(
                "SELECT canonical_name, registration_no FROM companies WHERE id = ?", (mid,)
            ).fetchone()
            for mid in member_ids
        ]
        names = [r["canonical_name"] for r in member_rows if r]
        regs  = [r["registration_no"] for r in member_rows if r]

        best_source, best_note = uf.best_source_for_group(member_ids)

        # Skip if all members already have a same_owner group
        if all(existing_company_groups.get(mid) in confirmed_group_ids for mid in member_ids):
            continue

        # Build group name
        group_name = min(names, key=len) if names else "Unknown"

        # Build notes
        details = []
        for reg, name in zip(regs, names):
            info = cache.get(reg)
            addr = info.address if info else "?"
            ubos = ", ".join(ubo_map.get(reg, [])) or "—"
            details.append(f"{name} ({reg}): {addr} | UBO: {ubos}")
        notes = best_note + " | " + "; ".join(details)

        log.info(
            "  [%s] %r — %d members: %s",
            best_source, group_name, len(member_ids),
            ", ".join(f"{n} ({r})" for n, r in zip(names, regs)),
        )
        if best_note:
            log.info("    Evidence: %s", best_note)

        if not dry_run:
            # Don't overwrite existing confirmed (same_owner) group memberships
            skip = any(
                existing_company_groups.get(mid) in confirmed_group_ids
                for mid in member_ids
            )
            if skip:
                log.info("    Skipping — member already in confirmed group")
                continue

            conn.execute(
                "INSERT INTO company_groups (name, source, notes) VALUES (?, ?, ?)",
                (group_name, best_source, notes),
            )
            gid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            for mid in member_ids:
                conn.execute("UPDATE companies SET group_id = ? WHERE id = ?", (gid, mid))
            conn.commit()
            created += 1

    log.info("\nCreated %d new groups.", created)

    # Final summary
    if not dry_run:
        rows = conn.execute("""
            SELECT g.name, g.source, COUNT(c.id) as members,
                   COALESCE(SUM(wins.cnt), 0) as total_wins
            FROM company_groups g
            JOIN companies c ON c.group_id = g.id
            LEFT JOIN (
                SELECT winner_company_id, COUNT(*) as cnt
                FROM procurement_records GROUP BY winner_company_id
            ) wins ON wins.winner_company_id = c.id
            GROUP BY g.id
            ORDER BY total_wins DESC
        """).fetchall()

        log.info("\nAll groups (by wins):")
        log.info("  %-42s %-14s %7s %7s", "Name", "Source", "Members", "Wins")
        log.info("  " + "-" * 76)
        for r in rows:
            log.info("  %-42s %-14s %7d %7d",
                     r["name"][:42], r["source"], r["members"], r["total_wins"])

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find corporate groups via UR open data (address + UBO)"
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--min-wins", type=int, default=2,
                        help="Min wins for a company to be included (default 2)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch data and print groups without writing to DB")
    args = parser.parse_args()
    run(args.db_path, args.min_wins, args.dry_run)


if __name__ == "__main__":
    main()
