from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "psycopg is required. Run this script via `cd backend && uv run python ../scripts/compare_sqlite_postgres_procurements.py`."
    ) from exc


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQLITE_PATH = ROOT / "database" / "eis_procurement_records.sqlite"
DEFAULT_OUTPUT_DIR = ROOT / "tmp" / "compare_sqlite_postgres"
ENV_PATHS = [ROOT / ".env", ROOT / "backend" / ".env"]

COMPARABLE_FIELDS = [
    "procurement_name",
    "purchaser_name",
    "procurement_status",
    "procurement_identification_number",
    "procurement_subject_type",
    "cpv_main",
    "cpv_additional",
    "estimated_value_eur",
    "procedure_type",
    "submission_date",
    "bid_deadline",
    "winner_selection_method",
    "evaluation_method",
    "eu_project_reference",
    "has_lots",
    "procurement_winner",
    "procurement_winner_registration_no",
    "procurement_winner_suggested_price_eur",
    "decision_date",
    "procurement_participants_count",
    "procurement_participants_json",
]

BUSINESS_FIELDS = [
    "purchaser_name",
    "procurement_status",
    "submission_date",
    "estimated_value_eur",
    "is_multi_lot",
    "lot_count",
    "lots_json",
    "has_lots",
    "lot_number",
    "lot_name",
    "decision_date",
    "procurement_winner",
    "procurement_winner_registration_no",
    "procurement_winner_suggested_price_eur",
    "winner_count",
    "winners_json",
    "procurement_participants_count",
    "procurement_participants_json",
]

NAME_FIELDS = {
    "purchaser_name",
    "procurement_winner",
}

FLOAT_FIELDS = {
    "estimated_value_eur",
    "procurement_winner_suggested_price_eur",
}

BOOL_FIELDS = {
    "is_multi_lot",
    "has_lots",
}

PARTY_TOKENS = (
    "SIA",
    "AS",
    "PSIA",
    "PAŠVALDĪBAS",
    "SABIEDRĪBA AR IEROBEŽOTU ATBILDĪBU",
)


@dataclass
class CompareResult:
    procurement_id: str
    status: str
    field: str
    sqlite_value: Any
    postgres_value: Any


@dataclass
class ProcurementComparisonRow:
    procurement_id: str
    exists_in_sqlite: bool
    exists_in_postgres: bool
    overall_status: str
    sqlite_values: dict[str, Any]
    postgres_values: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare SQLite procurement_records rows against normalized Postgres procurements data."
    )
    parser.add_argument("--sqlite-path", type=Path, default=DEFAULT_SQLITE_PATH)
    parser.add_argument("--postgres-dsn", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="Limit number of SQLite procurements to compare.")
    parser.add_argument(
        "--procurement-id",
        action="append",
        dest="procurement_ids",
        default=None,
        help="Compare only the given procurement_id. Can be repeated.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of procurement IDs to fetch from Postgres per batch.",
    )
    parser.add_argument(
        "--show-matches",
        action="store_true",
        help="Also write per-field exact matches to the CSV output.",
    )
    parser.add_argument(
        "--field-set",
        choices=["all", "business"],
        default="all",
        help="Choose which fields to compare.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def resolve_postgres_dsn(explicit_dsn: str | None) -> str:
    if explicit_dsn:
        return explicit_dsn
    for env_path in ENV_PATHS:
        load_env_file(env_path)
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL not found in environment, .env, or backend/.env")
    return to_psycopg_dsn(dsn)


def to_psycopg_dsn(dsn: str) -> str:
    if "://" not in dsn:
        return dsn
    parsed = urlsplit(dsn)
    scheme = parsed.scheme.replace("+psycopg", "")
    return urlunsplit((scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment))


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\u201c", '"').replace("\u201d", '"').replace("„", '"').replace("”", '"')
    return normalize_whitespace(text)


def normalize_party_name(value: Any) -> str | None:
    text = normalize_string(value)
    if text is None:
        return None
    cleaned = text.upper()
    for token in PARTY_TOKENS:
        cleaned = cleaned.replace(f"{token} ", "")
        cleaned = cleaned.replace(f" {token}", "")
    cleaned = cleaned.replace('"', "").replace("'", "").replace(".", " ").replace(",", " ")
    cleaned = normalize_whitespace(cleaned)
    return cleaned or None


def normalize_number(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        quantized = Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return normalize_string(value)
    return format(quantized, "f")


def normalize_bool_like(value: Any) -> bool | None:
    text = normalize_string(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "jā"}:
        return True
    if lowered in {"0", "false", "no", "nē"}:
        return False
    return None


def normalize_participants(value: Any) -> list[dict[str, str | None]] | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            return [{"name": normalize_party_name(value), "registration_no": None}]
    else:
        raw = value
    if not isinstance(raw, list):
        return None

    normalized: list[dict[str, str | None]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("participant_name")
        reg = (
            item.get("registration_no")
            or item.get("reg_no")
            or item.get("participant_reg_number")
        )
        normalized.append(
            {
                "name": normalize_party_name(name),
                "registration_no": normalize_string(reg),
            }
        )

    normalized.sort(key=lambda item: ((item["registration_no"] or ""), (item["name"] or "")))
    return normalized


def canonicalize(field: str, value: Any) -> Any:
    if field == "procurement_participants_json":
        return normalize_participants(value)
    if field == "winners_json":
        return normalize_winners(value)
    if field in BOOL_FIELDS:
        parsed = normalize_bool_like(value)
        return parsed if parsed is not None else normalize_string(value)
    if field in FLOAT_FIELDS:
        return normalize_number(value)
    if field in NAME_FIELDS:
        return normalize_party_name(value)
    return normalize_string(value)


def normalize_winners(value: Any) -> list[dict[str, str | None]] | None:
    if value in (None, "", "null"):
        return None
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            return [{"name": normalize_party_name(value), "registration_no": None, "value_eur": None}]
    else:
        raw = value
    if not isinstance(raw, list):
        return None

    normalized: list[dict[str, str | None]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "name": normalize_party_name(item.get("name") or item.get("winner_name")),
                "registration_no": normalize_string(
                    item.get("registration_no") or item.get("winner_reg_number")
                ),
                "value_eur": normalize_number(
                    item.get("value_eur") or item.get("contract_value_eur")
                ),
            }
        )

    normalized.sort(
        key=lambda item: (
            item["registration_no"] or "",
            item["name"] or "",
            item["value_eur"] or "",
        )
    )
    return normalized


def load_sqlite_rows(
    sqlite_path: Path,
    *,
    fields: list[str],
    procurement_ids: list[str] | None,
    limit: int | None,
) -> dict[str, dict[str, Any]]:
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite database not found: {sqlite_path}")

    with sqlite3.connect(sqlite_path) as conn:
        existing_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(procurement_records)").fetchall()
        }

    select_parts = ["procurement_id"]
    for field in fields:
        if field in existing_columns:
            select_parts.append(field)
        else:
            select_parts.append(f"NULL AS {field}")

    query = f"""
        SELECT {", ".join(select_parts)}
        FROM procurement_records
        WHERE procurement_id IS NOT NULL
    """
    params: list[Any] = []
    if procurement_ids:
        placeholders = ", ".join("?" for _ in procurement_ids)
        query += f" AND procurement_id IN ({placeholders})"
        params.extend(procurement_ids)
    query += " ORDER BY procurement_id"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    return {str(row["procurement_id"]): dict(row) for row in rows}


def batched(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def fetch_postgres_rows(dsn: str, procurement_ids: list[str], batch_size: int) -> dict[str, dict[str, Any]]:
    sql = """
        SELECT
            p.procurement_id,
            p.title AS procurement_name,
            p.buyer AS purchaser_name,
            p.status AS procurement_status,
            p.identification_number AS procurement_identification_number,
            p.subject_type AS procurement_subject_type,
            p.cpv_main,
            p.cpv_additional,
            p.estimated_value_eur,
            p.procedure_type,
            p.submission_deadline AS submission_date,
            p.submission_deadline AS bid_deadline,
            p.winner_selection_method,
            p.winner_selection_method AS evaluation_method,
            p.eu_project_reference,
            CASE WHEN p.has_lots = 'Jā' THEN true WHEN p.has_lots = 'Nē' THEN false ELSE NULL END AS has_lots,
            CASE WHEN p.has_lots = 'Jā' THEN true WHEN p.has_lots = 'Nē' THEN false ELSE NULL END AS is_multi_lot,
            p.lot_number,
            p.lot_name,
            r.winner_name AS procurement_winner,
            r.winner_reg_number AS procurement_winner_registration_no,
            r.contract_value_eur AS procurement_winner_suggested_price_eur,
            r.contract_signed_date AS decision_date,
            wc.winner_count AS winner_count,
            wj.winners_json AS winners_json,
            pc.participant_count AS procurement_participants_count,
            pj.participants_json AS procurement_participants_json,
            NULL::int AS lot_count,
            NULL::jsonb AS lots_json
        FROM procurements p
        LEFT JOIN LATERAL (
            SELECT winner_name, winner_reg_number, contract_value_eur, contract_signed_date
            FROM ckan_results
            WHERE procurement_id = p.procurement_id
            ORDER BY contract_value_eur DESC NULLS LAST, winner_name NULLS LAST
            LIMIT 1
        ) r ON true
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS winner_count
            FROM ckan_results cr
            WHERE cr.procurement_id = p.procurement_id
        ) wc ON true
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', cr.winner_name,
                    'registration_no', cr.winner_reg_number,
                    'value_eur', cr.contract_value_eur
                )
                ORDER BY cr.winner_reg_number NULLS FIRST, cr.winner_name NULLS FIRST, cr.contract_value_eur NULLS FIRST
            ) AS winners_json
            FROM ckan_results cr
            WHERE cr.procurement_id = p.procurement_id
        ) wj ON true
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS participant_count
            FROM ckan_participants cp
            WHERE cp.procurement_id = p.procurement_id
        ) pc ON true
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'name', cp.participant_name,
                    'registration_no', cp.participant_reg_number
                )
                ORDER BY cp.participant_reg_number NULLS FIRST, cp.participant_name NULLS FIRST
            ) AS participants_json
            FROM ckan_participants cp
            WHERE cp.procurement_id = p.procurement_id
        ) pj ON true
        WHERE p.procurement_id = ANY(%(ids)s)
    """

    records: dict[str, dict[str, Any]] = {}
    with psycopg.connect(dsn) as conn:
        for batch in batched(procurement_ids, batch_size):
            with conn.cursor() as cur:
                cur.execute(sql, {"ids": batch})
                col_names = [desc.name for desc in cur.description]
                for raw in cur.fetchall():
                    row = dict(zip(col_names, raw))
                    records[str(row["procurement_id"])] = row
    return records


def compare_rows(
    sqlite_rows: dict[str, dict[str, Any]],
    postgres_rows: dict[str, dict[str, Any]],
    *,
    fields: list[str],
    show_matches: bool,
) -> tuple[list[CompareResult], list[ProcurementComparisonRow], Counter[str], Counter[str]]:
    results: list[CompareResult] = []
    procurement_rows: list[ProcurementComparisonRow] = []
    field_mismatch_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()

    for procurement_id, sqlite_row in sqlite_rows.items():
        postgres_row = postgres_rows.get(procurement_id)
        if postgres_row is None:
            procurement_rows.append(
                ProcurementComparisonRow(
                    procurement_id=procurement_id,
                    exists_in_sqlite=True,
                    exists_in_postgres=False,
                    overall_status="missing_in_postgres",
                    sqlite_values={field: canonicalize(field, sqlite_row.get(field)) for field in fields},
                    postgres_values={field: None for field in fields},
                )
            )
            results.append(
                CompareResult(
                    procurement_id=procurement_id,
                    status="missing_in_postgres",
                    field="*",
                    sqlite_value="present",
                    postgres_value=None,
                )
            )
            status_counts["missing_in_postgres"] += 1
            continue

        row_had_mismatch = False
        sqlite_values = {field: canonicalize(field, sqlite_row.get(field)) for field in fields}
        postgres_values = {field: canonicalize(field, postgres_row.get(field)) for field in fields}
        for field in fields:
            sqlite_value = sqlite_values[field]
            postgres_value = postgres_values[field]
            if sqlite_value != postgres_value:
                row_had_mismatch = True
                field_mismatch_counts[field] += 1
                status_counts["field_mismatch"] += 1
                results.append(
                    CompareResult(
                        procurement_id=procurement_id,
                        status="field_mismatch",
                        field=field,
                        sqlite_value=sqlite_value,
                        postgres_value=postgres_value,
                    )
                )
            elif show_matches:
                status_counts["match"] += 1
                results.append(
                    CompareResult(
                        procurement_id=procurement_id,
                        status="match",
                        field=field,
                        sqlite_value=sqlite_value,
                        postgres_value=postgres_value,
                    )
                )

        if not row_had_mismatch:
            status_counts["fully_matched_procurement"] += 1
        procurement_rows.append(
            ProcurementComparisonRow(
                procurement_id=procurement_id,
                exists_in_sqlite=True,
                exists_in_postgres=True,
                overall_status="field_mismatch" if row_had_mismatch else "match",
                sqlite_values=sqlite_values,
                postgres_values=postgres_values,
            )
        )

    sqlite_ids = set(sqlite_rows)
    extra_postgres_ids = sorted(set(postgres_rows) - sqlite_ids)
    for procurement_id in extra_postgres_ids:
        procurement_rows.append(
            ProcurementComparisonRow(
                procurement_id=procurement_id,
                exists_in_sqlite=False,
                exists_in_postgres=True,
                overall_status="extra_in_postgres",
                sqlite_values={field: None for field in fields},
                postgres_values={
                    field: canonicalize(field, postgres_rows[procurement_id].get(field)) for field in fields
                },
            )
        )
        results.append(
            CompareResult(
                procurement_id=procurement_id,
                status="extra_in_postgres",
                field="*",
                sqlite_value=None,
                postgres_value="present",
            )
        )
        status_counts["extra_in_postgres"] += 1

    return results, procurement_rows, field_mismatch_counts, status_counts


def write_outputs(
    output_dir: Path,
    fields: list[str],
    results: list[CompareResult],
    procurement_rows: list[ProcurementComparisonRow],
    summary: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    procurement_csv_path = output_dir / "procurement_comparison.csv"
    procurement_columns = ["procurement_id", "exists_in_sqlite", "exists_in_postgres", "overall_status"]
    for field in fields:
        procurement_columns.append(f"sqlite_{field}")
        procurement_columns.append(f"postgres_{field}")
    with procurement_csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(procurement_columns)
        for row in procurement_rows:
            data = [row.procurement_id, row.exists_in_sqlite, row.exists_in_postgres, row.overall_status]
            for field in fields:
                data.append(json.dumps(row.sqlite_values.get(field), ensure_ascii=False))
                data.append(json.dumps(row.postgres_values.get(field), ensure_ascii=False))
            writer.writerow(data)

    csv_path = output_dir / "field_comparison.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["procurement_id", "status", "field", "sqlite_value", "postgres_value"])
        for result in results:
            writer.writerow(
                [
                    result.procurement_id,
                    result.status,
                    result.field,
                    json.dumps(result.sqlite_value, ensure_ascii=False),
                    json.dumps(result.postgres_value, ensure_ascii=False),
                ]
            )

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def build_summary(
    sqlite_rows: dict[str, dict[str, Any]],
    postgres_rows: dict[str, dict[str, Any]],
    results: list[CompareResult],
    field_mismatch_counts: Counter[str],
    status_counts: Counter[str],
) -> dict[str, Any]:
    mismatched_ids = sorted(
        {
            result.procurement_id
            for result in results
            if result.status in {"field_mismatch", "missing_in_postgres"}
        }
    )
    return {
        "sqlite_procurements_compared": len(sqlite_rows),
        "postgres_procurements_loaded": len(postgres_rows),
        "fully_matched_procurements": status_counts.get("fully_matched_procurement", 0),
        "procurements_with_any_difference": len(mismatched_ids),
        "missing_in_postgres": status_counts.get("missing_in_postgres", 0),
        "extra_in_postgres": status_counts.get("extra_in_postgres", 0),
        "field_mismatch_counts": dict(field_mismatch_counts.most_common()),
        "mismatched_procurement_ids_sample": mismatched_ids[:50],
    }


def print_summary(summary: dict[str, Any], output_dir: Path) -> None:
    print(f"SQLite procurements compared: {summary['sqlite_procurements_compared']}")
    print(f"Postgres procurements loaded: {summary['postgres_procurements_loaded']}")
    print(f"Fully matched procurements: {summary['fully_matched_procurements']}")
    print(f"Procurements with any difference: {summary['procurements_with_any_difference']}")
    print(f"Missing in Postgres: {summary['missing_in_postgres']}")
    print(f"Extra in Postgres: {summary['extra_in_postgres']}")
    print("Top mismatching fields:")
    for field, count in list(summary["field_mismatch_counts"].items())[:10]:
        print(f"  {field}: {count}")
    print(f"Reports written to: {output_dir}")


def main() -> int:
    args = parse_args()
    postgres_dsn = resolve_postgres_dsn(args.postgres_dsn)
    fields = BUSINESS_FIELDS if args.field_set == "business" else COMPARABLE_FIELDS

    sqlite_rows = load_sqlite_rows(
        args.sqlite_path,
        fields=fields,
        procurement_ids=args.procurement_ids,
        limit=args.limit,
    )
    if not sqlite_rows:
        print("No SQLite procurements found for the selected scope.", file=sys.stderr)
        return 1

    postgres_rows = fetch_postgres_rows(
        postgres_dsn,
        procurement_ids=list(sqlite_rows.keys()),
        batch_size=args.batch_size,
    )
    results, procurement_rows, field_mismatch_counts, status_counts = compare_rows(
        sqlite_rows,
        postgres_rows,
        fields=fields,
        show_matches=args.show_matches,
    )
    summary = build_summary(
        sqlite_rows,
        postgres_rows,
        results,
        field_mismatch_counts,
        status_counts,
    )
    write_outputs(args.output_dir, fields, results, procurement_rows, summary)
    print_summary(summary, args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
