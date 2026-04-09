#!/usr/bin/env python3
"""Read-only analytics API and static site for construction procurement data."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, Protocol, cast

try:
    from .collector_companies import normalize_party_name as _normalize_party_name
    _COMPANIES_MODULE_AVAILABLE = True
except ImportError:
    _normalize_party_name = None
    _COMPANIES_MODULE_AVAILABLE = False

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DEFAULT_DB_PATH = Path("database/eis_procurement_records.sqlite")
DATASET_REPORT_PREFIX = "data/construction_2023_2025/reports/%"

DATASET_BASE_COLUMNS = [
    "procurement_id",
    "year",
    "cpv_main",
    "cpv_additional",
    "purchaser_name",
    "delivery_location",
    "estimated_value_eur",
    "procurement_name",
    "procurement_status",
    "report_document_path",
    "procurement_winner",
    "procurement_winner_suggested_price_eur",
    "procurement_participants_count",
    "procurement_participants_json",
    "procurement_status_from_report",
    "bid_deadline",
    "decision_date",
    "funding_source",
    "eu_project_reference",
    "evaluation_method",
    "contract_scope_type",
    "is_multi_lot",
    "lot_count",
    "lots_json",
    "procurement_winner_registration_no",
]
DATASET_OPTIONAL_COLUMNS = ["winner_company_id"]


def json_response(handler: BaseHTTPRequestHandler, payload: Any, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(
    handler: BaseHTTPRequestHandler,
    body: str,
    *,
    status: int = 200,
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def parse_bool(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return None


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pct(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round((numerator / denominator) * 100.0, 1)


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def normalize_party_name(value: str | None) -> str:
    if _COMPANIES_MODULE_AVAILABLE and _normalize_party_name is not None:
        return _normalize_party_name(value)
    if not value:
        return "Nav norādīts"
    cleaned = value.strip().upper()
    cleaned = cleaned.replace("„", '"').replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", '"')
    for token in ["SIA", "AS", "PSIA", "PAŠVALDĪBAS", "SABIEDRĪBA AR IEROBEŽOTU ATBILDĪBU"]:
        cleaned = cleaned.replace(f"{token} ", "")
        cleaned = cleaned.replace(f" {token}", "")
    cleaned = cleaned.replace('"', "").replace(".", " ").replace(",", " ")
    cleaned = normalize_whitespace(cleaned)
    return cleaned or "Nav norādīts"


def pretty_party_name(value: str | None) -> str:
    if not value:
        return "Nav norādīts"
    return normalize_whitespace(str(value).replace("„", '"').replace("“", '"').replace("”", '"').strip())


def extract_location_bucket(location: str | None) -> str:
    if not location:
        return "Nav norādīta"
    text = location.strip()
    if not text or text == "-":
        return "Nav norādīta"
    text = text.replace("(LATVIJA)", "").replace("LATVIJAS REPUBLIKAS TERITORIJĀ", "Latvija")
    text = text.replace("Visā Latvijas Republikas teritorijā", "Latvija")
    parts = [part.strip(" .-") for part in text.split(",") if part.strip(" .-")]
    for part in parts:
        lower = part.lower()
        if lower.startswith("lv") or "iela" in lower or "prospekts" in lower or "bulvāris" in lower:
            continue
        return part
    return parts[0] if parts else "Nav norādīta"


PLANNING_REGION_KEYWORDS = {
    "Rīga": [
        "rīga",
        "jūrmala",
        "ādažu",
        "ķekavas",
        "mārupes",
        "olaines",
        "ropažu",
        "salaspils",
        "siguldas",
        "ogres",
    ],
    "Vidzeme": [
        "alūksnes",
        "balvu",
        "cēsu",
        "gulbenes",
        "limbažu",
        "madonas",
        "smiltenes",
        "valkas",
        "valmieras",
        "saulkrastu",
    ],
    "Kurzeme": [
        "dienvidkurzemes",
        "kuldīgas",
        "liepāja",
        "saldus",
        "talsu",
        "tukuma",
        "ventspils",
    ],
    "Zemgale": [
        "aizkraukles",
        "bauskas",
        "dobeles",
        "jelgava",
        "jelgavas",
        "jēkabpils",
    ],
    "Latgale": [
        "augšdaugavas",
        "balvu",
        "daugavpils",
        "krāslavas",
        "līvānu",
        "ludzas",
        "preiļu",
        "rēzekne",
        "rēzeknes",
    ],
}


def derive_planning_region(location: str | None) -> str:
    bucket = extract_location_bucket(location)
    lowered = bucket.lower()
    if bucket == "Latvija":
        return "Visa Latvija"
    for region, keywords in PLANNING_REGION_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return region
    return "Nezināms"


@dataclass
class Filters:
    year: int | None = None
    planning_region: str | None = None
    multi_lot: bool | None = None
    buyer: str | None = None
    category: str | None = None


class AnalyticsRepositoryProtocol(Protocol):
    def fetch_rows(self, filters: Filters) -> list[dict[str, Any]]: ...
    def available_filters(self) -> dict[str, Any]: ...


class AnalyticsRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def _fetch_company_name_map(self, conn: sqlite3.Connection) -> dict[int, str]:
        """Load {company_id: canonical_name} from the companies table if it exists."""
        try:
            rows = conn.execute("SELECT id, canonical_name FROM companies").fetchall()
            return {row["id"]: row["canonical_name"] for row in rows}
        except sqlite3.OperationalError:
            return {}

    def _procurement_record_columns(self, conn: sqlite3.Connection) -> set[str]:
        return {row["name"] for row in conn.execute("PRAGMA table_info(procurement_records)").fetchall()}

    def fetch_rows(self, filters: Filters) -> list[dict[str, Any]]:
        with self._connect() as conn:
            available_columns = self._procurement_record_columns(conn)
            selected_columns = list(DATASET_BASE_COLUMNS)
            selected_columns.extend(
                column for column in DATASET_OPTIONAL_COLUMNS if column in available_columns
            )
            query = (
                "SELECT\n    "
                + ",\n    ".join(selected_columns)
                + "\nFROM procurement_records\nWHERE report_document_path LIKE ?"
            )
            params: list[Any] = [DATASET_REPORT_PREFIX]
            if filters.year is not None:
                query += " AND year = ?"
                params.append(filters.year)
            if filters.multi_lot is not None:
                query += " AND coalesce(is_multi_lot, 0) = ?"
                params.append(1 if filters.multi_lot else 0)
            if filters.buyer:
                query += " AND purchaser_name = ?"
                params.append(filters.buyer)
            query += " ORDER BY year DESC, procurement_id DESC"
            rows = [dict(row) for row in conn.execute(query, params).fetchall()]
            company_name_map = self._fetch_company_name_map(conn)
        enriched: list[dict[str, Any]] = []
        for row in rows:
            row["estimated_value_eur"] = safe_float(row.get("estimated_value_eur"))
            row["procurement_winner_suggested_price_eur"] = safe_float(
                row.get("procurement_winner_suggested_price_eur")
            )
            row["procurement_participants_count"] = safe_int(row.get("procurement_participants_count"))
            row["lot_count"] = safe_int(row.get("lot_count"))
            row["is_multi_lot"] = bool(row.get("is_multi_lot"))
            row["location_bucket"] = extract_location_bucket(row.get("delivery_location"))
            row["planning_region"] = derive_planning_region(row.get("delivery_location"))
            row["buyer_normalized"] = normalize_party_name(row.get("purchaser_name"))
            row["winner_normalized"] = normalize_party_name(row.get("procurement_winner"))
            row["category"] = self._derive_category(row)
            # Use canonical company name if available, else fall back to normalized name
            company_id = row.get("winner_company_id")
            if company_id is not None and company_id in company_name_map:
                row["winner_display_name"] = company_name_map[company_id]
            else:
                row["winner_display_name"] = row["winner_normalized"]
            if filters.planning_region and row["planning_region"] != filters.planning_region:
                continue
            if filters.category and row["category"] != filters.category:
                continue
            enriched.append(row)
        return enriched

    def available_filters(self) -> dict[str, Any]:
        rows = self.fetch_rows(Filters())
        years = sorted({row["year"] for row in rows if row.get("year") is not None})
        buyers = sorted({row["purchaser_name"] for row in rows if row.get("purchaser_name")})
        regions = sorted({row["planning_region"] for row in rows if row.get("planning_region")})
        categories = sorted({row["category"] for row in rows if row.get("category")})
        return {"years": years, "buyers": buyers, "planning_regions": regions, "categories": categories}

    @staticmethod
    def _derive_category(row: dict[str, Any]) -> str:
        for key in ("contract_scope_type", "classification_final_category", "classification_scope_type"):
            value = row.get(key)
            if isinstance(value, str):
                value = value.strip()
                if value and value != "unknown" and not value.startswith("{"):
                    return value
        return "unknown"


class AnalyticsService:
    def __init__(self, repository: AnalyticsRepositoryProtocol) -> None:
        self.repository = repository

    def build_dashboard(self, filters: Filters) -> dict[str, Any]:
        rows = self.repository.fetch_rows(filters)
        total_projects = len(rows)

        awarded_rows = [row for row in rows if row.get("procurement_winner_suggested_price_eur") is not None]
        estimated_rows = [row for row in rows if row.get("estimated_value_eur") is not None]
        multi_lot_rows = [row for row in rows if row.get("is_multi_lot")]
        winners_counter = Counter()
        winners_amounts: dict[str, float] = defaultdict(float)
        buyers_counter = Counter()
        buyers_amounts: dict[str, float] = defaultdict(float)
        region_counter = Counter()
        region_amounts: dict[str, float] = defaultdict(float)
        category_counter = Counter()
        category_amounts: dict[str, float] = defaultdict(float)
        cpv_counter = Counter()
        cpv_amounts: dict[str, float] = defaultdict(float)
        bidder_metrics: dict[str, dict[str, Any]] = {}
        buyer_winner_counts: dict[str, Counter] = defaultdict(Counter)
        buyer_winner_amounts: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        yearly_stats: dict[int, dict[str, Any]] = defaultdict(
            lambda: {
                "year": None,
                "projects": 0,
                "awarded_sum_eur": 0.0,
                "estimated_sum_eur": 0.0,
                "multi_lot_projects": 0,
            }
        )
        quality_missing = Counter()

        for row in rows:
            amount = row.get("procurement_winner_suggested_price_eur") or 0.0
            estimated = row.get("estimated_value_eur") or 0.0
            region = row["planning_region"]
            buyer = row.get("buyer_normalized") or "Nav norādīts"
            category = row.get("category") or "unknown"
            cpv = row.get("cpv_main") or "Nav norādīts"
            year = row.get("year")
            if year is not None:
                year_stats = yearly_stats[year]
                year_stats["year"] = year
                year_stats["projects"] += 1
                year_stats["awarded_sum_eur"] += amount
                year_stats["estimated_sum_eur"] += estimated
                if row.get("is_multi_lot"):
                    year_stats["multi_lot_projects"] += 1

            buyers_counter[buyer] += 1
            buyers_amounts[buyer] += amount
            region_counter[region] += 1
            region_amounts[region] += amount
            category_counter[category] += 1
            category_amounts[category] += amount
            cpv_counter[cpv] += 1
            cpv_amounts[cpv] += amount

            winner = row.get("winner_display_name") or "Nav norādīts"
            if row.get("procurement_winner"):
                winners_counter[winner] += 1
                winners_amounts[winner] += amount
                buyer_winner_counts[buyer][winner] += 1
                buyer_winner_amounts[buyer][winner] += amount

            self._accumulate_bidder_metrics(bidder_metrics, row)

            if not row.get("procurement_winner"):
                quality_missing["winner"] += 1
            if row.get("procurement_winner_suggested_price_eur") is None:
                quality_missing["award_amount"] += 1
            if row.get("estimated_value_eur") is None:
                quality_missing["estimated_value"] += 1
            if not row.get("delivery_location"):
                quality_missing["delivery_location"] += 1
            if not row.get("bid_deadline"):
                quality_missing["bid_deadline"] += 1
            if not row.get("decision_date"):
                quality_missing["decision_date"] += 1
            if row.get("procurement_participants_count") is None:
                quality_missing["participants"] += 1
            if not row.get("eu_project_reference"):
                quality_missing["eu_project_reference"] += 1

        total_awarded = round(sum(row["procurement_winner_suggested_price_eur"] or 0.0 for row in rows), 2)
        total_estimated = round(sum(row["estimated_value_eur"] or 0.0 for row in rows), 2)
        avg_bidders = round(
            sum(row["procurement_participants_count"] or 0 for row in rows) / total_projects,
            2,
        ) if total_projects else None
        single_bidder_share = pct(
            sum(1 for row in rows if row.get("procurement_participants_count") == 1),
            total_projects,
        )

        winners_ranked = self._rank_entities(winners_counter, winners_amounts)
        buyers_ranked = self._rank_entities(buyers_counter, buyers_amounts)
        regions_ranked = self._rank_entities(region_counter, region_amounts)
        categories_ranked = self._rank_entities(category_counter, category_amounts)
        cpv_ranked = self._rank_entities(cpv_counter, cpv_amounts)
        design_count = category_counter.get("design", 0)
        design_build_count = category_counter.get("design_build", 0)
        build_categories = {
            "construction_new",
            "construction_renovation",
            "construction_repair",
            "construction_other",
            "mixed",
            "maintenance",
            "supervision_construction",
        }
        build_count = sum(category_counter.get(category, 0) for category in build_categories)
        build_awarded = sum(category_amounts.get(category, 0.0) for category in build_categories)
        bidder_leaderboard = self._build_bidder_leaderboard(bidder_metrics)
        biggest_losers = [
            item for item in bidder_leaderboard if item["wins"] == 0 and item["applications"] >= 5
        ][:10]
        biggest_winners = [item for item in bidder_leaderboard if item["wins"] >= 3]
        biggest_winners.sort(
            key=lambda item: (-item["win_rate_pct"], -item["wins"], -item["won_value_eur"], item["name"])
        )
        biggest_winners_by_value = [item for item in bidder_leaderboard if item["wins"] >= 1]
        biggest_winners_by_value.sort(
            key=lambda item: (-item["won_value_eur"], -item["wins"], -item["win_rate_pct"], item["name"])
        )
        close_losses = [item for item in bidder_leaderboard if item["close_losses_3pct"] > 0]
        close_losses.sort(
            key=lambda item: (-item["close_losses_3pct"], -item["applications"], item["avg_loss_gap_pct"] or 9999)
        )
        buyer_concentration = self._build_buyer_concentration(buyer_winner_counts, buyer_winner_amounts)

        top5_awarded = sum(item["awarded_sum_eur"] for item in winners_ranked[:5])
        shares = [
            item["awarded_sum_eur"] / total_awarded
            for item in winners_ranked
            if total_awarded > 0 and item["awarded_sum_eur"] > 0
        ]
        hhi = round(sum((share * 100.0) ** 2 for share in shares), 1) if shares else None

        quality_items = []
        for field, label in [
            ("winner", "Uzvarētājs"),
            ("award_amount", "Līgumcena"),
            ("estimated_value", "Paredzamā cena"),
            ("delivery_location", "Vieta"),
            ("bid_deadline", "Piedāvājumu termiņš"),
            ("decision_date", "Lēmuma datums"),
            ("participants", "Dalībnieku skaits"),
            ("eu_project_reference", "ES projekta atsauce"),
        ]:
            missing = quality_missing[field]
            quality_items.append(
                {
                    "field": field,
                    "label": label,
                    "missing_count": missing,
                    "coverage_pct": round(100.0 - ((missing / total_projects) * 100.0), 1)
                    if total_projects
                    else None,
                }
            )

        total_lots = 0
        for row in multi_lot_rows:
            lot_count = row.get("lot_count")
            if lot_count is None:
                lots_json = row.get("lots_json")
                if lots_json:
                    try:
                        parsed = json.loads(lots_json)
                        lot_count = len(parsed) if isinstance(parsed, list) else None
                    except json.JSONDecodeError:
                        lot_count = None
            total_lots += lot_count or 0

        overview = {
            "projects_in_scope": total_projects,
            "total_awarded_sum_eur": total_awarded,
            "total_estimated_sum_eur": total_estimated,
            "awarded_projects_count": len(awarded_rows),
            "estimated_projects_count": len(estimated_rows),
            "winners_count": len(winners_counter),
            "multi_lot_projects_count": len(multi_lot_rows),
            "multi_lot_share_pct": pct(len(multi_lot_rows), total_projects),
            "average_bidders": avg_bidders,
            "single_bidder_share_pct": single_bidder_share,
            "buyers_count": len(buyers_counter),
            "regions_count": len(region_counter),
        }

        return {
            "filters": self.repository.available_filters(),
            "applied_filters": {
                "year": filters.year,
                "planning_region": filters.planning_region,
                "multi_lot": filters.multi_lot,
                "buyer": filters.buyer,
                "category": filters.category,
            },
            "overview": overview,
            "market_concentration": {
                "top5_awarded_share_pct": pct(top5_awarded, total_awarded),
                "hhi": hhi,
                "note": "Uzvarētāji grupēti pēc vienkāršotas nosaukumu normalizācijas.",
            },
            "yearly_series": [
                {
                    **stats,
                    "awarded_sum_eur": round(stats["awarded_sum_eur"], 2),
                    "estimated_sum_eur": round(stats["estimated_sum_eur"], 2),
                    "multi_lot_share_pct": pct(stats["multi_lot_projects"], stats["projects"]),
                }
                for _, stats in sorted(yearly_stats.items())
            ],
            "top_winners": winners_ranked[:10],
            "top_buyers": buyers_ranked[:10],
            "top_regions": regions_ranked[:10],
            "categories": categories_ranked,
            "cpv_codes": cpv_ranked[:15],
            "category_summary": {
                "design": {
                    "project_count": design_count,
                    "awarded_sum_eur": round(category_amounts.get("design", 0.0), 2),
                },
                "build": {
                    "project_count": build_count,
                    "awarded_sum_eur": round(build_awarded, 2),
                },
                "design_build": {
                    "project_count": design_build_count,
                    "awarded_sum_eur": round(category_amounts.get("design_build", 0.0), 2),
                },
            },
            "bidder_leaderboard": bidder_leaderboard[:15],
            "biggest_winners": biggest_winners[:10],
            "biggest_winners_by_value": biggest_winners_by_value[:10],
            "biggest_losers": biggest_losers,
            "close_losses": close_losses[:10],
            "buyer_concentration": buyer_concentration[:10],
            "multi_lot": {
                "projects": len(multi_lot_rows),
                "share_pct": pct(len(multi_lot_rows), total_projects),
                "known_total_lots": total_lots,
                "average_lots_per_multi_lot_project": round(total_lots / len(multi_lot_rows), 2)
                if multi_lot_rows
                else None,
            },
            "data_quality": quality_items,
        }

    def list_projects(self, filters: Filters, *, limit: int, offset: int) -> dict[str, Any]:
        rows = self.repository.fetch_rows(filters)
        total = len(rows)
        rows = rows[offset : offset + limit]
        items = []
        for row in rows:
            items.append(
                {
                    "procurement_id": row["procurement_id"],
                    "year": row["year"],
                    "procurement_name": row["procurement_name"],
                    "purchaser_name": row["purchaser_name"],
                    "planning_region": row["planning_region"],
                    "category": row["category"],
                    "cpv_main": row.get("cpv_main"),
                    "cpv_additional": row.get("cpv_additional"),
                    "location_bucket": row["location_bucket"],
                    "procurement_status": row["procurement_status"],
                    "winner": row["procurement_winner"],
                    "awarded_sum_eur": row["procurement_winner_suggested_price_eur"],
                    "estimated_sum_eur": row["estimated_value_eur"],
                    "participants_count": row["procurement_participants_count"],
                    "is_multi_lot": row["is_multi_lot"],
                    "lot_count": row["lot_count"],
                }
            )
        return {"total": total, "limit": limit, "offset": offset, "items": items}

    def build_company_view(self, filters: Filters, companies: list[str] | None) -> dict[str, Any]:
        rows = self.repository.fetch_rows(filters)
        bidder_metrics: dict[str, dict[str, Any]] = {}
        buyer_winner_counts: dict[str, Counter] = defaultdict(Counter)
        buyer_winner_amounts: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for row in rows:
            buyer = row.get("buyer_normalized") or "Nav norādīts"
            winner = row.get("winner_normalized") or "Nav norādīts"
            amount = row.get("procurement_winner_suggested_price_eur") or 0.0
            if row.get("procurement_winner"):
                buyer_winner_counts[buyer][winner] += 1
                buyer_winner_amounts[buyer][winner] += amount
            self._accumulate_bidder_metrics(bidder_metrics, row)

        company_options = sorted(
            {
                alias
                for metric in bidder_metrics.values()
                for alias in metric.get("aliases", set())
                if alias and alias != "Nav norādīts"
            }
        )
        selected_companies = [company for company in (companies or []) if company]
        if not selected_companies and company_options:
            selected_companies = [company_options[0]]
        if not selected_companies:
            return {
                "filters": self.repository.available_filters(),
                "companies": [],
                "selected_company": None,
                "selected_companies": [],
            }

        selected_norms = {normalize_party_name(company) for company in selected_companies}
        selected_pretty_values = list(selected_companies)
        applications = wins = losses = 0
        won_value = 0.0
        losing_gap_total = 0.0
        losing_gap_count = 0
        winning_margin_total = 0.0
        winning_margin_count = 0
        close_losses_3pct = 0
        buyers = Counter()
        categories = Counter()
        regions = Counter()
        cpvs = Counter()
        size_bands = Counter()
        buyer_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"bids": 0, "wins": 0, "awarded_sum_eur": 0.0})
        competitor_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"meet_count": 0, "beat_us": 0, "we_beat": 0, "gap_pct_total": 0.0, "gap_count": 0}
        )
        segment_stats: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"bids": 0, "wins": 0, "avg_competitors_total": 0.0, "count": 0}
        )

        for row in rows:
            participants = self._parse_participants(row.get("procurement_participants_json"))
            if not participants:
                continue
            parsed_participants = []
            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                parsed_participants.append(
                    {
                        "name": pretty_party_name(participant.get("name")),
                        "norm_name": normalize_party_name(participant.get("name")),
                        "price": safe_float(participant.get("suggested_price_eur")),
                    }
                )
            company_entries = [p for p in parsed_participants if p["norm_name"] in selected_norms]
            if not company_entries:
                continue

            row_selected_names = sorted({entry["name"] for entry in company_entries if entry.get("name")})
            for name in row_selected_names:
                if name not in selected_pretty_values:
                    selected_pretty_values.append(name)
            applications += 1
            buyer = row.get("buyer_normalized") or "Nav norādīts"
            category = row.get("category") or "unknown"
            region = row.get("planning_region") or "Nezināms"
            cpv = row.get("cpv_main") or "Nav norādīts"
            estimated = safe_float(row.get("estimated_value_eur"))
            buyers[buyer] += 1
            categories[category] += 1
            regions[region] += 1
            cpvs[cpv] += 1
            size_bands[self._size_band(estimated)] += 1
            buyer_stats[buyer]["bids"] += 1
            segment = (category, region, cpv)
            segment_stats[segment]["bids"] += 1
            segment_stats[segment]["avg_competitors_total"] += max(len(parsed_participants) - 1, 0)
            segment_stats[segment]["count"] += 1

            winner_name = normalize_party_name(row.get("procurement_winner"))
            winner_price = safe_float(row.get("procurement_winner_suggested_price_eur"))
            selected_prices = [entry["price"] for entry in company_entries if entry["price"] is not None]
            company_price = min(selected_prices) if selected_prices else None
            selected_won = winner_name in selected_norms
            if selected_won:
                wins += 1
                won_value += winner_price or 0.0
                buyer_stats[buyer]["wins"] += 1
                buyer_stats[buyer]["awarded_sum_eur"] += winner_price or 0.0
                segment_stats[segment]["wins"] += 1
                ranked_prices = sorted(
                    [p["price"] for p in parsed_participants if p["price"] is not None]
                )
                if company_price is not None and len(ranked_prices) >= 2:
                    for price in ranked_prices:
                        if price > company_price:
                            winning_margin_total += ((price - company_price) / company_price) * 100.0
                            winning_margin_count += 1
                            break
            else:
                losses += 1
                if company_price is not None and winner_price not in (None, 0):
                    gap_pct = ((company_price - winner_price) / winner_price) * 100.0
                    if gap_pct >= 0:
                        losing_gap_total += gap_pct
                        losing_gap_count += 1
                        if gap_pct <= 3:
                            close_losses_3pct += 1

            for participant in parsed_participants:
                if participant["norm_name"] in selected_norms:
                    continue
                stat = competitor_stats[participant["name"]]
                stat["meet_count"] += 1
                if selected_won:
                    stat["we_beat"] += 1
                    if company_price not in (None, 0) and participant["price"] is not None:
                        stat["gap_pct_total"] += ((participant["price"] - company_price) / company_price) * 100.0
                        stat["gap_count"] += 1
                elif participant["norm_name"] == winner_name:
                    stat["beat_us"] += 1
                    if winner_price not in (None, 0) and company_price is not None:
                        stat["gap_pct_total"] += ((company_price - winner_price) / winner_price) * 100.0
                        stat["gap_count"] += 1

        competitor_rows = []
        for name, stat in competitor_stats.items():
            competitor_rows.append(
                {
                    "name": name,
                    "meet_count": stat["meet_count"],
                    "beat_us": stat["beat_us"],
                    "we_beat": stat["we_beat"],
                    "avg_gap_pct": round(stat["gap_pct_total"] / stat["gap_count"], 2) if stat["gap_count"] else None,
                }
            )
        met_most = sorted(competitor_rows, key=lambda item: (-item["meet_count"], item["name"]))[:10]
        beat_us = sorted(competitor_rows, key=lambda item: (-item["beat_us"], -item["meet_count"], item["name"]))[:10]
        we_beat = sorted(competitor_rows, key=lambda item: (-item["we_beat"], -item["meet_count"], item["name"]))[:10]

        buyer_rows = []
        for buyer, stat in buyer_stats.items():
            concentration = None
            if buyer in buyer_winner_counts and sum(buyer_winner_counts[buyer].values()) >= 5:
                top_wins = buyer_winner_counts[buyer].most_common(1)[0][1]
                concentration = round((top_wins / sum(buyer_winner_counts[buyer].values())) * 100.0, 1)
            buyer_rows.append(
                {
                    "name": buyer,
                    "bids": stat["bids"],
                    "wins": stat["wins"],
                    "win_rate_pct": round((stat["wins"] / stat["bids"]) * 100.0, 1) if stat["bids"] else None,
                    "won_value_eur": round(stat["awarded_sum_eur"], 2),
                    "market_concentration_pct": concentration,
                }
            )
        best_buyers = sorted(
            [row for row in buyer_rows if row["bids"] >= 2],
            key=lambda item: (-item["win_rate_pct"], -item["wins"], item["name"]),
        )[:10]
        buyer_targets = sorted(
            [row for row in buyer_rows if row["bids"] >= 2],
            key=lambda item: (
                item["market_concentration_pct"] if item["market_concentration_pct"] is not None else 999,
                -item["bids"],
                item["name"],
            ),
        )[:10]

        segment_rows = []
        for (category, region, cpv), stat in segment_stats.items():
            segment_rows.append(
                {
                    "category": category,
                    "region": region,
                    "cpv": cpv,
                    "bids": stat["bids"],
                    "wins": stat["wins"],
                    "win_rate_pct": round((stat["wins"] / stat["bids"]) * 100.0, 1) if stat["bids"] else None,
                    "avg_competitors": round(stat["avg_competitors_total"] / stat["count"], 2) if stat["count"] else None,
                }
            )
        fit_segments = sorted(
            [row for row in segment_rows if row["bids"] >= 2],
            key=lambda item: (-item["win_rate_pct"], item["avg_competitors"] or 999, -item["bids"]),
        )[:10]
        selected_pretty = " · ".join(selected_pretty_values)

        return {
            "filters": self.repository.available_filters(),
            "companies": company_options,
            "selected_company": selected_pretty,
            "selected_companies": selected_companies,
            "summary": {
                "applications": applications,
                "wins": wins,
                "losses": losses,
                "win_rate_pct": round((wins / applications) * 100.0, 1) if applications else None,
                "won_value_eur": round(won_value, 2),
                "avg_losing_gap_pct": round(losing_gap_total / losing_gap_count, 2) if losing_gap_count else None,
                "avg_winning_margin_pct": round(winning_margin_total / winning_margin_count, 2)
                if winning_margin_count
                else None,
                "close_losses_3pct": close_losses_3pct,
                "buyers_count": len(buyers),
                "categories_count": len(categories),
            },
            "our_fit": {
                "buyers": self._rank_counter_rows(buyers, "buyer"),
                "categories": self._rank_counter_rows(categories, "category"),
                "regions": self._rank_counter_rows(regions, "region"),
                "cpv_codes": self._rank_counter_rows(cpvs, "cpv"),
                "size_bands": self._rank_counter_rows(size_bands, "size_band"),
                "segments": fit_segments,
            },
            "competitors": {
                "met_most": met_most,
                "beat_us": beat_us,
                "we_beat": we_beat,
            },
            "buyers": {
                "best": best_buyers,
                "targets": buyer_targets,
            },
        }

    def build_purchaser_view(self, filters: Filters, purchaser: str | None) -> dict[str, Any]:
        rows = self.repository.fetch_rows(filters)
        purchaser_options = sorted({row["purchaser_name"] for row in rows if row.get("purchaser_name")})
        if not purchaser and purchaser_options:
            purchaser = purchaser_options[0]
        if not purchaser:
            return {"filters": self.repository.available_filters(), "purchasers": [], "selected_purchaser": None}

        selected_rows = [row for row in rows if row.get("purchaser_name") == purchaser]
        category_counter = Counter()
        region_counter = Counter()
        cpv_counter = Counter()
        yearly_counter = Counter()
        supplier_counts = Counter()
        supplier_amounts: dict[str, float] = defaultdict(float)
        evaluation_counter = Counter()
        segment_stats: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {"projects": 0, "awarded_sum_eur": 0.0, "avg_competitors_total": 0.0, "single_bidder_count": 0}
        )
        bidder_presence = Counter()
        bidder_wins_against_buyer = Counter()
        purchaser_amounts_by_region: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        purchaser_amounts_by_category: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        supplier_amounts_by_region: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        supplier_amounts_by_category: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        total_projects = len(selected_rows)
        total_awarded = 0.0
        total_estimated = 0.0
        total_competitors = 0.0
        competitor_count_rows = 0
        single_bidder_count = 0
        decision_lag_days_total = 0
        decision_lag_count = 0

        for row in selected_rows:
            amount = row.get("procurement_winner_suggested_price_eur") or 0.0
            total_awarded += amount
            total_estimated += row.get("estimated_value_eur") or 0.0
            category = row.get("category") or "unknown"
            region = row.get("planning_region") or "Nezināms"
            cpv = row.get("cpv_main") or "Nav norādīts"
            year = row.get("year")
            category_counter[category] += 1
            region_counter[region] += 1
            cpv_counter[cpv] += 1
            if year is not None:
                yearly_counter[year] += 1
            if row.get("evaluation_method"):
                evaluation_counter[row["evaluation_method"]] += 1

            participants = self._parse_participants(row.get("procurement_participants_json"))
            participant_count = len([p for p in participants if isinstance(p, dict)])
            if participant_count:
                total_competitors += participant_count
                competitor_count_rows += 1
                if participant_count == 1:
                    single_bidder_count += 1
            elif row.get("procurement_participants_count") is not None:
                total_competitors += row["procurement_participants_count"]
                competitor_count_rows += 1
                if row["procurement_participants_count"] == 1:
                    single_bidder_count += 1

            winner = pretty_party_name(row.get("procurement_winner"))
            if row.get("procurement_winner"):
                supplier_counts[winner] += 1
                supplier_amounts[winner] += amount
                bidder_wins_against_buyer[winner] += 1

            participants = self._parse_participants(row.get("procurement_participants_json"))
            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                name = pretty_party_name(participant.get("name"))
                bidder_presence[name] += 1

            segment = (category, region, cpv)
            segment_stats[segment]["projects"] += 1
            segment_stats[segment]["awarded_sum_eur"] += amount
            if participant_count:
                segment_stats[segment]["avg_competitors_total"] += participant_count
                if participant_count == 1:
                    segment_stats[segment]["single_bidder_count"] += 1

            lag = self._decision_lag_days(row.get("bid_deadline"), row.get("decision_date"))
            if lag is not None:
                decision_lag_days_total += lag
                decision_lag_count += 1

        for row in rows:
            amount = row.get("procurement_winner_suggested_price_eur") or 0.0
            region = row.get("planning_region") or "Nezināms"
            category = row.get("category") or "unknown"
            row_purchaser = row.get("purchaser_name") or "Nav norādīts"
            winner = pretty_party_name(row.get("procurement_winner"))
            purchaser_amounts_by_region[region][row_purchaser] += amount
            purchaser_amounts_by_category[category][row_purchaser] += amount
            if row.get("procurement_winner"):
                supplier_amounts_by_region[region][winner] += amount
                supplier_amounts_by_category[category][winner] += amount

        supplier_rows = []
        for name, wins in supplier_counts.items():
            supplier_rows.append(
                {
                    "name": name,
                    "project_count": wins,
                    "awarded_sum_eur": round(supplier_amounts[name], 2),
                    "win_share_pct": round((wins / total_projects) * 100.0, 1) if total_projects else None,
                }
            )
        supplier_rows.sort(key=lambda item: (-item["awarded_sum_eur"], -item["project_count"], item["name"]))

        frequent_bidders = []
        for name, count in bidder_presence.items():
            frequent_bidders.append(
                {
                    "name": name,
                    "project_count": count,
                    "awarded_sum_eur": round(supplier_amounts.get(name, 0.0), 2),
                    "win_count": bidder_wins_against_buyer.get(name, 0),
                    "win_rate_pct": round((bidder_wins_against_buyer.get(name, 0) / count) * 100.0, 1)
                    if count
                    else None,
                }
            )
        frequent_bidders.sort(key=lambda item: (-item["project_count"], -item["win_count"], item["name"]))

        segment_rows = []
        for (category, region, cpv), stat in segment_stats.items():
            projects = stat["projects"]
            segment_rows.append(
                {
                    "category": category,
                    "region": region,
                    "cpv": cpv,
                    "projects": projects,
                    "awarded_sum_eur": round(stat["awarded_sum_eur"], 2),
                    "avg_competitors": round(stat["avg_competitors_total"] / projects, 2) if projects else None,
                    "single_bidder_share_pct": round((stat["single_bidder_count"] / projects) * 100.0, 1)
                    if projects
                    else None,
                }
            )
        biggest_segments = sorted(
            segment_rows, key=lambda item: (-item["awarded_sum_eur"], -item["projects"], item["category"])
        )[:10]
        open_segments = sorted(
            [row for row in segment_rows if row["projects"] >= 2],
            key=lambda item: (
                item["single_bidder_share_pct"] if item["single_bidder_share_pct"] is not None else 999,
                item["avg_competitors"] if item["avg_competitors"] is not None else 999,
                -item["projects"],
            ),
        )[:10]

        top_supplier_share = round((supplier_rows[0]["project_count"] / total_projects) * 100.0, 1) if supplier_rows and total_projects else None
        dominant_region = region_counter.most_common(1)[0][0] if region_counter else None
        dominant_category = category_counter.most_common(1)[0][0] if category_counter else None
        top_purchasers_region = self._rank_amount_map(purchaser_amounts_by_region.get(dominant_region or "", {}), purchaser)
        top_purchasers_category = self._rank_amount_map(
            purchaser_amounts_by_category.get(dominant_category or "", {}), purchaser
        )
        top_companies_region = self._rank_amount_map(supplier_amounts_by_region.get(dominant_region or "", {}))
        top_companies_category = self._rank_amount_map(supplier_amounts_by_category.get(dominant_category or "", {}))

        return {
            "filters": self.repository.available_filters(),
            "purchasers": purchaser_options,
            "selected_purchaser": purchaser,
            "summary": {
                "projects": total_projects,
                "awarded_sum_eur": round(total_awarded, 2),
                "estimated_sum_eur": round(total_estimated, 2),
                "suppliers_count": len(supplier_counts),
                "avg_competitors": round(total_competitors / competitor_count_rows, 2) if competitor_count_rows else None,
                "single_bidder_share_pct": round((single_bidder_count / competitor_count_rows) * 100.0, 1)
                if competitor_count_rows
                else None,
                "top_supplier_share_pct": top_supplier_share,
                "avg_decision_lag_days": round(decision_lag_days_total / decision_lag_count, 1)
                if decision_lag_count
                else None,
            },
            "fit": {
                "categories": self._rank_counter_rows(category_counter, "category"),
                "regions": self._rank_counter_rows(region_counter, "region"),
                "cpv_codes": self._rank_counter_rows(cpv_counter, "cpv"),
                "years": self._rank_counter_rows(yearly_counter, "year"),
                "evaluation_methods": self._rank_counter_rows(evaluation_counter, "evaluation_method"),
            },
            "suppliers": {
                "top_winners": supplier_rows[:10],
                "frequent_bidders": frequent_bidders[:10],
            },
            "segments": {
                "biggest": biggest_segments,
                "open": open_segments,
            },
            "market_context": {
                "dominant_region": dominant_region,
                "dominant_category": dominant_category,
                "top_purchasers_region": top_purchasers_region[:10],
                "top_purchasers_category": top_purchasers_category[:10],
                "top_companies_region": top_companies_region[:10],
                "top_companies_category": top_companies_category[:10],
            },
        }

    def build_risk_view(self, filters: Filters) -> dict[str, Any]:
        rows = self.repository.fetch_rows(filters)
        total_projects = len(rows)
        with_estimate = 0
        award_above_estimate = 0
        award_above_estimate_10pct = 0
        award_below_estimate_20pct = 0
        single_bidder = 0
        low_competition = 0
        buyer_single_bidder = defaultdict(lambda: {"projects": 0, "single_bidder": 0, "awarded_sum_eur": 0.0})
        buyer_winner_counts: dict[str, Counter] = defaultdict(Counter)
        pair_close = Counter()
        pair_all = Counter()
        pair_lowest = Counter()
        pair_keys_by_procurement: dict[str, set[tuple[str, str]]] = {}
        winner_stats: dict[str, dict[str, Any]] = {}
        buyer_risk_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "project_count": 0,
                "risky_project_count": 0,
                "single_bidder_count": 0,
                "above_estimate_count": 0,
                "awarded_sum_eur": 0.0,
            }
        )
        region_risk_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "project_count": 0,
                "risky_project_count": 0,
                "single_bidder_count": 0,
                "above_estimate_count": 0,
                "awarded_sum_eur": 0.0,
            }
        )
        category_risk_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "project_count": 0,
                "risky_project_count": 0,
                "single_bidder_count": 0,
                "above_estimate_count": 0,
                "awarded_sum_eur": 0.0,
            }
        )

        for row in rows:
            participants = self._parse_participants(row.get("procurement_participants_json"))
            procurement_key = str(row.get("procurement_id") or row.get("procurement_url") or id(row))
            parsed = []
            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                name = normalize_party_name(participant.get("name"))
                price = safe_float(participant.get("suggested_price_eur"))
                if name and name != "Nav norādīts" and price is not None:
                    parsed.append((name, price))
            row_pair_keys = set()
            for i in range(len(parsed)):
                for j in range(i + 1, len(parsed)):
                    a, b = parsed[i], parsed[j]
                    key = tuple(sorted([a[0], b[0]]))
                    row_pair_keys.add(key)
                    pair_all[key] += 1
                    mn = min(a[1], b[1])
                    mx = max(a[1], b[1])
                    if mn > 0 and (mx - mn) / mn <= 0.03:
                        pair_close[key] += 1
                        if row.get("evaluation_method") == "lowest_price":
                            pair_lowest[key] += 1
            pair_keys_by_procurement[procurement_key] = row_pair_keys

        repeated_pair_keys = {
            key for key, close_count in pair_close.items() if close_count >= 3 and pair_all[key] >= 3
        }

        for row in rows:
            award = row.get("procurement_winner_suggested_price_eur")
            estimate = row.get("estimated_value_eur")
            procurement_key = str(row.get("procurement_id") or row.get("procurement_url") or id(row))
            participants_count = row.get("procurement_participants_count")
            has_single_bidder = participants_count == 1
            has_low_competition = participants_count is not None and participants_count <= 2
            is_above_estimate = False
            is_above_estimate_10pct = False
            if estimate not in (None, 0) and award not in (None, 0):
                with_estimate += 1
                if award > estimate:
                    award_above_estimate += 1
                    is_above_estimate = True
                if award > estimate * 1.1:
                    award_above_estimate_10pct += 1
                    is_above_estimate_10pct = True
                if award < estimate * 0.8:
                    award_below_estimate_20pct += 1

            if participants_count is not None:
                buyer = row.get("purchaser_name") or "Nav norādīts"
                buyer_single_bidder[buyer]["projects"] += 1
                buyer_single_bidder[buyer]["awarded_sum_eur"] += award or 0.0
                if has_single_bidder:
                    single_bidder += 1
                    buyer_single_bidder[buyer]["single_bidder"] += 1
                if has_low_competition:
                    low_competition += 1

            winner = normalize_party_name(row.get("procurement_winner"))
            if row.get("procurement_winner") and row.get("purchaser_name"):
                buyer_winner_counts[row["purchaser_name"]][winner] += 1

            has_repeated_close_pair = any(
                key in repeated_pair_keys for key in pair_keys_by_procurement.get(procurement_key, set())
            )
            is_risky = (
                has_single_bidder
                or has_low_competition
                or is_above_estimate
                or is_above_estimate_10pct
                or has_repeated_close_pair
            )

            buyer_name = row.get("purchaser_name") or "Nav norādīts"
            region_name = row.get("planning_region") or "Nezināms"
            category_name = row.get("category") or "unknown"
            for stats in (
                buyer_risk_stats[buyer_name],
                region_risk_stats[region_name],
                category_risk_stats[category_name],
            ):
                stats["project_count"] += 1
                stats["awarded_sum_eur"] += award or 0.0
                if has_single_bidder:
                    stats["single_bidder_count"] += 1
                if is_above_estimate:
                    stats["above_estimate_count"] += 1
                if is_risky:
                    stats["risky_project_count"] += 1

            if row.get("procurement_winner"):
                winner_norm = row.get("winner_normalized") or winner
                winner_entry = winner_stats.setdefault(
                    winner_norm,
                    {
                        "name": pretty_party_name(row.get("procurement_winner")),
                        "project_count": 0,
                        "risky_project_count": 0,
                        "awarded_sum_eur": 0.0,
                        "risky_awarded_sum_eur": 0.0,
                        "single_bidder_wins": 0,
                        "above_estimate_wins": 0,
                    },
                )
                winner_entry["project_count"] += 1
                winner_entry["awarded_sum_eur"] += award or 0.0
                if has_single_bidder:
                    winner_entry["single_bidder_wins"] += 1
                if is_above_estimate:
                    winner_entry["above_estimate_wins"] += 1
                if is_risky:
                    winner_entry["risky_project_count"] += 1
                    winner_entry["risky_awarded_sum_eur"] += award or 0.0

        buyer_single_bidder_rows = []
        for buyer, stats in buyer_single_bidder.items():
            if stats["projects"] < 5:
                continue
            buyer_single_bidder_rows.append(
                {
                    "name": buyer,
                    "project_count": stats["projects"],
                    "single_bidder_count": stats["single_bidder"],
                    "awarded_sum_eur": round(stats.get("awarded_sum_eur", 0.0), 2),
                    "single_bidder_share_pct": round((stats["single_bidder"] / stats["projects"]) * 100.0, 1),
                }
            )
        buyer_single_bidder_rows.sort(
            key=lambda item: (-item["single_bidder_share_pct"], -item["project_count"], item["name"])
        )

        buyer_concentration_rows = []
        for buyer, counts in buyer_winner_counts.items():
            total = sum(counts.values())
            if total < 5:
                continue
            top_winner, top_wins = counts.most_common(1)[0]
            buyer_concentration_rows.append(
                {
                    "name": buyer,
                    "project_count": total,
                    "top_winner": top_winner,
                    "top_winner_projects": top_wins,
                    "awarded_sum_eur": round(
                        sum(
                            row.get("procurement_winner_suggested_price_eur") or 0.0
                            for row in rows
                            if row.get("purchaser_name") == buyer
                        ),
                        2,
                    ),
                    "top_winner_share_pct": round((top_wins / total) * 100.0, 1),
                }
            )
        buyer_concentration_rows.sort(
            key=lambda item: (-item["top_winner_share_pct"], -item["project_count"], item["name"])
        )

        repeated_pairs = []
        for key, close_count in pair_close.items():
            total = pair_all[key]
            if close_count < 3 or total < 3:
                continue
            repeated_pairs.append(
                {
                    "name": f"{key[0]} · {key[1]}",
                    "close_bid_count": close_count,
                    "meet_count": total,
                    "lowest_price_close_count": pair_lowest[key],
                    "close_share_pct": round((close_count / total) * 100.0, 1),
                }
            )
        repeated_pairs.sort(
            key=lambda item: (-item["close_bid_count"], -item["meet_count"], item["name"])
        )

        return {
            "filters": self.repository.available_filters(),
            "summary": {
                "projects": total_projects,
                "single_bidder_count": single_bidder,
                "single_bidder_share_pct": round((single_bidder / total_projects) * 100.0, 1)
                if total_projects
                else None,
                "low_competition_count": low_competition,
                "low_competition_share_pct": round((low_competition / total_projects) * 100.0, 1)
                if total_projects
                else None,
                "with_estimate_count": with_estimate,
                "award_above_estimate_count": award_above_estimate,
                "award_above_estimate_share_pct": round((award_above_estimate / with_estimate) * 100.0, 1)
                if with_estimate
                else None,
                "award_above_estimate_10pct_count": award_above_estimate_10pct,
                "award_above_estimate_10pct_share_pct": round((award_above_estimate_10pct / with_estimate) * 100.0, 1)
                if with_estimate
                else None,
                "award_below_estimate_20pct_count": award_below_estimate_20pct,
                "award_below_estimate_20pct_share_pct": round((award_below_estimate_20pct / with_estimate) * 100.0, 1)
                if with_estimate
                else None,
            },
            "winners": self._build_risky_winner_rows(winner_stats)[:10],
            "buyers": {
                "single_bidder": buyer_single_bidder_rows[:10],
                "concentration": buyer_concentration_rows[:10],
                "risk_hotspots": self._build_risk_hotspot_rows(buyer_risk_stats, min_projects=5)[:10],
            },
            "hotspots": {
                "regions": self._build_risk_hotspot_rows(region_risk_stats, min_projects=5)[:10],
                "categories": self._build_risk_hotspot_rows(category_risk_stats, min_projects=5)[:10],
            },
            "pairs": repeated_pairs[:15],
        }

    @staticmethod
    def _build_risky_winner_rows(winner_stats: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for stats in winner_stats.values():
            project_count = stats["project_count"]
            if project_count < 3:
                continue
            rows.append(
                {
                    "name": stats["name"],
                    "project_count": project_count,
                    "risky_project_count": stats["risky_project_count"],
                    "risky_project_share_pct": round((stats["risky_project_count"] / project_count) * 100.0, 1)
                    if project_count
                    else None,
                    "awarded_sum_eur": round(stats["awarded_sum_eur"], 2),
                    "risky_awarded_sum_eur": round(stats["risky_awarded_sum_eur"], 2),
                    "single_bidder_wins": stats["single_bidder_wins"],
                    "above_estimate_wins": stats["above_estimate_wins"],
                }
            )
        rows.sort(
            key=lambda item: (
                -(item["risky_project_share_pct"] or 0.0),
                -item["risky_project_count"],
                -item["awarded_sum_eur"],
                item["name"],
            )
        )
        return rows

    @staticmethod
    def _build_risk_hotspot_rows(
        item_stats: dict[str, dict[str, Any]],
        *,
        min_projects: int,
    ) -> list[dict[str, Any]]:
        rows = []
        for name, stats in item_stats.items():
            project_count = stats["project_count"]
            if project_count < min_projects:
                continue
            rows.append(
                {
                    "name": name,
                    "project_count": project_count,
                    "risky_project_count": stats["risky_project_count"],
                    "risky_project_share_pct": round((stats["risky_project_count"] / project_count) * 100.0, 1)
                    if project_count
                    else None,
                    "single_bidder_count": stats["single_bidder_count"],
                    "above_estimate_count": stats["above_estimate_count"],
                    "awarded_sum_eur": round(stats["awarded_sum_eur"], 2),
                }
            )
        rows.sort(
            key=lambda item: (
                -(item["risky_project_share_pct"] or 0.0),
                -item["risky_project_count"],
                -item["awarded_sum_eur"],
                item["name"],
            )
        )
        return rows

    @staticmethod
    def _rank_entities(counts: Counter, amounts: dict[str, float]) -> list[dict[str, Any]]:
        items = []
        for name, project_count in counts.items():
            items.append(
                {
                    "name": name,
                    "project_count": project_count,
                    "awarded_sum_eur": round(amounts.get(name, 0.0), 2),
                }
            )
        items.sort(key=lambda item: (-item["awarded_sum_eur"], -item["project_count"], item["name"]))
        return items

    def _accumulate_bidder_metrics(self, bidder_metrics: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
        participants_raw = row.get("procurement_participants_json")
        if not participants_raw:
            return
        try:
            participants = json.loads(participants_raw)
        except (TypeError, json.JSONDecodeError):
            return
        if not isinstance(participants, list):
            return

        winner_price = safe_float(row.get("procurement_winner_suggested_price_eur"))
        winner_name_norm = normalize_party_name(row.get("procurement_winner"))
        seen_names = set()
        for participant in participants:
            if not isinstance(participant, dict):
                continue
            raw_name = participant.get("name")
            norm_name = normalize_party_name(raw_name)
            if norm_name == "Nav norādīts" or norm_name in seen_names:
                continue
            seen_names.add(norm_name)
            metric = bidder_metrics.setdefault(
                norm_name,
                {
                    "name": pretty_party_name(raw_name),
                    "aliases": set(),
                    "applications": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_bid_value_eur": 0.0,
                    "won_value_eur": 0.0,
                    "buyers": set(),
                    "categories": Counter(),
                    "close_losses_1pct": 0,
                    "close_losses_3pct": 0,
                    "close_losses_5pct": 0,
                    "loss_gap_pct_total": 0.0,
                    "loss_gap_count": 0,
                },
            )
            metric["aliases"].add(pretty_party_name(raw_name))
            bid_price = safe_float(participant.get("suggested_price_eur"))
            is_winner = norm_name == winner_name_norm
            metric["applications"] += 1
            metric["buyers"].add(row.get("buyer_normalized") or "Nav norādīts")
            metric["categories"][row.get("category") or "unknown"] += 1
            if bid_price is not None:
                metric["total_bid_value_eur"] += bid_price
            if is_winner:
                metric["wins"] += 1
                if winner_price is not None:
                    metric["won_value_eur"] += winner_price
            else:
                metric["losses"] += 1
                if bid_price is not None and winner_price not in (None, 0):
                    gap_pct = ((bid_price - winner_price) / winner_price) * 100.0
                    if gap_pct >= 0:
                        metric["loss_gap_pct_total"] += gap_pct
                        metric["loss_gap_count"] += 1
                        if gap_pct <= 1:
                            metric["close_losses_1pct"] += 1
                        if gap_pct <= 3:
                            metric["close_losses_3pct"] += 1
                        if gap_pct <= 5:
                            metric["close_losses_5pct"] += 1

    def _build_bidder_leaderboard(self, bidder_metrics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        items = []
        for metric in bidder_metrics.values():
            applications = metric["applications"]
            wins = metric["wins"]
            avg_gap = (
                round(metric["loss_gap_pct_total"] / metric["loss_gap_count"], 2)
                if metric["loss_gap_count"]
                else None
            )
            top_category = metric["categories"].most_common(1)[0][0] if metric["categories"] else None
            items.append(
                {
                    "name": metric["name"],
                    "applications": applications,
                    "wins": wins,
                    "losses": metric["losses"],
                    "win_rate_pct": round((wins / applications) * 100.0, 1) if applications else None,
                    "total_bid_value_eur": round(metric["total_bid_value_eur"], 2),
                    "won_value_eur": round(metric["won_value_eur"], 2),
                    "buyers_count": len(metric["buyers"]),
                    "top_category": top_category,
                    "close_losses_1pct": metric["close_losses_1pct"],
                    "close_losses_3pct": metric["close_losses_3pct"],
                    "close_losses_5pct": metric["close_losses_5pct"],
                    "avg_loss_gap_pct": avg_gap,
                }
            )
        items.sort(key=lambda item: (-item["applications"], -item["wins"], item["name"]))
        return items

    def _build_buyer_concentration(
        self,
        buyer_winner_counts: dict[str, Counter],
        buyer_winner_amounts: dict[str, dict[str, float]],
    ) -> list[dict[str, Any]]:
        items = []
        for buyer, counts in buyer_winner_counts.items():
            total_projects = sum(counts.values())
            if total_projects < 5:
                continue
            total_awarded = sum(buyer_winner_amounts[buyer].values())
            top_winner, top_wins = counts.most_common(1)[0]
            top_amount = buyer_winner_amounts[buyer].get(top_winner, 0.0)
            items.append(
                {
                    "name": buyer,
                    "project_count": total_projects,
                    "awarded_sum_eur": round(total_awarded, 2),
                    "top_winner": top_winner,
                    "top_winner_projects": top_wins,
                    "top_winner_share_pct": round((top_wins / total_projects) * 100.0, 1) if total_projects else None,
                    "top_winner_amount_share_pct": round((top_amount / total_awarded) * 100.0, 1)
                    if total_awarded
                    else None,
                }
            )
        items.sort(key=lambda item: (-item["top_winner_share_pct"], -item["project_count"], item["name"]))
        return items

    @staticmethod
    def _parse_participants(participants_raw: Any) -> list[dict[str, Any]]:
        if not participants_raw:
            return []
        try:
            parsed = json.loads(participants_raw)
        except (TypeError, json.JSONDecodeError):
            return []
        return parsed if isinstance(parsed, list) else []

    @staticmethod
    def _size_band(value: float | None) -> str:
        if value is None:
            return "Nav paredzamās cenas"
        if value < 100000:
            return "Līdz 100k"
        if value < 500000:
            return "100k-500k"
        if value < 1000000:
            return "500k-1M"
        if value < 5000000:
            return "1M-5M"
        return "5M+"

    @staticmethod
    def _rank_counter_rows(counter: Counter, label: str) -> list[dict[str, Any]]:
        rows = [{"name": name, "count": count, "label": label} for name, count in counter.most_common(10)]
        return rows

    @staticmethod
    def _rank_amount_map(amounts: dict[str, float], highlight_name: str | None = None) -> list[dict[str, Any]]:
        rows = []
        for name, amount in amounts.items():
            rows.append(
                {
                    "name": name,
                    "awarded_sum_eur": round(amount, 2),
                    "is_selected": name == highlight_name,
                }
            )
        rows.sort(key=lambda item: (-item["awarded_sum_eur"], item["name"]))
        return rows

    @staticmethod
    def _decision_lag_days(bid_deadline: str | None, decision_date: str | None) -> int | None:
        if not bid_deadline or not decision_date:
            return None
        deadline_formats = ("%Y-%m-%dT%H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y")
        decision_formats = ("%Y-%m-%d", "%d.%m.%Y")
        deadline_dt = None
        decision_dt = None
        for fmt in deadline_formats:
            try:
                deadline_dt = datetime.strptime(bid_deadline, fmt)
                break
            except ValueError:
                continue
        for fmt in decision_formats:
            try:
                decision_dt = datetime.strptime(decision_date, fmt)
                break
            except ValueError:
                continue
        if deadline_dt is None or decision_dt is None:
            return None
        return (decision_dt.date() - deadline_dt.date()).days


# ---------------------------------------------------------------------------
# Postgres-backed analytics repository
# ---------------------------------------------------------------------------

CPV_CATEGORY_MAP: dict[str, str] = {
    "03": "Lauksaimniecība",
    "09": "Enerģija",
    "14": "Izrakteņi",
    "15": "Pārtika",
    "16": "Lauksaimniecības tehnika",
    "18": "Apģērbs",
    "22": "Drukātie materiāli",
    "24": "Ķīmija",
    "30": "IT tehnika",
    "31": "Elektrotehnika",
    "32": "Radio un sakari",
    "33": "Medicīnas aprīkojums",
    "34": "Transports",
    "35": "Drošība",
    "37": "Sports",
    "38": "Laboratorijas iekārtas",
    "39": "Mēbeles",
    "41": "Ūdens apgāde",
    "42": "Ražošanas iekārtas",
    "43": "Derīkteņu ieguve",
    "44": "Celtniecības materiāli",
    "45": "Celtniecība",
    "48": "Programmatūra",
    "50": "Uzturēšana un remonts",
    "51": "Montāža",
    "55": "Viesnīcas un ēdināšana",
    "60": "Transports (pakalpojumi)",
    "63": "Loģistika",
    "64": "Sakaru pakalpojumi",
    "65": "Komunālie pakalpojumi",
    "66": "Finanšu pakalpojumi",
    "70": "Nekustamais īpašums",
    "71": "Inženierpakalpojumi",
    "72": "IT pakalpojumi",
    "73": "Pētniecība",
    "75": "Valsts pārvalde",
    "76": "Naftas nozare",
    "77": "Mežsaimniecība",
    "79": "Konsultācijas",
    "80": "Izglītība",
    "85": "Veselības aprūpe",
    "90": "Vides pakalpojumi",
    "92": "Kultūra",
    "98": "Citi pakalpojumi",
}


def cpv_to_category(cpv: str | None) -> str:
    if not cpv:
        return "unknown"
    code = cpv.strip().lstrip('="')[:2]
    return CPV_CATEGORY_MAP.get(code, f"CPV {code}xxx")


class PostgresAnalyticsRepository:
    """Analytics repository backed by Postgres CKAN tables."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def fetch_rows(self, filters: Filters) -> list[dict[str, Any]]:
        import psycopg

        where_clauses = [
            "p.status IN ('Līgums noslēgts','Noslēgts','Lēmums pieņemts','Uzsākta līguma slēgšana')"
        ]
        params: dict[str, Any] = {}

        if filters.year is not None:
            where_clauses.append(
                "EXTRACT(YEAR FROM TO_DATE(NULLIF(p.submission_deadline,''), 'DD.MM.YYYY'))::int = %(year)s"
            )
            params["year"] = filters.year

        if filters.multi_lot is not None:
            if filters.multi_lot:
                where_clauses.append("p.has_lots = 'Jā'")
            else:
                where_clauses.append("(p.has_lots IS NULL OR p.has_lots != 'Jā')")

        if filters.buyer:
            where_clauses.append("p.buyer = %(buyer)s")
            params["buyer"] = filters.buyer

        where_sql = " AND ".join(where_clauses)

        sql = f"""
SELECT
    p.procurement_id,
    EXTRACT(YEAR FROM TO_DATE(NULLIF(p.submission_deadline,''), 'DD.MM.YYYY'))::int AS year,
    p.cpv_main,
    p.cpv_additional,
    p.buyer                     AS purchaser_name,
    p.region                    AS delivery_location,
    p.estimated_value_eur,
    p.title                     AS procurement_name,
    p.status                    AS procurement_status,
    p.submission_deadline       AS bid_deadline,
    p.winner_selection_method   AS evaluation_method,
    p.eu_project_reference,
    p.has_lots,
    r.winner_name               AS procurement_winner,
    r.winner_reg_number         AS procurement_winner_registration_no,
    r.contract_value_eur        AS procurement_winner_suggested_price_eur,
    r.contract_signed_date      AS decision_date,
    COALESCE(pc.participant_count, 0) AS procurement_participants_count
FROM procurements p
LEFT JOIN LATERAL (
    SELECT winner_name, winner_reg_number, contract_value_eur, contract_signed_date
    FROM ckan_results
    WHERE procurement_id = p.procurement_id
    ORDER BY contract_value_eur DESC NULLS LAST
    LIMIT 1
) r ON true
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS participant_count
    FROM ckan_participants
    WHERE procurement_id = p.procurement_id
) pc ON true
WHERE {where_sql}
ORDER BY p.submission_deadline DESC, p.procurement_id DESC
"""
        with psycopg.connect(self.dsn) as conn:
            cur = conn.execute(cast(Any, sql), params)
            raw_rows = cur.fetchall()
            col_names = [desc.name for desc in cur.description or ()]

        rows: list[dict[str, Any]] = []
        for raw in raw_rows:
            row = dict(zip(col_names, raw, strict=False))
            row["estimated_value_eur"] = safe_float(row.get("estimated_value_eur"))
            row["procurement_winner_suggested_price_eur"] = safe_float(
                row.get("procurement_winner_suggested_price_eur")
            )
            row["procurement_participants_count"] = safe_int(row.get("procurement_participants_count"))
            row["is_multi_lot"] = row.get("has_lots") == "Jā"
            row["lot_count"] = None
            row["lots_json"] = None
            row["location_bucket"] = extract_location_bucket(row.get("delivery_location"))
            row["planning_region"] = derive_planning_region(row.get("delivery_location"))
            row["buyer_normalized"] = normalize_party_name(row.get("purchaser_name"))
            row["winner_normalized"] = normalize_party_name(row.get("procurement_winner"))
            row["winner_display_name"] = row["winner_normalized"]
            row["category"] = cpv_to_category(row.get("cpv_main"))
            row["procurement_participants_json"] = None  # filled in second query below
            row["funding_source"] = None
            row["procurement_status_from_report"] = None
            row["report_document_path"] = None
            row["contract_scope_type"] = None
            row["classification_final_category"] = None
            row["winner_company_id"] = None
            if filters.planning_region and row["planning_region"] != filters.planning_region:
                continue
            if filters.category and row["category"] != filters.category:
                continue
            rows.append(row)

        if not rows:
            return rows

        # Second query: fetch participants for participant-level analytics
        ids = [row["procurement_id"] for row in rows]
        id_to_participants: dict[str, list[dict[str, Any]]] = defaultdict(list)
        with psycopg.connect(self.dsn) as conn:
            part_rows = conn.execute(
                """
                SELECT procurement_id, participant_name, participant_reg_number
                FROM ckan_participants
                WHERE procurement_id = ANY(%(ids)s)
                """,
                {"ids": ids},
            ).fetchall()
        for part_row in part_rows:
            pid, pname, preg = part_row
            id_to_participants[pid].append(
                {"name": pname, "reg_no": preg, "suggested_price_eur": None}
            )
        for row in rows:
            participants = id_to_participants.get(row["procurement_id"], [])
            row["procurement_participants_json"] = json.dumps(participants, ensure_ascii=False)

        return rows

    def available_filters(self) -> dict[str, Any]:
        import psycopg

        with psycopg.connect(self.dsn) as conn:
            year_rows = conn.execute(
                """
                SELECT DISTINCT EXTRACT(YEAR FROM TO_DATE(NULLIF(submission_deadline,''), 'DD.MM.YYYY'))::int AS y
                FROM procurements
                WHERE submission_deadline IS NOT NULL AND submission_deadline != ''
                ORDER BY y
                """
            ).fetchall()
            buyer_rows = conn.execute(
                "SELECT DISTINCT buyer FROM procurements WHERE buyer IS NOT NULL ORDER BY buyer"
            ).fetchall()
            region_rows = conn.execute(
                "SELECT DISTINCT region FROM procurements WHERE region IS NOT NULL ORDER BY region"
            ).fetchall()

        years = [r[0] for r in year_rows if r[0] is not None]
        buyers = [r[0] for r in buyer_rows]
        regions = sorted({derive_planning_region(r[0]) for r in region_rows if r[0]})
        categories = sorted(set(CPV_CATEGORY_MAP.values()))
        return {"years": years, "buyers": buyers, "planning_regions": regions, "categories": categories}

    @staticmethod
    def _derive_category(row: dict[str, Any]) -> str:
        return cpv_to_category(row.get("cpv_main"))
