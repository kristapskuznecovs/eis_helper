#!/usr/bin/env python3
"""Heuristic helpers for collector classification and CKAN row normalization."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from .utils import normalize_text


def clean_cell(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if (text.startswith('="') and text.endswith('"')) or (
        text.startswith("='") and text.endswith("'")
    ):
        return text[2:-1]
    return text


def normalize_cpv_code(value: Any) -> str | None:
    if value is None:
        return None
    text = str(clean_cell(value)).strip().strip('"').strip("'")
    text = text.lstrip("=")
    match = re.search(r"(\d{8}-\d)", text)
    if not match:
        return None
    return match.group(1)


def extract_year_from_resource(resource_name: str) -> int | None:
    match = re.search(r"(20\d{2})", resource_name)
    return int(match.group(1)) if match else None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(clean_cell(value)).strip()
    if not text:
        return None
    text = text.replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def canonical_procurement_url(url_value: Any, procurement_id: Any) -> str | None:
    url_text = str(clean_cell(url_value or "")).strip()
    if not url_text:
        if procurement_id in (None, ""):
            return None
        url_text = f"https://www.eis.gov.lv/EKEIS/Supplier/Procurement/{clean_cell(procurement_id)}"
    elif url_text.startswith("/"):
        url_text = "https://www.eis.gov.lv" + url_text

    parsed = urlparse(url_text)
    if not parsed.scheme:
        url_text = "https://" + url_text.lstrip("/")
        parsed = urlparse(url_text)
    if parsed.netloc == "eis.gov.lv":
        parsed = parsed._replace(netloc="www.eis.gov.lv")
        url_text = parsed.geturl()
    return url_text


def keyword_matches(text: str, keywords: list[str]) -> list[str]:
    return [kw for kw in keywords if kw in text]


def classify_project_heuristic(project: dict[str, Any]) -> tuple[str, str]:
    name_text = normalize_text(
        " ".join(
            [
                str(project.get("procurement_name") or ""),
                str(project.get("procurement_identification_number") or ""),
            ]
        )
    )
    context_text = normalize_text(
        " ".join(
            [
                str(project.get("procurement_name") or ""),
                str(project.get("cpv_main") or ""),
                str(project.get("procurement_identification_number") or ""),
            ]
        )
    )
    cpv_code = normalize_cpv_code(project.get("cpv_main"))
    cpv_prefix4 = cpv_code[:4] if cpv_code else ""

    infrastructure_keywords = [
        "tilt",
        "cela",
        "celu",
        "iela",
        "autoce",
        "viadukt",
        "parvads",
        "caurteka",
        "infrastrukt",
        "inzenierkomunik",
        "udensvad",
        "kanaliz",
        "notekuden",
        "siltumtikl",
        "elektrotikl",
        "dzelzcel",
        "osta",
        "lidosta",
        "melioracij",
    ]
    building_keywords = [
        "eka",
        "eku celtniec",
        "skola",
        "bernudarz",
        "slimnic",
        "poliklin",
        "bibliotek",
        "muzej",
        "teatr",
        "dzivojam",
        "angar",
        "noliktav",
        "korpuss",
        "sporta hal",
        "administrativa",
    ]
    renovation_keywords = [
        "renovac",
        "atjaunos",
        "rekonstruk",
        "parbuv",
        "restaur",
        "remont",
        "energoefektiv",
        "siltinas",
        "moderniz",
    ]
    new_keywords = [
        "jaunbuv",
        "izbuv",
        "buvniec",
        "buvdarb",
        "jauna",
    ]

    matched_infra = keyword_matches(context_text, infrastructure_keywords)
    matched_building = keyword_matches(context_text, building_keywords)
    matched_renovation = keyword_matches(name_text, renovation_keywords)
    matched_new = keyword_matches(name_text, new_keywords)

    cpv_building = cpv_prefix4 == "4521"
    cpv_infrastructure = cpv_prefix4 in {"4522", "4523", "4524", "4525", "4526", "4529"}

    is_infrastructure = bool(matched_infra) or cpv_infrastructure
    is_building = bool(matched_building) or cpv_building
    is_renovation = bool(matched_renovation)
    is_new = bool(matched_new)

    label = "unknown"
    if is_infrastructure and is_renovation:
        label = "infrastructure_renovation"
    elif is_infrastructure and (is_new or not is_renovation):
        label = "infrastructure_new"
    elif is_building and is_renovation:
        label = "renovation"
    elif is_building and (is_new or not is_renovation):
        label = "new_building"
    elif is_renovation and not is_infrastructure:
        label = "renovation"
    elif is_new and is_infrastructure:
        label = "infrastructure_new"
    elif is_new and not is_infrastructure:
        label = "new_building"

    reasons: list[str] = []
    if cpv_code:
        reasons.append(f"cpv={cpv_code}")
    if matched_infra:
        reasons.append(f"infra_kw={','.join(matched_infra[:4])}")
    if matched_building:
        reasons.append(f"building_kw={','.join(matched_building[:4])}")
    if matched_renovation:
        reasons.append(f"renov_kw={','.join(matched_renovation[:4])}")
    if matched_new:
        reasons.append(f"new_kw={','.join(matched_new[:4])}")
    reason_text = "; ".join(reasons) if reasons else "no-strong-signals"
    return label, reason_text

