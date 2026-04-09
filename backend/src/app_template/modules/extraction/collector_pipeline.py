#!/usr/bin/env python3
"""Collector pipeline functions for procurement-record loading, classification, and page scanning."""

from __future__ import annotations

import concurrent.futures
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.request import Request

from .collector_classes import (
    CLASSIFICATION_FINAL_CATEGORIES,
    CKANClient,
    OpenAIClassifier,
    OutcomeLLMExtractor,
)
from .collector_heuristics import (
    clean_cell,
    normalize_cpv_code,
    extract_year_from_resource,
    to_float,
    canonical_procurement_url,
)
from .utils import extract_js_array, fetch_html, is_captcha_page, normalize_text
from .collector_outcomes import extract_final_report_outcome, is_finished_procurement_status

CPV_FIELD = "CPV_kods_galvenais_prieksmets"
def classify_procurement_records(
    procurement_records: List[Dict[str, Any]],
    classification_mode: str,
    classification_workers: int,
    openai_classifier: Optional[OpenAIClassifier],
    openai_max_projects: Optional[int],
    show_in_progress_messages: bool,
    log_every_n: int,
    on_project_classified: Optional[Any] = None,
) -> Dict[str, int]:
    if classification_mode != "openai":
        raise RuntimeError("Only 'openai' classification mode is supported.")
    if openai_classifier is None:
        raise RuntimeError("OpenAI classifier is required for classification mode 'openai'.")

    for project in procurement_records:
        project["classification_domain"] = "unknown"
        project["classification_scope_type"] = "unknown"
        project["classification_work_type"] = "unknown"
        project["classification_asset_scale"] = "unknown"
        project["classification_final_category"] = "unknown"
        project["classification_reason"] = "not_classified"

    target_indexes = list(range(len(procurement_records)))
    if openai_max_projects is not None and openai_max_projects >= 0:
        target_indexes = target_indexes[:openai_max_projects]

    lock = threading.Lock()
    processed = 0

    def worker(idx: int) -> None:
        nonlocal processed
        project = procurement_records[idx]
        try:
            normalized = openai_classifier.classify(project)
            with lock:
                project.update(normalized)
                processed += 1
                if on_project_classified is not None:
                    on_project_classified(project, processed, len(target_indexes))
                if show_in_progress_messages and log_every_n > 0 and processed % log_every_n == 0:
                    print(f"Classified with OpenAI: {processed}/{len(target_indexes)}")
        except Exception as exc:  # pragma: no cover - runtime robustness
            with lock:
                project["classification_domain"] = "unknown"
                project["classification_scope_type"] = "unknown"
                project["classification_work_type"] = "unknown"
                project["classification_asset_scale"] = "unknown"
                project["classification_final_category"] = "unknown"
                project["classification_reason"] = f"openai_error={exc}"
                processed += 1
                if on_project_classified is not None:
                    on_project_classified(project, processed, len(target_indexes))
                if show_in_progress_messages and log_every_n > 0 and processed % log_every_n == 0:
                    print(f"Classified with OpenAI: {processed}/{len(target_indexes)}")

    max_workers = max(1, classification_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(worker, target_indexes))

    return classification_counts(procurement_records)


def classification_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {label: 0 for label in sorted(CLASSIFICATION_FINAL_CATEGORIES)}
    for row in rows:
        label = str(row.get("classification_final_category") or "unknown").strip().lower()
        if label not in counts:
            label = "unknown"
        counts[label] = counts.get(label, 0) + 1
    return counts


def iter_resource_records(
    client: CKANClient,
    resource_id: str,
    batch_size: int,
    sleep_seconds: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    offset = 0
    while True:
        response = client.datastore_search(
            resource_id=resource_id,
            limit=batch_size,
            offset=offset,
            include_total=(offset == 0),
        )
        records = response["result"].get("records", [])
        if not records:
            break
        for record in records:
            yield record
        offset += len(records)
        if len(records) < batch_size:
            break
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def collect_procurement_records(
    client: CKANClient,
    package_id: str,
    from_year: int,
    to_year: Optional[int],
    cpv_prefixes: List[str],
    batch_size: int,
    min_estimated_value: Optional[float],
    max_projects: Optional[int],
) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[str, int], int]:
    package = client.package_show(package_id)["result"]
    resources = []
    for resource in package.get("resources") or []:
        if not resource.get("datastore_active"):
            continue
        year = extract_year_from_resource(str(resource.get("name") or ""))
        if year is None or year < from_year:
            continue
        if to_year is not None and year > to_year:
            continue
        resources.append((year, resource))
    resources.sort(key=lambda item: item[0])

    procurement_records_by_key: Dict[str, Dict[str, Any]] = {}
    counts_by_year: Dict[str, int] = {}
    prefilter_counts_by_year: Dict[str, int] = {}

    for year, resource in resources:
        resource_id = str(resource["id"])
        for record in iter_resource_records(client, resource_id=resource_id, batch_size=batch_size):
            cpv_code = normalize_cpv_code(record.get(CPV_FIELD))
            if cpv_code is None:
                continue
            if not any(cpv_code.startswith(prefix) for prefix in cpv_prefixes):
                continue

            procurement_id = clean_cell(record.get("Iepirkuma_ID"))
            procurement_url = canonical_procurement_url(
                record.get("Hipersaite_EIS_kura_pieejams_zinojums"),
                procurement_id=procurement_id,
            )
            if procurement_url is None:
                continue

            dedupe_key = str(procurement_id or procurement_url)
            if dedupe_key not in procurement_records_by_key:
                procurement_records_by_key[dedupe_key] = {
                    "base": {
                        "procurement_id": procurement_id,
                        "year": year,
                        "cpv_main": clean_cell(record.get("CPV_kods_galvenais_prieksmets")),
                        "cpv_additional": clean_cell(record.get("CPV_kodi_papildus_prieksmeti")),
                        "procurement_name": clean_cell(record.get("Iepirkuma_nosaukums")),
                        "procurement_identification_number": clean_cell(
                            record.get("Iepirkuma_identifikacijas_numurs")
                        ),
                        "purchaser_name": clean_cell(record.get("Pasutitaja_nosaukums")),
                        "purchaser_registration_no": clean_cell(record.get("Pasutitaja_registracijas_numurs")),
                        "procurement_subject_type": clean_cell(record.get("Iepirkuma_prieksmeta_veids")),
                        "procurement_status": clean_cell(record.get("Iepirkuma_statuss")),
                        "publish_date": clean_cell(record.get("Iepirkuma_izsludinasanas_datums")),
                        "submission_date": clean_cell(record.get("Piedavajumu_iesniegsanas_datums")),
                        "submission_time": clean_cell(record.get("Piedavajumu_iesniegsanas_laiks")),
                        "delivery_location": clean_cell(record.get("Precu_vai_pakalpojumu_sniegsanas_vieta")),
                        "planned_contract_term_type": clean_cell(
                            record.get("Planotais_liguma_darbibas_termina_termina_veids")
                        ),
                        "planned_contract_term": clean_cell(record.get("Planotais_liguma_darbibas_termins")),
                        "planned_contract_term_unit": clean_cell(
                            record.get("Planota_liguma_darbibas_termina_mervieniba")
                        ),
                        "planned_execution_from": clean_cell(record.get("Planota_liguma_izpilde_no")),
                        "planned_execution_to": clean_cell(record.get("Planota_liguma_izpilde_lidz")),
                        "procedure_type": clean_cell(record.get("Proceduras_veids")),
                        "contact_person": clean_cell(record.get("Pasutitaja_kontaktpersona")),
                        "submission_language": clean_cell(record.get("Piedavajuma_iesniegsanas_valoda")),
                        "variants_allowed": clean_cell(record.get("Vai_pielaujami_piedavajuma_varianti")),
                        "winner_selection_method": clean_cell(record.get("Uzvaretaja_izveles_metode")),
                        "has_lots": clean_cell(record.get("Ir_dalijums_dalas")),
                        "procurement_url": procurement_url,
                        "iub_publication_url": clean_cell(record.get("Hipersaite_uz_IUB_publikaciju")),
                        "source_resource_id": resource_id,
                    },
                    "project_values": [],
                    "lot_values": {},
                    "source_api_records": [],
                }
                prefilter_counts_by_year[str(year)] = prefilter_counts_by_year.get(str(year), 0) + 1

            item = procurement_records_by_key[dedupe_key]
            cleaned_record = {str(key): clean_cell(value) for key, value in record.items()}
            cleaned_record["source_resource_id"] = resource_id
            cleaned_record["source_year"] = year
            item["source_api_records"].append(cleaned_record)
            project_currency = str(clean_cell(record.get("Ligumcenas_valuta") or "")).upper()
            if project_currency in ("", "EUR"):
                for field_name in ("Planota_ligumcena", "Planota_ligumcena_lidz", "Planota_ligumcena_no"):
                    val = to_float(record.get(field_name))
                    if val is not None:
                        item["project_values"].append(val)

            lot_currency = str(clean_cell(record.get("Dalas_ligumcenas_valuta") or "")).upper()
            if lot_currency in ("", "EUR"):
                lot_value: Optional[float] = None
                for field_name in (
                    "Dalas_planota_ligumcena",
                    "Dalas_planota_ligumcena_lidz",
                    "Dalas_planota_ligumcena_no",
                ):
                    val = to_float(record.get(field_name))
                    if val is not None:
                        lot_value = val
                        break
                if lot_value is not None:
                    lot_key = clean_cell(record.get("Iepirkuma_dalas_nr"))
                    lot_key_str = str(lot_key) if lot_key not in (None, "") else f"row_{record.get('_id')}"
                    previous = item["lot_values"].get(lot_key_str)
                    item["lot_values"][lot_key_str] = lot_value if previous is None else max(previous, lot_value)

    procurement_records: List[Dict[str, Any]] = []
    for item in procurement_records_by_key.values():
        base = item["base"]
        project_values = item["project_values"]
        lot_values = list(item["lot_values"].values())

        project_est = max(project_values) if project_values else None
        lot_sum = sum(lot_values) if lot_values else None

        if project_est is not None and lot_sum is not None:
            estimated_value = max(project_est, lot_sum)
            estimated_source = "max(project, lots_sum)"
        elif project_est is not None:
            estimated_value = project_est
            estimated_source = "project"
        elif lot_sum is not None:
            estimated_value = lot_sum
            estimated_source = "lots_sum"
        else:
            estimated_value = None
            estimated_source = "unknown"

        if min_estimated_value is not None:
            if estimated_value is None or estimated_value < min_estimated_value:
                continue

        procurement_record = dict(base)
        procurement_record["estimated_value_eur"] = estimated_value
        procurement_record["estimated_value_source"] = estimated_source
        procurement_record["raw_api_record_count"] = len(item["source_api_records"])
        procurement_record["raw_api_records"] = item["source_api_records"]
        procurement_records.append(procurement_record)
        year_key = str(procurement_record.get("year") or "unknown")
        counts_by_year[year_key] = counts_by_year.get(year_key, 0) + 1

        if max_projects is not None and len(procurement_records) >= max_projects:
            break

    procurement_records.sort(key=lambda item: (item.get("year") or 0, str(item.get("procurement_id") or "")))
    return (
        procurement_records,
        dict(sorted(counts_by_year.items())),
        dict(sorted(prefilter_counts_by_year.items())),
        sum(prefilter_counts_by_year.values()),
    )


def scan_project_for_docs(
    project: Dict[str, Any],
    compiled_title_patterns: List[re.Pattern[str]],
    include_historical: bool,
    request_timeout_seconds: int,
    cookie_header: Optional[str],
    extract_final_report_outcomes: bool,
    outcome_extraction_mode: str,
    outcome_llm_extractor: Optional[OutcomeLLMExtractor],
) -> Dict[str, Any]:
    url = str(project.get("procurement_url") or "")
    html_text = fetch_html(
        url=url,
        timeout_seconds=request_timeout_seconds,
        cookie_header=cookie_header,
    )
    if is_captcha_page(html_text):
        raise RuntimeError("captcha_challenge")

    actual_docs = extract_js_array(html_text, "ActualDocuments_items")
    historical_docs = extract_js_array(html_text, "HistoricalDocuments_items") if include_historical else []

    matched_titles: List[str] = []
    for section, docs in (("actual", actual_docs), ("historical", historical_docs)):
        for doc in docs:
            title = str(doc.get("Title") or "").strip()
            if not title:
                continue
            normalized_title = normalize_text(title)
            if any(pattern.search(normalized_title) for pattern in compiled_title_patterns):
                matched_titles.append(f"{section}:{title}")

    result = dict(project)
    result["matched_document_count"] = len(matched_titles)
    result["matched_document_titles"] = matched_titles
    if extract_final_report_outcomes:
        result.update(
            extract_final_report_outcome(
                project=project,
                actual_docs=actual_docs,
                request_timeout_seconds=request_timeout_seconds,
                cookie_header=cookie_header,
                procurement_page_html=html_text,
                outcome_extraction_mode=outcome_extraction_mode,
                outcome_llm_extractor=outcome_llm_extractor,
            )
        )
    else:
        result["procurement_finished"] = is_finished_procurement_status(project.get("procurement_status"))
        result["procurement_winner"] = None
        result["procurement_winner_registration_no"] = None
        result["procurement_winner_suggested_price_eur"] = None
        result["procurement_winner_source"] = None
        result["procurement_participants_count"] = 0
        result["procurement_participants"] = []
        result["procurement_outcome_description"] = "final_report_outcome_extraction_disabled"
    return result
