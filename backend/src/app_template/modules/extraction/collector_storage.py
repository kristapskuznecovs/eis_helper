#!/usr/bin/env python3
"""SQLite storage for CPV-filtered procurement records."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

TABLE_NAME = "procurement_records"
OPEN_DATA_PORTAL_FIELDS = [
    "_id",
    "Iepirkuma_ID",
    "Iepirkuma_nosaukums",
    "Iepirkuma_identifikacijas_numurs",
    "Pasutitaja_nosaukums",
    "Pasutitaja_registracijas_numurs",
    "Pasutitaja_registracijas_numura_veids",
    "Pasutitaja_PVS_ID",
    "Augstak_stavosa_organizacija",
    "Citu_pasutitaju_vajadzibam",
    "Faktiskais_sanemejs",
    "Iepirkuma_prieksmeta_veids",
    "CPV_kods_galvenais_prieksmets",
    "CPV_kodi_papildus_prieksmeti",
    "Iepirkuma_statuss",
    "Iepirkuma_izsludinasanas_datums",
    "Piedavajumu_iesniegsanas_datums",
    "Piedavajumu_iesniegsanas_laiks",
    "Ieintereseto_personu_sanaksmes",
    "Precu_vai_pakalpojumu_sniegsanas_vieta",
    "Planotais_liguma_darbibas_termina_termina_veids",
    "Planotais_liguma_darbibas_termins",
    "Planota_liguma_darbibas_termina_mervieniba",
    "Planota_liguma_izpilde_no",
    "Planota_liguma_izpilde_lidz",
    "Ligumcenas_veids",
    "Planota_ligumcena",
    "Planota_ligumcena_no",
    "Planota_ligumcena_lidz",
    "Ligumcenas_valuta",
    "Regulejasais_tiesibu_akts",
    "Proceduras_veids",
    "Pasutitaja_kontaktpersona",
    "Piedavajuma_iesniegsanas_valoda",
    "Atsauce_uz_ES_projektiem_un_programmam",
    "Vai_pielaujami_piedavajuma_varianti",
    "Uzvaretaja_izveles_metode",
    "Iesniegsanas_vieta",
    "Hipersaite_EIS_kura_pieejams_zinojums",
    "Hipersaite_uz_IUB_publikaciju",
    "Ir_dalijums_dalas",
    "Dalu_iesniegsanas_nosacijumi",
    "Iepirkuma_dalas_nr",
    "Iepirkuma_dalas_nosaukums",
    "Iepirkuma_dalas_statuss",
    "Dalas_planotais_liguma_darbibas_termina_termina_veids",
    "Dalas_planotais_liguma_darbibas_termins",
    "Dalas_planota_liguma_darbibas_termina_mervieniba",
    "Dalas_planota_liguma_izpilde_no",
    "Dalas_planota_liguma_izpilde_lidz",
    "Dalas_ligumcenas_veids",
    "Dalas_planota_ligumcena",
    "Dalas_planota_ligumcena_no",
    "Dalas_planota_ligumcena_lidz",
    "Dalas_ligumcenas_valuta",
    "rank",
]

SCHEMA_COLUMNS = {
    "procurement_record_key": "TEXT PRIMARY KEY",
    "procurement_id": "TEXT",
    "procurement_url": "TEXT NOT NULL",
    "year": "INTEGER",
    "purchaser_name": "TEXT",
    "purchaser_registration_no": "TEXT",
    "procurement_name": "TEXT",
    "procurement_status": "TEXT",
    "procurement_identification_number": "TEXT",
    "procurement_subject_type": "TEXT",
    "cpv_main": "TEXT",
    "cpv_additional": "TEXT",
    "submission_date": "TEXT",
    "submission_time": "TEXT",
    "delivery_location": "TEXT",
    "planned_contract_term_type": "TEXT",
    "planned_contract_term": "TEXT",
    "planned_contract_term_unit": "TEXT",
    "planned_execution_from": "TEXT",
    "planned_execution_to": "TEXT",
    "procedure_type": "TEXT",
    "contact_person": "TEXT",
    "submission_language": "TEXT",
    "variants_allowed": "TEXT",
    "winner_selection_method": "TEXT",
    "has_lots": "TEXT",
    "iub_publication_url": "TEXT",
    "estimated_value_eur": "REAL",
    "estimated_value_source": "TEXT",
    "classification_domain": "TEXT",
    "classification_scope_type": "TEXT",
    "classification_work_type": "TEXT",
    "classification_asset_scale": "TEXT",
    "classification_final_category": "TEXT",
    "classification_reason": "TEXT",
    "report_document_path": "TEXT",
    "report_document_type": "TEXT",
    "report_original_archive": "TEXT",
    "report_download_status": "TEXT",
    "report_download_error": "TEXT",
    "report_downloaded_at": "TEXT",
    "report_download_url": "TEXT",
    "report_selection_method": "TEXT",
    "report_selection_reason": "TEXT",
    "report_selected_title": "TEXT",
    "is_eps_publication": "TEXT",
    "ingested_at": "TEXT",
    "classified_at": "TEXT",
    "classifier_model": "TEXT",
    "source_resource_id": "TEXT",
    "source_year": "INTEGER",
    "raw_api_record_count": "INTEGER",
    "raw_api_records_json": "TEXT",
    "raw_procurement_record_json": "TEXT NOT NULL",
    # Outcome extraction fields (from final reports)
    "procurement_status_from_report": "TEXT",
    "procurement_winner": "TEXT",
    "procurement_winner_registration_no": "TEXT",
    "procurement_winner_suggested_price_eur": "REAL",
    "procurement_winner_source": "TEXT",
    "procurement_participants_count": "INTEGER",
    "procurement_participants_json": "TEXT",
    "procurement_outcome_description": "TEXT",
    "extracted_at": "TEXT",
    # Enhanced outcome extraction fields
    "bid_deadline": "TEXT",
    "decision_date": "TEXT",
    "funding_source": "TEXT",
    "eu_project_reference": "TEXT",
    # Multi-lot procurement support
    "is_multi_lot": "BOOLEAN",
    "lot_count": "INTEGER",
    "lots_json": "TEXT",
    "evaluation_method": "TEXT",
    "contract_scope_type": "TEXT",
    "disqualified_participants_json": "TEXT",
    "subcontractors_json": "TEXT",
    "evaluation_criteria_json": "TEXT",
    # Company identity index (populated by pipelines/build_company_index.py)
    "winner_company_id": "INTEGER",
}
for portal_field in OPEN_DATA_PORTAL_FIELDS:
    SCHEMA_COLUMNS[portal_field] = "REAL" if portal_field == "rank" else "TEXT"


def _first_nonempty_portal_value(raw_api_records: List[Dict[str, Any]], field_name: str) -> Any:
    for record in raw_api_records:
        value = record.get(field_name)
        if value not in (None, ""):
            return value
    return None


def _build_storage_row(
    procurement_record: Dict[str, Any],
    *,
    ingested_at: str,
    classified_at: str,
    classifier_model: str,
) -> Dict[str, Any]:
    row = dict(procurement_record)
    raw_api_records = row.get("raw_api_records")
    if not isinstance(raw_api_records, list):
        raw_api_records = []

    storage_row: Dict[str, Any] = {
        "procurement_record_key": procurement_record_storage_key(row),
        "procurement_id": str(row.get("procurement_id") or "") or None,
        "procurement_url": str(row.get("procurement_url") or ""),
        "year": row.get("year"),
        "purchaser_name": row.get("purchaser_name"),
        "purchaser_registration_no": row.get("purchaser_registration_no"),
        "procurement_name": row.get("procurement_name"),
        "procurement_status": row.get("procurement_status"),
        "procurement_identification_number": row.get("procurement_identification_number"),
        "procurement_subject_type": row.get("procurement_subject_type"),
        "cpv_main": row.get("cpv_main"),
        "cpv_additional": row.get("cpv_additional"),
        "submission_date": row.get("submission_date"),
        "submission_time": row.get("submission_time"),
        "delivery_location": row.get("delivery_location"),
        "planned_contract_term_type": row.get("planned_contract_term_type"),
        "planned_contract_term": row.get("planned_contract_term"),
        "planned_contract_term_unit": row.get("planned_contract_term_unit"),
        "planned_execution_from": row.get("planned_execution_from"),
        "planned_execution_to": row.get("planned_execution_to"),
        "procedure_type": row.get("procedure_type"),
        "contact_person": row.get("contact_person"),
        "submission_language": row.get("submission_language"),
        "variants_allowed": row.get("variants_allowed"),
        "winner_selection_method": row.get("winner_selection_method"),
        "has_lots": row.get("has_lots"),
        "iub_publication_url": row.get("iub_publication_url"),
        "estimated_value_eur": row.get("estimated_value_eur"),
        "estimated_value_source": row.get("estimated_value_source"),
        "classification_domain": row.get("classification_domain"),
        "classification_scope_type": row.get("classification_scope_type"),
        "classification_work_type": row.get("classification_work_type"),
        "classification_asset_scale": row.get("classification_asset_scale"),
        "classification_final_category": row.get("classification_final_category"),
        "classification_reason": row.get("classification_reason"),
        "report_document_path": row.get("report_document_path"),
        "report_document_type": row.get("report_document_type"),
        "report_original_archive": row.get("report_original_archive"),
        "report_download_status": row.get("report_download_status") or "not_processed",
        "report_download_error": row.get("report_download_error"),
        "report_downloaded_at": row.get("report_downloaded_at"),
        "report_download_url": row.get("report_download_url"),
        "report_selection_method": row.get("report_selection_method") or "not_processed",
        "report_selection_reason": row.get("report_selection_reason")
        or "Downloader has not processed this procurement record yet.",
        "report_selected_title": row.get("report_selected_title") or "not_processed",
        "is_eps_publication": row.get("is_eps_publication") or "unknown",
        "ingested_at": ingested_at,
        "classified_at": classified_at,
        "classifier_model": classifier_model,
        "source_resource_id": row.get("source_resource_id"),
        "source_year": row.get("year"),
        "raw_api_record_count": row.get("raw_api_record_count"),
        "raw_api_records_json": json.dumps(raw_api_records, ensure_ascii=False),
        "raw_procurement_record_json": json.dumps(row, ensure_ascii=False),
        # Outcome extraction fields — NULL until extract_from_documents.py runs
        "procurement_status_from_report": None,
        "procurement_winner": None,
        "procurement_winner_registration_no": None,
        "procurement_winner_suggested_price_eur": None,
        "procurement_winner_source": None,
        "procurement_participants_count": None,
        "procurement_participants_json": None,
        "procurement_outcome_description": None,
        "extracted_at": None,
        "bid_deadline": None,
        "decision_date": None,
        "funding_source": None,
        "eu_project_reference": None,
        "is_multi_lot": None,
        "lot_count": None,
        "lots_json": None,
        "evaluation_method": None,
        "contract_scope_type": None,
        "disqualified_participants_json": None,
        "subcontractors_json": None,
        "evaluation_criteria_json": None,
        "winner_company_id": None,
    }
    for portal_field in OPEN_DATA_PORTAL_FIELDS:
        portal_value = _first_nonempty_portal_value(raw_api_records, portal_field)
        if portal_field == "rank" and portal_value not in (None, ""):
            try:
                portal_value = float(portal_value)
            except (TypeError, ValueError):
                portal_value = None
        storage_row[portal_field] = portal_value
    return storage_row


def ensure_procurement_record_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        existing_tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "candidate_projects" in existing_tables and TABLE_NAME not in existing_tables:
            conn.execute(f"ALTER TABLE candidate_projects RENAME TO {TABLE_NAME}")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                procurement_record_key TEXT PRIMARY KEY,
                procurement_id TEXT,
                procurement_url TEXT NOT NULL,
                year INTEGER,
                purchaser_name TEXT,
                purchaser_registration_no TEXT,
                procurement_name TEXT,
                procurement_status TEXT,
                procurement_identification_number TEXT,
                procurement_subject_type TEXT,
                cpv_main TEXT,
                cpv_additional TEXT,
                submission_date TEXT,
                submission_time TEXT,
                delivery_location TEXT,
                planned_contract_term_type TEXT,
                planned_contract_term TEXT,
                planned_contract_term_unit TEXT,
                planned_execution_from TEXT,
                planned_execution_to TEXT,
                procedure_type TEXT,
                contact_person TEXT,
                submission_language TEXT,
                variants_allowed TEXT,
                winner_selection_method TEXT,
                has_lots TEXT,
                iub_publication_url TEXT,
                estimated_value_eur REAL,
                estimated_value_source TEXT,
                classification_domain TEXT,
                classification_scope_type TEXT,
                classification_work_type TEXT,
                classification_asset_scale TEXT,
                classification_final_category TEXT,
                classification_reason TEXT,
                ingested_at TEXT,
                classified_at TEXT,
                classifier_model TEXT,
                source_resource_id TEXT,
                raw_api_record_count INTEGER,
                raw_api_records_json TEXT,
                raw_procurement_record_json TEXT NOT NULL
            )
            """
        )
        existing_columns = {
            row[1] for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
        }
        for column_name, column_type in SCHEMA_COLUMNS.items():
            if column_name not in existing_columns:
                conn.execute(
                    f"ALTER TABLE {TABLE_NAME} ADD COLUMN {column_name} {column_type}"
                )
        if "candidate_key" in existing_columns and "procurement_record_key" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {TABLE_NAME} RENAME COLUMN candidate_key TO procurement_record_key"
            )
        if "raw_candidate_json" in existing_columns and "raw_procurement_record_json" not in existing_columns:
            conn.execute(
                f"ALTER TABLE {TABLE_NAME} RENAME COLUMN raw_candidate_json TO raw_procurement_record_json"
            )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_procurement_records_year ON {TABLE_NAME}(year)"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_procurement_records_cpv_main ON {TABLE_NAME}(cpv_main)"
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_procurement_records_final_category
            ON {TABLE_NAME}(classification_final_category)
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_procurement_records_domain ON {TABLE_NAME}(classification_domain)"
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_procurement_records_scope_type
            ON {TABLE_NAME}(classification_scope_type)
            """
        )


def procurement_record_storage_key(procurement_record: Dict[str, Any]) -> str:
    procurement_id = str(procurement_record.get("procurement_id") or "").strip()
    if procurement_id:
        return f"id:{procurement_id}"
    procurement_url = str(procurement_record.get("procurement_url") or "").strip()
    if procurement_url:
        return f"url:{procurement_url}"
    raise RuntimeError("Procurement record is missing both procurement_id and procurement_url")


def upsert_procurement_records(
    db_path: Path,
    procurement_records: Iterable[Dict[str, Any]],
    *,
    ingested_at: str,
    classified_at: str,
    classifier_model: str,
) -> int:
    ensure_procurement_record_database(db_path)
    rows_written = 0
    ordered_columns = list(SCHEMA_COLUMNS.keys())
    placeholders = ", ".join("?" for _ in ordered_columns)
    update_assignments = ", ".join(
        f"{column} = excluded.{column}" for column in ordered_columns if column != "procurement_record_key"
    )
    with sqlite3.connect(db_path) as conn:
        for procurement_record in procurement_records:
            storage_row = _build_storage_row(
                procurement_record,
                ingested_at=ingested_at,
                classified_at=classified_at,
                classifier_model=classifier_model,
            )
            conn.execute(
                f"""
                INSERT INTO {TABLE_NAME} ({", ".join(ordered_columns)})
                VALUES ({placeholders})
                ON CONFLICT(procurement_record_key) DO UPDATE SET
                    {update_assignments}
                """,
                [storage_row[column] for column in ordered_columns],
            )
            rows_written += 1
        conn.commit()
    return rows_written


def initialize_procurement_records(
    db_path: Path,
    records: Iterable[Dict[str, Any]],
    *,
    ingested_at: str,
) -> int:
    prepared_records: List[Dict[str, Any]] = []
    for record in records:
        row = dict(record)
        row.setdefault("classification_domain", "unknown")
        row.setdefault("classification_scope_type", "unknown")
        row.setdefault("classification_work_type", "unknown")
        row.setdefault("classification_asset_scale", "unknown")
        row.setdefault("classification_final_category", "unknown")
        row.setdefault("classification_reason", "not_classified")
        prepared_records.append(row)
    return upsert_procurement_records(
        db_path,
        prepared_records,
        ingested_at=ingested_at,
        classified_at="",
        classifier_model="",
    )


def update_extraction_results(
    db_path: Path,
    extraction_results: Iterable[Dict[str, Any]],
    *,
    extracted_at: str,
) -> int:
    """Update procurement records with LLM extraction results.

    Args:
        db_path: Path to SQLite database
        extraction_results: Iterable of dicts with procurement_id and extracted fields
        extracted_at: ISO timestamp when extraction was performed

    Returns:
        Number of records updated
    """
    ensure_procurement_record_database(db_path)
    rows_updated = 0

    with sqlite3.connect(db_path) as conn:
        for result in extraction_results:
            procurement_id = result.get("procurement_id")
            if not procurement_id:
                continue

            # Build SET clause for outcome fields
            update_fields = {
                "procurement_status_from_report": result.get("procurement_status_from_report"),
                "procurement_winner": result.get("procurement_winner"),
                "procurement_winner_registration_no": result.get("procurement_winner_registration_no"),
                "procurement_winner_suggested_price_eur": result.get("procurement_winner_suggested_price_eur"),
                "procurement_winner_source": result.get("procurement_winner_source"),
                "procurement_participants_count": result.get("procurement_participants_count", 0),
                "procurement_outcome_description": result.get("procurement_outcome_description"),
                "bid_deadline": result.get("bid_deadline"),
                "decision_date": result.get("decision_date"),
                "funding_source": result.get("funding_source"),
                "eu_project_reference": result.get("eu_project_reference"),
                "evaluation_method": result.get("evaluation_method"),
                "contract_scope_type": result.get("contract_scope_type"),
                "extracted_at": extracted_at,
            }

            # Convert participants list to JSON
            participants = result.get("procurement_participants", [])
            if participants:
                update_fields["procurement_participants_json"] = json.dumps(participants, ensure_ascii=False)

                # Extract disqualified participants
                disqualified = [p for p in participants if p.get("disqualified")]
                if disqualified:
                    update_fields["disqualified_participants_json"] = json.dumps(
                        [{"name": p["name"], "reason": p.get("disqualification_reason")} for p in disqualified],
                        ensure_ascii=False
                    )

            # Extract subcontractors
            subcontractors = result.get("subcontractors")
            if subcontractors:
                update_fields["subcontractors_json"] = json.dumps(subcontractors, ensure_ascii=False)

            # Multi-lot support
            is_multi_lot = result.get("is_multi_lot", False)
            lot_count = result.get("lot_count")
            update_fields["is_multi_lot"] = is_multi_lot
            update_fields["lot_count"] = lot_count

            # If multi-lot, create lots_json with structured data
            if is_multi_lot and participants:
                # Group participants by lot
                lots_data = {}
                for p in participants:
                    lot_num = p.get("lot_number")
                    if lot_num:
                        if lot_num not in lots_data:
                            lots_data[lot_num] = {
                                "lot_number": lot_num,
                                "participants": [],
                                "winner": None
                            }
                        lots_data[lot_num]["participants"].append(p)
                        if p.get("won_lot"):
                            lots_data[lot_num]["winner"] = p.get("name")

                if lots_data:
                    update_fields["lots_json"] = json.dumps(
                        {"lots": list(lots_data.values())},
                        ensure_ascii=False
                    )

            # Build SQL UPDATE
            set_clause = ", ".join(f"{k} = ?" for k in update_fields.keys())
            values = list(update_fields.values())
            values.append(procurement_id)  # For WHERE clause

            conn.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET {set_clause}
                WHERE procurement_id = ?
                """,
                values,
            )
            rows_updated += 1

        conn.commit()

    return rows_updated


def load_procurement_records(db_path: Path) -> List[Dict[str, Any]]:
    ensure_procurement_record_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT raw_procurement_record_json
            FROM {TABLE_NAME}
            ORDER BY year ASC, procurement_id ASC, procurement_url ASC
            """
        ).fetchall()
    loaded: List[Dict[str, Any]] = []
    for row in rows:
        parsed = json.loads(str(row["raw_procurement_record_json"]))
        if isinstance(parsed, dict):
            loaded.append(parsed)
    return loaded


def load_procurement_records_for_pipeline(db_path: Path) -> List[Dict[str, Any]]:
    ensure_procurement_record_database(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT *
            FROM {TABLE_NAME}
            ORDER BY year ASC, procurement_id ASC, procurement_url ASC
            """
        ).fetchall()
    loaded: List[Dict[str, Any]] = []
    for row in rows:
        parsed = json.loads(str(row["raw_procurement_record_json"]))
        if not isinstance(parsed, dict):
            continue
        merged = dict(parsed)
        merged.update(
            {
                "report_document_path": row["report_document_path"],
                "report_document_type": row["report_document_type"],
                "report_original_archive": row["report_original_archive"],
                "report_download_status": row["report_download_status"],
                "report_download_error": row["report_download_error"],
                "report_downloaded_at": row["report_downloaded_at"],
                "report_download_url": row["report_download_url"],
                "report_selection_method": row["report_selection_method"],
                "report_selection_reason": row["report_selection_reason"],
                "report_selected_title": row["report_selected_title"],
                "is_eps_publication": row["is_eps_publication"],
                "classification_domain": row["classification_domain"],
                "classification_scope_type": row["classification_scope_type"],
                "classification_work_type": row["classification_work_type"],
                "classification_asset_scale": row["classification_asset_scale"],
                "classification_final_category": row["classification_final_category"],
                "classification_reason": row["classification_reason"],
            }
        )
        loaded.append(merged)
    return loaded


def load_procurement_records_by_keys(db_path: Path, procurement_record_keys: Iterable[str]) -> List[Dict[str, Any]]:
    ensure_procurement_record_database(db_path)
    keys = [str(key) for key in procurement_record_keys if str(key).strip()]
    if not keys:
        return []
    placeholders = ",".join("?" for _ in keys)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT raw_procurement_record_json
            FROM {TABLE_NAME}
            WHERE procurement_record_key IN ({placeholders})
            ORDER BY year ASC, procurement_id ASC, procurement_url ASC
            """,
            keys,
        ).fetchall()
    loaded: List[Dict[str, Any]] = []
    for row in rows:
        parsed = json.loads(str(row["raw_procurement_record_json"]))
        if isinstance(parsed, dict):
            loaded.append(parsed)
    return loaded


def update_procurement_report_metadata(
    db_path: Path,
    procurement_record: Dict[str, Any],
    *,
    downloaded_at: str,
) -> None:
    ensure_procurement_record_database(db_path)
    storage_row = _build_storage_row(
        procurement_record,
        ingested_at=str(procurement_record.get("ingested_at") or ""),
        classified_at=str(procurement_record.get("classified_at") or ""),
        classifier_model=str(procurement_record.get("classifier_model") or ""),
    )
    storage_row["report_downloaded_at"] = downloaded_at
    ordered_columns = list(SCHEMA_COLUMNS.keys())
    update_assignments = ", ".join(
        f"{column} = ?" for column in ordered_columns if column != "procurement_record_key"
    )
    values = [storage_row[column] for column in ordered_columns if column != "procurement_record_key"]
    values.append(storage_row["procurement_record_key"])
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET {update_assignments}
            WHERE procurement_record_key = ?
            """,
            values,
        )
        conn.commit()
