#!/usr/bin/env python3
"""Fetch all supplementary CKAN datasets into Postgres raw tables.

Datasets synced:
  - iepirkumu-rezultatu-datu-grupa        → ckan_results
  - iepirkumu-piedavajumu-atversanu-datu-grupa → ckan_participants
  - iepirkumu-grozijumu-datu-grupa        → ckan_amendments
  - pirkuma-pasutijumu-datu-grupa         → ckan_purchase_orders
  - piegazu-datu-grupa                    → ckan_deliveries
  - pasutitaju-datu-grupa                 → ckan_buyers

Usage:
    python -m app_template.modules.extraction.fetch_ckan_raw
    python -m app_template.modules.extraction.fetch_ckan_raw --datasets results participants
    python -m app_template.modules.extraction.fetch_ckan_raw --from-year 2022 --to-year 2025
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql as psy_sql

CKAN_BASE = "https://data.gov.lv/dati/api/3/action"
PAGE_SIZE = 10_000
REQUEST_DELAY = 0.3  # seconds between CKAN requests

# Resource IDs per dataset per year
RESOURCE_IDS: dict[str, dict[int, str]] = {
    "results": {
        2018: "cecd0be7-c8e0-451a-8314-f1d806db3bc1",
        2019: "1d37ba16-4d7b-4c1e-9650-1ee9b6a32666",
        2020: "abf811a3-26e8-48c2-bc86-e9b74ca0b385",
        2021: "a1342945-ce4b-480b-abb5-b74d43c41534",
        2022: "97a7c410-60c0-4d08-b554-4d1abb9092da",
        2023: "71f88053-97c1-4928-93c3-8d83d714f27f",
        2024: "3a02a1a7-0322-4c0d-9700-8af9832f0f91",
        2025: "79b34e1c-8989-4984-816a-8e8f92b701f3",
        2026: "c4007411-1ba5-40f3-9b50-f25881c93f51",
    },
    "participants": {
        2016: "8e77cc9e-554e-4bfb-8772-9dc0b7d24608",
        2017: "7733be61-bca2-4ae8-8577-d948058df6c0",
        2018: "e40819ee-3a84-4205-b64e-4c67263ac237",
        2019: "7f23e4c6-9bee-4552-ba0c-a05be1f6ac62",
        2020: "eb1ddcf9-e358-4ceb-a4d7-e406a0a60d7e",
        2021: "25883190-97ef-45a1-9b89-d15cc418a644",
        2022: "4b9317d7-8495-4621-966d-48e00639e2cb",
        2023: "7f4f7e75-8207-4ab1-9470-bcd0112653e9",
        2024: "0bba780e-5ae3-4701-ab16-8f804d5a3e57",
        2025: "4540cc38-0f5f-42a9-9749-3896c3da4488",
        2026: "a45acd54-2e8f-4fb5-b757-31654e875563",
    },
    "amendments": {
        2016: "792209b1-1465-41f9-b6e7-93878722c249",
        2017: "92e512ed-013b-4757-ba19-56662630f6e6",
        2018: "a1e67b0e-704a-4ca1-96dc-39c87d35c04e",
        2019: "d5e7494f-6f7f-42a9-9683-3c855f3d00a1",
        2020: "3438f0eb-d7d2-4d0c-a1f2-e1d2420c848f",
        2021: "ec6da601-c7f6-466e-9bcb-4efea45dab36",
        2022: "4ad302df-c832-48e5-bd67-89d470ae43b3",
        2023: "25b0c081-49d9-4bea-9e5f-ecac3573d6cb",
        2024: "f8e2e074-e29e-409f-8fec-5d22c7b0bcd1",
        2025: "f7b96bf6-2af8-446a-a6ee-6022601fef7c",
        2026: "d9e4d487-1f1a-4b37-9735-e411708d7288",
    },
    "purchase_orders": {
        2010: "a8f9c39c-5a68-4848-a6c6-57625f205c45",
        2011: "8f3d230c-1bc7-40fa-b786-8b5ac89b8496",
        2012: "2ef44ded-5080-4c93-90af-a9e605c470c4",
        2013: "c3d54b54-a2b4-4535-ac6a-40f264568648",
        2014: "9031948a-c906-4614-8274-6a62de1eeaf4",
        2015: "572a1153-371c-4a86-9cbe-a07b4a5a977a",
        2016: "4a32f248-feb9-4f54-9243-d4d98ea9d024",
        2017: "f50ac697-a3a1-453b-9fd0-c3ca16656092",
        2018: "e3be0379-c7bf-4439-9443-ffc9881f79af",
        2019: "227567af-fd4b-49df-8054-dba08676254e",
        2020: "d31fbf99-2708-4558-9c83-d16e69a70e08",
        2021: "bf623b89-ce86-4fc6-a2af-46c2c993074c",
        2022: "bad87d10-ef05-43fb-b9ba-9f0e7a761673",
        2023: "d89f4745-77b0-47e4-9e8b-ec494dc3ad1b",
        2024: "11f08c38-50f7-47f3-a700-4f60cd09d943",
        2025: "226db975-8dc7-4f59-9f92-773ef9b58739",
        2026: "a63a9503-d7f1-4840-8e53-0346ed0513a9",
    },
    "deliveries": {
        2010: "cd17b62c-b7e5-416e-adcc-707e2b109a6f",
        2011: "bb62ce35-46eb-49ed-8e95-474d0918dd65",
        2012: "1c638795-cb24-4b57-a11a-124e48b274c0",
        2013: "cba668cc-2535-458a-ac21-9022f0ded50f",
        2014: "cdd75d59-407b-4e7e-b4a2-834db2ee00f8",
        2015: "cd1bd12c-7e52-40ee-b65b-66364a0c9120",
        2016: "dc0f81fd-c48c-4740-b479-07b1218e7ef2",
        2017: "78ca35ae-8ce7-4593-97ac-faf8348a18af",
        2018: "7d152654-74c8-45af-a024-fdbd002fa706",
        2019: "addf3381-f57e-40a7-82e7-e228fb121367",
        2020: "fe5a1ee0-c228-46d8-9d2f-022d9aee1ae9",
        2021: "ead3e185-b006-45f5-ace9-4fb2fc3c15ad",
        2022: "99565544-1fd2-441d-b408-df33066a3867",
        2023: "b7cc1f82-10c7-45f1-ad22-d3339bf990ee",
        2024: "041097af-09c4-4ce6-b427-ad6921456c71",
        2025: "91f940ab-9132-40c0-aa8b-433ced22c17d",
        2026: "c6aed1c6-94bc-4c83-936f-3c37648aca4b",
    },
    "buyers": {
        0: "08a09865-b831-462a-a5ce-226f9293ff3e",  # single file, no year split
    },
}


# ---------------------------------------------------------------------------
# CKAN helpers
# ---------------------------------------------------------------------------

def _strip_excel(v: Any) -> str:
    """Strip Excel CSV escape: '=\"12345\"' -> '12345'"""
    if isinstance(v, str) and v.startswith('="'):
        return v[2:].rstrip('"')
    return str(v) if v is not None else ""


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return None


def _to_bool(v: Any) -> bool | None:
    if v is None or v == "":
        return None
    return str(v).strip().lower() in ("1", "true", "jā", "ja", "yes")


def ckan_fetch_all(resource_id: str) -> list[dict]:
    """Fetch all rows from a CKAN datastore resource via pagination."""
    rows: list[dict] = []
    offset = 0
    while True:
        url = (
            f"{CKAN_BASE}/datastore_search"
            f"?resource_id={resource_id}&limit={PAGE_SIZE}&offset={offset}"
        )
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            print(f"  CKAN fetch error at offset {offset}: {exc}")
            break

        result = data.get("result", {})
        batch = result.get("records", [])
        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

    return rows


def resolve_resource_ids(dataset: str, from_year: int, to_year: int) -> list[tuple[int, str]]:
    """Return [(year, resource_id)] for the given year range, skipping TODO placeholders."""
    mapping = RESOURCE_IDS.get(dataset, {})
    if dataset == "buyers":
        return [(0, mapping[0])]
    result = []
    for year, rid in sorted(mapping.items()):
        if year == 0:
            continue
        if from_year <= year <= to_year and not rid.startswith("TODO"):
            result.append((year, rid))
    return result


# ---------------------------------------------------------------------------
# Per-dataset row mappers
# ---------------------------------------------------------------------------

def map_results_row(row: dict, year: int) -> dict:
    return {
        "procurement_id": _strip_excel(row.get("Iepirkuma_ID")),
        "procurement_title": row.get("Iepirkuma_nosaukums"),
        "buyer": row.get("Pasutitaja_nosaukums"),
        "buyer_reg_number": str(row.get("Pasutitaja_registracijas_numurs") or "").strip() or None,
        "winner_name": row.get("Uzvaretaja_nosaukums"),
        "winner_reg_number": str(row.get("Uzvaretaja_registracijas_numurs") or "").strip() or None,
        "contract_value_eur": _to_float(row.get("Aktuala_liguma_summa")),
        "contract_value_original_eur": _to_float(row.get("Sakotneja_liguma_summa")),
        "contract_signed_date": row.get("Liguma_dok_noslegsanas_datums"),
        "source_year": year,
    }


def map_participants_row(row: dict, year: int) -> dict:
    return {
        "procurement_id": str(row.get("Iepirkuma_ID") or "").strip(),
        "procurement_title": row.get("Iepirkuma_nosaukums"),
        "identification_number": row.get("Iepirkuma_identifikacijas_numurs"),
        "buyer": row.get("Pasutitaja_nosaukums"),
        "buyer_reg_number": str(row.get("Pasutitaja_registracijas_numurs") or "").strip() or None,
        "buyer_pvs_id": _to_int(row.get("Pasutitaja_PVS_ID")),
        "parent_organization": row.get("Augstak_stavosa_organizacija"),
        "status": row.get("Iepirkuma_statuss"),
        "cpv_main": _strip_excel(row.get("CPV_kods_galvenais_prieksmets") or ""),
        "subject_type": row.get("Iepirkuma_prieksmeta_veids"),
        "estimated_value_eur": _to_float(row.get("Planota_ligumcena")),
        "currency": row.get("Ligumcenas_valuta"),
        "contract_duration_months": _to_int(row.get("Planotais_liguma_darbibas_termins")),
        "submission_deadline": str(row.get("Piedavajumu_iesniegsanas_termins_datums") or "").strip() or None,
        "participant_name": row.get("Pretendenta_nosaukums"),
        "participant_reg_number": str(row.get("Pretendenta_registracijas_numurs") or "").strip() or None,
        "participant_submitted_at": str(row.get("Pretendenta_piedavajuma_iesniegsanas_datums") or "").strip() or None,
        "source_year": year,
    }


def map_amendments_row(row: dict, year: int) -> dict:
    return {
        "procurement_id": str(row.get("Iepirkuma_ID") or "").strip(),
        "procurement_title": row.get("Iepirkuma_nosaukums"),
        "identification_number": row.get("Iepirkuma_identifikacijas_numurs"),
        "buyer": row.get("Pasutitaja_nosaukums"),
        "buyer_reg_number": str(row.get("Pasutitaja_registracijas_numurs") or "").strip() or None,
        "buyer_reg_number_type": row.get("Pasutitaja_registracijas_numura_veids"),
        "buyer_pvs_id": _to_int(row.get("Pasutitaja_PVS_ID")),
        "parent_organization": row.get("Augstak_stavosa_organizacija"),
        "status": row.get("Iepirkuma_statuss"),
        "announcement_date": row.get("Iepirkuma_izsludinasanas_datums"),
        "amendment_date": row.get("Grozijumu_datums"),
        "submission_deadline": row.get("Piedavajumu_iesniegsanas_datumlaiks"),
        "interested_parties_meeting": row.get("Ieintereseto_personu_sanaksmes"),
        "eu_project_reference": row.get("Atsauce_uz_ES_projektiem_un_programmam"),
        "eis_url": row.get("Hipersaite_EIS_kura_pieejams_zinojums"),
        "iub_url": row.get("Hipersaite_uz_IUB_publikaciju"),
        "has_lots": row.get("Ir_dalijums_dalas"),
        "lot_submission_conditions": row.get("Dalu_iesniegsanas_nosacijumi"),
        "lot_number": _to_int(row.get("Iepirkuma_dalas_nr")),
        "lot_name": row.get("Iepirkuma_dalas_nosaukums"),
        "lot_status": row.get("Iepirkuma_dalas_statuss"),
        "source_year": year,
    }


def map_purchase_orders_row(row: dict, year: int) -> dict:
    return {
        "order_number": row.get("Pasutijuma_Nr"),
        "order_status": row.get("Pasutijuma_statuss"),
        "order_confirmed_date": row.get("Pasutijuma_apstiprinasanas_datums"),
        "buyer": row.get("Pasutitajs"),
        "buyer_reg_number": str(row.get("Pasutitaja_registracijas_numurs") or "").strip() or None,
        "buyer_reg_number_type": row.get("Pasutitaja_registracijas_numura_veids"),
        "parent_organization": row.get("Augstak_stavosa_organizacija"),
        "supplier": row.get("Piegadatajs"),
        "supplier_reg_number": str(row.get("Piegadataja_registracijas_numurs") or "").strip() or None,
        "supplier_reg_number_type": row.get("Piegadataja_registracijas_numura_veids"),
        "catalog_number": row.get("Kataloga_numurs"),
        "catalog_name": row.get("Kataloga_nosaukums"),
        "item_name": row.get("Pasutitas_preces_nosaukums"),
        "item_manufacturer": row.get("Pasutitas_preces_razotajs"),
        "max_delivery_date": row.get("Max_piegades_datums"),
        "amount_eur_ex_vat": _to_float(row.get("Summa_bez_PVN")),
        "vat_pct": _to_float(row.get("PVN_proc")),
        "quantity": _to_float(row.get("Pasutitas_skaits")),
        "source_year": year,
    }


def map_deliveries_row(row: dict, year: int) -> dict:
    return {
        "order_number": row.get("PasutijumaNr"),
        "waybill_number": row.get("Pavadzimes_nr"),
        "delivery_status": row.get("Piegades_statuss"),
        "buyer": row.get("Pasutitajs"),
        "buyer_reg_number": str(row.get("Pasut_orgnr") or "").strip() or None,
        "buyer_reg_number_type": row.get("Pasut_org_reg_nr_veids"),
        "parent_organization": row.get("Augstak_stavosa_organizacija"),
        "supplier": row.get("Piegadatajs"),
        "supplier_reg_number": str(row.get("Piegnr") or "").strip() or None,
        "supplier_reg_number_type": row.get("Pieg_org_nr_veids"),
        "catalog_number": row.get("Kataloga_nr"),
        "catalog_name": row.get("Kataloga_nos"),
        "item_name": row.get("Preces_nosaukums"),
        "item_manufacturer": row.get("Piegadatas_preces_razotajs"),
        "purchase_created_date": row.get("Pirkuma_izveid_dat"),
        "quality_approved_date": row.get("Kval_apstipr_dat"),
        "delivery_address": row.get("Piegades_adrese"),
        "amount_eur_ex_vat": _to_float(row.get("Summa_bez_PVN")),
        "vat_pct": _to_float(row.get("PVN_proc")),
        "quantity": _to_float(row.get("Kval_skaits")),
        "source_year": year,
    }


def map_buyers_row(row: dict, _year: int) -> dict:
    return {
        "organization": row.get("Organizacija") or "",
        "reg_number": str(row.get("RegNr") or "").strip() or None,
        "reg_number_type": row.get("RegNrVeids"),
        "parent_organization": row.get("Augstak_stavosa_organizacija"),
        "registered_date": row.get("RegDatums"),
        "is_blocked": _to_bool(row.get("Blokets")),
        "is_deleted": _to_bool(row.get("Dzests")),
    }


# ---------------------------------------------------------------------------
# Dataset sync configs
# ---------------------------------------------------------------------------

DATASET_CONFIGS: dict[str, dict] = {
    "results": {
        "table": "ckan_results",
        "mapper": map_results_row,
        # ON CONFLICT: keep row with highest contract_value_eur per (procurement_id, winner_reg_number)
        "upsert_sql": """
            INSERT INTO ckan_results (
                procurement_id, procurement_title, buyer, buyer_reg_number,
                winner_name, winner_reg_number, contract_value_eur,
                contract_value_original_eur, contract_signed_date, source_year
            ) VALUES (
                %(procurement_id)s, %(procurement_title)s, %(buyer)s, %(buyer_reg_number)s,
                %(winner_name)s, %(winner_reg_number)s, %(contract_value_eur)s,
                %(contract_value_original_eur)s, %(contract_signed_date)s, %(source_year)s
            )
            ON CONFLICT (procurement_id, winner_reg_number)
            DO UPDATE SET
                contract_value_eur = GREATEST(
                    EXCLUDED.contract_value_eur,
                    ckan_results.contract_value_eur
                ),
                contract_value_original_eur = COALESCE(
                    ckan_results.contract_value_original_eur,
                    EXCLUDED.contract_value_original_eur
                ),
                contract_signed_date = COALESCE(EXCLUDED.contract_signed_date, ckan_results.contract_signed_date),
                source_year          = EXCLUDED.source_year,
                procurement_title    = COALESCE(EXCLUDED.procurement_title, ckan_results.procurement_title),
                synced_at            = NOW()
        """,
        "skip_if_null": ["procurement_id", "winner_reg_number"],
    },
    "participants": {
        "table": "ckan_participants",
        "mapper": map_participants_row,
        "upsert_sql": """
            INSERT INTO ckan_participants (
                procurement_id, procurement_title, identification_number,
                buyer, buyer_reg_number, buyer_pvs_id, parent_organization,
                status, cpv_main, subject_type, estimated_value_eur, currency,
                contract_duration_months, submission_deadline,
                participant_name, participant_reg_number, participant_submitted_at,
                source_year
            ) VALUES (
                %(procurement_id)s, %(procurement_title)s, %(identification_number)s,
                %(buyer)s, %(buyer_reg_number)s, %(buyer_pvs_id)s, %(parent_organization)s,
                %(status)s, %(cpv_main)s, %(subject_type)s, %(estimated_value_eur)s, %(currency)s,
                %(contract_duration_months)s, %(submission_deadline)s,
                %(participant_name)s, %(participant_reg_number)s, %(participant_submitted_at)s,
                %(source_year)s
            )
            ON CONFLICT (procurement_id, participant_reg_number)
            DO UPDATE SET
                status                  = EXCLUDED.status,
                participant_submitted_at = COALESCE(EXCLUDED.participant_submitted_at, ckan_participants.participant_submitted_at),
                synced_at               = NOW()
        """,
        "skip_if_null": ["procurement_id", "participant_reg_number"],
    },
    "amendments": {
        "table": "ckan_amendments",
        "mapper": map_amendments_row,
        # amendments have no natural unique key — insert all, skip exact duplicates via dedup query
        "upsert_sql": """
            INSERT INTO ckan_amendments (
                procurement_id, procurement_title, identification_number,
                buyer, buyer_reg_number, buyer_reg_number_type, buyer_pvs_id,
                parent_organization, status, announcement_date, amendment_date,
                submission_deadline, interested_parties_meeting, eu_project_reference,
                eis_url, iub_url, has_lots, lot_submission_conditions,
                lot_number, lot_name, lot_status, source_year
            ) VALUES (
                %(procurement_id)s, %(procurement_title)s, %(identification_number)s,
                %(buyer)s, %(buyer_reg_number)s, %(buyer_reg_number_type)s, %(buyer_pvs_id)s,
                %(parent_organization)s, %(status)s, %(announcement_date)s, %(amendment_date)s,
                %(submission_deadline)s, %(interested_parties_meeting)s, %(eu_project_reference)s,
                %(eis_url)s, %(iub_url)s, %(has_lots)s, %(lot_submission_conditions)s,
                %(lot_number)s, %(lot_name)s, %(lot_status)s, %(source_year)s
            )
        """,
        "skip_if_null": ["procurement_id"],
        "truncate_before_sync": True,  # full refresh — amendments table is idempotent per year
    },
    "purchase_orders": {
        "table": "ckan_purchase_orders",
        "mapper": map_purchase_orders_row,
        "upsert_sql": """
            INSERT INTO ckan_purchase_orders (
                order_number, order_status, order_confirmed_date,
                buyer, buyer_reg_number, buyer_reg_number_type, parent_organization,
                supplier, supplier_reg_number, supplier_reg_number_type,
                catalog_number, catalog_name, item_name, item_manufacturer,
                max_delivery_date, amount_eur_ex_vat, vat_pct, quantity, source_year
            ) VALUES (
                %(order_number)s, %(order_status)s, %(order_confirmed_date)s,
                %(buyer)s, %(buyer_reg_number)s, %(buyer_reg_number_type)s, %(parent_organization)s,
                %(supplier)s, %(supplier_reg_number)s, %(supplier_reg_number_type)s,
                %(catalog_number)s, %(catalog_name)s, %(item_name)s, %(item_manufacturer)s,
                %(max_delivery_date)s, %(amount_eur_ex_vat)s, %(vat_pct)s, %(quantity)s, %(source_year)s
            )
        """,
        "skip_if_null": [],
        "truncate_before_sync": True,
    },
    "deliveries": {
        "table": "ckan_deliveries",
        "mapper": map_deliveries_row,
        "upsert_sql": """
            INSERT INTO ckan_deliveries (
                order_number, waybill_number, delivery_status,
                buyer, buyer_reg_number, buyer_reg_number_type, parent_organization,
                supplier, supplier_reg_number, supplier_reg_number_type,
                catalog_number, catalog_name, item_name, item_manufacturer,
                purchase_created_date, quality_approved_date, delivery_address,
                amount_eur_ex_vat, vat_pct, quantity, source_year
            ) VALUES (
                %(order_number)s, %(waybill_number)s, %(delivery_status)s,
                %(buyer)s, %(buyer_reg_number)s, %(buyer_reg_number_type)s, %(parent_organization)s,
                %(supplier)s, %(supplier_reg_number)s, %(supplier_reg_number_type)s,
                %(catalog_number)s, %(catalog_name)s, %(item_name)s, %(item_manufacturer)s,
                %(purchase_created_date)s, %(quality_approved_date)s, %(delivery_address)s,
                %(amount_eur_ex_vat)s, %(vat_pct)s, %(quantity)s, %(source_year)s
            )
        """,
        "skip_if_null": [],
        "truncate_before_sync": True,
    },
    "buyers": {
        "table": "ckan_buyers",
        "mapper": map_buyers_row,
        "upsert_sql": """
            INSERT INTO ckan_buyers (
                organization, reg_number, reg_number_type, parent_organization,
                registered_date, is_blocked, is_deleted
            ) VALUES (
                %(organization)s, %(reg_number)s, %(reg_number_type)s, %(parent_organization)s,
                %(registered_date)s, %(is_blocked)s, %(is_deleted)s
            )
            ON CONFLICT (reg_number) DO UPDATE SET
                organization      = EXCLUDED.organization,
                reg_number_type   = EXCLUDED.reg_number_type,
                parent_organization = EXCLUDED.parent_organization,
                is_blocked        = EXCLUDED.is_blocked,
                is_deleted        = EXCLUDED.is_deleted,
                synced_at         = NOW()
        """,
        "skip_if_null": ["reg_number"],
        "truncate_before_sync": False,
    },
}


# ---------------------------------------------------------------------------
# Sync runner
# ---------------------------------------------------------------------------

def sync_dataset(
    conn: psycopg.Connection,
    dataset: str,
    from_year: int,
    to_year: int,
    dry_run: bool = False,
) -> dict[str, int]:
    cfg = DATASET_CONFIGS[dataset]
    resources = resolve_resource_ids(dataset, from_year, to_year)
    if not resources:
        print(f"  [{dataset}] No resources found for years {from_year}–{to_year}, skipping.")
        return {"skipped": 1}

    total_fetched = 0
    total_inserted = 0
    total_skipped = 0

    for year, resource_id in resources:
        print(f"  [{dataset}] year={year if year else 'all'} resource={resource_id} — fetching...")
        rows = ckan_fetch_all(resource_id)
        print(f"  [{dataset}] fetched {len(rows)} rows")
        total_fetched += len(rows)

        mapped = []
        for row in rows:
            try:
                m = cfg["mapper"](row, year)
            except Exception as exc:
                print(f"  [{dataset}] map error: {exc} — row={row}")
                total_skipped += 1
                continue

            # skip rows missing required fields
            skip = False
            for field in cfg.get("skip_if_null", []):
                val = m.get(field)
                if not val or val == "":
                    skip = True
                    break
            if skip:
                total_skipped += 1
                continue

            mapped.append(m)

        if dry_run:
            print(f"  [{dataset}] DRY RUN — would insert {len(mapped)} rows")
            total_inserted += len(mapped)
            continue

        if cfg.get("truncate_before_sync") and mapped:
            with conn.cursor() as cur:
                cur.execute(
                    psy_sql.SQL("DELETE FROM {} WHERE source_year = %s").format(
                        psy_sql.Identifier(str(cfg["table"]))
                    ),
                    (year,),
                )
            print(f"  [{dataset}] cleared existing rows for year={year}")

        BATCH = 500
        for i in range(0, len(mapped), BATCH):
            batch = mapped[i : i + BATCH]
            with conn.cursor() as cur:
                cur.executemany(cfg["upsert_sql"], batch)
            conn.commit()

        total_inserted += len(mapped)
        print(f"  [{dataset}] year={year if year else 'all'} — inserted/upserted {len(mapped)} rows")

    return {"fetched": total_fetched, "inserted": total_inserted, "skipped": total_skipped}


def run(args: argparse.Namespace) -> int:
    database_url = args.database_url
    if not database_url:
        from pathlib import Path
        env_path = Path.cwd() / ".env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line.startswith("DATABASE_URL="):
                    database_url = line.split("=", 1)[1].strip()
                    break

    if not database_url:
        print("ERROR: --database-url not set and DATABASE_URL not found in .env")
        return 1

    # psycopg3 uses postgresql:// scheme directly
    dsn = database_url.replace("postgresql+psycopg://", "postgresql://")

    datasets = args.datasets or list(DATASET_CONFIGS.keys())
    unknown = [d for d in datasets if d not in DATASET_CONFIGS]
    if unknown:
        print(f"ERROR: Unknown datasets: {unknown}. Valid: {list(DATASET_CONFIGS.keys())}")
        return 1

    print("Connecting to database...")
    print(f"Datasets: {datasets}")
    print(f"Years: {args.from_year}–{args.to_year}")
    if args.dry_run:
        print("DRY RUN mode — no writes")

    started = datetime.now(UTC)
    summary: dict[str, Any] = {}

    with psycopg.connect(dsn) as conn:
        for dataset in datasets:
            print(f"\n=== Syncing {dataset} ===")
            stats = sync_dataset(conn, dataset, args.from_year, args.to_year, dry_run=args.dry_run)
            summary[dataset] = stats

    elapsed = (datetime.now(UTC) - started).total_seconds()
    print(f"\nDone in {elapsed:.1f}s")
    print(json.dumps(summary, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch supplementary CKAN datasets into Postgres raw tables"
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=list(DATASET_CONFIGS.keys()),
        help="Which datasets to sync (default: all)",
    )
    parser.add_argument("--from-year", type=int, default=2020, help="Start year (inclusive)")
    parser.add_argument("--to-year", type=int, default=2026, help="End year (inclusive)")
    parser.add_argument("--database-url", default=None, help="Postgres DSN (falls back to .env DATABASE_URL)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but do not write to DB")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
