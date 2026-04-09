"""EIS procurement sync service — pulls from CKAN open data API into Postgres."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request

from sqlalchemy import text
from sqlalchemy.orm import Session

# Uses datastore_search (not SQL) for proper pagination up to 30k+ rows per resource
CKAN_SEARCH_URL = "https://data.gov.lv/dati/api/3/action/datastore_search"

# EIS open data resource IDs per year (2020 → present)
RESOURCE_IDS = [
    "f2a08974-d59a-4d83-92f2-4194cb2bd97d",  # 2026
    "42739f4f-d625-46a7-9668-ef2e0376e421",  # 2025
    "d7204b7f-0767-472e-b1c7-b85816992885",  # 2024
    "b8927a5d-1274-4262-bccc-16d21abcf4a3",  # 2023
    "0c4d15c4-cf59-4239-ba76-de4bbf48d824",  # 2022
    "6bd2e287-c494-4ddc-9525-802d88872edc",  # 2021
    "e6038531-e12d-4bc7-8357-89e37d7de476",  # 2020
]

PAGE_SIZE = 1000

# Complete CKAN field → our DB column mapping (all 54 source fields)
FIELD_MAP: dict[str, str] = {
    "Iepirkuma_ID":                                    "procurement_id",
    "Iepirkuma_nosaukums":                             "title",
    "Iepirkuma_identifikacijas_numurs":                "identification_number",
    "Pasutitaja_nosaukums":                            "buyer",
    "Pasutitaja_registracijas_numurs":                 "buyer_reg_number",
    "Pasutitaja_registracijas_numura_veids":           "buyer_reg_number_type",
    "Pasutitaja_PVS_ID":                               "buyer_pvs_id",
    "Augstak_stavosa_organizacija":                    "parent_organization",
    "Citu_pasutitaju_vajadzibam":                      "for_other_buyers",
    "Faktiskais_sanemejs":                             "actual_recipient",
    "Iepirkuma_prieksmeta_veids":                      "subject_type",
    "CPV_kods_galvenais_prieksmets":                   "cpv_main",
    "CPV_kodi_papildus_prieksmeti":                    "cpv_additional",
    "Iepirkuma_statuss":                               "status",
    "Iepirkuma_izsludinasanas_datums":                 "publication_date",
    "Piedavajumu_iesniegsanas_datums":                 "submission_deadline",
    "Piedavajumu_iesniegsanas_laiks":                  "submission_deadline_time",
    "Ieintereseto_personu_sanaksmes":                  "interested_parties_meeting",
    "Precu_vai_pakalpojumu_sniegsanas_vieta":          "region",
    "Planotais_liguma_darbibas_termina_termina_veids": "contract_duration_type",
    "Planotais_liguma_darbibas_termins":               "contract_duration",
    "Planota_liguma_darbibas_termina_mervieniba":      "contract_duration_unit",
    "Planota_liguma_izpilde_no":                       "contract_start_date",
    "Planota_liguma_izpilde_lidz":                     "contract_end_date",
    "Ligumcenas_veids":                                "value_type",
    "Planota_ligumcena":                               "estimated_value_eur",
    "Planota_ligumcena_no":                            "value_min_eur",
    "Planota_ligumcena_lidz":                          "value_max_eur",
    "Ligumcenas_valuta":                               "value_currency",
    "Regulejosais_tiesibu_akts":                       "governing_law",
    "Proceduras_veids":                                "procedure_type",
    "Pasutitaja_kontaktpersona":                       "contact_person",
    "Piedavajuma_iesniegsanas_valoda":                 "submission_language",
    "Atsauce_uz_ES_projektiem_un_programmam":          "eu_project_reference",
    "Vai_pielaujami_piedavajuma_varianti":             "variants_allowed",
    "Uzvaretaja_izveles_metode":                       "winner_selection_method",
    "Iesniegsanas_vieta":                              "submission_place",
    "Hipersaite_EIS_kura_pieejams_zinojums":           "eis_url",
    "Hipersaite_uz_IUB_publikaciju":                   "iub_url",
    "Ir_dalijums_dalas":                               "has_lots",
    "Dalu_iesniegsanas_nosacijumi":                    "lot_submission_conditions",
    "Iepirkuma_dalas_nr":                              "lot_number",
    "Iepirkuma_dalas_nosaukums":                       "lot_name",
    "Iepirkuma_dalas_statuss":                         "lot_status",
    "Dalas_planotais_liguma_darbibas_termina_termina_veids": "lot_contract_duration_type",
    "Dalas_planotais_liguma_darbibas_termins":         "lot_contract_duration",
    "Dalas_planota_liguma_darbibas_termina_mervieniba": "lot_contract_duration_unit",
    "Dalas_planota_liguma_izpilde_no":                 "lot_contract_start_date",
    "Dalas_planota_liguma_izpilde_lidz":               "lot_contract_end_date",
    "Dalas_ligumcenas_veids":                          "lot_value_type",
    "Dalas_planota_ligumcena":                         "lot_estimated_value_eur",
    "Dalas_planota_ligumcena_no":                      "lot_value_min_eur",
    "Dalas_planota_ligumcena_lidz":                    "lot_value_max_eur",
    "Dalas_ligumcenas_valuta":                         "lot_value_currency",
}

# Columns whose values should be cast to float
FLOAT_COLS = {
    "estimated_value_eur", "value_min_eur", "value_max_eur",
    "lot_estimated_value_eur", "lot_value_min_eur", "lot_value_max_eur",
}

# Columns whose values should be cast to int
INT_COLS = {"buyer_pvs_id", "contract_duration", "lot_number", "lot_contract_duration"}

ALL_COLS = list(FIELD_MAP.values())
UPSERT_COLS = [c for c in ALL_COLS if c != "procurement_id"]


class EISSyncService:
    """Pulls EIS procurement records from CKAN and upserts into the local Postgres DB."""

    def run(self, db: Session, full: bool = False) -> dict[str, int]:
        """
        full=False: delta sync — only the first page per resource (most recent records).
        full=True:  full backfill — fetches all records across all resource IDs.
        Returns counts: {"inserted": N, "updated": N, "errors": N}
        """
        inserted = updated = errors = 0

        for resource_id in RESOURCE_IDS:
            offset = 0
            while True:
                records, total, fetch_error = self._fetch_page(resource_id, offset)
                if fetch_error:
                    errors += 1
                    break
                if not records:
                    break

                for row in records:
                    try:
                        result = self._upsert(db, row)
                        if result == "inserted":
                            inserted += 1
                        else:
                            updated += 1
                    except Exception:
                        errors += 1

                db.commit()
                offset += PAGE_SIZE

                if offset >= total or len(records) < PAGE_SIZE:
                    break

                # Delta sync: only pull first page per resource
                if not full:
                    break

        return {"inserted": inserted, "updated": updated, "errors": errors}

    def _fetch_page(self, resource_id: str, offset: int) -> tuple[list[dict], int, bool]:
        params = {
            "resource_id": resource_id,
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        url = f"{CKAN_SEARCH_URL}?{urllib.parse.urlencode(params)}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "eis-helper-sync/1.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
            if not data.get("success"):
                return [], 0, True
            result = data["result"]
            return result["records"], result.get("total", 0), False
        except Exception:
            return [], 0, True

    def _upsert(self, db: Session, row: dict) -> str:
        """Upsert a single row. Returns 'inserted' or 'updated'."""
        mapped: dict = {}
        for lv_key, col in FIELD_MAP.items():
            val = row.get(lv_key)

            if col == "procurement_id":
                val = str(val).strip() if val is not None else None
                if val and val.startswith('="') and val.endswith('"'):
                    val = val[2:-1]
            elif col in FLOAT_COLS:
                try:
                    val = float(val) if val is not None else None
                except (TypeError, ValueError):
                    val = None
            elif col in INT_COLS:
                try:
                    val = int(val) if val is not None else None
                except (TypeError, ValueError):
                    val = None
            else:
                val = str(val).strip() if val is not None else None
                # CKAN sometimes wraps values in Excel formula syntax: ="value"
                if val and val.startswith('="') and val.endswith('"'):
                    val = val[2:-1]
                # Normalize empty strings to None
                if val == "":
                    val = None

            mapped[col] = val

        if not mapped.get("procurement_id"):
            raise ValueError("missing procurement_id")

        if not mapped.get("title"):
            mapped["title"] = mapped["procurement_id"]

        # Build eis_url if missing
        if not mapped.get("eis_url") or not mapped["eis_url"].startswith("http"):
            mapped["eis_url"] = (
                f"https://www.eis.gov.lv/EKEIS/Supplier/Procurement/{mapped['procurement_id']}"
            )

        insert_cols = ", ".join(ALL_COLS + ["synced_at"])
        insert_vals = ", ".join(f":{c}" for c in ALL_COLS) + ", NOW()"
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in UPSERT_COLS) + ", synced_at = NOW()"

        stmt = text(f"""
            INSERT INTO procurements ({insert_cols})
            VALUES ({insert_vals})
            ON CONFLICT (procurement_id) DO UPDATE SET {update_set}
            RETURNING (xmax = 0) AS was_inserted
        """)
        result = db.execute(stmt, mapped)
        row_result = result.fetchone()
        return "inserted" if row_result and row_result[0] else "updated"
