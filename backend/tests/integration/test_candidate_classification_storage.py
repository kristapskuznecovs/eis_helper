import sqlite3
import tempfile
import unittest
from pathlib import Path

from lib.collector_classes import derive_final_category, normalize_classification_result
from lib.collector_storage import (
    load_procurement_records_by_keys,
    procurement_record_storage_key,
    upsert_procurement_records,
)


class ProcurementRecordClassificationStorageTests(unittest.TestCase):
    def test_normalize_classification_result_uses_value_threshold_and_mapping(self) -> None:
        parsed = {
            "domain": "Building",
            "scope_type": "Design Only",
            "work_type": "unknown",
            "asset_scale": "small",
            "llm_reason": "Project is for building design.",
        }
        normalized = normalize_classification_result(
            parsed,
            estimated_value_eur=150_000,
            scale_thresholds={
                "default": 1_000_000,
                "by_final_category": {"building_design": 100_000},
            },
        )
        self.assertEqual(normalized["classification_domain"], "building")
        self.assertEqual(normalized["classification_scope_type"], "design_only")
        self.assertEqual(normalized["classification_asset_scale"], "large")
        self.assertEqual(normalized["classification_final_category"], "building_design")

    def test_normalize_classification_result_uses_default_threshold_for_build(self) -> None:
        parsed = {
            "domain": "building",
            "scope_type": "build_only",
            "work_type": "new_build",
            "asset_scale": "unknown",
            "llm_reason": "Construction works.",
        }
        normalized = normalize_classification_result(
            parsed,
            estimated_value_eur=900_000,
            scale_thresholds={
                "default": 1_000_000,
                "by_final_category": {"building_design": 100_000},
            },
        )
        self.assertEqual(normalized["classification_final_category"], "building_new_build")
        self.assertEqual(normalized["classification_asset_scale"], "small")

    def test_derive_final_category_examples(self) -> None:
        self.assertEqual(
            derive_final_category("building", "supervision_only", "unknown"),
            "building_supervision",
        )
        self.assertEqual(
            derive_final_category("building", "build_only", "new_build"),
            "building_new_build",
        )
        self.assertEqual(
            derive_final_category("infrastructure", "build_only", "repair"),
            "infrastructure_renovation_repair",
        )
        self.assertEqual(
            derive_final_category("maintenance_service", "service_only", "cleaning"),
            "maintenance_cleaning_service",
        )
        self.assertEqual(
            derive_final_category("non_construction", "unknown", "unknown"),
            "non_construction",
        )

    def test_sqlite_upsert_and_reload_current_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "database" / "eis_procurement_records.sqlite"
            procurement_record = {
                "procurement_id": "123",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/123",
                "year": 2025,
                "cpv_main": "45210000-2",
                "cpv_additional": "45300000-0",
                "submission_date": "2025-04-01",
                "planned_contract_term": "48",
                "planned_contract_term_unit": "Mēneši",
                "estimated_value_eur": 1_500_000,
                "estimated_value_source": "project",
                "classification_domain": "building",
                "classification_scope_type": "design_build",
                "classification_work_type": "new_build",
                "classification_asset_scale": "large",
                "classification_final_category": "building_design_build",
                "classification_reason": "Combined design and construction.",
                "raw_api_record_count": 2,
                "raw_api_records": [
                    {
                        "_id": 1,
                        "Iepirkuma_ID": "123",
                        "Iepirkuma_nosaukums": "Procurement Name",
                        "Iepirkuma_dalas_nr": "1",
                        "Planotais_liguma_darbibas_termins": "48",
                    },
                    {"_id": 2, "Iepirkuma_ID": "123", "Iepirkuma_dalas_nr": "2"},
                ],
            }
            updated = dict(procurement_record)
            updated["classification_reason"] = "Updated reason"

            upsert_procurement_records(
                db_path,
                [procurement_record],
                ingested_at="2026-03-13T00:00:00+00:00",
                classified_at="2026-03-13T00:01:00+00:00",
                classifier_model="gpt-4.1-mini",
            )
            upsert_procurement_records(
                db_path,
                [updated],
                ingested_at="2026-03-13T00:00:00+00:00",
                classified_at="2026-03-13T00:02:00+00:00",
                classifier_model="gpt-4.1-mini",
            )

            loaded = load_procurement_records_by_keys(
                db_path,
                [procurement_record_storage_key(procurement_record)],
            )
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["classification_reason"], "Updated reason")
            self.assertEqual(loaded[0]["raw_api_record_count"], 2)
            self.assertEqual(len(loaded[0]["raw_api_records"]), 2)

            with sqlite3.connect(db_path) as conn:
                stored = conn.execute(
                    """
                    SELECT "Iepirkuma_ID", "Iepirkuma_nosaukums", "Planotais_liguma_darbibas_termins"
                    FROM procurement_records
                    WHERE procurement_id = '123'
                    """
                ).fetchone()
            self.assertEqual(stored[0], "123")
            self.assertEqual(stored[1], "Procurement Name")
            self.assertEqual(stored[2], "48")


if __name__ == "__main__":
    unittest.main()
