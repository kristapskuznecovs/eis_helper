import argparse
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipelines import fetch_metadata


class FetchMetadataPipelineTests(unittest.TestCase):
    def test_run_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "out"

            args = argparse.Namespace(
                action_url="https://data.gov.lv/dati/api/3/action",
                package_id="pkg",
                from_year=2024,
                to_year=2024,
                cpv_prefixes=["45"],
                batch_size=100,
                min_estimated_value=None,
                classification_mode="openai",
                classification_workers=1,
                openai_model="gpt-4.1-mini",
                openai_api_key_env="OPENAI_API_KEY",
                openai_base_url="https://api.openai.com/v1",
                openai_temperature=0.0,
                openai_top_p=1.0,
                openai_max_output_tokens=128,
                openai_response_format="json_object",
                openai_max_projects=None,
                openai_system_prompt_file="config/agents/classification/system_prompt.txt",
                openai_user_prompt_file="config/agents/classification/user_prompt.txt",
                doc_title_regex=[r"buvprojekt"],
                include_historical=False,
                workers=1,
                min_request_interval_seconds=0.0,
                request_jitter_seconds=0.0,
                pause_every_requests=0,
                pause_duration_seconds=0.0,
                agent_config_file="config/agent_config.json",
                pipeline_config_file="pipeline_config.json",
                no_agent_config=True,
                output_dir=str(output_dir),
                database_path=str(Path(tmpdir) / "database" / "eis_procurement_records.sqlite"),
            )

            procurement_records = [
                {
                    "procurement_id": 100,
                    "year": 2024,
                    "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100",
                    "procurement_status": "Pabeigts",
                }
            ]

            classified_procurement_records = [
                {
                    **procurement_records[0],
                    "classification_domain": "building",
                    "classification_scope_type": "design_only",
                    "classification_work_type": "unknown",
                    "classification_asset_scale": "large",
                    "classification_final_category": "building_design",
                    "classification_reason": "Design procurement for a building.",
                }
            ]

            with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False):
                with mock.patch.object(
                    fetch_metadata,
                    "collect_procurement_records",
                    return_value=(classified_procurement_records, {"2024": 1}, {"2024": 1}, 1),
                ), mock.patch.object(
                    fetch_metadata,
                    "_build_openai_classifier",
                    return_value=mock.Mock(model="gpt-4.1-mini"),
                ), mock.patch.object(
                    fetch_metadata,
                    "classify_procurement_records",
                    side_effect=lambda **kwargs: (
                        kwargs["on_project_classified"](classified_procurement_records[0], 1, 1),
                        {"building_design": 1},
                    )[1],
                ), mock.patch.object(
                    fetch_metadata,
                    "scan_project_for_docs",
                    return_value={
                        **classified_procurement_records[0],
                        "matched_document_count": 1,
                        "matched_document_titles": ["actual:Būvprojekts"],
                    },
                ):
                    result = fetch_metadata.run(args)

            self.assertEqual(result, 0)
            self.assertTrue((output_dir / "all_construction_projects.jsonl").exists())
            self.assertTrue((output_dir / "projects_with_design_docs.jsonl").exists())
            self.assertTrue((output_dir / "failed_projects.jsonl").exists())
            self.assertTrue((Path(tmpdir) / "database" / "eis_procurement_records.sqlite").exists())

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["summary"]["procurement_records"], 1)
            self.assertEqual(manifest["summary"]["matched_projects"], 1)
            self.assertEqual(manifest["summary"]["sqlite_raw_rows_persisted"], 1)
            self.assertEqual(manifest["summary"]["sqlite_classification_updates_persisted"], 1)

            all_rows = (output_dir / "all_construction_projects.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(all_rows), 1)
            parsed = json.loads(all_rows[0])
            self.assertEqual(parsed["classification_final_category"], "building_design")

            with sqlite3.connect(Path(tmpdir) / "database" / "eis_procurement_records.sqlite") as conn:
                count = conn.execute("SELECT COUNT(*) FROM procurement_records").fetchone()[0]
            self.assertEqual(count, 1)

    def test_run_stores_raw_rows_then_persists_classification_batches_every_ten_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "out"
            args = argparse.Namespace(
                action_url="https://data.gov.lv/dati/api/3/action",
                package_id="pkg",
                from_year=2024,
                to_year=2024,
                cpv_prefixes=["45"],
                batch_size=100,
                min_estimated_value=None,
                classification_mode="openai",
                classification_workers=1,
                openai_model="gpt-4.1-mini",
                openai_api_key_env="OPENAI_API_KEY",
                openai_base_url="https://api.openai.com/v1",
                openai_temperature=0.0,
                openai_top_p=1.0,
                openai_max_output_tokens=128,
                openai_response_format="json_object",
                openai_max_projects=None,
                openai_system_prompt_file="config/agents/classification/system_prompt.txt",
                openai_user_prompt_file="config/agents/classification/user_prompt.txt",
                doc_title_regex=[r"buvprojekt"],
                include_historical=False,
                workers=1,
                min_request_interval_seconds=0.0,
                request_jitter_seconds=0.0,
                pause_every_requests=0,
                pause_duration_seconds=0.0,
                agent_config_file="config/agent_config.json",
                pipeline_config_file="pipeline_config.json",
                no_agent_config=True,
                output_dir=str(output_dir),
                database_path=str(Path(tmpdir) / "database" / "eis_procurement_records.sqlite"),
            )

            records = [
                {
                    "procurement_id": idx,
                    "year": 2024,
                    "procurement_url": f"https://www.eis.gov.lv/EKEIS/Supplier/Procurement/{idx}",
                    "procurement_status": "Pabeigts",
                    "estimated_value_eur": 1_500_000,
                }
                for idx in range(1, 12)
            ]

            classifier = mock.Mock()
            classifier.model = "gpt-4.1-mini"
            classifier.classify.side_effect = [
                {
                    "classification_domain": "building",
                    "classification_scope_type": "build_only",
                    "classification_work_type": "new_build",
                    "classification_asset_scale": "large",
                    "classification_final_category": "building_new_build",
                    "classification_reason": f"record-{idx}",
                }
                for idx in range(1, 12)
            ]

            with mock.patch.object(
                fetch_metadata,
                "collect_procurement_records",
                return_value=(records, {"2024": 11}, {"2024": 11}, 11),
            ), mock.patch.object(
                fetch_metadata,
                "_build_openai_classifier",
                return_value=classifier,
            ), mock.patch.object(
                fetch_metadata,
                "initialize_procurement_records",
                side_effect=lambda db_path, rows, **kwargs: len(list(rows)),
            ) as init_mock, mock.patch.object(
                fetch_metadata,
                "scan_project_for_docs",
                side_effect=lambda **kwargs: {
                    **kwargs["project"],
                    "matched_document_count": 0,
                    "matched_document_titles": [],
                },
            ), mock.patch.object(
                fetch_metadata,
                "upsert_procurement_records",
                side_effect=lambda db_path, rows, **kwargs: len(list(rows)),
            ) as upsert_mock, mock.patch.object(
                fetch_metadata,
                "load_procurement_records_by_keys",
                side_effect=lambda db_path, keys: records,
            ):
                result = fetch_metadata.run(args)

            self.assertEqual(result, 0)
            init_mock.assert_called_once()
            self.assertEqual(upsert_mock.call_count, 2)
            first_batch = upsert_mock.call_args_list[0].args[1]
            second_batch = upsert_mock.call_args_list[1].args[1]
            self.assertEqual(len(first_batch), 10)
            self.assertEqual(len(second_batch), 1)


if __name__ == "__main__":
    unittest.main()
