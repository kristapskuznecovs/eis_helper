import argparse
import json
import tempfile
import unittest
import os
from pathlib import Path
from unittest import mock

from pipelines import extract_from_documents


class ExtractOutcomesPipelineTests(unittest.TestCase):
    def test_run_updates_projects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects_with_documents.jsonl"
            output_file = base / "projects_with_outcomes.jsonl"
            input_file.write_text(
                json.dumps(
                    {
                        "procurement_id": 1,
                        "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/1",
                        "procurement_status": "Pabeigts",
                        "report_document_path": "/tmp/fake.pdf",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            args = argparse.Namespace(
                input_file=str(input_file),
                output_file=str(output_file),
                provider="openai",
                llm_model="gpt-4o",
                llm_vision_model=None,
                llm_base_url="https://api.openai.com/v1",
                database=None,
                request_delay=0.0,
            )

            with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}, clear=False), \
                mock.patch.object(extract_from_documents, "OutcomeLLMExtractor"), \
                mock.patch.object(
                    extract_from_documents,
                    "extract_from_local_document_llm",
                    return_value={
                        "procurement_winner": "Winner Ltd",
                        "procurement_winner_registration_no": "40000000000",
                        "procurement_winner_suggested_price_eur": 12345.0,
                        "procurement_winner_source": "final_report_text",
                        "procurement_participants_count": 1,
                        "procurement_participants": [{"name": "Winner Ltd"}],
                        "procurement_outcome_description": "ok",
                    },
                ):
                result = extract_from_documents.run(args)

            self.assertIsNone(result)
            self.assertTrue(output_file.exists())

            row = json.loads(output_file.read_text(encoding="utf-8").strip())
            self.assertEqual(row["procurement_winner"], "Winner Ltd")


if __name__ == "__main__":
    unittest.main()
