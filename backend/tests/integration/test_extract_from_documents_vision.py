import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipelines.extract_from_documents import extract_from_local_document_llm


class _DummyLLMExtractor:
    def extract(self, project, file_name, report_text):
        return {
            "winner_name": "Text Winner",
            "winner_registration_no": "40000000000",
            "winner_price_eur": 10.0,
            "participants": [{"name": "Text Winner", "registration_no": "40000000000", "suggested_price_eur": 10.0}],
            "confidence": "medium",
            "notes": "",
        }

    def extract_from_images(self, project, file_name, images_base64):
        return {
            "winner_name": "Vision Winner",
            "winner_registration_no": "50000000000",
            "winner_price_eur": 20.0,
            "participants": [{"name": "Vision Winner", "registration_no": "50000000000", "suggested_price_eur": 20.0}],
            "confidence": "high",
            "notes": "",
        }


class ExtractFromDocumentsVisionTests(unittest.TestCase):
    def test_uses_vision_when_pdf_text_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "report.pdf"
            pdf_path.write_bytes(b"%PDF-1.4 fake")

            with mock.patch(
                "pipelines.extract_from_documents.extract_text_from_report_bytes",
                return_value="",
            ), mock.patch(
                "pipelines.extract_from_documents.convert_pdf_to_images_base64",
                return_value=["ZmFrZV9pbWFnZQ=="],
            ):
                result = extract_from_local_document_llm(
                    project={"procurement_id": 1},
                    document_path=str(pdf_path),
                    llm_extractor=_DummyLLMExtractor(),
                )

            self.assertEqual(result["procurement_winner"], "Vision Winner")
            self.assertEqual(result["procurement_winner_source"], "final_report_vision")
            self.assertEqual(result["procurement_participants_count"], 1)
            self.assertIn("llm_method:vision", str(result["procurement_outcome_description"]))


if __name__ == "__main__":
    unittest.main()
