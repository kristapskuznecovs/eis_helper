import argparse
import json
import tempfile
import unittest
from pathlib import Path

from pipelines import organize_documents


class OrganizeDocumentsPipelineTests(unittest.TestCase):
    def test_run_organizes_files_to_other(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            raw_dir = base / "raw"
            output_dir = base / "organized"
            (raw_dir / "123").mkdir(parents=True, exist_ok=True)
            (raw_dir / "123" / "doc.pdf").write_text("x", encoding="utf-8")

            args = argparse.Namespace(
                input_file=None,
                procurement_ids=[123],
                max_projects=None,
                raw_data_dir=str(raw_dir),
                output_dir=str(output_dir),
                classification_mode="heuristic",
                min_request_interval_seconds=0.0,
                request_jitter_seconds=0.0,
            )

            result = organize_documents.run(args)
            self.assertEqual(result, 0)

            out_file = output_dir / "123" / "OTHER" / "doc.pdf"
            self.assertTrue(out_file.exists())

            metadata = json.loads((output_dir / "123" / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["total_files"], 1)


if __name__ == "__main__":
    unittest.main()
