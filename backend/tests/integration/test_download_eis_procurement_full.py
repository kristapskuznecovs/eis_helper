import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipelines import download_documents


class DownloadDocumentsPipelineTests(unittest.TestCase):
    def test_run_from_input_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects.jsonl"
            output_dir = base / "downloads"
            input_file.write_text(
                "\n".join(
                    [
                        json.dumps({"procurement_id": 11}),
                        json.dumps({"procurement_id": 22}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            args = argparse.Namespace(
                procurement_id=None,
                procurement_ids=None,
                input_file=str(input_file),
                max_projects=None,
                output_dir=str(output_dir),
                include_historical=False,
                keep_zips=False,
                request_timeout=5,
                min_request_interval_seconds=0.0,
                request_jitter_seconds=0.0,
                pause_every_requests=0,
                pause_duration_seconds=0.0,
            )

            with mock.patch.object(
                download_documents,
                "download_procurement_files",
                side_effect=[{"status": "success", "files_downloaded": 2, "total_size": 100}, {"status": "error", "error": "boom"}],
            ):
                result = download_documents.run(args)

            self.assertEqual(result, 0)
            summary = json.loads((output_dir / "download_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["summary"]["total_procurements"], 2)
            self.assertEqual(summary["summary"]["successful_downloads"], 1)
            self.assertEqual(summary["summary"]["failed_downloads"], 1)


if __name__ == "__main__":
    unittest.main()
