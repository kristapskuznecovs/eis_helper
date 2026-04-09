import argparse
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from lib.collector_storage import initialize_procurement_records, update_procurement_report_metadata
from pipelines import download_reports


class DownloadReportsPipelineTests(unittest.TestCase):
    def test_run_loads_projects_directly_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            output_dir = base / "out"
            db_path = base / "database" / "eis_procurement_records.sqlite"

            project = {
                "procurement_id": "100005",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100005",
                "procurement_name": "Test Procurement",
                "raw_api_records": [{"Iepirkuma_ID": "100005"}],
                "raw_api_record_count": 1,
            }
            initialize_procurement_records(db_path, [project], ingested_at="2026-03-13T00:00:00+00:00")

            args = argparse.Namespace(
                input_file=None,
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=1,
                only_not_processed=True,
                export_jsonl=False,
            )

            with mock.patch.object(
                download_reports,
                "download_final_report_document",
                return_value={
                    "document_found": True,
                    "document_path": str(output_dir / "reports" / "100005.pdf"),
                    "document_type": ".pdf",
                    "download_url": "https://www.eis.gov.lv/EKEIS/Document/DownloadDocumentFile?x=1",
                    "selection_method": "select_final_report_document",
                    "selection_reason": "Matched current final-report selector.",
                    "selected_title": "Noslēguma ziņojums",
                    "error": None,
                    "original_archive": None,
                    "is_eps_publication": "no",
                },
            ):
                result = download_reports.run(args)

            self.assertIsNone(result)

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT report_download_status, is_eps_publication
                    FROM procurement_records
                    WHERE procurement_id = '100005'
                    """
                ).fetchone()
            self.assertEqual(row[0], "downloaded")
            self.assertEqual(row[1], "no")
            self.assertFalse((output_dir / "projects_with_documents.jsonl").exists())

    def test_run_updates_sqlite_with_report_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects.jsonl"
            output_dir = base / "out"
            db_path = base / "database" / "eis_procurement_records.sqlite"

            project = {
                "procurement_id": "100005",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100005",
                "procurement_name": "Test Procurement",
                "raw_api_records": [{"Iepirkuma_ID": "100005"}],
                "raw_api_record_count": 1,
            }
            input_file.write_text(json.dumps(project) + "\n", encoding="utf-8")
            initialize_procurement_records(db_path, [project], ingested_at="2026-03-13T00:00:00+00:00")

            args = argparse.Namespace(
                input_file=str(input_file),
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=None,
                only_not_processed=False,
                export_jsonl=False,
            )

            with mock.patch.object(
                download_reports,
                "download_final_report_document",
                return_value={
                    "document_found": True,
                    "document_path": str(output_dir / "reports" / "100005.pdf"),
                    "document_type": ".pdf",
                    "download_url": "https://www.eis.gov.lv/EKEIS/Document/DownloadDocumentFile?x=1",
                    "selection_method": "select_final_report_document",
                    "selection_reason": "Matched current final-report selector.",
                    "selected_title": "Noslēguma ziņojums",
                    "error": None,
                    "original_archive": None,
                },
            ):
                result = download_reports.run(args)

            self.assertIsNone(result)

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT report_document_path,
                           report_document_type,
                           report_download_status,
                           report_selection_method,
                           report_selected_title
                    FROM procurement_records
                    WHERE procurement_id = '100005'
                    """
                ).fetchone()
            self.assertEqual(row[0], str(output_dir / "reports" / "100005.pdf"))
            self.assertEqual(row[1], ".pdf")
            self.assertEqual(row[2], "downloaded")
            self.assertEqual(row[3], "select_final_report_document")
            self.assertEqual(row[4], "Noslēguma ziņojums")

    def test_run_filters_failed_rows_by_error_prefix_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            output_dir = base / "out"
            db_path = base / "database" / "eis_procurement_records.sqlite"

            first = {
                "procurement_id": "100005",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100005",
                "procurement_name": "Retry Me",
                "raw_api_records": [{"Iepirkuma_ID": "100005"}],
                "raw_api_record_count": 1,
            }
            second = {
                "procurement_id": "100006",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100006",
                "procurement_name": "Do Not Retry",
                "raw_api_records": [{"Iepirkuma_ID": "100006"}],
                "raw_api_record_count": 1,
            }
            initialize_procurement_records(db_path, [first, second], ingested_at="2026-03-13T00:00:00+00:00")

            failed_retry = dict(first)
            failed_retry.update(
                {
                    "report_download_status": "failed",
                    "report_download_error": "download_error:Remote end closed connection without response",
                    "report_selection_method": "select_final_report_document",
                    "report_selection_reason": "Matched current final-report selector.",
                    "report_selected_title": "Noslēguma ziņojums",
                    "is_eps_publication": "no",
                }
            )
            update_procurement_report_metadata(
                db_path,
                failed_retry,
                downloaded_at="2026-03-13T00:01:00+00:00",
            )

            failed_other = dict(second)
            failed_other.update(
                {
                    "report_download_status": "failed",
                    "report_download_error": "final_report_not_found",
                    "report_selection_method": "select_final_report_document",
                    "report_selection_reason": "No document matched the current final-report selector.",
                    "report_selected_title": "not_processed",
                    "is_eps_publication": "no",
                }
            )
            update_procurement_report_metadata(
                db_path,
                failed_other,
                downloaded_at="2026-03-13T00:02:00+00:00",
            )

            args = argparse.Namespace(
                input_file=None,
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=None,
                only_not_processed=False,
                only_failed=True,
                retry_error_prefix="download_error:",
                export_jsonl=False,
            )

            with mock.patch.object(
                download_reports,
                "download_final_report_document",
                return_value={
                    "document_found": True,
                    "document_path": str(output_dir / "reports" / "100005.pdf"),
                    "document_type": ".pdf",
                    "download_url": "https://www.eis.gov.lv/EKEIS/Document/DownloadDocumentFile?x=1",
                    "selection_method": "select_final_report_document",
                    "selection_reason": "Matched current final-report selector.",
                    "selected_title": "Noslēguma ziņojums",
                    "error": None,
                    "original_archive": None,
                    "is_eps_publication": "no",
                },
            ) as download_mock:
                result = download_reports.run(args)

            self.assertIsNone(result)
            self.assertEqual(download_mock.call_count, 1)

    def test_run_skips_when_local_report_file_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects.jsonl"
            output_dir = base / "out"
            reports_dir = output_dir / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            existing_file = reports_dir / "100005.pdf"
            existing_file.write_bytes(b"existing pdf bytes")
            db_path = base / "database" / "eis_procurement_records.sqlite"

            project = {
                "procurement_id": "100005",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100005",
                "procurement_name": "Test Procurement",
                "raw_api_records": [{"Iepirkuma_ID": "100005"}],
                "raw_api_record_count": 1,
            }
            input_file.write_text(json.dumps(project) + "\n", encoding="utf-8")
            initialize_procurement_records(db_path, [project], ingested_at="2026-03-13T00:00:00+00:00")

            args = argparse.Namespace(
                input_file=str(input_file),
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=None,
                only_not_processed=False,
                export_jsonl=False,
            )

            with mock.patch.object(download_reports, "download_final_report_document") as download_mock:
                result = download_reports.run(args)

            self.assertIsNone(result)
            download_mock.assert_not_called()

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT report_document_path,
                           report_download_status,
                           report_selection_method
                    FROM procurement_records
                    WHERE procurement_id = '100005'
                    """
                ).fetchone()
            self.assertEqual(row[0], str(existing_file))
            self.assertEqual(row[1], "already_present")
            self.assertEqual(row[2], "existing_local_file")

    def test_run_skips_when_procurement_status_is_interrupted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects.jsonl"
            output_dir = base / "out"
            db_path = base / "database" / "eis_procurement_records.sqlite"

            project = {
                "procurement_id": "100005",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100005",
                "procurement_name": "Test Procurement",
                "procurement_status": "Pārtraukts",
                "raw_api_records": [{"Iepirkuma_ID": "100005", "Iepirkuma_statuss": "Pārtraukts"}],
                "raw_api_record_count": 1,
            }
            input_file.write_text(json.dumps(project) + "\n", encoding="utf-8")
            initialize_procurement_records(db_path, [project], ingested_at="2026-03-13T00:00:00+00:00")

            args = argparse.Namespace(
                input_file=str(input_file),
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=None,
                only_not_processed=False,
                export_jsonl=False,
            )

            with mock.patch.object(download_reports, "download_final_report_document") as download_mock:
                result = download_reports.run(args)

            self.assertIsNone(result)
            download_mock.assert_not_called()

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT report_download_status, report_selection_method
                    FROM procurement_records
                    WHERE procurement_id = '100005'
                    """
                ).fetchone()
            self.assertEqual(row[0], "skipped_status")
            self.assertEqual(row[1], "skipped_by_procurement_status")

    def test_run_marks_eps_publication_and_skips_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            input_file = base / "projects.jsonl"
            output_dir = base / "out"
            db_path = base / "database" / "eis_procurement_records.sqlite"

            project = {
                "procurement_id": "100080",
                "procurement_url": "https://www.eis.gov.lv/EKEIS/Supplier/Procurement/100080",
                "procurement_name": "EPS Publication Example",
                "raw_api_records": [{"Iepirkuma_ID": "100080"}],
                "raw_api_record_count": 1,
            }
            input_file.write_text(json.dumps(project) + "\n", encoding="utf-8")
            initialize_procurement_records(db_path, [project], ingested_at="2026-03-13T00:00:00+00:00")

            args = argparse.Namespace(
                input_file=str(input_file),
                output_dir=str(output_dir),
                database_path=str(db_path),
                rate_limit=2.0,
                request_timeout=60,
                workers=1,
                limit=None,
                only_not_processed=False,
                export_jsonl=False,
            )

            with mock.patch.object(
                download_reports,
                "download_final_report_document",
                return_value={
                    "document_found": False,
                    "document_path": None,
                    "document_type": None,
                    "download_url": None,
                    "selection_method": "page_type_detection",
                    "selection_reason": (
                        "Skipped report download because the EIS page is marked as an EPS publication "
                        "for consultation or non-standard submission."
                    ),
                    "selected_title": None,
                    "error": "eps_publication",
                    "original_archive": None,
                    "is_eps_publication": "yes",
                },
            ):
                result = download_reports.run(args)

            self.assertIsNone(result)

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT report_download_status,
                           report_selection_method,
                           is_eps_publication
                    FROM procurement_records
                    WHERE procurement_id = '100080'
                    """
                ).fetchone()
            self.assertEqual(row[0], "skipped_eps_publication")
            self.assertEqual(row[1], "page_type_detection")
            self.assertEqual(row[2], "yes")


if __name__ == "__main__":
    unittest.main()
