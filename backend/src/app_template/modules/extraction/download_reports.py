#!/usr/bin/env python3
"""
Download final report documents for outcome extraction.

This step downloads all final report documents (PDF, DOC, DOCX, EDOC) to local storage
WITHOUT extracting data. Data extraction happens in a separate step.

Workflow:
1. Read procurement records from SQLite or input JSONL
2. For each project, find and download final report document
3. Save documents to reports/ directory
4. Extract archives (.edoc) to get the actual document
5. Optionally export JSONL with document paths
"""

from __future__ import annotations

import argparse
import concurrent.futures
import http.cookiejar
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.request import HTTPCookieProcessor, Request, build_opener
from urllib.parse import urlencode, urlsplit

from .collector_classes import configure_request_pacer, paced_opener_open
from .collector_io import read_projects_file, write_jsonl
from .collector_storage import load_procurement_records_for_pipeline, update_procurement_report_metadata
from .collector_outcomes import (
    extract_view_document_json_params,
    extract_view_document_files,
    select_final_report_document,
)
from .document_extractor import extract_documents_from_archive
from .utils import extract_js_array, is_captcha_page, parse_csrf_token

SKIP_DOWNLOAD_STATUSES = {"Pārtraukts", "Izbeigts"}
EIS_PUBLICATION_MARKER = "Uzmanību, šī ir EIS publikācija"
DEFAULT_DOWNLOAD_WORKERS = 3


def find_existing_report_file(output_dir: Path, procurement_id: Any) -> Path | None:
    procurement_id_text = str(procurement_id or "").strip()
    if not procurement_id_text or not output_dir.exists():
        return None
    matches = sorted(output_dir.glob(f"{procurement_id_text}.*"))
    return matches[0] if matches else None


def should_skip_download_for_status(procurement_status: Any) -> bool:
    return str(procurement_status or "").strip() in SKIP_DOWNLOAD_STATUSES


def is_eps_publication_page(page_html: str) -> bool:
    return EIS_PUBLICATION_MARKER in page_html


def download_final_report_document(
    project: Dict[str, Any],
    output_dir: Path,
    request_timeout_seconds: int = 60,
) -> Dict[str, Any]:
    """
    Download final report document for a project.

    Returns dict with:
        - document_path: Path to downloaded document
        - document_found: bool
        - error: Optional error message
    """
    procurement_id = project.get("procurement_id", "unknown")
    procurement_url = project.get("procurement_url", "")

    result = {
        "document_found": False,
        "document_path": None,
        "document_type": None,
        "error": None,
        "download_url": None,
        "selection_method": "select_final_report_document",
        "selection_reason": None,
        "selected_title": None,
        "is_eps_publication": "unknown",
    }

    if not procurement_url:
        result["error"] = "missing_procurement_url"
        return result

    try:
        cookie_jar = http.cookiejar.CookieJar()
        opener = build_opener(HTTPCookieProcessor(cookie_jar))

        # Fetch procurement page (use real browser User-Agent to avoid blocking)
        browser_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        req = Request(procurement_url, headers={"User-Agent": browser_ua})
        with paced_opener_open(opener, req, timeout=request_timeout_seconds) as response:
            page_html = response.read().decode("utf-8", "ignore")

        if is_captcha_page(page_html):
            result["error"] = "captcha_challenge"
            return result

        result["is_eps_publication"] = "yes" if is_eps_publication_page(page_html) else "no"
        if result["is_eps_publication"] == "yes":
            result["error"] = "eps_publication"
            result["selection_method"] = "page_type_detection"
            result["selection_reason"] = (
                "Skipped report download because the EIS page is marked as an EPS publication "
                "for consultation or non-standard submission."
            )
            return result

        # Find final report document
        actual_docs = extract_js_array(page_html, "ActualDocuments_items")
        final_report_doc = select_final_report_document(actual_docs)

        if not final_report_doc:
            result["error"] = "final_report_not_found"
            result["selection_reason"] = "No document matched the current final-report selector."
            return result

        result["selected_title"] = str(final_report_doc.get("Title") or "")
        result["selection_reason"] = (
            f"Matched current final-report selector with title: {result['selected_title']}"
        )

        csrf_token = parse_csrf_token(page_html)
        if not csrf_token:
            result["error"] = "csrf_token_missing"
            return result

        # Extract document metadata
        file_id = final_report_doc.get("FileId") or final_report_doc.get("Id")
        file_name = final_report_doc.get("Title", "report") + ".bin"

        # Fetch ViewDocument modal HTML via POST (same flow as the full downloader).
        view_params = {
            "__RequestVerificationToken": csrf_token,
            "Id": str(final_report_doc.get("Id") or ""),
            "FileId": str(file_id or ""),
            "DocumentLinkTypeCode": str(final_report_doc.get("DocumentLinkTypeCode") or ""),
            "ParentObjectTypeCode": str(final_report_doc.get("ParentObjectTypeCode") or ""),
            "ParentId": str(final_report_doc.get("ParentId") or ""),
            "ParentIdentifier": str(final_report_doc.get("ParentIdentifier") or ""),
            "ProcurementIdentifier": str(final_report_doc.get("ProcurementIdentifier") or procurement_id),
            "StageIdentifier": str(final_report_doc.get("StageIdentifier") or ""),
        }
        parsed = urlsplit(procurement_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}/EKEIS"
        view_req = Request(
            f"{base_url}/Document/ViewDocument",
            data=urlencode(view_params).encode("utf-8"),
            headers={
                "User-Agent": browser_ua,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "RequestVerificationToken": csrf_token,
                "Referer": procurement_url,
                "Origin": f"{parsed.scheme}://{parsed.netloc}",
            },
            method="POST",
        )
        with paced_opener_open(opener, view_req, timeout=request_timeout_seconds) as response:
            view_html = response.read().decode("utf-8", "ignore")

        # Extract base parameters and file list
        json_params = extract_view_document_json_params(view_html)
        files_list = extract_view_document_files(view_html)

        # Get the actual file information (should be only one file)
        if not files_list:
            result["error"] = "no_files_in_view_document"
            return result

        file_info = files_list[0]  # Take first file
        file_name = file_info.get("Title", file_name)
        actual_file_id = file_info.get("Id", file_id)

        # Build download URL using json_params as base and file_info for FileId
        download_params = {
            "Id": str(json_params.get("Id") or final_report_doc.get("Id") or ""),
            "FileId": str(actual_file_id),
            "DocumentLinkTypeCode": str(json_params.get("DocumentLinkTypeCode") or final_report_doc.get("DocumentLinkTypeCode") or ""),
            "ParentObjectTypeCode": str(json_params.get("ParentObjectTypeCode") or final_report_doc.get("ParentObjectTypeCode") or ""),
            "ParentId": str(json_params.get("ParentId") or final_report_doc.get("ParentId") or ""),
            "ParentIdentifier": str(json_params.get("ParentIdentifier") or final_report_doc.get("ParentIdentifier") or ""),
            "ProcurementIdentifier": str(json_params.get("ProcurementIdentifier") or procurement_id),
            "StageIdentifier": str(json_params.get("StageIdentifier") or final_report_doc.get("StageIdentifier") or ""),
        }

        download_url = f"{base_url}/Document/DownloadDocumentFile?{urlencode(download_params)}"
        result["download_url"] = download_url

        # Download document
        download_request = Request(
            download_url,
            headers={"User-Agent": browser_ua, "Referer": procurement_url},
        )

        with paced_opener_open(opener, download_request, timeout=request_timeout_seconds) as response:
            report_bytes = response.read()

        # Check if response is HTML error page instead of actual document
        if report_bytes.startswith(b'<!DOCTYPE html') or report_bytes.startswith(b'<html') or report_bytes.startswith(b'\xef\xbb\xbf<!DOCTYPE'):
            result["error"] = "html_error_page_received"
            return result

        # Check content type to ensure we got a document
        if len(report_bytes) < 100:
            result["error"] = f"file_too_small:{len(report_bytes)}_bytes"
            return result

        # Save document
        output_dir.mkdir(parents=True, exist_ok=True)
        ext = Path(file_name).suffix.lower() or ".bin"

        # Handle archives - extract and save only the main document
        if ext in {".edoc", ".asice", ".zip", ".rar"}:
            # Write to temp for extraction
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                tmp_path = Path(tmp.name)
                tmp.write(report_bytes)

            try:
                extracted_docs = extract_documents_from_archive(tmp_path)
                if extracted_docs:
                    # Save extracted document
                    main_doc = extracted_docs[0]
                    extracted_ext = Path(main_doc.filename).suffix.lower()
                    save_path = output_dir / f"{procurement_id}{extracted_ext}"
                    save_path.write_bytes(main_doc.content)

                    result["document_found"] = True
                    result["document_path"] = str(save_path)
                    result["document_type"] = extracted_ext
                    result["original_archive"] = file_name
                else:
                    result["error"] = "archive_extraction_failed"
            finally:
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
        else:
            # Regular document
            save_path = output_dir / f"{procurement_id}{ext}"
            save_path.write_bytes(report_bytes)

            result["document_found"] = True
            result["document_path"] = str(save_path)
            result["document_type"] = ext

    except Exception as exc:
        result["error"] = f"download_error:{str(exc)}"

    return result


def run(args):
    """Download final report documents for all projects."""
    print("STEP: Downloading Final Report Documents")
    print("-" * 60)
    if getattr(args, "input_file", None):
        print(f"Input JSONL: {args.input_file}")
    else:
        print(f"Input SQLite: {args.database_path}")
    print(f"Output dir: {args.output_dir}")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")

    # Load projects
    if getattr(args, "input_file", None):
        projects = read_projects_file(Path(args.input_file))
    else:
        if not getattr(args, "database_path", None):
            raise RuntimeError("--database-path is required when --input-file is not provided.")
        projects = load_procurement_records_for_pipeline(Path(args.database_path))
        if getattr(args, "only_not_processed", False):
            projects = [
                row
                for row in projects
                if str(row.get("report_download_status") or "not_processed") == "not_processed"
            ]
        if getattr(args, "only_failed", False):
            projects = [
                row
                for row in projects
                if str(row.get("report_download_status") or "") == "failed"
            ]
        retry_error_prefix = str(getattr(args, "retry_error_prefix", "") or "").strip()
        if retry_error_prefix:
            projects = [
                row
                for row in projects
                if str(row.get("report_download_error") or "").startswith(retry_error_prefix)
            ]
        if getattr(args, "limit", None):
            projects = projects[: max(0, int(args.limit))]
    print(f"Loaded {len(projects)} projects")

    # Configure rate limiting
    configure_request_pacer(
        min_interval_seconds=1.0 / args.rate_limit,
        jitter_seconds=0.1,
        pause_every_requests=100,
        pause_duration_seconds=1.0,
    )

    # Ensure top-level output directory exists for summary JSONL writing.
    base_output_dir = Path(args.output_dir)
    base_output_dir.mkdir(parents=True, exist_ok=True)

    # Download documents
    output_dir = base_output_dir / "reports"
    downloaded_count = 0
    failed_count = 0
    skipped_existing_count = 0
    skipped_status_count = 0

    max_workers = max(1, int(getattr(args, "workers", DEFAULT_DOWNLOAD_WORKERS)))

    def apply_download_result(project: Dict[str, Any], result: Dict[str, Any], downloaded_at: str) -> None:
        nonlocal downloaded_count, failed_count
        project["report_document_path"] = result.get("document_path")
        project["report_document_type"] = result.get("document_type")
        project["report_download_url"] = result.get("download_url")
        project["report_selection_method"] = result.get("selection_method")
        project["report_selection_reason"] = result.get("selection_reason")
        project["report_selected_title"] = result.get("selected_title")
        project["report_downloaded_at"] = downloaded_at
        project["report_download_error"] = result.get("error")
        project["is_eps_publication"] = result.get("is_eps_publication") or "unknown"
        if result.get("document_found"):
            project["report_download_status"] = "downloaded"
        elif result.get("error") == "eps_publication":
            project["report_download_status"] = "skipped_eps_publication"
        else:
            project["report_download_status"] = "failed"

        if result.get("original_archive"):
            project["report_original_archive"] = result["original_archive"]

        if result.get("document_found"):
            downloaded_count += 1
            doc_type = result["document_type"]
            print(f"OK {doc_type}")
        else:
            failed_count += 1
            print(f"FAILED {result.get('error', 'unknown_error')}")

        if getattr(args, "database_path", None):
            update_procurement_report_metadata(
                Path(args.database_path),
                project,
                downloaded_at=downloaded_at,
            )

    futures: dict[concurrent.futures.Future[Dict[str, Any]], tuple[int, Dict[str, Any], str]] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, project in enumerate(projects, 1):
            procurement_id = project.get("procurement_id", "unknown")
            print(f"[{idx}/{len(projects)}] {procurement_id}...", end=" ")
            downloaded_at = datetime.now(timezone.utc).isoformat()

            if should_skip_download_for_status(project.get("procurement_status")):
                project["is_eps_publication"] = "unknown"
                project["report_document_path"] = None
                project["report_document_type"] = None
                project["report_download_url"] = None
                project["report_selection_method"] = "skipped_by_procurement_status"
                project["report_selection_reason"] = (
                    f"Skipped remote download because procurement_status is {project.get('procurement_status')}."
                )
                project["report_selected_title"] = None
                project["report_downloaded_at"] = downloaded_at
                project["report_download_error"] = None
                project["report_download_status"] = "skipped_status"
                skipped_status_count += 1
                print(f"SKIPPED status {project.get('procurement_status')}")

                if getattr(args, "database_path", None):
                    update_procurement_report_metadata(
                        Path(args.database_path),
                        project,
                        downloaded_at=downloaded_at,
                    )
                continue

            existing_report = find_existing_report_file(output_dir, procurement_id)
            if existing_report is not None:
                project["is_eps_publication"] = project.get("is_eps_publication") or "unknown"
                project["report_document_path"] = str(existing_report)
                project["report_document_type"] = existing_report.suffix.lower()
                project["report_download_url"] = None
                project["report_selection_method"] = "existing_local_file"
                project["report_selection_reason"] = "Skipped remote download because a local report file already exists."
                project["report_selected_title"] = existing_report.name
                project["report_downloaded_at"] = downloaded_at
                project["report_download_error"] = None
                project["report_download_status"] = "already_present"
                skipped_existing_count += 1
                print(f"SKIPPED existing {existing_report.name}")

                if getattr(args, "database_path", None):
                    update_procurement_report_metadata(
                        Path(args.database_path),
                        project,
                        downloaded_at=downloaded_at,
                    )
                continue

            future = executor.submit(
                download_final_report_document,
                project=project,
                output_dir=output_dir,
                request_timeout_seconds=args.request_timeout,
            )
            futures[future] = (idx, project, downloaded_at)

        for future in concurrent.futures.as_completed(futures):
            idx, project, downloaded_at = futures[future]
            procurement_id = project.get("procurement_id", "unknown")
            print(f"[{idx}/{len(projects)}] {procurement_id}...", end=" ")
            result = future.result()
            apply_download_result(project, result, downloaded_at)

    print(f"\nFinished at: {datetime.now(timezone.utc).isoformat()}")
    print("-" * 60)
    print(f"Downloaded: {downloaded_count}")
    print(f"Already present: {skipped_existing_count}")
    print(f"Skipped by status: {skipped_status_count}")
    print(f"Failed: {failed_count}")
    print(f"Documents saved to: {output_dir}")
    output_file = None
    if getattr(args, "export_jsonl", False):
        output_file = base_output_dir / "projects_with_documents.jsonl"
        write_jsonl(output_file, projects)
        print(f"Project list: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download final report documents")
    parser.add_argument("--input-file", default=None, help="Optional input JSONL file with projects")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--database-path", default=None, help="SQLite database path")
    parser.add_argument("--rate-limit", type=float, default=2.0, help="Requests per second")
    parser.add_argument("--request-timeout", type=int, default=60, help="Request timeout in seconds")
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_DOWNLOAD_WORKERS,
        help="Worker threads for bounded parallel downloads",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional limit when loading from SQLite")
    parser.add_argument(
        "--only-not-processed",
        action="store_true",
        help="When loading from SQLite, process only rows with report_download_status = not_processed",
    )
    parser.add_argument(
        "--only-failed",
        action="store_true",
        help="When loading from SQLite, process only rows with report_download_status = failed",
    )
    parser.add_argument(
        "--retry-error-prefix",
        default=None,
        help="When loading from SQLite, restrict rows to report_download_error values starting with this prefix",
    )
    parser.add_argument(
        "--export-jsonl",
        action="store_true",
        help="Also export projects_with_documents.jsonl after the run",
    )

    args = parser.parse_args()
    run(args)
