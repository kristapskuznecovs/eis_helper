#!/usr/bin/env python3
"""
Pipeline Step 3: Download Documents

Download all files from EIS procurement pages.

Inputs: projects list (from Step 1 or 2)
Outputs: Downloaded files in data/downloads/{procurement_id}/
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import re
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .collector_classes import configure_request_pacer
from .collector_io import read_projects_file
from .utils import (
    extract_js_array,
    is_captcha_page,
    load_dotenv_file,
    parse_csrf_token,
    slugify,
)

# Constants
DEFAULT_INPUT_FILE = "data/eis_building_docs_projects_2020_plus/projects_with_building_docs.jsonl"
DEFAULT_OUTPUT_DIR = "data/downloads"
DEFAULT_REQUEST_TIMEOUT = 60
DEFAULT_ENV_FILE = ".env"
EIS_BASE_URL = "https://www.eis.gov.lv"
EIS_APP_BASE_URL = f"{EIS_BASE_URL}/EKEIS"


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(UTC).isoformat()


def download_procurement_files(
    procurement_id: int,
    output_dir: Path,
    include_historical: bool = False,
    keep_zips: bool = False,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> dict[str, Any]:
    """
    Download all files for a procurement.

    Args:
        procurement_id: EIS procurement ID
        output_dir: Output directory for this procurement
        include_historical: Include historical documents section
        keep_zips: Keep ZIP files after extraction
        timeout: HTTP request timeout in seconds

    Returns:
        Dict with download results
    """
    procurement_url = f"{EIS_APP_BASE_URL}/Supplier/Procurement/{procurement_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup cookie jar and opener
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    urllib.request.install_opener(opener)

    # Fetch procurement page
    print(f"  Fetching {procurement_url}...")
    req = urllib.request.Request(procurement_url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            html_bytes = response.read()
            html_text = html_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        return {"status": "error", "error": str(e)}

    # Check for CAPTCHA
    if is_captcha_page(html_text):
        return {"status": "captcha"}

    # Save HTML
    html_path = output_dir / "procurement_page.html"
    html_path.write_text(html_text, encoding="utf-8")

    # Extract CSRF token
    csrf_token = parse_csrf_token(html_text)

    # Find document sections
    sections = []
    if include_historical:
        sections_match = re.search(
            r'var\s+documentSections\s*=\s*(\[.*?\]);',
            html_text,
            re.DOTALL,
        )
        if sections_match:
            sections = extract_js_array(html_text, "documentSections")

    # Default to "Actual" section
    if not sections:
        sections = [{"name": "actual", "label": "Actual"}]

    # Download files from each section
    downloaded_files = []
    total_size = 0

    for section in sections:
        section_name = slugify(section.get("name", "unknown"))
        section_dir = output_dir / section_name
        section_dir.mkdir(parents=True, exist_ok=True)

        print(f"  Section: {section_name}")

        # Extract document list for this section.

        doc_pattern = re.compile(
            r'ViewDocument\(\s*(\d+)\s*,\s*["\']([^"\']+)["\']\s*,\s*(\d+)\s*\)',
            re.IGNORECASE,
        )

        for match in doc_pattern.finditer(html_text):
            doc_id = match.group(1)
            doc_name = match.group(2)
            doc_type = match.group(3)

            # Download file
            download_url = f"{EIS_APP_BASE_URL}/Supplier/DownloadDocument"
            post_data = urllib.parse.urlencode({
                "documentId": doc_id,
                "documentType": doc_type,
                "__RequestVerificationToken": csrf_token or "",
            }).encode()

            try:
                req = urllib.request.Request(
                    download_url,
                    data=post_data,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    file_data = response.read()

                file_name = slugify(doc_name)
                file_path = section_dir / file_name

                # Check if ZIP
                if file_data[:4] == b"PK\x03\x04":
                    zip_path = section_dir / f"{file_name}.zip"
                    zip_path.write_bytes(file_data)

                    # Extract ZIP
                    with zipfile.ZipFile(zip_path) as zf:
                        zf.extractall(section_dir)

                    if not keep_zips:
                        zip_path.unlink()
                else:
                    file_path.write_bytes(file_data)

                downloaded_files.append(str(file_path.relative_to(output_dir)))
                total_size += len(file_data)

            except Exception as e:
                print(f"    Failed to download {doc_name}: {e}")

    # Save metadata
    metadata = {
        "procurement_id": procurement_id,
        "procurement_url": procurement_url,
        "downloaded_at": utc_now_iso(),
        "sections": [s.get("name") for s in sections],
        "downloaded_files": downloaded_files,
        "total_files": len(downloaded_files),
        "total_size_bytes": total_size,
    }

    metadata_path = output_dir / "full_project_data.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "status": "success",
        "files_downloaded": len(downloaded_files),
        "total_size": total_size,
    }


def run(args: argparse.Namespace) -> int:
    """
    Run the document download pipeline step.

    Args:
        args: Parsed command-line arguments

    Returns:
        0 on success, non-zero on failure
    """
    started_at = utc_now_iso()
    print(f"Started at: {started_at}")

    # Load environment variables
    load_dotenv_file(Path.cwd() / DEFAULT_ENV_FILE, override=False)

    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Configure request pacing
    configure_request_pacer(
        min_interval_seconds=args.min_request_interval_seconds,
        jitter_seconds=args.request_jitter_seconds,
        pause_every_requests=args.pause_every_requests,
        pause_duration_seconds=args.pause_duration_seconds,
    )

    # Get procurement IDs to process
    procurement_ids = []
    if args.procurement_id:
        procurement_ids = [args.procurement_id]
    elif args.procurement_ids:
        procurement_ids = args.procurement_ids
    elif args.input_file:
        # Read from input file
        input_path = Path(args.input_file)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            return 1
        projects = read_projects_file(input_path)
        procurement_ids = [p["procurement_id"] for p in projects if "procurement_id" in p]

        if args.max_projects:
            procurement_ids = procurement_ids[: args.max_projects]

    if not procurement_ids:
        print("Error: No procurement IDs to process")
        return 1

    print(f"Processing {len(procurement_ids)} procurements...")

    # Download files
    success_count = 0
    failed_count = 0

    for proc_id in procurement_ids:
        print(f"\nProcurement {proc_id}:")
        proc_dir = output_dir / str(proc_id)

        result = download_procurement_files(
            procurement_id=proc_id,
            output_dir=proc_dir,
            include_historical=args.include_historical,
            keep_zips=args.keep_zips,
            timeout=args.request_timeout,
        )

        if result["status"] == "success":
            success_count += 1
            print(f"  OK Downloaded {result['files_downloaded']} files")
        else:
            failed_count += 1
            print(f"  FAILED: {result.get('error', result['status'])}")

    # Write summary
    finished_at = utc_now_iso()
    summary = {
        "started_at": started_at,
        "finished_at": finished_at,
        "pipeline_step": "download_documents",
        "summary": {
            "total_procurements": len(procurement_ids),
            "successful_downloads": success_count,
            "failed_downloads": failed_count,
        },
        "outputs": {
            "download_directory": str(output_dir),
        },
    }

    summary_path = output_dir / "download_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nFinished at: {finished_at}")
    print(json.dumps(summary["summary"], ensure_ascii=False))
    return 0


def main() -> int:
    """Standalone CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Pipeline Step 3: Download procurement documents from EIS"
    )

    # Input options
    parser.add_argument("--procurement-id", type=int, help="Single procurement ID to download")
    parser.add_argument("--procurement-ids", nargs="+", type=int, help="Multiple procurement IDs")
    parser.add_argument("--input-file", default=DEFAULT_INPUT_FILE, help="Input projects file")
    parser.add_argument("--max-projects", type=int, help="Limit number of procurements to process")

    # Output options
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--include-historical", action="store_true", help="Include historical documents")
    parser.add_argument("--keep-zips", action="store_true", help="Keep ZIP files after extraction")

    # Performance
    parser.add_argument("--request-timeout", type=int, default=DEFAULT_REQUEST_TIMEOUT, help="HTTP timeout (seconds)")
    parser.add_argument("--min-request-interval-seconds", type=float, default=1.0)
    parser.add_argument("--request-jitter-seconds", type=float, default=0.5)
    parser.add_argument("--pause-every-requests", type=int, default=10)
    parser.add_argument("--pause-duration-seconds", type=float, default=5.0)

    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
