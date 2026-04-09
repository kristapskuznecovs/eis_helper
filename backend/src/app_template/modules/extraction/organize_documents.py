#!/usr/bin/env python3
"""Pipeline Step 4: organize downloaded project documents into LBN folders."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .collector_classes import configure_request_pacer
from .collector_io import read_projects_file
from .organizer_helpers import LBN_MARKS
from .utils import load_dotenv_file

DEFAULT_INPUT_FILE = "data/eis_building_docs_projects_2020_plus/projects_with_building_docs.jsonl"
DEFAULT_RAW_DATA_DIR = "data/downloads"
DEFAULT_OUTPUT_DIR = "data/organized_projects"
DEFAULT_CLASSIFICATION_MODE = "hybrid"
DEFAULT_ENV_FILE = ".env"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def organize_project(
    procurement_id: int,
    raw_dir: Path,
    output_dir: Path,
    classification_mode: str = "hybrid",
) -> Dict[str, Any]:
    project_dir = output_dir / str(procurement_id)
    project_dir.mkdir(parents=True, exist_ok=True)

    for mark in LBN_MARKS:
        (project_dir / mark).mkdir(exist_ok=True)
    (project_dir / "OTHER").mkdir(exist_ok=True)

    if not raw_dir.exists():
        return {"status": "error", "error": "Raw directory not found"}

    files = [
        f
        for f in raw_dir.rglob("*")
        if f.is_file() and f.suffix.lower() in [".pdf", ".doc", ".docx", ".txt"]
    ]

    classifications = {
        "folder_based": 0,
        "filename_based": 0,
        "llm_based": 0,
        "unclassified": 0,
    }

    for file_path in files:
        lbn_mark = "OTHER"
        classifications["unclassified"] += 1

        dest_dir = project_dir / lbn_mark
        dest_path = dest_dir / file_path.name

        counter = 1
        while dest_path.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1

        shutil.copy2(file_path, dest_path)

    metadata = {
        "procurement_id": procurement_id,
        "organized_at": utc_now_iso(),
        "classification_mode": classification_mode,
        "total_files": len(files),
        "classifications": classifications,
        "sections": {mark: len(list((project_dir / mark).iterdir())) for mark in [*LBN_MARKS, "OTHER"]},
    }

    metadata_path = project_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "status": "success",
        "total_files": len(files),
        "classifications": classifications,
    }


def run(args: argparse.Namespace) -> int:
    started_at = utc_now_iso()
    print(f"Started at: {started_at}")

    load_dotenv_file(Path.cwd() / DEFAULT_ENV_FILE, override=False)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_data_dir = Path(args.raw_data_dir)

    configure_request_pacer(
        min_interval_seconds=args.min_request_interval_seconds,
        jitter_seconds=args.request_jitter_seconds,
        pause_every_requests=0,
        pause_duration_seconds=0.0,
    )

    procurement_ids = []
    if args.procurement_ids:
        procurement_ids = args.procurement_ids
    elif args.input_file:
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

    print(f"Organizing {len(procurement_ids)} projects...")

    success_count = 0
    failed_count = 0
    total_files = 0
    total_classifications = {
        "folder_based": 0,
        "filename_based": 0,
        "llm_based": 0,
        "unclassified": 0,
    }

    for proc_id in procurement_ids:
        print(f"\nOrganizing procurement {proc_id}...")
        raw_dir = raw_data_dir / str(proc_id)

        result = organize_project(
            procurement_id=proc_id,
            raw_dir=raw_dir,
            output_dir=output_dir,
            classification_mode=args.classification_mode,
        )

        if result["status"] == "success":
            success_count += 1
            total_files += result["total_files"]
            for key, value in result["classifications"].items():
                total_classifications[key] += value
            print(f"  Organized {result['total_files']} files")
        else:
            failed_count += 1
            print(f"  Failed: {result.get('error', 'unknown')}")

    finished_at = utc_now_iso()
    summary = {
        "started_at": started_at,
        "finished_at": finished_at,
        "pipeline_step": "organize_documents",
        "classification_mode": args.classification_mode,
        "summary": {
            "total_projects": len(procurement_ids),
            "successful": success_count,
            "failed": failed_count,
            "total_files_organized": total_files,
            "classification_breakdown": total_classifications,
        },
        "outputs": {
            "organized_directory": str(output_dir),
        },
    }

    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nFinished at: {finished_at}")
    print(json.dumps(summary["summary"], ensure_ascii=False))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pipeline Step 4: Organize documents into LBN section folders"
    )

    parser.add_argument("--input-file", default=DEFAULT_INPUT_FILE, help="Input projects file")
    parser.add_argument("--procurement-ids", nargs="+", type=int, help="Specific procurement IDs")
    parser.add_argument("--max-projects", type=int, help="Limit number of projects to process")

    parser.add_argument("--raw-data-dir", default=DEFAULT_RAW_DATA_DIR, help="Downloaded files directory")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Organized output directory")

    parser.add_argument(
        "--classification-mode",
        choices=["heuristic", "hybrid", "llm"],
        default=DEFAULT_CLASSIFICATION_MODE,
        help="Classification mode",
    )

    parser.add_argument("--min-request-interval-seconds", type=float, default=0.5)
    parser.add_argument("--request-jitter-seconds", type=float, default=0.2)

    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
