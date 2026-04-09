#!/usr/bin/env python3
"""Pipeline Step 1: fetch metadata, classify procurement records, and detect building docs."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .collector_classes import CKANClient, OpenAIClassifier, configure_request_pacer
from .collector_config import (
    load_agent_config,
    load_pipeline_config,
    load_text_file,
    resolve_script_relative_path,
)
from .collector_io import write_jsonl
from .collector_pipeline import (
    classify_procurement_records,
    collect_procurement_records,
    scan_project_for_docs,
)
from .collector_storage import (
    procurement_record_storage_key,
    initialize_procurement_records,
    load_procurement_records_by_keys,
    upsert_procurement_records,
)
from .utils import load_dotenv_file

# Constants
DEFAULT_ACTION_URL = "https://data.gov.lv/dati/api/3/action"
DEFAULT_PACKAGE_ID = "izsludinato-iepirkumu-datu-grupa"
DEFAULT_FROM_YEAR = 2020
DEFAULT_CPV_PREFIXES = ["45"]
DEFAULT_DOC_TITLE_REGEX = [
    r"projekta[_\s-]*dokument",
    r"projektu[_\s-]*dokument",
    r"buvprojekt",
    r"buvniecibas[_\s-]*ieceres[_\s-]*dokument",
]
DEFAULT_WORKERS = 12
DEFAULT_BATCH_SIZE = 5000
DEFAULT_CLASSIFICATION_MODE = "openai"
DEFAULT_CLASSIFICATION_WORKERS = 4
DEFAULT_OUTPUT_DIR = "data/eis_design_docs_projects_2020_plus"
DEFAULT_PIPELINE_CONFIG_FILE = "pipeline_config.json"
DEFAULT_DATABASE_PATH = "database/eis_procurement_records.sqlite"
DEFAULT_ENV_FILE = ".env"
DEFAULT_AGENT_CONFIG_FILE = "config/agents/classification/config.json"
CLASSIFICATION_PERSIST_BATCH_SIZE = 10


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_openai_classifier(
    args: argparse.Namespace,
    agent_config: Dict[str, Any],
    effective_classification_mode: str,
) -> OpenAIClassifier | None:
    if effective_classification_mode != "openai":
        return None

    openai_api_key = os.getenv(args.openai_api_key_env, "")
    if not openai_api_key:
        return None

    agent = agent_config.get("classification_agent", {}) if isinstance(agent_config, dict) else {}
    prompts = agent.get("prompts", {}) if isinstance(agent, dict) else {}
    generation = agent.get("generation", {}) if isinstance(agent, dict) else {}
    api_cfg = agent.get("api", {}) if isinstance(agent, dict) else {}
    output_cfg = agent.get("output", {}) if isinstance(agent, dict) else {}
    history_cfg = agent.get("history", {}) if isinstance(agent, dict) else {}
    tools_cfg = agent.get("tools", {}) if isinstance(agent, dict) else {}

    system_prompt_path = resolve_script_relative_path(
        str(prompts.get("system_file") or args.openai_system_prompt_file)
    )
    user_prompt_path = resolve_script_relative_path(
        str(prompts.get("user_file") or args.openai_user_prompt_file)
    )

    system_prompt_template = load_text_file(system_prompt_path, "OpenAI system prompt")
    user_prompt_template = load_text_file(user_prompt_path, "OpenAI user prompt")

    history_path_value = history_cfg.get("path") if isinstance(history_cfg, dict) else None
    history_path = resolve_script_relative_path(str(history_path_value)) if history_path_value else None
    scale_thresholds = agent_config.get("scale_thresholds") if isinstance(agent_config, dict) else None

    return OpenAIClassifier(
        model=str(agent.get("model") or args.openai_model),
        api_key=openai_api_key,
        system_prompt_template=system_prompt_template,
        user_prompt_template=user_prompt_template,
        base_url=str(api_cfg.get("base_url") or args.openai_base_url),
        timeout_seconds=int(api_cfg.get("timeout_seconds") or 60),
        retries=int(api_cfg.get("retries") or 3),
        retry_backoff_seconds=float(api_cfg.get("retry_backoff_seconds") or 1.5),
        temperature=float(generation.get("temperature") or args.openai_temperature),
        top_p=float(generation.get("top_p") or args.openai_top_p),
        max_output_tokens=int(generation.get("max_output_tokens") or args.openai_max_output_tokens),
        response_format=str(output_cfg.get("format") or args.openai_response_format),
        output_schema=output_cfg.get("schema") if isinstance(output_cfg, dict) else None,
        tools_allowed=tools_cfg.get("allowed") if isinstance(tools_cfg, dict) else None,
        show_sources=bool(tools_cfg.get("show_sources", True)) if isinstance(tools_cfg, dict) else True,
        history_enabled=bool(history_cfg.get("enabled", False)) if isinstance(history_cfg, dict) else False,
        history_path=history_path,
        history_store_raw_responses=bool(history_cfg.get("store_raw_responses", True))
        if isinstance(history_cfg, dict)
        else True,
        history_max_entries=int(history_cfg.get("max_entries"))
        if isinstance(history_cfg, dict) and isinstance(history_cfg.get("max_entries"), int)
        else None,
        scale_thresholds=scale_thresholds if isinstance(scale_thresholds, dict) else None,
    )


def run(args: argparse.Namespace) -> int:
    started_at = utc_now_iso()
    print(f"Started at: {started_at}")

    load_dotenv_file(Path.cwd() / DEFAULT_ENV_FILE, override=False)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    agent_config: Dict[str, Any] = {}
    pipeline_config: Dict[str, Any] = {}
    effective_classification_mode = args.classification_mode
    effective_classification_workers = max(1, int(args.classification_workers))
    effective_show_in_progress_messages = True
    effective_classification_log_every = 200
    effective_scan_log_every = 250

    if not args.no_agent_config:
        agent_config_path = resolve_script_relative_path(args.agent_config_file)
        loaded_agent_config, config_loaded = load_agent_config(agent_config_path)
        if config_loaded:
            agent_config = loaded_agent_config
            classification_agent = loaded_agent_config.get("classification_agent", {})
            progress_cfg = classification_agent.get("progress", {})
            if args.classification_mode == DEFAULT_CLASSIFICATION_MODE:
                mode_cfg = str(classification_agent.get("mode") or "").strip()
                if mode_cfg == "openai":
                    effective_classification_mode = mode_cfg
            if args.classification_workers == DEFAULT_CLASSIFICATION_WORKERS:
                workers_cfg = classification_agent.get("workers")
                if isinstance(workers_cfg, int) and workers_cfg > 0:
                    effective_classification_workers = workers_cfg
            if isinstance(progress_cfg, dict):
                effective_show_in_progress_messages = bool(
                    progress_cfg.get("show_in_progress_messages", True)
                )
                effective_classification_log_every = int(
                    progress_cfg.get("classification_log_every_n", 200)
                )
                effective_scan_log_every = int(progress_cfg.get("scan_log_every_n", 250))

    pipeline_config_path = resolve_script_relative_path(args.pipeline_config_file)
    loaded_pipeline_config, pipeline_config_loaded = load_pipeline_config(pipeline_config_path)
    if pipeline_config_loaded:
        pipeline_config = loaded_pipeline_config
        classification_cfg = pipeline_config.get("classification", {})
        if isinstance(classification_cfg, dict):
            scale_thresholds = classification_cfg.get("asset_scale_thresholds_eur")
            if isinstance(scale_thresholds, dict):
                agent_config = dict(agent_config)
                agent_config["scale_thresholds"] = scale_thresholds

    cpv_prefixes = args.cpv_prefixes if args.cpv_prefixes else DEFAULT_CPV_PREFIXES
    title_patterns = args.doc_title_regex if args.doc_title_regex else DEFAULT_DOC_TITLE_REGEX
    compiled_title_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in title_patterns]

    configure_request_pacer(
        min_interval_seconds=args.min_request_interval_seconds,
        jitter_seconds=args.request_jitter_seconds,
        pause_every_requests=args.pause_every_requests,
        pause_duration_seconds=args.pause_duration_seconds,
    )

    print(f"Collecting procurement records from CKAN (from year {args.from_year})...")
    client = CKANClient(action_url=args.action_url)
    procurement_records, procurement_record_counts_by_year, prefilter_counts_by_year, prefilter_total = collect_procurement_records(
        client=client,
        package_id=args.package_id,
        from_year=args.from_year,
        to_year=args.to_year,
        cpv_prefixes=cpv_prefixes,
        batch_size=args.batch_size,
        min_estimated_value=args.min_estimated_value,
        max_projects=None,
    )

    database_path = resolve_script_relative_path(args.database_path)
    stored_record_keys = [procurement_record_storage_key(record) for record in procurement_records]
    initial_rows_persisted = initialize_procurement_records(
        database_path,
        procurement_records,
        ingested_at=started_at,
    )
    print(
        f"Stored {initial_rows_persisted}/{len(procurement_records)} raw procurement records in SQLite: {database_path}"
    )

    openai_classifier = _build_openai_classifier(args, agent_config, effective_classification_mode)
    if openai_classifier is None:
        raise RuntimeError(f"{args.openai_api_key_env} not set. OpenAI classification is required.")

    persist_buffer: List[Dict[str, Any]] = []
    persist_lock = threading.Lock()
    classification_rows_persisted = 0

    def flush_persist_buffer(force: bool = False) -> None:
        nonlocal classification_rows_persisted
        with persist_lock:
            if not persist_buffer:
                return
            if not force and len(persist_buffer) < CLASSIFICATION_PERSIST_BATCH_SIZE:
                return
            batch = [dict(row) for row in persist_buffer]
            persist_buffer.clear()
        classified_at = utc_now_iso()
        written = upsert_procurement_records(
            database_path,
            batch,
            ingested_at=started_at,
            classified_at=classified_at,
            classifier_model=openai_classifier.model,
        )
        classification_rows_persisted += written
        print(
            "Updated classification for "
            f"{classification_rows_persisted}/{len(procurement_records)} procurement records in SQLite: {database_path}"
        )

    def on_project_classified(project: Dict[str, Any], processed: int, total: int) -> None:
        del total
        with persist_lock:
            persist_buffer.append(dict(project))
            should_flush = len(persist_buffer) >= CLASSIFICATION_PERSIST_BATCH_SIZE
        if should_flush:
            flush_persist_buffer(force=True)
        elif processed % CLASSIFICATION_PERSIST_BATCH_SIZE == 0:
            flush_persist_buffer(force=True)

    print(f"Classifying {len(procurement_records)} procurement records (mode: {effective_classification_mode})...")
    class_counts = classify_procurement_records(
        procurement_records=procurement_records,
        classification_mode=effective_classification_mode,
        classification_workers=effective_classification_workers,
        openai_classifier=openai_classifier,
        openai_max_projects=args.openai_max_projects,
        show_in_progress_messages=effective_show_in_progress_messages,
        log_every_n=effective_classification_log_every,
        on_project_classified=on_project_classified,
    )
    flush_persist_buffer(force=True)
    print(f"Classification results: {json.dumps(class_counts, ensure_ascii=False)}")

    print("Scanning procurement pages for design documentation titles...")
    matched: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    captcha_failures = 0
    scanned_count = 0
    lock = threading.Lock()

    def worker(procurement_record: Dict[str, Any]) -> None:
        nonlocal scanned_count, captcha_failures
        try:
            result = scan_project_for_docs(
                project=procurement_record,
                compiled_title_patterns=compiled_title_patterns,
                include_historical=args.include_historical,
                request_timeout_seconds=45,
                cookie_header=None,
                extract_final_report_outcomes=False,
                outcome_extraction_mode="hybrid",
                outcome_llm_extractor=None,
            )
        except Exception as exc:
            with lock:
                scanned_count += 1
                failed_row = dict(procurement_record)
                failed_row["scan_error"] = str(exc)
                failed.append(failed_row)
                if "captcha" in str(exc).lower():
                    captcha_failures += 1
            return

        with lock:
            scanned_count += 1
            if int(result.get("matched_document_count") or 0) > 0:
                matched.append(result)
            if (
                effective_show_in_progress_messages
                and effective_scan_log_every > 0
                and scanned_count % effective_scan_log_every == 0
            ):
                print(
                    f"Scanned {scanned_count}/{len(procurement_records)} "
                    f"(matched {len(matched)}, failed {len(failed)})"
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        list(executor.map(worker, procurement_records))

    print("Writing outputs...")
    all_projects_path = output_dir / "all_construction_projects.jsonl"
    matched_path = output_dir / "projects_with_design_docs.jsonl"
    failed_path = output_dir / "failed_projects.jsonl"

    exported_procurement_records = load_procurement_records_by_keys(
        database_path,
        stored_record_keys,
    )
    write_jsonl(all_projects_path, exported_procurement_records)
    matched.sort(key=lambda item: (item.get("year") or 0, str(item.get("procurement_id") or "")))
    failed.sort(key=lambda item: (item.get("year") or 0, str(item.get("procurement_id") or "")))
    matched_count = write_jsonl(matched_path, matched)
    failed_count = write_jsonl(failed_path, failed)

    matched_counts_by_year: Dict[str, int] = {}
    for row in matched:
        key = str(row.get("year") or "unknown")
        matched_counts_by_year[key] = matched_counts_by_year.get(key, 0) + 1

    finished_at = utc_now_iso()
    manifest = {
        "started_at": started_at,
        "finished_at": finished_at,
        "pipeline_step": "fetch_metadata",
        "action_url": args.action_url,
        "package_id": args.package_id,
        "from_year": args.from_year,
        "to_year": args.to_year,
        "cpv_prefixes": cpv_prefixes,
        "min_estimated_value": args.min_estimated_value,
        "batch_size": args.batch_size,
        "classification_mode": effective_classification_mode,
        "classification_workers": effective_classification_workers,
        "doc_title_regex": title_patterns,
        "include_historical": args.include_historical,
        "workers": args.workers,
        "summary": {
            "procurement_records_before_value_filter": prefilter_total,
            "procurement_records": len(procurement_records),
            "classification_counts": class_counts,
            "sqlite_raw_rows_persisted": initial_rows_persisted,
            "sqlite_classification_updates_persisted": classification_rows_persisted,
            "matched_projects": len(matched),
            "failed_projects": len(failed),
            "captcha_failures": captcha_failures,
            "procurement_record_counts_by_year_before_value_filter": prefilter_counts_by_year,
            "procurement_record_counts_by_year": procurement_record_counts_by_year,
            "matched_counts_by_year": dict(sorted(matched_counts_by_year.items())),
        },
        "outputs": {
            "database": str(database_path),
            "all_projects": str(all_projects_path),
            "projects_with_design_docs": str(matched_path),
            "failed_projects": str(failed_path),
            "rows_written": {
                "database_raw_records": initial_rows_persisted,
                "database_classification_updates": classification_rows_persisted,
                "all_projects": len(exported_procurement_records),
                "with_design_docs": matched_count,
                "failed": failed_count,
            },
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Finished at: {finished_at}")
    print(f"Output dir: {output_dir}")
    print(
        json.dumps(
            {
                "procurement_records": len(procurement_records),
                "matched_projects": len(matched),
                "failed_projects": len(failed),
            },
            ensure_ascii=False,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pipeline Step 1: Fetch construction procurement metadata from CKAN API"
    )

    parser.add_argument("--action-url", default=DEFAULT_ACTION_URL, help="CKAN API action URL")
    parser.add_argument("--package-id", default=DEFAULT_PACKAGE_ID, help="CKAN package ID")
    parser.add_argument("--from-year", type=int, default=DEFAULT_FROM_YEAR, help="Start year")
    parser.add_argument("--to-year", type=int, default=None, help="End year (inclusive)")
    parser.add_argument(
        "--cpv-prefix",
        action="append",
        dest="cpv_prefixes",
        help="CPV main code prefix (repeatable, default: 45)",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--min-estimated-value",
        type=float,
        default=None,
        help="Filter projects with estimated value >= amount (EUR)",
    )

    parser.add_argument(
        "--classification-mode",
        choices=["openai"],
        default=DEFAULT_CLASSIFICATION_MODE,
        help="Classification mode: openai (all procurement records classified with LLM)",
    )
    parser.add_argument(
        "--classification-workers",
        type=int,
        default=DEFAULT_CLASSIFICATION_WORKERS,
        help="Worker threads for OpenAI classification",
    )
    parser.add_argument("--openai-model", default="gpt-4.1-mini", help="OpenAI model name")
    parser.add_argument("--openai-api-key-env", default="OPENAI_API_KEY", help="OpenAI API key env var")
    parser.add_argument("--openai-base-url", default="https://api.openai.com/v1")
    parser.add_argument("--openai-temperature", type=float, default=0.0)
    parser.add_argument("--openai-top-p", type=float, default=1.0)
    parser.add_argument("--openai-max-output-tokens", type=int, default=300)
    parser.add_argument("--openai-response-format", default="json_object")
    parser.add_argument("--openai-max-projects", type=int, default=None, help="Limit OpenAI API calls")
    parser.add_argument(
        "--openai-system-prompt-file",
        default="config/agents/classification/system_prompt.txt",
    )
    parser.add_argument(
        "--openai-user-prompt-file",
        default="config/agents/classification/user_prompt.txt",
    )

    parser.add_argument(
        "--doc-title-regex",
        action="append",
        dest="doc_title_regex",
        help="Regex for building doc detection (repeatable)",
    )
    parser.add_argument("--include-historical", action="store_true", help="Include historical documents")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Worker threads for scanning")

    parser.add_argument("--min-request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--request-jitter-seconds", type=float, default=0.1)
    parser.add_argument("--pause-every-requests", type=int, default=100)
    parser.add_argument("--pause-duration-seconds", type=float, default=2.0)

    parser.add_argument("--agent-config-file", default=DEFAULT_AGENT_CONFIG_FILE)
    parser.add_argument("--pipeline-config-file", default=DEFAULT_PIPELINE_CONFIG_FILE)
    parser.add_argument("--no-agent-config", action="store_true")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--database-path", default=DEFAULT_DATABASE_PATH, help="SQLite database path")

    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
