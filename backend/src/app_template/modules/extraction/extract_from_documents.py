#!/usr/bin/env python3
"""Extract outcome data from local report documents using LLM."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .collector_classes import OutcomeLLMExtractor
from .collector_io import read_projects_file, write_jsonl
from .collector_outcomes import (
    convert_pdf_to_images_base64,
    extract_text_from_report_bytes,
    normalize_outcome_llm_result,
)
from .collector_storage import update_extraction_results
from .utils import load_dotenv_file


def extract_from_local_document_llm(
    project: dict[str, Any],
    document_path: str,
    llm_extractor: OutcomeLLMExtractor,
) -> dict[str, Any]:
    """
    Extract outcome data from a local document file using LLM only.

    Args:
        project: Project metadata
        document_path: Path to local document file
        llm_extractor: LLM extractor instance (required)

    Returns:
        Dict with extracted outcome data
    """
    file_path = Path(document_path)

    result = {
        "procurement_winner": None,
        "procurement_winner_registration_no": None,
        "procurement_winner_suggested_price_eur": None,
        "procurement_winner_source": None,
        "procurement_participants_count": 0,
        "procurement_participants": [],
        "procurement_outcome_description": None,
    }

    if not file_path.exists():
        result["procurement_outcome_description"] = f"file_not_found:{document_path}"
        return result

    try:
        report_bytes = file_path.read_bytes()
        file_name = file_path.name
    except Exception as exc:
        result["procurement_outcome_description"] = f"file_read_error:{exc}"
        return result

    report_text = extract_text_from_report_bytes(file_name, report_bytes)

    # STEP 1: Detect multi-lot using dedicated LLM agent (two-agent approach)
    multi_lot_detection = {}
    is_multi_lot_detected = False
    lot_count_detected = 1
    if report_text:
        try:
            multi_lot_detection = llm_extractor.detect_multi_lot(report_text)
            is_multi_lot_detected = multi_lot_detection.get("is_multi_lot", False)
            lot_count_detected = multi_lot_detection.get("lot_count") or 1
        except Exception:
            # If multi-lot detection fails, continue with single-lot assumption
            is_multi_lot_detected = False
            lot_count_detected = 1

    # STEP 2: Extract data with guidance from multi-lot detection
    used_vision = False
    try:
        llm_parsed: dict[str, Any] | None = None
        text_is_insufficient = not report_text or len(report_text.strip()) < 100

        if file_path.suffix.lower() == ".pdf" and text_is_insufficient:
            images_b64 = convert_pdf_to_images_base64(report_bytes, max_pages=10)
            if images_b64:
                llm_parsed = llm_extractor.extract_from_images(
                    project=project,
                    file_name=file_name,
                    images_base64=images_b64,
                )
                used_vision = True

        if llm_parsed is None:
            if not report_text:
                result["procurement_outcome_description"] = "report_text_empty"
                return result
            llm_parsed = llm_extractor.extract(
                project=project,
                file_name=file_name,
                report_text=report_text,
            )

        llm_normalized = normalize_outcome_llm_result(llm_parsed)

        # STEP 3: Merge multi-lot detection results with extraction results
        # The extraction agent may also detect multi-lot, but we trust the dedicated detector more
        participants = llm_normalized.get("participants") or []
        if is_multi_lot_detected:
            # Override with dedicated detector's result
            llm_normalized["is_multi_lot"] = True
            llm_normalized["lot_count"] = lot_count_detected
            llm_normalized["participants"] = participants  # Keep all participants from extractor

        # STEP 4: For multi-lot procurements, extract lot-level winners (Agent 3)
        lots_data = None
        if is_multi_lot_detected and lot_count_detected >= 2 and participants:
            try:
                lot_winners_result = llm_extractor.extract_lot_winners(
                    report_text=report_text,
                    lot_count=lot_count_detected,
                    participants=participants
                )
                lots_data = lot_winners_result.get("lots", [])

                # Enhance main participants list with lot-specific prices
                if lots_data:
                    # Create map of participant name -> lot-specific bids
                    participant_lot_bids = {}

                    for lot in lots_data:
                        lot_num = lot.get("lot_number")
                        lot_participants = lot.get("participants", [])

                        for lot_p in lot_participants:
                            p_name = lot_p.get("name", "")
                            p_price = lot_p.get("suggested_price_eur")
                            is_winner = lot_p.get("is_winner", False)

                            if p_name not in participant_lot_bids:
                                participant_lot_bids[p_name] = {
                                    "lot_bids": [],
                                    "won_lots": [],
                                    "all_prices": []
                                }

                            participant_lot_bids[p_name]["lot_bids"].append({
                                "lot_number": lot_num,
                                "price_eur": p_price,
                                "won": is_winner
                            })

                            if p_price is not None:
                                participant_lot_bids[p_name]["all_prices"].append(p_price)

                            if is_winner:
                                participant_lot_bids[p_name]["won_lots"].append(lot_num)

                    # Update main participants with lot-specific data
                    for p in participants:
                        p_name = p.get("name", "")
                        if p_name in participant_lot_bids:
                            lot_data = participant_lot_bids[p_name]

                            # Add lot-specific bid information
                            p["lot_bids"] = lot_data["lot_bids"]
                            p["won_lots"] = lot_data["won_lots"]

                            # Update suggested_prices_eur with ALL lot-specific prices
                            if lot_data["all_prices"]:
                                p["suggested_prices_eur"] = lot_data["all_prices"]
                                # Set suggested_price_eur to first/lowest for compatibility
                                p["suggested_price_eur"] = min(lot_data["all_prices"])

            except Exception as lot_err:
                # If lot winner extraction fails, continue with what we have
                print(f"  Warning: Lot winner extraction failed: {lot_err}")
                lots_data = None

        procurement_status = llm_normalized.get("procurement_status") or "unknown"
        winner_name = llm_normalized.get("winner_name")
        winner_reg_no = llm_normalized.get("winner_registration_no")
        winner_price = llm_normalized.get("winner_price_eur")
        confidence = llm_normalized.get("confidence") or "unknown"

    except Exception as exc:
        result["procurement_outcome_description"] = f"llm_extraction_error:{exc}"
        return result

    result["procurement_status_from_report"] = procurement_status
    result["procurement_participants"] = participants
    result["procurement_participants_count"] = len(participants)
    result["procurement_winner"] = winner_name
    result["procurement_winner_registration_no"] = winner_reg_no
    result["procurement_winner_suggested_price_eur"] = winner_price
    result["procurement_winner_source"] = "final_report_vision" if used_vision else "final_report_text"

    # Add enhanced extraction fields
    result["bid_deadline"] = llm_normalized.get("bid_deadline")
    result["decision_date"] = llm_normalized.get("decision_date")
    result["funding_source"] = llm_normalized.get("funding_source")
    result["eu_project_reference"] = llm_normalized.get("eu_project_reference")
    result["evaluation_method"] = llm_normalized.get("evaluation_method")
    result["contract_scope_type"] = llm_normalized.get("contract_scope_type")
    result["subcontractors"] = llm_normalized.get("subcontractors")
    result["is_multi_lot"] = llm_normalized.get("is_multi_lot", False)
    result["lot_count"] = llm_normalized.get("lot_count")

    # Add lot-level data if multi-lot
    result["lots"] = lots_data if lots_data else None

    method = "vision" if used_vision else "text"
    result["procurement_outcome_description"] = (
        f"file:{file_name}; status:{procurement_status}; llm_method:{method}; llm_confidence:{confidence}"
    )

    return result


def run(args):
    """Extract outcomes from local documents using LLM only."""
    print("STEP: Extracting Outcomes from Local Documents (LLM Only)")
    print("-" * 60)
    print(f"Input: {args.input_file}")
    print(f"Provider: {args.provider}")
    print(f"LLM Model: {args.llm_model}")
    if args.database:
        print(f"Database: {args.database} (direct SQLite write enabled)")
    print(f"Started at: {datetime.now(UTC).isoformat()}")

    load_dotenv_file(Path(".env"))

    import os
    if args.provider == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            print("ERROR: ANTHROPIC_API_KEY not set in .env")
            return 1
    else:
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set in .env")
            return 1

    llm_extractor = OutcomeLLMExtractor(
        model=args.llm_model,
        api_key=api_key,
        base_url=args.llm_base_url,
        vision_model=getattr(args, "llm_vision_model", None),
        provider=args.provider,
        request_delay_seconds=args.request_delay,
    )

    projects = read_projects_file(Path(args.input_file))
    print(f"Loaded {len(projects)} projects")

    extracted_count = 0
    failed_count = 0
    skipped_count = 0
    output_file = Path(args.output_file)
    save_interval = 10  # Save every 10 projects

    for idx, project in enumerate(projects, 1):
        procurement_id = project.get("procurement_id", "unknown")
        document_path = project.get("report_document_path")

        if not document_path:
            print(f"[{idx}/{len(projects)}] {procurement_id}: SKIPPED No document")
            skipped_count += 1
            continue

        print(f"[{idx}/{len(projects)}] {procurement_id}...", end=" ")

        result = extract_from_local_document_llm(
            project=project,
            document_path=document_path,
            llm_extractor=llm_extractor,
        )

        project.update(result)

        # Check if extraction was successful
        status = result.get("procurement_status_from_report")
        description = result.get("procurement_outcome_description", "")

        # Count as successful if we got a valid status OR if we have winner/participants
        if status or result.get("procurement_winner") or result.get("procurement_participants_count", 0) > 0:
            extracted_count += 1

            # Different messages for different scenarios
            if status in ("pre_consultation", "no_applications", "cancelled", "terminated"):
                print(f"OK status={status}")
            else:
                winner = result.get("procurement_winner", "N/A")
                count = result.get("procurement_participants_count", 0)
                print(f"OK {winner} ({count} participants)")
        else:
            # Only count as failed if we didn't get any valid extraction
            failed_count += 1
            error = description if description else "unknown"
            print(f"FAILED {error}")

        # Save incrementally every 10 projects
        if idx % save_interval == 0:
            write_jsonl(output_file, projects)
            print(f"  💾 Saved {idx}/{len(projects)} projects to {output_file}")

            # Also update database incrementally
            if args.database:
                db_path = Path(args.database)
                extracted_at = datetime.now(UTC).isoformat()
                rows_updated = update_extraction_results(db_path, projects, extracted_at=extracted_at)
                print(f"  💾 Updated {rows_updated} records in database")

    # Final save
    write_jsonl(output_file, projects)

    # Final database update if specified
    if args.database:
        db_path = Path(args.database)
        extracted_at = datetime.now(UTC).isoformat()
        print(f"\n📊 Final database write: {db_path}")
        rows_updated = update_extraction_results(db_path, projects, extracted_at=extracted_at)
        print(f"✅ Updated {rows_updated} records in database")

    print(f"\nFinished at: {datetime.now(UTC).isoformat()}")
    print("-" * 60)
    print(f"Extracted: {extracted_count}")
    print(f"Failed: {failed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract outcomes from local documents (LLM only)")
    parser.add_argument("--input-file", required=True, help="Input JSONL with document paths")
    parser.add_argument("--output-file", required=True, help="Output JSONL file")
    parser.add_argument(
        "--provider",
        default="anthropic",
        choices=["openai", "anthropic"],
        help="LLM provider: openai or anthropic (default: anthropic)"
    )
    parser.add_argument(
        "--llm-model",
        default="claude-3-haiku-20240307",
        help="LLM model to use for text extraction (default: claude-3-haiku-20240307)"
    )
    parser.add_argument(
        "--llm-vision-model",
        default=None,
        help="LLM model to use for vision/image extraction (defaults to same as --llm-model)"
    )
    parser.add_argument(
        "--llm-base-url",
        default="https://api.anthropic.com",
        help="LLM API base URL (default: https://api.anthropic.com)"
    )
    parser.add_argument(
        "--database",
        default=None,
        help="SQLite database path for direct write (optional, e.g., database/eis_procurement_records.sqlite)"
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.5,
        help="Delay in seconds between API requests to avoid rate limits (default: 0.5)"
    )

    args = parser.parse_args()
    run(args)
