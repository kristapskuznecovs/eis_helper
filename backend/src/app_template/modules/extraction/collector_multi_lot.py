"""Multi-lot procurement detection and handling."""

from typing import Dict, Any, List, Tuple
import re


def detect_multi_lot_from_text(text: str) -> Tuple[bool, int]:
    """
    Detect if document is multi-lot procurement by analyzing text.

    Returns:
        (is_multi_lot, lot_count)
    """
    # Pattern 1: Explicit lot markers like "1. daļa:", "2. daļa:", etc.
    # This is the MOST RELIABLE indicator of multi-lot procurement
    lot_pattern = r'(?:^|\n)\s*(\d+)\.\s*daļ[aā]'
    lot_matches = re.findall(lot_pattern, text, re.IGNORECASE | re.MULTILINE)

    if lot_matches:
        lot_numbers = [int(m) for m in lot_matches]
        max_lot = max(lot_numbers) if lot_numbers else 0
        if max_lot >= 2:
            return True, max_lot

    # Pattern 2: Text explicitly mentions "sadalīts X daļās" or similar
    split_pattern = r'sadalīts\s+(\d+)\s+daļās'
    split_match = re.search(split_pattern, text, re.IGNORECASE)
    if split_match:
        lot_count = int(split_match.group(1))
        if lot_count >= 2:
            return True, lot_count

    # Pattern 3: Alternative split mentions
    # "iepirkums sastāv no X daļām" or similar phrasings
    split_alt_patterns = [
        r'sastāv\s+no\s+(\d+)\s+daļām',
        r'(\d+)\s+daļās\s+sadalīts',
        r'iepirkums\s+ir\s+sadalīts\s+(\d+)\s+daļās',
    ]
    for pattern in split_alt_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            lot_count = int(match.group(1))
            if lot_count >= 2:
                return True, lot_count

    # NOTE: We intentionally do NOT use "Pretendenta nosaukums" count as indicator
    # because standard single-lot reports have this phrase appear multiple times
    # in different sections (prices, winner, disqualifications, etc.)

    return False, 1


def extract_lots_structure(text: str, lot_count: int) -> List[Dict[str, Any]]:
    """
    Extract basic structure of lots from text.

    Returns list of lot info: [{"lot_number": 1, "text_section": "..."}, ...]
    """
    lots = []

    # Try multiple patterns for lot splitting
    patterns = [
        r'(\d+)\.\s*daļ[aā]:',  # "1. daļa:"
        r'(\d+)\.\s*daļ[aā]\b',  # "1. daļa" (word boundary)
        r'(\d+)\.?\s*dal[aā]',    # Variations
    ]

    for pattern in patterns:
        sections = re.split(pattern, text, flags=re.IGNORECASE)
        if len(sections) > 2:  # Need at least pre-text + 1 lot
            # sections format: [pre-text, "1", lot1-text, "2", lot2-text, ...]
            for i in range(1, len(sections), 2):
                if i < len(sections):
                    try:
                        lot_num = int(sections[i])
                        lot_text = sections[i + 1] if i + 1 < len(sections) else ""
                        lots.append({
                            "lot_number": lot_num,
                            "text_section": lot_text[:5000]  # Limit text size
                        })
                    except (ValueError, IndexError):
                        continue
            if lots:
                break  # Found lots, stop trying patterns

    # Fallback: If we have lot_count but no structure, create placeholder lots
    # This allows us to at least mark it as multi-lot even if we can't assign participants
    if not lots and lot_count > 1:
        for i in range(1, lot_count + 1):
            lots.append({
                "lot_number": i,
                "text_section": ""  # No section identified
            })

    return lots


def enhance_participants_with_lot_info(
    participants: List[Dict[str, Any]],
    lots_structure: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Enhance participant list with lot information.

    This is a fallback to manually assign lot numbers based on text sections.
    """
    if not lots_structure:
        return participants

    # For each participant, try to find which lot section mentions them
    enhanced = []
    for p in participants:
        name = p.get("name", "")

        # Find which lot this participant belongs to
        found_lot = None
        for lot in lots_structure:
            if name in lot["text_section"]:
                found_lot = lot["lot_number"]
                break

        # Create enhanced participant entry
        p_copy = p.copy()
        if found_lot:
            p_copy["lot_number"] = found_lot

        enhanced.append(p_copy)

    return enhanced
