"""Company name normalization and identity utilities.

Used by pipelines/build_company_index.py and dashboard/server.py.
Does not import from pipeline or dashboard modules.
"""

from __future__ import annotations

import re
import unicodedata
from typing import List, Optional, Tuple

from .collector_outcomes import (
    is_plausible_company_name,
    trim_company_name_noise,
)
from .utils import normalize_text

FUZZY_MATCH_THRESHOLD = 88

# Legal form tokens to strip, in accent-folded uppercase (for normalize_party_name)
_LEGAL_FORMS_UPPER = [
    "SABIEDRIBA AR IEROBEZOTU ATBILDIBU",  # full Latvian form, accent-folded
    "VALSTS AKCIJU SABIEDRIBA",
    "PIEGADATAJU APVIENIBA",
    "PERSONU APVIENIBA",
    "PAŠVALDIBAS",
]
_LEGAL_FORM_TOKENS_UPPER = [
    r"\bSIA\b", r"\bAS\b", r"\bPS\b", r"\bIK\b", r"\bZS\b",
    r"\bPSIA\b", r"\bVAS\b", r"\bAB\b", r"\bPA\b",
]


def _accent_fold(text: str) -> str:
    """NFKD accent-fold: ā→a, ē→e, etc."""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def normalize_reg_no(raw: Optional[str]) -> Optional[str]:
    """Return a clean 11-digit Latvian registration number, or None."""
    if not raw:
        return None
    cleaned = raw.strip()
    # Strip LV prefix (e.g. LV40003356530)
    if cleaned.upper().startswith("LV"):
        cleaned = cleaned[2:]
    cleaned = re.sub(r"\s", "", cleaned)
    if re.fullmatch(r"\d{11}", cleaned):
        return cleaned
    return None


def normalize_for_matching(name: str) -> str:
    """Aggressive normalization for fuzzy comparison.

    Returns a bare lowercase alphanumeric core string with:
    - accent-folding
    - all legal form words removed (including full Latvian)
    - all non-alphanumeric characters removed
    - whitespace collapsed
    """
    name = trim_company_name_noise(name)
    # accent-fold + lowercase via normalize_text (NFKD)
    text = normalize_text(name)

    # Remove full-form legal names first (before token removal)
    full_forms = [
        r"sabiedriba ar ierobezotu atbildibu",
        r"valsts akciju sabiedriba",
        r"piegadataju apvieniba",
        r"personu apvieniba",
        r"pasvaldibu apvieniba",
    ]
    for form in full_forms:
        text = re.sub(form, " ", text, flags=re.IGNORECASE)

    # Remove short legal form tokens
    text = re.sub(r"\b(sia|as|ps|ik|zs|ab|vas|psia|pa)\b", " ", text, flags=re.IGNORECASE)

    # Remove all non-alphanumeric characters (quotes, dashes, dots, etc.)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_canonical_name(names: List[str]) -> str:
    """Pick the best display name from a list of aliases.

    Prefers the shortest name that passes is_plausible_company_name
    and has the fewest quote/parenthesis characters.
    Falls back to the first element.
    """
    if not names:
        return ""

    def score(n: str) -> Tuple[int, int, int]:
        noise = n.count('"') + n.count("(") + n.count(")") + n.count("\u201e") + n.count("\u201c")
        return (0 if is_plausible_company_name(n) else 1, noise, len(n))

    return min(names, key=score)


def fuzzy_score(a: str, b: str) -> float:
    """Return token_sort_ratio similarity (0-100) between two strings."""
    try:
        from rapidfuzz import fuzz
        return fuzz.token_sort_ratio(a, b)
    except ImportError:
        import difflib
        # Approximate token_sort_ratio: sort tokens, then use SequenceMatcher
        a_sorted = " ".join(sorted(a.split()))
        b_sorted = " ".join(sorted(b.split()))
        ratio = difflib.SequenceMatcher(None, a_sorted, b_sorted).ratio()
        return ratio * 100


def normalize_party_name(value: Optional[str]) -> str:
    """Normalize a company name for grouping and display.

    Strips legal form indicators, quotes, and accent-folds for consistent
    aggregation. Used in the dashboard when no company_id is available.
    """
    if not value:
        return "Nav norādīts"

    # Accent-fold, then uppercase for uniform token matching
    text = _accent_fold(value.strip()).upper()

    # Normalize smart quotes to standard double-quote
    for q in ["„", "\u201c", "\u201d", "\u2018", "\u2019"]:
        text = text.replace(q, '"')

    # Remove full legal form phrases first
    for phrase in _LEGAL_FORMS_UPPER:
        text = text.replace(phrase, " ")

    # Remove short legal form tokens as whole words
    for pattern in _LEGAL_FORM_TOKENS_UPPER:
        text = re.sub(pattern, " ", text)

    # Strip all remaining quote characters
    text = text.replace('"', "")

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text or "Nav norādīts"
