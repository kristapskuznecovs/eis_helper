#!/usr/bin/env python3
"""Shared utility helpers used across EIS scripts."""

from __future__ import annotations

import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.request import Request


def slugify(value: str) -> str:
    """Create a filesystem-friendly lowercase slug."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "_", value)
    return value.strip("_") or "item"


def normalize_text(value: str) -> str:
    """Normalize text for robust matching (accent-insensitive lowercase)."""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def load_dotenv_file(path: Path, override: bool = False) -> int:
    """Load key=value pairs from .env into environment; return loaded key count."""
    if not path.is_file():
        return 0
    loaded = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if override or key not in os.environ:
            os.environ[key] = value
            loaded += 1
    return loaded


def render_prompt_template(template: str, replacements: Dict[str, str]) -> str:
    """Replace {{KEY}} placeholders in a prompt template."""
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    return rendered


def extract_js_array(html_text: str, variable_name: str) -> list[Dict[str, Any]]:
    """Extract and decode a JS array assigned to a variable in HTML."""
    marker = f"var {variable_name} = "
    marker_index = html_text.find(marker)
    if marker_index < 0:
        return []
    start = html_text.find("[", marker_index)
    if start < 0:
        return []
    depth = 0
    in_string = False
    escape_next = False
    end = -1
    for idx in range(start, len(html_text)):
        char = html_text[idx]
        if in_string:
            if escape_next:
                escape_next = False
            elif char == "\\":
                escape_next = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                end = idx
                break
    if end < 0:
        return []
    raw_array = html_text[start : end + 1]
    try:
        parsed = json.loads(raw_array)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def is_captcha_page(html_text: str) -> bool:
    """Detect anti-bot captcha pages returned by EIS."""
    checks = [
        "Pārbaude pret robotiem",
        "Captcha.aspx?eventCode=SUP.850",
        "uxVerifyCaptcha",
        "g-recaptcha",
    ]
    return any(marker in html_text for marker in checks)


def parse_csrf_token(page_html: str) -> Optional[str]:
    """Extract CSRF token from page HTML, if present."""
    match = re.search(
        r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"',
        page_html,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


def fetch_html(url: str, timeout_seconds: int = 45, cookie_header: Optional[str] = None) -> str:
    """
    Fetch HTML content from a URL with optional cookie header.
    Uses rate-limited requests via paced_urlopen to avoid overloading servers.

    Args:
        url: The URL to fetch
        timeout_seconds: Request timeout in seconds (default: 45)
        cookie_header: Optional Cookie header string

    Returns:
        HTML content as string
    """
    # Import here to avoid circular dependency
    from lib.collector_classes import paced_urlopen

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    if cookie_header:
        headers["Cookie"] = cookie_header
    req = Request(url, headers=headers)
    with paced_urlopen(req, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", "ignore")
