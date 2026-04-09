#!/usr/bin/env python3
"""Collector-related classes and request pacing machinery."""

from __future__ import annotations

import json
import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .utils import render_prompt_template

CLASSIFICATION_DOMAINS: set[str] = {
    "building",
    "infrastructure",
    "maintenance_service",
    "non_construction",
    "unknown",
}
CLASSIFICATION_SCOPE_TYPES: set[str] = {
    "design_only",
    "design_build",
    "build_only",
    "supervision_only",
    "service_only",
    "unknown",
}
CLASSIFICATION_WORK_TYPES: set[str] = {
    "new_build",
    "renovation",
    "repair",
    "maintenance",
    "cleaning",
    "unknown",
}
CLASSIFICATION_ASSET_SCALES: set[str] = {"large", "small", "unknown"}
CLASSIFICATION_FINAL_CATEGORIES: set[str] = {
    "building_design",
    "building_design_build",
    "building_supervision",
    "building_new_build",
    "building_renovation",
    "building_repair",
    "infrastructure_design",
    "infrastructure_build",
    "infrastructure_renovation_repair",
    "infrastructure_maintenance_service",
    "maintenance_cleaning_service",
    "non_construction",
    "unknown",
}

DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_TEMPERATURE = 0.0
DEFAULT_OPENAI_TOP_P = 1.0
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS = 300
DEFAULT_OPENAI_RESPONSE_FORMAT = "json_object"
DEFAULT_OUTCOME_LLM_BASE_URL = DEFAULT_OPENAI_BASE_URL
DEFAULT_OUTCOME_LLM_TEMPERATURE = 0.0
DEFAULT_OUTCOME_LLM_TOP_P = 1.0
DEFAULT_OUTCOME_LLM_MAX_OUTPUT_TOKENS = 2000
DEFAULT_OUTCOME_LLM_TIMEOUT_SECONDS = 90
DEFAULT_OUTCOME_LLM_RETRIES = 2
DEFAULT_OUTCOME_LLM_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_OUTCOME_LLM_TEXT_LIMIT = 20000


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_classification_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in CLASSIFICATION_FINAL_CATEGORIES:
        return text
    synonyms = {
        "building design": "building_design",
        "design_only": "building_design",
        "building design build": "building_design_build",
        "design_build": "building_design_build",
        "building supervision": "building_supervision",
        "supervision_only": "building_supervision",
        "building new": "building_new_build",
        "new_building": "building_new_build",
        "building renovation": "building_renovation",
        "renovation": "building_renovation",
        "building repair": "building_repair",
        "infrastructure design": "infrastructure_design",
        "infrastructure_new": "infrastructure_build",
        "infrastructure build": "infrastructure_build",
        "infrastructure renovation": "infrastructure_renovation_repair",
        "infrastructure_renovation": "infrastructure_renovation_repair",
        "infrastructure repair": "infrastructure_renovation_repair",
        "infrastructure maintenance": "infrastructure_maintenance_service",
        "maintenance cleaning": "maintenance_cleaning_service",
        "maintenance service": "maintenance_cleaning_service",
        "not_construction": "non_construction",
    }
    mapped = synonyms.get(text)
    return mapped if mapped in CLASSIFICATION_FINAL_CATEGORIES else "unknown"


def _normalize_enum(value: Any, allowed: set[str], synonyms: dict[str, str]) -> str:
    text = str(value or "").strip().lower()
    if text in allowed:
        return text
    mapped = synonyms.get(text)
    return mapped if mapped in allowed else "unknown"


def normalize_domain(value: Any) -> str:
    return _normalize_enum(
        value,
        CLASSIFICATION_DOMAINS,
        {
            "building_project": "building",
            "buildings": "building",
            "infra": "infrastructure",
            "infrastructure_build": "infrastructure",
            "service": "maintenance_service",
            "maintenance": "maintenance_service",
            "maintenance service": "maintenance_service",
            "non-construction": "non_construction",
            "not_construction": "non_construction",
        },
    )


def normalize_scope_type(value: Any) -> str:
    return _normalize_enum(
        value,
        CLASSIFICATION_SCOPE_TYPES,
        {
            "design": "design_only",
            "design only": "design_only",
            "design-build": "design_build",
            "design and build": "design_build",
            "build": "build_only",
            "construction": "build_only",
            "works": "build_only",
            "supervision": "supervision_only",
            "construction supervision": "supervision_only",
            "service": "service_only",
            "services": "service_only",
        },
    )


def normalize_work_type(value: Any) -> str:
    return _normalize_enum(
        value,
        CLASSIFICATION_WORK_TYPES,
        {
            "new": "new_build",
            "new_building": "new_build",
            "new construction": "new_build",
            "capital works": "new_build",
            "reconstruction": "renovation",
            "restoration": "renovation",
            "capital renovation": "renovation",
            "small repair": "repair",
            "minor repair": "repair",
            "current repair": "repair",
            "upkeep": "maintenance",
            "road maintenance": "maintenance",
            "winter maintenance": "maintenance",
            "snow cleaning": "cleaning",
            "road cleaning": "cleaning",
        },
    )


def normalize_asset_scale(value: Any) -> str:
    return _normalize_enum(
        value,
        CLASSIFICATION_ASSET_SCALES,
        {
            "big": "large",
            "major": "large",
            "minor": "small",
        },
    )


def _coerce_threshold_value(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value or "").strip()
        return float(text) if text else None
    except (TypeError, ValueError):
        return None


def resolve_scale_threshold(
    final_category: Any,
    scale_thresholds: dict[str, Any] | None = None,
) -> float:
    thresholds = scale_thresholds if isinstance(scale_thresholds, dict) else {}
    category_thresholds = thresholds.get("by_final_category")
    normalized_category = normalize_classification_label(final_category)
    if isinstance(category_thresholds, dict):
        category_value = _coerce_threshold_value(category_thresholds.get(normalized_category))
        if category_value is not None:
            return category_value
    default_threshold = _coerce_threshold_value(thresholds.get("default"))
    if default_threshold is not None:
        return default_threshold
    return 1_000_000.0


def derive_asset_scale(
    estimated_value_eur: Any,
    fallback_value: Any = None,
    *,
    final_category: Any = None,
    scale_thresholds: dict[str, Any] | None = None,
) -> str:
    if isinstance(estimated_value_eur, (int, float)):
        threshold = resolve_scale_threshold(final_category, scale_thresholds)
        return "large" if float(estimated_value_eur) >= threshold else "small"
    return normalize_asset_scale(fallback_value)


def derive_final_category(
    domain: Any,
    scope_type: Any,
    work_type: Any,
) -> str:
    normalized_domain = normalize_domain(domain)
    normalized_scope = normalize_scope_type(scope_type)
    normalized_work_type = normalize_work_type(work_type)

    if normalized_domain == "non_construction":
        return "non_construction"
    if normalized_domain == "maintenance_service":
        if normalized_work_type == "cleaning":
            return "maintenance_cleaning_service"
        return "infrastructure_maintenance_service"
    if normalized_scope == "supervision_only" and normalized_domain == "building":
        return "building_supervision"
    if normalized_scope == "service_only":
        if normalized_work_type == "cleaning":
            return "maintenance_cleaning_service"
        return "infrastructure_maintenance_service"
    if normalized_domain == "building":
        if normalized_scope == "design_only":
            return "building_design"
        if normalized_scope == "design_build":
            return "building_design_build"
        if normalized_work_type == "new_build":
            return "building_new_build"
        if normalized_work_type == "renovation":
            return "building_renovation"
        if normalized_work_type == "repair":
            return "building_repair"
        return "unknown"
    if normalized_domain == "infrastructure":
        if normalized_scope == "design_only":
            return "infrastructure_design"
        if normalized_scope in {"design_build", "build_only"} and normalized_work_type == "new_build":
            return "infrastructure_build"
        if normalized_work_type in {"renovation", "repair"}:
            return "infrastructure_renovation_repair"
        if normalized_work_type in {"maintenance", "cleaning"} or normalized_scope == "service_only":
            return "infrastructure_maintenance_service"
        return "infrastructure_build" if normalized_scope == "design_build" else "unknown"
    return "unknown"


def normalize_classification_result(
    parsed: dict[str, Any],
    estimated_value_eur: Any,
    scale_thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    domain = normalize_domain(parsed.get("domain"))
    scope_type = normalize_scope_type(parsed.get("scope_type"))
    work_type = normalize_work_type(parsed.get("work_type"))
    final_category = derive_final_category(domain, scope_type, work_type)
    asset_scale = derive_asset_scale(
        estimated_value_eur,
        parsed.get("asset_scale"),
        final_category=final_category,
        scale_thresholds=scale_thresholds,
    )
    reason = str(parsed.get("llm_reason") or parsed.get("reason") or "").strip() or "openai-no-reason"
    return {
        "classification_domain": domain,
        "classification_scope_type": scope_type,
        "classification_work_type": work_type,
        "classification_asset_scale": asset_scale,
        "classification_final_category": final_category,
        "classification_reason": reason,
    }


def extract_json_object_from_text(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        return {}
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    try:
        payload = json.loads(stripped)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", stripped, re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def extract_first_nonempty_line(text: str) -> str:
    for line in str(text or "").splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def validate_model_output_against_schema(
    parsed: dict[str, Any],
    output_schema: dict[str, Any] | None,
) -> None:
    if not output_schema:
        return
    required = output_schema.get("required")
    if isinstance(required, list):
        for key in required:
            if key not in parsed:
                raise RuntimeError(f"Model output missing required field: {key}")
    properties = output_schema.get("properties")
    if isinstance(properties, dict):
        for key, definition in properties.items():
            if not isinstance(definition, dict):
                continue
            enum_values = definition.get("enum")
            if isinstance(enum_values, list) and enum_values:
                if parsed.get(key) not in enum_values:
                    raise RuntimeError(
                        f"Model field '{key}' not in allowed enum: {parsed.get(key)}"
                    )


@dataclass
class RequestPacer:
    """Thread-safe request pacing for polite remote API access."""

    min_interval_seconds: float = 0.0
    jitter_seconds: float = 0.0
    pause_every_requests: int = 0
    pause_duration_seconds: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _next_allowed_monotonic: float = field(default=0.0, repr=False)
    _requests_seen: int = field(default=0, repr=False)

    def wait_for_slot(self) -> None:
        sleep_seconds = 0.0
        with self._lock:
            now = time.monotonic()
            if self._next_allowed_monotonic > now:
                sleep_seconds = self._next_allowed_monotonic - now

            if self.pause_every_requests > 0 and self._requests_seen > 0:
                if self._requests_seen % self.pause_every_requests == 0:
                    batch_pause = max(0.0, self.pause_duration_seconds)
                    if self.jitter_seconds > 0:
                        batch_pause += random.uniform(0.0, self.jitter_seconds)
                    sleep_seconds = max(sleep_seconds, batch_pause)

            if self.min_interval_seconds > 0:
                sleep_seconds += self.min_interval_seconds
                if self.jitter_seconds > 0:
                    sleep_seconds += random.uniform(0.0, self.jitter_seconds)

            self._next_allowed_monotonic = now + sleep_seconds
            self._requests_seen += 1

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


REQUEST_PACER: RequestPacer | None = None


def configure_request_pacer(
    min_interval_seconds: float,
    jitter_seconds: float,
    pause_every_requests: int,
    pause_duration_seconds: float,
) -> None:
    global REQUEST_PACER
    REQUEST_PACER = RequestPacer(
        min_interval_seconds=max(0.0, min_interval_seconds),
        jitter_seconds=max(0.0, jitter_seconds),
        pause_every_requests=max(0, pause_every_requests),
        pause_duration_seconds=max(0.0, pause_duration_seconds),
    )


def paced_urlopen(request: Request, timeout: int) -> Any:
    if REQUEST_PACER is not None:
        REQUEST_PACER.wait_for_slot()
    return urlopen(request, timeout=timeout)


def paced_opener_open(opener: Any, request: Request, timeout: int) -> Any:
    if REQUEST_PACER is not None:
        REQUEST_PACER.wait_for_slot()
    return opener.open(request, timeout=timeout)


@dataclass
class OpenAIClassifier:
    """Classify procurement rows using configured OpenAI-compatible API."""

    model: str
    api_key: str
    system_prompt_template: str
    user_prompt_template: str
    base_url: str = DEFAULT_OPENAI_BASE_URL
    timeout_seconds: int = 60
    retries: int = 3
    retry_backoff_seconds: float = 1.5
    temperature: float = DEFAULT_OPENAI_TEMPERATURE
    top_p: float = DEFAULT_OPENAI_TOP_P
    max_output_tokens: int = DEFAULT_OPENAI_MAX_OUTPUT_TOKENS
    response_format: str = DEFAULT_OPENAI_RESPONSE_FORMAT
    output_schema: dict[str, Any] | None = None
    tools_allowed: list[str] | None = None
    show_sources: bool = True
    history_enabled: bool = False
    history_path: Path | None = None
    history_store_raw_responses: bool = True
    history_max_entries: int | None = None
    scale_thresholds: dict[str, Any] | None = None
    _history_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_history(self, row: dict[str, Any]) -> None:
        if not self.history_enabled or self.history_path is None:
            return
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        with self._history_lock:
            with self.history_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            if self.history_max_entries and self.history_max_entries > 0:
                try:
                    lines = self.history_path.read_text(encoding="utf-8").splitlines()
                    if len(lines) > self.history_max_entries:
                        trimmed = lines[-self.history_max_entries :]
                        self.history_path.write_text("\n".join(trimmed) + "\n", encoding="utf-8")
                except OSError:
                    pass

    def classify(self, project: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is missing")
        system_prompt = render_prompt_template(
            self.system_prompt_template,
            {
                "ALLOWED_DOMAINS": ", ".join(sorted(CLASSIFICATION_DOMAINS - {"unknown"})),
                "ALLOWED_SCOPE_TYPES": ", ".join(sorted(CLASSIFICATION_SCOPE_TYPES - {"unknown"})),
                "ALLOWED_WORK_TYPES": ", ".join(sorted(CLASSIFICATION_WORK_TYPES - {"unknown"})),
                "ALLOWED_ASSET_SCALES": ", ".join(sorted(CLASSIFICATION_ASSET_SCALES - {"unknown"})),
            },
        )
        user_payload = {
            "procurement_name": project.get("procurement_name"),
            "cpv_main": project.get("cpv_main"),
            "procurement_identification_number": project.get("procurement_identification_number"),
            "estimated_value_eur": project.get("estimated_value_eur"),
            "purchaser_name": project.get("purchaser_name"),
            "procurement_status": project.get("procurement_status"),
        }
        user_prompt = render_prompt_template(
            self.user_prompt_template,
            {"PROJECT_JSON": json.dumps(user_payload, ensure_ascii=False, indent=2)},
        )
        body = {
            "model": self.model,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "max_tokens": self.max_output_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if self.response_format == "json_object":
            body["response_format"] = {"type": "json_object"}
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = Request(
                url=url,
                data=json.dumps(body).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                    "User-Agent": "eis-building-docs-scanner/1.0",
                },
                method="POST",
            )
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    payload = json.load(response)
                content = (
                    payload.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                if self.response_format == "label_string":
                    raw_label = extract_first_nonempty_line(str(content))
                    parsed = {
                        "domain": "unknown",
                        "scope_type": "unknown",
                        "work_type": "unknown",
                        "asset_scale": "unknown",
                        "llm_reason": f"label_string_output:{raw_label}",
                    }
                else:
                    parsed = extract_json_object_from_text(str(content))
                    validate_model_output_against_schema(parsed, self.output_schema)
                normalized = normalize_classification_result(
                    parsed,
                    estimated_value_eur=project.get("estimated_value_eur"),
                    scale_thresholds=self.scale_thresholds,
                )
                self.append_history(
                    {
                        "ts": utc_now_iso(),
                        "status": "ok",
                        "project": user_payload,
                        "request": {
                            "model": self.model,
                            "temperature": self.temperature,
                            "top_p": self.top_p,
                            "max_output_tokens": self.max_output_tokens,
                            "response_format": self.response_format,
                            "tools_allowed": self.tools_allowed or [],
                            "show_sources": self.show_sources,
                            "web_used": False,
                            "sources": [],
                        },
                        "response": {
                            "parsed": normalized,
                            "raw_parsed": parsed,
                            "raw_content": str(content) if self.history_store_raw_responses else None,
                        },
                    }
                )
                return normalized
            except Exception as exc:  # pragma: no cover
                last_error = exc
                self.append_history(
                    {
                        "ts": utc_now_iso(),
                        "status": "error",
                        "project": user_payload,
                        "request": {
                            "model": self.model,
                            "temperature": self.temperature,
                            "top_p": self.top_p,
                            "max_output_tokens": self.max_output_tokens,
                            "response_format": self.response_format,
                            "tools_allowed": self.tools_allowed or [],
                            "show_sources": self.show_sources,
                            "web_used": False,
                            "sources": [],
                        },
                        "error": str(exc),
                    }
                )
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(f"OpenAI classification failed after {self.retries} attempts: {last_error}")


@dataclass
class OutcomeLLMExtractor:
    """Extract procurement winner/participants from final report documents using Claude or OpenAI."""

    model: str
    api_key: str
    base_url: str = DEFAULT_OUTCOME_LLM_BASE_URL
    timeout_seconds: int = DEFAULT_OUTCOME_LLM_TIMEOUT_SECONDS
    retries: int = DEFAULT_OUTCOME_LLM_RETRIES
    retry_backoff_seconds: float = DEFAULT_OUTCOME_LLM_RETRY_BACKOFF_SECONDS
    temperature: float = DEFAULT_OUTCOME_LLM_TEMPERATURE
    top_p: float = DEFAULT_OUTCOME_LLM_TOP_P
    max_output_tokens: int = DEFAULT_OUTCOME_LLM_MAX_OUTPUT_TOKENS
    text_limit: int = DEFAULT_OUTCOME_LLM_TEXT_LIMIT
    vision_model: str | None = None  # Model to use for vision/image extraction (defaults to main model)
    config_dir: Path = field(default_factory=lambda: Path("config/agents/outcome_extractor"))
    provider: str = "openai"  # "openai" or "anthropic"
    request_delay_seconds: float = 0.0  # Delay between API calls to avoid rate limits

    def __post_init__(self):
        """Load prompts from configuration files."""
        self._system_prompt = self._load_prompt_file("system_prompt.txt")
        self._user_prompt_template = self._load_prompt_file("user_prompt.txt")
        self._vision_system_prompt = self._load_prompt_file("vision_system_prompt.txt")
        self._vision_user_prompt_template = self._load_prompt_file("vision_user_prompt.txt")

        # Load multi-lot detector prompts
        multi_lot_dir = Path("config/agents/multi_lot_detector")
        if multi_lot_dir.exists():
            self._multi_lot_system_prompt = (multi_lot_dir / "system_prompt.txt").read_text(encoding="utf-8").strip()
            self._multi_lot_user_prompt_template = (multi_lot_dir / "user_prompt.txt").read_text(encoding="utf-8").strip()
        else:
            self._multi_lot_system_prompt = None
            self._multi_lot_user_prompt_template = None

        # Load lot winner extractor prompts
        lot_winner_dir = Path("config/agents/lot_winner_extractor")
        if lot_winner_dir.exists():
            self._lot_winner_system_prompt = (lot_winner_dir / "system_prompt.txt").read_text(encoding="utf-8").strip()
            self._lot_winner_user_prompt_template = (lot_winner_dir / "user_prompt.txt").read_text(encoding="utf-8").strip()
        else:
            self._lot_winner_system_prompt = None
            self._lot_winner_user_prompt_template = None

    def _load_prompt_file(self, filename: str) -> str:
        """Load a prompt template from config directory."""
        prompt_path = self.config_dir / filename
        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
        return prompt_path.read_text(encoding="utf-8").strip()

    def detect_multi_lot(self, report_text: str) -> dict[str, Any]:
        """
        Detect if document contains multi-lot procurement using dedicated agent.

        Returns:
            Dict with keys: is_multi_lot, lot_count, lot_terminology, confidence, evidence
        """
        if not self._multi_lot_system_prompt or not self._multi_lot_user_prompt_template:
            # Fallback if multi-lot detector not configured
            return {
                "is_multi_lot": False,
                "lot_count": None,
                "lot_terminology": None,
                "confidence": "low",
                "evidence": "Multi-lot detector not configured"
            }

        text = str(report_text or "").strip()
        if len(text) > self.text_limit:
            text = text[: self.text_limit]

        # Render user prompt
        user_prompt = render_prompt_template(
            self._multi_lot_user_prompt_template,
            {"report_text": text}
        )

        if self.provider == "anthropic":
            body = {
                "model": self.model,
                "max_tokens": 1000,  # Multi-lot detection needs less tokens
                "temperature": 0.0,
                "system": self._multi_lot_system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
        else:
            body = {
                "model": self.model,
                "temperature": 0.0,
                "max_tokens": 1000,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": self._multi_lot_system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }

        return self._call_api(body)

    def extract_lot_winners(
        self,
        report_text: str,
        lot_count: int,
        participants: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Extract lot-level winner information for multi-lot procurements.

        Args:
            report_text: Full document text
            lot_count: Number of lots detected
            participants: List of all participants already extracted

        Returns:
            Dict with keys: lots (array), confidence, notes
        """
        if not self._lot_winner_system_prompt or not self._lot_winner_user_prompt_template:
            # Fallback if lot winner extractor not configured
            return {
                "lots": [],
                "confidence": "low",
                "notes": "Lot winner extractor not configured"
            }

        text = str(report_text or "").strip()
        if len(text) > self.text_limit:
            text = text[: self.text_limit]

        # Format participant list for prompt
        participant_names = [p.get('name', 'Unknown') for p in participants]
        participant_list = "\n".join([f"- {name}" for name in participant_names])

        # Render user prompt
        user_prompt = render_prompt_template(
            self._lot_winner_user_prompt_template,
            {
                "document_text": text,
                "lot_count": str(lot_count),
                "participant_list": participant_list
            }
        )

        if self.provider == "anthropic":
            body = {
                "model": self.model,
                "max_tokens": 2000,  # Need more tokens for lot breakdown
                "temperature": 0.0,
                "system": self._lot_winner_system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
        else:
            body = {
                "model": self.model,
                "temperature": 0.0,
                "max_tokens": 2000,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": self._lot_winner_system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }

        return self._call_api(body)

    def extract_from_images(
        self,
        project: dict[str, Any],
        file_name: str,
        images_base64: list[str],
    ) -> dict[str, Any]:
        """Extract from PDF images using Vision API (for scanned PDFs)."""
        if not self.api_key:
            raise RuntimeError("Outcome LLM API key is missing")

        # Render user prompt with variables
        user_text = render_prompt_template(
            self._vision_user_prompt_template,
            {
                "PROCUREMENT_ID": str(project.get("procurement_id", "unknown")),
                "FILE_NAME": str(file_name),
                "SCHEMA": json.dumps(self._get_schema(), ensure_ascii=False, indent=2),
            }
        )

        # Use vision_model if specified, otherwise fall back to main model
        model_to_use = self.vision_model or self.model

        if self.provider == "anthropic":
            # Claude format - images in content blocks
            content: list[dict[str, Any]] = [{"type": "text", "text": user_text}]
            for img_b64 in images_base64[:10]:
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": img_b64
                    }
                })

            body = {
                "model": model_to_use,
                "max_tokens": self.max_output_tokens,
                "temperature": self.temperature,
                "system": self._vision_system_prompt,
                "messages": [{"role": "user", "content": content}],
            }
        else:
            # OpenAI format
            content = [{"type": "text", "text": user_text}]
            for img_b64 in images_base64[:10]:
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{img_b64}"
                    }
                })

            body = {
                "model": model_to_use,
                "temperature": self.temperature,
                "max_tokens": self.max_output_tokens,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": self._vision_system_prompt},
                    {"role": "user", "content": content}
                ],
            }

        return self._call_api(body)

    def _get_schema(self) -> dict:
        """Get the extraction schema."""
        return {
            "procurement_status": "completed|cancelled|terminated|no_applications|pre_consultation|unknown",
            "winner_name": "string|null",
            "winner_registration_no": "string|null",
            "winner_price_eur": "number|null",
            "participants": [
                {
                    "name": "string",
                    "registration_no": "string|null",
                    "suggested_price_eur": "number|null",
                    "consortium_members": "array of strings|null",
                    "disqualified": "boolean (default false)",
                    "disqualification_reason": "string|null",
                }
            ],
            "bid_deadline": "string|null (date/time)",
            "decision_date": "string|null (date)",
            "funding_source": "string|null (ERAF|municipal|national|mixed)",
            "eu_project_reference": "string|null",
            "evaluation_method": "string|null (lowest_price|best_value)",
            "contract_scope_type": "string|null (design|construction_new|construction_renovation|construction_repair|supervision_construction|design_build|maintenance|construction_other|mixed|unknown)",
            "subcontractors": "array of strings|null",
            "confidence": "low|medium|high",
            "notes": "string",
        }

    def extract(
        self,
        project: dict[str, Any],
        file_name: str,
        report_text: str,
    ) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("Outcome LLM API key is missing")

        text = str(report_text or "").strip()
        if len(text) > self.text_limit:
            text = text[: self.text_limit]

        # Render user prompt with variables
        user_prompt = render_prompt_template(
            self._user_prompt_template,
            {
                "PROCUREMENT_ID": str(project.get("procurement_id", "unknown")),
                "FILE_NAME": str(file_name),
                "DOCUMENT_TEXT": text,
                "SCHEMA": json.dumps(self._get_schema(), ensure_ascii=False, indent=2),
            }
        )

        if self.provider == "anthropic":
            # Claude format - system prompt separate, no response_format
            body = {
                "model": self.model,
                "max_tokens": self.max_output_tokens,
                "temperature": self.temperature,
                "system": self._system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
        else:
            # OpenAI format
            body = {
                "model": self.model,
                "temperature": self.temperature,
                "top_p": self.top_p,
                "max_tokens": self.max_output_tokens,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            }

        return self._call_api(body)

    def _call_api(self, body: dict[str, Any]) -> dict[str, Any]:
        """Make API call with retry logic and rate limit handling."""
        if self.provider == "anthropic":
            url = f"{self.base_url.rstrip('/')}/v1/messages"
            headers = {
                "Content-Type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "User-Agent": "eis-building-docs-scanner/1.0",
            }
        else:
            url = f"{self.base_url.rstrip('/')}/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "eis-building-docs-scanner/1.0",
            }

        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            # Add delay before request to avoid rate limits
            if self.request_delay_seconds > 0 and attempt == 1:
                time.sleep(self.request_delay_seconds)

            request = Request(
                url=url,
                data=json.dumps(body).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    payload = json.load(response)

                if self.provider == "anthropic":
                    # Claude response format
                    content_blocks = payload.get("content", [])
                    if not content_blocks:
                        raise RuntimeError("Claude returned empty content")
                    content = content_blocks[0].get("text", "")
                else:
                    # OpenAI response format
                    content = (
                        payload.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    )

                parsed = extract_json_object_from_text(str(content))
                if not isinstance(parsed, dict) or not parsed:
                    raise RuntimeError("Outcome LLM returned empty JSON")
                return parsed
            except HTTPError as exc:
                last_error = exc
                # Handle rate limit (429) with longer backoff
                if exc.code == 429:
                    if attempt >= self.retries:
                        break
                    backoff = self.retry_backoff_seconds * (2 ** attempt)  # Exponential backoff for 429
                    time.sleep(backoff)
                elif attempt >= self.retries:
                    break
                else:
                    time.sleep(self.retry_backoff_seconds * attempt)
            except Exception as exc:  # pragma: no cover
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(f"Outcome LLM extraction failed after {self.retries} attempts: {last_error}")


@dataclass
class CKANClient:
    """Minimal CKAN API client with retry/backoff behavior."""

    action_url: str
    timeout_seconds: int = 90
    retries: int = 4
    retry_backoff_seconds: float = 1.5

    def call(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.action_url.rstrip('/')}/{action}?{urlencode(params, doseq=True)}"
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            req = Request(url, headers={"Accept": "application/json", "User-Agent": "eis-building-docs-scanner/1.0"})
            try:
                with paced_urlopen(req, timeout=self.timeout_seconds) as response:
                    payload = json.load(response)
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)
                continue
            if payload.get("success") is not True:
                raise RuntimeError(f"CKAN action '{action}' failed: {payload.get('error') or payload}")
            return payload
        raise RuntimeError(
            f"Request to CKAN action '{action}' failed after {self.retries} attempts: {last_error}"
        )

    def package_show(self, package_id: str) -> dict[str, Any]:
        return self.call("package_show", {"id": package_id})

    def datastore_search(self, resource_id: str, limit: int, offset: int, include_total: bool) -> dict[str, Any]:
        return self.call(
            "datastore_search",
            {
                "resource_id": resource_id,
                "limit": limit,
                "offset": offset,
                "include_total": str(include_total).lower(),
            },
        )
