#!/usr/bin/env python3
"""Collector agent config loading and validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple

ALLOWED_DOMAINS = {
    "building",
    "infrastructure",
    "maintenance_service",
    "non_construction",
    "unknown",
}
ALLOWED_SCOPE_TYPES = {
    "design_only",
    "design_build",
    "build_only",
    "supervision_only",
    "service_only",
    "unknown",
}
ALLOWED_WORK_TYPES = {
    "new_build",
    "renovation",
    "repair",
    "maintenance",
    "cleaning",
    "unknown",
}
ALLOWED_ASSET_SCALES = {"large", "small", "unknown"}

DEFAULT_CLASSIFICATION_MODE = "openai"
DEFAULT_OPENAI_MODEL = "gpt-4.1-mini"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_CLASSIFICATION_WORKERS = 4
DEFAULT_OPENAI_SYSTEM_PROMPT_FILE = "config/agents/classification/system_prompt.txt"
DEFAULT_OPENAI_USER_PROMPT_FILE = "config/agents/classification/user_prompt.txt"
DEFAULT_OPENAI_TEMPERATURE = 0.0
DEFAULT_OPENAI_TOP_P = 1.0
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS = 300
DEFAULT_OPENAI_RESPONSE_FORMAT = "json_object"
DEFAULT_SHOW_IN_PROGRESS_MESSAGES = True
DEFAULT_CLASSIFICATION_LOG_EVERY = 200
DEFAULT_SCAN_LOG_EVERY = 250


def resolve_script_relative_path(path_value: str) -> Path:
    path_obj = Path(path_value)
    if path_obj.is_absolute():
        return path_obj
    return (Path(__file__).resolve().parent.parent / path_obj).resolve()


def load_text_file(path: Path, label: str) -> str:
    if not path.is_file():
        raise RuntimeError(f"{label} file not found: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise RuntimeError(f"{label} file is empty: {path}")
    return text


def deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def default_agent_config() -> Dict[str, Any]:
    return {
        "version": 1,
        "classification_agent": {
            "name": "classification_agent",
            "description": "Classifies construction procurement projects by type.",
            "enabled": True,
            "mode": DEFAULT_CLASSIFICATION_MODE,
            "provider": "openai",
            "workers": DEFAULT_CLASSIFICATION_WORKERS,
            "model": DEFAULT_OPENAI_MODEL,
            "max_tokens": DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
            "api": {
                "base_url": DEFAULT_OPENAI_BASE_URL,
                "api_key_env": "OPENAI_API_KEY",
                "timeout_seconds": 60,
                "retries": 3,
                "retry_backoff_seconds": 1.5,
                "organization": None,
                "project": None,
            },
            "generation": {
                "temperature": DEFAULT_OPENAI_TEMPERATURE,
                "top_p": DEFAULT_OPENAI_TOP_P,
                "top_k": None,
                "max_output_tokens": DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
                "max_completion_tokens": DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
                "candidate_count": 1,
                "presence_penalty": 0,
                "frequency_penalty": 0,
                "seed": None,
            },
            "output": {
                "format": DEFAULT_OPENAI_RESPONSE_FORMAT,
                "strict": False,
                "json_schema_name": "classification_output",
                "response_mime_type": "application/json",
                "schema": {
                    "type": "object",
                    "required": ["domain", "scope_type", "work_type", "asset_scale", "llm_reason"],
                    "properties": {
                        "domain": {"type": "string", "enum": sorted(ALLOWED_DOMAINS)},
                        "scope_type": {"type": "string", "enum": sorted(ALLOWED_SCOPE_TYPES)},
                        "work_type": {"type": "string", "enum": sorted(ALLOWED_WORK_TYPES)},
                        "asset_scale": {"type": "string", "enum": sorted(ALLOWED_ASSET_SCALES)},
                        "llm_reason": {"type": "string"},
                    },
                },
            },
            "tools": {
                "allowed": [],
                "tool_choice": "auto",
                "parallel_tool_calls": True,
                "max_tool_calls": None,
                "show_sources": True,
            },
            "history": {
                "enabled": False,
                "path": "data/agent_history/classification_agent_history.jsonl",
                "store_raw_responses": False,
                "redact_api_keys": True,
                "max_entries": 1000,
            },
            "progress": {
                "show_in_progress_messages": DEFAULT_SHOW_IN_PROGRESS_MESSAGES,
                "classification_log_every_n": DEFAULT_CLASSIFICATION_LOG_EVERY,
                "scan_log_every_n": DEFAULT_SCAN_LOG_EVERY,
            },
            "prompts": {
                "system_file": DEFAULT_OPENAI_SYSTEM_PROMPT_FILE,
                "user_file": DEFAULT_OPENAI_USER_PROMPT_FILE,
            },
            "openai": {
                "api_style": "chat_completions",
                "service_tier": "auto",
                "store": False,
                "user": None,
                "stream": False,
                "stream_options": {"include_usage": True},
                "metadata": {},
                "reasoning": {"effort": "medium"},
                "modalities": ["text"],
                "audio": None,
                "include": [],
                "previous_response_id": None,
                "truncation": "disabled",
                "logprobs": False,
                "top_logprobs": None,
                "logit_bias": {},
                "n": 1,
                "stop": None,
            },
            "gemini": {
                "api_style": "generateContent",
                "response_mime_type": "application/json",
                "response_schema": None,
                "safety_settings": [],
                "tools": [],
                "tool_config": None,
                "system_instruction": None,
                "cached_content": None,
                "labels": {},
                "thinking_config": {"thinking_budget": None, "include_thoughts": False},
            },
            "claude": {
                "api_style": "messages",
                "anthropic_version": "2023-06-01",
                "anthropic_beta": [],
                "service_tier": "auto",
                "metadata": {},
                "thinking": {"type": "disabled", "budget_tokens": None},
                "container": None,
                "context_management": None,
                "mcp_servers": [],
                "stop_sequences": [],
            },
        },
    }


def validate_agent_config(config: Dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise RuntimeError("Agent config root must be an object.")
    agent = config.get("classification_agent")
    if not isinstance(agent, dict):
        raise RuntimeError("Agent config must include object 'classification_agent'.")
    required_top_keys = [
        "enabled",
        "mode",
        "provider",
        "workers",
        "model",
        "api",
        "generation",
        "output",
        "history",
        "progress",
        "prompts",
    ]
    for key in required_top_keys:
        if key not in agent:
            raise RuntimeError(f"classification_agent missing required key: {key}")

    mode = str(agent.get("mode") or "")
    if mode not in {"openai"}:
        raise RuntimeError("classification_agent.mode must be 'openai'")
    if not isinstance(agent.get("enabled"), bool):
        raise RuntimeError("classification_agent.enabled must be true/false.")
    workers = agent.get("workers")
    if not isinstance(workers, int) or workers <= 0:
        raise RuntimeError("classification_agent.workers must be integer > 0.")
    model = str(agent.get("model") or "").strip()
    if not model:
        raise RuntimeError("classification_agent.model must be a non-empty string.")

    api = agent.get("api")
    if not isinstance(api, dict):
        raise RuntimeError("classification_agent.api must be an object.")
    for api_key in ("base_url", "api_key_env", "timeout_seconds", "retries", "retry_backoff_seconds"):
        if api_key not in api:
            raise RuntimeError(f"classification_agent.api missing key: {api_key}")

    output = agent.get("output")
    if not isinstance(output, dict):
        raise RuntimeError("classification_agent.output must be an object.")
    output_format = str(output.get("format") or "")
    if output_format not in {"json_object", "json_schema", "label_string"}:
        raise RuntimeError("classification_agent.output.format must be one of: json_object, json_schema, label_string")
    schema = output.get("schema")
    if output_format == "json_object":
        if not isinstance(schema, dict):
            raise RuntimeError("classification_agent.output.schema must be an object for json_object format.")
        props = schema.get("properties")
        if not isinstance(props, dict):
            raise RuntimeError("classification_agent.output.schema.properties must be an object.")
        for key in ("domain", "scope_type", "work_type", "asset_scale", "llm_reason"):
            if key not in props:
                raise RuntimeError(f"classification_agent.output.schema.properties missing key: {key}")
        for key in ("domain", "scope_type", "work_type", "asset_scale"):
            enum_values = props.get(key, {}).get("enum")
            if not isinstance(enum_values, list) or not enum_values:
                raise RuntimeError(
                    f"classification_agent.output.schema.properties.{key}.enum must be a list."
                )

    history_obj = agent.get("history")
    if not isinstance(history_obj, dict):
        raise RuntimeError("classification_agent.history must be an object.")
    if not isinstance(history_obj.get("enabled"), bool):
        raise RuntimeError("classification_agent.history.enabled must be true/false.")

    progress_obj = agent.get("progress")
    if not isinstance(progress_obj, dict):
        raise RuntimeError("classification_agent.progress must be an object.")
    if not isinstance(progress_obj.get("show_in_progress_messages"), bool):
        raise RuntimeError("classification_agent.progress.show_in_progress_messages must be true/false.")
    for key in ("classification_log_every_n", "scan_log_every_n"):
        if not isinstance(progress_obj.get(key), int) or int(progress_obj.get(key)) < 0:
            raise RuntimeError(f"classification_agent.progress.{key} must be integer >= 0.")


def load_agent_config(path: Path) -> Tuple[Dict[str, Any], bool]:
    defaults = default_agent_config()
    if not path.is_file():
        return defaults, False
    raw = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse agent config JSON ({path}): {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Agent config root must be object: {path}")

    normalized: Dict[str, Any]

    # Format A: legacy/global config root with classification_agent object
    if isinstance(parsed.get("classification_agent"), dict):
        normalized = parsed
    # Format B: direct classification agent config (config/agents/classification/config.json)
    elif isinstance(parsed.get("mode"), str) and isinstance(parsed.get("prompts"), dict):
        normalized = {"version": parsed.get("version", 1), "classification_agent": parsed}
    # Format C: thin index config with agents.classification path
    elif isinstance(parsed.get("agents"), dict) and isinstance(parsed["agents"].get("classification"), str):
        classification_rel = str(parsed["agents"]["classification"])
        classification_path = (path.parent / classification_rel).resolve()
        if not classification_path.is_file():
            raise RuntimeError(f"classification agent config file not found: {classification_path}")
        classification_raw = classification_path.read_text(encoding="utf-8")
        try:
            classification_obj = json.loads(classification_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse classification agent config JSON ({classification_path}): {exc}"
            ) from exc
        if not isinstance(classification_obj, dict):
            raise RuntimeError(f"classification agent config root must be object: {classification_path}")
        normalized = {
            "version": parsed.get("version", 1),
            "classification_agent": classification_obj,
        }
    else:
        raise RuntimeError(
            "Unsupported agent config format. Expected one of: "
            "{classification_agent: {...}}, direct classification agent object, "
            "or {agents: {classification: <path>}}."
        )

    merged = deep_merge_dict(defaults, normalized)
    validate_agent_config(merged)
    return merged, True


def load_pipeline_config(path: Path) -> Tuple[Dict[str, Any], bool]:
    if not path.is_file():
        return {}, False
    raw = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse pipeline config JSON ({path}): {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Pipeline config root must be object: {path}")
    return parsed, True
