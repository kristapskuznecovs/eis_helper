from __future__ import annotations

import json
from contextvars import ContextVar
from functools import cache
from pathlib import Path
from typing import Any

from .config import DEFAULT_LOCALE

_locale_ctx: ContextVar[str] = ContextVar("locale", default=DEFAULT_LOCALE.value)


def _locale_path(locale: str) -> Path:
    return Path(__file__).resolve().parents[4] / "locales" / locale / "messages.json"


@cache
def _load(locale: str) -> dict[str, Any]:
    path = _locale_path(locale)
    fallback = _locale_path(DEFAULT_LOCALE.value)
    target = path if path.exists() else fallback
    return json.loads(target.read_text(encoding="utf-8"))


def set_locale(locale: str) -> None:
    _locale_ctx.set(locale)


def get_locale() -> str:
    return _locale_ctx.get()


def translate(key: str, **kwargs: Any) -> str:
    text = _load(get_locale()).get(key) or _load(DEFAULT_LOCALE.value).get(key) or key
    if not isinstance(text, str):
        return key
    return text.format(**kwargs) if kwargs else text


def _(key: str, **kwargs: Any) -> str:
    return translate(key, **kwargs)
