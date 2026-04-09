from __future__ import annotations

from fastapi import Request
from structlog.contextvars import bind_contextvars

from .config import DEFAULT_LOCALE, LOCALE_HEADER, SUPPORTED_LOCALES
from .translator import set_locale


def _pick_accept_language(header: str) -> str | None:
    for part in header.split(","):
        lang = part.strip().split(";")[0][:2].lower()
        if lang in SUPPORTED_LOCALES:
            return lang
    return None


async def i18n_middleware(request: Request, call_next):
    query_locale = request.query_params.get("lang")
    explicit_locale = request.headers.get(LOCALE_HEADER)
    accept_language = _pick_accept_language(request.headers.get("accept-language", ""))

    locale = explicit_locale or query_locale or accept_language or DEFAULT_LOCALE.value
    resolved = locale if locale in SUPPORTED_LOCALES else DEFAULT_LOCALE.value

    set_locale(resolved)
    request.state.locale = resolved
    bind_contextvars(locale=resolved)

    response = await call_next(request)
    response.headers[LOCALE_HEADER] = resolved
    return response
