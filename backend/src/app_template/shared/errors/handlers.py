from typing import Any

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app_template.shared.errors.exceptions import AppError
from app_template.shared.i18n import _
from app_template.shared.logging.config import get_logger
from app_template.shared.logging.middleware import REQUEST_ID_HEADER

logger = get_logger(__name__)


def _request_id(request: Request) -> str:
    candidate = getattr(request.state, "request_id", None)
    if isinstance(candidate, str) and candidate:
        return candidate
    return request.headers.get(REQUEST_ID_HEADER, "")


def _error_payload(*, code: str, message: str, request_id: str, details: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
        }
    }
    if details is not None:
        payload["error"]["details"] = details
    return payload


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    request_id = _request_id(request)
    logger.warning("application error", error_code=exc.code, status_code=exc.status_code)
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_payload(code=exc.code, message=_(exc.code, **exc.params), request_id=request_id),
        headers={REQUEST_ID_HEADER: request_id},
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    request_id = _request_id(request)
    message = str(exc.detail)
    logger.warning("http exception", status_code=exc.status_code, detail=message)
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_payload(
            code="errors.http",
            message=message if isinstance(exc.detail, str) else _("errors.http"),
            request_id=request_id,
            details=exc.detail,
        ),
        headers={REQUEST_ID_HEADER: request_id},
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    request_id = _request_id(request)
    logger.warning("validation error", errors=exc.errors())
    return JSONResponse(
        status_code=422,
        content=_error_payload(
            code="errors.validation",
            message=_("errors.validation"),
            request_id=request_id,
            details=exc.errors(),
        ),
        headers={REQUEST_ID_HEADER: request_id},
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = _request_id(request)
    logger.error("unhandled exception", error_type=type(exc).__name__, exc_info=True)
    return JSONResponse(
        status_code=500,
        content=_error_payload(
            code="errors.internal",
            message=_("errors.internal"),
            request_id=request_id,
        ),
        headers={REQUEST_ID_HEADER: request_id},
    )
