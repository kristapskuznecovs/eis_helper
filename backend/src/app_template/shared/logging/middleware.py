import time
import uuid

from fastapi import Request
from structlog.contextvars import bind_contextvars, clear_contextvars

from app_template.shared.logging.config import get_logger

logger = get_logger(__name__)
REQUEST_ID_HEADER = "X-Request-ID"


async def logging_middleware(request: Request, call_next):
    clear_contextvars()

    request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
    started_at = time.perf_counter()

    bind_contextvars(
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        query=str(request.query_params) or None,
        client_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )

    request.state.request_id = request_id
    logger.info("request started")

    try:
        response = await call_next(request)
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        bind_contextvars(status_code=response.status_code, duration_ms=duration_ms)

        if response.status_code >= 500:
            logger.error("request failed")
        elif response.status_code >= 400:
            logger.warning("request client error")
        else:
            logger.info("request completed")

        response.headers[REQUEST_ID_HEADER] = request_id
        return response
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.error(
            "request crashed",
            error=str(exc),
            error_type=type(exc).__name__,
            duration_ms=duration_ms,
            exc_info=True,
        )
        raise
