try:
    from celery import Celery  # pyright: ignore[reportMissingImports]
except Exception:  # pragma: no cover
    Celery = None  # type: ignore[assignment]

from app_template.settings import get_settings

settings = get_settings()

celery_app = (
    Celery("app_template", broker=settings.redis_url, backend=settings.redis_url)
    if Celery is not None
    else None
)
