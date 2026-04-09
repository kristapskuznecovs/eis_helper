from functools import lru_cache

from app_template.settings import get_settings
from app_template.shared.jobs.backends.inline import InlineBackend
from app_template.shared.jobs.models import JobResult


@lru_cache(maxsize=1)
def _get_backend():
    settings = get_settings()
    if settings.jobs_backend == "celery":
        from app_template.celery_app import celery_app
        from app_template.shared.jobs.backends.celery import CeleryBackend

        if celery_app is None:
            raise RuntimeError("Celery backend requested but celery_app is not configured.")
        return CeleryBackend(celery_app)
    return InlineBackend()


async def enqueue(task: str, payload: dict) -> str:
    return await _get_backend().enqueue(task, payload)


async def get_result(job_id: str) -> JobResult:
    return await _get_backend().get_result(job_id)


async def retry(job_id: str) -> str:
    return await _get_backend().retry(job_id)
