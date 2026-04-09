import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from app_template.shared.jobs.backends.base import BaseBackend
from app_template.shared.jobs.models import JobResult, JobStatus
from app_template.shared.logging.config import get_logger

logger = get_logger(__name__)
_REGISTRY: dict[str, Callable[..., Any]] = {}


def register(name: str):
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        _REGISTRY[name] = fn
        return fn

    return decorator


class InlineBackend(BaseBackend):
    async def enqueue(self, task: str, payload: dict) -> str:
        job_id = str(uuid.uuid4())
        fn = _REGISTRY.get(task)
        if fn is None:
            raise ValueError(f"Unknown task: {task}")
        logger.info("job enqueued inline", job_id=job_id, task=task)
        asyncio.create_task(self._run(job_id, task, fn, payload))
        return job_id

    async def _run(
        self, job_id: str, task: str, fn: Callable[..., Any], payload: dict[str, Any]
    ) -> None:
        try:
            logger.info("job started", job_id=job_id, task=task)
            result = fn(**payload)
            if isinstance(result, Awaitable):
                await result
            logger.info("job completed", job_id=job_id, task=task)
        except Exception as exc:  # pragma: no cover - runtime safety net
            logger.error("job failed", job_id=job_id, task=task, error=str(exc), exc_info=True)

    async def get_result(self, job_id: str) -> JobResult:
        return JobResult(job_id=job_id, status=JobStatus.RUNNING)

    async def retry(self, job_id: str) -> str:
        raise NotImplementedError("Inline backend does not support retry.")
