from app_template.shared.jobs.backends.base import BaseBackend
from app_template.shared.jobs.models import JobResult, JobStatus
from app_template.shared.logging.config import get_logger

logger = get_logger(__name__)


class CeleryBackend(BaseBackend):
    def __init__(self, celery_app) -> None:
        self.celery = celery_app

    async def enqueue(self, task: str, payload: dict) -> str:
        result = self.celery.send_task(task, kwargs=payload)
        logger.info("job enqueued celery", job_id=result.id, task=task)
        return str(result.id)

    async def get_result(self, job_id: str) -> JobResult:
        result = self.celery.AsyncResult(job_id)
        status_map = {
            "PENDING": JobStatus.PENDING,
            "STARTED": JobStatus.RUNNING,
            "SUCCESS": JobStatus.SUCCESS,
            "FAILURE": JobStatus.FAILED,
            "RETRY": JobStatus.RETRYING,
        }
        return JobResult(
            job_id=job_id,
            status=status_map.get(result.status, JobStatus.PENDING),
            result=result.result if result.successful() else None,
            error=str(result.result) if result.failed() else None,
        )

    async def retry(self, job_id: str) -> str:
        raise NotImplementedError("Retry flow depends on task-specific Celery policies.")
