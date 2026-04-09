from abc import ABC, abstractmethod

from app_template.shared.jobs.models import JobResult


class BaseBackend(ABC):
    @abstractmethod
    async def enqueue(self, task: str, payload: dict) -> str:
        """Enqueue a task and return the job ID."""

    @abstractmethod
    async def get_result(self, job_id: str) -> JobResult:
        """Return job status and result."""

    @abstractmethod
    async def retry(self, job_id: str) -> str:
        """Retry a failed job and return a new job ID."""
