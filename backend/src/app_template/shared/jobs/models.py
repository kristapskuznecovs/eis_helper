from dataclasses import dataclass
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class JobResult:
    job_id: str
    status: JobStatus
    result: Any = None
    error: str | None = None
