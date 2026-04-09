from abc import ABC, abstractmethod

from app_template.shared.storage.models import StoredFile


class BaseStorage(ABC):
    @abstractmethod
    def upload(self, key: str, data: bytes, content_type: str) -> StoredFile:
        """Upload a file and return metadata."""

    @abstractmethod
    def download(self, key: str) -> bytes:
        """Download file content by key."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a file by key."""

    @abstractmethod
    def get_url(self, key: str, expires_in: int = 3600) -> str:
        """Return a public or presigned URL."""
