from pathlib import Path

from app_template.shared.logging.config import get_logger
from app_template.shared.storage.backends.base import BaseStorage
from app_template.shared.storage.models import StoredFile

logger = get_logger(__name__)


class LocalStorage(BaseStorage):
    def __init__(self, base_path: str) -> None:
        self.base = Path(base_path)
        self.base.mkdir(parents=True, exist_ok=True)

    def upload(self, key: str, data: bytes, content_type: str) -> StoredFile:
        path = self.base / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        logger.info("file uploaded local", key=key, size_bytes=len(data))
        return StoredFile(
            key=key,
            url=f"/local-files/{key}",
            size_bytes=len(data),
            content_type=content_type,
            backend="local",
        )

    def download(self, key: str) -> bytes:
        return (self.base / key).read_bytes()

    def delete(self, key: str) -> None:
        (self.base / key).unlink(missing_ok=True)
        logger.info("file deleted local", key=key)

    def get_url(self, key: str, expires_in: int = 3600) -> str:
        del expires_in
        return f"/local-files/{key}"
