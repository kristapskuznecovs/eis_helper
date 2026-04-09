from functools import lru_cache

from app_template.settings import get_settings
from app_template.shared.storage.backends.local import LocalStorage
from app_template.shared.storage.models import StoredFile


@lru_cache(maxsize=1)
def _get_backend():
    settings = get_settings()
    if settings.storage_backend == "s3":
        from app_template.shared.storage.backends.s3 import S3Storage

        return S3Storage(
            bucket=settings.s3_bucket,
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
    return LocalStorage(base_path=settings.storage_local_root)


def upload(key: str, data: bytes, content_type: str) -> StoredFile:
    return _get_backend().upload(key, data, content_type)


def download(key: str) -> bytes:
    return _get_backend().download(key)


def delete(key: str) -> None:
    _get_backend().delete(key)


def get_url(key: str, expires_in: int = 3600) -> str:
    return _get_backend().get_url(key, expires_in)
