import boto3

from app_template.shared.logging.config import get_logger
from app_template.shared.storage.backends.base import BaseStorage
from app_template.shared.storage.models import StoredFile

logger = get_logger(__name__)


class S3Storage(BaseStorage):
    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str | None,
        aws_access_key_id: str | None,
        aws_secret_access_key: str | None,
    ) -> None:
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def upload(self, key: str, data: bytes, content_type: str) -> StoredFile:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        logger.info("file uploaded s3", key=key, bucket=self.bucket, size_bytes=len(data))
        return StoredFile(
            key=key,
            url=self.get_url(key),
            size_bytes=len(data),
            content_type=content_type,
            backend="s3",
        )

    def download(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)
        logger.info("file deleted s3", key=key, bucket=self.bucket)

    def get_url(self, key: str, expires_in: int = 3600) -> str:
        return str(
            self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        )
