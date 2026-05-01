import logging
from typing import Optional
from minio import Minio
from minio.error import S3Error
from config import Settings

logger = logging.getLogger(__name__)


class MinioClientWrapper:
    def __init__(self, settings: Settings):
        self.client = Minio(
            f"{settings.MINIO_HOST}:{settings.MINIO_PORT}",
            access_key=settings.MINIO_USER,
            secret_key=settings.MINIO_PASSWORD,
            secure=False,
        )
        self.bucket_resumes = settings.MINIO_BUCKET_RESUMES
        self._ensure_buckets()

    def _ensure_buckets(self):
        """Ensure required buckets exist."""
        try:
            buckets = [self.bucket_resumes]
            for bucket in buckets:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
        except S3Error as e:
            logger.error(f"Error ensuring buckets: {e}")

    def upload_resume(
        self, user_id: str, file_data: bytes, filename: str
    ) -> Optional[str]:
        """Upload user resume to MinIO bucket.

        Args:
            user_id: The user's ID
            file_data: The file content as bytes
            filename: The original filename

        Returns:
            The object name in MinIO if successful, None otherwise
        """
        try:
            object_name = f"{user_id}/{filename}"
            self.client.put_object(
                self.bucket_resumes,
                object_name,
                file_data,
                length=len(file_data),
            )
            logger.info(f"Uploaded resume for user {user_id}: {object_name}")
            return object_name
        except S3Error as e:
            logger.error(f"Error uploading resume: {e}")
            return None

    def download_resume(self, user_id: str, filename: str) -> Optional[bytes]:
        """Download user resume from MinIO bucket.

        Args:
            user_id: The user's ID
            filename: The filename to download

        Returns:
            The file content as bytes if successful, None otherwise
        """
        try:
            object_name = f"{user_id}/{filename}"
            response = self.client.get_object(self.bucket_resumes, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error downloading resume: {e}")
            return None

    def delete_resume(self, user_id: str, filename: str) -> bool:
        """Delete user resume from MinIO bucket.

        Args:
            user_id: The user's ID
            filename: The filename to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            object_name = f"{user_id}/{filename}"
            self.client.remove_object(self.bucket_resumes, object_name)
            logger.info(f"Deleted resume for user {user_id}: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting resume: {e}")
            return False
