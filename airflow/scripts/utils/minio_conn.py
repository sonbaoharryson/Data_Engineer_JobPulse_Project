from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
import hashlib
import imghdr
import base64
from dotenv import load_dotenv
load_dotenv()
class MinIOConnection:

    def __init__(self):
        self.minio_client = self._connect_minio()

    def _connect_minio(self):
        return Minio(
            "object-store:9000", # change to localhost for local testing
            access_key=os.getenv("MINIO_USER"),
            secret_key=os.getenv("MINIO_PASSWORD"),
            secure=False
        )
    
    def upload_data_object(self, bucket_name: str, data_object: list[dict], destination_file: str):
        import json
        import io
        """Upload a file to a specified bucket in MinIO"""
        try:
            self.minio_client.bucket_exists(bucket_name)
        except S3Error as e:
            print(f"Error checking bucket: {e}")
            print(f"Creating bucket: {bucket_name}")
            self.minio_client.make_bucket(bucket_name)
        try:
            json_string = json.dumps(data_object, ensure_ascii=False)
            json_bytes_raw = json_string.encode("utf-8")
            json_bytes = io.BytesIO(json_bytes_raw)

            self.minio_client.put_object(
                bucket_name,
                destination_file,
                json_bytes,
                length=len(json_bytes_raw),   # ✅ BYTES length
                content_type="application/json"
            )
        except S3Error as e:
            print(f"Error uploading file: {e}")

    def read_file(self, bucket_name: str, object_name: str):
        """Read a file from a specified bucket in MinIO"""
        response = None
        try:
            response = self.minio_client.get_object(bucket_name, object_name)
            
            data = response.read().decode('utf-8')
            return data
        except S3Error as e:
            print(f"Error reading file: {e}")
        finally:
            if response:
                response.close()
                response.release_conn()
    
    def upload_file(self, bucket_name: str, source_url: str, content: bytes):
        def _detect_extension(content: bytes) -> str:
            img_type = imghdr.what(None, content)
            if not img_type:
                raise ValueError("Unknown image type")
            return img_type

        def _object_name(url: str, ext: str) -> str:
            h = hashlib.md5(url.encode()).hexdigest()
            return f"logos/{h}.{ext}"

        """Upload a file to a specified bucket in MinIO"""
        ext = _detect_extension(content)
        object_name = _object_name(source_url, ext)
        try:
            self.minio_client.bucket_exists(bucket_name)
        except S3Error as e:
            print(f"Error checking bucket: {e}")
            print(f"Creating bucket: {bucket_name}")
            self.minio_client.make_bucket(bucket_name)
        try:
            self.minio_client.put_object(
                bucket_name,
                object_name=object_name,
                data=BytesIO(content),
                length=len(content),
                content_type=f"image/{ext}",
            )
        except S3Error as e:
            print(f"Error uploading file: {e}")
        mime = "png" if ext.lower() == "png" else "jpeg"
        return object_name, mime