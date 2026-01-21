from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
load_dotenv()
class MinIOConnection:

    def __init__(self):
        self.minio_client = self._connect_minio()

    def _connect_minio(self):
        return Minio(
            "object-store:9000",
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
            json_bytes = io.BytesIO(json_string.encode('utf-8'))
            self.minio_client.put_object(
                bucket_name,
                destination_file,
                json_bytes,
                length=len(json_string),
                content_type='application/json'
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