import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.minio_conn import MinIOConnection
import json
minio_conn = MinIOConnection()
bucket_name = "crawled-data"
_data = minio_conn.read_file(bucket_name, "itviec/itviec_jobs_2026_01_21_18_04_17.json")
data = json.loads(_data) if _data else []
print(type(data))