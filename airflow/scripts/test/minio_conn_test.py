import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.minio_conn import MinIOConnection

minio_conn = MinIOConnection()
bucket_name = "crawled-data"
source_file = r'C:\Users\user\Desktop\bot_chat_discord\airflow\scripts\source_topcv.json'
destination_file = "it_viec/it_viec_jobs.json"
minio_conn.upload_file(bucket_name, destination_file, source_file)