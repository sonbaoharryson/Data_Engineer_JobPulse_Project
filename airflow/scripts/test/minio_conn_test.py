import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.minio_conn import MinIOConnection

minio_conn = MinIOConnection()
bucket_name = "crawled-data"
source_file = r'C:\Users\user\Desktop\bot_chat_discord\airflow\scripts\source_topcv.json'
destination_file = "it_viec/it_viec_jobs.json"
data_object = [{"title": "Sample Job", "url": "http://example.com/job1"}]
minio_conn.upload_data_object(bucket_name, destination_file=destination_file, data_object=data_object)