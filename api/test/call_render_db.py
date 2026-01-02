from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

db_conn = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
conn = create_engine(db_conn).connect()

r = conn.execute(text("""
        SELECT * 
        FROM bronze.topcv_data_job
    """))

for row in r:
    print(row)
