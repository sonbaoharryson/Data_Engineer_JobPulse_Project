import os
import sys
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
# Add project root (airflow) to sys.path so `scripts` package can be imported
project_root = Path(__file__).resolve().parents[3]
project_root_str = str(project_root)

if project_root_str not in sys.path:
    sys.path.insert(1, project_root_str)
print("sys.path[0:5] =", sys.path[0:5])

from scripts.utils.db_conn import DBConnection

try:
    db_conn = DBConnection()._create_db_connection()
    print("Imported DBConnection successfully", db_conn)
except Exception as e:
    print("Import failed:", e)


with db_conn.connect() as connection:
    query = text("SELECT * FROM bronze.itviec_data_job LIMIT 5;")
    try:
        result = connection.execute(query)
        print("Sample query executed successfully.")
        print(result.fetchall())
    except SQLAlchemyError as e:
        print(f"Query execution error: {e}")