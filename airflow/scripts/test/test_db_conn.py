import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


from utils.db_conn import DBConnection

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