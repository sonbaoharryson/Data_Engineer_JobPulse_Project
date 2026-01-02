import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

class DBConnection:
    def __init__(self):
        self.engine = self._create_db_connection()

    def _create_db_connection(self):
        """Create database connection"""
        db_url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_JOB')}"
        try:
            return create_engine(db_url)
        except SQLAlchemyError as e:
            print(f"Database connection error: {e}")
            return None