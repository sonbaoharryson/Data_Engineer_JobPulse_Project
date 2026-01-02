from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # API settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Job API"
    
    # Database settings
    DB_USER: str = os.getenv("DB_USER", "")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_HOST: str = os.getenv("DB_HOST", "")
    DB_PORT: str = os.getenv("DB_PORT", "")
    DB_NAME: str = os.getenv("DB_NAME", "")
    
    # Rate limiting disabled (unrestricted)
    RATE_LIMIT: int = 0  # 0 = no limit
    RATE_LIMIT_EXPIRES: int = 0  # no expiration

    # API host/port (used when running `python app.py`)
    API_HOST: str = "127.0.0.1"
    API_PORT: int = 8000

    class Config:
        env_file = ".env"