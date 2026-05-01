from pydantic import BaseModel
import os


class Settings(BaseModel):
    # API settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Job Recommendation API"

    # Qdrant settings
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "localhost")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", "6333"))
    QDRANT_COLLECTION: str = "jobs"

    # MongoDB settings
    MONGODB_HOST: str = os.getenv("MONGODB_HOST", "localhost")
    MONGODB_PORT: int = int(os.getenv("MONGODB_PORT", "27017"))
    MONGODB_DB: str = os.getenv("MONGODB_DB", "chat_db")
    MONGODB_USERNAME: str = os.getenv("MONGODB_USERNAME", "")
    MONGODB_PASSWORD: str = os.getenv("MONGODB_PASSWORD", "")

    # Ollama settings
    OLLAMA_HOST: str = os.getenv("OLLAMA_HOST", "localhost")
    OLLAMA_PORT: int = int(os.getenv("OLLAMA_PORT", "11434"))
    OLLAMA_EMBED_MODEL: str = os.getenv("OLLAMA_EMBED_MODEL", "mxbai-embed-large")
    OLLAMA_GENERATE_MODEL: str = os.getenv("OLLAMA_GENERATE_MODEL", "gemma3:1b")

    # Memory settings
    MAX_MEMORY_MESSAGES: int = 100

    # MinIO settings
    MINIO_HOST: str = os.getenv("MINIO_HOST", "localhost")
    MINIO_PORT: int = int(os.getenv("MINIO_PORT", "9000"))
    MINIO_USER: str = os.getenv("MINIO_USER", "minioadmin")
    MINIO_PASSWORD: str = os.getenv("MINIO_PASSWORD", "minioadmin")
    MINIO_BUCKET_RESUMES: str = "user_resume"
