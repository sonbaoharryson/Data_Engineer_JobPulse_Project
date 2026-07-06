import os
import requests
import logging
import uuid
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Initialize Qdrant client
client = QdrantClient(host=os.getenv("QDRANT_HOST"), port=int(os.getenv("QDRANT_PORT")))

OLLAMA_HOST = os.getenv("OLLAMA_HOST")
OLLAMA_PORT = os.getenv("OLLAMA_PORT")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION")
VECTOR_SIZE = 1024
MODEL_NAME = os.getenv("OLLAMA_EMBED_MODEL")
OLLAMA_EMBED_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/embed"


# Fields to embed separately for each job
FIELDS_TO_EMBED = [
    "job_title",
    "company_name",
    "job_location",
    "experiences_level",
    "requirements",
    "descriptions",
    "tags",
]


def embed_texts(texts: str):
    if not texts:
        return None

    try:
        response = requests.post(
            OLLAMA_EMBED_URL, json={"model": MODEL_NAME, "input": texts}, timeout=360
        )

        response.raise_for_status()

        data = response.json()

        if "embeddings" not in data:
            raise ValueError(f"Invalid response: {data}")

        if len(data["embeddings"]) == 0:
            return None

        return data["embeddings"][0]

    except Exception as e:
        logger.exception(f"Embedding failed: {e}")
        return None


def embed_and_save_data(data: List[Dict[str, Any]]) -> None:
    if not data:
        logger.warning("No data provided for embedding and saving.")
        return

    try:
        if not client.collection_exists(COLLECTION_NAME):
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
            )

        points = []
        for item in data:
            if not item.get("embedding_text"):
                continue

            job_id = str(item["job_id"])
            try:
                embedding_vector = embed_texts(item.get("embedding_text", ""))
                point_id = str(uuid.uuid4())

                payload = {
                    "job_id": job_id,
                    "job_title": item.get("job_title"),
                    "url": item.get("url"),
                    "company_name": item.get("company_name"),
                    "embedding_text": item.get("embedding_text"),
                }

                point = PointStruct(
                    id=point_id,
                    vector=embedding_vector,
                    payload=payload,
                )
                points.append(point)
            except Exception as e:
                logger.error(f"Error embedding data for job_id '{job_id}': {e}")

        # Upsert all points
        if points:
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            logger.info(
                f"Successfully embedded and saved {len(points)} vectors for {len(data)} jobs."
            )
        else:
            logger.warning("No valid fields to embed.")

    except Exception as e:
        logger.error(f"Error embedding and saving data to Qdrant: {e}")
        raise
