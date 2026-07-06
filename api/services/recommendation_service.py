import logging
from typing import Any, Dict, List

from clients.ollama_client import OllamaClient
from clients.qdrant_client import QdrantClientWrapper
from config import Settings

logger = logging.getLogger(__name__)


class RecommendationService:
    def __init__(self, settings: Settings):
        self.ollama_client = OllamaClient(settings)
        self.qdrant_client = QdrantClientWrapper(settings)

    def recommend_jobs(
        self,
        query: str,
        experience_level: str = None,
        location: str = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Recommend jobs based on user profile/query using vector similarity."""
        try:
            # Generate embedding for the query/profile text
            embedding = self.ollama_client.generate_embedding(query)
            if not embedding:
                logger.error("Failed to generate embedding for query")
                return []

            # Search similar jobs in Qdrant using embedding
            # In future you can pass filter_conditions to qdrant for server-side filtering
            results = self.qdrant_client.search_similar(
                vector=embedding,
                limit=limit * 3,  # Get extra results so we can enforce filters in-app
                score_threshold=0.7,  # Minimum similarity score
            )

            # Process and filter results
            recommendations = []
            seen_job_ids = set()

            for result in results:
                payload = result.payload
                job_id = payload.get("job_id")

                if not job_id:
                    continue

                # Skip duplicates
                if job_id in seen_job_ids:
                    continue
                seen_job_ids.add(job_id)

                # Apply user filters
                if experience_level and payload.get("experiences_level"):
                    if (
                        payload.get("experiences_level").lower()
                        != experience_level.lower()
                    ):
                        continue

                if location and payload.get("job_location"):
                    if location.lower() not in payload.get("job_location", "").lower():
                        continue

                recommendations.append(
                    {
                        "job_id": job_id,
                        "job_title": payload.get("job_title", ""),
                        "company_name": payload.get("company_name", ""),
                        "url": payload.get("url", ""),
                        "embedding_text": payload.get("embedding_text", ""),
                        "score": float(getattr(result, "score", 0.0)),
                    }
                )

                if len(recommendations) >= limit:
                    break

            return recommendations

        except Exception as e:
            logger.error(f"Error recommending jobs: {e}")
            return []
