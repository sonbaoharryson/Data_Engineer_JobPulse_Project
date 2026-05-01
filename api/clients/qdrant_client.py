import logging
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import ScoredPoint
from config import Settings

logger = logging.getLogger(__name__)


class QdrantClientWrapper:
    def __init__(self, settings: Settings):
        self.client = QdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT)
        self.collection = settings.QDRANT_COLLECTION

    def search_similar(
        self,
        vector: List[float],
        limit: int = 5,
        score_threshold: Optional[float] = None,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[ScoredPoint]:
        """Search for similar vectors in Qdrant."""
        try:
            search_params = {
                "collection_name": self.collection,
                "query_vector": vector,
                "limit": limit,
            }
            if score_threshold is not None:
                search_params["score_threshold"] = score_threshold
            if filter_conditions is not None:
                # Qdrant accepts JSON filters in the `filter` key.
                # Example: {'must': [{'key':'job_location', 'match': {'value':'HCM'}}]}
                search_params["filter"] = filter_conditions

            results = self.client.search(**search_params)
            return results
        except Exception as e:
            logger.error(f"Error searching Qdrant: {e}")
            return []

    def get_job_details(self, job_ids: List[str]) -> List[Dict[str, Any]]:
        """Get job details by job IDs."""
        try:
            # This is a simplified implementation
            # In practice, you might need to query the database or use scroll API
            jobs = []
            for job_id in job_ids:
                # Search with filter for job_id
                results = self.client.scroll(
                    collection_name=self.collection,
                    scroll_filter={
                        "must": [{"key": "job_id", "match": {"value": job_id}}]
                    },
                    limit=1,
                )
                if results[0]:
                    jobs.append(results[0][0].payload)
            return jobs
        except Exception as e:
            logger.error(f"Error getting job details: {e}")
            return []
