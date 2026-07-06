import logging
from typing import List

from config import Settings
from fastapi import APIRouter, Depends, HTTPException
from schemas import RecommendJobsRequest, RecommendJobsResponse
from services import recommendation_service

logger = logging.getLogger(__name__)

router = APIRouter()


# ------------------------------------------------------------------------------
@router.post("/jobs/recommend", response_model=RecommendJobsResponse)
async def recommend_jobs_endpoint(
    payload: RecommendJobsRequest,
    settings: Settings = Depends(Settings),
):
    """Recommend jobs using vector similarity (Qdrant + embedding)."""
    try:
        service = recommendation_service.RecommendationService(settings)
        recs = service.recommend_jobs(
            user_id=payload.user_id,
            query=payload.query,
            experience_level=payload.experience_level,
            location=payload.location,
            min_salary=payload.min_salary,
            limit=payload.limit,
        )

        return RecommendJobsResponse(recommendations=recs)
    except Exception:
        logger.exception("Error recommending jobs")
        raise HTTPException(status_code=500, detail="Internal server error")
