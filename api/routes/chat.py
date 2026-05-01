from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from services.chat_service import ChatService
from services.recommendation_service import RecommendationService
from services.resume_service import ResumeService
from services.memory_service import MemoryService
from clients.mongodb_client import MongoDBClientWrapper
from clients.minio_client import MinioClientWrapper
from schemas import (
    ChatRequest,
    ChatResponse,
    RecommendJobsRequest,
    RecommendJobsResponse,
    ResumeReviewRequest,
    ResumeReviewResponse,
    ChatHistory,
    UserProfile,
    UserProfileUpdate,
    UserProfileResponse,
    UploadResumeRequest,
    UploadResumeResponse,
)
from config import Settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


def get_settings():
    return Settings()


def get_chat_service(settings: Settings = Depends(get_settings)):
    return ChatService(settings)


def get_recommendation_service(settings: Settings = Depends(get_settings)):
    return RecommendationService(settings)


def get_resume_service(settings: Settings = Depends(get_settings)):
    return ResumeService(settings)


def get_memory_service(settings: Settings = Depends(get_settings)):
    return MemoryService(settings)


def get_mongodb_client(settings: Settings = Depends(get_settings)):
    return MongoDBClientWrapper(settings)


def get_minio_client(settings: Settings = Depends(get_settings)):
    return MinioClientWrapper(settings)


@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(
    request: ChatRequest, chat_service: ChatService = Depends(get_chat_service)
):
    """Process chat message and return response."""
    try:
        result = chat_service.process_message(request.user_id, request.message)
        logger.info(f"ChatService result: {result}")
        return ChatResponse(**result)
    except Exception as e:
        logger.exception("Error in chat endpoint")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/recommend_jobs", response_model=RecommendJobsResponse)
async def recommend_jobs_endpoint(
    request: RecommendJobsRequest,
    recommendation_service: RecommendationService = Depends(get_recommendation_service),
):
    """Get job recommendations based on user query."""
    try:
        recommendations = recommendation_service.recommend_jobs(
            user_id=request.user_id,
            query=request.query,
            experience_level=request.experience_level,
            location=request.location,
            limit=request.limit,
        )
        return RecommendJobsResponse(recommendations=recommendations)
    except Exception as e:
        logger.exception("Error in recommend jobs endpoint")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/resume_review", response_model=ResumeReviewResponse)
async def resume_review_endpoint(
    request: ResumeReviewRequest,
    resume_service: ResumeService = Depends(get_resume_service),
):
    """Review user's resume and provide feedback."""
    try:
        result = resume_service.review_resume(request.user_id, request.resume_text)
        return ResumeReviewResponse(**result)
    except Exception as e:
        logger.exception("Error in resume review endpoint")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/history/{user_id}", response_model=ChatHistory)
async def get_chat_history_endpoint(
    user_id: str, memory_service: MemoryService = Depends(get_memory_service)
):
    """Get chat history for a user."""
    try:
        messages = memory_service.get_chat_history(user_id)
        return ChatHistory(user_id=user_id, messages=messages)
    except Exception as e:
        logger.exception("Error getting chat history")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== User Profile Endpoints ====================


@router.get("/user_profile/{user_id}", response_model=UserProfileResponse)
async def get_user_profile_endpoint(
    user_id: str, mongodb_client: MongoDBClientWrapper = Depends(get_mongodb_client)
):
    """Get user profile by user_id."""
    try:
        profile = mongodb_client.get_user_profile(user_id)
        if not profile:
            return UserProfileResponse(user_id=user_id, profile_exists=False)

        return UserProfileResponse(
            user_id=profile.get("user_id", user_id),
            name=profile.get("name"),
            preferred_job=profile.get("preferred_job"),
            skills=profile.get("skills"),
            expected_salary=profile.get("expected_salary"),
            experience_level=profile.get("experience_level"),
            location=profile.get("location"),
            resume_filename=profile.get("resume_filename"),
            profile_exists=True,
        )
    except Exception as e:
        logger.exception("Error getting user profile")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/user_profile", response_model=UserProfileResponse)
async def create_user_profile_endpoint(
    request: UserProfile,
    mongodb_client: MongoDBClientWrapper = Depends(get_mongodb_client),
):
    """Create or update user profile."""
    try:
        profile_data = request.model_dump(exclude={"user_id"})
        success = mongodb_client.save_user_profile(request.user_id, profile_data)

        if not success:
            raise HTTPException(status_code=400, detail="Failed to save user profile")

        return UserProfileResponse(
            user_id=request.user_id, **profile_data, profile_exists=True
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error creating user profile")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/user_profile/{user_id}", response_model=UserProfileResponse)
async def update_user_profile_endpoint(
    user_id: str,
    request: UserProfileUpdate,
    mongodb_client: MongoDBClientWrapper = Depends(get_mongodb_client),
):
    """Update specific fields in user profile."""
    try:
        profile_data = request.model_dump(exclude_none=True)
        success = mongodb_client.update_user_profile(user_id, profile_data)

        if not success:
            raise HTTPException(status_code=400, detail="Failed to update user profile")

        # Get updated profile
        profile = mongodb_client.get_user_profile(user_id)

        return UserProfileResponse(
            user_id=user_id,
            name=profile.get("name") if profile else None,
            preferred_job=profile.get("preferred_job") if profile else None,
            skills=profile.get("skills") if profile else None,
            expected_salary=profile.get("expected_salary") if profile else None,
            experience_level=profile.get("experience_level") if profile else None,
            location=profile.get("location") if profile else None,
            resume_filename=profile.get("resume_filename") if profile else None,
            profile_exists=True,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error updating user profile")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/user_profile/{user_id}")
async def delete_user_profile_endpoint(
    user_id: str, mongodb_client: MongoDBClientWrapper = Depends(get_mongodb_client)
):
    """Delete user profile."""
    try:
        success = mongodb_client.delete_user_profile(user_id)
        if not success:
            raise HTTPException(status_code=404, detail="User profile not found")
        return {"message": "User profile deleted successfully", "user_id": user_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error deleting user profile")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== Resume Upload Endpoint ====================


@router.post("/upload_resume", response_model=UploadResumeResponse)
async def upload_resume_endpoint(
    user_id: str = Form(...),
    file: UploadFile = File(...),
    mongodb_client: MongoDBClientWrapper = Depends(get_mongodb_client),
    minio_client: MinioClientWrapper = Depends(get_minio_client),
):
    """Upload user resume to MinIO and save reference in MongoDB."""
    try:
        # Read file content
        file_content = await file.read()

        # Upload to MinIO
        object_name = minio_client.upload_resume(
            user_id=user_id, file_data=file_content, filename=file.filename
        )

        if not object_name:
            raise HTTPException(
                status_code=400, detail="Failed to upload resume to storage"
            )

        # Update user profile with resume info
        profile_data = {
            "resume_filename": file.filename,
            "resume_object_name": object_name,
        }
        mongodb_client.update_user_profile(user_id, profile_data)

        return UploadResumeResponse(
            user_id=user_id,
            filename=file.filename,
            object_name=object_name,
            success=True,
            message="Resume uploaded successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error uploading resume")
        raise HTTPException(status_code=500, detail="Internal server error")
