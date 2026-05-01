from pydantic import BaseModel
from typing import Optional, List


class ChatRequest(BaseModel):
    user_id: str
    message: str


class ChatResponse(BaseModel):
    response: str
    intent: str


class Message(BaseModel):
    role: str
    content: str


class ChatHistory(BaseModel):
    user_id: str
    messages: List[Message]


class JobRecommendation(BaseModel):
    job_id: str
    job_title: str
    company_name: str
    job_location: str
    salary: Optional[str] = None
    url: str
    score: float


class RecommendJobsRequest(BaseModel):
    user_id: str
    query: str
    experience_level: Optional[str] = None
    location: Optional[str] = None
    min_salary: Optional[int] = None
    limit: int = 5


class RecommendJobsResponse(BaseModel):
    recommendations: List[JobRecommendation]


class ResumeReviewRequest(BaseModel):
    user_id: str
    resume_text: str


class ResumeReviewResponse(BaseModel):
    feedback: str
    suggestions: List[str]


# ==================== User Profile Schemas ====================


class UserProfile(BaseModel):
    """User profile information for job searching."""

    user_id: str
    name: Optional[str] = None
    preferred_job: Optional[str] = None
    skills: Optional[List[str]] = None
    expected_salary: Optional[str] = None
    experience_level: Optional[str] = None
    location: Optional[str] = None
    resume_filename: Optional[str] = None
    resume_object_name: Optional[str] = None


class UserProfileUpdate(BaseModel):
    """Schema for updating user profile fields."""

    name: Optional[str] = None
    preferred_job: Optional[str] = None
    skills: Optional[List[str]] = None
    expected_salary: Optional[str] = None
    experience_level: Optional[str] = None
    location: Optional[str] = None


class UserProfileResponse(BaseModel):
    """Response schema for user profile."""

    user_id: str
    name: Optional[str] = None
    preferred_job: Optional[str] = None
    skills: Optional[List[str]] = None
    expected_salary: Optional[str] = None
    experience_level: Optional[str] = None
    location: Optional[str] = None
    resume_filename: Optional[str] = None
    profile_exists: bool


class UploadResumeRequest(BaseModel):
    """Request schema for uploading resume."""

    user_id: str


class UploadResumeResponse(BaseModel):
    """Response schema for resume upload."""

    user_id: str
    filename: str
    object_name: str
    success: bool
    message: str
