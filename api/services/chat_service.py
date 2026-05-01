import logging
from typing import List, Dict, Optional, Any
from clients.ollama_client import OllamaClient
from clients.mongodb_client import MongoDBClientWrapper
from services.memory_service import MemoryService
from services.recommendation_service import RecommendationService
from services.resume_service import ResumeService
from config import Settings

logger = logging.getLogger(__name__)

# Prompt for asking user information
USER_INFO_REQUEST_PROMPT = """You are a job search assistant. The user wants to search for jobs but we don't have their profile information yet.

Please ask the user for the following information in a friendly manner:
1. Their name
2. What job position they're looking for (preferred job)
3. Their skills
4. Expected salary range
5. Years of experience level (e.g., junior, mid-level, senior)
6. Preferred work location

Format your response as a friendly request for this information."""


class ChatService:
    def __init__(self, settings: Settings):
        self.ollama_client = OllamaClient(settings)
        self.memory_service = MemoryService(settings)
        self.recommendation_service = RecommendationService(settings)
        self.resume_service = ResumeService(settings)
        self.mongodb_client = MongoDBClientWrapper(settings)

    def process_message(self, user_id: str, message: str) -> Dict[str, str]:
        """Process user message and return response."""
        try:
            # Get chat history first (for all intents)
            history = self.memory_service.get_chat_history(user_id)
            context = self._format_history(history)

            # Classify intent
            intent = self.ollama_client.intent_classification(message)

            if intent == "greeting":
                response = self.ollama_client.text_generation(message, intent)
                return {"response": response, "intent": intent}

            elif intent == "general_chat":
                response = self.ollama_client.text_generation(message, intent)
                return {"response": response, "intent": intent}

            elif intent == "job_search":
                user_extracted_info = self.ollama_client.text_generation(
                    message, intent
                )
                response = self._handle_job_search(user_id, user_extracted_info)
                return {"response": response, "intent": intent}

            # elif intent == "resume_review":
            #     response = self._handle_resume_review(user_id, message, context)

            elif intent == "personal_information":
                # Use formatted history context for personal information queries
                response = self.ollama_client.text_generation_with_history(
                    message, intent, context
                )
                return {"response": response, "intent": intent}

            self.memory_service.add_message(user_id, "user", message)
            self.memory_service.add_message(user_id, "assistant", response)
            return {"response": response, "intent": intent}

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return {
                "response": "Sorry, I encountered an error processing your message.",
                "intent": "error",
            }

    def _format_history(self, history: List[Dict[str, str]]) -> str:
        """Format chat history for context."""
        if not history:
            return ""

        formatted = []
        for msg in history:  # Last 5 messages for context
            role = msg.get("role", "")
            content = msg.get("content", "")
            formatted.append(f"{role.title()}: {content}")

        return "\n".join(formatted)

    def _handle_job_search(self, user_id: str, message: str) -> str:
        """Handle job search intent - checks for user profile first."""
        try:
            # Check if user profile exists in MongoDB
            user_profile = self.mongodb_client.get_user_profile(user_id)

            if not user_profile:
                # No profile exists - ask user for information
                response = self.ollama_client.text_generation(
                    message=USER_INFO_REQUEST_PROMPT, intent="job_search"
                )
                return response

            # User profile exists - use it for job search
            # Build query from profile + user message
            profile_query = self._build_profile_query(user_profile, message)

            recommendations = self.recommendation_service.recommend_jobs(
                query=profile_query, limit=5
            )

            if not recommendations:
                return "I couldn't find any jobs matching your criteria. Could you provide more details about what you're looking for?"

            # Format response
            response = (
                f"I found {len(recommendations)} job recommendations for you:\n\n"
            )
            for i, rec in enumerate(recommendations, 1):
                response += f"{i}. {rec.get('job_title', 'Job')}: {rec.get('company_name', '')}\n"
                response += f"   Location: {rec.get('job_location', 'N/A')}\n"
                response += f"   Score: {rec.get('score', 0):.2f}\n\n"

            final_response = self.ollama_client.text_generation(
                message=response, intent="job_search"
            )
            return final_response
        except Exception as e:
            logger.error(f"Error handling job search: {e}")
            return "Sorry, I had trouble searching for jobs. Please try later."

    def _build_profile_query(self, profile: Dict[str, Any], user_message: str) -> str:
        """Build a comprehensive query from user profile and message."""
        query_parts = []

        if profile.get("name"):
            query_parts.append(f"User: {profile['name']}")

        if profile.get("preferred_job"):
            query_parts.append(f"Looking for: {profile['preferred_job']}")

        if profile.get("skills"):
            skills_str = ", ".join(profile["skills"])
            query_parts.append(f"Skills: {skills_str}")

        if profile.get("expected_salary"):
            query_parts.append(f"Expected salary: {profile['expected_salary']}")

        if profile.get("experience_level"):
            query_parts.append(f"Experience: {profile['experience_level']}")

        if profile.get("location"):
            query_parts.append(f"Location: {profile['location']}")

        # Add user's current message
        if user_message:
            query_parts.append(f"Additional request: {user_message}")

        return " | ".join(query_parts)

    def save_user_profile(self, user_id: str, profile_data: Dict[str, Any]) -> bool:
        """Save user profile to MongoDB."""
        return self.mongodb_client.save_user_profile(user_id, profile_data)

    def get_user_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user profile from MongoDB."""
        return self.mongodb_client.get_user_profile(user_id)

    def user_profile_exists(self, user_id: str) -> bool:
        """Check if user profile exists."""
        return self.mongodb_client.user_profile_exists(user_id)

    def _handle_resume_review(self, user_id: str, message: str, context: str) -> str:
        """Handle resume review intent."""
        # For resume review, we need the resume text
        # This is a simplified version - in practice, you'd extract resume from message or database
        return "I'd be happy to help review your resume! Please share your resume text, and I'll provide feedback and suggestions for improvement."
