import logging
from typing import Dict, List

from clients.ollama_client import OllamaClient
from config import Settings

logger = logging.getLogger(__name__)


class ResumeService:
    def __init__(self, settings: Settings):
        self.ollama_client = OllamaClient(settings)

    def review_resume(self, user_id: str, resume_text: str) -> Dict[str, any]:
        """Review and provide feedback on resume."""
        try:
            prompt = f"""
                You are an expert career counselor. Review the following resume and provide constructive feedback and suggestions for improvement.

                Resume:
                {resume_text}

                Please provide:
                1. Overall assessment
                2. Strengths
                3. Areas for improvement
                4. Specific suggestions

                Be helpful, professional, and encouraging.
            """

            feedback = self.ollama_client.generate_text(prompt)

            # Parse feedback into structured format (simplified)
            suggestions = self._extract_suggestions(feedback)

            return {"feedback": feedback, "suggestions": suggestions}

        except Exception as e:
            logger.error(f"Error reviewing resume: {e}")
            return {
                "feedback": "Sorry, I encountered an error reviewing your resume. Please try again.",
                "suggestions": [],
            }

    def _extract_suggestions(self, feedback: str) -> List[str]:
        """Extract suggestions from feedback text."""
        # Simple extraction - look for numbered or bulleted lists
        lines = feedback.split("\n")
        suggestions = []

        for line in lines:
            line = line.strip()
            if line.startswith(("1.", "2.", "3.", "4.", "5.", "-", "*")):
                suggestions.append(line)

        return suggestions[:5]  # Limit to 5 suggestions
