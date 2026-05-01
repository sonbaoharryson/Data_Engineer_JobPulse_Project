import requests
import logging
from typing import List, Optional, Literal
from pathlib import Path
from config import Settings

IntentType = Literal["job_search", "resume_review", "general_chat"]
logger = logging.getLogger(__name__)

PROMPT_DIR = Path(__file__).parent / "prompt"


class OllamaClient:
    def __init__(self, settings: Settings):
        self.base_url = f"http://{settings.OLLAMA_HOST}:{settings.OLLAMA_PORT}"
        self.embed_model = settings.OLLAMA_EMBED_MODEL
        self.generate_model = settings.OLLAMA_GENERATE_MODEL

    @staticmethod
    def _load_prompt(prompt_filename: str) -> str:
        """Load prompt template from .md file."""
        prompt_path = PROMPT_DIR / prompt_filename
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"Prompt file not found: {prompt_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading prompt from {prompt_path}: {e}")
            raise

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text using Ollama."""
        try:
            response = requests.post(
                f"{self.base_url}/api/embed",
                json={"model": self.embed_model, "input": text},
                timeout=300,
            )
            response.raise_for_status()
            embeddings = response.json().get("embeddings", [])
            return embeddings[0] if embeddings else None
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None

    def intent_classification(self, message: str):
        prompt_template = self._load_prompt("classify_intent.md")
        prompt = prompt_template.replace("{message}", message)
        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.generate_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1},
                },
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response").strip()
        except requests.RequestException as e:
            logger.error(f"Error classifying intent: {e}")

    def text_generation(self, message: str, intent: str) -> str:
        """Simple rule-based intent classification."""

        message_lower = message.lower()

        # Load prompt template and substitute message
        if intent == "general_chat":
            prompt_template = self._load_prompt("general_chat_limit.md")
        elif intent == "greeting":
            prompt_template = self._load_prompt("greeting_user.md")
        elif intent == "job_search":
            prompt_template = self._load_prompt("job_search_intent.md")
        elif intent == "personal_information":
            prompt_template = self._load_prompt("personal_information_intent.md")

        prompt = prompt_template.replace("{message}", message_lower)

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.generate_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.2},
                },
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response").strip()
        except requests.RequestException as e:
            logger.error(f"Error classifying intent: {e}")

    def generate_text(self, context: Optional[str] = None) -> str:
        """Generate text response using Ollama."""
        try:
            response = requests.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": self.generate_model,
                    "messages": context,
                    "stream": True,
                    "options": {"temperature": 0.1},
                },
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()

            if "message" not in data:
                logger.error(f"Invalid response: {data}")
                return ""

            return data["message"]["content"]
        except Exception as e:
            logger.error(f"Error generating text: {e}")
            return "Sorry, I encountered an error generating a response."

    def text_generation_with_history(
        self, message: str, intent: str, history: str
    ) -> str:
        """Generate text with conversation history context."""
        message_lower = message.lower()

        # Load prompt template based on intent
        if intent == "personal_information":
            prompt_template = self._load_prompt("personal_information_intent.md")
            prompt = prompt_template.replace("{message}", message_lower).replace(
                "{history}", history
            )
        else:
            # Fallback to regular text_generation
            return self.text_generation(message, intent)

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.generate_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1},
                },
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "").strip()
        except requests.RequestException as e:
            logger.error(f"Error generating text with history: {e}")
            return "Sorry, I encountered an error generating a response."
