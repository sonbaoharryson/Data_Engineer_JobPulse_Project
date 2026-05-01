import logging
from typing import List, Dict
from clients.mongodb_client import MongoDBClientWrapper
from config import Settings

logger = logging.getLogger(__name__)


class MemoryService:
    def __init__(self, settings: Settings):
        self.mongodb_client = MongoDBClientWrapper(settings)

    def get_chat_history(self, user_id: str) -> List[Dict[str, str]]:
        """Get chat history for a user."""
        return self.mongodb_client.get_chat_history(user_id)

    def add_message(self, user_id: str, role: str, content: str):
        """Add a message to user's memory."""
        self.mongodb_client.add_message(user_id, role, content)

    def clear_memory(self, user_id: str):
        """Clear user's chat history."""
        self.mongodb_client.clear_history(user_id)
