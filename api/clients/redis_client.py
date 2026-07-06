import json
import logging
from typing import Any, Dict, List, Optional

from config import Settings

import redis

logger = logging.getLogger(__name__)


class RedisClientWrapper:
    def __init__(self, settings: Settings):
        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            decode_responses=True,
        )
        self.max_messages = settings.MAX_MEMORY_MESSAGES

    def get_chat_history(
        self, user_id: str, first_message: str = "Hello!"
    ) -> List[Dict[str, str]]:
        """Get chat history for a user."""
        try:
            key = f"chat:{user_id}"
            messages = self.client.lrange(key, 0, -1)
            if not messages:
                self.add_message(user_id, "user", first_message)
            return [json.loads(msg) for msg in messages]
        except Exception as e:
            logger.error(f"Error getting chat history: {e}")
            return []

    def add_message(self, user_id: str, role: str, content: str):
        """Add a message to user's chat history."""
        try:
            key = f"chat:{user_id}"
            message = {"role": role, "content": content}
            self.client.lpush(key, json.dumps(message))
            # Keep only the last N messages
            self.client.ltrim(key, 0, self.max_messages - 1)
            self.client.expire(key, 86400)
        except Exception as e:
            logger.error(f"Error adding message: {e}")

    def clear_history(self, user_id: str):
        """Clear chat history for a user."""
        try:
            key = f"chat:{user_id}"
            self.client.delete(key)
        except Exception as e:
            logger.error(f"Error clearing history: {e}")
