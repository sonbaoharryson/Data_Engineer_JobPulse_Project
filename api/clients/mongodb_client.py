import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from config import Settings

logger = logging.getLogger(__name__)


class MongoDBClientWrapper:
    def __init__(self, settings: Settings):
        try:
            # Build connection string
            if settings.MONGODB_USERNAME and settings.MONGODB_PASSWORD:
                connection_string = (
                    f"mongodb://{settings.MONGODB_USERNAME}:{settings.MONGODB_PASSWORD}"
                    f"@{settings.MONGODB_HOST}:{settings.MONGODB_PORT}/{settings.MONGODB_DB}"
                    f"?authSource=admin"
                )
            else:
                connection_string = f"mongodb://{settings.MONGODB_HOST}:{settings.MONGODB_PORT}/{settings.MONGODB_DB}"

            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Verify connection
            self.client.admin.command("ping")

            self.db = self.client[settings.MONGODB_DB]
            self.collection = self.db["chat_messages"]
            self.user_profile_collection = self.db["user_profiles"]
            self.max_messages = settings.MAX_MEMORY_MESSAGES

            # Create index on user_id for faster queries
            self.collection.create_index("user_id")
            self.user_profile_collection.create_index("user_id")
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise

    def get_chat_history(
        self, user_id: str, first_message: str = "Hello!"
    ) -> List[Dict[str, str]]:
        """Get chat history for a user."""
        try:
            # Find all messages for the user, sorted by timestamp (oldest first)
            messages = list(
                self.collection.find({"user_id": user_id}, sort=[("timestamp", 1)])
            )

            if not messages:
                # Initialize with first message if no history exists
                self.add_message(user_id, "system", first_message)
                return [{"role": "assistant", "content": first_message}]

            # Convert to the expected format (role and content only)
            return [
                {"role": msg.get("role", "user"), "content": msg.get("message", "")}
                for msg in messages
            ]
        except Exception as e:
            logger.error(f"Error getting chat history: {e}")
            return []

    def add_message(self, user_id: str, role: str, content: str):
        """Add a message to user's chat history."""
        try:
            message_doc = {
                "user_id": user_id,
                "role": role,
                "message": content,
                "timestamp": datetime.utcnow(),
            }
            self.collection.insert_one(message_doc)

            # Keep only the last N messages for each user
            # Get all documents for this user sorted by timestamp descending
            all_messages = list(
                self.collection.find({"user_id": user_id}, sort=[("timestamp", -1)])
            )

            # Delete messages beyond the max_messages limit
            if len(all_messages) > self.max_messages:
                messages_to_delete = all_messages[self.max_messages :]
                delete_ids = [msg["_id"] for msg in messages_to_delete]
                self.collection.delete_many({"_id": {"$in": delete_ids}})

        except Exception as e:
            logger.error(f"Error adding message: {e}")

    def clear_history(self, user_id: str):
        """Clear chat history for a user."""
        try:
            self.collection.delete_many({"user_id": user_id})
            logger.info(f"Cleared chat history for user: {user_id}")
        except Exception as e:
            logger.error(f"Error clearing history: {e}")

    def get_user_messages(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent messages for a user with full details."""
        try:
            messages = list(
                self.collection.find(
                    {"user_id": user_id}, sort=[("timestamp", -1)], limit=limit
                )
            )
            # Reverse to get chronological order
            return messages[::-1]
        except Exception as e:
            logger.error(f"Error getting user messages: {e}")
            return []

    def close(self):
        """Close MongoDB connection."""
        try:
            self.client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

    # ==================== User Profile Methods ====================

    def get_user_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user profile from database.

        Args:
            user_id: The user's ID

        Returns:
            User profile dict if found, None otherwise
        """
        try:
            profile = self.user_profile_collection.find_one({"user_id": user_id})
            if profile:
                # Remove MongoDB internal fields
                profile.pop("_id", None)
            return profile
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return None

    def save_user_profile(self, user_id: str, profile_data: Dict[str, Any]) -> bool:
        """Save or update user profile in database.

        Args:
            user_id: The user's ID
            profile_data: Dictionary containing user profile information
                         (name, preferred_job, skills, expected_salary, etc.)

        Returns:
            True if successful, False otherwise
        """
        try:
            profile_doc = {
                "user_id": user_id,
                **profile_data,
                "updated_at": datetime.utcnow(),
            }

            # Use upsert to insert if not exists, update if exists
            self.user_profile_collection.update_one(
                {"user_id": user_id},
                {
                    "$set": profile_doc,
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                },
                upsert=True,
            )
            logger.info(f"Saved user profile for user: {user_id}")
            return True
        except Exception as e:
            logger.error(f"Error saving user profile: {e}")
            return False

    def update_user_profile(self, user_id: str, profile_data: Dict[str, Any]) -> bool:
        """Update specific fields in user profile.

        Args:
            user_id: The user's ID
            profile_data: Dictionary containing fields to update

        Returns:
            True if successful, False otherwise
        """
        try:
            update_doc = {
                **profile_data,
                "updated_at": datetime.utcnow(),
            }
            result = self.user_profile_collection.update_one(
                {"user_id": user_id},
                {"$set": update_doc},
            )
            return result.modified_count > 0 or result.upserted_id is not None
        except Exception as e:
            logger.error(f"Error updating user profile: {e}")
            return False

    def delete_user_profile(self, user_id: str) -> bool:
        """Delete user profile from database.

        Args:
            user_id: The user's ID

        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.user_profile_collection.delete_one({"user_id": user_id})
            logger.info(f"Deleted user profile for user: {user_id}")
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Error deleting user profile: {e}")
            return False

    def user_profile_exists(self, user_id: str) -> bool:
        """Check if user profile exists in database.

        Args:
            user_id: The user's ID

        Returns:
            True if profile exists, False otherwise
        """
        try:
            count = self.user_profile_collection.count_documents(
                {"user_id": user_id}, limit=1
            )
            return count > 0
        except Exception as e:
            logger.error(f"Error checking user profile existence: {e}")
            return False
