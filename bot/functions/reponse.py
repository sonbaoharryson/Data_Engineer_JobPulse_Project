import requests
import os
import logging
import sys

# Configure logging explicitly
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

API_BASE_URL = "http://api_layer:8000/api/v1"


async def generate_response(user_id: str, user_message: str):
    """Generate response using the FastAPI backend."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/chat",
            json={"user_id": user_id, "message": user_message},
            timeout=30,
        )
        logger.info(f"API call status: {response.status_code}")
        logger.info(f"API response: {response.text[:500]}")
        response.raise_for_status()
        data = response.json()
        logger.info(f"API Response: {data}")
        if response.status_code != 200:
            return "API error"

        data = response.json()
        if "response" not in data:
            return f"Invalid response: {data}"

        return data["response"]
    except Exception as e:
        logger.error(f"Error calling API: {e}")
        return "Sorry, I'm having trouble connecting to the server. Please try again later."
