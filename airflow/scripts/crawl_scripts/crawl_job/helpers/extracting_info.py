from typing import Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def _safe_text(element) -> Optional[str]:
    try:
        return element.get_text(strip=True)
    except Exception:
        logger.warning(f"Failed to extract text from {element}.")
        return None

def _safe_attr(element, attr: str) -> Optional[str]:
    try:
        return element[attr]
    except Exception:
        logger.warning(f"Failed to extract attribute '{attr}' from element.")
        return None

def _safe_find(parent, *args, **kwargs):
    try:
        return parent.find(*args, **kwargs)
    except Exception:
        logger.warning(f"Failed to find element with args: {args}, kwargs: {kwargs}.")
        return None