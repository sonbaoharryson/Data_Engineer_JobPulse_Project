import json
import os


def load_crawl_sources(file_name: str):
    """Load the list of web sources from the JSON configuration file."""
    file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f"{file_name}"
    )
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Configuration file not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
