import requests


def generate_embeddings(text: str):
    response = requests.post(
        "http://ollama:11434/api/embed",
        json={"model": "mxbai-embed-large", "input": text},
    )
    if len(response.json()["embeddings"]) > 0:
        return response.json()["embeddings"][0]
    else:
        return None
