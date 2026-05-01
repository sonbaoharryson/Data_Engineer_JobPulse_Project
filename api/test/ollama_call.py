from clients.ollama_client import OllamaClient
from config import Settings

settings = Settings()
client = OllamaClient(settings)
print("Testing embedding...")
embedding = client.generate_embedding("Hello world")
print("Embedding result:", embedding is not None, len(embedding) if embedding else 0)
print("Testing intent classification...")
intent = client.classify_intent("Find me a job")
print("Intent:", intent)
