import os
import requests
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class LLMModel:
    def __init__(self, model_name:str="qwen/qwen2.5-vl-32b-instruct:free"):
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.model_name = model_name
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"   
        }

    def __call__(self, prompt: str = None, system_message: str = None, **kwargs):
        messages = []
        
        if system_message:
            messages.append({"role": "system", "content": system_message})
        
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "model": self.model_name,
            "messages": messages,
            **kwargs  # Additional params like temperature, max_tokens etc.
        }
        response = requests.post(
            url=self.base_url,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code == 200:
            return response
        raise Exception(f"API Error: {response.status_code} - {response.text}")