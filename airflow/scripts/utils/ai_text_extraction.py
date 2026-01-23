import os
import json
import requests
import pandas as pd
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

SYSTEM_PROMPT = """
You are a job description information extraction engine.
Rules:
- Extract only information explicitly present in the input text.
- For each field, output only text **directly copied or clearly normalized** from the job text.
- Do NOT add any information not in the input text.
- All fields must be single string values (comma-separated if multiple).
- If a field cannot be found, output an empty string "".
- Avoid placeholders like "...", "etc.", or generic guesses.
"""

SCHEMA_PROMPT = """
Extract the job information into the following JSON schema exactly.
Every output field must be a STRING (comma-separated if needed) or "" if not found.

Example:
Job Text:
\"We require 3+ years of Python, strong SQL skills, and a Bachelor's degree in Computer Science.\"

Output:
{
  "job_family": "",
  "job_functions": "",
  "responsibilities": "",
  "technical_skills": "Python, SQL",
  "programming_languages": "Python, SQL",
  "data_skills": "SQL",
  "ml_ai_skills": "",
  "bi_analytics_skills": "",
  "etl_tools": "",
  "big_data_technologies": "",
  "cloud_platforms": "",
  "cloud_services": "",
  "mlops_devops_skills": "",
  "data_architecture_components": "",
  "business_domains": "",
  "industry_domain": "",
  "ml_use_cases": "",
  "seniority_level": "3+ years",
  "education_level": "Bachelor's degree in Computer Science",
  "certifications": "",
  "soft_skills": "",
  "management_skills": "",
  "language_requirements": "",
  "nice_to_have": ""
}

Now extract:
"""

class AITextExtraction:
    def __init__(self):
        self.OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
        # Better free model options:
        # Try Llama 3.3 70B Instruct free or DeepSeek R1 free
        self.MODEL_NAME = os.getenv("OPENROUTER_MODEL")
        self.API_KEY = os.getenv("OPENROUTER_API_KEY")

    def call_openrouter(self, prompt: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.API_KEY}",
            "Content-Type": "application/json; charset=utf-8"
        }

        payload = {
            "model": self.MODEL_NAME,
            "temperature": 0.0,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ]
        }

        response = requests.post(
            self.OPENROUTER_API_URL,
            headers=headers,
            json=payload,
            timeout=120
        )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        return json.loads(content)

    def extract_job(self, row: pd.Series) -> Dict[str, Any]:
        combined_text = f"""
            DESCRIPTION:
            {row.get('descriptions', '')}

            REQUIREMENTS:
            {row.get('requirements', '')}

            EXPERIENCE:
            {row.get('experiences', '')}
            """
        prompt = f"""
            {SCHEMA_PROMPT}

            Job Text:
            \"\"\"{combined_text}\"\"\"
            """
        return self.call_openrouter(prompt)

    def extract_dataframe(self, data) -> list[dict]:
        bronze_df = pd.DataFrame(data)
        results = []
        for _, row in bronze_df.iterrows():
            try:
                extracted = self.extract_job(row)
                results.append(extracted)
            except Exception as e:
                results.append({"error": str(e), "error_type": type(e).__name__})
        return results
