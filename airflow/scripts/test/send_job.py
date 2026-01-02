import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.sender import send_job_alerts
from dotenv import load_dotenv

load_dotenv()

list_job = [
    {
        "company":"Be Group",
        "descriptions":"Implementation and deployment of machine learning models, including development of APIs and ML-related backend systems. Model monitoring and iterative improvements such as retraining pipelines, feature engineering and performance optimization. Ongoing development of ML platform assets such as feature store, model management, ML ops workflows. Maintenance and improvement of SLAs and engineering best practices in ML use cases. Documentation, knowledge sharing and mentoring of junior members.",
        "location":"Ho Chi Minh",
        "logo":"https://itviec.com/rails/active_storage/representations/proxy/eyJfcmFpbHMiOnsiZGF0YSI6NDE2NjA3NywicHVyIjoiYmxvYl9pZCJ9fQ==--3c66c0a96aff1a9a173af415bffadee6f87ab4ca/eyJfcmFpbHMiOnsiZGF0YSI6eyJmb3JtYXQiOiJwbmciLCJyZXNpemVfdG9fZml0IjpbMTAwLDEwMF19LCJwdXIiOiJ2YXJpYXRpb24ifX0=--b5e297002dbdc2208aa0a6d439f098e2570e9b6e/images.png",
        "mode":"At office",
        "requirements":"Bachelor's degree in computer science, software engineering, mathematics or other numerical disciplines. Postgraduate degree preferred. 3+ years of industry experience with machine learning use cases. Proficiency in big data technology and feature engineering. Proficiency in implementation and deployment of common machine learning models and infrastructure. Clear communication, initiative and a strong sense of ownership. Strong mathematics or data science is an advantage. Strong software engineering or data engineering is an advantage. Experience with deep learning, NLP or LLM / VLM is an advantage.",
        "tags":"Machine Learning, Python, Data modeling, Data Science, Big Data, MLOps",
        "title":"Senior Machine Learning Engineer",
        "url":"https://itviec.com/it-jobs/senior-machine-learning-engineer-be-group-2146"
    },
    {
        "company":"Be Group",
        "descriptions":"Implementation and deployment of machine learning models, including development of APIs and ML-related backend systems. Model monitoring and iterative improvements such as retraining pipelines, feature engineering and performance optimization. Ongoing development of ML platform assets such as feature store, model management, ML ops workflows. Maintenance and improvement of SLAs and engineering best practices in ML use cases. Documentation, knowledge sharing and mentoring of junior members.",
        "location":"Ho Chi Minh",
        "logo":"https://itviec.com/rails/active_storage/representations/proxy/eyJfcmFpbHMiOnsiZGF0YSI6NDE2NjA3NywicHVyIjoiYmxvYl9pZCJ9fQ==--3c66c0a96aff1a9a173af415bffadee6f87ab4ca/eyJfcmFpbHMiOnsiZGF0YSI6eyJmb3JtYXQiOiJwbmciLCJyZXNpemVfdG9fZml0IjpbMTAwLDEwMF19LCJwdXIiOiJ2YXJpYXRpb24ifX0=--b5e297002dbdc2208aa0a6d439f098e2570e9b6e/images.png",
        "mode":"At office",
        "requirements":"Bachelor's degree in computer science, software engineering, mathematics or other numerical disciplines. Postgraduate degree preferred. 3+ years of industry experience with machine learning use cases. Proficiency in big data technology and feature engineering. Proficiency in implementation and deployment of common machine learning models and infrastructure. Clear communication, initiative and a strong sense of ownership. Strong mathematics or data science is an advantage. Strong software engineering or data engineering is an advantage. Experience with deep learning, NLP or LLM / VLM is an advantage.",
        "tags":"Machine Learning, Python, Data modeling, Data Science, Big Data, MLOps",
        "title":"Senior Machine Learning Engineer",
        "url":"https://itviec.com/it-jobs/senior-machine-learning-engineer-be-group-2146"
    }
]

send_job_alerts(jobs=list_job, token=os.getenv("DISCORD_TOKEN"), channel_id=int(os.getenv("DISCORD_CHANNEL_ID")))