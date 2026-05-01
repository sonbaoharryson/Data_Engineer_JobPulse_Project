# Job Recommendation API

A FastAPI backend service for AI-powered job recommendation and resume improvement, designed to work with a Discord bot.

## Features

- **Intelligent Chat**: AI-powered conversations with intent classification
- **Job Recommendations**: Vector-based job search using Qdrant and Ollama embeddings
- **Resume Review**: AI-powered resume feedback and improvement suggestions
- **Memory Management**: Persistent chat history using Redis
- **Clean Architecture**: Modular design with clear separation of concerns

## Architecture

```
api/
├── app.py                 # Main FastAPI app with CORS and error handling
├── config.py             # Settings and configuration
├── schemas.py            # Pydantic models for all endpoints
├── requirements.txt      # Dependencies
├── Dockerfile           # Production container
├── README.md            # This documentation
├── clients/             # External service wrappers
│   ├── ollama_client.py     # HTTP client for embeddings + generation
│   ├── qdrant_client.py     # Vector search wrapper
│   └── redis_client.py      # Memory management
├── services/            # Business logic layer
│   ├── chat_service.py      # Main chat processing with intent classification
│   ├── recommendation_service.py  # Job search logic
│   ├── resume_service.py    # Resume review logic
│   └── memory_service.py    # Chat history management
├── routes/              # API endpoints
│   └── chat.py          # All chat and AI endpoints
└── utils/               # Utilities
    └── intent_classification.py  # Simple rule-based intent detection
```

## API Endpoints

### POST `/api/v1/chat`
Process a chat message and return AI response with intent detection.

**Request:**
```json
{
  "user_id": "string",
  "message": "string"
}
```

**Response:**
```json
{
  "response": "string",
  "intent": "job_search|resume_review|general_chat"
}
```

### POST `/api/v1/recommend_jobs`
Get job recommendations based on user query with optional filters.

**Request:**
```json
{
  "user_id": "string",
  "query": "software engineer",
  "experience_level": "mid",
  "location": "hanoi",
  "limit": 5
}
```

**Response:**
```json
{
  "recommendations": [
    {
      "job_id": "string",
      "job_title": "string",
      "company_name": "string",
      "job_location": "string",
      "salary": "string",
      "url": "string",
      "score": 0.95
    }
  ]
}
```

### POST `/api/v1/resume_review`
Review a resume and provide AI-powered feedback.

**Request:**
```json
{
  "user_id": "string",
  "resume_text": "string"
}
```

**Response:**
```json
{
  "feedback": "string",
  "suggestions": ["string"]
}
```

### GET `/api/v1/history/{user_id}`
Get chat history for a user.

**Response:**
```json
{
  "user_id": "string",
  "messages": [
    {
      "role": "user|assistant",
      "content": "string"
    }
  ]
}
```

## Configuration

Set these environment variables in `.env`:

```bash
# API
API_HOST=0.0.0.0
API_PORT=8000

# Ollama (for embeddings and text generation)
OLLAMA_HOST=localhost
OLLAMA_PORT=11434
OLLAMA_EMBED_MODEL=mxbai-embed-large
OLLAMA_GENERATE_MODEL=gemma:2b

# Qdrant (vector database)
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=jobs

# Redis (chat memory)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Memory settings
MAX_MEMORY_MESSAGES=10
```

## Running Locally

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Start the API:**
```bash
python app.py
```

Or with uvicorn:
```bash
uvicorn app:app --reload
```

3. **Access API documentation:**
Visit `http://localhost:8000/docs` for interactive Swagger UI.

## Docker

Build and run with Docker:
```bash
docker build -t job-recommendation-api .
docker run -p 8000:8000 job-recommendation-api
```

## Integration with Discord Bot

The Discord bot calls the `/api/v1/chat` endpoint. Update the bot's environment:

```bash
API_BASE_URL=http://localhost:8000/api/v1
```

## Dependencies

- **FastAPI**: Web framework
- **Qdrant Client**: Vector database for job search
- **Redis**: In-memory store for chat history
- **Requests**: HTTP client for Ollama API
- **Pydantic**: Data validation and serialization

## Development

The API uses dependency injection for clean testing and modularity. Services are initialized with configuration and can be easily mocked for testing.

### Intent Classification

The system automatically detects user intent:
- **job_search**: Messages containing job-related keywords
- **resume_review**: Messages about resumes, CVs, or feedback
- **general_chat**: All other conversations

### Memory Management

Chat history is stored in Redis with automatic cleanup:
- Last 10 messages per user
- 24-hour expiration
- User-specific conversation context
```json
{
  "response": "string",
  "intent": "job_search|resume_review|general_chat"
}
```

### POST /api/v1/recommend_jobs
Get job recommendations based on user query.

**Request:**
```json
{
  "user_id": "string",
  "query": "software engineer",
  "experience_level": "mid",
  "location": "hanoi",
  "limit": 5
}
```

### POST /api/v1/resume_review
Review a resume and provide feedback.

**Request:**
```json
{
  "user_id": "string",
  "resume_text": "string"
}
```

### GET /api/v1/history/{user_id}
Get chat history for a user.

## Configuration

Set the following environment variables:

```bash
# API
API_HOST=0.0.0.0
API_PORT=8000

# Ollama
OLLAMA_HOST=localhost
OLLAMA_PORT=11434
OLLAMA_EMBED_MODEL=mxbai-embed-large
OLLAMA_GENERATE_MODEL=gemma:2b

# Qdrant
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_COLLECTION=jobs

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Memory
MAX_MEMORY_MESSAGES=10
```

## Running Locally

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start the API:
```bash
python app.py
```

Or with uvicorn:
```bash
uvicorn app:app --reload
```

## Docker

Build and run with Docker:
```bash
docker build -t job-api .
docker run -p 8000:8000 job-api
```

## Integration with Discord Bot

The Discord bot calls the `/chat` endpoint with user messages. Update the bot's `API_BASE_URL` environment variable to point to this API.

## Dependencies

- FastAPI: Web framework
- Qdrant Client: Vector database
- Redis: In-memory data store
- Requests: HTTP client for Ollama
- Pydantic: Data validation
- SQLAlchemy: Database ORM (for job data)

## Development

The API uses dependency injection for clean testing and modularity. Services are initialized with configuration and can be easily mocked for testing.