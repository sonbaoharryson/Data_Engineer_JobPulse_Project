from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import Settings
import os

# Initialize FastAPI app
app = FastAPI(
    title="Job Recommendation API",
    description="AI-powered job recommendation and resume improvement API",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Load settings
settings = Settings()

# Configure basic logging
import logging
import sys

# Create logger with explicit stdout handler
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("api")

# Global exception handler to ensure unhandled exceptions are logged
from fastapi.responses import JSONResponse


async def _internal_exception_handler(request, exc):
    logger.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


app.add_exception_handler(Exception, _internal_exception_handler)


# Root endpoint
@app.get("/")
async def root():
    return {"message": "Job Recommendation API is running"}


# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# Include routers (use API version prefix from settings)
from routes import chat, jobs

app.include_router(chat.router, prefix=settings.API_V1_STR, tags=["chat"])
app.include_router(jobs.router, prefix=settings.API_V1_STR, tags=["jobs"])


def main(host: str | None = None, port: int | None = None, reload: bool = False):
    import uvicorn

    _host = host or "0.0.0.0"
    _port = int(port or "8000")
    uvicorn.run("app:app", host=_host, port=_port, reload=reload)


if __name__ == "__main__":
    main()
