from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from config import Settings
from database import get_db, engine
import models
import schemas
import services
import os

# Create database tables
models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title="Job API",
    description="API for job data from TopCV and ITViec",
    version="1.0.0"
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
logging.basicConfig(level=logging.INFO)
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
    return {"message": "API is running"}

# Include routers (use API version prefix from settings)
from routes import router
app.include_router(router, prefix=settings.API_V1_STR)

def main(host: str | None = None, port: int | None = None, reload: bool = False):
    import uvicorn
    _host = host or os.getenv("API_HOST", str(settings.API_HOST))
    _port = port or int(os.getenv("API_PORT", str(settings.API_PORT)))
    uvicorn.run("app:app", host=_host, port=_port, reload=reload)


if __name__ == "__main__":
    main()