from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

logger = logging.getLogger(__name__)

# Create FastAPI app instance
app = FastAPI(
    title="Open Data ETL API",
    description="API for accessing cleaned datasets from  open data portals",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Run on application startup."""
    logger.info("Starting Open Data ETL API")


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown."""
    logger.info("Shutting down Open Data ETL API")


@app.get("/")
async def root():
    """Root endpoint - API information."""
    return {
        "message": "Open Data ETL API",
        "description": "Access cleaned and normalized datasets from CKAN portals",
        "version": "0.1.0",
        "docs": "/docs",
        "available_datasets": [
            "/api/v1/building-permits",
            # Future datasets will be added here dynamically
        ]
    }