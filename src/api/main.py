from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from api.routes import building_permits_router,download_router
from datetime import datetime
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

# Include routers
app.include_router(building_permits_router)
app.include_router(download_router)

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


@app.get("/health")
async def health_check():
    """
    Health check endpoint - verifies API and database connectivity.
    
    Returns:
        dict: Health status with timestamp and database connection status
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "api_version": "0.1.0",
        "checks": {}
    }
    
    # Check database connection
    try:
        with DatabaseConnection.get_session() as session:
            # Simple query to test connection
            session.execute("SELECT 1")
            health_status["checks"]["database"] = {
                "status": "connected",
                "message": "Database connection successful"
            }
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["checks"]["database"] = {
            "status": "error",
            "message": f"Database connection failed: {str(e)}"
        }
        logger.error(f"Health check failed - Database error: {str(e)}")
    
    return health_status