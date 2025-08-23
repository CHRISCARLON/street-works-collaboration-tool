import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from backend.middleware.security import security_middleware
from backend.api.routes import projects, complex_metrics, metrics
from backend.database.init_db import create_database_and_extensions, create_tables
from loguru import logger

logger.remove()
logger.add(
    sys.stdout,
    level="DEBUG",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    colorize=True,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application lifespan events"""
    try:
        logger.info("Initialising database...")
        create_database_and_extensions()
        create_tables()
        logger.info("Database initialisation complete!")
    except Exception as e:
        logger.error(f"Database initialisation failed: {e}")
    yield
    logger.info("Application shutting down...")


app = FastAPI(
    title="Collaboration Tool API",
    description="API to manage and assess collaborative street work projects",
    version="0.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def apply_security_middleware(request, call_next):
    return await security_middleware(request, call_next)


app.include_router(projects.router, prefix="/projects", tags=["Projects"])
app.include_router(metrics.router, prefix="/phase-1/metrics", tags=["MetricsPhase1"])
app.include_router(
    complex_metrics.router, prefix="/phase-2/metrics", tags=["MetricsPhase2"]
)


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the API is running"""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "message": "API is running successfully",
            "version": "0.1.1",
        },
    )


@app.get("/")
async def root():
    """Root endpoint with basic API information"""
    return {
        "message": "Welcome to Collaboration Tool API",
        "health_check": "/health",
        "docs": "/docs",
        "projects": {
            "create": "/projects/create",
        },
        "metrics": {
            "bus_network": "/metrics/bus-network/{project_id}",
            "road_network": "/metrics/road-network/{project_id}",
            "asset_network": "/metrics/asset-network/{project_id}",
        },
        "complex_metrics": {
            "wellbeing": "/complex-metrics/wellbeing/{project_id}",
        },
    }
