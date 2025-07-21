import sys

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from backend.motherduck.database_pool import MotherDuckPool
from backend.metrics.strategies import Wellbeing, Bus, Network
from backend.middleware.security import security_middleware
from backend.schemas.schemas import (
    WellbeingResponse,
    TransportResponse,
    NetworkResponse,
)
from loguru import logger

logger.remove() 
logger.add(
    sys.stdout,
    level="DEBUG",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    colorize=True
)

app = FastAPI(
    title="Collaboration Tool API",
    description="A simple API to generate impact metrics for street works projects",
    version="0.1.0",
)


@app.middleware("http")
async def apply_security_middleware(request, call_next):
    return await security_middleware(request, call_next)


motherduck_pool = MotherDuckPool()
wellbeing_strategy = Wellbeing(motherduck_pool)
bus_strategy = Bus(motherduck_pool)
network_strategy = Network(motherduck_pool)


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the API is running"""
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "message": "API is running successfully",
            "version": "0.1.0",
        },
    )


@app.get("/")
async def root():
    """Root endpoint with basic API information"""
    return {
        "message": "Welcome to Collaboration Tool API",
        "health_check": "/health",
        "docs": "/docs",
        "calculate_wellbeing": "/calculate-wellbeing/{project_id}",
        "calculate_transport": "/calculate-transport/{project_id}",
        "get_naptan_buffer": "/get-naptan-buffer/{project_id}",
    }


@app.get("/calculate-wellbeing/{project_id}", response_model=WellbeingResponse)
async def calculate_wellbeing_impact(project_id: str):
    """
    Calculate wellbeing impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        WellbeingResponse with calculated wellbeing metrics
    """
    try:
        response = await wellbeing_strategy.calculate_impact(project_id)
        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating wellbeing impact for project {project_id}: {str(e)}",
        )


@app.get("/calculate-transport/{project_id}", response_model=TransportResponse)
async def calculate_transport_impact(project_id: str):
    """
    Calculate transport impact for a specific project ID using NaPTAN data

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        TransportResponse with calculated transport metrics including affected bus stops
    """
    try:
        response = await bus_strategy.calculate_impact(project_id)
        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating transport impact for project {project_id}: {str(e)}",
        )


@app.get("/calculate-network/{project_id}", response_model=NetworkResponse)
async def calculate_network_impact(project_id: str):
    """
    Calculate network impact for a specific project ID
    """
    try:
        response = await network_strategy.calculate_impact(project_id)
        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating network impact for project {project_id}: {str(e)}",
        )
