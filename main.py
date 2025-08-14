import sys

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from backend.duckdb_pool.database_pool import MotherDuckPool, DuckDBPool
from backend.metrics.strategies import Wellbeing, BusNetwork, RoadNetwork, AssetNetwork
from backend.middleware.security import security_middleware
from backend.schemas.schemas import (
    WellbeingResponse,
    TransportResponse,
    BusNetworkResponse,
    AssetResponse,
)
from loguru import logger

logger.remove()
logger.add(
    sys.stdout,
    level="DEBUG",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
    colorize=True,
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
duckdb_pool = DuckDBPool()
wellbeing_strategy = Wellbeing(motherduck_pool, duckdb_pool)
bus_network_strategy = BusNetwork(motherduck_pool, duckdb_pool)
road_network_strategy = RoadNetwork(motherduck_pool, duckdb_pool)
asset_network_strategy = AssetNetwork(motherduck_pool, duckdb_pool)


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
        "calculate_bus_network": "/calculate-bus-network/{project_id}",
        "calculate_road_network": "/calculate-road-network/{project_id}",
        "calculate_asset_network": "/calculate-asset-network/{project_id}",
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


@app.get("/calculate-bus-network/{project_id}", response_model=TransportResponse)
async def calculate_bus_network_impact(project_id: str):
    """
    Calculate bus network impact for a specific project ID using NaPTAN data

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        TransportResponse with calculated bus network metrics including affected bus stops
    """
    try:
        response = await bus_network_strategy.calculate_impact(project_id)
        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating bus network impact for project {project_id}: {str(e)}",
        )


@app.get("/calculate-road-network/{project_id}", response_model=BusNetworkResponse)
async def calculate_road_network_impact(project_id: str):
    """
    Calculate road network impact for a specific project ID
    """
    try:
        response = await road_network_strategy.calculate_impact(project_id)
        return response

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating road network impact for project {project_id}: {str(e)}",
        )


@app.get("/calculate-asset-network/{project_id}", response_model=AssetResponse)
async def calculate_asset_network_impact(project_id: str, zoom_level: str = "11"):
    """
    Calculate asset network impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")
        zoom_level: NUAR zoom level (default: "11")
    """
    try:
        response = await asset_network_strategy.calculate_impact(project_id, zoom_level)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating asset network impact for project {project_id}: {str(e)}",
        )
