from fastapi import APIRouter, HTTPException, Depends
from backend.services.metrics import BusNetwork, RoadNetwork, AssetNetwork, WorkHistory
from backend.schemas.schemas import (
    TransportResponse,
    BusNetworkResponse,
    AssetResponse,
    WorkHistoryResponse
)
from backend.api.dependencies import (
    get_bus_network_service,
    get_road_network_service,
    get_asset_network_service,
    get_work_history_service,
)
from loguru import logger

router = APIRouter()


@router.get("/bus-network/{project_id}", response_model=TransportResponse)
async def calculate_bus_network_impact(
    project_id: str, bus_network_strategy: BusNetwork = Depends(get_bus_network_service)
):
    """
    Calculate bus network impact for a specific project ID using NaPTAN data

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        TransportResponse with calculated bus network metrics including affected bus stops
    """
    try:
        response = await bus_network_strategy.calculate_impact(project_id)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating bus network impact for project {project_id}: {str(e)}",
        )


@router.get("/road-network/{project_id}", response_model=BusNetworkResponse)
async def calculate_road_network_impact(
    project_id: str,
    road_network_strategy: RoadNetwork = Depends(get_road_network_service),
):
    """
    Calculate road network impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        BusNetworkResponse with calculated road network metrics
    """
    try:
        response = await road_network_strategy.calculate_impact(project_id)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating road network impact for project {project_id}: {str(e)}",
        )


@router.get("/asset-network/{project_id}", response_model=AssetResponse)
async def calculate_asset_network_impact(
    project_id: str,
    zoom_level: str = "11",
    asset_network_strategy: AssetNetwork = Depends(get_asset_network_service),
):
    """
    Calculate asset network impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")
        zoom_level: NUAR zoom level (default: "11")

    Returns:
        AssetResponse with calculated asset network metrics
    """
    try:
        response = await asset_network_strategy.calculate_impact(project_id, zoom_level)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating asset network impact for project {project_id}: {str(e)}",
        )


@router.get("/work-history/{project_id}", response_model=WorkHistoryResponse)
async def calculate_work_history_impact(
    project_id: str,
    work_history_strategy: WorkHistory = Depends(get_work_history_service),
):
    """
    Calculate work history impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        WorkHistoryResponse with completed works count from the last 6 months
    """
    try:
        response = await work_history_strategy.calculate_impact(project_id)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating work history impact for project {project_id}: {str(e)}",
        )
