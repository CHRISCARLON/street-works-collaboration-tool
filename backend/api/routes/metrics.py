from typing import Annotated
from fastapi import APIRouter, HTTPException, Depends
from backend.services.metrics import BusNetwork, RoadNetwork, AssetNetwork, WorkHistory, Households
from backend.schemas.schemas import (
    TransportResponse,
    BusNetworkResponse,
    AssetResponse,
    WorkHistoryResponse,
    HouseholdsResponse,
)
from backend.api.dependencies import (
    get_bus_network_service,
    get_road_network_service,
    get_asset_network_service,
    get_work_history_service,
    get_households_service,
)
from loguru import logger

router = APIRouter()

# TODO: add this annotated types as the dependencies to be injected
BusNetworkDep = Annotated[BusNetwork, Depends(get_bus_network_service)]
RoadNetworkDep = Annotated[RoadNetwork, Depends(get_road_network_service)]
AssetNetworkDep = Annotated[AssetNetwork, Depends(get_asset_network_service)]
WorkHistoryDep = Annotated[WorkHistory, Depends(get_work_history_service)]


@router.get("/bus-network/{project_id}", response_model=TransportResponse)
async def calculate_bus_network_impact(
    project_id: str, bus_network_strategy: BusNetwork = Depends(get_bus_network_service)
):
    """
    Calculate bus network impact based on affected bus stops and services.

    This endpoint:
    - Identifies bus stops within ~300m buffer of the project location
    - Counts unique bus stops, operators, and routes affected
    - Uses BODS (Bus Open Data Service) timetable data

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        TransportResponse containing:
        - Number of bus stops affected
        - Count of unique bus operators impacted
        - Count of unique bus routes/services affected
        - Project duration in days
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
    Calculate road network impact using OS NGD API data.

    This endpoint:
    - Fetches street network data from Ordnance Survey NGD API
    - Identifies traffic-sensitive streets and strategic routes
    - Counts traffic signals and control systems (UTC, SCOOT, MOVA)
    - Calculates total road geometry length affected

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        BusNetworkResponse containing:
        - Traffic sensitivity status
        - Strategic/winter maintenance route indicators
        - Traffic signals and control systems count
        - Total geometry length in meters
        - Responsible authorities list
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
    Calculate underground asset impact using NUAR (National Underground Asset Register).

    This endpoint:
    - Queries NUAR API for underground assets near the project USRN
    - Uses hex grid system to count assets within buffer zone
    - Applies USRN geometry clipping for accurate counts
    - Calculates asset density per hex grid

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")
        zoom_level: NUAR hex grid zoom level (default: "11", range: 0-15)

    Returns:
        AssetResponse containing:
        - Total count of underground assets affected
        - Number of intersecting hex grids
        - Asset density (assets per grid)
        - Bounding box coordinates
        - List of affected hex grid details
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
    Calculate historical works impact based on completed works at the same USRN.

    This endpoint:
    - Queries last 6 months of historical work data (excluding current month)
    - Filters for completed works at the same USRN
    - Groups results by promoter organization
    - Provides total count and breakdown by promoter

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        WorkHistoryResponse containing:
        - Total count of completed works in last 6 months
        - Breakdown of works by promoter organization
        - Project duration in days
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


@router.get("/households/{project_id}", response_model=HouseholdsResponse)
async def get_household_demographics(
    project_id: str, households_service: Households = Depends(get_households_service)
):
    """
    Get household and population demographics for a specific project area.

    This endpoint:
    - Finds all postcodes within 250m of the project location
    - Returns population counts (total, female, male)
    - Returns total household count
    - Does NOT calculate wellbeing impact scores

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        HouseholdsResponse containing:
        - Total population within 250m
        - Total households within 250m
        - Female and male population breakdown
        - Number of postcodes in affected area
    """
    try:
        response = await households_service.calculate_impact(project_id)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching household demographics for project {project_id}: {str(e)}",
        )
