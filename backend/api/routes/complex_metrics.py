from fastapi import APIRouter, HTTPException, Depends
from backend.services.metrics import Wellbeing
from backend.schemas.schemas import WellbeingResponse
from backend.api.dependencies import get_wellbeing_service
from loguru import logger

router = APIRouter()


@router.get("/wellbeing/{project_id}", response_model=WellbeingResponse)
async def calculate_wellbeing_impact(
    project_id: str, wellbeing_strategy: Wellbeing = Depends(get_wellbeing_service)
):
    """
    Calculate wellbeing impact for a specific project ID

    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")

    Returns:
        WellbeingResponse with calculated wellbeing metrics
    """
    try:
        response = await wellbeing_strategy.calculate_impact(project_id)
        logger.debug(response)
        return response
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating wellbeing impact for project {project_id}: {str(e)}",
        )
