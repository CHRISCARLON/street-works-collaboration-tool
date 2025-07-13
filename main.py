from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from backend.motherduck.database_pool import MotherDuckPool
from backend.metrics.strategies import WellbeingStrategy

app = FastAPI(
    title="Collaboration Tool API",
    description="A simple collaboration tool API",
    version="0.1.0",
)

motherduck_pool = MotherDuckPool()
wellbeing_strategy = WellbeingStrategy(motherduck_pool)

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
    }


@app.get("/calculate-wellbeing/{project_id}")
async def calculate_wellbeing_impact(project_id: str):
    """
    Calculate wellbeing impact for a specific project ID
    
    Args:
        project_id: The project identifier (e.g., "PROJ_CDT440003968937")
        
    Returns:
        ImpactScore with calculated wellbeing metrics
    """
    try:
        impact_score = await wellbeing_strategy.calculate_score(project_id)
        
        return {
            "success": True,
            "project_id": project_id,
            "impact_score": impact_score.model_dump()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating wellbeing impact for project {project_id}: {str(e)}"
        )
