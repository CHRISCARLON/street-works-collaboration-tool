from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
import logging

from backend.schemas.schemas import ProjectCreate, ProjectResponse
from backend.api.dependencies import get_postgres_pool
from backend.db_pool.postgres_pool import PostgresPool

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/create", response_model=ProjectResponse)
async def create_project(
    project: ProjectCreate, postgres_pool: PostgresPool = Depends(get_postgres_pool)
) -> ProjectResponse:
    """
    Create a new project in the database.

    Args:
        project: Project data from form submission
        db_pool: Database connection pool

    Returns:
        Dict containing project creation status and details
    """

    coords_str = None

    try:
        # Validate before getting connection
        if len(project.geometry_coordinates) != 2:
            raise ValueError(
                f"geometry_coordinates must contain exactly 2 values [lon, lat], got {len(project.geometry_coordinates)} values: {project.geometry_coordinates}"
            )

        lon, lat = project.geometry_coordinates
        geometry = f"POINT({lon} {lat})"

        async with postgres_pool.get_connection() as conn:

            geo_shape = None
            if project.geo_shape_coordinates:
                coords_str = ", ".join(
                    [f"{lon} {lat}" for lon, lat in project.geo_shape_coordinates]
                )
                geo_shape = f"LINESTRING({coords_str})"

            import uuid

            project_id = f"PROJ_{uuid.uuid4().hex[:12].upper()}"

            query = """
                INSERT INTO collaboration.raw_projects (
                    project_id, programme_id, source, swa_code, contact, department,
                    tele, email, title, scheme, simple_theme, multi_theme,
                    comments, geo_point, geometry, geo_shape, usrn, post_code,
                    site_area, location_meta, asset_type, pressure, material,
                    diameter, diam_unit, carr_mat, carr_dia, carr_di_un,
                    asset_id, depth, ag_ind, inst_date, length, length_unit,
                    start_date, start_date_yy, start_date_meta,
                    completion_date, completion_date_yy, completion_date_meta,
                    dates_yy_range, flexibility,
                    programme_value, programme_range, programme_value_meta,
                    project_value, project_range, project_value_meta,
                    funding_status, planning_status, collaboration, restrictions,
                    created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, ST_GeomFromText($15, 4326),
                    ST_GeomFromText($16, 4326), $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28,
                    $29, $30, $31, $32, $33, $34, $35, $36,
                    $37, $38, $39, $40, $41, $42, $43, $44,
                    $45, $46, $47, $48, $49, $50, $51, $52,
                    $53
                ) RETURNING project_id, created_at
            """

            result = await conn.fetchrow(
                query,
                project_id,
                project.programme_id,
                project.source,
                project.swa_code,
                project.contact,
                project.department,
                project.tele,
                project.email,
                project.title,
                project.scheme,
                project.simple_theme,
                project.multi_theme,
                project.comments,
                project.geo_point,
                geometry,
                geo_shape,
                project.usrn,
                project.post_code,
                project.site_area,
                project.location_meta,
                project.asset_type,
                project.pressure,
                project.material,
                project.diameter if hasattr(project, "diameter") else None,
                project.diam_unit if hasattr(project, "diam_unit") else None,
                project.carr_mat if hasattr(project, "carr_mat") else None,
                project.carr_dia if hasattr(project, "carr_dia") else None,
                project.carr_di_un if hasattr(project, "carr_di_un") else None,
                project.asset_id,
                project.depth if hasattr(project, "depth") else None,
                project.ag_ind if hasattr(project, "ag_ind") else False,
                project.inst_date if hasattr(project, "inst_date") else None,
                project.length if hasattr(project, "length") else None,
                project.length_unit if hasattr(project, "length_unit") else None,
                project.start_date if hasattr(project, "start_date") else None,
                project.start_date_yy,
                project.start_date_meta
                if hasattr(project, "start_date_meta")
                else None,
                project.completion_date
                if hasattr(project, "completion_date")
                else None,
                project.completion_date_yy,
                project.completion_date_meta
                if hasattr(project, "completion_date_meta")
                else None,
                project.dates_yy_range if hasattr(project, "dates_yy_range") else None,
                project.flexibility if hasattr(project, "flexibility") else None,
                project.programme_value
                if hasattr(project, "programme_value")
                else None,
                project.programme_range
                if hasattr(project, "programme_range")
                else None,
                project.programme_value_meta
                if hasattr(project, "programme_value_meta")
                else None,
                project.project_value if hasattr(project, "project_value") else None,
                project.project_range if hasattr(project, "project_range") else None,
                project.project_value_meta
                if hasattr(project, "project_value_meta")
                else None,
                project.funding_status,
                project.planning_status
                if hasattr(project, "planning_status")
                else None,
                project.collaboration if hasattr(project, "collaboration") else True,
                project.restrictions if hasattr(project, "restrictions") else None,
                datetime.now(),
            )

            if result is None:
                raise ValueError("Returned Null")

            logger.info(f"Successfully created project with ID: {result['project_id']}")

            return ProjectResponse(
                success=True,
                project_id=result["project_id"],
                message="Project created successfully",
                created_at=result["created_at"],
            )

    except Exception as e:
        logger.error(f"Error creating project: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to create project: {str(e)}"
        )


@router.delete("/delete/{project_id}", response_model=ProjectResponse)
async def delete_project(
    project_id: str, postgres_pool: PostgresPool = Depends(get_postgres_pool)
) -> ProjectResponse:
    """
    Delete a project from the database.

    Args:
        project_id: The project identifier to delete
        postgres_pool: Database connection pool

    Returns:
        Dict containing deletion status and details
    """
    try:
        async with postgres_pool.get_connection() as conn:
            check_query = """
                SELECT project_id, created_at
                FROM collaboration.raw_projects
                WHERE project_id = $1
            """

            existing_project = await conn.fetchrow(check_query, project_id)

            if not existing_project:
                raise HTTPException(
                    status_code=404,
                    detail=f"Project {project_id} not found"
                )

            delete_query = """
                DELETE FROM collaboration.raw_projects
                WHERE project_id = $1
                RETURNING project_id
            """

            result = await conn.fetchrow(delete_query, project_id)

            if result:
                logger.info(f"Successfully deleted project with ID: {project_id}")

                return ProjectResponse(
                    success=True,
                    project_id=project_id,
                    message=f"Project {project_id} deleted successfully",
                    created_at=existing_project["created_at"]
                )
            else:
                raise ValueError("Failed to delete project")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting project: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to delete project: {str(e)}"
        )
