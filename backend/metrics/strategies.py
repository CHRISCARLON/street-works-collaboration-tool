from abc import ABC, abstractmethod
from ..schemas.schemas import Project, ImpactScore
from ..motherduck.database_pool import MotherDuckPool
from typing import Dict, Optional, List
import asyncio

import duckdb
import os
import re
import uuid
from datetime import datetime


class MetricCalculationStrategy(ABC):
    """Abstract base class for metric calculation strategies"""

    @abstractmethod
    def calculate_score(self, project: Project) -> ImpactScore:
        """Calculate the metric and return an ImpactScore object"""
        pass


class WellbeingStrategy(MetricCalculationStrategy):
    """Simple wellbeing strategy that fetches geo shape data using DuckDB"""

    def __init__(self, motherduck_pool: MotherDuckPool):
        """Initialise DuckDB connection with PostgreSQL extension"""
        self.db_url = os.getenv(
            "DATABASE_URL", 
            "postgresql://postgres:password@localhost:5432/collaboration_tool"
        )
        self.motherduck_pool = motherduck_pool
        
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with PostgreSQL and spatial extensions"""
        conn = duckdb.connect(':memory:')
        
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
        
        conn.execute(f"""
            ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)
        """)
        
        return conn

    # TODO: Add this as a middleware function for all routes in the future
    def _validate_project_id(self, project_id: str) -> str:
        """Validate and sanitise project ID"""
        if not project_id or not isinstance(project_id, str):
            raise ValueError("Invalid project ID")
        
        # Check for potential SQL injection characters and raise error if found
        if re.search(r'[^\w\-]', project_id):
            raise ValueError("Project ID contains invalid characters")
        
        if len(project_id) > 50:
            raise ValueError("Project ID too long")
        
        return project_id

    async def get_postcodes_stats(self, unvalidated_project_id: str) -> Optional[Dict]:
        """
        Fetch postcodes within exactly 500m distance with population and household data.
        
        Args:
            project_id: Project ID to fetch geometry for
            
        Returns:
            Dictionary containing postcodes within 500m distance with demographic data and project duration
        """

        project_id = self._validate_project_id(unvalidated_project_id)

        postgres_conn = self._get_duckdb_connection()
        
        try:
            # Get coordinates AND project dates
            result = await asyncio.to_thread(postgres_conn.execute, """
                SELECT 
                    project_id,
                    easting,
                    northing,
                    start_date,
                    completion_date
                FROM postgres_db.raw_projects 
                WHERE project_id = ?
            """, [project_id])
            
            geometry_result = result.fetchone()
            
            if not geometry_result:
                return None
            
            # Use the stored BNG coordinates
            stored_easting = geometry_result[1]
            stored_northing = geometry_result[2]
            start_date = geometry_result[3]
            completion_date = geometry_result[4]
            
            if start_date and completion_date:
                duration_days = (completion_date - start_date).days + 1
            else:
                duration_days = 30  # Default fallback
            
            # Query MotherDuck for postcodes within exactly 500m distance with demographic data
            async with self.motherduck_pool.get_connection() as md_conn:
                postcodes_result = await asyncio.to_thread(md_conn.execute, """
                    WITH postcodes_in_range AS (
                        SELECT 
                            cp.Postcode,
                            cp.PQI,
                            cp.Easting,
                            cp.Northing,
                            cp.Country_code,
                            cp.NHS_regional_code,
                            cp.NHS_health_code,
                            cp.Admin_county_code,
                            cp.Admin_district_code,
                            cp.Admin_ward_code,
                            SQRT(POW(cp.Easting - ?, 2) + POW(cp.Northing - ?, 2)) as distance_m
                        FROM sm_permit.post_code_data.code_point cp
                        WHERE SQRT(POW(cp.Easting - ?, 2) + POW(cp.Northing - ?, 2)) <= 500
                    ),
                    population_data AS (
                        SELECT 
                            Postcode,
                            SUM(Count) as total_population,
                            SUM(CASE WHEN "Sex (2 categories) Code" = 1 THEN Count ELSE 0 END) as female_population,
                            SUM(CASE WHEN "Sex (2 categories) Code" = 2 THEN Count ELSE 0 END) as male_population
                        FROM sm_permit.post_code_data.pcd_p001
                        GROUP BY Postcode
                    ),
                    household_data AS (
                        SELECT 
                            Postcode,
                            Count as total_households
                        FROM sm_permit.post_code_data.pcd_p002
                    )
                    SELECT 
                        pir.Postcode,
                        pir.PQI,
                        pir.Easting,
                        pir.Northing,
                        pir.Country_code,
                        pir.NHS_regional_code,
                        pir.NHS_health_code,
                        pir.Admin_county_code,
                        pir.Admin_district_code,
                        pir.Admin_ward_code,
                        pir.distance_m,
                        COALESCE(pd.total_population, 0) as total_population,
                        COALESCE(pd.female_population, 0) as female_population,
                        COALESCE(pd.male_population, 0) as male_population,
                        COALESCE(hd.total_households, 0) as total_households
                    FROM postcodes_in_range pir
                    LEFT JOIN population_data pd ON pir.Postcode = pd.Postcode
                    LEFT JOIN household_data hd ON pir.Postcode = hd.Postcode
                    ORDER BY pir.distance_m
                """, [stored_easting, stored_northing, stored_easting, stored_northing])
                
                postcodes = postcodes_result.fetchall()
                
                # Calculate totals
                total_population = sum(row[11] for row in postcodes)  # total_population column
                total_female = sum(row[12] for row in postcodes)     # female_population column
                total_male = sum(row[13] for row in postcodes)       # male_population column
                total_households = sum(row[14] for row in postcodes) # total_households column
            
            return {
                "project_id": geometry_result[0],
                "project_easting": stored_easting,
                "project_northing": stored_northing,
                "start_date": start_date,
                "completion_date": completion_date,
                "duration_days": duration_days,
                "postcode_count": len(postcodes),
                "summary": {
                    "total_population_affected": total_population,
                    "total_female_population": total_female,
                    "total_male_population": total_male,
                    "total_households_affected": total_households
                }
            }
            
        except Exception as e:
            raise Exception(f"Error fetching postcodes with demographics for project {project_id}: {str(e)}")
        finally:
            postgres_conn.close()

    async def calculate_score(self, project_id: str) -> ImpactScore:
        """
        Calculate the wellbeing impact metric using postcode data.
        
        Formula: Wellbeing Impact = £1.61 × Days × Households_Affected
        
        Args:
            project_id: Project ID string
            
        Returns:
            ImpactScore object with calculated wellbeing metrics
        """
        # Get postcode statistics with duration
        postcode_stats = await self.get_postcodes_stats(project_id)
        
        if not postcode_stats:
            raise ValueError(f"No postcode data found for project {project_id}")
        
        # Get values from postcode stats
        duration_days = postcode_stats["duration_days"]
        postcode_count = postcode_stats["postcode_count"]
        total_population = postcode_stats["summary"]["total_population_affected"]
        households_affected = postcode_stats["summary"]["total_households_affected"]
        
        # Calculate wellbeing impact
        # Formula: Wellbeing Impact = £1.61 × Days × Households_Affected
        wellbeing_impact_per_day = 1.61
        wellbeing_total_impact = wellbeing_impact_per_day * duration_days * households_affected
        
        # Create ImpactScore object
        impact_score = ImpactScore(
            impact_score_id=str(uuid.uuid4()),
            project_id=project_id,
            project_duration_days=duration_days,
            wellbeing_postcode_count=postcode_count,
            wellbeing_total_population=total_population,
            wellbeing_households_affected=households_affected,
            wellbeing_total_impact=wellbeing_total_impact,
            is_valid=True,
            updated_at=datetime.now(),
            version="1.0"
        )
        
        return impact_score
