from abc import ABC, abstractmethod
from ..schemas.schemas import WellbeingResponse, TransportResponse, NetworkResponse
from ..motherduck.database_pool import MotherDuckPool
from typing import Dict, Optional, Any
import asyncio
import aiohttp
from loguru import logger

import duckdb
import os
from urllib.parse import urlencode


class MetricCalculationStrategy(ABC):
    """Abstract base class for metric calculation strategies"""

    @abstractmethod
    async def calculate_impact(self, project_id: str):
        """Calculate the metric and return the appropriate response object"""
        pass


class Wellbeing(MetricCalculationStrategy):
    """Simple wellbeing strategy that fetches geo shape data using DuckDB"""

    def __init__(self, motherduck_pool: MotherDuckPool):
        """Initialise DuckDB connection with PostgreSQL extension"""
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/collaboration_tool",
        )
        self.motherduck_pool = motherduck_pool

    # TODO: Maybe I should set up a local duckdb pool as well?
    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a local DuckDB connection with PostgreSQL and spatial extensions"""
        conn = duckdb.connect(":memory:")

        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")

        conn.execute(f"""
            ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)
        """)

        return conn

    async def get_postcodes_stats(self, project_id: str) -> Optional[Dict]:
        """
        Fetch postcodes within exactly 500m distance with population and household data.
        Note: project_id validation is now handled by middleware

        Args:
            project_id: Project ID to fetch geometry for (pre-validated by middleware)

        Returns:
            Dictionary containing postcodes within 500m distance with demographic data and project duration
        """
        postgres_conn = self._get_duckdb_connection()

        try:
            result = await asyncio.to_thread(
                postgres_conn.execute,
                """
                SELECT
                    project_id,
                    easting,
                    northing,
                    start_date,
                    completion_date
                FROM postgres_db.collaboration.raw_projects
                WHERE project_id = ?
            """,
                [project_id],
            )

            geometry_result = result.fetchone()

            if not geometry_result:
                return None

            stored_easting = geometry_result[1]
            stored_northing = geometry_result[2]
            start_date = geometry_result[3]
            completion_date = geometry_result[4]

            if start_date and completion_date:
                duration_days = (completion_date - start_date).days + 1
            else:
                duration_days = 30

            # Query MotherDuck for postcodes within exactly 500m distance with demographic data
            async with self.motherduck_pool.get_connection() as md_conn:
                postcodes_result = await asyncio.to_thread(
                    md_conn.execute,
                    """
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
                        FROM post_code_data.code_point cp
                        WHERE ST_Contains(
                            ST_Buffer(ST_Point(?, ?), 500),
                            ST_Point(cp.Easting, cp.Northing)
                        )
                    ),
                    population_data AS (
                        SELECT
                            Postcode,
                            SUM(Count) as total_population,
                            SUM(CASE WHEN "Sex (2 categories) Code" = 1 THEN Count ELSE 0 END) as female_population,
                            SUM(CASE WHEN "Sex (2 categories) Code" = 2 THEN Count ELSE 0 END) as male_population
                        FROM post_code_data.pcd_p001
                        GROUP BY Postcode
                    ),
                    household_data AS (
                        SELECT
                            Postcode,
                            Count as total_households
                        FROM post_code_data.pcd_p002
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
                """,
                    [stored_easting, stored_northing, stored_easting, stored_northing],
                )

                postcodes = postcodes_result.fetchall()
                logger.debug(f"postcodes (closest first): {postcodes[-100:]}")

                total_population = sum(row[11] for row in postcodes)
                total_female = sum(row[12] for row in postcodes)
                total_male = sum(row[13] for row in postcodes)
                total_households = sum(row[14] for row in postcodes)

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
                    "total_households_affected": total_households,
                },
            }

        except Exception as e:
            raise Exception(
                f"Error fetching postcodes with demographics for project {project_id}: {str(e)}"
            )
        finally:
            postgres_conn.close()

    async def calculate_impact(self, project_id: str) -> WellbeingResponse:
        """
        Calculate the wellbeing impact metric using postcode data.
        Note: project_id validation is now handled by middleware

        Formula: Wellbeing Impact = £1.61 × Days × Households_Affected

        Args:
            project_id: Project ID string (pre-validated by middleware)

        Returns:
            WellbeingResponse object with calculated wellbeing metrics
        """
        postcode_stats = await self.get_postcodes_stats(project_id)

        if not postcode_stats:
            raise ValueError(f"No postcode data found for project {project_id}")

        duration_days = postcode_stats["duration_days"]
        postcode_count = postcode_stats["postcode_count"]
        total_population = postcode_stats["summary"]["total_population_affected"]
        households_affected = postcode_stats["summary"]["total_households_affected"]

        # Calculate wellbeing impact
        # Formula: Wellbeing Impact = £1.61 × Days × Households_Affected
        wellbeing_impact_per_day = 1.61
        wellbeing_total_impact = (
            wellbeing_impact_per_day * duration_days * households_affected
        )

        response = WellbeingResponse(
            success=True,
            project_id=project_id,
            project_duration_days=duration_days,
            wellbeing_postcode_count=postcode_count,
            wellbeing_total_population=total_population,
            wellbeing_households_affected=households_affected,
            wellbeing_total_impact=wellbeing_total_impact,
        )

        return response


class Bus(MetricCalculationStrategy):
    """
    Bus delay strategy that finds NaPTAN stops within buffer around project coordinates
    """

    def __init__(self, motherduck_pool: MotherDuckPool):
        """Initialise DuckDB connection with PostgreSQL extension"""
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/collaboration_tool",
        )
        self.motherduck_pool = motherduck_pool

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with PostgreSQL and spatial extensions"""
        conn = duckdb.connect(":memory:")

        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")

        conn.execute(f"""
            ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)
        """)

        return conn

    async def get_naptan_stops_in_buffer(
        self, project_id: str, buffer_distance: float = 0.003
    ) -> Optional[Dict]:
        postgres_conn = self._get_duckdb_connection()

        try:
            result = await asyncio.to_thread(
                postgres_conn.execute,
                """
                SELECT
                    project_id,
                    geo_point,
                    start_date,
                    completion_date
                FROM postgres_db.collaboration.raw_projects
                WHERE project_id = ?
            """,
                [project_id],
            )

            geometry_result = result.fetchone()
            if not geometry_result:
                return None

            geo_point = geometry_result[1]
            lat_str, lon_str = geo_point.split(", ")
            project_lat = float(lat_str)
            project_lon = float(lon_str)

            start_date = geometry_result[2]
            completion_date = geometry_result[3]

            if start_date and completion_date:
                duration_days = (completion_date - start_date).days + 1
            else:
                duration_days = 30

            async with self.motherduck_pool.get_connection() as md_conn:
                bods_stops_result = await asyncio.to_thread(
                    md_conn.execute,
                    """
                    SELECT DISTINCT
                        s.stop_id,
                        s.stop_name,
                        CAST(s.stop_lat AS DOUBLE) as lat,
                        CAST(s.stop_lon AS DOUBLE) as lon,
                        a.agency_name AS operator,
                        r.route_short_name AS route_number,
                        st.arrival_time,
                        st.departure_time,
                        t.service_id,
                        t.trip_headsign
                    FROM bods_timetables.stops s
                    JOIN bods_timetables.stop_times st ON s.stop_id = st.stop_id
                    JOIN bods_timetables.trips t ON st.trip_id = t.trip_id
                    JOIN bods_timetables.routes r ON t.route_id = r.route_id
                    JOIN bods_timetables.agency a ON r.agency_id = a.agency_id
                    WHERE ST_Contains(
                        ST_Buffer(ST_Point(?, ?), ?),
                        ST_Point(CAST(s.stop_lon AS DOUBLE), CAST(s.stop_lat AS DOUBLE))
                    )
                    AND s.stop_lat IS NOT NULL
                    AND s.stop_lon IS NOT NULL
                    ORDER BY s.stop_name, st.arrival_time
                """,
                    [project_lon, project_lat, buffer_distance],
                )

                bods_stops = bods_stops_result.fetchall()
                logger.debug(
                    f"Found {len(bods_stops)} stop-time records within {buffer_distance} degree buffer"
                )

                if bods_stops:
                    logger.debug(f"First few stops: {bods_stops[:3]}")

                return {
                    "project_id": geometry_result[0],
                    "project_lat": project_lat,
                    "project_lon": project_lon,
                    "start_date": start_date,
                    "completion_date": completion_date,
                    "duration_days": duration_days,
                    "buffer_distance": buffer_distance,
                    "stops_count": len(bods_stops),
                    "bods_stops": bods_stops,
                    "route_operator_info": bods_stops,
                }

        except Exception as e:
            raise Exception(
                f"Error fetching transport data for project {project_id}: {str(e)}"
            )
        finally:
            postgres_conn.close()

    async def calculate_impact(self, project_id: str) -> TransportResponse:
        naptan_data = await self.get_naptan_stops_in_buffer(project_id)

        if not naptan_data:
            raise ValueError(f"No NaPTAN data found for project {project_id}")

        duration_days = naptan_data["duration_days"]
        bods_stops = naptan_data["bods_stops"]

        # Count unique stops, operators, and routes
        # TODO: rename this as services and not routes
        unique_stops = len(set(stop[0] for stop in bods_stops)) if bods_stops else 0
        unique_operators = len(set(stop[4] for stop in bods_stops)) if bods_stops else 0
        unique_routes = len(set(stop[5] for stop in bods_stops)) if bods_stops else 0

        logger.debug(
            f"Unique stops: {unique_stops}, operators: {unique_operators}, routes: {unique_routes}"
        )

        response = TransportResponse(
            success=True,
            project_id=project_id,
            project_duration_days=duration_days,
            transport_stops_affected=unique_stops,
            transport_operators_count=unique_operators,
            transport_routes_count=unique_routes,
        )

        return response


class Network(MetricCalculationStrategy):
    """
    Network strategy that fetches OS NGD API data for transport networks around a project
    """

    def __init__(self, motherduck_pool: MotherDuckPool):
        """Initialise DuckDB connection with PostgreSQL extension"""
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/collaboration_tool",
        )
        self.motherduck_pool = motherduck_pool
        self.api_key = os.getenv("OS_KEY")
        if not self.api_key:
            raise ValueError(
                "An API key must be provided through the environment variable 'OS_KEY'"
            )

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with PostgreSQL and spatial extensions"""
        conn = duckdb.connect(":memory:")

        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")

        conn.execute(f"""
            ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)
        """)

        return conn

    async def _fetch_data_auth(self, endpoint: str) -> dict:
        """
        Asynchronous function to fetch data from an endpoint using OS API key from environment variables

        Args:
            endpoint: str - The endpoint to fetch data from
        Returns:
            dict - The data from the endpoint
        Raises:
            Exception - If the request fails
        """
        try:
            headers = {"key": self.api_key, "Content-Type": "application/json"}
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint, headers=headers) as response:
                    response.raise_for_status()
                    result = await response.json()
                    return result
        except aiohttp.ClientError as e:
            raise e
        except Exception as e:
            raise e

    async def get_single_collection_feature(
        self, collection_id: str, query_attr: Optional[str] = None
    ) -> dict:
        """
        Fetches collection features with USRN filter

        Args:
            collection_id: str - The ID of the collection
            query_attr: Optional[str] - USRN value to filter by
        Returns:
            API response with collection features
        """
        base_url = "https://api.os.uk/features/ngd/ofa/v1/collections"
        endpoint = f"{base_url}/{collection_id}/items"

        query_params: Dict[str, Any] = {}

        if query_attr:
            query_params["filter"] = f"usrn={query_attr}"

        if query_params:
            endpoint = f"{endpoint}?{urlencode(query_params)}"

        try:
            result = await self._fetch_data_auth(endpoint)
            return result
        except Exception as e:
            raise e

    async def get_street_info(self, project_id: str) -> Optional[Dict]:
        """
        Fetch street info data for a project using its USRN

        Args:
            project_id: Project ID to fetch USRN for

        Returns:
            Dictionary containing street info data from OS NGD API
        """
        postgres_conn = self._get_duckdb_connection()

        try:
            result = await asyncio.to_thread(
                postgres_conn.execute,
                """
                SELECT
                    project_id,
                    usrn,
                    start_date,
                    completion_date
                FROM postgres_db.collaboration.raw_projects
                WHERE project_id = ?
            """,
                [project_id],
            )

            project_result = result.fetchone()

            if not project_result:
                logger.debug(f"No project found for project_id: {project_id}")
                return None

            usrn = project_result[1]
            start_date = project_result[2]
            completion_date = project_result[3]

            if not usrn:
                logger.debug(f"No USRN found for project {project_id}")
                return None

            if start_date and completion_date:
                duration_days = (completion_date - start_date).days + 1
            else:
                duration_days = 30

            logger.debug(f"Processing street info for USRN: {usrn}")

            # TODO: add more collection ids if they can be filtered directly with the usrn
            collection_ids = [
                "trn-ntwk-street-1",
                "trn-rami-specialdesignationarea-1",
                "trn-rami-specialdesignationline-1",
                "trn-rami-specialdesignationpoint-1",
            ]

            feature_coroutines = [
                self.get_single_collection_feature(
                    collection_id=collection_id, query_attr=str(usrn)
                )
                for collection_id in collection_ids
            ]

            feature_results = await asyncio.gather(
                *feature_coroutines, return_exceptions=True
            )

            all_features = []
            latest_timestamp = None

            for collection_id, result in zip(collection_ids, feature_results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch {collection_id}: {str(result)}")
                    continue

                if not isinstance(result, dict) or "features" not in result:
                    logger.error(f"Invalid response format from {collection_id}")
                    continue

                filtered_features = []
                for feature in result["features"]:
                    feature_copy = feature.copy()
                    if "geometry" in feature_copy:
                        del feature_copy["geometry"]
                    filtered_features.append(feature_copy)

                all_features.extend(filtered_features)

                if result.get("timeStamp"):
                    if (
                        latest_timestamp is None
                        or result["timeStamp"] > latest_timestamp
                    ):
                        latest_timestamp = result["timeStamp"]

            if not all_features:
                logger.error(f"No features found for USRN: {usrn}")

            logger.debug(f"All features: {all_features}")

            return {
                "project_id": project_id,
                "usrn": usrn,
                "start_date": start_date,
                "completion_date": completion_date,
                "duration_days": duration_days,
                "street_info": {
                    "type": "FeatureCollection",
                    "numberReturned": len(all_features),
                    "timeStamp": latest_timestamp or "",
                    "features": all_features,
                },
            }

        except Exception as e:
            raise Exception(
                f"Error fetching street info for project {project_id}: {str(e)}"
            )
        finally:
            postgres_conn.close()

    async def calculate_impact(self, project_id: str) -> NetworkResponse:
        """
        Calculate the network impact metric using street info data.
        Note: project_id validation is now handled by middleware

        Args:
            project_id: Project ID string (pre-validated by middleware)

        Returns:
            NetworkResponse object with network metrics
        """
        # Get street info data
        street_data = await self.get_street_info(project_id)

        if not street_data:
            raise ValueError(f"No street data found for project {project_id}")

        duration_days = street_data["duration_days"]
        features = street_data["street_info"]["features"]

        strategic_routes_count = False
        winter_maintenance_routes_count = False
        traffic_signals_count = 0
        unique_usrns = set()
        traffic_control_systems = set()
        responsible_authorities = set()
        designation_types = set()
        operational_states = set()
        total_geometry_length = 0.0

        traffic_sensitive = False

        for feature in features:
            properties = feature.get("properties", {})
            
            if "usrn" in properties and properties["usrn"]:
                unique_usrns.add(properties["usrn"])
            
            if "geometry_length" in properties:
                try:
                    total_geometry_length += float(properties["geometry_length"])
                except (ValueError, TypeError):
                    pass
            
            if "responsibleauthority_name" in properties:
                responsible_authorities.add(properties["responsibleauthority_name"])
            elif "contactauthority_authorityname" in properties:
                responsible_authorities.add(properties["contactauthority_authorityname"])
            
            if "operationalstate" in properties:
                operational_states.add(properties["operationalstate"])
            
            designation = properties.get("designation", "")
            description = properties.get("description", "")
            designation_desc = properties.get("designationdescription", "")
            
            if designation:
                designation_types.add(designation)
                
                if "Strategic Route" in designation:
                    strategic_routes_count = True
                
                elif "Winter Maintenance" in designation:
                    winter_maintenance_routes_count = True
                
                elif "Pedestrian Crossings, Traffic Signals And Traffic Sensors" in designation:
                    traffic_signals_count += 1
                
                elif "Traffic Sensitive Street" in designation:
                    traffic_sensitive = True
            
            if designation_desc:
                control_systems = ["UTC", "SCOOT", "MOVA", "GEMINI", "PUFFIN", "STRATOS"]
                for system in control_systems:
                    if system in designation_desc.upper():
                        traffic_control_systems.add(system)
            
            if description and not designation:
                designation_types.add(description)


        response = NetworkResponse(
            success=True,
            project_id=project_id,
            project_duration_days=duration_days,
            traffic_sensitive=traffic_sensitive,
            strategic_routes_count=strategic_routes_count,
            winter_maintenance_routes_count=winter_maintenance_routes_count,
            unique_usrn_count=len(unique_usrns),
            traffic_signals_count=traffic_signals_count,
            traffic_control_systems=list(traffic_control_systems),
            total_geometry_length_meters=total_geometry_length,
            responsible_authorities=list(responsible_authorities),
            authority_count=len(responsible_authorities),
            designation_types=list(designation_types),
            operational_states=list(operational_states),
        )

        return response
