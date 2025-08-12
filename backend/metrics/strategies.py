import asyncio
import aiohttp
import duckdb
import os
import math
import base64
import struct

from abc import ABC, abstractmethod
from ..schemas.schemas import (
    WellbeingResponse,
    TransportResponse,
    BusNetworkResponse,
    AssetResponse,
)
from ..motherduck.database_pool import MotherDuckPool
from typing import Dict, Optional, Any
from loguru import logger
from urllib.parse import urlencode
from shapely.wkt import loads
from shapely.geometry import Polygon


# TODO: We need to do a connection pool to postgres!


class MetricCalculationStrategy(ABC):
    """Abstract base class for metric calculation strategies"""

    @abstractmethod
    async def calculate_impact(self, project_id: str) -> WellbeingResponse | TransportResponse | BusNetworkResponse | AssetResponse:
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

        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

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
                    geo_point,
                    ST_AsText(ST_GeomFromWKB(geometry)) as geom_wkt,
                    ST_X(ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:27700', true)) as easting,
                    ST_Y(ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:4326', 'EPSG:27700', true)) as northing,
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

            logger.debug(f"{geometry_result}")
            logger.debug(f"WKT from geometry column: {geometry_result[2]}")

            stored_easting = geometry_result[3]
            stored_northing = geometry_result[4]
            start_date = geometry_result[5]
            completion_date = geometry_result[6]

            if start_date and completion_date:
                duration_days = (completion_date - start_date).days + 1
            else:
                duration_days = 30

            async with self.motherduck_pool.get_connection() as md_conn:
                postcodes_result = await asyncio.to_thread(
                    md_conn.execute,
                    """
                    WITH postcodes_in_range AS (
                        SELECT
                            cp.postcode,
                            cp.positional_quality_indicator,
                            cp.geometry,
                            ST_X(ST_GeomFromText(cp.geometry)) as easting,
                            ST_Y(ST_GeomFromText(cp.geometry)) as northing,
                            cp.country_code,
                            cp.nhs_regional_ha_code,
                            cp.nhs_ha_code,
                            cp.admin_county_code,
                            cp.admin_district_code,
                            cp.admin_ward_code,
                            ST_Distance(
                                ST_Point(?, ?),
                                ST_GeomFromText(cp.geometry)
                            ) as distance_m
                        FROM post_code_data.code_point cp
                        WHERE ST_DWithin(
                            ST_Point(?, ?),
                            ST_GeomFromText(cp.geometry),
                            250
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
                        pir.postcode,
                        pir.positional_quality_indicator,
                        pir.easting,
                        pir.northing,
                        pir.country_code,
                        pir.nhs_regional_ha_code,
                        pir.nhs_ha_code,
                        pir.admin_county_code,
                        pir.admin_district_code,
                        pir.admin_ward_code,
                        pir.distance_m,
                        COALESCE(pd.total_population, 0) as total_population,
                        COALESCE(pd.female_population, 0) as female_population,
                        COALESCE(pd.male_population, 0) as male_population,
                        COALESCE(hd.total_households, 0) as total_households
                    FROM postcodes_in_range pir
                    LEFT JOIN population_data pd ON pir.postcode = pd.Postcode
                    LEFT JOIN household_data hd ON pir.postcode = hd.Postcode
                    ORDER BY pir.distance_m ASC
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


class BusNetwork(MetricCalculationStrategy):
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

        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

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


class RoadNetwork(MetricCalculationStrategy):
    """
    Road network strategy that fetches OS NGD API data for transport networks around a project
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

        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

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

    async def calculate_impact(self, project_id: str) -> BusNetworkResponse:
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
                responsible_authorities.add(
                    properties["contactauthority_authorityname"]
                )

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

                elif (
                    "Pedestrian Crossings, Traffic Signals And Traffic Sensors"
                    in designation
                ):
                    traffic_signals_count += 1

                elif "Traffic Sensitive Street" in designation:
                    traffic_sensitive = True

            if designation_desc:
                control_systems = [
                    "UTC",
                    "SCOOT",
                    "MOVA",
                    "GEMINI",
                    "PUFFIN",
                    "STRATOS",
                ]
                for system in control_systems:
                    if system in designation_desc.upper():
                        traffic_control_systems.add(system)

            if description and not designation:
                designation_types.add(description)

        response = BusNetworkResponse(
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


class AssetNetwork(MetricCalculationStrategy):
    """
    Asset network strategy that finds asset count within a buffer around usrn coordinates
    """

    def __init__(self, motherduck_pool: MotherDuckPool):
        """Initialise DuckDB connection with PostgreSQL extension"""
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/collaboration_tool",
        )
        self.motherduck_pool = motherduck_pool
        self.nuar_base_url = os.getenv("NUAR_BASE_URL")
        self.buffer_distance = float(os.getenv("USRN_BUFFER_DISTANCE", "5"))
        self.nuar_zoom_level = os.getenv("NUAR_ZOOM_LEVEL", "11")

    # N3GB HEX GRID SYSTEM CONSTANTS
    CELL_RADIUS = [
        1281249.9438829257,
        48304.58762201923,
        182509.65769514776,
        68979.50076169973,
        26069.67405498836,
        9849.595592375015,
        3719.867784388759,
        1399.497052515653,
        529.4301968468868,
        199.76319313961054,
        75.05553499465135,
        28.290163190291665,
        10.392304845413264,
        4.041451884327381,
        1.7320508075688774,
        0.5773502691896258,
    ]

    def _get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with PostgreSQL and spatial extensions"""
        conn = duckdb.connect(":memory:")

        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")

        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        conn.execute(f"""
            ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)
        """)

        return conn

    async def _get_bbox_from_usrn(self, usrn: str, buffer_distance: float = 5) -> tuple:
        """Get bounding box coordinates for a given USRN"""
        try:
            async with self.motherduck_pool.get_connection() as con:
                query = """
                    SELECT geometry
                    FROM os_open_usrns.open_usrns_latest
                    WHERE usrn = ?
                """

                logger.debug(f"Executing query for USRN: {usrn}")
                result = await asyncio.to_thread(con.execute, query, [usrn])
                df = result.fetchdf()

                if df.empty:
                    logger.warning(f"No geometry found for USRN: {usrn}")
                    raise ValueError(f"No geometry found for USRN: {usrn}")

                geom = loads(df["geometry"].iloc[0])
                buffered = geom.buffer(buffer_distance, cap_style="round")
                logger.debug(f"Buffered geometry: {buffered}")

                return tuple(round(coord) for coord in buffered.bounds)
        except Exception as e:
            logger.error(f"Error in get_bbox_from_usrn: {str(e)}")
            raise

    def _decode_hex_identifier(self, identifier: str) -> tuple:
        """Decode a hex grid identifier to get easting, northing, and zoom level"""
        padding = "=" * (-len(identifier) % 4)
        base64_str = identifier + padding
        binary_data = base64.urlsafe_b64decode(base64_str)
        easting_int, northing_int, zoom_level = struct.unpack(">QQB", binary_data)
        easting = easting_int / 10000.0
        northing = northing_int / 10000.0
        return easting, northing, zoom_level

    def _create_hexagon(self, center_x: float, center_y: float, size: float) -> Polygon:
        """Create a hexagon polygon centered at (center_x, center_y) with the given size"""
        points = [
            (
                center_x + size * math.cos(math.radians(angle)),
                center_y + size * math.sin(math.radians(angle)),
            )
            for angle in range(30, 390, 60)
        ]
        return Polygon(points)

    def _create_hex_grids_from_nuar_data(
        self, collection_items: list
    ) -> Optional[list]:
        """Create hex grid geometries from NUAR collection items"""
        if not collection_items:
            return None

        hex_data = []

        for item in collection_items:
            grid_id = item.get("gridId")
            asset_count = item.get("assetCount", 0)

            if grid_id:
                try:
                    easting, northing, zoom_level = self._decode_hex_identifier(grid_id)

                    radius = (
                        self.CELL_RADIUS[zoom_level]
                        if zoom_level < len(self.CELL_RADIUS)
                        else self.CELL_RADIUS[-1]
                    )
                    hexagon = self._create_hexagon(easting, northing, radius)

                    hex_data.append(
                        {
                            "grid_id": grid_id,
                            "easting": easting,
                            "northing": northing,
                            "zoom_level": zoom_level,
                            "asset_count": asset_count,
                            "geometry": hexagon,
                        }
                    )

                except Exception as e:
                    logger.warning(f"Error decoding hex grid ID {grid_id}: {e}")
                    continue

        return hex_data if hex_data else None

    def _filter_hex_grids_by_usrn_intersection(
        self, hex_grids: list, usrn_geometry
    ) -> list:
        """Filter hex grids to only those that intersect with the USRN geometry"""
        if not hex_grids or not usrn_geometry:
            return []

        intersecting_grids = []

        for grid in hex_grids:
            if grid["geometry"].intersects(usrn_geometry):
                intersecting_grids.append(grid)

        logger.debug(
            f"Filtered hex grids by USRN intersection: {len(hex_grids)} -> {len(intersecting_grids)}"
        )

        return intersecting_grids

    async def _fetch_nuar_data(self, endpoint: str) -> dict:
        """
        Asynchronous function to fetch data from NUAR API endpoint

        Args:
            endpoint: str - The NUAR API endpoint to fetch data from
        Returns:
            dict - The data from the endpoint
        """
        try:
            nuar_key = os.environ.get("NUAR_KEY")
            if not nuar_key:
                raise ValueError(
                    "NUAR_KEY environment variable is required for NUAR API access"
                )

            headers = {
                "Authorization": f"Bearer {nuar_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint, headers=headers) as response:
                    response.raise_for_status()
                    result = await response.json()
                    return result
        except aiohttp.ClientError as e:
            raise e
        except Exception as e:
            raise e

    async def _get_nuar_asset_count_with_usrn_clipping(
        self, usrn: str, zoom_level: str = ""
    ) -> Dict[str, Any]:
        """
        Get asset count from NUAR API with USRN geometry clipping for accurate results

        Args:
            usrn: str - USRN to get geometry for and clip assets
            zoom_level: str - NUAR zoom level (optional, uses instance default if not provided)

        Returns:
            Dict[str, Any] - Asset count data from NUAR API with clipping applied
        """

        zoom = zoom_level or self.nuar_zoom_level
        try:
            usrn_geometry = None
            try:
                bbox_coords = await self._get_bbox_from_usrn(
                    str(usrn), self.buffer_distance
                )
                bbox = f"{bbox_coords[0]},{bbox_coords[1]},{bbox_coords[2]},{bbox_coords[3]}"
                logger.debug(f"Calculated buffered bbox for USRN {usrn}: {bbox}")

                async with self.motherduck_pool.get_connection() as md_conn:
                    geometry_result = await asyncio.to_thread(
                        md_conn.execute,
                        """
                        SELECT geometry
                        FROM os_open_usrns.open_usrns_latest
                        WHERE usrn = ?
                        """,
                        [usrn],
                    )

                    geometry_data = geometry_result.fetchall()
                    if geometry_data:
                        wkt_geometry = geometry_data[0][0]
                        usrn_geometry = loads(wkt_geometry)
                        logger.debug("Successfully loaded USRN geometry for clipping")

            except Exception as e:
                logger.error(f"Error getting USRN geometry for clipping: {str(e)}")
                bbox = ""

            if not bbox:
                return {
                    "error": "Failed to calculate bbox from USRN",
                    "asset_count": None,
                    "usrn": usrn,
                }

            # Use configurable zoom level in endpoint
            endpoint = (
                f"{self.nuar_base_url}metrics/AssetCount/nuar/{zoom}/?bbox={bbox}"
            )

            logger.debug(
                f"Fetching NUAR asset count for USRN {usrn} with bbox: {bbox}, zoom: {zoom}"
            )
            logger.debug(f"NUAR API endpoint: {endpoint}")

            nuar_result = await self._fetch_nuar_data(endpoint)

            if (
                nuar_result
                and "data" in nuar_result
                and "collectionItems" in nuar_result["data"]
            ):
                collection_items = nuar_result["data"]["collectionItems"]
                logger.debug(
                    f"Retrieved {len(collection_items)} hex grids from NUAR API at zoom {zoom}"
                )

                hex_grids = self._create_hex_grids_from_nuar_data(collection_items)

                if hex_grids and usrn_geometry:
                    intersecting_grids = self._filter_hex_grids_by_usrn_intersection(
                        hex_grids, usrn_geometry
                    )

                    total_asset_count = sum(
                        grid["asset_count"] for grid in intersecting_grids
                    )

                    logger.debug(
                        f"USRN intersection filtering: {len(hex_grids)} total grids -> {len(intersecting_grids)} intersecting grids"
                    )
                    logger.debug(
                        f"Total asset count after USRN clipping: {total_asset_count}"
                    )

                    return {
                        "total_asset_count": total_asset_count,
                        "total_grids": len(hex_grids),
                        "intersecting_grids": len(intersecting_grids),
                        "hex_grids": intersecting_grids,
                        "bbox": bbox,
                        "usrn": usrn,
                        "zoom_level": zoom,
                        "clipping_applied": True,
                    }
                elif hex_grids:
                    total_asset_count = 0
                    logger.warning(
                        "No USRN geometry available for clipping - count set to 0"
                    )

                    return {
                        "total_asset_count": total_asset_count,
                        "total_grids": len(hex_grids),
                        "intersecting_grids": 0,
                        "hex_grids": [],
                        "bbox": bbox,
                        "usrn": usrn,
                        "zoom_level": zoom,
                        "clipping_applied": False,
                    }
                else:
                    return {
                        "total_asset_count": 0,
                        "total_grids": 0,
                        "intersecting_grids": 0,
                        "hex_grids": [],
                        "bbox": bbox,
                        "usrn": usrn,
                        "zoom_level": zoom,
                        "clipping_applied": False,
                    }
            else:
                logger.warning("Invalid NUAR API response structure")
                return {
                    "error": "Invalid NUAR API response",
                    "asset_count": None,
                    "bbox": bbox,
                    "usrn": usrn,
                    "zoom_level": zoom,
                }

        except Exception as e:
            logger.error(f"Error fetching NUAR asset count with clipping: {str(e)}")
            return {
                "error": f"Failed to fetch NUAR asset count: {str(e)}",
                "asset_count": None,
                "usrn": usrn,
                "zoom_level": zoom,
            }

    async def _get_asset_count_in_buffer(
        self, project_id: str, zoom_level: str = ""
    ) -> Optional[Dict]:
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

            logger.debug(f"Processing asset data for USRN: {usrn}")

            nuar_data = await self._get_nuar_asset_count_with_usrn_clipping(
                str(usrn), zoom_level
            )

            bbox = nuar_data.get("bbox", "")

            return {
                "project_id": project_id,
                "usrn": usrn,
                "start_date": start_date,
                "completion_date": completion_date,
                "duration_days": duration_days,
                "bbox": bbox,
                "nuar_asset_data": nuar_data,
            }

        except Exception as e:
            raise Exception(
                f"Error fetching asset data for project {project_id}: {str(e)}"
            )
        finally:
            postgres_conn.close()

    async def calculate_impact(
        self, project_id: str, zoom_level: str = ""
    ) -> AssetResponse:
        """
        Calculate the asset network impact metric using USRN geometry data and NUAR API with clipping.

        Args:
            project_id: Project ID string (pre-validated by middleware)
            zoom_level: NUAR zoom level (optional)

        Returns:
            AssetResponse object with asset metrics including bbox and NUAR asset count
        """
        asset_data = await self._get_asset_count_in_buffer(project_id, zoom_level)

        if not asset_data:
            raise ValueError(f"No asset data found for project {project_id}")

        duration_days = asset_data["duration_days"]
        usrn = asset_data["usrn"]
        bbox = asset_data["bbox"]
        nuar_data = asset_data["nuar_asset_data"]

        asset_count = 0
        intersecting_hex_grids = []
        clipping_applied = False

        if nuar_data and not nuar_data.get("error"):
            if "total_asset_count" in nuar_data:
                asset_count = nuar_data["total_asset_count"]
                clipping_applied = nuar_data.get("clipping_applied", False)
                intersecting_grids_count = nuar_data.get("intersecting_grids", 0)
                total_grids = nuar_data.get("total_grids", 0)
                hex_grids_data = nuar_data.get("hex_grids", [])

                asset_density = (
                    asset_count / intersecting_grids_count
                    if intersecting_grids_count > 0
                    else 0
                )

                intersecting_hex_grids = [
                    {
                        "grid_id": grid["grid_id"],
                        "asset_count": grid["asset_count"],
                        "zoom_level": grid["zoom_level"],
                        "easting": grid["easting"],
                        "northing": grid["northing"],
                    }
                    for grid in hex_grids_data
                ]

                logger.info(
                    f"Using NUAR asset count with{'out' if not clipping_applied else ''} USRN clipping: {asset_count}"
                )
                logger.info(
                    f"Grids: {intersecting_grids_count}/{total_grids} intersecting with USRN"
                )
                logger.debug(
                    f"Intersecting hex grids: {[grid['grid_id'] for grid in intersecting_hex_grids]}"
                )
            else:
                intersecting_grids_count = 0
                asset_density = 0
                logger.warning("NUAR data missing total_asset_count field")
        else:
            intersecting_grids_count = 0
            asset_density = 0
            logger.warning(
                f"NUAR data unavailable or has error: {nuar_data.get('error', 'Unknown error')}"
            )

        response = AssetResponse(
            success=True,
            project_id=project_id,
            project_duration_days=duration_days,
            usrn=usrn,
            asset_count=asset_count,
            hex_grid_count=intersecting_grids_count,
            asset_density=asset_density,
            bbox=bbox,
            clipping_applied=clipping_applied,
            intersecting_hex_grids=intersecting_hex_grids,
        )

        return response
