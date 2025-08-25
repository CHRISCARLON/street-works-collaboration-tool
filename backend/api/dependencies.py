from functools import lru_cache
from backend.db_pool.duckdb_pool import MotherDuckPool, DuckDBPool
from backend.db_pool.postgres_pool import PostgresPool
from backend.services.metrics import (
    Wellbeing,
    BusNetwork,
    RoadNetwork,
    AssetNetwork,
    WorkHistory,
)


from typing import Annotated
from fastapi import Depends
from backend.api.dependencies import (
    get_bus_network_service,
    get_road_network_service,
    get_asset_network_service,
    get_work_history_service,
    get_wellbeing_service,
)

# Create type aliases for all your services
BusNetworkDep = Annotated[BusNetwork, Depends(get_bus_network_service)]
RoadNetworkDep = Annotated[RoadNetwork, Depends(get_road_network_service)]
AssetNetworkDep = Annotated[AssetNetwork, Depends(get_asset_network_service)]
WorkHistoryDep = Annotated[WorkHistory, Depends(get_work_history_service)]
WellbeingDep = Annotated[Wellbeing, Depends(get_wellbeing_service)]


@lru_cache()
def get_postgres_pool() -> PostgresPool:
    """Get PostgreSQL connection pool"""
    return PostgresPool()


@lru_cache()
def get_motherduck_pool() -> MotherDuckPool:
    """Get MotherDuck connection pool"""
    return MotherDuckPool()


@lru_cache()
def get_duckdb_pool() -> DuckDBPool:
    """Get DuckDB connection pool"""
    return DuckDBPool()


@lru_cache()
def get_wellbeing_service() -> Wellbeing:
    """Get Wellbeing service"""
    return Wellbeing(get_motherduck_pool(), get_duckdb_pool())


@lru_cache()
def get_bus_network_service() -> BusNetwork:
    """Get BusNetwork service"""
    return BusNetwork(get_motherduck_pool(), get_duckdb_pool())


@lru_cache()
def get_road_network_service() -> RoadNetwork:
    """Get RoadNetwork service"""
    return RoadNetwork(get_motherduck_pool(), get_duckdb_pool())


@lru_cache()
def get_asset_network_service() -> AssetNetwork:
    """Get AssetNetwork service"""
    return AssetNetwork(get_motherduck_pool(), get_duckdb_pool())


@lru_cache()
def get_work_history_service() -> WorkHistory:
    """Get Work History service"""
    return WorkHistory(get_motherduck_pool(), get_duckdb_pool())
