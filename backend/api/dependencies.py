from functools import lru_cache
from backend.db_pool.duckdb_pool import MotherDuckPool, DuckDBPool
from backend.db_pool.postgres_pool import PostgresPool
from backend.services.metrics import (
    Wellbeing,
    Households,
    BusNetwork,
    RoadNetwork,
    AssetNetwork,
    WorkHistory,
    Section58History,
    BdukHistory,
)


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
def get_households_service() -> Households:
    """Get Households service"""
    return Households(get_motherduck_pool(), get_duckdb_pool())


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


@lru_cache()
def get_section58_service() -> Section58History:
    """Get Section58History service"""
    return Section58History(get_motherduck_pool(), get_duckdb_pool())


@lru_cache()
def get_bduk_service() -> BdukHistory:
    """Get BdukHistory service"""
    return BdukHistory(get_motherduck_pool(), get_duckdb_pool())
