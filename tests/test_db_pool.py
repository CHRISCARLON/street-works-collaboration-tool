import pytest
import asyncio
import threading
from backend.db_pool.duckdb_pool import MotherDuckPool, DuckDBPool


def test_motherduck_singleton():
    """Test that MotherDuckPool is a true singleton"""
    pool1 = MotherDuckPool()
    pool2 = MotherDuckPool()
    pool3 = MotherDuckPool()

    print("")
    print(pool1)
    print(pool2)
    print(pool3)

    assert pool1 is pool2
    assert pool2 is pool3
    assert id(pool1) == id(pool2) == id(pool3)

    assert pool1._connections is pool2._connections
    assert pool1._connection_semaphore is pool2._connection_semaphore
    assert pool1._max_connections == pool2._max_connections

    assert hasattr(pool1, "_connections")
    assert hasattr(pool1, "_connection_semaphore")
    assert pool1._max_connections > 0


def test_duckdb_singleton():
    """Test that DuckDBPool is a true singleton"""
    pool1 = DuckDBPool()
    pool2 = DuckDBPool()

    print("")
    print(pool1)
    print(pool2)

    assert pool1 is pool2
    assert id(pool1) == id(pool2)

    assert pool1._connections is pool2._connections
    assert pool1.db_url == pool2.db_url

    assert hasattr(pool1, "_connections")
    assert hasattr(pool1, "db_url")


def test_motherduck_thread_safety():
    """Test MotherDuckPool singleton behavior under concurrent access"""
    results = []
    lock = threading.Lock()

    def create_pool():
        pool = MotherDuckPool()
        with lock:
            results.append(id(pool))

    threads = [threading.Thread(target=create_pool) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(set(results)) == 1, f"Got different instances: {set(results)}"
    assert len(results) == 10


def test_duckdb_thread_safety():
    """Test DuckDBPool singleton behavior under concurrent access"""
    results = []
    lock = threading.Lock()

    def create_pool():
        pool = DuckDBPool()
        with lock:
            results.append(id(pool))

    threads = [threading.Thread(target=create_pool) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len(set(results)) == 1, f"Got different instances: {set(results)}"
    assert len(results) == 10


@pytest.mark.asyncio
async def test_motherduck_async_singleton():
    """Test MotherDuckPool singleton in async context"""

    async def create_pool():
        return MotherDuckPool()

    tasks = [create_pool() for _ in range(8)]
    pools = await asyncio.gather(*tasks)

    assert all(pool is pools[0] for pool in pools)
    assert len(set(id(pool) for pool in pools)) == 1


@pytest.mark.asyncio
async def test_duckdb_async_singleton():
    """Test DuckDBPool singleton in async context"""

    async def create_pool():
        return DuckDBPool()

    tasks = [create_pool() for _ in range(8)]
    pools = await asyncio.gather(*tasks)

    assert all(pool is pools[0] for pool in pools)
    assert len(set(id(pool) for pool in pools)) == 1


def test_pool_attributes_consistency():
    """Test that pool attributes remain consistent across instances"""
    motherduck_pool = MotherDuckPool()
    duckdb_pool = DuckDBPool()

    motherduck_pool2 = MotherDuckPool()
    duckdb_pool2 = DuckDBPool()

    assert motherduck_pool._max_connections == motherduck_pool2._max_connections
    assert motherduck_pool._connections is motherduck_pool2._connections

    assert duckdb_pool.db_url == duckdb_pool2.db_url
    assert duckdb_pool._connections is duckdb_pool2._connections


def test_singleton_after_deletion():
    """Test that singleton persists even after attempting deletion"""
    pool1 = MotherDuckPool()
    pool1_id = id(pool1)

    del pool1

    pool2 = MotherDuckPool()
    pool2_id = id(pool2)

    assert pool1_id == pool2_id
