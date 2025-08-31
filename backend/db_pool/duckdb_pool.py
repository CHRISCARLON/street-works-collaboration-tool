from typing import Optional, AsyncGenerator, Deque
import asyncio
import duckdb
from loguru import logger
import os
from contextlib import asynccontextmanager
from collections import deque


class MotherDuckPool:
    """MotherDuck connection pool"""

    _instance: Optional["MotherDuckPool"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, "initialized"):
            self.database = os.getenv("MD_DB")
            self.token = os.getenv("MD_TOKEN")

            if not all([self.database, self.token]):
                raise ValueError("MotherDuck environment variables are not present")

            self.connection_string = f"md:{self.database}?motherduck_token={self.token}"
            self._connections: Deque[duckdb.DuckDBPyConnection] = deque()
            self._max_connections = 5

            self._connection_semaphore = asyncio.Semaphore(self._max_connections)
            self._connections_lock = asyncio.Lock()
            self._creation_lock = asyncio.Lock()
            self.initialized = True

    async def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new MotherDuck connection"""
        async with self._creation_lock:
            try:
                conn = await asyncio.to_thread(duckdb.connect, self.connection_string)

                await asyncio.to_thread(conn.execute, "INSTALL spatial")
                await asyncio.to_thread(conn.execute, "LOAD spatial")
                return conn
            except Exception as e:
                logger.error(f"Failed to create connection: {e}")
                raise

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[duckdb.DuckDBPyConnection, None]:
        """Get a connection from the pool using semaphore-based approach"""
        await self._connection_semaphore.acquire()

        connection = None
        try:
            async with self._connections_lock:
                if self._connections:
                    connection = self._connections.popleft()
                    logger.debug("Reusing existing MotherDuck connection")

            if connection is None:
                connection = await self._create_connection()
                logger.debug("Created new MotherDuck connection")

            await asyncio.to_thread(connection.execute, "SELECT 1")
            yield connection

            async with self._connections_lock:
                self._connections.append(connection)
                logger.debug("Returned MotherDuck connection to pool")

        except Exception as e:
            logger.error(f"MotherDuck connection error: {e}")
            if connection:
                await asyncio.to_thread(connection.close)
            raise
        finally:
            self._connection_semaphore.release()

    async def close_all(self):
        """Close all connections in the pool"""
        async with self._connections_lock:
            while self._connections:
                conn = self._connections.popleft()
                await asyncio.to_thread(conn.close)
            logger.debug("Closed all MotherDuck connections in pool")


class DuckDBPool:
    """DuckDB connection pool"""

    _instance: Optional["DuckDBPool"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_url: Optional[str] = None) -> None:
        if not hasattr(self, "initialized"):
            if db_url:
                self.db_url = db_url
            else:
                host = os.getenv("POSTGRES_HOST", "localhost")
                port = os.getenv("POSTGRES_PORT", "5432")
                db = os.getenv("POSTGRES_DB", "collaboration_tool")
                user = os.getenv("POSTGRES_USER", "postgres")
                password = os.getenv("POSTGRES_PASSWORD", "password")

                self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
            self._connections: Deque[duckdb.DuckDBPyConnection] = deque()
            self._max_connections = 5

            self._connection_semaphore = asyncio.Semaphore(self._max_connections)
            self._connections_lock = asyncio.Lock()
            self._creation_lock = asyncio.Lock()
            self.initialized = True

    async def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new local DuckDB connection with PostgreSQL and spatial extensions"""
        async with self._creation_lock:
            try:
                conn = await asyncio.to_thread(duckdb.connect, ":memory:")

                await asyncio.to_thread(conn.execute, "INSTALL postgres")
                await asyncio.to_thread(conn.execute, "LOAD postgres")

                await asyncio.to_thread(conn.execute, "INSTALL spatial")
                await asyncio.to_thread(conn.execute, "LOAD spatial")

                await asyncio.to_thread(
                    conn.execute,
                    f"ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)",
                )
                return conn
            except Exception as e:
                logger.error(f"Failed to create DuckDB connection: {e}")
                raise

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[duckdb.DuckDBPyConnection, None]:
        """Get a connection from the pool using semaphore-based approach"""

        await self._connection_semaphore.acquire()

        connection = None
        try:
            async with self._connections_lock:
                if self._connections:
                    connection = self._connections.popleft()
                    logger.debug("Reusing existing DuckDB connection")

            if connection is None:
                connection = await self._create_connection()
                logger.debug("Created new DuckDB connection")

            await asyncio.to_thread(connection.execute, "SELECT 1")
            yield connection

            async with self._connections_lock:
                self._connections.append(connection)
                logger.debug("Returned DuckDB connection to pool")

        except Exception as e:
            logger.error(f"DuckDB connection error: {e}")
            if connection:
                await asyncio.to_thread(connection.close)
            raise
        finally:
            self._connection_semaphore.release()

    async def close_all(self):
        """Close all connections in the pool"""
        async with self._connections_lock:
            while self._connections:
                conn = self._connections.popleft()
                await asyncio.to_thread(conn.close)
            logger.debug("Closed all DuckDB connections in pool")
