from typing import List, Optional, AsyncGenerator, Deque
import asyncio
import duckdb
from loguru import logger
import os
from contextlib import asynccontextmanager
from collections import deque


class MotherDuckPool:
    """Improved MotherDuck connection pool using semaphores for better concurrency"""

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

            self.connection_string = f"md:{self.database}?motherduck_token={self.token}&access_mode=read_only"
            self._connections: Deque[duckdb.DuckDBPyConnection] = deque()
            self._max_connections = 5

            # Semaphore to limit concurrent access to available connections
            self._connection_semaphore = asyncio.Semaphore(self._max_connections)
            # Lock only for modifying the connections deque
            self._connections_lock = asyncio.Lock()
            # Lock for creating connections to avoid race conditions with extensions
            self._creation_lock = asyncio.Lock()

            self.initialized = True

    async def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new MotherDuck connection"""
        async with self._creation_lock:
            try:
                conn = await asyncio.to_thread(duckdb.connect, self.connection_string)

                await asyncio.to_thread(conn.execute, "INSTALL spatial")
                await asyncio.to_thread(conn.execute, "LOAD spatial")

                logger.debug("Created new MotherDuck connection with spatial extension")
                return conn
            except Exception as e:
                logger.error(f"Failed to create connection: {e}")
                raise

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[duckdb.DuckDBPyConnection, None]:
        """Get a connection from the pool using semaphore-based approach"""
        # Wait for a connection slot to become available
        await self._connection_semaphore.acquire()

        connection = None
        try:
            # Quick access to get an existing connection
            async with self._connections_lock:
                if self._connections:
                    connection = self._connections.popleft()
                    logger.debug("Reusing existing MotherDuck connection")

            # If no existing connection, create a new one
            if connection is None:
                connection = await self._create_connection()
                logger.debug("Created new MotherDuck connection")

            # Test the connection
            await asyncio.to_thread(connection.execute, "SELECT 1")
            yield connection

            # Return connection to pool
            async with self._connections_lock:
                self._connections.append(connection)
                logger.debug("Returned MotherDuck connection to pool")

        except Exception as e:
            logger.error(f"MotherDuck connection error: {e}")
            if connection:
                await asyncio.to_thread(connection.close)
            raise
        finally:
            # Always release the semaphore
            self._connection_semaphore.release()

    async def close_all(self):
        """Close all connections in the pool"""
        async with self._connections_lock:
            while self._connections:
                conn = self._connections.popleft()
                await asyncio.to_thread(conn.close)
            logger.debug("Closed all MotherDuck connections in pool")


class DuckDBPool:
    """Improved DuckDB connection pool using semaphores for better concurrency"""

    _instance: Optional["DuckDBPool"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_url: Optional[str] = None) -> None:
        if not hasattr(self, "initialized"):
            self.db_url = db_url or os.getenv(
                "DATABASE_URL",
                "postgresql://postgres:password@localhost:5432/collaboration_tool"
            )
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
                    f"ATTACH '{self.db_url}' AS postgres_db (TYPE postgres)"
                )

                logger.debug("Created new local DuckDB connection with postgres and spatial extensions")
                return conn
            except Exception as e:
                logger.error(f"Failed to create DuckDB connection: {e}")
                raise

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[duckdb.DuckDBPyConnection, None]:
        """Get a connection from the pool using semaphore-based approach"""
        # Wait for a connection slot to become available
        await self._connection_semaphore.acquire()

        connection = None
        try:
            # Quick access to get an existing connection
            async with self._connections_lock:
                if self._connections:
                    connection = self._connections.popleft()
                    logger.debug("Reusing existing DuckDB connection")

            # If no existing connection, create a new one
            if connection is None:
                connection = await self._create_connection()
                logger.debug("Created new DuckDB connection")

            # Test the connection
            await asyncio.to_thread(connection.execute, "SELECT 1")
            yield connection

            # Return connection to pool
            async with self._connections_lock:
                self._connections.append(connection)
                logger.debug("Returned DuckDB connection to pool")

        except Exception as e:
            logger.error(f"DuckDB connection error: {e}")
            if connection:
                await asyncio.to_thread(connection.close)
            raise
        finally:
            # Always release the semaphore
            self._connection_semaphore.release()

    async def close_all(self):
        """Close all connections in the pool"""
        async with self._connections_lock:
            while self._connections:
                conn = self._connections.popleft()
                await asyncio.to_thread(conn.close)
            logger.debug("Closed all DuckDB connections in pool")
