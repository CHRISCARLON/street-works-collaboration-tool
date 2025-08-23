from typing import Optional, AsyncGenerator, Deque
import asyncio
from collections import deque
from contextlib import asynccontextmanager
import asyncpg
from loguru import logger
import os


class PostgresPool:
    """PostgreSQL connection pool"""

    _instance: Optional["PostgresPool"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, "initialized"):
            # Build connection string from environment variables
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            db = os.getenv("POSTGRES_DB", "collaboration_tool")
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "password")

            self.database_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

            self._parse_connection_string()

            self._connections: Deque[asyncpg.Connection] = deque()
            self._max_connections = 10
            self._min_connections = 2

            self._connection_semaphore = asyncio.Semaphore(self._max_connections)
            self._connections_lock = asyncio.Lock()
            self._creation_lock = asyncio.Lock()
            self.initialized = True

    def _parse_connection_string(self):
        """Parse the database URL for asyncpg connection parameters"""
        url = self.database_url.replace("postgresql://", "")

        if "@" in url:
            credentials, host_info = url.split("@")
            if ":" in credentials:
                self.user, self.password = credentials.split(":")
            else:
                self.user = credentials
                self.password = None
        else:
            host_info = url
            self.user = "postgres"
            self.password = None

        if "/" in host_info:
            host_port, self.database = host_info.split("/")
        else:
            host_port = host_info
            self.database = "postgres"

        if ":" in host_port:
            self.host, port_str = host_port.split(":")
            self.port = int(port_str)
        else:
            self.host = host_port
            self.port = 5432

    async def _create_connection(self) -> asyncpg.Connection:
        """Create a new PostgreSQL connection"""
        async with self._creation_lock:
            try:
                conn = await asyncpg.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    timeout=10,
                    command_timeout=10,
                )

                logger.debug("Created new PostgreSQL connection")
                return conn
            except Exception as e:
                logger.error(f"Failed to create PostgreSQL connection: {e}")
                raise

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get a connection from the pool"""
        await self._connection_semaphore.acquire()

        connection = None
        try:
            async with self._connections_lock:
                if self._connections:
                    connection = self._connections.popleft()
                    logger.debug("Reusing existing PostgreSQL connection")

            if connection is None:
                connection = await self._create_connection()
                logger.debug("Created new PostgreSQL connection")

            await connection.fetchval("SELECT 1")
            yield connection

            async with self._connections_lock:
                if len(self._connections) < self._max_connections:
                    self._connections.append(connection)
                    logger.debug("Returned PostgreSQL connection to pool")
                else:
                    await connection.close()
                    logger.debug("Closed excess PostgreSQL connection")

        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            if connection:
                await connection.close()
            raise
        finally:
            self._connection_semaphore.release()

    async def execute(self, query: str, *args):
        """Execute a query that doesn't return results"""
        async with self.get_connection() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        """Execute a query and fetch all results"""
        async with self.get_connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Execute a query and fetch a single row"""
        async with self.get_connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        """Execute a query and fetch a single value"""
        async with self.get_connection() as conn:
            return await conn.fetchval(query, *args)

    async def initialize_pool(self):
        """Initialize the connection pool with minimum connections"""
        for _ in range(self._min_connections):
            conn = await self._create_connection()
            async with self._connections_lock:
                self._connections.append(conn)
        logger.info(
            f"Initialized PostgreSQL pool with {self._min_connections} connections"
        )

    async def close_all(self):
        """Close all connections in the pool"""
        async with self._connections_lock:
            while self._connections:
                conn = self._connections.popleft()
                await conn.close()
            logger.debug("Closed all PostgreSQL connections in pool")
