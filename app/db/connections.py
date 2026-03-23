"""Database connection management - connection pooling for all service databases and metadata database."""

import logging
from typing import Dict, Optional

import asyncpg
from app.exceptions import DBError


from app.config import Settings, get_settings

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages async connection pools for all enabled service databases."""

    def __init__(self, settings: Optional[Settings] = None):
        """Initialize connection manager with settings."""
        self.settings = settings or get_settings()
        self._pools: Dict[str, asyncpg.Pool] = {}
        self._connection_strings: Dict[str, str] = {}

        # Preload connection strings from resolved config
        # Note: config.py already resolves ${ENV_VAR} → actual value,
        # so connection_key IS the connection string, not an env var name.
        enabled_services = self.settings.get_enabled_services()
        for service_name, service_config in enabled_services.items():
            conn_str = service_config.connection_key
            if conn_str:
                self._connection_strings[service_name] = conn_str
            else:
                logger.warning(
                    f"Connection string not resolved for service '{service_name}'"
                )

    async def get_connection(self, service_name: str) -> asyncpg.Connection:
        """Get a connection from the pool for the specified service.

        Creates the pool lazily on first use.

        Args:
            service_name: Name of the service (e.g., "coredata", "crm")

        Returns:
            An async database connection

        Raises:
            ValueError: If service not found or disabled
            RuntimeError: If connection string not available
        """
        if service_name not in self.settings.services:
            raise ValueError(f"Unknown service: {service_name}")

        service_config = self.settings.services[service_name]
        if not service_config.enabled:
            raise ValueError(f"Service '{service_name}' is disabled")

        if service_name not in self._connection_strings:
            raise RuntimeError(
                f"Connection string not resolved for service '{service_name}'"
            )

        # Create pool lazily
        if service_name not in self._pools:
            conn_str = self._connection_strings[service_name]
            try:
                pool = await asyncpg.create_pool(
                    conn_str,
                    min_size=2,
                    max_size=10,
                    timeout=30,
                    command_timeout=60,
                    statement_cache_size=0  # Read-only, no need for prepared statements
                )
                self._pools[service_name] = pool
                logger.info(f"Created connection pool for service '{service_name}'")
            except Exception as e:
                logger.error(f"Failed to create connection pool for '{service_name}': {e}")
                raise DBError(f"Cannot connect to service '{service_name}'. Check configuration.", str(e))

        pool = self._pools[service_name]
        return await pool.acquire()

    async def release_connection(self, service_name: str, connection: asyncpg.Connection) -> None:
        """Release a connection back to the pool.

        Args:
            service_name: Name of the service
            connection: Connection to release
        """
        if service_name in self._pools:
            await self._pools[service_name].release(connection)

    def get_sync_connection_string(self, service_name: str) -> str:
        """Get the raw connection string for a service.

        Used for synchronous database operations (e.g., Dapper-style queries).

        Args:
            service_name: Name of the service

        Returns:
            Connection string

        Raises:
            ValueError: If service not found or disabled
            RuntimeError: If connection string not available
        """
        if service_name not in self.settings.services:
            raise ValueError(f"Unknown service: {service_name}")

        service_config = self.settings.services[service_name]
        if not service_config.enabled:
            raise ValueError(f"Service '{service_name}' is disabled")

        if service_name not in self._connection_strings:
            raise RuntimeError(
                f"Connection string not resolved for service '{service_name}'"
            )

        return self._connection_strings[service_name]

    async def close_all(self) -> None:
        """Close all connection pools."""
        for service_name, pool in self._pools.items():
            try:
                await pool.close()
                logger.info(f"Closed connection pool for service '{service_name}'")
            except Exception as e:
                logger.error(f"Error closing pool for '{service_name}': {e}")

        self._pools.clear()

    def __del__(self):
        """Cleanup on deletion (best effort)."""
        if self._pools:
            logger.warning("ConnectionManager deleted with open pools. Call close_all() explicitly.")
