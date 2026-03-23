"""Universal database connector — creates read-only SQLAlchemy engines for any supported dialect."""

import logging
import os
import shutil
from pathlib import Path
from typing import Literal, cast
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import get_settings

logger = logging.getLogger(__name__)

DBDialect = Literal["sqlite", "postgresql", "mysql", "sqlserver"]


class ConnectionError(Exception):
    pass


class UnsupportedDialectError(Exception):
    pass


class UniversalConnector:
    """Creates read-only SQLAlchemy engines from file uploads or connection strings."""

    DIALECT_PREFIXES = {
        "sqlite":       ("sqlite:///", "sqlite+aiosqlite:///"),
        "postgresql":   ("postgresql://", "postgresql+psycopg2://", "postgres://"),
        "mysql":        ("mysql://", "mysql+pymysql://"),
        "sqlserver":    ("mssql://", "mssql+pyodbc://"),
    }

    def __init__(self):
        self.settings = get_settings()
        self._uploads_dir = Path(self.settings.uploads.dir)
        self._uploads_dir.mkdir(parents=True, exist_ok=True)

    def from_upload(self, session_id: str, file_bytes: bytes, filename: str) -> Engine:
        """Save uploaded SQLite file and create engine.

        Args:
            session_id: Unique session identifier
            file_bytes: Raw file content
            filename: Original filename (used for extension check)

        Returns:
            Read-only SQLAlchemy Engine

        Raises:
            ConnectionError: If file is invalid or connection fails
        """
        ext = Path(filename).suffix.lower()
        if ext not in self.settings.uploads.allowed_extensions:
            raise ConnectionError(
                f"Unsupported file type '{ext}'. "
                f"Allowed: {', '.join(self.settings.uploads.allowed_extensions)}"
            )

        max_bytes = self.settings.uploads.max_size_mb * 1024 * 1024
        if len(file_bytes) > max_bytes:
            raise ConnectionError(
                f"File too large ({len(file_bytes) / 1024 / 1024:.1f} MB). "
                f"Maximum allowed: {self.settings.uploads.max_size_mb} MB"
            )

        session_dir = self._uploads_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        db_path = session_dir / "database.db"

        try:
            db_path.write_bytes(file_bytes)
            logger.info(f"Saved uploaded file to {db_path} ({len(file_bytes)} bytes)")
        except OSError as e:
            raise ConnectionError(f"Failed to save uploaded file: {e}")

        url = f"sqlite:///{db_path.as_posix()}"
        return self._make_engine(url, "sqlite")

    def from_connection_string(self, conn_str: str) -> Engine:
        """Create engine from a connection string.

        Args:
            conn_str: SQLAlchemy-compatible connection string

        Returns:
            Read-only SQLAlchemy Engine

        Raises:
            UnsupportedDialectError: If dialect cannot be determined
            ConnectionError: If connection fails
        """
        conn_str = conn_str.strip()
        dialect = self._detect_dialect(conn_str)

        # Normalise postgres:// shorthand
        if conn_str.startswith("postgres://"):
            conn_str = conn_str.replace("postgres://", "postgresql+psycopg2://", 1)
        elif conn_str.startswith("postgresql://"):
            conn_str = conn_str.replace("postgresql://", "postgresql+psycopg2://", 1)
        elif conn_str.startswith("mysql://"):
            conn_str = conn_str.replace("mysql://", "mysql+pymysql://", 1)
        elif conn_str.startswith("mssql://"):
            conn_str = conn_str.replace("mssql://", "mssql+pyodbc://", 1)

        return self._make_engine(conn_str, dialect)

    def cleanup_session(self, session_id: str) -> None:
        """Delete uploaded files for a session.

        Args:
            session_id: Session identifier to clean up
        """
        session_dir = self._uploads_dir / session_id
        if session_dir.exists():
            shutil.rmtree(session_dir, ignore_errors=True)
            logger.info(f"Cleaned up uploads for session {session_id}")

    def _detect_dialect(self, conn_str: str) -> DBDialect:
        lower = conn_str.lower()
        for dialect, prefixes in self.DIALECT_PREFIXES.items():
            if any(lower.startswith(p) for p in prefixes):
                return cast(DBDialect, dialect)   # ← tell Pylance this str is a DBDialect
        raise UnsupportedDialectError(
            f"Cannot determine database type from connection string. "
            f"Supported: sqlite://, postgresql://, mysql://, mssql://"
        )

    def _make_engine(self, url: str, dialect: DBDialect) -> Engine:
        """Create a read-only SQLAlchemy engine and test the connection."""
        kwargs = {
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }

        if dialect == "sqlite":
            # SQLite: connect_args for read-only mode
            kwargs["connect_args"] = {"check_same_thread": False}
            kwargs["execution_options"] = {"compiled_cache": {}}
        else:
            kwargs["pool_size"] = 2
            kwargs["max_overflow"] = 3

        try:
            engine = create_engine(url, **kwargs)
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Connected to {dialect} database successfully")
            return engine
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
