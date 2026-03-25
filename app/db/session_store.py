"""Session store — in-memory registry mapping session_id to DB engine and metadata."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Any, List
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class SessionContext:
    """All runtime data for a connected database session."""
    session_id: str
    engine: Engine
    dialect: str
    db_name: str
    source_type: str          # "upload" | "connection_string"
    table_count: int
    qdrant_collection: str    # "session_{session_id}"
    db_key: str = ""          # SHA256 of file bytes or connection string — stable across sessions
    tables: List[Any] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    training_complete: bool = False
    connection_string: Optional[str] = None
    common_filter_keys: List[str] = field(default_factory=list)
    session_filter_values: Dict[str, Any] = field(default_factory=dict)
    is_owner: bool = False


class SessionStore:
    """Thread-safe in-memory session registry."""

    def __init__(self, ttl_hours: int = 4, max_sessions: int = 50):
        self._sessions: Dict[str, SessionContext] = {}
        self._ttl_seconds = ttl_hours * 3600
        self._max_sessions = max_sessions
        self._lock = asyncio.Lock()

    async def register(self, ctx: SessionContext) -> None:
        """Register a new session.

        Args:
            ctx: SessionContext to store
        """
        logger.info(f"Registering session {ctx}")
        async with self._lock:
            if len(self._sessions) >= self._max_sessions:
                await self._evict_oldest()
            self._sessions[ctx.session_id] = ctx
            logger.info(
                f"Registered session {ctx.session_id} "
                f"({ctx.dialect}, {ctx.table_count} tables)"
            )

    async def get(self, session_id: str) -> Optional[SessionContext]:
        """Get a session context, updating last_used.

        Args:
            session_id: Session identifier

        Returns:
            SessionContext or None if not found / expired
        """
        async with self._lock:
            ctx = self._sessions.get(session_id)
            if ctx is None:
                return None
            if time.time() - ctx.last_used > self._ttl_seconds:
                del self._sessions[session_id]
                logger.info(f"Session {session_id} expired and evicted")
                return None
            ctx.last_used = time.time()
            return ctx

    async def mark_trained(self, session_id: str, table_count: int) -> None:
        """Mark a session's training as complete and update table count."""
        async with self._lock:
            ctx = self._sessions.get(session_id)
            if ctx:
                ctx.training_complete = True
                ctx.table_count = table_count

    async def evict(self, session_id: str) -> Optional[SessionContext]:
        """Remove and return a session context.

        Args:
            session_id: Session to evict

        Returns:
            The evicted SessionContext (caller should dispose engine)
        """
        async with self._lock:
            return self._sessions.pop(session_id, None)

    async def cleanup_expired(self) -> int:
        """Remove all expired sessions.

        Returns:
            Number of sessions removed
        """
        async with self._lock:
            now = time.time()
            expired = [
                sid for sid, ctx in self._sessions.items()
                if now - ctx.last_used > self._ttl_seconds
            ]
            for sid in expired:
                del self._sessions[sid]
            if expired:
                logger.info(f"Cleaned up {len(expired)} expired sessions")
            return len(expired)

    async def _evict_oldest(self) -> None:
        """Evict the least recently used session."""
        if not self._sessions:
            return
        oldest_id = min(self._sessions, key=lambda sid: self._sessions[sid].last_used)
        del self._sessions[oldest_id]
        logger.warning(f"Max sessions reached — evicted LRU session {oldest_id}")

    def count(self) -> int:
        return len(self._sessions)


# Global singleton
_store: Optional[SessionStore] = None


def get_session_store() -> SessionStore:
    global _store
    if _store is None:
        from app.config import get_settings
        s = get_settings()
        _store = SessionStore(
            ttl_hours=s.session.ttl_hours,
            max_sessions=s.session.max_concurrent,
        )
    return _store
