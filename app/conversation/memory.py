"""Conversation memory - stores and retrieves conversation history, manages context window for Gemini."""

import json
import logging
import time
import pathlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 3600  # 1 hour
MAX_SESSIONS = 10000  # Hard cap on total sessions in memory


@dataclass
class ConversationTurn:
    """Single conversation turn with question, SQL, and result summary."""
    question: str
    sql_generated: Optional[str]
    result_summary: Optional[str]  # column names, row count, sample values
    services_used: List[str]
    filters_applied: List[str]  # e.g., ["AgencyId = 42"]
    result_sample: list = field(default_factory=list)


class ConversationMemory:
    """In-memory conversation history manager, limited to last N turns."""

    def __init__(self, max_turns: int = 4):
        """Initialize conversation memory with a maximum turn limit.

        Args:
            max_turns: Maximum number of turns to retain. Defaults to 10.
        """
        self._max_turns = max_turns
        self._turns: List[ConversationTurn] = []
        self._pending_clarification: Optional[dict] = None

    def add_turn(self, turn: ConversationTurn) -> None:
        """Add a turn to history. Drops oldest if over limit.

        Args:
            turn: The conversation turn to add.
        """
        self._turns.append(turn)
        if len(self._turns) > self._max_turns:
            self._turns.pop(0)

    def get_history(self) -> List[ConversationTurn]:
        """Get all turns in chronological order.

        Returns:
            List of conversation turns.
        """
        return self._turns.copy()

    def get_context_for_prompt(self) -> str:
        if not self._turns:
            return ""

        lines = ["Previous conversation:"]
        for idx, turn in enumerate(self._turns, start=1):
            lines.append(f"[Turn {idx}] User asked: \"{turn.question}\"")
            lines.append(f"  SQL used: {turn.sql_generated}")

            parts = []
            if turn.services_used:
                parts.append(f"Queried: {', '.join(turn.services_used)}")
            if turn.result_summary:
                parts.append(turn.result_summary)
            if turn.filters_applied:
                parts.append(f"Filters: {', '.join(turn.filters_applied)}")
            if parts:
                lines.append(f"  Result: {' | '.join(parts)}")

            # ✅ Include actual row data so follow-ups can reference real values
            if turn.result_sample:
                lines.append(f"  Sample data from result:")
                for row in turn.result_sample:
                    lines.append(f"    {row}")

        return "\n".join(lines)

    def clear(self) -> None:
        """Clear all history."""
        self._turns.clear()

    def set_pending_clarification(self, data: dict) -> None:
        existing = getattr(self, "_pending_clarification", None)
        
        # Never overwrite a real clarification with an empty/unknown one
        if existing and existing.get("entity_name") and not data.get("entity_name"):
            logger.warning(
                f"[memory] Refusing to overwrite valid clarification "
                f"'{existing}' with empty one '{data}'"
            )
            return
        
        self._pending_clarification = data
        logger.debug(f"[memory] Pending clarification set: {data}")
    
    
    def get_pending_clarification(self) -> Optional[dict]:
        """Return stored clarification context, or None if there isn't one."""
        return getattr(self, "_pending_clarification", None)
    
    
    def clear_pending_clarification(self) -> None:
        """Remove the pending clarification after it has been resolved."""
        self._pending_clarification = None
        logger.debug("[memory] Pending clarification cleared.")

    def pop_pending_clarification(self) -> Optional[dict]:
        """
        Atomically read AND clear pending clarification in one operation.
        This prevents race conditions where two requests both read it
        before either clears it.
        """
        data = getattr(self, "_pending_clarification", None)
        self._pending_clarification = None  # clear immediately in same operation
        logger.debug(f"[memory] Pending clarification popped: {data}")
        return data

# Session-based memory store (in-memory, not persisted across server restarts)
# Each entry: {"memory": ConversationMemory, "last_accessed": float}
_session_memories: Dict[str, Dict] = {}

MEMORY_DIR = pathlib.Path("uploads/session_store")
MEMORY_DIR.mkdir(exist_ok=True, parents=True)

def _load_memory_from_disk(session_id: str, memory: ConversationMemory) -> bool:
    path = MEMORY_DIR / f"{session_id}_memory.json"
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        memory.clear()
        for turn_data in data.get("turns", []):
            memory.add_turn(ConversationTurn(
                question=turn_data.get("question", ""),
                sql_generated=turn_data.get("sql_generated"),
                result_summary=turn_data.get("result_summary"),
                services_used=turn_data.get("services_used", []),
                filters_applied=turn_data.get("filters_applied", []),
                result_sample=turn_data.get("result_sample", [])
            ))
        memory._pending_clarification = data.get("pending_clarification")
        return True
    except Exception as e:
        logger.error(f"Failed to load memory from disk for {session_id}: {e}")
        return False

def save_session_memory(session_id: str):
    """Save the in-memory session to disk for multi-worker sync."""
    if session_id not in _session_memories:
        return
    memory: ConversationMemory = _session_memories[session_id]["memory"]
    path = MEMORY_DIR / f"{session_id}_memory.json"
    try:
        data = {
            "turns": [
                {
                    "question": turn.question,
                    "sql_generated": turn.sql_generated,
                    "result_summary": turn.result_summary,
                    "services_used": turn.services_used,
                    "filters_applied": turn.filters_applied,
                    "result_sample": turn.result_sample,
                }
                for turn in memory.get_history()
            ],
            "pending_clarification": memory.get_pending_clarification()
        }
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to save memory to disk for {session_id}: {e}")



def _cleanup_expired_sessions() -> None:
    """Remove sessions older than SESSION_TTL_SECONDS."""
    now = time.time()
    expired = [
        sid for sid, data in _session_memories.items()
        if now - data["last_accessed"] > SESSION_TTL_SECONDS
    ]
    for sid in expired:
        del _session_memories[sid]
    if expired:
        logger.debug(f"Cleaned up {len(expired)} expired sessions")


def get_session_memory(session_id: str, max_turns: int = 4) -> ConversationMemory:
    """Get or create a conversation memory for a session.

    Args:
        session_id: Unique session identifier.
        max_turns: Maximum turns to retain. Defaults to 10.

    Returns:
        ConversationMemory instance for the session.
    """
    # Periodic cleanup (every access, cheap O(n) scan)
    if len(_session_memories) > MAX_SESSIONS:
        _cleanup_expired_sessions()

    if session_id not in _session_memories:
        memory = ConversationMemory(max_turns=max_turns)
        _load_memory_from_disk(session_id, memory)
        _session_memories[session_id] = {
            "memory": memory,
            "last_accessed": time.time()
        }
    else:
        memory = _session_memories[session_id]["memory"]
        _load_memory_from_disk(session_id, memory)
        _session_memories[session_id]["last_accessed"] = time.time()

    return _session_memories[session_id]["memory"]


def clear_session_memory(session_id: str) -> None:
    """Clear conversation history for a session.

    Args:
        session_id: Unique session identifier.
    """
    if session_id in _session_memories:
        _session_memories[session_id]["memory"].clear()
    path = MEMORY_DIR / f"{session_id}_memory.json"
    if path.exists():
        path.unlink(missing_ok=True)


def delete_session_memory(session_id: str) -> None:
    """Delete conversation memory for a session entirely.

    Args:
        session_id: Unique session identifier.
    """
    if session_id in _session_memories:
        del _session_memories[session_id]
    path = MEMORY_DIR / f"{session_id}_memory.json"
    if path.exists():
        path.unlink(missing_ok=True)
