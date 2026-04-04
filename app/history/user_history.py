import json
import logging
import pathlib
from datetime import datetime, timezone
from typing import Optional
 
logger = logging.getLogger(__name__)
 
# ── Root folder ──────────────────────────────────────────────────────────────
USER_HISTORY_DIR = pathlib.Path("uploads/user_history")
USER_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
 
 
# ── Public API ────────────────────────────────────────────────────────────────
 
def save_user_history(
    user_detail_id: int | str,
    question: str,
    ai_response: str,
    sql: Optional[str] = None,
) -> None:
    """
    Append one chat turn to uploads/user_history/<user_detail_id>/<today>.json.
 
    Args:
        user_detail_id: Pulled from the JWT (e.g. jwt_payload["UserDetailId"]).
        question:       The (spell-corrected) user question.
        ai_response:    The text_summary returned to the frontend.
        sql:            The final SQL that was executed (may be None for chat-only turns).
    """
    try:
        user_dir = USER_HISTORY_DIR / str(user_detail_id)
        user_dir.mkdir(parents=True, exist_ok=True)
 
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        history_file = user_dir / f"{today}.json"
 
        # Load existing entries (or start fresh)
        entries: list = []
        if history_file.exists():
            try:
                entries = json.loads(history_file.read_text(encoding="utf-8"))
                if not isinstance(entries, list):
                    entries = []
            except Exception:
                entries = []
 
        # Append new entry
        entries.append({
            "timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "question": question,
            "ai_response": ai_response,
            "sql": sql or "",
        })
 
        history_file.write_text(
            json.dumps(entries, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.debug(
            f"[user_history] Saved turn for user={user_detail_id} → {history_file}"
        )
 
    except Exception as exc:
        # Never crash the main pipeline because of history logging
        logger.error(f"[user_history] Failed to save history: {exc}", exc_info=True)
 
 
def load_user_history(
    user_detail_id: int | str,
    date: Optional[str] = None,
) -> list:
    """
    Load history entries for a user on a given date (default: today).
 
    Returns:
        List of entry dicts, newest last. Empty list if not found.
    """
    if date is None:
        date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
 
    history_file = USER_HISTORY_DIR / str(user_detail_id) / f"{date}.json"
    if not history_file.exists():
        return []
 
    try:
        data = json.loads(history_file.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.error(f"[user_history] Failed to load history: {exc}")
        return []
