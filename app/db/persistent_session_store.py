import json
import pathlib
from typing import Optional
from app.db.session_store import SessionContext

SESSIONS_DIR = pathlib.Path("uploads/session_store")
SESSIONS_DIR.mkdir(exist_ok=True, parents=True)

def save_session(ctx: SessionContext):
    path = SESSIONS_DIR / f"{ctx.session_id}.json"
    data = {
        "session_id": ctx.session_id,
        "dialect": ctx.dialect,
        "db_name": ctx.db_name,
        "source_type": ctx.source_type,
        "db_key": ctx.db_key,
        "qdrant_collection": ctx.qdrant_collection,
        "training_complete": ctx.training_complete,
        "connection_string": ctx.connection_string,
        "user_descriptions": getattr(ctx, 'user_descriptions', {})
    }
    path.write_text(json.dumps(data), encoding="utf-8")


def load_session(session_id: str) -> Optional[dict]:
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data


def save_user_descriptions(session_id: str, descriptions: dict[str, str]):
    """Save user descriptions to session store."""
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Session {session_id} not found")
    
    data = json.loads(path.read_text(encoding="utf-8"))
    data["user_descriptions"] = descriptions
    path.write_text(json.dumps(data), encoding="utf-8")


def load_user_descriptions(session_id: str) -> Optional[dict[str, str]]:
    """Load user descriptions from session store."""
    path = SESSIONS_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("user_descriptions", {})
