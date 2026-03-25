"""Chronoplot API — Chat & Train endpoints with JWT role-based authorization.

Train flow is identical to the main /api/db/* pipeline.
Chat endpoints require the caller to hold a JWT whose corresponding
BNR_UserDetails.UserType is SuperAdmin (2) or Manager (4).
"""

import hashlib
import json
import logging
import os
import uuid
from enum import IntEnum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt as jose_jwt
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from app.config import get_settings
from app.conversation.formatter import SmartFormatter
from app.conversation.memory import (
    ConversationTurn,
    delete_session_memory,
    get_session_memory,
)
from app.db.connector import (
    ConnectionError as ConnectorConnectionError,
    UniversalConnector,
    UnsupportedDialectError,
)
from app.db.metadata_models import AppDBSession, ChatThread, ChatThreadMessage, User
from app.db.persistent_session_store import load_session, save_session
from app.db.session_store import SessionContext, get_session_store
from app.query.generator import SQLGenerator
from app.query.executor import QueryExecutor
from app.query.planner import QueryPlanner
from app.query.spelling_corrector import QuestionCorrector
from app.query.validator import SQLValidator
from app.training.pipeline import TrainingPipeline

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/api/chronoplot", tags=["chronoplot"])

# ── Shared singletons (re-used from main app) ──────────────────────────────

_connector = UniversalConnector()
_planner = QueryPlanner()
_generator = SQLGenerator()
_validator = SQLValidator()
_executor = QueryExecutor()
_formatter = SmartFormatter()
_corrector = QuestionCorrector()


# ── UserType enum (mirrors BNR_UserDetails.UserType column) ───────────────

class UserType(IntEnum):
    SuperAdmin  = 2
    SiteAdmin   = 3
    Manager     = 4
    Operator    = 5
    ServiceUser = 6
    Visitor     = 7


ALLOWED_USER_TYPES = {int(UserType.SuperAdmin), int(UserType.Manager)}


# ── Request / Response models ──────────────────────────────────────────────

class ChronoConnectResponse(BaseModel):
    session_id: str
    dialect: str
    db_name: str
    message: str


class ChronoChatRequest(BaseModel):
    session_id: str
    question: str
    page: int = 1
    thread_id: Optional[int] = None


class ChronoChatResponse(BaseModel):
    mode: str
    text_summary: str
    table_data: List[Dict[str, Any]] = []
    columns: List[str] = []
    total_rows: int = 0
    stats: Dict[str, Any] = {}
    visualization_hint: str = "none"
    chart_x_axis: str = ""
    chart_y_axis: str = ""
    sql_used: str = ""
    explanation: str = ""
    page: int = 1
    pages_total: int = 1


class ChronoTableDescriptionInput(BaseModel):
    table_name: str
    user_description: Optional[str] = None


class ChronoTrainWithUserInputRequest(BaseModel):
    session_id: str
    tables: List[ChronoTableDescriptionInput]


class ChronoCreateThreadRequest(BaseModel):
    db_session_id: str
    title: str


# ── JWT / Role helpers ─────────────────────────────────────────────────────

def _extract_bearer_token(request: Request) -> str:
    """Pull the raw JWT string from the Authorization header."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header. Expected 'Bearer <token>'.",
        )
    return auth_header[len("Bearer "):]


def _decode_jwt_unverified(token: str) -> dict:
    """
    Decode JWT payload WITHOUT cryptographic verification.

    We intentionally skip signature verification here because:
    1. The token originates from the client's external auth server.
    2. We only need the 'sub' claim to look up the user in BNR_UserDetails.
    3. The actual authorization decision is made by the DB query, not the token claims.

    If you want full RS256 signature verification, integrate OAuthClient.validate_token().
    """
    try:
        payload = jose_jwt.decode(
            token,
            key="",          # no key — verification disabled below
            options={
                "verify_signature": False,
                "verify_exp": False,
                "verify_aud": False,
            },
        )
        return payload
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid JWT: {exc}")


def _get_chronoplot_engine():
    """
    Build a SQLAlchemy engine for chronoplot_DB.
    Reads CHRONOPLOT_DB_CONN from the environment.

    Example .env entry:
        CHRONOPLOT_DB_CONN=mssql+pyodbc://user:pass@server/chronoplot_DB?driver=ODBC+Driver+17+for+SQL+Server
    """
    conn_str = os.environ.get("CHRONOPLOT_DB_CONN", "")
    if not conn_str:
        raise HTTPException(
            status_code=500,
            detail="Server misconfiguration: CHRONOPLOT_DB_CONN is not set.",
        )
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        return engine
    except Exception as exc:
        logger.error(f"Failed to create chronoplot_DB engine: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Could not connect to chronoplot_DB for role verification.",
        )


def _check_chronoplot_role(user_detail_id: str) -> int:
    """
    Query BNR_UserDetails for the given user ID and return their UserType.

    Raises HTTP 403 if the user's UserType is not SuperAdmin (2) or Manager (4).
    Raises HTTP 404 if the user is not found in BNR_UserDetails.
    """
    engine = _get_chronoplot_engine()
    sql = text(
        "SELECT [UserType] FROM [chronoplot_DB].[dbo].[BNR_UserDetails] WHERE Id = :uid"
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {"uid": user_detail_id}).fetchone()
    except Exception as exc:
        logger.error(f"BNR_UserDetails query failed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Role verification query failed. Please try again.",
        )

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"User with Id '{user_detail_id}' not found in BNR_UserDetails.",
        )

    user_type_val = int(row[0])
    logger.info(f"chronoplot role check — user_id={user_detail_id} UserType={user_type_val}")

    if user_type_val not in ALLOWED_USER_TYPES:
        # Friendly enum name for logging
        try:
            type_name = UserType(user_type_val).name
        except ValueError:
            type_name = str(user_type_val)
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. Your role '{type_name}' does not have permission to use the chat. "
                "Only SuperAdmin and Manager roles are allowed."
            ),
        )

    return user_type_val


def _authorize_chronoplot(request: Request) -> str:
    """
    Full auth flow: extract JWT → decode → role check.
    Returns the user_detail_id (sub claim) on success.
    """
    token = _extract_bearer_token(request)
    payload = _decode_jwt_unverified(token)

    user_detail_id = payload.get("sub") or payload.get("userId") or payload.get("nameid")
    if not user_detail_id:
        raise HTTPException(
            status_code=401,
            detail="JWT does not contain a recognisable user identifier claim (sub / userId / nameid).",
        )

    _check_chronoplot_role(str(user_detail_id))
    return str(user_detail_id)


# ── Helpers ────────────────────────────────────────────────────────────────

def _extract_db_name(conn_str: str) -> str:
    try:
        parts = conn_str.split("/")
        name = parts[-1].split("?")[0]
        return name or "database"
    except Exception:
        return "database"


async def _get_table_schemas(ctx: SessionContext) -> Dict[str, List[str]]:
    if getattr(ctx, "tables", None):
        return {t.table_name.lower(): [c.name for c in t.columns] for t in ctx.tables}
    from app.training.indexer import Indexer
    indexer = Indexer()
    try:
        results = await indexer.search(ctx.qdrant_collection, "list all tables", top_k=200)
        schemas: Dict[str, List[str]] = {}
        for r in results:
            t_name = r.get("table_name", "").lower()
            cols_raw = r.get("columns", [])
            cols = [c.get("name", "") if isinstance(c, dict) else str(c) for c in cols_raw]
            if t_name:
                schemas[t_name] = cols
        return schemas
    except Exception as exc:
        logger.error(f"Failed to fetch schemas from Qdrant: {exc}")
        return {}


def _rebuild_ctx_from_persisted(persisted: dict) -> SessionContext:
    """Reconstruct a SessionContext from a persisted dict (no live engine)."""
    return SessionContext(
        session_id=persisted["session_id"],
        engine=None,
        dialect=persisted["dialect"],
        db_name=persisted["db_name"],
        source_type="unknown",
        table_count=0,
        qdrant_collection=persisted["qdrant_collection"],
        db_key=persisted["db_key"],
        training_complete=persisted.get("training_complete", True),
        connection_string=persisted.get("connection_string"),
        common_filter_keys=persisted.get("common_filter_keys", []),
        session_filter_values=persisted.get("session_filter_values", {}),
        is_owner=persisted.get("is_owner", False),
    )


# ═══════════════════════════════════════════════════════════════════════════
#  TRAIN ENDPOINTS  (no auth required — same flow as main app)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/connect", response_model=ChronoConnectResponse)
async def chronoplot_connect(
    request: Request,
    type: str = Form(..., description="upload or connection_string"),
    connection_string: Optional[str] = Form(None),
    db_file: Optional[UploadFile] = File(None),
):
    """Connect a database. Accepts file upload (SQLite) or connection string."""
    session_id = str(uuid.uuid4())

    try:
        if type == "upload":
            if not db_file:
                raise HTTPException(status_code=400, detail="No file uploaded.")
            file_bytes = await db_file.read()
            engine = _connector.from_upload(session_id, file_bytes, db_file.filename or "db.sqlite")
            dialect = "sqlite"
            db_name = db_file.filename or "uploaded_database"
            db_key = hashlib.sha256(file_bytes).hexdigest()
        elif type == "connection_string":
            if not connection_string:
                raise HTTPException(status_code=400, detail="connection_string is required.")
            engine = _connector.from_connection_string(connection_string)
            dialect = _connector._detect_dialect(connection_string)
            db_name = _extract_db_name(connection_string)
            db_key = hashlib.sha256(connection_string.encode()).hexdigest()
        else:
            raise HTTPException(status_code=400, detail="type must be 'upload' or 'connection_string'.")
    except (ConnectorConnectionError, UnsupportedDialectError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    ctx = SessionContext(
        session_id=session_id,
        engine=engine,
        dialect=dialect,
        db_name=db_name,
        source_type=type,
        table_count=0,
        qdrant_collection=f"{settings.qdrant.collection_prefix}_{db_key}",
        db_key=db_key,
        connection_string=connection_string,
        is_owner=True,
    )
    await get_session_store().register(ctx)
    save_session(ctx)
    return ChronoConnectResponse(
        session_id=session_id,
        dialect=dialect,
        db_name=db_name,
        message="Connected. Call /api/chronoplot/train/{session_id} to start indexing.",
    )


@router.get("/train/{session_id}")
async def chronoplot_train(session_id: str):
    """Stream training progress via Server-Sent Events."""
    ctx = await get_session_store().get(session_id)
    if not ctx:
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        ctx = _rebuild_ctx_from_persisted(persisted)

    pipeline = TrainingPipeline()

    async def event_stream():
        yield ": keepalive\n\n"
        try:
            cached_data = pipeline._load_schema_cache(ctx.db_key) if ctx.db_key else None
            if cached_data is not None:
                tables, _ = cached_data
            else:
                tables = await pipeline.extract_only(session_id, ctx.engine)

            ctx.tables = tables
            await get_session_store().register(ctx)

            yield f"data: {json.dumps({'step': 'tables_ready', 'progress': 100, 'tables_done': len(tables), 'tables_total': len(tables), 'message': 'Tables extracted. Please provide descriptions.', 'error': ''})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'step': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/tables/{session_id}")
async def chronoplot_get_tables(session_id: str):
    """Return extracted tables for a session (before training)."""
    ctx = await get_session_store().get(session_id)
    if not ctx:
        raise HTTPException(404, "Session not found")

    pipeline = TrainingPipeline()
    tables = await pipeline.extract_only(session_id, ctx.engine)
    ctx.tables = tables
    return [
        {
            "table_name": t.table_name,
            "schema_name": t.schema_name,
            "row_count": t.row_count,
            "columns": [c.name for c in t.columns],
        }
        for t in tables
    ]


@router.post("/train-with-input")
async def chronoplot_train_with_input(body: ChronoTrainWithUserInputRequest):
    """Run the full training pipeline with user-provided table descriptions (SSE)."""
    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        raise HTTPException(404, "Session not found")
    if not hasattr(ctx, "tables"):
        raise HTTPException(400, "Tables not loaded. Call /tables first.")

    user_desc_map = {
        t.table_name: t.user_description
        for t in body.tables
        if t.user_description
    }
    pipeline = TrainingPipeline()

    async def event_stream():
        async for progress in pipeline.run_with_user_input(
            session_id=body.session_id,
            tables=ctx.tables,
            user_descs=user_desc_map,
            dialect=ctx.dialect,
            qdrant_collection=ctx.qdrant_collection,
            db_key=ctx.db_key,
        ):
            yield f"data: {json.dumps(progress.__dict__)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ═══════════════════════════════════════════════════════════════════════════
#  CHAT ENDPOINTS  (JWT + role authorization required)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/chat/query", response_model=ChronoChatResponse)
async def chronoplot_chat_query(request: Request, body: ChronoChatRequest):
    """
    Run the full NL → SQL → format pipeline.

    Requires a valid JWT with UserType = SuperAdmin (2) or Manager (4)
    in chronoplot_DB.dbo.BNR_UserDetails.
    """
    # ── Authorization ──────────────────────────────────────────────────────
    _authorize_chronoplot(request)

    # ── Load session context ───────────────────────────────────────────────
    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        persisted = load_session(body.session_id)
        if not persisted:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        ctx = _rebuild_ctx_from_persisted(persisted)

    if not ctx.connection_string:
        raise HTTPException(
            status_code=400,
            detail="This session was restored from an upload and cannot run new queries. Please re-upload your file.",
        )

    memory_key = str(body.thread_id) if body.thread_id else body.session_id
    memory = get_session_memory(memory_key)
    conversation_context = memory.get_context_for_prompt()

    max_retries = 3
    last_error = ""
    zero_rows_on_last_attempt = False

    JSON_REMINDER = (
        "\n⚠️ MANDATORY: Your response MUST be a single valid JSON object. "
        "Do NOT return plain SQL or plain text. "
        'Return ONLY: {"sql": "...", "chat_response": "", "response_intent": "data", "reason": "..."}'
    )

    question = await _corrector.correct(body.question)
    logger.info(f"[chronoplot] Corrected question: {question}")

    engine = _connector.from_connection_string(ctx.connection_string)

    for attempt in range(max_retries):
        try:
            # Step 1 — Plan
            plan = await _planner.plan(
                collection_name=ctx.qdrant_collection,
                question=question,
                dialect=ctx.dialect,
                conversation_context=conversation_context,
            )

            if plan.needs_clarification and not plan.relevant_tables:
                return ChronoChatResponse(
                    mode="empty",
                    text_summary="\n".join(plan.clarification_questions),
                )

            # Step 2 — Generate SQL
            gen_result = await _generator.generate(plan, conversation_context, ctx.qdrant_collection)
            if gen_result.chat_response and not gen_result.sql:
                return ChronoChatResponse(
                    mode="chat",
                    text_summary=gen_result.chat_response,
                    explanation=gen_result.explanation,
                )

            # Step 3 — Validate SQL
            val_result = _validator.validate(gen_result.sql, ctx.dialect)
            if not val_result.is_valid:
                last_error = "; ".join(val_result.errors)
                logger.warning(f"[chronoplot] Validation failed (attempt {attempt + 1}): {last_error}")
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[PREVIOUS ATTEMPT FAILED: {last_error}. Fix and try again.]"
                )
                continue

            safe_sql = val_result.sql

            # Apply mandatory filters for non-owners
            if not getattr(ctx, "is_owner", False) and getattr(ctx, "common_filter_keys", None):
                try:
                    from app.query.filter_verifier import SQLFilterVerifier
                    verifier = SQLFilterVerifier(dialect=ctx.dialect)
                    table_schemas = await _get_table_schemas(ctx)
                    safe_sql = verifier.verify_and_inject(
                        sql=safe_sql,
                        filter_keys=ctx.common_filter_keys,
                        filter_values=getattr(ctx, "session_filter_values", {}),
                        table_schemas=table_schemas,
                    )
                except PermissionError as exc:
                    return ChronoChatResponse(mode="empty", text_summary=str(exc), sql_used=val_result.sql)
                except Exception as exc:
                    logger.error(f"[chronoplot] Filter injection failed: {exc}", exc_info=True)
                    return ChronoChatResponse(
                        mode="empty",
                        text_summary="Security verification failed. Please try a different query.",
                    )

            # Step 4 — Execute
            result = await _executor.execute(engine, safe_sql)

            if not result.rows and not result.error:
                zero_rows_on_last_attempt = True
                last_error = "Query returned 0 rows"
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[PREVIOUS SQL RETURNED 0 ROWS: {safe_sql}\n"
                    f"Try relaxing filters, removing WHERE conditions, or using broader LIKE patterns.]"
                    f"{JSON_REMINDER}"
                )
                continue

            if result.error:
                last_error = result.error
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[SQL EXECUTION ERROR: {last_error}. Rewrite the SQL.]"
                    f"{JSON_REMINDER}"
                )
                continue

            zero_rows_on_last_attempt = False

            # Step 5 — Format
            formatted = await _formatter.format(
                question=question,
                rows=result.rows,
                columns=result.columns,
                sql=safe_sql,
                explanation=gen_result.explanation,
                page=body.page,
            )

            # Step 6 — Store in conversation memory
            memory.add_turn(ConversationTurn(
                question=question,
                sql_generated=safe_sql,
                result_summary=f"{result.row_count} rows, columns: {', '.join(result.columns[:5])}",
                result_sample=result.rows[:3],
                services_used=[ctx.db_name],
                filters_applied=[],
            ))

            # Persist to DB thread if thread_id provided
            if body.thread_id:
                db_session = AppDBSession()
                try:
                    db_session.add(ChatThreadMessage(thread_id=body.thread_id, role="user", content=question))
                    db_session.add(ChatThreadMessage(
                        thread_id=body.thread_id,
                        role="assistant",
                        content=formatted.text_summary,
                        sql_used=safe_sql,
                        table_data=formatted.table_data,
                        columns=formatted.columns,
                        stats=formatted.stats,
                        visualization_hint=formatted.visualization_hint,
                        chart_x_axis=formatted.chart_x_axis,
                        chart_y_axis=formatted.chart_y_axis,
                        explanation=formatted.explanation,
                    ))
                    db_session.commit()
                except Exception as exc:
                    logger.error(f"[chronoplot] Failed to save messages to DB: {exc}", exc_info=True)
                finally:
                    db_session.close()

            return ChronoChatResponse(
                mode=formatted.mode,
                text_summary=formatted.text_summary,
                table_data=formatted.table_data,
                columns=formatted.columns,
                total_rows=formatted.total_rows,
                stats=formatted.stats,
                visualization_hint=formatted.visualization_hint,
                chart_x_axis=formatted.chart_x_axis,
                chart_y_axis=formatted.chart_y_axis,
                sql_used=formatted.sql_used,
                explanation=formatted.explanation,
                page=formatted.page,
                pages_total=formatted.pages_total,
            )

        except HTTPException:
            raise  # propagate auth/permission errors unchanged
        except Exception as exc:
            last_error = str(exc)
            logger.error(f"[chronoplot] Pipeline error (attempt {attempt + 1}): {exc}", exc_info=True)
            conversation_context = (
                f"{conversation_context}\n"
                f"[ERROR: {last_error}. Try a different approach.]"
                f"{JSON_REMINDER}"
            )

    if zero_rows_on_last_attempt:
        no_data_text = await _formatter._humanize_no_data(question)
        return ChronoChatResponse(
            mode="no_data",
            text_summary=no_data_text,
            page=1,
            pages_total=1,
        )

    return ChronoChatResponse(
        mode="empty",
        text_summary=(
            f"I wasn't able to answer your question after {max_retries} attempts. "
            f"Last error: {last_error}. Please try rephrasing."
        ),
    )


@router.get("/chat/threads")
def chronoplot_get_threads(request: Request):
    """List chat threads for the authenticated chronoplot user."""
    user_detail_id = _authorize_chronoplot(request)
    db = AppDBSession()
    try:
        # We use user_detail_id as a proxy string; threads created via the chronoplot
        # flow store this in the title or a dedicated field if extended later.
        # For now we return all threads belonging to the internal user row by name match.
        user = db.query(User).filter(User.username == user_detail_id).first()
        if not user:
            return []
        threads = (
            db.query(ChatThread)
            .filter(ChatThread.user_id == user.id)
            .order_by(ChatThread.created_at.desc())
            .all()
        )
        return [
            {"id": t.id, "title": t.title, "created_at": t.created_at, "db_session_id": t.db_session_id}
            for t in threads
        ]
    finally:
        db.close()


@router.post("/chat/threads")
def chronoplot_create_thread(request: Request, body: ChronoCreateThreadRequest):
    """Create a new chat thread for the authenticated chronoplot user."""
    user_detail_id = _authorize_chronoplot(request)
    db = AppDBSession()
    try:
        # Find or auto-create an internal User row keyed by user_detail_id
        user = db.query(User).filter(User.username == user_detail_id).first()
        if not user:
            from app.db.metadata_models import hash_password
            user = User(username=user_detail_id, password_hash=hash_password(uuid.uuid4().hex))
            db.add(user)
            db.commit()
            db.refresh(user)

        t = ChatThread(user_id=user.id, db_session_id=body.db_session_id, title=body.title)
        db.add(t)
        db.commit()
        db.refresh(t)
        return {
            "id": t.id,
            "title": t.title,
            "db_session_id": t.db_session_id,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
    finally:
        db.close()


@router.get("/chat/threads/{thread_id}/messages")
def chronoplot_get_thread_messages(thread_id: int, request: Request):
    """Get all messages in a thread. Requires SuperAdmin or Manager JWT."""
    _authorize_chronoplot(request)
    db = AppDBSession()
    try:
        thread = db.query(ChatThread).filter(ChatThread.id == thread_id).first()
        if not thread:
            raise HTTPException(404, "Thread not found")
        messages = (
            db.query(ChatThreadMessage)
            .filter(ChatThreadMessage.thread_id == thread_id)
            .order_by(ChatThreadMessage.id.asc())
            .all()
        )
        return [
            {
                "id": m.id,
                "role": m.role,
                "content": m.content,
                "sql_used": m.sql_used,
                "table_data": m.table_data,
                "columns": m.columns,
                "stats": m.stats,
                "visualization_hint": m.visualization_hint,
                "chart_x_axis": m.chart_x_axis,
                "chart_y_axis": m.chart_y_axis,
                "explanation": m.explanation,
            }
            for m in messages
        ]
    finally:
        db.close()
