"""FastAPI application — Universal Database Chatbot API."""

import asyncio
import hashlib
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from enum import IntEnum
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm, HTTPBearer
import secrets
from app.db.metadata_models import AppDBSession, User, UserSession, SavedModel, ChatThread, ChatThreadMessage, hash_password, verify_password
from app.exceptions import AppError, DBError, QueryError, SessionError, ValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request

from app.config import get_settings
from app.db.connector import UniversalConnector, ConnectionError, UnsupportedDialectError
from app.db.session_store import SessionContext, get_session_store
from app.training.pipeline import TrainingPipeline
from app.query.planner import QueryPlanner
from app.query.generator import SQLGenerator
from app.query.validator import SQLValidator
from app.query.executor import QueryExecutor
from app.conversation.formatter import SmartFormatter
from app.conversation.memory import get_session_memory, delete_session_memory, ConversationTurn
from app.db.persistent_session_store import load_session, save_session
from app.query.spelling_corrector import QuestionCorrector
from jose import JWTError, jwt as jose_jwt
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()
limiter = Limiter(key_func=get_remote_address)


# ── Lifespan ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background session cleanup task."""
    async def cleanup_loop():
        store = get_session_store()
        interval = settings.session.cleanup_interval_minutes * 60
        while True:
            await asyncio.sleep(interval)
            evicted = await store.cleanup_expired()
            if evicted:
                logger.info(f"Session cleanup: removed {evicted} expired sessions")

    task = asyncio.create_task(cleanup_loop())
    yield
    task.cancel()


app = FastAPI(
    title="Universal Database Chatbot API",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    status_code = 422 if any(word in exc.user_message.lower() for word in ["invalid", "required", "expired"]) else 500
    raise HTTPException(status_code=status_code, detail=exc.user_message)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

app.mount(
    "/assets",
    StaticFiles(directory="frontend/dist/assets"),
    name="static-assets",
)

# ── Singletons ─────────────────────────────────────────────────────────────

connector = UniversalConnector()
planner = QueryPlanner()
from app.query.generator import SQLGenerator
generator = SQLGenerator()
validator = SQLValidator()
executor = QueryExecutor()
formatter = SmartFormatter()
corrector = QuestionCorrector()


# ── Chronoplot Security Scheme ──────────────────────────────────────────────
cp_bearer = HTTPBearer(auto_error=False)

# ── Request/Response Models ────────────────────────────────────────────────

class ConnectResponse(BaseModel):
    session_id: str
    dialect: str
    db_name: str
    message: str


class TrainStatusResponse(BaseModel):
    step: str
    progress: int
    tables_done: int
    tables_total: int
    message: str
    error: str = ""


class ChatRequest(BaseModel):
    session_id: str
    question: str
    page: int = 1
    thread_id: Optional[int] = None


class TableDescriptionInput(BaseModel):
    table_name: str
    user_description: Optional[str] = None


class TrainWithUserInputRequest(BaseModel):
    session_id: str
    tables: List[TableDescriptionInput]


class ChatResponse(BaseModel):
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


class SchemaTable(BaseModel):
    table_name: str
    schema_name: Optional[str]
    description: str
    row_count: int
    column_count: int


class FilterKeysRequest(BaseModel):
    session_id: str
    keys: List[str]


class FilterValuesRequest(BaseModel):
    session_id: str
    values: Dict[str, Any]




# ── Auth & User Models ─────────────────────────────────────────────────────

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

def get_current_user(token: str = Depends(oauth2_scheme)):
    db = AppDBSession()
    try:
        session = db.query(UserSession).filter(UserSession.token == token).first()
        if not session:
            raise HTTPException(status_code=401, detail="Invalid token")
        return session.user
    except Exception as e:
        logger.info(f"Error : {e}")
    finally:
        db.close()

class RegisterRequest(BaseModel):
    username: str
    password: str

@app.get("/")
async def root():
    """Serve React frontend."""
    return FileResponse("frontend/dist/index.html")


@app.post("/api/auth/register")
def register(body: RegisterRequest):
    db = AppDBSession()
    try:
        if db.query(User).filter(User.username == body.username).first():
            raise HTTPException(status_code=400, detail="Username already registered")
        user = User(username=body.username, password_hash=hash_password(body.password))
        db.add(user)
        db.commit()
        return {"message": "User registered successfully"}
    finally:
        db.close()

@app.post("/api/auth/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    db = AppDBSession()
    try:
        user = db.query(User).filter(User.username == form_data.username).first()
        if not user or not verify_password(form_data.password, user.password_hash):
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        logger.info(f'user : {user}')
        token = secrets.token_urlsafe(32)
        session = UserSession(token=token, user_id=user.id)
        db.add(session)
        db.commit()
        return {"access_token": token, "token_type": "bearer"}
    finally:
        db.close()

@app.post("/api/auth/logout")
def logout(token: str = Depends(oauth2_scheme)):
    db = AppDBSession()
    try:
        db.query(UserSession).filter(UserSession.token == token).delete()
        db.commit()
        return {"message": "Logged out successfully"}
    finally:
        db.close()


class SaveModelRequest(BaseModel):
    session_id: str
    name: str

class UpdateModelMetadataRequest(BaseModel):
    common_filter_keys: List[str]

@app.post("/api/models/save")
async def save_model(body: SaveModelRequest, user: User = Depends(get_current_user)):
    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        ctx_data = load_session(body.session_id)
        if not ctx_data:
            raise HTTPException(404, "Session not found")
        ctx = SessionContext(**ctx_data)
        
    db = AppDBSession()
    try:
        model = SavedModel(
            user_id=user.id,
            name=body.name,
            dialect=ctx.dialect,
            db_name=ctx.db_name,
            connection_string=ctx.connection_string or "",
            db_key=ctx.db_key,
            qdrant_collection=ctx.qdrant_collection,
            common_filter_keys=getattr(ctx, 'common_filter_keys', [])
        )
        db.add(model)
        db.commit()
        return {"message": "Model saved successfully", "id": model.id}
    finally:
        db.close()

@app.get("/api/models")
def list_models(user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        # Get all models, joined with User to get the owner's name
        models = db.query(SavedModel, User.username).join(User, SavedModel.user_id == User.id).all()
        return [
            {
                "id": m.SavedModel.id,
                "name": m.SavedModel.name,
                "dialect": m.SavedModel.dialect,
                "db_name": m.SavedModel.db_name,
                "common_filter_keys": m.SavedModel.common_filter_keys or [],
                "created_at": m.SavedModel.created_at,
                "is_owner": m.SavedModel.user_id == user.id,
                "owner_name": m.username
            }
            for m in models
        ]
    finally:
        db.close()

@app.post("/api/models/{model_id}/load")
async def load_model(model_id: int, user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        # Allow any user to find any model
        model = db.query(SavedModel).filter(SavedModel.id == model_id).first()
        if not model:
            raise HTTPException(404, "Model not found")
        
        session_id = str(uuid.uuid4())
        try:
            engine = connector.from_connection_string(model.connection_string) if model.connection_string else None
        except Exception:
            engine = None
 
        ctx = SessionContext(
            session_id=session_id,
            engine=engine,
            dialect=model.dialect,
            db_name=model.db_name,
            source_type="connection_string" if model.connection_string else "upload",
            table_count=0,
            qdrant_collection=model.qdrant_collection,
            db_key=model.db_key,
            connection_string=model.connection_string,
            common_filter_keys=model.common_filter_keys or [],
            is_owner=(model.user_id == user.id)
        )
        await get_session_store().register(ctx)
        save_session(ctx)
        return {
            "session_id": session_id, 
            "is_owner": ctx.is_owner,
            "message": "Model loaded successfully"
        }
    finally:
        db.close()

@app.delete("/api/models/{model_id}")
async def delete_model(model_id: int, user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        model = db.query(SavedModel).filter(SavedModel.id == model_id, SavedModel.user_id == user.id).first()
        if not model:
            raise HTTPException(404, "Model not found")
        
        # Keep Qdrant clean by deleting the corresponding vector collection
        from app.training.indexer import Indexer
        indexer = Indexer()
        await indexer.delete_collection(model.qdrant_collection)
        
        db.delete(model)
        db.commit()
        return {"message": "Model deleted successfully"}
    finally:
        db.close()

@app.patch("/api/models/{model_id}/metadata")
async def update_model_metadata(model_id: int, body: UpdateModelMetadataRequest, user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        model = db.query(SavedModel).filter(SavedModel.id == model_id, SavedModel.user_id == user.id).first()
        if not model:
            raise HTTPException(404, "Model not found or access denied")
        
        model.common_filter_keys = body.common_filter_keys
        db.commit()
        return {"message": "Model metadata updated successfully", "common_filter_keys": model.common_filter_keys}
    finally:
        db.close()

# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "sessions": get_session_store().count()}


@app.post("/api/db/connect", response_model=ConnectResponse)
async def connect_db(
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
            engine = connector.from_upload(session_id, file_bytes, db_file.filename or "db.sqlite")
            dialect = "sqlite"
            db_name = db_file.filename or "uploaded_database"
            db_key = hashlib.sha256(file_bytes).hexdigest()
        elif type == "connection_string":
            if not connection_string:
                raise HTTPException(status_code=400, detail="connection_string is required.")
            engine = connector.from_connection_string(connection_string)
            dialect = connector._detect_dialect(connection_string)
            db_name = _extract_db_name(connection_string)
            db_key = hashlib.sha256(connection_string.encode()).hexdigest()
        else:
            raise HTTPException(status_code=400, detail="type must be 'upload' or 'connection_string'.")

    except (ConnectionError, UnsupportedDialectError) as e:
        raise HTTPException(status_code=422, detail=str(e))

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
        is_owner=True
    )
    await get_session_store().register(ctx)
    save_session(ctx)
    return ConnectResponse(
        session_id=session_id,
        dialect=dialect,
        db_name=db_name,
        message="Connected. Call /api/db/train/{session_id} to start indexing.",
    )


@app.get("/api/db/train/{session_id}")
async def train_db(session_id: str):
    """Stream training progress via Server-Sent Events."""
    ctx = await get_session_store().get(session_id)
    if not ctx:
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        
        # Rebuild a minimal SessionContext (engine cannot be recovered for upload)
        ctx = SessionContext(

            session_id=persisted["session_id"],
            engine=None,  # Engine is gone, cannot run queries, but schema/Qdrant works
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
            is_owner=persisted.get("is_owner", False)

        )

    pipeline = TrainingPipeline()

    logger.info("Out from Training Pipeline")
    async def event_stream():
        # Send immediate keepalive so client knows connection is open
        # (schema extraction on large DBs can take > 2 min before first yield)
        yield ": keepalive\n\n"
        try:
            cached_data = pipeline._load_schema_cache(ctx.db_key) if ctx.db_key else None
            if cached_data is not None:
                logger.info("If call")
                tables, descriptions = cached_data
                logger.info(tables)

            else:
                tables = await pipeline.extract_only(session_id, ctx.engine)

            ctx.tables = tables
            await get_session_store().register(ctx)

            data = {
                "step": "tables_ready",
                "progress": 100,
                "tables_done": len(tables),
                "tables_total": len(tables),
                "message": "Tables extracted. Please provide descriptions.",
                "error": ""
            }

            yield f"data: {json.dumps(data)}\n\n"

        except Exception as e:
            error_data = {
                "step": "error",
                "message": str(e)
            }
            yield f"data: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/session/{session_id}/schema", response_model=List[SchemaTable])
async def get_schema(session_id: str):
    """Return list of indexed tables for the session sidebar."""
    from app.training.indexer import Indexer
    ctx = await get_session_store().get(session_id)
    if not ctx:
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        
        # Rebuild a minimal SessionContext (engine cannot be recovered for upload)
        ctx = SessionContext(
            session_id=persisted["session_id"],
            engine=None,  # Engine is gone, cannot run queries, but schema/Qdrant works
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
            is_owner=persisted.get("is_owner", False)

        )
    logger.info(f"CTX : {ctx}")
    indexer = Indexer()
    try:
        # Search with a broad query to get all table payloads
        results = await indexer.search(ctx.qdrant_collection, "list all tables", top_k=200)
        return [
            SchemaTable(
                table_name=r.get("table_name", ""),
                schema_name=r.get("schema_name"),
                description=r.get("description", ""),
                row_count=r.get("row_count", 0),
                column_count=len(r.get("columns", [])),
            )
            for r in results
        ]
    except Exception as e:
        logger.error(f"Schema fetch error: {e}", exc_info=True)
        raise DBError("Unable to retrieve database schema. Please check your connection and try again.")


@app.get("/api/db/tables/{session_id}")
async def get_tables(session_id: str):
    ctx = await get_session_store().get(session_id)

    if not ctx:
        raise HTTPException(404, "Session not found")

    pipeline = TrainingPipeline()

    tables = await pipeline.extract_only(session_id, ctx.engine)

    # Save tables temporarily (important)
    ctx.tables = tables  

    return [
        {
            "table_name": t.table_name,
            "schema_name": t.schema_name,
            "row_count": t.row_count,
            "columns": [c.name for c in t.columns]
        }
        for t in tables
    ]


@app.post("/api/db/train-with-input")
async def train_with_user_input(body: TrainWithUserInputRequest):
    ctx = await get_session_store().get(body.session_id)
    logger.info(ctx)
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
            db_key=ctx.db_key
        ):
            yield f"data: {json.dumps(progress.__dict__)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/api/session/{session_id}/filter-keys")
async def save_filter_keys(session_id: str, body: FilterKeysRequest):
    ctx = await get_session_store().get(session_id)
    if not ctx:
        raise HTTPException(404, "Session not found")
    ctx.common_filter_keys = body.keys
    save_session(ctx)
    return {"message": "Filter keys saved successfully"}


@app.post("/api/session/{session_id}/filter-values")
async def save_filter_values(session_id: str, body: FilterValuesRequest):
    logger.info(f"POST /api/session/{session_id}/filter-values: {body.values}")
    ctx = await get_session_store().get(session_id)
    if not ctx:
        logger.warning(f"Session {session_id} not in memory, attempting to load from disk")
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(404, "Session not found")
        ctx = SessionContext(**persisted)
        ctx.session_filter_values = body.values
        await get_session_store().register(ctx)
        save_session(ctx)
    else:
        ctx.session_filter_values = body.values
        save_session(ctx)
    logger.info(f"Saved session_filter_values for {session_id}: {ctx.session_filter_values}")
    return {"message": "Filter values saved for this session"}


@app.get("/api/session/{session_id}/schema-report")
async def get_schema_report(session_id: str):
    """Return the full schema + AI description report as JSON.

    Written to uploads/{session_id}/schema_report.json after training,
    but falls back to Qdrant for loaded sessions.
    """
    import pathlib, json as _json
    report_path = pathlib.Path(settings.uploads.dir) / session_id / "schema_report.json"
    if report_path.exists():
        try:
            data = _json.loads(report_path.read_text(encoding="utf-8"))
            return data
        except Exception as e:
            logger.warning(f"Error reading schema_report.json: {e}")
            pass
            
    # Fallback to Qdrant if file doesn't exist (e.g., loaded model)
    ctx = await get_session_store().get(session_id)
    if not ctx:
        ctx_data = load_session(session_id)
        if ctx_data:
            ctx = SessionContext(**ctx_data)
            
    if ctx and ctx.qdrant_collection:
        from app.training.indexer import Indexer
        indexer = Indexer()
        try:
            scroll_results = indexer.client.scroll(
                collection_name=ctx.qdrant_collection,
                limit=500,
                with_payload=True
            )
            return [r.payload for r in scroll_results[0]]
        except Exception as e:
            logger.error(f"Failed to fetch schema report from Qdrant: {e}")
            
    raise HTTPException(
        status_code=404,
        detail="Schema report not found. Training may not have completed yet.",
    )


# @app.post("/api/chat/query", response_model=ChatResponse)
# @limiter.limit("20/minute")
# async def chat_query(request: Request, body: ChatRequest):
#     """Run the full NL → SQL → format pipeline."""
#     ctx = await get_session_store().get(body.session_id)
#     last_error = ""
#     zero_rows_on_last_attempt = False
#     if not ctx:
#         persisted = load_session(body.session_id)
#         if not persisted:
#             raise HTTPException(status_code=404, detail="Session not found or expired.")
        
#         # Rebuild a minimal SessionContext (engine cannot be recovered for upload)
#         ctx = SessionContext(
#             session_id=persisted["session_id"],
#             engine=None,  # Engine is gone, cannot run queries, but schema/Qdrant works
#             dialect=persisted["dialect"],
#             db_name=persisted["db_name"],
#             source_type="unknown",
#             table_count=0,
#             qdrant_collection=persisted["qdrant_collection"],
#             db_key=persisted["db_key"],
#             training_complete=persisted.get("training_complete", True),
#             connection_string=persisted.get("connection_string"),
#             common_filter_keys=persisted.get("common_filter_keys", []),
#             session_filter_values=persisted.get("session_filter_values", {})

#         )
#     memory_key = str(body.thread_id) if body.thread_id else body.session_id
#     memory = get_session_memory(memory_key)
#     conversation_context = memory.get_context_for_prompt()
#     max_retries = 3
#     last_error = ""
#     final_existence_reason = None

#     question = await corrector.correct(body.question)
#     logger.info(f"Corrected Text : {question}")
#     JSON_REMINDER = (
#         "\n⚠️ MANDATORY: Your response MUST be a single valid JSON object. "
#         "Do NOT return plain SQL or plain text. "
#         'Return ONLY: {"sql": "...", "chat_response": "", "response_intent": "data", "reason": "..."}'
#     )
#     for attempt in range(max_retries):
#         try:
#             # Step 1: Plan
#             plan = await planner.plan(
#                 collection_name=ctx.qdrant_collection,
#                 question=question,
#                 dialect=ctx.dialect,
#                 conversation_context=conversation_context,
#             )

#             if plan.needs_clarification and not plan.relevant_tables:
#                 return ChatResponse(
#                     mode="empty",
#                     text_summary="\n".join(plan.clarification_questions),
#                 )

#             # Step 2: Generate SQL
#             gen_result = await generator.generate(plan, conversation_context, ctx.qdrant_collection)
#             logger.debug(f"Generated SQL for question: {gen_result}...")
#             if gen_result.chat_response and not gen_result.sql:
#                 logger.info(f"Chat intent detected, skipping SQL pipeline: {gen_result.explanation}")
#                 return ChatResponse(
#                     mode="chat",
#                     text_summary=gen_result.chat_response,
#                     explanation=gen_result.explanation,
#                     sql_used="",
#                     page=1,
#                     pages_total=1,
#                 )

#             # Step 3: Validate SQL
#             val_result = validator.validate(gen_result.sql, ctx.dialect)
#             if not val_result.is_valid:
#                 last_error = "; ".join(val_result.errors)
#                 logger.warning(f"Validation failed (attempt {attempt+1}): {last_error}")
#                 # Feed error back as context for retry
#                 conversation_context = (
#                     f"{conversation_context}\n"
#                     f"[PREVIOUS ATTEMPT FAILED: {last_error}. Fix and try again.]"
#                     f"{JSON_REMINDER}"
#                 )
#                 continue

#             safe_sql = val_result.sql
#             logger.debug(f"Executing validated SQL (length: {len(safe_sql)} chars)")
#             engine = connector.from_connection_string(ctx.connection_string)
#             # Step 4: Execute
            
#             result = await executor.execute(engine, safe_sql)
#             is_zero_result = False
#             if not result.error:
#                 if not result.rows:
#                     is_zero_result = True
#                 elif len(result.rows) == 1:
#                     vals = list(result.rows[0].values())
#                     if vals and all(v in {0, 0.0, '0', None} for v in vals):
#                         is_zero_result = True

#             if is_zero_result:
#                 zero_rows_on_last_attempt = True
#                 entity_exists = None
#                 try:
#                     from app.query.existence_checker import ExistenceChecker
#                     checker = ExistenceChecker()
#                     existence_info = await checker.check_existence_sql(safe_sql, question, ctx.dialect)
                    
#                     if existence_info.existence_sql:
#                         engine = connector.from_connection_string(ctx.connection_string)
#                         ex_result = await executor.execute(engine, existence_info.existence_sql)
#                         if not ex_result.rows and not ex_result.error:
#                             entity_exists = False
#                             final_existence_reason = f"The core entity ({existence_info.entity_name}) does not exist in the database at all."
#                         elif not ex_result.error:
#                             entity_exists = True
#                             final_existence_reason = None
#                 except Exception as e:
#                     logger.warning(f"Existence check flow failed silently: {e}")
                
#                 if attempt < max_retries - 1:
#                     logger.warning(f"Zero rows returned (attempt {attempt+1}), retrying with smart existence feedback...")
#                     if entity_exists is False:
#                         smart_feedback = (
#                             f"Furthermore, the entity '{existence_info.entity_name}' DOES NOT EXIST in the filtered table/column! "
#                             f"You must check another table, or check different column names."
#                         )
#                     elif entity_exists is True:
#                         smart_feedback = (
#                             f"The entity '{existence_info.entity_name}' DOES EXIST, but the entire query returned 0 rows. "
#                             f"Check your JOIN conditions or other filters to see why records were excluded."
#                         )
#                     else:
#                         smart_feedback = (
#                             f"Check another table, check column names, or use broader LIKE patterns."
#                         )

#                     conversation_context = (
#                         f"{conversation_context}\n"
#                         f"[PREVIOUS SQL RETURNED 0 ROWS: {safe_sql}\n"
#                         f"The query executed successfully but found no data. "
#                         f"{smart_feedback}]"
#                         f"{JSON_REMINDER}"
#                     )
#                     continue
#                 else:
#                     logger.info(f"Query returned 0 rows on final attempt. Breaking loop.")
#                     break
            
#             if result.error:
#                 last_error = result.error

#                 logger.warning(f"Execution failed (attempt {attempt+1}): {last_error}")
#                 conversation_context = (
#                     f"{conversation_context}\n"
#                     f"[SQL EXECUTION ERROR: {last_error}. Rewrite the SQL.]"
#                     f"Check if the given column exists in the table."
#                     f"{JSON_REMINDER}"
#                 )
#                 continue
#             zero_rows_on_last_attempt = False
#             # Step 5: Format
#             formatted = await formatter.format(
#                 question=question,
#                 rows=result.rows,
#                 columns=result.columns,
#                 sql=safe_sql,
#                 explanation=gen_result.explanation,
#                 page=body.page,
#                 response_intent=gen_result.response_intent
#             )

#             # Step 6: Store in conversation memory
#             memory.add_turn(ConversationTurn(
#                 question=question,
#                 sql_generated=safe_sql,
#                 result_summary=(
#                     f"{result.row_count} rows, columns: {', '.join(result.columns[:5])}"
#                 ),
#                 # ✅ Add this — store up to 3 rows so follow-ups can reference actual data
#                 result_sample=result.rows[:3],
#                 services_used=[ctx.db_name],
#                 filters_applied=[],
#             ))

#             # Save to Db if thread_id is present
#             if body.thread_id:
#                 db_session = AppDBSession()
#                 try:
#                     u_msg = ChatThreadMessage(
#                         thread_id=body.thread_id,
#                         role="user",
#                         content=question
#                     )
#                     db_session.add(u_msg)
#                     a_msg = ChatThreadMessage(
#                         thread_id=body.thread_id,
#                         role="assistant",
#                         content=formatted.text_summary,
#                         sql_used=safe_sql,
#                         table_data=formatted.table_data,
#                         columns=formatted.columns,
#                         stats=formatted.stats,
#                         visualization_hint=formatted.visualization_hint,
#                         chart_x_axis=formatted.chart_x_axis,
#                         chart_y_axis=formatted.chart_y_axis,
#                         explanation=formatted.explanation
#                     )
#                     db_session.add(a_msg)
#                     db_session.commit()
#                 except Exception as e:
#                     logger.error(f"Failed to save messages to DB: {e}", exc_info=True)
#                 finally:
#                     db_session.close()

#             return ChatResponse(
#                 mode=formatted.mode,
#                 text_summary=formatted.text_summary,
#                 table_data=formatted.table_data,
#                 columns=formatted.columns,
#                 total_rows=formatted.total_rows,
#                 stats=formatted.stats,
#                 visualization_hint=formatted.visualization_hint,
#                 chart_x_axis=formatted.chart_x_axis,
#                 chart_y_axis=formatted.chart_y_axis,
#                 sql_used=formatted.sql_used,
#                 explanation=formatted.explanation,
#                 page=formatted.page,
#                 pages_total=formatted.pages_total,
#             )

#         except Exception as e:
#             last_error = str(e)
#             logger.error(f"Query pipeline error (attempt {attempt+1}): {e}", exc_info=True)
#             conversation_context = (
#                 f"{conversation_context}\n"
#                 f"[ERROR: {last_error}. Try a different approach.]"
#                 f"{JSON_REMINDER}"
#             )

#     if zero_rows_on_last_attempt:
#         no_data_text = await formatter._humanize_no_data(question, final_existence_reason)
#         return ChatResponse(
#             mode="empty",
#             text_summary=no_data_text,
#             sql_used=safe_sql,
#             page=1,
#             pages_total=1,
#         )
    
#     error_text = await formatter._humanize_error(question)
#     return ChatResponse(
#         mode="empty",
#         text_summary=error_text,
#     )

@app.post("/api/chat/query", response_model=ChatResponse)
@limiter.limit("20/minute")
async def chat_query(request: Request, body: ChatRequest):
    """Run the full NL → SQL → format pipeline."""
    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        persisted = load_session(body.session_id)
        if not persisted:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        
        # Rebuild a minimal SessionContext (engine cannot be recovered for upload)
        ctx = SessionContext(
            session_id=persisted["session_id"],
            engine=None,  # Engine is gone, cannot run queries, but schema/Qdrant works
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
            is_owner=persisted.get("is_owner", False)

        )
    # if not ctx.training_complete:
    #     raise HTTPException(status_code=409, detail="Database training not complete. Please wait.")

    memory = get_session_memory(body.session_id)
    conversation_context = memory.get_context_for_prompt()
    last_error = ""
    zero_rows_on_last_attempt = False
    max_retries = 3
    last_error = ""
    JSON_REMINDER = (
        "\n⚠️ MANDATORY: Your response MUST be a single valid JSON object. "
        "Do NOT return plain SQL or plain text. "
        'Return ONLY: {"sql": "...", "chat_response": "", "response_intent": "data", "reason": "..."}'
    )
    question = await corrector.correct(body.question)
    logger.info(f"Corrected Text : {question}")
    if not ctx.connection_string:
        raise HTTPException(
            status_code=400,
            detail="This session was restored from an upload and cannot run new queries. Please re-upload your file."
        )
    engine = connector.from_connection_string(ctx.connection_string)
    for attempt in range(max_retries):
        try:
            # Step 1: Plan
            plan = await planner.plan(
                collection_name=ctx.qdrant_collection,
                question=question,
                dialect=ctx.dialect,
                conversation_context=conversation_context,
            )

            if plan.needs_clarification and not plan.relevant_tables:
                return ChatResponse(
                    mode="empty",
                    text_summary="\n".join(plan.clarification_questions),
                )

            # Step 2: Generate SQL
            gen_result = await generator.generate(plan, conversation_context, ctx.qdrant_collection)
            logger.debug(f"Generated SQL for question: {gen_result}...")
            if gen_result.chat_response and not gen_result.sql:
                logger.info(f"Chat intent detected, skipping SQL pipeline: {gen_result.explanation}")
                return ChatResponse(
                    mode="chat",
                    text_summary=gen_result.chat_response,
                    explanation=gen_result.explanation,
                    sql_used="",
                    page=1,
                    pages_total=1,
                )

            # Step 3: Validate SQL
            val_result = validator.validate(gen_result.sql, ctx.dialect)
            if not val_result.is_valid:
                last_error = "; ".join(val_result.errors)
                logger.warning(f"Validation failed (attempt {attempt+1}): {last_error}")
                # Feed error back as context for retry
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[PREVIOUS ATTEMPT FAILED: {last_error}. Fix and try again.]"
                )
                continue

            safe_sql = val_result.sql
            
            # Apply mandatory filters if present (skipped for owners)
            logger.info(f"Chat session: {body.session_id} (is_owner: {getattr(ctx, 'is_owner', False)})")
            if not getattr(ctx, 'is_owner', False) and getattr(ctx, 'common_filter_keys', None):
                logger.info(f"Filter Values (Keys): {ctx.common_filter_keys}")
                logger.info(f"Filter Values (Values): {getattr(ctx, 'session_filter_values', {})}")
                try:
                    from app.query.filter_verifier import SQLFilterVerifier
                    verifier = SQLFilterVerifier(dialect=ctx.dialect)
                    table_schemas = await _get_table_schemas(ctx)
                    logger.info(f"Filter Values {ctx.common_filter_keys}")
                    safe_sql = verifier.verify_and_inject(
                        sql=safe_sql,
                        filter_keys=ctx.common_filter_keys,
                        filter_values=getattr(ctx, 'session_filter_values', {}),
                        table_schemas=table_schemas
                    )
                    logger.info(f"Filtered SQL applied: {safe_sql}")
                except PermissionError as e:
                    return ChatResponse(
                        mode="empty",
                        text_summary=str(e),
                        sql_used=val_result.sql,
                    )
                except Exception as e:
                    logger.error(f"Filter injection failed: {e}", exc_info=True)
                    return ChatResponse(
                        mode="empty",
                        text_summary="Security verification failed. Please try a different query.",
                    )

            logger.debug(f"Executing validated SQL (length: {len(safe_sql)} chars)")
            
            # Step 4: Execute
            
            result = await executor.execute(engine, safe_sql)
            if not result.rows and not result.error:
                zero_rows_on_last_attempt = True
                last_error = "Query returned 0 rows"
                logger.warning(f"Zero rows returned (attempt {attempt+1}), retrying with feedback...")
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[PREVIOUS SQL RETURNED 0 ROWS: {safe_sql}\n"
                    f"The query executed successfully but found no data. "
                    f"Try relaxing filters, removing WHERE conditions, "
                    f"checking column names, or using broader LIKE patterns.]"
                    f"{JSON_REMINDER}"
                )
                continue
            
            if result.error:
                last_error = result.error

                logger.warning(f"Execution failed (attempt {attempt+1}): {last_error}")
                conversation_context = (
                    f"{conversation_context}\n"
                    f"[SQL EXECUTION ERROR: {last_error}. Rewrite the SQL.]"
                    f"Check if the given column exists in the table."
                    f"{JSON_REMINDER}"
                )
                continue
            zero_rows_on_last_attempt = False
            # Step 5: Format
            formatted = await formatter.format(
                question=question,
                rows=result.rows,
                columns=result.columns,
                sql=safe_sql,
                explanation=gen_result.explanation,
                page=body.page,
            )

            # Step 6: Store in conversation memory
            memory.add_turn(ConversationTurn(
                question=question,
                sql_generated=safe_sql,
                result_summary=(
                    f"{result.row_count} rows, columns: {', '.join(result.columns[:5])}"
                ),
                # ✅ Add this — store up to 3 rows so follow-ups can reference actual data
                result_sample=result.rows[:3],
                services_used=[ctx.db_name],
                filters_applied=[],
            ))


            return ChatResponse(
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

        except Exception as e:
            last_error = str(e)
            logger.error(f"Query pipeline error (attempt {attempt+1}): {e}", exc_info=True)
            conversation_context = (
                f"{conversation_context}\n"
                f"[ERROR: {last_error}. Try a different approach.]"
                f"{JSON_REMINDER}"
            )
    if zero_rows_on_last_attempt:
        no_data_text = await formatter._humanize_no_data(question)
        return ChatResponse(
            mode="no_data",
            text_summary=no_data_text,
            sql_used="",
            page=1,
            pages_total=1,
        )

    return ChatResponse(
        mode="empty",
        text_summary=(
            f"I wasn't able to answer your question after {max_retries} attempts. "
            f"Last error: {last_error}. "
            f"Please try rephrasing your question."
        ),
    )


@app.delete("/api/session/{session_id}")
async def disconnect(session_id: str):
    """Disconnect a session — cleans up Qdrant collection and uploaded files."""
    ctx = await get_session_store().evict(session_id)
    if ctx:
        from app.training.indexer import Indexer
        indexer = Indexer()
        await indexer.delete_collection(session_id)
        if ctx.source_type == "upload":
            connector.cleanup_session(session_id)
        delete_session_memory(session_id)
        return {"message": f"Session {session_id} disconnected and cleaned up."}
    return {"message": "Session not found (may have already expired)."}

class CreateThreadRequest(BaseModel):
    db_session_id: str
    title: str

@app.get("/api/chat/threads")
def get_chat_threads(user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        threads = db.query(ChatThread).filter(ChatThread.user_id == user.id).order_by(ChatThread.created_at.desc()).all()
        return [{"id": t.id, "title": t.title, "created_at": t.created_at, "db_session_id": t.db_session_id} for t in threads]
    finally:
        db.close()

@app.post("/api/chat/threads")
def create_chat_thread(req: CreateThreadRequest, user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        t = ChatThread(user_id=user.id, db_session_id=req.db_session_id, title=req.title)
        db.add(t)
        db.commit()
        db.refresh(t)
        return {"id": t.id, "title": t.title, "db_session_id": t.db_session_id, "created_at": t.created_at.isoformat() if t.created_at else None}
    finally:
        db.close()

@app.get("/api/chat/threads/{thread_id}/messages")
def get_thread_messages(thread_id: int, user: User = Depends(get_current_user)):
    db = AppDBSession()
    try:
        t = db.query(ChatThread).filter(ChatThread.id == thread_id, ChatThread.user_id == user.id).first()
        if not t:
            raise HTTPException(404, "Thread not found")
        messages = db.query(ChatThreadMessage).filter(ChatThreadMessage.thread_id == thread_id).order_by(ChatThreadMessage.id.asc()).all()
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
                "explanation": m.explanation
            }
            for m in messages
        ]
    finally:
        db.close()

# ── Helpers ────────────────────────────────────────────────────────────────

def _extract_db_name(conn_str: str) -> str:
    """Extract database name from connection string for display."""
    try:
        parts = conn_str.split("/")
        name = parts[-1].split("?")[0]
        return name or "database"
    except Exception:
        return "database"

async def _get_table_schemas(ctx: SessionContext) -> Dict[str, List[str]]:
    """Fetch all table schemas (column names) for the session."""
    if ctx.tables:
        return {t.table_name.lower(): [c.name for c in t.columns] for t in ctx.tables}
    
    # Fallback to Qdrant (for loaded models)
    from app.training.indexer import Indexer
    indexer = Indexer()
    try:
        # Search with a broad query to get all table payloads
        results = await indexer.search(ctx.qdrant_collection, "list all tables", top_k=200)
        schemas = {}
        for r in results:
            t_name = r.get("table_name", "").lower()
            # columns could be list of strings or list of dicts
            cols_raw = r.get("columns", [])
            cols = []
            for c in cols_raw:
                if isinstance(c, dict):
                    cols.append(c.get("name", ""))
                else:
                    cols.append(str(c))
            if t_name:
                schemas[t_name] = cols
        return schemas
    except Exception as e:
        logger.error(f"Failed to fetch schemas from Qdrant: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════════════════
#  CHRONOPLOT — UserType enum & role-check helpers
# ═══════════════════════════════════════════════════════════════════════════

class UserType(IntEnum):
    SuperAdmin  = 2
    SiteAdmin   = 3
    Manager     = 4
    Operator    = 5
    ServiceUser = 6
    Visitor     = 7

_CHRONOPLOT_ALLOWED = {int(UserType.SuperAdmin), int(UserType.Manager), int(UserType.SiteAdmin)}


def _cp_extract_token(request: Request) -> str:
    """Pull Bearer token from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header. Expected 'Bearer <token>'.",
        )
    return auth[len("Bearer "):]


def _cp_decode_jwt(token: str) -> dict:
    """Decode JWT without signature verification to extract the sub claim."""
    try:
        return jose_jwt.decode(
            token,
            key="",
            options={"verify_signature": False, "verify_exp": False, "verify_aud": False},
        )
    except JWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid JWT: {exc}")

_CP_ROLE_ENGINE: Engine | None = None

def _cp_get_role_engine() -> Engine:
    global _CP_ROLE_ENGINE
    if _CP_ROLE_ENGINE is None:
        conn_str = os.environ.get("CHRONOPLOT_DB_CONN", "")
        if not conn_str:
            raise HTTPException(
                status_code=500,
                detail="Server misconfiguration: CHRONOPLOT_DB_CONN env variable is not set.",
            )
        try:
            _CP_ROLE_ENGINE = create_engine(
                conn_str,
                pool_pre_ping=True,
                pool_size=2,
                max_overflow=3,
                pool_recycle=1800,
            )
        except Exception as exc:
            logger.error(f"[chronoplot] Failed to create role engine: {exc}", exc_info=True)
            raise HTTPException(status_code=500, detail="Could not connect to chronoplot_DB for role check.")
    return _CP_ROLE_ENGINE


def _cp_check_role(user_detail_id: str) -> int:
    cp_engine = _cp_get_role_engine()  

    sql = sa_text(
        "SELECT [UserType] FROM [chronoplot_DB].[dbo].[BNR_UserDetails] WHERE Id = :uid"
    )
    try:
        with cp_engine.connect() as conn:
            row = conn.execute(sql, {"uid": user_detail_id}).fetchone()
    except Exception as exc:
        logger.error(f"[chronoplot] BNR_UserDetails query failed: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail="Role verification query failed.")

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"User '{user_detail_id}' not found in BNR_UserDetails.",
        )

    user_type_val = int(row[0])
    logger.info(f"[chronoplot] user_id={user_detail_id} UserType={user_type_val}")

    if user_type_val not in _CHRONOPLOT_ALLOWED:
        try:
            type_name = UserType(user_type_val).name
        except ValueError:
            type_name = str(user_type_val)
        raise HTTPException(
            status_code=403,
            detail=(
                f"Access denied. Your role '{type_name}' is not permitted. "
                "Only SuperAdmin, SiteAdmin and Manager roles are allowed."
            ),
        )
    return user_type_val


def _cp_authorize(request: Request) -> tuple:
    """Full auth: extract token → decode → role check. Returns (user_detail_id, user_type, payload)."""
    token = _cp_extract_token(request)
    payload = _cp_decode_jwt(token)
    user_detail_id = payload.get("USER_DETAIL_ID")
    logger.info(f"User details Id : {user_detail_id}")
    if not user_detail_id:
        raise HTTPException(
            status_code=401,
            detail="JWT does not contain a recognisable user ID claim (USER_DETAIL_ID).",
        )
    user_type = _cp_check_role(str(user_detail_id))
    return str(user_detail_id), user_type, payload


def _cp_rebuild_ctx(persisted: dict) -> SessionContext:
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


# ─── Chronoplot Request / Response models ─────────────────────────────────
def save_message(thread_id, role, content, **kwargs):
    if not thread_id:
        return
    db = AppDBSession()
    try:
        msg = ChatThreadMessage(
            thread_id=thread_id,
            role=role,
            content=content,
            sql_used=kwargs.get("sql_used"),
            table_data=kwargs.get("table_data"),
            columns=kwargs.get("columns"),
            stats=kwargs.get("stats"),
            visualization_hint=kwargs.get("visualization_hint"),
            chart_x_axis=kwargs.get("chart_x_axis"),
            chart_y_axis=kwargs.get("chart_y_axis"),
            explanation=kwargs.get("explanation"),
        )
        db.add(msg)
        db.commit()
    except Exception as e:
        logger.error(f"[chronoplot] Save message failed: {e}", exc_info=True)
    finally:
        db.close()

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

class ChronoTableDescInput(BaseModel):
    table_name: str
    user_description: Optional[str] = None

class ChronoTrainRequest(BaseModel):
    session_id: str
    tables: List[ChronoTableDescInput]

class ChronoCreateThreadRequest(BaseModel):
    db_session_id: str
    title: str

class ChronoSaveModelRequest(BaseModel):
    session_id: str
    name: str

class ChronoUpdateModelMetadataRequest(BaseModel):
    common_filter_keys: List[str]

# ═══════════════════════════════════════════════════════════════════════════
#  CHRONOPLOT — Train endpoints  (no auth required)
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/chronoplot/connect", response_model=ChronoConnectResponse)
async def chronoplot_connect(
    request: Request,
    type: str = Form(...),
    connection_string: Optional[str] = Form(None),
    db_file: Optional[UploadFile] = File(None),
):
    """Connect a database for chronoplot (upload or connection string)."""
    session_id = str(uuid.uuid4())
    try:
        if type == "upload":
            if not db_file:
                raise HTTPException(400, "No file uploaded.")
            file_bytes = await db_file.read()
            engine = connector.from_upload(session_id, file_bytes, db_file.filename or "db.sqlite")
            dialect = "sqlite"
            db_name = db_file.filename or "uploaded_database"
            db_key = hashlib.sha256(file_bytes).hexdigest()
        elif type == "connection_string":
            if not connection_string:
                raise HTTPException(400, "connection_string is required.")
            engine = connector.from_connection_string(connection_string)
            dialect = connector._detect_dialect(connection_string)
            db_name = _extract_db_name(connection_string)
            db_key = hashlib.sha256(connection_string.encode()).hexdigest()
        else:
            raise HTTPException(400, "type must be 'upload' or 'connection_string'.")
    except (ConnectionError, UnsupportedDialectError) as exc:
        raise HTTPException(422, str(exc))

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


@app.get("/api/chronoplot/train/{session_id}")
async def chronoplot_train(session_id: str):
    """Train chronoplot and return result directly."""
    ctx = await get_session_store().get(session_id)
    if not ctx:
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(404, "Session not found or expired.")
        ctx = _cp_rebuild_ctx(persisted)

    pipeline = TrainingPipeline()

    try:
        cached_data = pipeline._load_schema_cache(ctx.db_key) if ctx.db_key else None
        if cached_data is not None:
            tables, _ = cached_data
        else:
            tables = await pipeline.extract_only(session_id, ctx.engine)

        ctx.tables = tables
        await get_session_store().register(ctx)

        return {
            "step": "tables_ready",
            "progress": 100,
            "tables_done": len(tables),
            "tables_total": len(tables),
            "message": "Tables extracted. Please provide descriptions.",
            "error": ""
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/chronoplot/tables/{session_id}")
async def chronoplot_get_tables(session_id: str):
    """Return extracted tables for a chronoplot session (before training)."""
    ctx = await get_session_store().get(session_id)
    if not ctx:
        persisted = load_session(session_id)
        if not persisted:
            raise HTTPException(404, "Session not found or expired.")
        ctx = _cp_rebuild_ctx(persisted)
        await get_session_store().register(ctx)   # put back in memory for next call
    pipeline = TrainingPipeline()
    tables = await pipeline.extract_only(session_id, ctx.engine)
    ctx.tables = tables
    return [
        {"table_name": t.table_name, "schema_name": t.schema_name,
         "row_count": t.row_count, "columns": [c.name for c in t.columns]}
        for t in tables
    ]


@app.post("/api/chronoplot/train-with-input")
async def chronoplot_train_with_input(body: ChronoTrainRequest):
    """Run chronoplot training with user-provided table descriptions."""
    logger.info(f"Received training input for session {body.session_id} with {len(body.tables)} table descriptions.")
    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        # Fallback: session may have been evicted from memory (e.g. server restart, LRU eviction)
        persisted = load_session(body.session_id)
        logger.info(f"Session context not found in memory for session_id={body.session_id}. Attempting to load from persistence.")
        if not persisted:
            raise HTTPException(404, "Session not found or expired.")
        ctx = _cp_rebuild_ctx(persisted)
        logger.info(ctx)
        await get_session_store().register(ctx)  # restore into memory

    # ctx.tables is always present (dataclass default=[]), but may be empty
    # if /tables was never called. Re-extract when the list is empty.
    if not ctx.tables:
        pipeline_pre = TrainingPipeline()
        cached = pipeline_pre._load_schema_cache(ctx.db_key) if ctx.db_key else None
        if cached:
            ctx.tables, _ = cached
            logger.info(f"[chronoplot] Loaded {len(ctx.tables)} tables from cache.")
        elif ctx.connection_string:
            logger.info(f"[chronoplot] No cache found, rebuilding engine from connection_string.")
            ctx.engine = connector.from_connection_string(ctx.connection_string)
            ctx.tables = await pipeline_pre.extract_only(body.session_id, ctx.engine)
            logger.info(f"[chronoplot] Extracted {len(ctx.tables)} tables from DB.")
        else:
            raise HTTPException(400, "No cached schema and no connection string available.")



    user_desc_map = {t.table_name: t.user_description for t in body.tables if t.user_description}
    pipeline = TrainingPipeline()

    try:
        last_progress = None
        async for progress in pipeline.run_with_user_input(
            session_id=body.session_id,
            tables=ctx.tables,
            user_descs=user_desc_map,
            dialect=ctx.dialect,
            qdrant_collection=ctx.qdrant_collection,
            db_key=ctx.db_key,
        ):
            last_progress = progress

        return last_progress.__dict__ if last_progress else {"message": "Training complete"}

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ═══════════════════════════════════════════════════════════════════════════
#  CHRONOPLOT — Chat endpoints  (JWT + role check required)
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/chronoplot/chat/query", response_model=ChronoChatResponse)
async def chronoplot_chat_query(request: Request, body: ChronoChatRequest, _token: Any = Depends(cp_bearer)):
    """
    NL → SQL → response pipeline.
    Requires Authorization: Bearer <jwt> with UserType SuperAdmin(2) or Manager(4).
    """
    user_detail_id, user_type, jwt_payload = _cp_authorize(request)

    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        persisted = load_session(body.session_id)
        if not persisted:
            raise HTTPException(404, "Session not found or expired.")
        ctx = _cp_rebuild_ctx(persisted)

    if not ctx.connection_string:
        raise HTTPException(400, "Session was restored from an upload; please re-upload your file.")

    memory_key = str(body.thread_id) if body.thread_id else body.session_id
    mem = get_session_memory(memory_key)
    conversation_context = mem.get_context_for_prompt()
    max_retries = 3
    last_error = ""
    zero_rows_on_last_attempt = False
    JSON_REMINDER = (
        "\n⚠️ MANDATORY: Your response MUST be a single valid JSON object. "
        'Return ONLY: {"sql": "...", "chat_response": "", "response_intent": "data", "reason": "..."}'
    )

    question = await corrector.correct(body.question)
    save_message(body.thread_id, "user", question)
    logger.info(f"[chronoplot] Corrected question: {question}")
    eng = connector.from_connection_string(ctx.connection_string)

    # Fallback: if session has no filter keys, try to fetch from SavedModel table by db_key
    logger.info(f"[chronoplot] Checking for common_filter_keys in session context: {getattr(ctx, 'common_filter_keys', None)}, db_key: {getattr(ctx, 'db_key', None)}")
    if not getattr(ctx, "common_filter_keys", None) and getattr(ctx, "db_key", None):
        db_s = AppDBSession()
        try:
            model = db_s.query(SavedModel).filter(SavedModel.db_key == ctx.db_key).order_by(SavedModel.created_at.desc()).first()
            logger.info(f"[chronoplot] Fallback: Queried SavedModel with db_key={ctx.db_key}, found: {bool(model)} and filters: {getattr(model, 'common_filter_keys', None)}")
            if model and model.common_filter_keys:
                ctx.common_filter_keys = model.common_filter_keys
                logger.info(f"[chronoplot] Fallback: Found saved model with filters: {ctx.common_filter_keys}")
                save_session(ctx)
        except Exception as exc:
            logger.error(f"[chronoplot] Fallback: Failed to fetch model filters: {exc}")
        finally:
            db_s.close()
    
    # FOR TESTING PURPOSE: Ensure SiteID is in common_filter_keys for Chronoplot
    if not getattr(ctx, "common_filter_keys", None):
        logger.info("[chronoplot] Testing: Forcing 'SiteID' into common_filter_keys")
        ctx.common_filter_keys = ["site_id"]

    # SuperAdmin bypasses filters
    is_super_admin = (user_type == int(UserType.SuperAdmin))
    effective_is_owner = getattr(ctx, "is_owner", False) or is_super_admin

    for attempt in range(max_retries):
        try:
            plan = await planner.plan(
                collection_name=ctx.qdrant_collection,
                question=question,
                dialect=ctx.dialect,
                conversation_context=conversation_context,
            )
            if plan.needs_clarification and not plan.relevant_tables:
                text = "\n".join(plan.clarification_questions)
                save_message(body.thread_id, "assistant", text)
                return ChronoChatResponse(mode="empty", text_summary="\n".join(plan.clarification_questions))

            gen_result = await generator.generate(plan, conversation_context, ctx.qdrant_collection)
            if gen_result.chat_response and not gen_result.sql:
                save_message(body.thread_id, "assistant", gen_result.chat_response)
                return ChronoChatResponse(mode="chat", text_summary=gen_result.chat_response, explanation=gen_result.explanation)

            val_result = validator.validate(gen_result.sql, ctx.dialect)
            if not val_result.is_valid:
                last_error = "; ".join(val_result.errors)
                conversation_context += f"\n[PREVIOUS ATTEMPT FAILED: {last_error}. Fix and try again.]"
                continue

            safe_sql = val_result.sql

            # Apply mandatory filters for non-owners (Managers and SiteAdmins)
            logger.info(f"Chronoplot Filter : {getattr(ctx, 'common_filter_keys', None)} Is super admin : {is_super_admin}")
            if not is_super_admin and getattr(ctx, "common_filter_keys", None):
                try:
                    from app.query.filter_verifier import SQLFilterVerifier
                    verifier = SQLFilterVerifier(dialect=ctx.dialect)
                    table_schemas = await _get_table_schemas(ctx)
                    
                    # Prepare overrides for filter values (e.g. SiteID from JWT)
                    filter_values = getattr(ctx, "session_filter_values", {}).copy()
                    site_id_val = jwt_payload.get("SITE_ID")
                    logger.info(f"[chronoplot] JWT SITE_ID: {site_id_val}, common_filter_keys: {ctx.common_filter_keys}")
                    # If common_filter_keys contains SiteID, and we have it in JWT, use the JWT value
                    for key in ctx.common_filter_keys:
                        if key.lower() in ["siteid", "site_id"]:
                            if site_id_val is not None:
                                filter_values[key] = site_id_val
                                logger.info(f"[chronoplot] Using SITE_ID from JWT: {site_id_val}")
                    
                    # Column mapping for tables where site_id is represented by another name (e.g. 'id' in bnr_sites)
                    column_mappings = {
                        "bnr_sites": { "site_id": "id", "siteid": "id" }
                    }

                    safe_sql = verifier.verify_and_inject(
                        sql=safe_sql,
                        filter_keys=ctx.common_filter_keys,
                        filter_values=filter_values,
                        table_schemas=table_schemas,
                        column_mappings=column_mappings
                    )
                    logger.info(f"Filtered SQL applied: {safe_sql}")
                except PermissionError as exc:
                    return ChronoChatResponse(mode="empty", text_summary=str(exc), sql_used=val_result.sql)
                except Exception as exc:
                    logger.error(f"[chronoplot] Filter injection failed: {exc}", exc_info=True)
                    return ChronoChatResponse(mode="empty", text_summary="Security verification failed.")

            result = await executor.execute(eng, safe_sql)

            if not result.rows and not result.error:
                zero_rows_on_last_attempt = True
                last_error = "Query returned 0 rows"
                conversation_context += (
                    f"\n[PREVIOUS SQL RETURNED 0 ROWS: {safe_sql}\n"
                    "Try relaxing filters or using broader LIKE patterns.]"
                    + JSON_REMINDER
                )
                continue

            if result.error:
                last_error = result.error
                conversation_context += f"\n[SQL EXECUTION ERROR: {last_error}. Rewrite the SQL.]{JSON_REMINDER}"
                continue

            zero_rows_on_last_attempt = False
            formatted = await formatter.format(
                question=question,
                rows=result.rows,
                columns=result.columns,
                sql=safe_sql,
                explanation=gen_result.explanation,
                page=body.page,
            )

            mem.add_turn(ConversationTurn(
                question=question,
                sql_generated=safe_sql,
                result_summary=f"{result.row_count} rows, columns: {', '.join(result.columns[:5])}",
                result_sample=result.rows[:3],
                services_used=[ctx.db_name],
                filters_applied=[],
            ))

            if body.thread_id:
                save_message(
                    body.thread_id,
                    "assistant",
                    formatted.text_summary,
                    sql_used=safe_sql,
                    table_data=formatted.table_data,
                    columns=formatted.columns,
                    stats=formatted.stats,
                    visualization_hint=formatted.visualization_hint,
                    chart_x_axis=formatted.chart_x_axis,
                    chart_y_axis=formatted.chart_y_axis,
                    explanation=formatted.explanation,
                )

            return ChronoChatResponse(
                mode=formatted.mode, text_summary=formatted.text_summary,
                table_data=formatted.table_data, columns=formatted.columns,
                total_rows=formatted.total_rows, stats=formatted.stats,
                visualization_hint=formatted.visualization_hint,
                chart_x_axis=formatted.chart_x_axis, chart_y_axis=formatted.chart_y_axis,
                sql_used=formatted.sql_used, explanation=formatted.explanation,
                page=formatted.page, pages_total=formatted.pages_total,
            )

        except HTTPException:
            raise
        except Exception as exc:
            last_error = str(exc)
            logger.error(f"[chronoplot] Pipeline error (attempt {attempt + 1}): {exc}", exc_info=True)
            conversation_context += f"\n[ERROR: {last_error}. Try a different approach.]{JSON_REMINDER}"

    if zero_rows_on_last_attempt:
        no_data_text = await formatter._humanize_no_data(question)
        save_message(body.thread_id, "assistant", no_data_text)
        return ChronoChatResponse(mode="no_data", text_summary=no_data_text, page=1, pages_total=1)

    return ChronoChatResponse(
        mode="empty",
        text_summary=f"I wasn't able to answer after {max_retries} attempts. Last error: {last_error}.",
    )


@app.get("/api/chronoplot/chat/threads")
def chronoplot_get_threads(request: Request, _token: Any = Depends(cp_bearer)):
    """List chat threads for the authenticated chronoplot user."""
    user_detail_id, user_type, payload = _cp_authorize(request)
    db = AppDBSession()
    try:
        user = db.query(User).filter(User.username == user_detail_id).first()
        if not user:
            return []
        threads = (
            db.query(ChatThread)
            .filter(ChatThread.user_id == user.id)
            .order_by(ChatThread.created_at.desc())
            .all()
        )
        return [{"id": t.id, "title": t.title, "created_at": t.created_at, "db_session_id": t.db_session_id} for t in threads]
    finally:
        db.close()


@app.post("/api/chronoplot/chat/threads")
def chronoplot_create_thread(request: Request, body: ChronoCreateThreadRequest, _token: Any = Depends(cp_bearer)):
    """Create a new chat thread for the authenticated chronoplot user."""
    user_detail_id, user_type, payload = _cp_authorize(request)
    db = AppDBSession()
    try:
        user = db.query(User).filter(User.username == user_detail_id).first()
        if not user:
            user = User(username=user_detail_id, password_hash=hash_password(uuid.uuid4().hex))
            db.add(user)
            db.commit()
            db.refresh(user)
        t = ChatThread(user_id=user.id, db_session_id=body.db_session_id, title=body.title)
        db.add(t)
        db.commit()
        db.refresh(t)
        return {"id": t.id, "title": t.title, "db_session_id": t.db_session_id,
                "created_at": t.created_at.isoformat() if t.created_at else None}
    finally:
        db.close()


@app.get("/api/chronoplot/chat/threads/{thread_id}/messages")
def chronoplot_get_thread_messages(thread_id: int, request: Request, _token: Any = Depends(cp_bearer)):
    """Get all messages in a thread. Requires SuperAdmin or Manager JWT."""
    _cp_authorize(request)
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
            {"id": m.id, "role": m.role, "content": m.content, "sql_used": m.sql_used,
             "table_data": m.table_data, "columns": m.columns, "stats": m.stats,
             "visualization_hint": m.visualization_hint, "chart_x_axis": m.chart_x_axis,
             "chart_y_axis": m.chart_y_axis, "explanation": m.explanation}
            for m in messages
        ]
    finally:
        db.close()


@app.get("/api/chronoplot/models")
async def chronoplot_list_models(
    request: Request,
    _token: Any = Depends(cp_bearer),
):
    """List all saved Chronoplot models with owner info."""
    user_detail_id, user_type, jwt_payload = _cp_authorize(request)
    is_super_admin = (user_type == int(UserType.SuperAdmin))

    db = AppDBSession()
    try:
        models = (
            db.query(SavedModel, User.username)
            .join(User, SavedModel.user_id == User.id)
            .all()
        )
        return [
            {
                "id": m.SavedModel.id,
                "name": m.SavedModel.name,
                "dialect": m.SavedModel.dialect,
                "db_name": m.SavedModel.db_name,
                "common_filter_keys": m.SavedModel.common_filter_keys or [],
                "created_at": m.SavedModel.created_at,
                "is_owner": (m.username == str(user_detail_id)) or is_super_admin,
                "owner_name": m.username,
            }
            for m in models
        ]
    finally:
        db.close()


@app.post("/api/chronoplot/models/save")
async def chronoplot_save_model(
    request: Request,
    body: ChronoSaveModelRequest,
    _token: Any = Depends(cp_bearer),
):
    user_detail_id, user_type, jwt_payload = _cp_authorize(request)

    ctx = await get_session_store().get(body.session_id)
    if not ctx:
        ctx_data = load_session(body.session_id)
        if not ctx_data:
            raise HTTPException(status_code=404, detail="Session not found or expired.")
        ctx = _cp_rebuild_ctx(ctx_data)

    db = AppDBSession()
    try:
        # Reuse the same upsert pattern as chronoplot_create_thread
        # user_detail_id is stored as username — row likely already exists from chat
        user = db.query(User).filter(User.username == str(user_detail_id)).first()
        if not user:
            user = User(
                username=str(user_detail_id),
                password_hash=hash_password(uuid.uuid4().hex),
            )
            db.add(user)
            db.commit()
            db.refresh(user)

        model = SavedModel(
            user_id=user.id,
            name=body.name,
            dialect=ctx.dialect,
            db_name=ctx.db_name,
            connection_string=ctx.connection_string or "",
            db_key=ctx.db_key,
            qdrant_collection=ctx.qdrant_collection,
            common_filter_keys=getattr(ctx, "common_filter_keys", []),
        )
        db.add(model)
        db.commit()
        db.refresh(model)
        return {"message": "Model saved successfully", "id": model.id}
    finally:
        db.close()


@app.post("/api/chronoplot/models/{model_id}/load")
async def chronoplot_load_model(
    model_id: int,
    request: Request,
    _token: Any = Depends(cp_bearer),
):
    user_detail_id, user_type, jwt_payload = _cp_authorize(request)

    db = AppDBSession()
    try:
        # Any authenticated chronoplot user can load any model
        result = (
            db.query(SavedModel, User.username)
            .join(User, SavedModel.user_id == User.id)
            .filter(SavedModel.id == model_id)
            .first()
        )
        if not result:
            raise HTTPException(status_code=404, detail="Model not found.")

        model, owner_username = result.SavedModel, result.username
    finally:
        db.close()

    session_id = str(uuid.uuid4())

    try:
        engine = (
            connector.from_connection_string(model.connection_string)
            if model.connection_string
            else None
        )
    except Exception:
        engine = None

    # SuperAdmin bypasses filters regardless of ownership
    is_super_admin = (user_type == int(UserType.SuperAdmin))
    effective_is_owner = (owner_username == str(user_detail_id)) or is_super_admin

    ctx = SessionContext(
        session_id=session_id,
        engine=engine,
        dialect=model.dialect,
        db_name=model.db_name,
        source_type="connection_string" if model.connection_string else "upload",
        table_count=0,
        qdrant_collection=model.qdrant_collection,
        db_key=model.db_key,
        connection_string=model.connection_string,
        common_filter_keys=model.common_filter_keys or [],
        is_owner=effective_is_owner,
    )
    await get_session_store().register(ctx)
    save_session(ctx)

    logger.info(
        f"[chronoplot] Model {model_id} loaded by user_detail_id={user_detail_id} "
        f"(is_owner={effective_is_owner}, session={session_id})"
    )
    return {
        "session_id": session_id,
        "is_owner": effective_is_owner,
        "message": "Model loaded successfully",
    }