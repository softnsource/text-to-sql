import asyncio
import json
import logging
import os
import pathlib
from datetime import datetime, timezone
from typing import Dict, Optional
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)
 
# ── Config ───────────────────────────────────────────────────────────────────
USER_HISTORY_DIR = pathlib.Path("uploads/user_history")
SYNC_INTERVAL_SECONDS = 300          # 5 minutes
GDRIVE_ROOT_FOLDER_NAME = "ChronoplotHistory"
 
# ── Lazy imports (so app still starts even if google libs aren't installed) ──
_drive_service = None          # cached Drive API client
_folder_id_cache: Dict[str, str] = {}   # local path → Drive folder ID
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Drive client initialisation
# ─────────────────────────────────────────────────────────────────────────────
 
def _build_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service

    try:
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/drive"]

        creds = None

        # ✅ Build creds from ENV directly
        access_token = os.getenv("GOOGLE_ACCESS_TOKEN")
        refresh_token = os.getenv("GOOGLE_REFRESH_TOKEN")
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")

        if all([access_token, refresh_token, client_id, client_secret]):
            creds = Credentials(
                token=access_token,
                refresh_token=refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret,
                scopes=scopes,
            )

        if not creds:
            raise EnvironmentError(
                "Missing Google OAuth env variables."
            )

        # 🔄 Auto refresh
        if creds.expired and creds.refresh_token:
            logger.info("[gdrive_sync] Token expired — refreshing...")
            creds.refresh(Request())

        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        logger.info("[gdrive_sync] Drive service initialised successfully.")
        return _drive_service

    except Exception as exc:
        logger.error(f"[gdrive_sync] Could not build Drive service: {exc}")
        return None
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Folder helpers
# ─────────────────────────────────────────────────────────────────────────────
 
def _get_or_create_folder(service, name: str, parent_id: str) -> str:
    """
    Return the Drive folder ID for `name` under `parent_id`.
    Creates the folder if it doesn't exist.
    """
    cache_key = f"{parent_id}/{name}"
    if cache_key in _folder_id_cache:
        return _folder_id_cache[cache_key]
 
    # Search for existing folder
    query = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])
 
    if files:
        folder_id = files[0]["id"]
    else:
        meta = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = service.files().create(
            body=meta,
            fields="id",
            supportsAllDrives=True
        ).execute()
        folder_id = folder["id"]
        logger.debug(f"[gdrive_sync] Created Drive folder: {name} ({folder_id})")
 
    _folder_id_cache[cache_key] = folder_id
    return folder_id
 
 
def _get_existing_file_id(service, name: str, parent_id: str) -> Optional[str]:
    """Return the file ID of an existing file (or None)."""
    query = f"name='{name}' and '{parent_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Upload logic
# ─────────────────────────────────────────────────────────────────────────────
 
def _upload_file(service, local_path: pathlib.Path, parent_id: str) -> None:
    """Upload (or update) a single JSON file to Drive."""
    from googleapiclient.http import MediaFileUpload
 
    file_name = local_path.name
    media = MediaFileUpload(str(local_path), mimetype="application/json", resumable=False)
 
    existing_id = _get_existing_file_id(service, file_name, parent_id)
    if existing_id:
        service.files().update(
            fileId=existing_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        logger.debug(f"[gdrive_sync] Updated: {local_path.name}")
    else:
        meta = {"name": file_name, "parents": [parent_id]}
        service.files().create(
            body=meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()
        logger.debug(f"[gdrive_sync] Uploaded: {local_path.name}")
 
 
def _sync_all() -> None:
    """
    Walk uploads/user_history/ and upload every .json file to Drive.
 
    Drive structure mirrors local:
      ChronoplotHistory/<user_detail_id>/<YYYY-MM-DD>.json
    """
    service = _build_drive_service()
    if service is None:
        return
 
    root_folder_id = os.getenv("GDRIVE_HISTORY_FOLDER_ID")
    if not root_folder_id:
        logger.warning("[gdrive_sync] GDRIVE_HISTORY_FOLDER_ID not set — skipping sync.")
        return
 
    if not USER_HISTORY_DIR.exists():
        return   # nothing to sync yet
 
    # Ensure root "ChronoplotHistory" sub-folder exists inside the shared folder
    root_id = _get_or_create_folder(service, GDRIVE_ROOT_FOLDER_NAME, root_folder_id)
 
    synced = 0
    errors = 0
 
    for user_dir in USER_HISTORY_DIR.iterdir():
        if not user_dir.is_dir():
            continue
        try:
            user_folder_id = _get_or_create_folder(service, user_dir.name, root_id)
            for json_file in user_dir.glob("*.json"):
                try:
                    _upload_file(service, json_file, user_folder_id)
                    synced += 1
                except Exception as exc:
                    errors += 1
                    logger.error(f"[gdrive_sync] Failed to upload {json_file}: {exc}")
        except Exception as exc:
            errors += 1
            logger.error(f"[gdrive_sync] Failed to process user dir {user_dir}: {exc}")
 
    logger.info(
        f"[gdrive_sync] Sync complete — {synced} file(s) synced, {errors} error(s). "
        f"[{datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}]"
    )
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Async background task (runs forever inside asyncio event loop)
# ─────────────────────────────────────────────────────────────────────────────
 
async def _sync_loop() -> None:
    """Infinite loop: sync every SYNC_INTERVAL_SECONDS seconds."""
    logger.info(
        f"[gdrive_sync] Background sync started "
        f"(interval={SYNC_INTERVAL_SECONDS}s)."
    )
    while True:
        try:
            # Run the blocking sync in a thread pool so it doesn't block the event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _sync_all)
        except Exception as exc:
            logger.error(f"[gdrive_sync] Unexpected error in sync loop: {exc}", exc_info=True)
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)
 
 
_sync_task: Optional[asyncio.Task] = None
 
 
def start_gdrive_sync_job() -> None:
    """
    Call this once at app startup.
 
    Example (FastAPI lifespan):
 
        from app.history.gdrive_sync import start_gdrive_sync_job
 
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            start_gdrive_sync_job()
            yield
 
        app = FastAPI(lifespan=lifespan)
 
    Or with the older @app.on_event("startup") style:
 
        @app.on_event("startup")
        async def startup():
            start_gdrive_sync_job()
    """
    global _sync_task
    if _sync_task and not _sync_task.done():
        logger.warning("[gdrive_sync] Sync job already running — skipping duplicate start.")
        return
    _sync_task = asyncio.ensure_future(_sync_loop())
    logger.info("[gdrive_sync] Sync task scheduled.")
 
 
def stop_gdrive_sync_job() -> None:
    """Cancel the background sync task (call during shutdown if needed)."""
    global _sync_task
    if _sync_task and not _sync_task.done():
        _sync_task.cancel()
        logger.info("[gdrive_sync] Sync task cancelled.")
