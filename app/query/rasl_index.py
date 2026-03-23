import json
import logging
import uuid
from dataclasses import dataclass

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, Filter, FieldCondition, MatchValue

from app.config import get_settings
from app.utils.gemini_key_manager import get_key_manager

logger = logging.getLogger(__name__)
EMBEDDING_DIM = 3072


@dataclass
class EntityChunk:
    chunk_id: str         # "TableName.__table__" or "TableName.col_name"
    level: str            # "table" or "column"
    table_name: str
    column_name: str | None
    text: str


def _qdrant_client() -> QdrantClient:
    s = get_settings()
    return QdrantClient(
        url="https://1e3848ab-b68d-441c-beda-d086923df770.us-east4-0.gcp.cloud.qdrant.io",
        api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.rXXjSwuYvpQ_xagT0PI-wRCieyldSWBI5yIcM4SMRI4",
        prefer_grpc=False,
    )


def _rasl_collection(session_id: str) -> str:
    """Separate collection from your table-level collection — keeps concerns clean."""
    return f"rasl_chunks_{session_id}"


# ── Chunking ──────────────────────────────────────────────────────────────────

def build_chunks(report: list[dict]) -> list[EntityChunk]:
    chunks = []
    for t in report:
        table = t["table_name"]
        schema = t.get("schema_name", "dbo")
        desc = t.get("description", "")

        # Table-level chunk
        chunks.append(EntityChunk(
            chunk_id=f"{table}.__table__",
            level="table",
            table_name=table,
            column_name=None,
            text=f"Table {schema}.{table}. {desc}".strip(),
        ))

        # Column-level chunks
        # Your schema_report columns are strings like: "col_name (TYPE) nullable"
        # or dicts {"name": ..., "type": ..., "nullable": ...} depending on your extractor
        for col in t.get("columns", []):
            if isinstance(col, dict):
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                col_text = f"Column {col_name} ({col_type}) in table {table}."
            else:
                # formatted string — first word is the column name
                col_name = col.split()[0] if col else ""
                col_text = f"Column {col_name} in table {table}. {col}"

            if not col_name:
                continue

            chunks.append(EntityChunk(
                chunk_id=f"{table}.{col_name}",
                level="column",
                table_name=table,
                column_name=col_name,
                text=col_text,
            ))

    return chunks


# ── Index builder ─────────────────────────────────────────────────────────────

class RASLIndex:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.collection = _rasl_collection(session_id)
        self.client = _qdrant_client()

    def _collection_exists(self) -> bool:
        try:
            cols = self.client.get_collections().collections
            return any(c.name == self.collection for c in cols)
        except Exception:
            return False

    async def build(self, report: list[dict], force_rebuild: bool = False):
        """Embed every entity chunk and upsert into Qdrant."""
        if self._collection_exists() and not force_rebuild:
            logger.info(f"RASL collection '{self.collection}' already exists — skipping rebuild.")
            return

        # (Re)create collection
        self.client.recreate_collection(
            collection_name=self.collection,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
        )

        chunks = build_chunks(report)
        logger.info(f"RASL: building {len(chunks)} chunks for session {self.session_id}")

        points: list[PointStruct] = []
        for chunk in chunks:
            # Use retrieval_document task type at index time — matches your existing Indexer
            embedding = await get_key_manager().embed_content(
                text=chunk.text,
                task_type="retrieval_document",
            )
            points.append(PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk.chunk_id)),  # stable deterministic ID
                vector=embedding,
                payload={
                    "chunk_id":    chunk.chunk_id,
                    "level":       chunk.level,
                    "table_name":  chunk.table_name,
                    "column_name": chunk.column_name,
                    "text":        chunk.text,
                },
            ))

        # Batch upsert
        batch_size = 50
        for i in range(0, len(points), batch_size):
            self.client.upsert(collection_name=self.collection, points=points[i:i+batch_size])
            logger.info(f"RASL upsert batch {i//batch_size + 1} ({len(points[i:i+batch_size])} pts)")

        logger.info(f"RASL index ready: {len(points)} chunks in '{self.collection}'")

    async def retrieve(self, keywords: list[str], top_k: int = 5) -> list[EntityChunk]:
        """Embed each keyword with retrieval_query, search Qdrant, deduplicate."""
        seen: set[str] = set()
        results: list[EntityChunk] = []

        for keyword in keywords:
            embedding = await get_key_manager().embed_content(
                text=keyword,
                task_type="retrieval_query",   # query task type — important for Gemini embeddings
            )
            hits = self.client.query_points(
                collection_name=self.collection,
                query=embedding,
                limit=top_k,
                with_payload=True,
            )
            for hit in hits.points:
                p = hit.payload
                cid = p["chunk_id"]
                if cid not in seen:
                    seen.add(cid)
                    results.append(EntityChunk(
                        chunk_id=p["chunk_id"],
                        level=p["level"],
                        table_name=p["table_name"],
                        column_name=p.get("column_name"),
                        text=p["text"],
                    ))

        return results