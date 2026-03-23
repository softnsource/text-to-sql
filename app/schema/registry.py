"""Schema registry - stores and retrieves database schema metadata, manages schema versioning."""

import io
import logging
from typing import List, Optional, Tuple, Callable

import numpy as np
from sqlalchemy.orm import Session

from app.db.metadata_models import ServiceMeta, TableMeta, ColumnMeta, JoinKey, get_session

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Provides access to schema metadata stored in the metadata database."""

    def __init__(self, db_session_factory: Optional[Callable] = None):
        """Initialize schema registry.

        Args:
            db_session_factory: Factory function for creating database sessions.
                               If None, uses get_session from metadata_models.
        """
        self.db_session_factory = db_session_factory or get_session

    def get_all_tables(self) -> List[TableMeta]:
        """Get all tables across all services.

        Returns:
            List of TableMeta objects
        """
        with self.db_session_factory() as session:
            return session.query(TableMeta).join(ServiceMeta).order_by(
                ServiceMeta.service_name,
                TableMeta.schema_name,
                TableMeta.table_name
            ).all()

    def get_tables_for_service(self, service_name: str) -> List[TableMeta]:
        """Get all tables for a specific service.

        Args:
            service_name: Name of the service

        Returns:
            List of TableMeta objects
        """
        with self.db_session_factory() as session:
            service = session.query(ServiceMeta).filter_by(service_name=service_name).first()
            if not service:
                logger.warning(f"Service '{service_name}' not found in metadata")
                return []

            return session.query(TableMeta).filter_by(service_id=service.id).order_by(
                TableMeta.schema_name,
                TableMeta.table_name
            ).all()

    def get_columns_for_table(self, table_id: int) -> List[ColumnMeta]:
        """Get all columns for a table.

        Args:
            table_id: ID of the table

        Returns:
            List of ColumnMeta objects ordered by ordinal position
        """
        with self.db_session_factory() as session:
            return session.query(ColumnMeta).filter_by(table_id=table_id).order_by(
                ColumnMeta.ordinal_position
            ).all()

    def get_join_keys(self) -> List[JoinKey]:
        """Get all cross-service join keys.

        Returns:
            List of JoinKey objects
        """
        with self.db_session_factory() as session:
            return session.query(JoinKey).order_by(JoinKey.column_name).all()

    def search_tables_by_embedding(
        self,
        query_embedding: List[float],
        top_k: int = 10
    ) -> List[Tuple[TableMeta, float]]:
        """Cosine similarity search against table embeddings.

        Args:
            query_embedding: Query embedding vector
            top_k: Number of top results to return

        Returns:
            List of (TableMeta, similarity_score) tuples sorted by score descending
        """
        with self.db_session_factory() as session:
            # Get all tables with embeddings
            tables = session.query(TableMeta).filter(TableMeta.embedding.isnot(None)).all()

            if not tables:
                logger.warning("No table embeddings found in metadata")
                return []

            # Convert query embedding to numpy array
            query_vec = np.array(query_embedding, dtype=np.float32)
            query_norm = np.linalg.norm(query_vec)

            if query_norm == 0:
                logger.warning("Query embedding has zero norm")
                return []

            # Compute cosine similarity for each table
            results = []
            for table in tables:
                try:
                    # Deserialize embedding from bytes
                    table_vec = self._deserialize_embedding(table.embedding)

                    if table_vec is None:
                        continue

                    # Compute cosine similarity
                    table_norm = np.linalg.norm(table_vec)
                    if table_norm == 0:
                        continue

                    similarity = np.dot(query_vec, table_vec) / (query_norm * table_norm)
                    results.append((table, float(similarity)))

                except Exception as e:
                    logger.warning(
                        f"Failed to compute similarity for table {table.table_name}: {e}"
                    )
                    continue

            # Sort by similarity descending and take top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

    def get_table_schema_text(self, table: TableMeta) -> str:
        """Build a text representation of a table's schema for Gemini prompt.

        Args:
            table: TableMeta object

        Returns:
            Formatted schema text
        """
        lines = []

        # Table header
        lines.append(f"TABLE: {table.schema_name}.{table.table_name}")

        # Description
        if table.description:
            lines.append(f"DESCRIPTION: {table.description}")

        # Service info
        with self.db_session_factory() as session:
            service = session.query(ServiceMeta).filter_by(id=table.service_id).first()
            if service:
                lines.append(f"SERVICE: {service.display_name or service.service_name}")

        # Columns
        columns = self.get_columns_for_table(table.id)
        if columns:
            lines.append("COLUMNS:")
            for col in columns:
                col_parts = [f"  {col.name} ({col.data_type})"]

                if col.is_primary_key:
                    col_parts.append("[PK]")
                if col.is_foreign_key and col.fk_target_table:
                    col_parts.append(f"[FK -> {col.fk_target_table}.{col.fk_target_column}]")
                if not col.is_nullable:
                    col_parts.append("[NOT NULL]")

                if col.column_comment:
                    col_parts.append(f"# {col.column_comment}")

                lines.append(" ".join(col_parts))

        # Foreign keys summary
        fk_columns = [c for c in columns if c.is_foreign_key]
        if fk_columns:
            lines.append("FOREIGN KEYS:")
            for col in fk_columns:
                lines.append(
                    f"  {col.name} -> {col.fk_target_table}.{col.fk_target_column}"
                )

        # Row count
        if table.row_count_estimate:
            lines.append(f"ROW COUNT (estimate): {table.row_count_estimate:,}")

        return "\n".join(lines)

    def _deserialize_embedding(self, embedding_bytes: bytes) -> Optional[np.ndarray]:
        """Deserialize embedding from bytes to numpy array.

        Args:
            embedding_bytes: Serialized embedding

        Returns:
            Numpy array or None if deserialization fails
        """
        try:
            buffer = io.BytesIO(embedding_bytes)
            return np.load(buffer, allow_pickle=False)
        except Exception as e:
            logger.warning(f"Failed to deserialize embedding: {e}")
            return None

    def get_service_by_name(self, service_name: str) -> Optional[ServiceMeta]:
        """Get service metadata by name.

        Args:
            service_name: Name of the service

        Returns:
            ServiceMeta object or None if not found
        """
        with self.db_session_factory() as session:
            return session.query(ServiceMeta).filter_by(service_name=service_name).first()

    def get_table_by_name(
        self,
        service_name: str,
        table_name: str,
        schema_name: str = "public"
    ) -> Optional[TableMeta]:
        """Get table metadata by service and table name.

        Args:
            service_name: Name of the service
            table_name: Name of the table
            schema_name: Schema name (defaults to "public")

        Returns:
            TableMeta object or None if not found
        """
        with self.db_session_factory() as session:
            service = session.query(ServiceMeta).filter_by(service_name=service_name).first()
            if not service:
                return None

            return session.query(TableMeta).filter_by(
                service_id=service.id,
                schema_name=schema_name,
                table_name=table_name
            ).first()

    def get_all_services(self) -> List[ServiceMeta]:
        """Get all registered services.

        Returns:
            List of ServiceMeta objects
        """
        with self.db_session_factory() as session:
            return session.query(ServiceMeta).order_by(ServiceMeta.service_name).all()
