"""Schema crawler - dialect-aware crawler for PostgreSQL/SQL Server. Extracts table/column metadata, descriptions, row counts, PK/FK relationships."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.db.connections import ConnectionManager
from app.db.metadata_models import ServiceMeta, TableMeta, ColumnMeta, JoinKey

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Column metadata extracted from database."""
    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    column_comment: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    fk_target_table: Optional[str] = None
    fk_target_column: Optional[str] = None


@dataclass
class ForeignKeyInfo:
    """Foreign key relationship metadata."""
    source_table: str
    source_column: str
    target_schema: str
    target_table: str
    target_column: str


@dataclass
class TableInfo:
    """Complete table metadata."""
    schema_name: str
    table_name: str
    table_comment: Optional[str]
    row_count_estimate: int
    columns: List[ColumnInfo] = field(default_factory=list)


@dataclass
class JoinKeyInfo:
    """Cross-service join key."""
    column_name: str
    data_type: str
    services: List[str]


class SchemaCrawler:
    """Crawls database schemas from enabled services."""

    def __init__(self, connection_manager: ConnectionManager, settings: Optional[Settings] = None):
        """Initialize schema crawler.

        Args:
            connection_manager: Connection manager for database access
            settings: Application settings (uses global if not provided)
        """
        self.conn_mgr = connection_manager
        self.settings = settings or get_settings()

    async def crawl_all_services(self) -> Dict:
        """Crawl all enabled services in parallel.

        Returns:
            Summary statistics: {total_services, total_tables, total_columns, errors}
        """
        enabled_services = self.settings.get_enabled_services()
        logger.info(f"Starting schema crawl for {len(enabled_services)} services")

        tasks = [
            self._crawl_service_safe(service_name)
            for service_name in enabled_services.keys()
        ]

        results = await asyncio.gather(*tasks)

        total_tables = sum(len(tables) for tables in results if tables is not None)
        total_columns = sum(
            sum(len(t.columns) for t in tables)
            for tables in results if tables is not None
        )
        errors = sum(1 for r in results if r is None)

        summary = {
            "total_services": len(enabled_services),
            "total_tables": total_tables,
            "total_columns": total_columns,
            "errors": errors,
            "timestamp": datetime.now().isoformat()
        }

        logger.info(f"Crawl complete: {summary}")
        return summary

    async def _crawl_service_safe(self, service_name: str) -> Optional[List[TableInfo]]:
        """Crawl a service with error handling.

        Args:
            service_name: Name of the service to crawl

        Returns:
            List of TableInfo or None if failed
        """
        try:
            return await self.crawl_service(service_name)
        except Exception as e:
            logger.error(f"Failed to crawl service '{service_name}': {e}", exc_info=True)
            return None

    async def crawl_service(self, service_name: str) -> List[TableInfo]:
        """Crawl a single service's database schema (PostgreSQL/SQL Server).

        Args:
            service_name: Name of the service to crawl

        Returns:
            List of TableInfo objects
        """
        logger.info(f"Crawling service: {service_name}")

        conn = await self.conn_mgr.get_connection(service_name)
        dialect_name = conn.dialect.name.lower()
        
        try:
            # Build excluded schema list
            excluded_schemas = tuple(self.settings.crawler.excluded_schemas)

            # Query 1: Get all tables
            if dialect_name == 'postgresql':
                table_query = """
                    SELECT table_schema, table_name,
                           obj_description((table_schema || '.' || table_name)::regclass) as comment
                    FROM information_schema.tables
                    WHERE table_schema NOT IN %s
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                """
                table_rows = await conn.fetch(table_query, (excluded_schemas,))
            else:  # SQL Server
                table_query = """
                    SELECT TABLE_SCHEMA, TABLE_NAME, 
                           CASE WHEN ep.value IS NULL THEN '' ELSE ep.value END as comment
                    FROM INFORMATION_SCHEMA.TABLES t
                    LEFT JOIN sys.tables st ON t.TABLE_NAME = st.name
                    LEFT JOIN sys.extended_properties ep ON st.object_id = ep.major_id 
                        AND ep.name = 'MS_Description' AND ep.class = 1 AND ep.minor_id = 0
                    WHERE TABLE_TYPE = 'BASE TABLE'
                      AND TABLE_SCHEMA NOT IN ({})
                    ORDER BY TABLE_SCHEMA, TABLE_NAME
                """.format(','.join(['?' for _ in excluded_schemas]))
                table_rows = await conn.fetch(table_query, *excluded_schemas)

            # Filter by excluded table patterns
            tables = {}
            for row in table_rows:
                table_name = row['table_name']
                if self._is_table_excluded(table_name):
                    continue

                key = (row['table_schema'], table_name)
                tables[key] = TableInfo(
                    schema_name=row['table_schema'],
                    table_name=table_name,
                    table_comment=row['comment'],
                    row_count_estimate=0
                )

            if not tables:
                logger.info(f"No tables found for service '{service_name}'")
                return []

            # Query 2: Get columns with comments
            if dialect_name == 'postgresql':
                column_query = """
                    SELECT table_schema, table_name, column_name, data_type,
                           is_nullable, ordinal_position,
                           col_description((table_schema || '.' || table_name)::regclass, ordinal_position) as column_comment
                    FROM information_schema.columns
                    WHERE table_schema NOT IN %s
                    ORDER BY table_schema, table_name, ordinal_position
                """
                column_rows = await conn.fetch(column_query, (excluded_schemas,))
            else:  # SQL Server
                column_query = """
                    SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE,
                           c.IS_NULLABLE, c.ORDINAL_POSITION,
                           ISNULL(ep.value, '') as column_comment
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN sys.extended_properties ep ON c.TABLE_NAME = OBJECT_NAME(ep.major_id) 
                        AND c.COLUMN_NAME = COL_NAME(ep.major_id, ep.minor_id)
                        AND ep.class = 1 AND ep.name = 'MS_Description'
                    WHERE c.TABLE_SCHEMA NOT IN ({})
                    ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
                """.format(','.join(['?' for _ in excluded_schemas]))
                column_rows = await conn.fetch(column_query, *excluded_schemas)

            for row in column_rows:
                key = (row['table_schema'], row['table_name'])
                if key not in tables:
                    continue

                col_info = ColumnInfo(
                    name=row['column_name'],
                    data_type=row['data_type'],
                    is_nullable=(row['is_nullable'] == 'YES'),
                    ordinal_position=row['ordinal_position'],
                    column_comment=row['column_comment']
                )
                tables[key].columns.append(col_info)

            # Query 3: Get primary keys
            pk_query = """
                SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema NOT IN $1
            """

            pk_rows = await conn.fetch(pk_query, excluded_schemas)

            for row in pk_rows:
                key = (row['table_schema'], row['table_name'])
                if key not in tables:
                    continue

                for col in tables[key].columns:
                    if col.name == row['column_name']:
                        col.is_primary_key = True

            # Query 4: Get foreign keys
            fk_query = """
                SELECT
                    kcu.table_schema,
                    kcu.table_name as source_table,
                    kcu.column_name as source_column,
                    ccu.table_schema as target_schema,
                    ccu.table_name as target_table,
                    ccu.column_name as target_column
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage kcu
                  ON rc.constraint_name = kcu.constraint_name
                  AND rc.constraint_schema = kcu.constraint_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON rc.unique_constraint_name = ccu.constraint_name
                WHERE kcu.table_schema NOT IN $1
            """

            fk_rows = await conn.fetch(fk_query, excluded_schemas)

            for row in fk_rows:
                key = (row['table_schema'], row['source_table'])
                if key not in tables:
                    continue

                for col in tables[key].columns:
                    if col.name == row['source_column']:
                        col.is_foreign_key = True
                        col.fk_target_table = row['target_table']
                        col.fk_target_column = row['target_column']

            # Query 5: Get row count estimates
            rowcount_query = """
                SELECT schemaname, relname, n_live_tup
                FROM pg_stat_user_tables
                WHERE schemaname NOT IN $1
            """

            rowcount_rows = await conn.fetch(rowcount_query, excluded_schemas)

            for row in rowcount_rows:
                key = (row['schemaname'], row['relname'])
                if key in tables:
                    tables[key].row_count_estimate = row['n_live_tup'] or 0

            result = list(tables.values())
            logger.info(f"Crawled {len(result)} tables from service '{service_name}'")
            return result

        finally:
            await self.conn_mgr.release_connection(service_name, conn)

    def _is_table_excluded(self, table_name: str) -> bool:
        """Check if table matches any excluded patterns.

        Args:
            table_name: Name of the table

        Returns:
            True if table should be excluded
        """
        patterns = self.settings.crawler.excluded_table_patterns
        for pattern in patterns:
            if pattern in table_name:
                return True
        return False

    async def detect_join_keys(self, all_tables: Dict[str, List[TableInfo]]) -> List[JoinKeyInfo]:
        """Find columns with same name and type across multiple services.

        Args:
            all_tables: Dict of service_name -> list of TableInfo

        Returns:
            List of JoinKeyInfo for cross-service join keys
        """
        logger.info("Detecting cross-service join keys")

        # Build index: (column_name, data_type) -> list of services
        column_index: Dict[tuple, set] = {}

        for service_name, tables in all_tables.items():
            for table in tables:
                for col in table.columns:
                    key = (col.name.lower(), col.data_type.lower())
                    if key not in column_index:
                        column_index[key] = set()
                    column_index[key].add(service_name)

        # Filter to keys found in 2+ services
        join_keys = []
        for (col_name, data_type), services in column_index.items():
            if len(services) >= 2:
                join_keys.append(JoinKeyInfo(
                    column_name=col_name,
                    data_type=data_type,
                    services=sorted(list(services))
                ))

        logger.info(f"Found {len(join_keys)} cross-service join keys")
        return join_keys

    async def save_to_metadata_db(
        self,
        service_name: str,
        tables: List[TableInfo],
        session: Session
    ) -> None:
        """Save crawled data to metadata store.

        Uses upsert pattern: updates existing records, adds new ones, removes deleted tables.

        Args:
            service_name: Name of the service
            tables: List of TableInfo to save
            session: SQLAlchemy session
        """
        logger.info(f"Saving {len(tables)} tables for service '{service_name}' to metadata DB")

        # Get or create ServiceMeta
        service = session.query(ServiceMeta).filter_by(service_name=service_name).first()
        if not service:
            service_config = self.settings.services[service_name]
            service = ServiceMeta(
                service_name=service_name,
                connection_key=service_config.connection_key,
                display_name=service_config.display_name
            )
            session.add(service)
            session.flush()

        # Update service metadata
        service.last_crawled_at = datetime.now()
        service.table_count = len(tables)

        # Build set of current table keys
        current_tables = {(t.schema_name, t.table_name) for t in tables}

        # Get existing tables
        existing_tables = session.query(TableMeta).filter_by(service_id=service.id).all()
        existing_by_key = {
            (t.schema_name, t.table_name): t
            for t in existing_tables
        }

        # Remove deleted tables
        for key, existing_table in existing_by_key.items():
            if key not in current_tables:
                logger.debug(f"Removing deleted table: {key}")
                session.delete(existing_table)

        # Upsert tables and columns
        for table_info in tables:
            key = (table_info.schema_name, table_info.table_name)

            if key in existing_by_key:
                # Update existing table
                table_meta = existing_by_key[key]
                table_meta.description = table_info.table_comment
                table_meta.row_count_estimate = table_info.row_count_estimate
                table_meta.updated_at = datetime.now()
            else:
                # Create new table
                table_meta = TableMeta(
                    service_id=service.id,
                    schema_name=table_info.schema_name,
                    table_name=table_info.table_name,
                    description=table_info.table_comment,
                    row_count_estimate=table_info.row_count_estimate
                )
                session.add(table_meta)
                session.flush()

            # Delete existing columns and recreate (simpler than diffing)
            session.query(ColumnMeta).filter_by(table_id=table_meta.id).delete()

            # Add columns
            for col_info in table_info.columns:
                col_meta = ColumnMeta(
                    table_id=table_meta.id,
                    name=col_info.name,
                    data_type=col_info.data_type,
                    is_nullable=col_info.is_nullable,
                    is_primary_key=col_info.is_primary_key,
                    is_foreign_key=col_info.is_foreign_key,
                    fk_target_table=col_info.fk_target_table,
                    fk_target_column=col_info.fk_target_column,
                    ordinal_position=col_info.ordinal_position,
                    column_comment=col_info.column_comment
                )
                session.add(col_meta)

        session.commit()
        logger.info(f"Saved metadata for service '{service_name}'")
