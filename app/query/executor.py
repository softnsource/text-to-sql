"""Query executor — runs validated SQL against the session's SQLAlchemy engine."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from app.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: float
    error: Optional[str] = None
    service_name: str = ""


class QueryExecutor:
    """Executes SQL queries using a session-scoped SQLAlchemy engine."""

    def __init__(self):
        self.settings = get_settings()

    async def execute(self, engine: Engine, sql: str) -> QueryResult:
        """Execute SQL and return structured result.

        Args:
            engine: SQLAlchemy engine for the session's database
            sql: Validated SQL string

        Returns:
            QueryResult with rows, columns, and timing
        """
        return await asyncio.to_thread(self._execute_sync, engine, sql)

    def _validate_tables_columns(self, engine: Engine, sql: str) -> Optional[str]:
        """Pre-validate tables and columns exist before full execution."""
        from sqlalchemy import inspect
        inspector = inspect(engine)
        try:
            # Simple heuristic: extract FROM/JOIN tables, SELECT columns
            sql_lower = sql.lower()
            if 'from' not in sql_lower:
                return "No FROM clause found"

# Extract qualified table names and unqualified tables
            import re
            
            # Strip quotes/brackets for simple parsing
            clean_sql = re.sub(r'[\[\]"`()]', '', sql_lower)
            
            # Find schema.table patterns
            qualified = re.findall(r'\b(\w+)\.(\w+)\b', clean_sql)
            table_pairs = [(schema, table) for schema, table in qualified]
            
            # Simple unqualified tables after FROM/JOIN
            unqual = re.findall(r'\b(?:from|join)\s+(\w+)', clean_sql)
            
            all_validations = table_pairs + [(None, t) for t in set(unqual)]
            
            validated_tables = []
            for schema, tname in set(all_validations):
                try:
                    if not inspector.has_table(tname, schema):
                        continue  # Not a real table (e.g. alias.column)
                    cols = inspector.get_columns(tname, schema)
                    if not cols:
                        return f"Table not found: {schema + '.' + tname if schema else tname}"
                    validated_tables.append(schema + '.' + tname if schema else tname)
                except Exception as ex:
                    return f"Cannot access table {schema + '.' + tname if schema else tname}: {str(ex)}"
            
            logger.info(f"Pre-validation passed for real tables: {validated_tables}")

            return None
        except Exception as e:
            return f"Validation failed: {str(e)}"

    def _execute_sync(self, engine: Engine, sql: str) -> QueryResult:
        start = time.perf_counter()
        timeout = self.settings.query.query_timeout_seconds

        # Pre-validate schema objects
        validation_error = self._validate_tables_columns(engine, sql)
        if validation_error:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.warning(f"Schema validation failed: {validation_error}")
            return QueryResult(
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=elapsed_ms,
                error=f"SCHEMA ERROR: {validation_error}",
            )

        try:
            with engine.connect() as conn:
                conn = conn.execution_options(timeout=timeout)
                df = pd.read_sql(text(sql), conn)

            elapsed_ms = (time.perf_counter() - start) * 1000

            columns = list(df.columns)
            rows: List[dict[str, Any]] = cast(List[dict[str, Any]], df.to_dict(orient="records"))

            # Convert non-serializable types (dates, decimals, etc.)
            rows = [self._serialize_row(row) for row in rows]

            logger.info(
                f"Query executed: {len(rows)} rows in {elapsed_ms:.1f}ms"
            )

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time_ms=elapsed_ms,
            )

        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(f"Query execution failed after {elapsed_ms:.1f}ms: {e}", exc_info=True)
            return QueryResult(
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=elapsed_ms,
                error=str(e),
            )

    def _serialize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Convert non-JSON-serializable values to strings."""
        result = {}
        for k, v in row.items():
            if v is None or isinstance(v, (int, float, bool, str)):
                result[k] = v
            else:
                result[k] = str(v)
        return result
