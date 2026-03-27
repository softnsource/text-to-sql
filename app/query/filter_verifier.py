import logging
import sqlglot
from sqlglot import exp, parse_one
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)
DIALECT_MAP = {
    "sqlite":       "sqlite",
    "postgresql":   "postgres",
    "mysql":        "mysql",
    "sqlserver":    "tsql",
}
class SQLFilterVerifier:
    """Verifies generated SQL against mandatory filter keys and injects session values."""
    
    def __init__(self, dialect: str = "sqlite"):
        self.dialect = dialect

    
    def verify_and_inject(
        self,
        sql: str,
        filter_keys: List[str],
        filter_values: Dict[str, Any],
        table_schemas: Dict[str, List[str]],
        column_mappings: Optional[Dict[str, Dict[str, str]]] = None
    ) -> str:
        if not filter_keys:
            return sql

        def build_condition(alias: str, col: str, val) -> str:
            """Always generates IN (...) — handles scalar, list, or comma-separated string."""
            if isinstance(val, (list, tuple)):
                values = list(val)
            elif isinstance(val, str):
                parts = val.split(",")
                values = [p.strip().strip("'").strip('"') for p in parts]
            else:
                values = [val]

            formatted = []
            for v in values:
                if isinstance(v, str):
                    escaped = v.replace("'", "''")
                    formatted.append(f"'{escaped}'")
                else:
                    formatted.append(str(v))

            return f"{alias}.{col} IN ({', '.join(formatted)})"

        try:
            expression = parse_one(sql, read=DIALECT_MAP.get(self.dialect.lower(), "postgres"))
        except Exception as e:
            logger.error(f"SQL parsing failed: {e}")
            return sql

        tables_in_query = []
        for table in expression.find_all(exp.Table):
            t_name = table.name.lower()
            t_alias = table.alias if table.alias else t_name
            tables_in_query.append({"name": t_name, "alias": t_alias, "expression": table})

        logger.info(f"Tables in query: {[t['name'] for t in tables_in_query]}")
        norm_filter_values = {k.lower(): v for k, v in filter_values.items()}

        def normalize(col: str) -> str:
            return col.replace("_", "").lower()

        filters_to_inject = []

        for table_info in tables_in_query:
            t_name = table_info["name"]
            t_alias = table_info["alias"]

            raw_columns = table_schemas.get(t_name, [])
            normalized_columns = {normalize(c): c for c in raw_columns}
            table_mappings = (column_mappings or {}).get(t_name, {})
            norm_table_mappings = {k.lower(): v for k, v in table_mappings.items()}

            table_applied_keys = []
            for key in filter_keys:
                k_lower = key.lower()
                norm_key = normalize(key)

                actual_col = norm_table_mappings.get(k_lower) or normalized_columns.get(norm_key)

                if actual_col:
                    val = norm_filter_values.get(k_lower)
                    if val is not None and val != "" and val != []:
                        condition = build_condition(t_alias, actual_col, val)
                        filters_to_inject.append(condition)
                        table_applied_keys.append(key)
                    else:
                        logger.warning(f"Mandatory key '{key}' found in table '{t_name}' but NO VALUE in session!")

            if table_applied_keys:
                logger.info(f"Applied filters to table '{t_name}': {table_applied_keys}")
            else:
                logger.info(f"Table '{t_name}' has no matching mandatory filter keys.")

        if not filters_to_inject:
            logger.warning(f"Security Block: No mandatory filters applied. Tables: {[t['name'] for t in tables_in_query]}")
            raise PermissionError("You don't have permission of this message")

        for cond_str in filters_to_inject:
            try:
                cond_expr = parse_one(cond_str)
                expression = expression.where(cond_expr, copy=False)
            except Exception as e:
                logger.error(f"Failed to inject filter '{cond_str}': {e}")

        return expression.sql(dialect=DIALECT_MAP.get(self.dialect.lower(), "postgres"))