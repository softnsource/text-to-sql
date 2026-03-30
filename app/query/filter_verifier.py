# import logging
# import sqlglot
# from sqlglot import exp, parse_one
# from typing import List, Dict, Any, Optional

# logger = logging.getLogger(__name__)
# DIALECT_MAP = {
#     "sqlite":       "sqlite",
#     "postgresql":   "postgres",
#     "mysql":        "mysql",
#     "sqlserver":    "tsql",
# }
# class SQLFilterVerifier:
#     """Verifies generated SQL against mandatory filter keys and injects session values."""
    
#     def __init__(self, dialect: str = "sqlite"):
#         self.dialect = dialect

    
#     def verify_and_inject(
#         self,
#         sql: str,
#         filter_keys: List[str],
#         filter_values: Dict[str, Any],
#         table_schemas: Dict[str, List[str]],
#         column_mappings: Optional[Dict[str, Dict[str, str]]] = None
#     ) -> str:
#         if not filter_keys:
#             return sql

#         def build_condition(alias: str, col: str, val) -> str:
#             """Always generates IN (...) — handles scalar, list, or comma-separated string."""
#             if isinstance(val, (list, tuple)):
#                 values = list(val)
#             elif isinstance(val, str):
#                 parts = val.split(",")
#                 values = [p.strip().strip("'").strip('"') for p in parts]
#             else:
#                 values = [val]

#             formatted = []
#             for v in values:
#                 if isinstance(v, str):
#                     escaped = v.replace("'", "''")
#                     formatted.append(f"'{escaped}'")
#                 else:
#                     formatted.append(str(v))

#             return f"{alias}.{col} IN ({', '.join(formatted)})"

#         try:
#             expression = parse_one(sql, read=DIALECT_MAP.get(self.dialect.lower(), "postgres"))
#         except Exception as e:
#             logger.error(f"SQL parsing failed: {e}")
#             return sql

#         tables_in_query = []
#         for table in expression.find_all(exp.Table):
#             t_name = table.name.lower()
#             t_alias = table.alias if table.alias else t_name
#             tables_in_query.append({"name": t_name, "alias": t_alias, "expression": table})

#         logger.info(f"Tables in query: {[t['name'] for t in tables_in_query]}")
#         norm_filter_values = {k.lower(): v for k, v in filter_values.items()}

#         def normalize(col: str) -> str:
#             return col.replace("_", "").lower()

#         filters_to_inject = []

#         for table_info in tables_in_query:
#             t_name = table_info["name"]
#             t_alias = table_info["alias"]

#             raw_columns = table_schemas.get(t_name, [])
#             normalized_columns = {normalize(c): c for c in raw_columns}
#             table_mappings = (column_mappings or {}).get(t_name, {})
#             norm_table_mappings = {k.lower(): v for k, v in table_mappings.items()}

#             table_applied_keys = []
#             for key in filter_keys:
#                 k_lower = key.lower()
#                 norm_key = normalize(key)

#                 actual_col = norm_table_mappings.get(k_lower) or normalized_columns.get(norm_key)

#                 if actual_col:
#                     val = norm_filter_values.get(k_lower)
#                     if val is not None and val != "" and val != []:
#                         condition = build_condition(t_alias, actual_col, val)
#                         filters_to_inject.append(condition)
#                         table_applied_keys.append(key)
#                     else:
#                         logger.warning(f"Mandatory key '{key}' found in table '{t_name}' but NO VALUE in session!")

#             if table_applied_keys:
#                 logger.info(f"Applied filters to table '{t_name}': {table_applied_keys}")
#             else:
#                 logger.info(f"Table '{t_name}' has no matching mandatory filter keys.")

#         if not filters_to_inject:
#             logger.warning(f"Security Block: No mandatory filters applied. Tables: {[t['name'] for t in tables_in_query]}")
#             raise PermissionError("You don't have permission of this message")

#         for cond_str in filters_to_inject:
#             try:
#                 cond_expr = parse_one(cond_str)
#                 expression = expression.where(cond_expr, copy=False)
#             except Exception as e:
#                 logger.error(f"Failed to inject filter '{cond_str}': {e}")

#         return expression.sql(dialect=DIALECT_MAP.get(self.dialect.lower(), "postgres"))

import logging
import sqlglot
from sqlglot import exp, parse_one
from typing import List, Dict, Any, Optional
from collections import deque

logger = logging.getLogger(__name__)

DIALECT_MAP = {
    "sqlite":     "sqlite",
    "postgresql": "postgres",
    "mysql":      "mysql",
    "sqlserver":  "tsql",
}


class SQLFilterVerifier:
    """Verifies generated SQL against mandatory filter keys and injects session values.
    
    Filter resolution order for each table in the query:
      1. Primary table has the filter column directly → inject WHERE
      2. A table already JOINed in the SQL has it → inject WHERE on that alias
      3. BFS up through reverse_foreign_keys until a parent with the column is found
         → inject the missing JOIN(s) + WHERE
      4. None found → PermissionError
    """

    def __init__(self, dialect: str = "sqlite"):
        self.dialect = dialect

    # ──────────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ──────────────────────────────────────────────────────────────────────
    def verify_and_inject(
        self,
        sql: str,
        filter_keys: List[str],
        filter_values: Dict[str, Any],
        table_schemas: Dict[str, Any],          # table_name → full payload dict OR list of col names
        column_mappings: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> str:
        if not filter_keys:
            return sql

        try:
            expression = parse_one(sql, read=DIALECT_MAP.get(self.dialect.lower(), "postgres"))
        except Exception as e:
            logger.error(f"SQL parsing failed: {e}")
            return sql

        # Collect every table currently in the query
        tables_in_query = []
        for table in expression.find_all(exp.Table):
            t_name  = table.name.lower()
            t_alias = table.alias if table.alias else t_name
            tables_in_query.append({"name": t_name, "alias": t_alias, "expression": table})

        logger.info(f"Tables in query: {[t['name'] for t in tables_in_query]}")

        norm_filter_values  = {k.lower(): v for k, v in filter_values.items()}
        column_mappings     = column_mappings or {}
        filters_to_inject   = []   # plain SQL condition strings
        joins_to_inject     = []   # (join_table, join_alias, on_clause) tuples

        # Determine the primary (FROM) table — first table in the query
        primary_table_name = tables_in_query[0]["name"] if tables_in_query else ""

        for key in filter_keys:
            k_lower = key.lower()
            val = norm_filter_values.get(k_lower)

            # ── PASS 1: check every table already present in the SQL ──────
            matched = self._check_tables_in_query(
                key, val, tables_in_query, table_schemas, column_mappings
            )
            if matched:
                filters_to_inject.append(matched)
                continue

            # ── PASS 2: BFS through reverse_foreign_keys ──────────────────
            logger.info(
                f"Filter key '{key}' not found in any in-query table. "
                f"Starting BFS from primary table '{primary_table_name}'."
            )
            primary_table_alias = tables_in_query[0]["alias"] if tables_in_query else primary_table_name
            bfs_result = self._bfs_find_filter(
                start_table=primary_table_name,
                start_alias=primary_table_alias,
                filter_key=key,
                filter_val=val,
                table_schemas=table_schemas,
                column_mappings=column_mappings,
                existing_table_names={t["name"] for t in tables_in_query},
            )

            logger.info(f"Bfs Result : {bfs_result}")
            if bfs_result:
                condition, new_joins = bfs_result
                filters_to_inject.append(condition)
                joins_to_inject.extend(new_joins)
                logger.info(
                    f"BFS resolved '{key}' via ancestor joins: "
                    f"{[j[0] for j in new_joins]}"
                )
            else:
                logger.warning(
                    f"Security Block: mandatory filter '{key}' could not be resolved "
                    f"for any table in the query or via BFS. Tables: "
                    f"{[t['name'] for t in tables_in_query]}"
                )
                raise PermissionError(
                    f"Access denied: could not enforce the mandatory '{key}' filter "
                    f"for this query. Please contact your administrator."
                )

        # ── Inject discovered JOINs into the SQL ─────────────────────────
        if joins_to_inject:
            expression = self._inject_joins(expression, joins_to_inject)

        # ── Inject WHERE conditions ───────────────────────────────────────
        for cond_str in filters_to_inject:
            try:
                cond_expr = parse_one(cond_str)
                expression = expression.where(cond_expr, copy=False)
            except Exception as e:
                logger.error(f"Failed to inject filter '{cond_str}': {e}")

        result_sql = expression.sql(dialect=DIALECT_MAP.get(self.dialect.lower(), "postgres"))
        logger.info(f"Final filtered SQL: {result_sql}")
        return result_sql

    # ──────────────────────────────────────────────────────────────────────
    # PASS 1 — check tables already present in the query
    # ──────────────────────────────────────────────────────────────────────
    def _check_tables_in_query(
        self,
        key: str,
        val: Any,
        tables_in_query: List[Dict],
        table_schemas: Dict[str, Any],
        column_mappings: Dict[str, Dict[str, str]],
    ) -> Optional[str]:
        """Return a WHERE condition string if `key` exists in any in-query table, else None."""
        for table_info in tables_in_query:
            t_name  = table_info["name"]
            t_alias = table_info["alias"]

            actual_col = self._resolve_column(t_name, key, table_schemas, column_mappings)
            if actual_col:
                if val is not None and val != "" and val != []:
                    condition = self._build_condition(t_alias, actual_col, val)
                    logger.info(f"Filter '{key}' resolved directly on table '{t_name}' as '{actual_col}'")
                    return condition
                else:
                    logger.warning(
                        f"Mandatory key '{key}' found in table '{t_name}' "
                        f"but NO VALUE provided in session!"
                    )
        return None

    # ──────────────────────────────────────────────────────────────────────
    # PASS 2 — BFS through reverse_foreign_keys
    # ──────────────────────────────────────────────────────────────────────
    def _bfs_find_filter(
        self,
        start_table: str,
        start_alias: str,
        filter_key: str,
        filter_val: Any,
        table_schemas: Dict[str, Any],
        column_mappings: Dict[str, Dict[str, str]],
        existing_table_names: set,
        max_depth: int = 3,
    ) -> Optional[tuple]:
        """
        BFS upward through reverse_foreign_keys until a table with `filter_key` is found.

        Returns:
            (condition_str, [(join_table, join_alias, on_clause), ...])
            or None if unreachable within max_depth.
        """
        visited = set()
        # Each queue item: (current_table_name, current_alias, path_of_join_steps)
        # path_of_join_steps: list of (join_table, join_alias, on_clause_str)
        queue = deque()
        queue.append((start_table.lower(), start_alias, []))

        alias_counter = [0]   # mutable counter shared across BFS iterations

        while queue:
            current_table, current_alias, path = queue.popleft()

            if current_table in visited or len(path) > max_depth:
                continue
            visited.add(current_table)

            schema = self._get_schema_payload(current_table, table_schemas)
            if schema is None:
                logger.debug(f"BFS: no schema found for '{current_table}', skipping.")
                continue

            # Check if this table has the filter column
            actual_col = self._resolve_column(
                current_table, filter_key, table_schemas, column_mappings
            )
            if actual_col and path:
                # Found it — and we needed at least one JOIN to get here
                if filter_val is not None and filter_val != "" and filter_val != []:
                    condition = self._build_condition(current_alias, actual_col, filter_val)
                    return condition, path
                else:
                    logger.warning(
                        f"BFS found '{filter_key}' on '{current_table}' "
                        f"but no value provided."
                    )
                    return None

            # Walk reverse FKs (tables that point AT this table)
            rfks = schema.get("reverse_foreign_keys", []) if isinstance(schema, dict) else []
            logger.info(f"BFS at '{current_table}': checking {len(rfks)} reverse FKs for '{filter_key}'")
            for rfk in rfks:
                ref_table = rfk.get("referencing_table", "").lower()
                ref_col   = rfk.get("referencing_col", "")   # col on ref_table
                local_col = rfk.get("local_col", "")         # col on current_table

                if ref_table in visited or ref_table in existing_table_names:
                    continue

                alias_counter[0] += 1
                new_alias = f"t_anc{alias_counter[0]}"

                # JOIN ref_table new_alias ON current_alias.local_col = new_alias.ref_col
                on_clause = f"{current_alias}.{local_col} = {new_alias}.{ref_col}"
                join_step = (ref_table, new_alias, on_clause)

                queue.append((ref_table, new_alias, path + [join_step]))

        logger.info(f"BFS: could not find '{filter_key}' within depth {max_depth} from '{start_table}'.")
        return None

    # ──────────────────────────────────────────────────────────────────────
    # INJECT JOINs INTO THE PARSED SQL EXPRESSION
    # ──────────────────────────────────────────────────────────────────────
    def _inject_joins(self, expression, joins: List[tuple]):
        """
        Append LEFT JOINs to the SQL expression for each (table, alias, on_clause) tuple.
        """
        for join_table, join_alias, on_clause in joins:
            try:
                join_node = exp.Join(
                    this=exp.Table(
                        this=exp.Identifier(this=join_table, quoted=False),
                        alias=exp.TableAlias(this=exp.Identifier(this=join_alias, quoted=False)),
                    ),
                    on=parse_one(on_clause),
                    kind="LEFT",
                )
                expression = expression.copy()
                expression.append("joins", join_node)
                logger.info(f"Injected JOIN: {join_table} {join_alias} ON {on_clause}")
            except Exception as e:
                logger.error(f"Failed to inject JOIN for '{join_table}': {e}", exc_info=True)
        return expression

    # ──────────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────────
    def _get_schema_payload(self, table_name: str, table_schemas: Dict[str, Any]) -> Optional[Any]:
        """
        table_schemas can be:
          - Dict[str, dict]  — full Qdrant payload per table (preferred)
          - Dict[str, list]  — just a list of column name strings (legacy)
        Returns the raw value for the table, or None.
        """
        # case-insensitive lookup
        for k, v in table_schemas.items():
            if k.lower() == table_name.lower():
                return v
        return None

    def _resolve_column(
        self,
        table_name: str,
        filter_key: str,
        table_schemas: Dict[str, Any],
        column_mappings: Dict[str, Dict[str, str]],
    ) -> Optional[str]:
        """
        Returns the actual column name on `table_name` that matches `filter_key`,
        considering both column_mappings overrides and normalize-match on schema columns.
        Returns None if no match.
        """
        def normalize(s: str) -> str:
            return s.replace("_", "").lower()

        k_lower    = filter_key.lower()
        norm_key   = normalize(filter_key)

        # 1. Explicit column_mappings override
        table_map  = column_mappings.get(table_name, {})
        norm_map   = {k.lower(): v for k, v in table_map.items()}
        if k_lower in norm_map:
            return norm_map[k_lower]

        # 2. Fuzzy-normalize match against schema columns
        schema = self._get_schema_payload(table_name, table_schemas)
        if schema is None:
            return None

        # Support both payload dict (columns is a list of {name, type}) and plain list of strings
        if isinstance(schema, dict):
            col_list = [c["name"] if isinstance(c, dict) else c for c in schema.get("columns", [])]
        elif isinstance(schema, list):
            col_list = schema
        else:
            return None

        normalized_columns = {normalize(c): c for c in col_list}
        return normalized_columns.get(norm_key)

    def _build_condition(self, alias: str, col: str, val: Any) -> str:
        """Always generates IN (...) — handles scalar, list, or comma-separated string."""
        if isinstance(val, (list, tuple)):
            values = list(val)
        elif isinstance(val, str):
            parts  = val.split(",")
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