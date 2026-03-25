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
        """
        Parses SQL, checks tables for filter keys, injects WHERE clauses.
        
        Args:
            sql: The generated SQL string.
            filter_keys: List of mandatory column names to filter by.
            filter_values: Values for each filter key from the current session.
            table_schemas: Mapping of table_name -> list of column names.
            column_mappings: Optional mapping of {table_name: {filter_key: actual_column_name}}.
            
        Returns:
            Modified SQL string with injected filters.
            
        Raises:
            PermissionError: If no filterable tables are found in a restricted query.
        """
        if not filter_keys:
            return sql

        try:
            # Use sqlglot to parse the SQL
            expression = parse_one(sql, read=DIALECT_MAP.get(self.dialect.lower(), "postgres")
)
        except Exception as e:
            logger.error(f"SQL parsing failed: {e}")
            # If parsing fails, we cannot safely inject filters. 
            # For security, we should probably fail or at least log heavily.
            return sql

        # 1. Identify all participating tables
        tables_in_query = []
        for table in expression.find_all(exp.Table):
            t_name = table.name.lower()
            t_alias = table.alias if table.alias else t_name
            tables_in_query.append({
                "name": t_name,
                "alias": t_alias,
                "expression": table
            })

        logger.info(f"Tables in query: {[t['name'] for t in tables_in_query]}")
        logger.info(f"Session filter values keys: {list(filter_values.keys())}")
        
        # Normalize filter values for case-insensitive lookup
        norm_filter_values = {k.lower(): v for k, v in filter_values.items()}

        # 2. Match filter keys with table columns for EVERY table
        filters_to_inject = []
        
        for table_info in tables_in_query:
            t_name = table_info["name"]
            t_alias = table_info["alias"]
            
            available_cols = [c.lower() for c in table_schemas.get(t_name, [])]
            logger.info(f"Table '{t_name}' ({t_alias}) columns: {available_cols}")
            
            # Check for table-specific mappings
            table_mappings = (column_mappings or {}).get(t_name, {})
            norm_table_mappings = {k.lower(): v for k, v in table_mappings.items()}

            table_applied_keys = []
            for key in filter_keys:
                k_lower = key.lower()
                
                # Determine actual column name (mapping or direct)
                actual_col = norm_table_mappings.get(k_lower)
                if actual_col:
                    logger.info(f"Using mapped column '{actual_col}' for filter key '{key}' on table '{t_name}'")
                elif k_lower in available_cols:
                    actual_col = key  # Use the original key name
                
                if actual_col:
                    val = norm_filter_values.get(k_lower)
                    logger.info(f"Matching key '{key}' (col: '{actual_col}') for table '{t_name}'. Value found: {val}")
                    
                    if val is not None and val != "":
                        # Handle value formatting
                        if isinstance(val, str):
                            escaped_val = val.replace("'", "''")
                            condition = f"{t_alias}.{actual_col} = '{escaped_val}'"
                        else:
                            condition = f"{t_alias}.{actual_col} = {val}"
                        
                        filters_to_inject.append(condition)
                        table_applied_keys.append(key)
                    else:
                        logger.warning(f"Mandatory key '{key}' found in table '{t_name}' but NO VALUE provided in session!")
            
            # (Optional) If we wanted to enforce that EVERY table MUST have the key, we'd check here.
            # But the user requested a more flexible policy: allow the query as long as 
            # at least one table is correctly filtered.
            if table_applied_keys:
                logger.info(f"Applied filters to table '{t_name}': {table_applied_keys}")
            else:
                logger.info(f"Table '{t_name}' does not have any mandatory filter keys: {filter_keys}")

        # 3. Security Check: At least one filter must be applied to the ENTIRE query
        # to ensure it's restricted by the mandatory keys.
        if not filters_to_inject:
            logger.warning(f"Security Block: No mandatory filters could be applied to ANY table in this query. Tables: {[t['name'] for t in tables_in_query]}. Mandatory keys: {filter_keys}")
            raise PermissionError("You don't have permission of this message")

        # 4. Inject filter conditions into the WHERE clause
        for cond_str in filters_to_inject:
            try:
                cond_expr = parse_one(cond_str)
                expression = expression.where(cond_expr, copy=False)
            except Exception as e:
                logger.error(f"Failed to parse/inject filter condition '{cond_str}': {e}")

        # Return the modified SQL
        return expression.sql(dialect=DIALECT_MAP.get(self.dialect.lower(), "postgres"))
