# """SQL validator — AST-level security check using sqlglot. Blocks all non-SELECT statements."""

# import logging
# import re
# from dataclasses import dataclass
# from typing import List, Literal

# import sqlglot
# from sqlglot import exp

# from app.config import get_settings

# logger = logging.getLogger(__name__)

# Dialect = Literal["sqlite", "postgresql", "mysql", "sqlserver", "tsql"]

# DIALECT_MAP = {
#     "sqlite":       "sqlite",
#     "postgresql":   "postgres",
#     "mysql":        "mysql",
#     "sqlserver":    "tsql",
# }


# @dataclass
# class ValidationResult:
#     is_valid: bool
#     errors: List[str]
#     sql: str                # Possibly modified (LIMIT injected)


# class SQLValidator:
#     """Validates and sanitises generated SQL using sqlglot AST parsing."""

#     BLOCKED_KEYWORDS = {
#         "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
#         "TRUNCATE", "EXEC", "EXECUTE", "GRANT", "REVOKE", "MERGE",
#         "CALL", "XP_CMDSHELL", "OPENROWSET", "BULK",
#     }

#     def __init__(self):
#         self.settings = get_settings()

#     def validate(self, sql: str, dialect: str) -> ValidationResult:
#         """Validate SQL and inject LIMIT if missing.

#         Args:
#             sql: SQL query string
#             dialect: Database dialect (sqlite, postgresql, mysql, sqlserver)

#         Returns:
#             ValidationResult with is_valid flag and any error messages
#         """
#         errors: List[str] = []
#         sqlglot_dialect = DIALECT_MAP.get(dialect.lower(), "postgres")

#         # Rule 1: No semicolons (prevents statement chaining)
#         if ";" in sql:
#             errors.append("Semicolons are not allowed in queries.")

#         # Rule 2: Block dangerous keywords via regex (fast pre-check)
#         sql_upper = sql.upper()
#         for keyword in self.BLOCKED_KEYWORDS:
#             if re.search(rf'\b{keyword}\b', sql_upper):
#                 errors.append(f"Blocked keyword detected: {keyword}")

#         if errors:
#             return ValidationResult(is_valid=False, errors=errors, sql=sql)

#         # Rule 3: Parse with sqlglot AST
#         try:
#             parsed = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
#         except Exception as e:
#             return ValidationResult(
#                 is_valid=False,
#                 errors=[f"SQL parse error: {e}"],
#                 sql=sql,
#             )

#         # Rule 4: Root must be SELECT
#         if not isinstance(parsed, exp.Select):
#             errors.append("Only SELECT statements are allowed.")

#         # Rule 5: Check subqueries for blocked keywords
#         for subq in parsed.find_all(exp.Subquery):
#             subq_sql = subq.sql(dialect=sqlglot_dialect).upper()
#             for keyword in self.BLOCKED_KEYWORDS:
#                 if re.search(rf'\b{keyword}\b', subq_sql):
#                     errors.append(f"Subquery contains blocked keyword: {keyword}")
#                     break

#         if errors:
#             return ValidationResult(is_valid=False, errors=errors, sql=sql)

#         # Rule 6: Inject LIMIT if missing; cap if too large
#         max_rows = self.settings.query.max_rows_per_query
#         sql = self._ensure_limit(parsed, sql, sqlglot_dialect, max_rows)

#         return ValidationResult(is_valid=True, errors=[], sql=sql)

#     def _ensure_limit(
#         self, parsed: exp.Expression, original_sql: str,
#         dialect: str, max_rows: int
#     ) -> str:
#         """Add or cap row-limiting clause — LIMIT for standard SQL, TOP for SQL Server."""
#         is_tsql = (dialect == "tsql")

#         if is_tsql:
#             return self._ensure_top_tsql(parsed, original_sql, max_rows)
#         else:
#             return self._ensure_limit_standard(parsed, original_sql, dialect, max_rows)

#     def _ensure_limit_standard(
#         self, parsed: exp.Expression, original_sql: str,
#         dialect: str, max_rows: int
#     ) -> str:
#         """Handle LIMIT clause for sqlite / postgresql / mysql."""
#         limit_node = next(parsed.find_all(exp.Limit), None)

#         if limit_node is None:
#             return f"{original_sql.rstrip()} LIMIT {max_rows}"

#         try:
#             limit_expr = limit_node.expression
#             if isinstance(limit_expr, exp.Literal):
#                 current = int(limit_expr.this)
#                 if current > max_rows:
#                     limit_expr.set("this", str(max_rows))
#                     return parsed.sql(dialect=dialect)
#         except Exception:
#             pass

#         return original_sql

#     def _ensure_top_tsql(
#         self, parsed: exp.Expression, original_sql: str, max_rows: int
#     ) -> str:
#         """Handle row limiting for SQL Server using sqlglot.

#         sqlglot internally uses LIMIT and converts it to TOP for T-SQL.
#         """

#         limit_node = next(parsed.find_all(exp.Limit), None)

#         if limit_node is None:
#             # Inject LIMIT (sqlglot will convert to TOP when using tsql dialect)
#             parsed.set("limit", exp.Limit(this=exp.Literal.number(max_rows)))
#             try:
#                 return parsed.sql(dialect="tsql")
#             except Exception:
#                 # fallback if AST generation fails
#                 sql_stripped = original_sql.strip()
#                 if sql_stripped.upper().startswith("SELECT"):
#                     return f"SELECT TOP {max_rows} {sql_stripped[6:]}"
#                 return original_sql

#         else:
#             try:
#                 limit_expr = limit_node.expression
#                 if isinstance(limit_expr, exp.Literal):
#                     current = int(limit_expr.this)
#                     if current > max_rows:
#                         limit_expr.set("this", str(max_rows))
#                         return parsed.sql(dialect="tsql")
#             except Exception:
#                 pass

#         return original_sql


"""SQL validator — AST-level security check using sqlglot. Blocks all non-SELECT statements."""

import logging
import re
from dataclasses import dataclass
from typing import List, Literal

import sqlglot
from sqlglot import exp

from app.config import get_settings

logger = logging.getLogger(__name__)

Dialect = Literal["sqlite", "postgresql", "mysql", "sqlserver", "tsql"]

DIALECT_MAP = {
    "sqlite":       "sqlite",
    "postgresql":   "postgres",
    "mysql":        "mysql",
    "sqlserver":    "tsql",
}


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    sql: str


class SQLValidator:
    """Validates and sanitises generated SQL using sqlglot AST parsing."""

    BLOCKED_KEYWORDS = {
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
        "TRUNCATE", "EXEC", "EXECUTE", "GRANT", "REVOKE", "MERGE",
        "XP_CMDSHELL", "OPENROWSET", "BULK",
    }

    def __init__(self):
        self.settings = get_settings()

    def validate(self, sql: str, dialect: str) -> ValidationResult:
        errors: List[str] = []
        sqlglot_dialect = DIALECT_MAP.get(dialect.lower(), "postgres")

        # Rule 1: No semicolons
        if ";" in sql:
            errors.append("Semicolons are not allowed in queries.")

        # Rule 2: Block dangerous keywords
        sql_upper = sql.upper()
        for keyword in self.BLOCKED_KEYWORDS:
            if re.search(rf'\b{keyword}\b', sql_upper):
                errors.append(f"Blocked keyword detected: {keyword}")

        if errors:
            return ValidationResult(is_valid=False, errors=errors, sql=sql)

        # Rule 3: Parse with sqlglot AST
        try:
            parsed = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                errors=[f"SQL parse error: {e}"],
                sql=sql,
            )

        # Rule 4: Root must be SELECT
        if not isinstance(parsed, (exp.Select, exp.Union)):
            errors.append("Only SELECT statements and UNION queries are allowed.")

        if isinstance(parsed, exp.Union):
            for node in [parsed.left, parsed.right]:
                if not isinstance(node, (exp.Select, exp.Union)):
                    errors.append("UNION branches must be SELECT statements only.")

        # Rule 5: Check subqueries for blocked keywords
        for subq in parsed.find_all(exp.Subquery):
            subq_sql = subq.sql(dialect=sqlglot_dialect).upper()
            for keyword in self.BLOCKED_KEYWORDS:
                if re.search(rf'\b{keyword}\b', subq_sql):
                    errors.append(f"Subquery contains blocked keyword: {keyword}")
                    break
        

        if errors:
            return ValidationResult(is_valid=False, errors=errors, sql=sql)

        # SQL is valid — return as-is
        # Limit is fully the generator's responsibility via prompt rules
        return ValidationResult(is_valid=True, errors=[], sql=sql)