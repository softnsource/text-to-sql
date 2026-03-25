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