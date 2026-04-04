# import json
# from pathlib import Path
# from typing import List
# from app.query.planner import QueryPlan, TableContext
# import logging

# logger = logging.getLogger(__name__)

# def build_enriched_schema(plan: QueryPlan, qdrant_collection: str) -> str:
#     from app.training.indexer import Indexer
#     indexer = Indexer()
    
#     needed = set()
#     for t in plan.relevant_tables:
#         needed.add(t.table_name.lower())
#         for fk in t.foreign_keys:
#             needed.add(fk['to_table'].lower())
            
#     # Fetch all table schemas directly from Qdrant instead of disk
#     try:
#         scroll_results = indexer.client.scroll(
#             collection_name=qdrant_collection,
#             limit=500,
#             with_payload=True
#         )
#         report = [r.payload for r in scroll_results[0]]
#     except Exception as e:
#         logger.warning(f"Failed to fetch schemas from Qdrant for {qdrant_collection}: {e}")
#         report = []
        
#     filtered = [t for t in report if t.get('table_name', '').lower() in needed]
    
#     lines = ['ENRICHED SCHEMA (relevant + FKs):']
#     for t in filtered:
#         s = t.get('schema_name', 'dbo')
#         name = f"{s}.{t['table_name']}"
#         lines.append(f'\nTABLE {name}')
#         lines.append(f"ROWS: {t['row_count']:,}")
#         lines.append('COLUMNS:')
#         for c in t['columns']:
#             if isinstance(c, dict):
#                 nullable_str = "NULL" if c.get('nullable') else "NOT NULL"
#                 lines.append(f"  {c.get('name')} ({c.get('type')}) [{nullable_str}]")
#             else:
#                 lines.append(f"  {c}")
#         if 'foreign_keys' in t:
#             lines.append('FKs:')
#             for fk in t['foreign_keys']:
#                 lines.append(f'  {fk}')
    
#     return '\n'.join(lines)

# if __name__ == '__main__':
#     print('Schema enricher ready')


import json
from pathlib import Path
from typing import List, Optional, Tuple
from app.query.planner import QueryPlan, TableContext, USER_TYPE_OPTIONS, build_multi_table_clarification_text, _normalize_table_name
import logging

logger = logging.getLogger(__name__)

# Must match USER_TYPE_OPTIONS tables (normalized)
USER_TYPE_TABLE_NORMALIZED = {
    _normalize_table_name(opt["table"])
    for opt in USER_TYPE_OPTIONS
    if opt["table"]  # skip "Other" which has None
}

def get_user_tables_in_enriched(needed: set) -> List[str]:
    """Returns which user-type tables ended up in the enriched schema set."""
    return [
        t for t in needed
        if _normalize_table_name(t) in USER_TYPE_TABLE_NORMALIZED
    ]


import json
from pathlib import Path
from typing import List, Optional, Tuple
from app.query.planner import QueryPlan, TableContext, USER_TYPE_OPTIONS, build_multi_table_clarification_text, _normalize_table_name
import logging

logger = logging.getLogger(__name__)

# Normalized set of all user-type table names (excludes "Other" which has None)
USER_TYPE_TABLE_NORMALIZED = {
    _normalize_table_name(opt["table"])
    for opt in USER_TYPE_OPTIONS
    if opt["table"]
}


def get_user_tables_in_enriched(needed: set) -> List[str]:
    """Returns which user-type tables ended up in the enriched schema set."""
    return [
        t for t in needed
        if _normalize_table_name(t) in USER_TYPE_TABLE_NORMALIZED
    ]


async def build_enriched_schema(
    plan: QueryPlan,
    qdrant_collection: str,
    question: str = "",
    resolved_user_table: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (schema_str, clarification_msg).
    - If clarification needed: (None, "please clarify...")
    - If all good:             (schema_str, None)
    """
    from app.training.indexer import Indexer
    indexer = Indexer()
    def _safe(val):
        return None if (val is None or str(val).strip().lower() == 'none') else val

    effective_resolved_table = _safe(resolved_user_table) or _safe(plan.resolved_user_table)

    logger.info(f"[enricher] effective_resolved_table='{effective_resolved_table}' (raw plan='{plan.resolved_user_table}', raw param='{resolved_user_table}')")

    needed = set()
    for t in plan.relevant_tables:
        needed.add(t.table_name.lower())
        for fk in t.foreign_keys:
            needed.add(fk['to_table'].lower())

    logger.info(
        f"[enricher] plan.resolved_user_table='{plan.resolved_user_table}' "
        f"needed={needed}"
    )
    # ------------------------------------------------------------------
    # KEY FIX: If the user already resolved which user-type table to use,
    # strip every OTHER user-type table out of `needed` right now.
    # This prevents the clarification check below from firing again,
    # which was causing the infinite loop.
    # ------------------------------------------------------------------
    logger.info(f"[enricher] resolved_user_table check: '{effective_resolved_table}', needed before strip: {needed}")

    if effective_resolved_table and effective_resolved_table != "__skip__":
        resolved_normalized = _normalize_table_name(plan.resolved_user_table)
        tables_to_remove = {
            t for t in needed
            if _normalize_table_name(t) in USER_TYPE_TABLE_NORMALIZED
            and _normalize_table_name(t) != resolved_normalized
        }
        if tables_to_remove:
            logger.info(
                f"[enricher] Dropping competing user-type tables (resolved='{plan.resolved_user_table}'): {tables_to_remove}"
            )
            needed -= tables_to_remove

    # ------------------------------------------------------------------
    # Check: do multiple user-type tables remain? (only fires when
    # resolved_user_table is NOT set, because we stripped the extras above)
    # ------------------------------------------------------------------
    user_tables_found = get_user_tables_in_enriched(needed)
    logger.info(f"[enricher] User-type tables in enriched schema: {user_tables_found}")

    if len(user_tables_found) > 1:
        found_normalized = {_normalize_table_name(t) for t in user_tables_found}
        matched_options = [
            opt for opt in USER_TYPE_OPTIONS
            if opt["table"] and _normalize_table_name(opt["table"]) in found_normalized
        ]
        clarification_msg = await build_multi_table_clarification_text(
            question, matched_options
        )
        logger.info(f"[enricher] Clarification needed for: {matched_options}")
        return None, clarification_msg

    # ------------------------------------------------------------------
    # Fetch table schemas from Qdrant and build the schema string
    # ------------------------------------------------------------------
    try:
        scroll_results = indexer.client.scroll(
            collection_name=qdrant_collection,
            limit=500,
            with_payload=True
        )
        report = [r.payload for r in scroll_results[0]]
    except Exception as e:
        logger.warning(f"Failed to fetch schemas from Qdrant for {qdrant_collection}: {e}")
        report = []

    filtered = [t for t in report if t.get('table_name', '').lower() in needed]

    lines = ['ENRICHED SCHEMA (relevant + FKs):']
    for t in filtered:
        s = t.get('schema_name', 'dbo')
        name = f"{s}.{t['table_name']}"
        lines.append(f'\nTABLE {name}')
        lines.append(f"ROWS: {t['row_count']:,}")
        lines.append('COLUMNS:')
        for c in t['columns']:
            if isinstance(c, dict):
                nullable_str = "NULL" if c.get('nullable') else "NOT NULL"
                lines.append(f"  {c.get('name')} ({c.get('type')}) [{nullable_str}]")
            else:
                lines.append(f"  {c}")
        if 'foreign_keys' in t:
            lines.append('FKs:')
            for fk in t['foreign_keys']:
                lines.append(f'  {fk}')

    return '\n'.join(lines), None


if __name__ == '__main__':
    print('Schema enricher ready')
