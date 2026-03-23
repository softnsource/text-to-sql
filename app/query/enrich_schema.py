import json
from pathlib import Path
from typing import List
from app.query.planner import QueryPlan, TableContext
import logging

logger = logging.getLogger(__name__)

def build_enriched_schema(plan: QueryPlan, qdrant_collection: str) -> str:
    from app.training.indexer import Indexer
    indexer = Indexer()
    
    needed = set()
    for t in plan.relevant_tables:
        needed.add(t.table_name.lower())
        for fk in t.foreign_keys:
            needed.add(fk['to_table'].lower())
            
    # Fetch all table schemas directly from Qdrant instead of disk
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
    
    return '\n'.join(lines)

if __name__ == '__main__':
    print('Schema enricher ready')
