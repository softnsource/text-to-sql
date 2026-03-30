"""Query planner — searches Qdrant for relevant tables, then uses Gemini to confirm intent."""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import google.generativeai as genai

from app.config import get_settings
from app.training.indexer import Indexer
from app.utils.gemini_key_manager import get_key_manager
from app.exceptions import QueryError

logger = logging.getLogger(__name__)


@dataclass
class TableContext:
    """Lightweight table reference returned from planner."""
    table_name: str
    schema_name: Optional[str]
    dialect: str
    description: str
    columns: List[Dict]       # list of {name, type, is_pk, is_fk, fk_target_table}
    foreign_keys: List[Dict]  # [{from, to_table, to_col}]
    row_count: int
    sample_rows: List[Dict] = field(default_factory=list)
    reverse_foreign_keys: List[Dict] = field(default_factory=list)  # ← NEW


@dataclass
class QueryPlan:
    question: str
    confidence: float
    needs_clarification: bool
    clarification_questions: List[str]
    relevant_tables: List[TableContext]
    dialect: str


class QueryPlanner:
    """Determines which tables are needed to answer a user question."""

    def __init__(self):
        self.settings = get_settings()
        self.indexer = Indexer()

    async def plan(
        self,
        collection_name: str,
        question: str,
        dialect: str,
        conversation_context: str = "",
    ) -> QueryPlan:
        """Create a query plan for the user's question.

        Args:
            session_id: Session identifier (maps to Qdrant collection)
            question: Natural language question from user
            dialect: SQL dialect for this session
            conversation_context: Formatted prior conversation turns

        Returns:
            QueryPlan with relevant tables and confidence score
        """
        top_k = self.settings.query.top_k_tables

        # Step 1: Semantic search in Qdrant
        try:
            raw_results = await self.indexer.search(
                collection_name=collection_name,
                question=question,
                top_k=top_k,
            )
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}", exc_info=True)
            raise QueryError("Search index temporarily unavailable. Please try again.", str(e))

        if not raw_results:
            return QueryPlan(
                question=question,
                confidence=0.0,
                needs_clarification=True,
                clarification_questions=[
                    "I couldn't find relevant tables for your question.",
                    "Could you rephrase or provide more details?",
                ],
                relevant_tables=[],
                dialect=dialect,
            )
        # Convert raw Qdrant payloads → TableContext objects
        candidate_tables = [self._payload_to_context(p) for p in raw_results]

        # Step 2: Ask Gemini which of these candidates are actually needed
        schema_context = self._build_schema_context(candidate_tables)
        logger.info(f"Created Schema : {schema_context}")
        analysis = await self._analyze_with_gemini(
            question, schema_context, conversation_context
        )

        # Filter to Gemini-selected tables
        selected_names = {n.lower() for n in analysis.get("relevant_tables", [])}
        if selected_names:
            relevant = [t for t in candidate_tables if t.table_name.lower() in selected_names]
            if not relevant:
                relevant = candidate_tables[:5]  # fallback: use top Qdrant results
        else:
            relevant = candidate_tables[:5]

        confidence = float(analysis.get("confidence", 0.6))
        needs_clarification = confidence < self.settings.query.confidence_threshold

        return QueryPlan(
            question=question,
            confidence=confidence,
            needs_clarification=needs_clarification,
            clarification_questions=analysis.get("clarification_questions", []),
            relevant_tables=relevant,
            dialect=dialect,
        )

    def _build_schema_context(self, tables: List[TableContext]) -> str:
        lines = ["AVAILABLE TABLES (exact names):\n"]
        for t in tables:
            col_summary = ", ".join(
                f"{c['name']} ({c['type']})" + (" [PK]" if c.get("is_pk") else "")
                for c in t.columns[:10]
            )
            # Forward FKs
            fk_out = " | ".join(
                f"{fk['from']} → {fk['to_table']}.{fk['to_col']}"
                for fk in t.foreign_keys
            ) if t.foreign_keys else "none"

            # Reverse FKs — who points AT this table
            fk_in = " | ".join(
                f"{r['referencing_table']}.{r['referencing_col']} → {t.table_name}.{r['local_col']}"
                for r in t.reverse_foreign_keys
            ) if t.reverse_foreign_keys else "none"

            lines.append(
                f"Table: {t.table_name} ({t.row_count:,} rows)\n"
                f"  Desc: {t.description[:100]}...\n"
                f"  Cols: {col_summary}\n"
                f"  FK (out): {fk_out}\n"
                f"  FK (in):  {fk_in}\n"   # ← NEW
            )
        return "\n".join(lines)

    async def _analyze_with_gemini(
        self,
        question: str,
        schema_context: str,
        history: str,
    ) -> dict:
        logger.info(f"Schema Context for Gemini:\n{schema_context}")
        history_block = f"\n{history}\n" if history else ""
        prompt = f'''Expert table selector. Pick EXACT table names from list ONLY.

{schema_context}
{history_block}
QUESTION: {question}

JSON ONLY:
{{
  "confidence": 0.95,
  "needs_clarification": false,
  "clarification_questions": [],
  "relevant_tables": ["exact_table_name1", "exact_table_name2"],
  "reasoning": "1 sentence why these tables"
}}

RULES:
- relevant_tables: EXACT names from AVAILABLE TABLES above ONLY
- No guessing - if unsure, low confidence + questions
- confidence 0.9+ for obvious matches, 0.6-0.8 ambiguous, <0.6 no tables
- needs_clarification true if <0.7
- JSON first line, NO markdown/'''

        response_text = None
        try:
            response = await get_key_manager().generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=self.settings.gemini.temperature,
                    max_output_tokens=512,
                ),
            )
            response_text = response.text.strip()
            # Strip markdown code blocks
            for prefix in ("```json", "```"):
                if response_text.startswith(prefix):
                    response_text = response_text[len(prefix):]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            return json.loads(response_text.strip())
        except json.JSONDecodeError:
            logger.warning(f"Non-JSON planner response: {response_text}")
            return {"confidence": 0.5, "needs_clarification": True,
                    "clarification_questions": ["Could you rephrase your question?"],
                    "relevant_tables": []}
        except Exception as e:
            logger.error(f"Gemini planner error: {e}", exc_info=True)
            raise QueryError("Planning service unavailable. Please try a simpler query.", str(e))

    def _payload_to_context(self, payload: dict) -> TableContext:
        import json as _json
        sample_raw = payload.get("sample_rows", "[]")
        try:
            samples = _json.loads(sample_raw) if isinstance(sample_raw, str) else sample_raw
        except Exception:
            samples = []
        return TableContext(
            table_name=payload.get("table_name", ""),
            schema_name=payload.get("schema_name"),
            dialect=payload.get("dialect", ""),
            description=payload.get("description", ""),
            columns=payload.get("columns", []),
            foreign_keys=payload.get("foreign_keys", []),
            row_count=payload.get("row_count", 0),
            sample_rows=samples,
            reverse_foreign_keys=payload.get("reverse_foreign_keys", []),
        )

    def _error_plan(self, question: str, dialect: str, msg: str) -> QueryPlan:
        return QueryPlan(
            question=question,
            confidence=0.0,
            needs_clarification=True,
            clarification_questions=[msg],
            relevant_tables=[],
            dialect=dialect,
        )

