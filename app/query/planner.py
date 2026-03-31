# """Query planner — searches Qdrant for relevant tables, then uses Gemini to confirm intent."""

# import asyncio
# import json
# import logging
# from dataclasses import dataclass, field
# from typing import Dict, List, Optional

# import google.generativeai as genai

# from app.config import get_settings
# from app.training.indexer import Indexer
# from app.utils.gemini_key_manager import get_key_manager
# from app.exceptions import QueryError

# logger = logging.getLogger(__name__)


# @dataclass
# class TableContext:
#     """Lightweight table reference returned from planner."""
#     table_name: str
#     schema_name: Optional[str]
#     dialect: str
#     description: str
#     columns: List[Dict]       # list of {name, type, is_pk, is_fk, fk_target_table}
#     foreign_keys: List[Dict]  # [{from, to_table, to_col}]
#     row_count: int
#     sample_rows: List[Dict] = field(default_factory=list)
#     reverse_foreign_keys: List[Dict] = field(default_factory=list)  # ← NEW


# @dataclass
# class QueryPlan:
#     question: str
#     confidence: float
#     needs_clarification: bool
#     clarification_questions: List[str]
#     relevant_tables: List[TableContext]
#     dialect: str


# class QueryPlanner:
#     """Determines which tables are needed to answer a user question."""

#     def __init__(self):
#         self.settings = get_settings()
#         self.indexer = Indexer()

#     async def plan(
#         self,
#         collection_name: str,
#         question: str,
#         dialect: str,
#         conversation_context: str = "",
#     ) -> QueryPlan:
#         """Create a query plan for the user's question.

#         Args:
#             session_id: Session identifier (maps to Qdrant collection)
#             question: Natural language question from user
#             dialect: SQL dialect for this session
#             conversation_context: Formatted prior conversation turns

#         Returns:
#             QueryPlan with relevant tables and confidence score
#         """
#         top_k = self.settings.query.top_k_tables

#         # Step 1: Semantic search in Qdrant
#         try:
#             raw_results = await self.indexer.search(
#                 collection_name=collection_name,
#                 question=question,
#                 top_k=top_k,
#             )
#         except Exception as e:
#             logger.error(f"Qdrant search failed: {e}", exc_info=True)
#             raise QueryError("Search index temporarily unavailable. Please try again.", str(e))

#         if not raw_results:
#             return QueryPlan(
#                 question=question,
#                 confidence=0.0,
#                 needs_clarification=True,
#                 clarification_questions=[
#                     "I couldn't find relevant tables for your question.",
#                     "Could you rephrase or provide more details?",
#                 ],
#                 relevant_tables=[],
#                 dialect=dialect,
#             )
#         # Convert raw Qdrant payloads → TableContext objects
#         candidate_tables = [self._payload_to_context(p) for p in raw_results]

#         # Step 2: Ask Gemini which of these candidates are actually needed
#         schema_context = self._build_schema_context(candidate_tables)
#         logger.info(f"Created Schema : {schema_context}")
#         analysis = await self._analyze_with_gemini(
#             question, schema_context, conversation_context
#         )

#         # Filter to Gemini-selected tables
#         selected_names = {n.lower() for n in analysis.get("relevant_tables", [])}
#         if selected_names:
#             relevant = [t for t in candidate_tables if t.table_name.lower() in selected_names]
#             if not relevant:
#                 relevant = candidate_tables[:5]  # fallback: use top Qdrant results
#         else:
#             relevant = candidate_tables[:5]

#         confidence = float(analysis.get("confidence", 0.6))
#         needs_clarification = confidence < self.settings.query.confidence_threshold

#         return QueryPlan(
#             question=question,
#             confidence=confidence,
#             needs_clarification=needs_clarification,
#             clarification_questions=analysis.get("clarification_questions", []),
#             relevant_tables=relevant,
#             dialect=dialect,
#         )

#     def _build_schema_context(self, tables: List[TableContext]) -> str:
#         lines = ["AVAILABLE TABLES (exact names):\n"]
#         for t in tables:
#             col_summary = ", ".join(
#                 f"{c['name']} ({c['type']})" + (" [PK]" if c.get("is_pk") else "")
#                 for c in t.columns[:10]
#             )
#             # Forward FKs
#             fk_out = " | ".join(
#                 f"{fk['from']} → {fk['to_table']}.{fk['to_col']}"
#                 for fk in t.foreign_keys
#             ) if t.foreign_keys else "none"

#             # Reverse FKs — who points AT this table
#             fk_in = " | ".join(
#                 f"{r['referencing_table']}.{r['referencing_col']} → {t.table_name}.{r['local_col']}"
#                 for r in t.reverse_foreign_keys
#             ) if t.reverse_foreign_keys else "none"

#             lines.append(
#                 f"Table: {t.table_name} ({t.row_count:,} rows)\n"
#                 f"  Desc: {t.description[:100]}...\n"
#                 f"  Cols: {col_summary}\n"
#                 f"  FK (out): {fk_out}\n"
#                 f"  FK (in):  {fk_in}\n"   # ← NEW
#             )
#         return "\n".join(lines)

#     async def _analyze_with_gemini(
#         self,
#         question: str,
#         schema_context: str,
#         history: str,
#     ) -> dict:
#         logger.info(f"Schema Context for Gemini:\n{schema_context}")
#         history_block = f"\n{history}\n" if history else ""
#         prompt = f'''Expert table selector. Pick EXACT table names from list ONLY.

# {schema_context}
# {history_block}
# QUESTION: {question}

# JSON ONLY:
# {{
#   "confidence": 0.95,
#   "needs_clarification": false,
#   "clarification_questions": [],
#   "relevant_tables": ["exact_table_name1", "exact_table_name2"],
#   "reasoning": "1 sentence why these tables"
# }}

# RULES:
# - relevant_tables: EXACT names from AVAILABLE TABLES above ONLY
# - No guessing - if unsure, low confidence + questions
# - confidence 0.9+ for obvious matches, 0.6-0.8 ambiguous, <0.6 no tables
# - needs_clarification true if <0.7
# - JSON first line, NO markdown/'''

#         response_text = None
#         try:
#             response = await get_key_manager().generate_content(
#                 prompt,
#                 generation_config=genai.GenerationConfig(
#                     temperature=self.settings.gemini.temperature,
#                     max_output_tokens=512,
#                 ),
#             )
#             response_text = response.text.strip()
#             # Strip markdown code blocks
#             for prefix in ("```json", "```"):
#                 if response_text.startswith(prefix):
#                     response_text = response_text[len(prefix):]
#             if response_text.endswith("```"):
#                 response_text = response_text[:-3]
#             return json.loads(response_text.strip())
#         except json.JSONDecodeError:
#             logger.warning(f"Non-JSON planner response: {response_text}")
#             return {"confidence": 0.5, "needs_clarification": True,
#                     "clarification_questions": ["Could you rephrase your question?"],
#                     "relevant_tables": []}
#         except Exception as e:
#             logger.error(f"Gemini planner error: {e}", exc_info=True)
#             raise QueryError("Planning service unavailable. Please try a simpler query.", str(e))

#     def _payload_to_context(self, payload: dict) -> TableContext:
#         import json as _json
#         sample_raw = payload.get("sample_rows", "[]")
#         try:
#             samples = _json.loads(sample_raw) if isinstance(sample_raw, str) else sample_raw
#         except Exception:
#             samples = []
#         return TableContext(
#             table_name=payload.get("table_name", ""),
#             schema_name=payload.get("schema_name"),
#             dialect=payload.get("dialect", ""),
#             description=payload.get("description", ""),
#             columns=payload.get("columns", []),
#             foreign_keys=payload.get("foreign_keys", []),
#             row_count=payload.get("row_count", 0),
#             sample_rows=samples,
#             reverse_foreign_keys=payload.get("reverse_foreign_keys", []),
#         )

#     def _error_plan(self, question: str, dialect: str, msg: str) -> QueryPlan:
#         return QueryPlan(
#             question=question,
#             confidence=0.0,
#             needs_clarification=True,
#             clarification_questions=[msg],
#             relevant_tables=[],
#             dialect=dialect,
#         )
"""Query planner — searches Qdrant for relevant tables, then uses Gemini to confirm intent."""

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


# ---------------------------------------------------------------------------
# User-type routing — single source of truth
# Each entry: (human-readable label, canonical table name, example keywords)
# ---------------------------------------------------------------------------
USER_TYPE_OPTIONS = [
    {
        "label": "Service User / Client",
        "table": "BNR_ServiceUser",
        "keywords": ["service user", "service client", "client", "service"],
    },
    {
        "label": "Visitor / Guest",
        "table": "BNR_Visitors",
        "keywords": ["visitor", "visitors", "guest", "guests"],
    },
    {
        "label": "Staff / Operator / Manager",
        "table": "BNR_UserDetails",
        "keywords": ["staff", "operator", "support staff", "manager", "employee", "personnel"],
    },
    {
        "label": "Other / Not Sure",
        "table": None,  # ← No specific table; let Qdrant decide naturally
        "keywords": ["other", "not sure", "unknown", "don't know", "unsure", "general"],
    },
]


def resolve_user_type_table(user_type_answer: str) -> Optional[str]:
    answer_lower = user_type_answer.lower()
    for option in USER_TYPE_OPTIONS:
        for kw in option["keywords"]:
            if kw in answer_lower:
                # "Other" option has no table — return sentinel to skip injection
                return option["table"] if option["table"] else "__skip__"
    # Partial match fallback
    for option in USER_TYPE_OPTIONS:
        if any(word in answer_lower for word in option["label"].lower().split("/")):
            return option["table"] if option["table"] else "__skip__"
    return None


async def build_user_type_clarification_text(entity_name: str) -> str:
    """Generate a natural, human-like clarification message using LLM."""

    options_text = "\n".join(
        f"{i + 1}. {opt['label']}" for i, opt in enumerate(USER_TYPE_OPTIONS)
    )

    prompt = f"""
You are a helpful assistant.

A user asked a question that mentions a person named "{entity_name}", but we need clarification about that PERSON — not the user asking the question.

Your task:
Ask the user what type of user "{entity_name}" is.

Guidelines:
- Be clear that you are asking about "{entity_name}"
- Do NOT ask about "you" or the person asking the question
- Be natural and conversational
- Keep it short and clear

Options:
{options_text}

Return ONLY the final message.
"""

    try:
        response = await get_key_manager().generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.7,  # more natural tone
                max_output_tokens=150,
            ),
        )
        return response.text.strip()

    except Exception as e:
        logger.warning(f"LLM clarification generation failed: {e}")

        # Fallback (your current version)
        return await build_user_type_clarification_text(entity_name)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TableContext:
    """Lightweight table reference returned from planner."""
    table_name: str
    schema_name: Optional[str]
    dialect: str
    description: str
    columns: List[Dict]
    foreign_keys: List[Dict]
    row_count: int
    sample_rows: List[Dict] = field(default_factory=list)
    reverse_foreign_keys: List[Dict] = field(default_factory=list)


@dataclass
class QueryPlan:
    question: str
    confidence: float
    needs_clarification: bool
    clarification_questions: List[str]
    relevant_tables: List[TableContext]
    dialect: str
    # Set when we need the user to clarify which user type a named person belongs to
    pending_user_type_clarification: bool = False
    # The name we detected (e.g. "Louis") — passed back so the API can store it
    pending_entity_name: Optional[str] = None
    resolved_user_table: Optional[str] = None
    user_entity_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------

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
        # When the API has already resolved the user table (after clarification),
        # pass it here so we skip the clarification gate entirely.
        resolved_user_table: Optional[str] = None,
        # The name of the person detected (e.g. "Louis") — used for logging / hint injection
        user_entity_name: Optional[str] = None,
        # Set True if this question contains a user-entity reference detected by corrector
        has_user_reference: bool = False,
    ) -> QueryPlan:
        """Create a query plan for the user's question.

        User-type handling
        ------------------
        • If `has_user_reference=True` and `resolved_user_table` is NOT set →
          return a clarification plan immediately (no Qdrant / Gemini call).

        • If `resolved_user_table` is provided → inject it as a hint into the
          question so Qdrant and Gemini both target the right table.
        """

        # ------------------------------------------------------------------
        # 1. User-entity clarification gate
        # ------------------------------------------------------------------
        user_opted_out = resolved_user_table == "__skip__"
        effective_resolved_table = None if user_opted_out else resolved_user_table

        if has_user_reference and not effective_resolved_table and not user_opted_out:
            entity_name = user_entity_name or "the person"
            logger.info(
                f"[planner] User entity '{entity_name}' detected but type unresolved — "
                "returning clarification plan."
            )
            return QueryPlan(
                question=question,
                confidence=0.0,
                needs_clarification=True,
                clarification_questions=[await build_user_type_clarification_text(entity_name)],
                relevant_tables=[],
                dialect=dialect,
                pending_user_type_clarification=True,
                pending_entity_name=entity_name,
            )

        # ------------------------------------------------------------------
        # 2. Enrich the question with the resolved user table hint (if any)
        # ------------------------------------------------------------------
        effective_question = question
        if resolved_user_table:
            effective_question = (
                f"{question} [user table to use: {resolved_user_table}]"
            )
            logger.info(
                f"[planner] Injecting resolved user table '{resolved_user_table}' into question."
            )

        # ------------------------------------------------------------------
        # 3. Semantic search in Qdrant
        # ------------------------------------------------------------------
        top_k = self.settings.query.top_k_tables
        try:
            raw_results = await self.indexer.search(
                collection_name=collection_name,
                question=effective_question,
                top_k=top_k,
            )
        except Exception as e:
            logger.error(f"Qdrant search failed: {e}", exc_info=True)
            raise QueryError(
                "Search index temporarily unavailable. Please try again.", str(e)
            )

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

        candidate_tables = [self._payload_to_context(p) for p in raw_results]

        # ------------------------------------------------------------------
        # 4. If resolved_user_table isn't already in candidates, add a stub
        #    so Gemini can see and select it.
        # ------------------------------------------------------------------
        if resolved_user_table:
            candidate_names = {t.table_name.lower() for t in candidate_tables}
            if resolved_user_table.lower() not in candidate_names:
                logger.info(
                    f"[planner] '{resolved_user_table}' not in top-{top_k}; "
                    "fetching by exact metadata match."
                )
                payload = await self.indexer.get_by_table_name(
                    collection_name, resolved_user_table
                )
                if payload:
                    real_table = self._payload_to_context(payload)
                    candidate_tables.append(real_table)
                    logger.info(
                        f"[planner] Appended '{resolved_user_table}' with "
                        f"{len(real_table.columns)} columns to candidates."
                    )
                else:
                    # Rare: table genuinely missing from index
                    logger.warning(
                        f"[planner] '{resolved_user_table}' not found in Qdrant — "
                        "inserting empty stub. Generator may hallucinate columns."
                    )
                    candidate_tables.append(
                        TableContext(
                            table_name=resolved_user_table,
                            schema_name=None,
                            dialect=dialect,
                            description=f"Resolved user table for: {question}",
                            columns=[],
                            foreign_keys=[],
                            row_count=0,
                        )
                    )


        # ------------------------------------------------------------------
        # 5. Ask Gemini which candidates are actually needed
        # ------------------------------------------------------------------
        schema_context = self._build_schema_context(candidate_tables)
        logger.info(f"[planner] Schema context for Gemini:\n{schema_context}")

        analysis = await self._analyze_with_gemini(
            effective_question, schema_context, conversation_context
        )

        selected_names = {n.lower() for n in analysis.get("relevant_tables", [])}

        # Always honour the resolved user table even if Gemini missed it
        if resolved_user_table:
            selected_names.add(resolved_user_table.lower())

        if selected_names:
            relevant = [
                t for t in candidate_tables if t.table_name.lower() in selected_names
            ]
            if not relevant:
                relevant = candidate_tables[:5]
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
            resolved_user_table=resolved_user_table,
            user_entity_name=user_entity_name,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_schema_context(self, tables: List[TableContext]) -> str:
        lines = ["AVAILABLE TABLES (exact names):\n"]
        for t in tables:
            col_summary = ", ".join(
                f"{c['name']} ({c['type']})" + (" [PK]" if c.get("is_pk") else "")
                for c in t.columns[:10]
            )
            fk_out = " | ".join(
                f"{fk['from']} → {fk['to_table']}.{fk['to_col']}"
                for fk in t.foreign_keys
            ) if t.foreign_keys else "none"

            fk_in = " | ".join(
                f"{r['referencing_table']}.{r['referencing_col']} → {t.table_name}.{r['local_col']}"
                for r in t.reverse_foreign_keys
            ) if t.reverse_foreign_keys else "none"

            lines.append(
                f"Table: {t.table_name} ({t.row_count:,} rows)\n"
                f"  Desc: {t.description[:100]}...\n"
                f"  Cols: {col_summary}\n"
                f"  FK (out): {fk_out}\n"
                f"  FK (in):  {fk_in}\n"
            )
        return "\n".join(lines)

    async def _analyze_with_gemini(
        self,
        question: str,
        schema_context: str,
        history: str,
    ) -> dict:
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
            for prefix in ("```json", "```"):
                if response_text.startswith(prefix):
                    response_text = response_text[len(prefix):]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            return json.loads(response_text.strip())
        except json.JSONDecodeError:
            logger.warning(f"Non-JSON planner response: {response_text}")
            return {
                "confidence": 0.5,
                "needs_clarification": True,
                "clarification_questions": ["Could you rephrase your question?"],
                "relevant_tables": [],
            }
        except Exception as e:
            logger.error(f"Gemini planner error: {e}", exc_info=True)
            raise QueryError(
                "Planning service unavailable. Please try a simpler query.", str(e)
            )

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