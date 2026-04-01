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
        "label": "Service User",
        "table": "BNR_Service_User",
        "keywords": ["service user", "service client", "client", "service"],
    },
    {
        "label": "Staff",
        "table": "BNR_User_Details",
        "keywords": ["staff", "operator", "support staff", "manager", "employee", "personnel"],
    },
    {
        "label": "Visitor",
        "table": "BNR_Visitors",
        "keywords": ["visitor", "visitors", "guest", "guests"],
    },
    {
        "label": "Other",
        "table": None,  # ← No specific table; let Qdrant decide naturally
        "keywords": ["other", "not sure", "unknown", "don't know", "unsure", "general"],
    },
]
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
    pending_user_type_options: List[Dict] = field(default_factory=list)
    
def _normalize_table_name(name: str) -> str:
    """Remove underscores and lowercase for fuzzy matching."""
    return name.lower().replace("_", "")


def get_user_type_tables_in_candidates(
    candidate_tables: List[TableContext],
) -> List[Dict]:
    """
    Returns only the USER_TYPE_OPTIONS entries whose canonical table
    actually appeared in the Qdrant candidate list.
    Normalized comparison: case-insensitive + underscore-agnostic.
    """
    # Normalize all candidate names once
    logger.info(f"Candidate tables : {candidate_tables}")
    candidate_normalized = {
        _normalize_table_name(t.table_name): t.table_name  # normalized → original
        for t in candidate_tables
    }
    logger.info(f"Candidate normalized : {candidate_normalized}")

    matched = []
    for option in USER_TYPE_OPTIONS:
        if option["table"]:
            normalized_option = _normalize_table_name(option["table"])
            if normalized_option in candidate_normalized:
                # ✅ Store the ACTUAL table name from Qdrant (not the one in USER_TYPE_OPTIONS)
                # so resolved_user_table always matches exactly what's in the DB
                matched.append({
                    **option,
                    "table": candidate_normalized[normalized_option],  # use real name
                })
    return matched


async def build_multi_table_clarification_text(
    question: str,
    matched_options: List[Dict],
) -> str:
    """
    Dynamically generates a clarification message when multiple user-type
    tables are found. Options list is built from actual Qdrant results.
    """
    options_text = "\n".join(
        f"{i + 1}. {opt['label']}" for i, opt in enumerate(matched_options)
    )

    prompt = f"""
You are a helpful database assistant.

A user asked: "{question}"

The question could relate to multiple types of users in the system.
You need to ask which type of user data they are referring to.

Available user types found:
{options_text}

Guidelines:
- Be conversational and concise
- Reference the user types by their label names
- Do NOT mention table names or technical details
- Ask clearly which type of data they want to look at

Return ONLY the final message to the user.
"""

    try:
        response = await get_key_manager().generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.7,
                max_output_tokens=150,
            ),
        )
        return response.text.strip()

    except Exception as e:
        logger.warning(f"LLM multi-table clarification failed: {e}")
        labels = ", ".join(f"'{o['label']}'" for o in matched_options)
        return (
            f"Your question could relate to different types of users. "
            f"Could you clarify whether you mean: {labels}?"
        )

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

        # # ------------------------------------------------------------------
        # # 1. User-entity clarification gate
        # # ------------------------------------------------------------------
        # No name-based clarification gate — proceed directly
        user_opted_out = resolved_user_table == "__skip__"
        effective_resolved_table = None if user_opted_out else resolved_user_table
        logger.info(f"effective_resolved_table: {effective_resolved_table}")
        # if has_user_reference and not effective_resolved_table and not user_opted_out:
        #     entity_name = user_entity_name or "the person"
        #     return QueryPlan(
        #         question=question,
        #         confidence=0.0,
        #         needs_clarification=True,
        #         clarification_questions=[await build_user_type_clarification_text(entity_name)],
        #         relevant_tables=[],
        #         dialect=dialect,
        #         pending_user_type_clarification=True,
        #         pending_entity_name=entity_name,
        #     )

        # ------------------------------------------------------------------
        # 2. Enrich the question with the resolved user table hint (if any)
        # ------------------------------------------------------------------
        effective_question = question
        # if effective_resolved_table:
        #     effective_question = (
        #         f"{question} [include table: {effective_resolved_table}]"
        #     )

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

        # if not effective_resolved_table and not user_opted_out:
        # matched_user_options = get_user_type_tables_in_candidates(candidate_tables)
        # logger.info(f"Matched User options : {matched_user_options}")
        # if len(matched_user_options) > 1:
        #     clarification_msg = await build_multi_table_clarification_text(
        #         question, matched_user_options
        #     )
        #     return QueryPlan(
        #         question=question,
        #         confidence=0.0,
        #         needs_clarification=True,
        #         clarification_questions=[clarification_msg],
        #         relevant_tables=[],
        #         dialect=dialect,
        #         pending_user_type_clarification=True,
        #         # No entity name — this is a table-ambiguity clarification
        #         pending_entity_name=None,
        #         # Only the matched options — not the full static list
        #         pending_user_type_options=matched_user_options,
        #     )

        # elif len(matched_user_options) == 1:
        #     # Only one user table found — auto-resolve, no need to ask
        #     auto_table = matched_user_options[0]["table"]
        #     effective_resolved_table = auto_table
        #     effective_question = (
        #         f"{question} [user table to use: {auto_table}]"
        #     )

        # # ------------------------------------------------------------------
        # # 4. If resolved_user_table isn't already in candidates, add a stub
        # #    so Gemini can see and select it.
        # # ------------------------------------------------------------------
        # if effective_resolved_table:
        #     # Use normalized comparison — handles underscore/case mismatches
        #     resolved_normalized = _normalize_table_name(effective_resolved_table)
        #     candidate_names_normalized = {
        #         _normalize_table_name(t.table_name) for t in candidate_tables
        #     }

        #     if resolved_normalized not in candidate_names_normalized:
        #         # Table genuinely not in candidates — fetch from Qdrant
        #         payload = await self.indexer.get_by_table_name(
        #             collection_name, effective_resolved_table
        #         )
        #         if payload:
        #             real_table = self._payload_to_context(payload)
        #             candidate_tables.append(real_table)
        #         else:
        #             logger.warning(
        #                 f"[planner] '{effective_resolved_table}' not found in Qdrant either — skipping stub."
        #             )
        #             # ← DO NOT add empty stub — it confuses Gemini more than helps
        #     else:
        #         logger.info(
        #             f"[planner] '{effective_resolved_table}' already in candidates — skipping fetch."
        #         )


        # ------------------------------------------------------------------
        # 5. Ask Gemini which candidates are actually needed
        # ------------------------------------------------------------------
        schema_context = self._build_schema_context(candidate_tables)

        analysis = await self._analyze_with_gemini(
            effective_question, schema_context, conversation_context
        )

        selected_names = {n.lower() for n in analysis.get("relevant_tables", [])}
        logger.info(f"Selected Name : {selected_names}")
        # Always honour the resolved user table even if Gemini missed it
        if effective_resolved_table:
            selected_names.add(effective_resolved_table.lower())

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
                    temperature=0.0,
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