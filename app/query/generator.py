"""SQL generator — converts a QueryPlan into executable SQL using Gemini."""

import json
import logging
from dataclasses import dataclass

import google.generativeai as genai

from app.config import get_settings
from app.query.planner import QueryPlan, TableContext
from app.utils.gemini_key_manager import get_key_manager
from app.query.enrich_schema import build_enriched_schema

logger = logging.getLogger(__name__)


@dataclass
class GenerationResult:
    sql: str
    explanation: str
    chat_response: str = ""
    response_intent: str = "data" 


import re

BOOLEAN_TRUE_PATTERN = [
    "1", "'1'", "'true'", "'True'", "'TRUE'",
    "'yes'", "'Yes'", "'YES'", "'y'", "'Y'"
]

BOOLEAN_FALSE_PATTERN = [
    "0", "'0'","'false'", "'False'", "'FALSE'",
    "'no'", "'No'", "'NO'", "'n'", "'N'"
]


def expand_boolean_conditions(sql: str) -> str:
    def replacer(match):
        column = match.group(1)
        value = match.group(2).lower()

        if value in ["1", "true"]:
            return f"""(
    TRY_CAST({column} AS INT) = 1
    OR LOWER(CAST({column} AS VARCHAR)) IN ('true','yes','y')
)"""
        elif value in ["0", "false"]:
            return f"""(
    TRY_CAST({column} AS INT) = 0
    OR LOWER(CAST({column} AS VARCHAR)) IN ('false','no','n')
)"""
        return match.group(0)

    pattern = r"(\w+\.\w+)\s*=\s*(1|0|true|false)"
    return re.sub(pattern, replacer, sql, flags=re.IGNORECASE)

class SQLGenerator:
    """Generates dialect-aware SQL from a query plan using Gemini."""

    def __init__(self):
        self.settings = get_settings()

    async def generate(
        self,
        plan: QueryPlan,
        conversation_context: str = "",
        session_id: str = '07acccf1-21fb-47d4-bf90-aaa83f047cfd',
    ) -> GenerationResult:
        schema_context = build_enriched_schema(plan, session_id)
        history_block = f"\n{conversation_context}\n" if conversation_context else ""
        logger.info(f"Hstory block for SQL generation (length {len(history_block)} chars): {history_block}")
        # history_block = ""
        max_rows = self.settings.query.max_rows_per_query
        # Pre-compute dialect syntax OUTSIDE the f-string to avoid expression-in-brace bugs
        if plan.dialect.lower() == "sqlserver":
            limit_syntax = f"Place TOP {max_rows} immediately after SELECT: SELECT TOP {max_rows} col1, col2 ..."
        else:
            limit_syntax = f"Place LIMIT {max_rows} at the end of the query: SELECT col1, col2 ... LIMIT {max_rows}"

        prompt = f"""
            You are a SQL generator.
            
            Your job is to convert a user question into VALID {plan.dialect.upper()} SQL using ONLY the schema provided.
            
            You MUST strictly follow the schema. NEVER invent tables or columns.
            =====================
            CONVERSATION HISTORY
            =====================
            {history_block}
            =====================
            DATABASE SCHEMA
            =====================
            {schema_context}
            
            =====================
            USER QUESTION
            =====================
            {plan.question}
            
            =====================
            INTENT DETECTION (CHECK THIS FIRST)
            =====================
            
            Before generating SQL, classify the user's message intent:
            
            1. GREETING  → "hi", "hello", "hey", "good morning", etc.
            2. WELLBEING → "how are you", "how r u", etc.
            3. THANKS    → "thanks", "thank you", "great", "awesome", etc.
            4. GOODBYE   → "bye", "goodbye", "see you", etc.
            5. OFF_TOPIC → ONLY completely non-database topics: weather, jokes, cooking, sports, etc.
            6. FOLLOWUP  → user is referring to something from CONVERSATION HISTORY above.
                        Look at the previous SQL in history and expand/modify it to answer
                        the current question. Keep the same filters/WHERE conditions from
                        the previous SQL. Remove any TOP 1 limits. Add more columns if needed.
                        Examples of followup signals: "give me details", "show more", "tell me 
                        more", "its information", "show that", "expand", "what about that",
                        anything using "its", "that", "those", "it", "this", "same", "above"
            7. DATABASE  → a fresh independent data question with no reference to prior context.
            
            CRITICAL RULES:
            - Check CONVERSATION HISTORY first before classifying as DATABASE or FOLLOWUP.
            - If history exists AND current question refers to prior result → always FOLLOWUP.
            - For FOLLOWUP: reuse the previous SQL from history as the base, modify it to answer 
            the new question. NEVER start fresh ignoring history.
            - If you cannot find a direct join path → still return SQL with best available columns.
            - NEVER return chat_response because a query is complex or join path is unclear.
            - chat_response is ONLY for pure social messages (intents 1-5). Nothing else.
            - When in doubt and history exists → FOLLOWUP.
            - When in doubt and no history → DATABASE.
            
            =====================
            STRICT RULES
            =====================
            
            1. ONLY use tables listed in SCHEMA.
            2. ONLY use columns listed under each table.
            3. NEVER invent columns.
            4. NEVER invent tables.
            5. JOIN tables ONLY using FK relationships listed in SCHEMA.
            
            6. Use aliases: t1, t2, t3, t4
            
            7. STRING FILTER RULE (CRITICAL):
            When filtering on a text/varchar column, ALWAYS use LIKE instead of =.
            Wrap the value with % wildcards so partial matches are found.
            
            ALWAYS do this:   WHERE t1.LocationOfIncident LIKE '%Oldfield%'
            NEVER do this:    WHERE t1.LocationOfIncident = 'Oldfield'
            
            This applies to ANY column that holds text values:
            names, descriptions, locations, types, statuses, and any other string field.
            
            Exceptions — use = (not LIKE) for:
            - Integer / numeric columns  (e.g. Id, SiteId, Count)
            - Boolean columns            (e.g. IsDeleted, IsPrivate)
            - Date / datetime columns    (e.g. CreationTime, DateOfBirth)
            
            IMPORTANT DATATYPE RULE: You must purely look at the datatypes annotated in the DATABASE SCHEMA (e.g. VARCHAR, NVARCHAR, INT, FLOAT, BOOLEAN, DATE) to decide this. Do NOT guess the type based on the column name alone.
            
            8. UNION COLUMN PARITY RULE (CRITICAL):
            When writing a UNION query, ALL SELECT branches MUST have the IDENTICAL
            number of columns in the SAME order.
            
            If one branch naturally has fewer meaningful columns, pad it with NULL
            placeholders using aliases that match the first branch.
            
            RULES:
            - Count columns in branch 1 first, then match exactly in branch 2.
            - Use NULL AS <alias> for columns that don't apply to a branch.
            - Never produce a UNION where branch column counts differ.
            - Always use UNION ALL (not UNION) unless deduplication is explicitly needed.
            
            =====================
            ANTI-HALLUCINATION RULE (HIGHEST PRIORITY)
            =====================

            You are FORBIDDEN from inventing any column or table name.
            If you cannot find an EXACT column that matches the user's intent after carefully splitting every schema column name into English words, then:
            - Do NOT guess or create a column like "CqcNotification", "HasCqcNotification", etc.
            - Instead, return sql = "" and set chat_response to a polite message asking for clarification, e.g.:
            "I couldn't find a column related to 'CQC notification'. Could you tell me the exact field name or describe it differently?"

            Only use column names that appear verbatim (case-insensitive) in the provided DATABASE SCHEMA.

            =====================
            COLUMN SELECTION RULE (CRITICAL)
            =====================
            For select column use all tables columns you can use join tables columns also if available
            You must carefully choose which columns to include in the SELECT clause.
            9. DO NOT use STRFTIME, DATE_FORMAT, or TO_CHAR if Dialect is Mssqlserver.
            RULES:
            
            1. ONLY select columns that are directly relevant to the user's question.
            - MAXIMUM 3-6 COLUMNS: Do NOT output massive, wide tables. If a table has many descriptive columns (like 10+ intervention or behavioral notes), pick ONLY the 3 to 5 most important core fields (e.g. Title, Name, Status, Date) and ignore the rest to avoid overwhelming the user.
            - Do NOT use SELECT * unless the user literally writes "select *" or "all details".
            
            2. NEVER include TECHNICAL, SYSTEM, or DATE columns in the SELECT output unless explicitly asked:
            - ID columns: Id, UserId, SiteId, LocationId, MasterFieldId, etc.
            - Timestamps & system dates: CreationTime, CreatedAt, UpdatedAt, DeletedAt, Time, ReviewedDate, etc.
            - ANY date/datetime column: DateOfBirth, DateOfIncident, StartDate, EndDate, ReportDate,
                IncidentDate, AdmissionDate, DischargeDate, or ANY column with a DATE/DATETIME datatype.
            - System flags: IsDeleted, IsPrimary, IsActive, WasIinvolved, etc.
            - These are internal or supplementary columns and should NEVER appear in SELECT unless
                the user's question explicitly mentions a date (e.g. "show me the date", "when did",
                "what date", "date of birth", "admission date", etc.).
            - If in User query if a user says give me this date or this then and then u can add date or else never add date if its birthdate of user then also dont add birthdate if user dont ask about it
            
            3. Technical/System/Date columns are allowed ONLY for:
            - JOIN conditions
            - WHERE / ORDER BY / GROUP BY filters (e.g. filtering by a date range the user specified)
            - NOT in SELECT unless the user explicitly asks for the date value itself
            
            4. If multiple useful columns exist, select a meaningful, HUMAN-READABLE subset:
            - Pick ONLY columns that an end-user actually cares about (e.g., Names, Titles, Statuses, Amounts, Descriptions, Summaries).
            Example:
            User asks: "show investigations"
            GOOD:
                SELECT t1.Title, t1.OutCome, t1.Consequence, t1.Severity
            BAD:
                SELECT t1.Id, t1.CreationTime, t1.IsDeleted, t1.UserId, t1.DateOfIncident
            
            5. STRICT COLUMN VALIDATION:
            - Every column in SELECT must exist in the SCHEMA
            - NEVER invent or guess column names
            
            6. NEVER use SELECT * unless explicitly asked. Even if the user asks for "all the Service User Support Plan", do NOT use SELECT *. Explicitly list only the relevant, human-readable, non-technical, non-date columns.
            
            7. CHARTING & AGGREGATION RULE:
            - If the user asks for a chart, graph, pie chart, or distribution (e.g., "across all months", "by category"):
            - You MUST select the appropriate dimension column (a date, category, or identifier).
            - EXTREMELY IMPORTANT: NEVER select long free-text or description paragraph columns when charting. These columns cannot be used as chart axes.
            - If they ask for a count or across months, ensure you group correctly or select the categorical columns explicitly.
            - Exception: date columns MAY be selected here only if the chart/grouping is date-based (e.g. "by month", "over time").
            
            =====================
            COLUMN MATCHING RULE (CRITICAL)
            =====================
            
            Users describe columns in plain English. You must map their words to the real schema column
            by mentally splitting every column name into its English words:
            
            e.g.  "LocationOfIncident" → "location of incident"
                    "TypeOfIncident"     → "type of incident"
                    "DegreeOfHarm"       → "degree of harm"
                    "DateOfBirth"        → "date of birth"
                    "SiteId"             → "site id"
            
            Do this for EVERY column in EVERY relevant table, then match against what the user said.
            
            GOLDEN RULE: If a value the user is filtering/selecting already exists as a direct column
            on the primary table, use it directly — do NOT add a JOIN to another table just because
            a related table also contains similar data.
            
            This applies universally to all tables and all columns in the schema.
            
            =====================
            EXPLICIT LIMIT DETECTION (HIGHEST PRIORITY)
            =====================

            If the user explicitly mentions a number of records, you MUST ALWAYS apply that limit.

            This OVERRIDES all other rules including table row count.

            Detect patterns like:
            - "top 10"
            - "first 5"
            - "last 20"
            - "show 15"
            - "give me 100"
            - "limit 50"
            - "only 25 records"

            RULES:
            - Extract the number N from the user query
            - Apply TOP N (SQL Server) or LIMIT N (other dialects)
            - NEVER ignore this even if table has fewer than 2000 rows

            EXAMPLES:
            User: "Give me top 10 incidents"
            → SELECT TOP 10 ...

            User: "Show 5 users"
            → SELECT TOP 5 ...

            User: "List 20 records"
            → SELECT TOP 20 ...

            This rule has STRICT PRIORITY over all LIMIT/TOP logic below.
            
            =====================
            LIMIT / TOP RULE (CRITICAL)
            =====================
            
            Before adding any LIMIT or TOP, reason through these steps in order:
            
            STEP 1 — Is the question asking for an aggregate?
            Signals: "how many", "count", "total", "sum", "average", "avg",
                    "minimum", "maximum", "min", "max", "percentage", "proportion"
            → If YES: Do NOT add LIMIT or TOP. Aggregates summarize all rows by design.
            
            STEP 2 — Did the user explicitly ask for N rows?
            Signals: "top N", "first N", "last N", "limit N", "show N records/rows/results"
            → If YES: Use exactly N as the row limit.
            
            STEP 3 — No aggregate, no explicit limit?
            → Find the ROWS count of the PRIMARY table (the table in the FROM clause) in the SCHEMA.
            → Compare ROWS against 2000:
            
            DECISION TABLE:
            ┌──────────────────────┬──────────────────────────┐
            │ Primary Table Rows   │ Action                   │
            ├──────────────────────┼──────────────────────────┤
            │ ROWS > 2000          │ Apply {limit_syntax}     │
            │ ROWS <= 2000         │ NO limit at all          │
            └──────────────────────┴──────────────────────────┘
            
            EXAMPLES:
            - BNR_Incidents    ROWS: 1722 → 1722 <= 2000 → NO limit
            - Player_history   ROWS: 235561 → 235561 > 2000 → apply {limit_syntax}
            - BNR_Safeguarding ROWS: 70   → 70 <= 2000 → NO limit
            - Order_tab        ROWS: 39   → 39 <= 2000 → NO limit
            
            JOIN RULE: Always use the FROM clause table row count ONLY. Ignore all joined tables.
            Example: FROM BNR_Incidents JOIN BNR_Sites → use BNR_Incidents ROWS: 1722 → NO limit
            
            =====================
            MANDATORY REASONING STEPS (follow in order before writing SQL)
            =====================
            
            1. Apply COLUMN MATCHING RULE — does the user's phrase map to a direct column? Use it.
            2. Identify only the tables truly needed — avoid unnecessary JOINs.
            3. Confirm every table and column exists in SCHEMA.
            4. Apply LIMIT / TOP RULE.
            5. Write the SQL.
            
            =====================
            FINAL CHECK (MANDATORY)
            =====================
            
            Before returning SQL:
            - Ensure NO column ending with "Id" is present in SELECT
            - Ensure NO date/datetime column is present in SELECT unless the user explicitly asked for it
            - Ensure all selected columns are relevant to the user query
            
            =====================
            RESPONSE INTENT (for DATABASE questions only)
            =====================
            
            After generating SQL, also classify what kind of answer the user wants:
            
            - "existence" → user asks IF data exists: "is there any X", "do we have Y", "is X available", "are there any Z"
            - "count"     → user wants HOW MANY: "how many X", "total number of Y", "count of Z"  
            - "summary"   → user wants overview/insight: "summarize X", "give me overview of Y"
            - "data"      → user wants to see actual records: "show me X", "list all Y", "get Z"
            
            Set this as the "response_intent" field in your JSON output.
            
            =====================
            OUTPUT FORMAT
            =====================
            CRITICAL: Your response MUST be a single valid JSON object. NOTHING ELSE.
            - Do NOT write plain text explanations
            - Do NOT ask clarifying questions in plain text  
            - Do NOT say "I need more information" in plain text
            - Never add chat_response for no relevnt columns found or join path issues. Just return sql="" and a clear reason.
            - If the name cannot be found → still attempt SQL searching all name columns
            - EVERY response must be valid JSON, no exceptions
            For DATABASE intent:
            {{
            "sql": "SQL_QUERY_HERE",
            "chat_response": "",
            "response_intent": "data", # one of: existence, count, summary, data
            "reason": "short explanation of logic used"
            }}
            
            For ALL other intents (greeting, wellbeing, thanks, goodbye, off_topic):
            {{
            "sql": "",
            "chat_response": "YOUR FRIENDLY NATURAL REPLY HERE",
            "reason": "intent name"
            }}
        """


        response_text = None
        try:
            response = await get_key_manager().generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=self.settings.gemini.max_tokens,
                    response_mime_type="application/json",
                ),
            )
            response_text = response.text.strip()

            # Model sometimes reasons out loud before the JSON block.
            # Grab the first { ... } block regardless of what surrounds it.
            import re
            json_match = re.search(r'(\{[\s\S]*?\})', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Fallback: remove common markdown and extra lines
                json_str = response_text
                for prefix in ("```json", "```", "json"):
                    json_str = json_str.replace(prefix, "")
                json_str = json_str.strip()

            # Clean up any trailing text after the JSON
            # This handles the "Extra data" error
            try:
                # Use raw_decode to get only the first valid JSON object
                decoder = json.JSONDecoder()
                data, idx = decoder.raw_decode(json_str)
                # If there's extra text after, we ignore it
            except json.JSONDecodeError:
                # Final aggressive cleanup
                json_str = re.sub(r'^.*?(\{.*\})', r'\1', json_str, flags=re.DOTALL)
                data = json.loads(json_str)
            sql = data.get("sql", "").strip().rstrip(";")
            chat_response = data.get("chat_response", "").strip()
            response_intent = data.get("response_intent", "data").strip()
            explanation = data.get("reason", data.get("explanation", ""))
            if chat_response and not sql:
                logger.info(f"Chat intent detected: {explanation}")
                return GenerationResult(sql="", explanation=explanation, chat_response=chat_response)

            sql = expand_boolean_conditions(sql)
            logger.info(f"Sql -> {sql}")
            if not sql:
                raise ValueError("Gemini returned empty SQL")

            return GenerationResult(sql=sql, explanation=explanation,response_intent=response_intent)

        except json.JSONDecodeError as e:
            logger.error(f"Non-JSON generator response: {response_text}")
            raise ValueError(f"SQL generation returned invalid JSON: {e}")
        except Exception as e:
            logger.error(f"SQL generation failed: {e}", exc_info=True)
            raise
