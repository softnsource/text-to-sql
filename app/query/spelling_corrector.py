"""Question spell corrector — fixes typos in user questions using Gemini."""

import logging

import google.generativeai as genai

from app.utils.gemini_key_manager import get_key_manager

logger = logging.getLogger(__name__)


class QuestionCorrector:
    """Corrects spelling/grammar in user questions before passing to the SQL pipeline."""

    async def correct(self, question: str) -> str:
        """Return a spell-corrected version of the question.

        - Fixes typos and obvious misspellings.
        - Preserves domain terms, names, numbers, and the original intent.
        - If nothing needs fixing, returns the original question unchanged.
        """
        prompt = f"""
You are a spell-checker for a database query assistant.

The user has typed a question that will be converted into SQL.
Your ONLY job is to fix obvious spelling mistakes in common English words only.

STRICT RULES:
1. Fix spelling mistakes in common English words only (e.g. "incedent" → "incident").
2. Do NOT change any proper nouns — names of people, places, or organizations must be kept EXACTLY as written.
3. Do NOT rephrase, reword, or change the meaning.
4. Do NOT add or remove words unless fixing a clear typo in a common word.
5. Do NOT try to guess or "correct" names — "Vikas", "Nisarg", "Seabrooke" are valid names even if they look unusual.
6. Numbers, dates, and domain-specific terms must be preserved exactly.
7. If unsure whether something is a name or a typo → leave it unchanged.
8. If the question has no spelling errors in common words, return it exactly as-is.

USER QUESTION:
{question}

Return ONLY the corrected question as plain text. No explanation. No quotes. No JSON.
"""
        try:
            response = await get_key_manager().generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=256,
                ),
            )
            corrected = response.text.strip()

            # Safety: if response is empty or suspiciously long, fall back to original
            if not corrected or len(corrected) > len(question) * 3:
                logger.warning("Corrector returned unexpected output, using original question")
                return question

            if corrected != question:
                logger.info(f"Question corrected: '{question}' → '{corrected}'")

            return corrected

        except Exception as e:
            logger.warning(f"Spell correction failed, using original question: {e}")
            return question