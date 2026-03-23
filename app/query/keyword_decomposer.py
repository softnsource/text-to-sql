# app/query/keyword_decomposer.py
import json
import google.generativeai as genai
from app.utils.gemini_key_manager import get_key_manager

async def decompose_to_keywords(question: str) -> list[str]:
    """Lightweight LLM call — returns 4-6 search keywords from the question."""
    prompt = (
        "Extract 4-6 short search keywords from this database question to help "
        "find relevant tables and columns. Focus on entity names, column concepts, "
        "and filter values.\n"
        "Return ONLY a JSON array of strings. No explanation.\n\n"
        f"Question: {question}"
    )
    response = await get_key_manager().generate_content(
        prompt,
        generation_config=genai.GenerationConfig(temperature=0, max_output_tokens=100),
    )
    try:
        return json.loads(response.text.strip())
    except Exception:
        return [question]   # fallback: full question as single keyword