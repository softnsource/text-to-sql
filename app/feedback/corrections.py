"""User corrections handler - captures user feedback on incorrect results, learns from corrections to improve future queries."""

from typing import Any, List, Dict, Optional
from sqlalchemy.orm import Session

from app.db.metadata_models import Correction, get_session


class CorrectionStore:
    """Store and retrieve user corrections for improved query accuracy."""

    def save_correction(
        self,
        question: str,
        correct_interpretation: str,
        created_by: Optional[str] = None
    ) -> int:
        """Save a user correction to the database.

        Args:
            question: Original question that was misinterpreted.
            correct_interpretation: The correct interpretation or expected behavior.
            created_by: User ID who provided the correction.

        Returns:
            ID of the created correction record.
        """
        with get_session() as session:
            correction = Correction(
                trigger_phrase=question,
                correct_interpretation=correct_interpretation,
                created_by=created_by,
                times_used=0
            )
            session.add(correction)
            session.flush()
            correction_id = correction.id
            return correction_id

    def get_relevant_corrections(
        self,
        question: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find corrections relevant to the given question.

        Uses simple substring matching. Can be upgraded to embedding similarity later.

        Args:
            question: User question to find corrections for.
            limit: Maximum number of corrections to return.

        Returns:
            List of correction dicts with id, trigger_phrase, correct_interpretation, times_used.
        """
        with get_session() as session:
            # Simple substring match: find corrections where trigger phrase appears in question
            # or question appears in trigger phrase (case-insensitive)
            question_lower = question.lower()

            corrections = session.query(Correction).filter(
                Correction.trigger_phrase.ilike(f"%{question_lower}%")
            ).order_by(
                Correction.times_used.desc(),
                Correction.created_at.desc()
            ).limit(limit).all()

            return [
                {
                    "id": c.id,
                    "trigger_phrase": c.trigger_phrase,
                    "correct_interpretation": c.correct_interpretation,
                    "times_used": c.times_used,
                    "created_at": c.created_at.isoformat()
                }
                for c in corrections
            ]

    def increment_usage(self, correction_id: int) -> None:
        """Increment the times_used counter for a correction.

        Args:
            correction_id: ID of the correction to increment.
        """
        with get_session() as session:
            correction = session.query(Correction).filter(
                Correction.id == correction_id
            ).first()

            if correction:
                correction.times_used += 1


# Singleton instance
_correction_store: Optional[CorrectionStore] = None


def get_correction_store() -> CorrectionStore:
    """Get or create the singleton CorrectionStore instance.

    Returns:
        CorrectionStore instance.
    """
    global _correction_store
    if _correction_store is None:
        _correction_store = CorrectionStore()
    return _correction_store
