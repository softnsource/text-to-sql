"""Custom application exceptions with user-friendly messages."""

from typing import Optional


class AppError(Exception):
    """Base exception for all application errors."""
    
    def __init__(self, user_message: str, internal: Optional[str] = None):
        self.user_message = user_message
        self.internal = internal or user_message
        super().__init__(self.internal)


class DBError(AppError):
    """Database connection or query errors."""


class QueryError(AppError):
    """Query planning, generation, or execution errors."""


class TrainingError(AppError):
    """Training/indexing pipeline errors."""


class SessionError(AppError):
    """Session management errors."""


class ValidationError(AppError):
    """Input validation errors."""
