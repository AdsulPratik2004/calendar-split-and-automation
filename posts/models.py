"""
Pydantic models for request validation.
"""
from pydantic import BaseModel


class CalendarPayload(BaseModel):
    """Payload model for calendar split request."""
    calendarRowId: str

