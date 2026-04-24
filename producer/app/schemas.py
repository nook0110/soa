"""
Pydantic models for HTTP API validation.
These mirror the Protobuf schema in proto/movie_event.proto.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEventRequest(BaseModel):
    event_id: Optional[UUID] = None  # auto-generated if absent
    user_id: str = Field(..., min_length=1)
    movie_id: str = Field(..., min_length=1)
    event_type: EventType
    timestamp: Optional[datetime] = None  # UTC; defaults to now()
    device_type: DeviceType
    session_id: str = Field(..., min_length=1)
    progress_seconds: int = Field(default=0, ge=0)


class MovieEventResponse(BaseModel):
    event_id: str
    status: str = "published"
