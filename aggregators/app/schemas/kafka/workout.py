from uuid import UUID
from pydantic import BaseModel
from app.core.clients.kafka.consumers.schemas import CUDMessageValue


class WorkoutPayload(BaseModel):
    workout_id: UUID
    workout_type: str #Enum
    start_time: str
    end_time: str
    steps: int | None = None
    gps: list[dict] | None = None
    heart_rate: list[dict]
    weight_kg: float

class WorkoutMessage(CUDMessageValue[WorkoutPayload]):
    pass
