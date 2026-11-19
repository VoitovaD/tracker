import logging
import traceback
from uuid import UUID

from ninja import Router

from django.http import JsonResponse
from pydantic import BaseModel

from workout.models import TestItem
from core.kafka_producer import get_producer

logger = logging.getLogger(__name__)

test_router = Router()

@test_router.get("/items")
def get_items(request):
    items = TestItem.objects.all().values("id", "name", "created_at")
    return list(items)

@test_router.post("/items")
def create_item(request, name: str):
    item = TestItem.objects.create(name=name)
    return {"id": item.id, "name": item.name}


# перенести в схемы
class WorkoutEvent(BaseModel):
    workout_id: UUID
    workout_type: str
    start_time: str
    end_time: str
    heart_rate: list[dict]
    weight_kg: float
    steps: int | None = None
    gps: list[dict] | None = None

class WorkoutMessage(BaseModel):
    action: str
    data: WorkoutEvent


@test_router.post("/kafka-1")
def test_kafka(request, message: WorkoutMessage):
    try:
        # key = f"{data.user_id}-{int(time.time() * 1000)}"
        key = str(message.data.workout_id)
        # producer.send(key=key, value=data)
        producer = get_producer()
        producer.send(key=key, value=message.model_dump(mode="json"))
        producer.flush()
        return JsonResponse({"status": "sent"})
    except Exception as e:
        print(f"ERROR: {e}")
        print(traceback.format_exc())
        return JsonResponse({"status": "error", "detail": str(e)}, status=500)