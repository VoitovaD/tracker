from typing import Any

from aiokafka import ConsumerRecord

from app.core.clients.kafka.consumers.base import KafkaActionConsumer
from app.schemas.kafka.workout import WorkoutMessage


class WorkoutConsumer(KafkaActionConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, schema=WorkoutMessage, **kwargs)

    async def _handle_action_create(
        self, key, message: WorkoutMessage, context: Any = None
    ) -> None:
        print(f"CREATE key={key!r} data={message.data}")
    async def _handle_action_update(
        self, key, message: WorkoutMessage, context: Any = None
    ) -> None:
        print(f"UPDATE key={key!r} data={message.data}")
    async def _handle_action_delete(
        self, key, message: WorkoutMessage, context: Any = None
    ) -> None:
        print(f"DELETE key={key!r} data={message.data}")
