import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import PositiveInt

from .exceptions import ConsumerImproperlyConfigurationError, ConsumerRetriesExceededError
from .schemas import CUDMessageValue


class KafkaConsumer(ABC):
    def __init__(
        self,
        consumer_class: type[AIOKafkaConsumer],
        consumer_args: tuple,
        consumer_kwargs: dict,
        process_retries: PositiveInt = 1,
        logger: logging.Logger | None = None,
    ):
        if not (isinstance(process_retries, int) and process_retries > 0):
            raise ConsumerImproperlyConfigurationError("'process_retries' must be positive integer")

        self.consumer_class = consumer_class
        self.consumer_args = consumer_args
        self.consumer_kwargs = consumer_kwargs
        self.consumer = None
        self._process_retries = process_retries

        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger

    def consumer_is_started(self) -> bool:
        return self.consumer is not None

    async def start(self):
        if self.consumer:
            await self.stop()
        self.consumer = self.consumer_class(*self.consumer_args, **self.consumer_kwargs)
        await self.consumer.start()

    async def stop(self):
        if not self.consumer:
            return
        await self.consumer.stop()
        self.consumer = None

    async def run(self, once: bool = False) -> None:
        self.logger.info(f"{self.__class__.__name__}: Consumer started")
        if not self.consumer_is_started():
            await self.start()
        try:
            await self._read_messages(once=once)
        finally:
            await self.stop()
            self.logger.info(f"{self.__class__.__name__}: Consumer stopped")

    async def _read_messages(self, once: bool = False) -> None:
        async for msg in self.consumer:  # type: ignore
            async with self.context_manager() as context:
                self.logger.debug(f"{self.__class__.__name__}: Reading message: {msg}")
                for _ in range(self._process_retries):
                    try:
                        await self.process_message(msg, context)
                        break
                    except Exception:
                        self.logger.exception(f"{self.__class__.__name__}: Error while processing message {msg.value}")
                else:
                    raise ConsumerRetriesExceededError(
                        f"{self.__class__.__name__}: Number of retires exceeded ({self._process_retries})"
                    )
                await self.consumer.commit()  # type: ignore
                if once:
                    break

    @abstractmethod
    async def process_message(self, message: ConsumerRecord, context: Any = None) -> None:
        pass

    @asynccontextmanager
    async def context_manager(self) -> AsyncGenerator:
        yield


class KafkaActionConsumer(KafkaConsumer):
    def __init__(
        self,
        consumer_class: type[AIOKafkaConsumer],
        consumer_args: tuple,
        consumer_kwargs: dict,
        schema: type[CUDMessageValue],
        process_retries: int = 1,
        logger: logging.Logger | None = None,
    ):
        super().__init__(consumer_class, consumer_args, consumer_kwargs, process_retries, logger)
        self._schema = schema

    async def process_message(self, message: ConsumerRecord, context: Any = None) -> None:
        self.logger.debug(f"Message {message} | {message.key}")
        message_value = self._schema.model_validate(message.value)
        action = message_value.get_action()
        if hasattr(self, f"_handle_action_{action.lower()}"):
            await getattr(self, f"_handle_action_{action.lower()}")(message.key, message_value, context)
