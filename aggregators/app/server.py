import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import orjson
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.apps.api import main_router
from app.apps.consumers.workout import WorkoutConsumer
from app.config import app_settings, kafka_settings


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    consumer_params = {
        "bootstrap_servers": kafka_settings.KAFKA_BOOTSTRAP_SERVERS,
        "group_id": kafka_settings.KAFKA_GROUP_ID,
        "enable_auto_commit": False,
        "value_deserializer": lambda x: orjson.loads(x) if x else None,
        "security_protocol": kafka_settings.KAFKA_SECURITY_PROTOCOL,
    }
    if kafka_settings.KAFKA_SASL_MECHANISM:
        consumer_params["sasl_mechanism"] = kafka_settings.KAFKA_SASL_MECHANISM
        consumer_params["sasl_plain_username"] = kafka_settings.KAFKA_SASL_USERNAME
        consumer_params["sasl_plain_password"] = kafka_settings.KAFKA_SASL_PASSWORD
    workout_consumer = WorkoutConsumer(
        consumer_class=AIOKafkaConsumer,
        consumer_args=(kafka_settings.KAFKA_WORKOUT_TOPIC,),
        consumer_kwargs=consumer_params,
        logger=logger,
    )
    task = asyncio.create_task(workout_consumer.run(), name="workout-consumer")
    logger.info("Workout consumer task started")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await workout_consumer.stop()
        logger.info("Workout consumer stopped")


def setup_middlewares(application: FastAPI) -> None:
    application.add_middleware(
        CORSMiddleware,
        allow_origins=app_settings.allow_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def setup_routers(application: FastAPI) -> None:
    application.include_router(main_router)


def get_app(app_name: str) -> FastAPI:
    application = FastAPI(
        title=app_name,
        root_path=app_settings.ROOT_PATH,
        debug=app_settings.DEBUG,
        lifespan=lifespan,
    )
    setup_middlewares(application)
    setup_routers(application)
    return application


app = get_app(app_name=app_settings.SERVICE_NAME)