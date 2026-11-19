import json

from confluent_kafka import Producer
from django.conf import settings
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer
# from confluent_kafka.serialization import (
#     SerializationContext,
#     MessageField,
# )
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)


# def workout_event_to_dict(obj: dict, ctx) -> dict:
#     return obj

# class WorkoutEvent(BaseModel):
#     user_id: int
#     event: str
#     type: str
#     distance: float | None = None

# WORKOUT_EVENT_SCHEMA = {
#     "type": "record",
#     "name": "WorkoutEvent",
#     "namespace": "fitnesstracker",
#     "fields": [
#         {"name": "user_id",  "type": "int"},
#         {"name": "event",    "type": "string"},
#         {"name": "type",     "type": "string"},
#         {"name": "distance", "type": ["null", "float"], "default": None},
#     ]
# }

class KafkaProducer:
    def __init__(self, config: dict):
        # schema_registry_client = SchemaRegistryClient({
        #     "url": config["SCHEMA_REGISTRY_URL"],
        # })                                                      # клиент к http://schema-registry:8081 (отправляет схему, получает id. схемы)
        # # with open(config["AVRO_SCHEMA_PATH"]) as f:
        # #     schema_str = f.read()
        # schema_str = json.dumps(WORKOUT_EVENT_SCHEMA)
        #                                                         # schema_str = WorkoutEvent.schema_json()
        # self.avro_serializer = AvroSerializer(                  # при первом вызове регистрирует схему и получает id
        #     schema_registry_client,
        #     schema_str,                                         # строка с описанием json структуры
        #     lambda obj, ctx: obj.model_dump(),                  # avro_serializer принимает только dict
        # )

        producer_conf = {"bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"]}
        if config.get("KAFKA_SECURITY_PROTOCOL") not in (None, "PLAINTEXT"):
            producer_conf["security.protocol"] = config["KAFKA_SECURITY_PROTOCOL"]
        if config.get("KAFKA_SASL_MECHANISM"):
            producer_conf.update(
                sasl_mechanism=config["KAFKA_SASL_MECHANISM"],
                sasl_username=config.get("KAFKA_SASL_USERNAME", ""),
                sasl_password=config.get("KAFKA_SASL_PASSWORD", ""),
            )
        self.producer = Producer(producer_conf)
        self.topic = config["KAFKA_TOPIC"]

    def send(self, key: str, value: dict):
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json")
        self.producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            # value=self.avro_serializer(                           # получение id, перевод в dict данных, валидация и сериализация через fastavro, собирается Confluent wire format
            #     value,
            #     SerializationContext(self.topic, MessageField.VALUE),
            # ),
            value=json.dumps(value, ensure_ascii=False).encode("utf-8"),   # простая JSON-сериализация, без Avro и Schema Registry
            callback=self.delivery_report,
        )
        self.producer.poll(0)

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(f"Delivered to {msg.topic()} [{msg.partition()}]")

    def flush(self):
        self.producer.flush()


_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer({
            "KAFKA_BOOTSTRAP_SERVERS": settings.KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": settings.KAFKA_TOPIC,
            "KAFKA_SECURITY_PROTOCOL": settings.KAFKA_SECURITY_PROTOCOL,
            "KAFKA_SASL_MECHANISM": settings.KAFKA_SASL_MECHANISM,
            "KAFKA_SASL_USERNAME": settings.KAFKA_SASL_USERNAME,
            "KAFKA_SASL_PASSWORD": settings.KAFKA_SASL_PASSWORD,
            "SCHEMA_REGISTRY_URL": settings.SCHEMA_REGISTRY_URL,
            "AVRO_SCHEMA_PATH": settings.AVRO_SCHEMA_PATH,
        })
    return _producer