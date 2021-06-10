from config import PORT
import json
from kafka import KafkaConsumer, KafkaProducer


class Engine:
    def __init__(self, kafka_prod_config: dict = {
        "bootstrap_servers": 'localhost:9092',
        "value_serializer": lambda v: json.dumps(
            v).encode('utf-8'),
        "acks": 'all'},
        kafka_consumer_config: dict = {
        "bootstrap_servers": 'localhost:9092',
        "value_deserializer": lambda v: json.loads(
            v).decode('utf-8')},
            order_topic: str = "incoming-order",
            notification_topic: str = "fulfill-notification",
            price_topic: str = "current-price"):
        self.broadcast_producer = KafkaProducer(**kafka_prod_config)
