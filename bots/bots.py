from abc import abstractmethod
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from config import PORT
import json
import asyncio


class Bot:

    def __init__(self,
                 kafka_prod_config: dict = {"bootstrap_servers": 'localhost:9092',
                                            "value_serializer": lambda v: json.dumps(
                                                v).encode('utf-8'),
                                            "acks": 'all'},
                 kafka_consumer_config: dict = {
                     "bootstrap_servers": 'localhost:9092',
                     "value_deserializer": lambda v: json.loads(
                         v).decode('utf-8'),
                 },
                 order_topic: str = "incoming_order",
                 notification_topic: str = "fulfill-notification",
                 price_topic: str = "current-price"):

        self.producer = KafkaProducer(**kafka_prod_config)
        self.consumer = KafkaConsumer(**kafka_consumer_config)
        self.order_topic = order_topic
        self.notification_topic = notification_topic
        self.price_topic = price_topic
        self.data = {}  # store any data you want in order to perform trading

    @abstractmethod
    async def run(self):
        pass


class NaiveBot(Bot):

    def __init__(self):
        super().__init__(self)

    async def run(self):
        """
        A naive bot will periodically pull from the current-price topic, 
        generate a price following normal distribution centered at the current price with a standard deviation of 5.
        generate the size a random int between 10 and 100
        If the price is at least 2.5 greater/less than last trading price, send the order as a limit sell/buy order.
        Otherwise, randomly pick buy and sell, send as a market order.
        
        The bot regularly check order fulfill status but ignores the information it contains.
        
        A naive bot trades until being halted explicitly
        """
        for 
