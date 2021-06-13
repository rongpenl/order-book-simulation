from abc import abstractmethod
import random
from kafka import KafkaProducer
from aiokafka import AIOKafkaConsumer
from config import PORT
import json
import asyncio
import time

random.seed(1)

class Bot:
    def __init__(self, kafka_prod_config: dict = {"bootstrap_servers": 'localhost:9092',
                                                  "value_serializer": lambda v: json.dumps(
                                                      v).encode('utf-8'),
                                                  "acks": 'all'},
                 kafka_consumer_config: dict = {
                     "bootstrap_servers": 'localhost:9092',
                     "value_deserializer": lambda v: json.loads(
                         v.decode("utf-8")),
    },
            order_topic: str = "incoming-order",
            notification_topic: str = "fulfill-notification",
            price_topic: str = "current-price"):

        self.order_producer = None
        self.broadcast_consumer = None
        self.notification_consumer = None
        self.kafka_consumer_config = kafka_consumer_config
        self.kafka_prod_config = kafka_prod_config
        self.order_topic = order_topic
        self.notification_topic = notification_topic
        self.price_topic = price_topic
        self.data = {}  # store any data you want in order to perform trading

    @abstractmethod
    def run(self):
        pass


class NaiveBot(Bot):
    
    def __init__(self, limit_only: bool = False, time_offset: float = 1):
        super(NaiveBot, self).__init__()
        self.limit_only = limit_only
        self.time_offset = time_offset

    def run(self):
        """
        A naive bot will periodically pull from the current-price topic, 
        generate a price following normal distribution centered at the current price with a standard deviation of 5.
        generate the size a random int between 10 and 100
        If the price is at least 2.5 greater/less than last trading price, send the order as a limit sell/buy order.
        Otherwise, randomly pick buy and sell, send as a market order.

        The bot regularly check order fulfill status but ignores the information it contains.

        A naive bot trades until being halted explicitly. It is going to submit a lot of nonsense orders that lose money.
        """

        # a Naive cold start
        self.data["current_price"] = 100

        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.submit_order())
        asyncio.ensure_future(self.obtain_notification())
        asyncio.ensure_future(self.receive_broadcast())
        loop.run_forever()

    async def submit_order(self):
        self.order_producer = KafkaProducer(**self.kafka_prod_config)
        i, BURNIN = 0, 100
        while True:
            i += 1 # first 100 orders must be limit order
            order_size = random.randint(10, 100)
            order_timestamp = int(time.time()*6)
            order_id = id(self) + id(order_timestamp)

            order_price = int(random.normalvariate(
                mu=self.data["current_price"], sigma=5))
            if abs(order_price - self.data["current_price"]) > 2.5:
                order_direction = "buy" if order_price - \
                    self.data["current_price"] < 0 else "sell"
                order_type = "limit"
            else:
                order_direction = "buy" if random.random() > 0.5 else "sell"
                order_type = "limit" if (i < BURNIN or self.limit_only or random.random() > 0.5) else "market"
                order_size = int(order_size * 0.5) # mimic small order around current price

            order_struct = {
                "order_id": order_id,
                "order_type": order_type,
                "order_size": order_size,
                "order_price": order_price if order_type == "limit" else None,
                "order_direction": order_direction,
                "submit_timestamp": order_timestamp,
            }
            self.order_producer.send(topic=self.order_topic, value=order_struct)
            self.order_producer.flush()
            await asyncio.sleep(self.time_offset)

    async def obtain_notification(self):
        self.notification_consumer = AIOKafkaConsumer(self.notification_topic,
                                                   **self.kafka_consumer_config)
        await self.notification_consumer.start()
        try:
            async for msg in self.notification_consumer:
                notification = msg.value
                # print("fulfill notification: {}".format(notification), "\n")
                await asyncio.sleep(0)
        finally:
            await self.notification_consumer.stop()

    async def receive_broadcast(self):
        self.broadcast_consumer = AIOKafkaConsumer(self.price_topic,
                                                **self.kafka_consumer_config)
        await self.broadcast_consumer.start()
        try:
            async for msg in self.broadcast_consumer:
                feed = msg.value
                # if feed.get("level_2", {}) == {}:
                #     print("price broadcast : {}".format(feed), "\n")
                self.data["current_price"] = feed.get("fill_price", 100)
                print(self.data["current_price"])
                await asyncio.sleep(0)
        finally:
            await self.broadcast_consumer.stop()


if __name__ == "__main__":
    NaiveBot(limit_only=False, time_offset= 0.01).run()
