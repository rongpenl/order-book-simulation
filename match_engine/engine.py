# https://stackoverflow.com/questions/64113913/how-to-solve-circular-reference-detected
from collections import defaultdict
import json
from kafka import KafkaProducer
from aiokafka import AIOKafkaConsumer
from config import PORT
import asyncio
import time


class Engine:

    def __init__(self, kafka_prod_config: dict = {
        "bootstrap_servers": 'localhost:9092',
        "value_serializer": lambda v: json.dumps(
            v).encode('utf-8'),
        "acks": 'all'},
        kafka_consumer_config: dict = {
        "bootstrap_servers": 'localhost:9092',
        "value_deserializer": lambda v: json.loads(
            v.decode("utf-8"))},
            order_topic: str = "incoming-order",
            notification_topic: str = "fulfill-notification",
            price_topic: str = "current-price"):
        self.broadcast_producer = KafkaProducer(**kafka_prod_config)
        self.notification_producer = KafkaProducer(**kafka_prod_config)
        self.order_consumer = None

        self.kafka_consumer_config = kafka_consumer_config
        self.kafka_prod_config = kafka_prod_config
        self.order_topic = order_topic
        self.notification_topic = notification_topic
        self.price_topic = price_topic
        self.data = {}
        def dd_list():
            return list()
        # how to serialize a defaultdict
        # https://stackoverflow.com/questions/16439301/cant-pickle-defaultdict/16439531#16439531        
        self.data["limit_orderbook"] = {
            "buy": defaultdict(dd_list), "sell": defaultdict(dd_list)}
        self.data["latest_feed"] = {}
        self.data["market_orderbook"] = {"buy": [], "sell": []}

    def run(self):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.listen_orders())
        asyncio.ensure_future(self.broadcast_price_async())
        loop.run_forever()

    async def listen_orders(self):
        self.order_consumer = AIOKafkaConsumer(
            self.order_topic, **self.kafka_consumer_config)

        await self.order_consumer.start()
        try:
            async for msg in self.order_consumer:
                order = msg.value
                if order["order_type"] == "limit":
                    if order["order_direction"] == "buy":
                        self.data["limit_orderbook"]["buy"][order["order_price"]].append(
                            order)
                    else:
                        self.data["limit_orderbook"]["sell"][order["order_price"]].append(
                            order)
                else:
                    if order["order_type"] == "buy":
                        self.data["market_orderbook"]["buy"].append(order)
                    else:
                        self.data["market_orderbook"]["sell"].append(order)
                self.data["latest_order"] = order
                self.fulfill_orders()
                self.print_orderbook()
        finally:
            await self.order_consumer.stop()

    def fulfill_orders(self):
        """
        # When this method runs, there should be only one of the following three cases possible.
        1. There is one and only one market order that can be executed with the tip of the opposite side
        2. There is no market order
            2.1 There is no matching opposite orders
            2.2 The latest limited order can be executed by fulfilling the orders 
                on the opposite side one by one

        The three cases are mutually exclusive so we will handle that one by one.
        """
        self.fulfill_market_orders()
        self.fulfill_limit_orders()

    def fulfill_market_orders(self):
        pass

    def fulfill_limit_orders(self):
        self.clean_limit_orderbook()
        latest_order = self.data["latest_order"].copy()
        
        # print(latest_order)
        
        opposite_orderbook = self.data["limit_orderbook"]["buy"] if latest_order[
            "order_direction"] == "sell" else self.data["limit_orderbook"]["sell"]

        if latest_order["order_direction"] == "buy":
            opposite_prices = sorted(opposite_orderbook.keys())
        else:
            opposite_prices = sorted(opposite_orderbook.keys(), reverse=True)
        
        # print(opposite_prices)

        for opposite_price in opposite_prices:
            
            valid_buy = latest_order["order_direction"] == "buy" and opposite_price <= latest_order["order_price"]
            
            valid_sell = latest_order["order_direction"] == "sell" and opposite_price >= latest_order["order_price"]
            
            valid = valid_buy or valid_sell
            
            if not valid:
                self.clean_limit_orderbook()
                break
            else:
                # print("possible trade found.")
                for queued_order in opposite_orderbook[opposite_price]:
                    if latest_order["order_size"] == 0:
                        break
                    
                    if queued_order["order_size"] <= latest_order["order_size"]:
                        # the latest order will eat all the queued order
                        
                        latest_order["order_size"] -= queued_order["order_size"]
                        fill_size = queued_order["order_size"]
                        queued_order["order_size"] = 0
                        
                        # notify fulfill information about latest order
                        latest_order_notification = self.order_to_notification(
                            order=latest_order, partial=latest_order["order_size"] > 0, fill_size=fill_size, fill_price=opposite_price)
                        self.notify_fulfill(latest_order_notification)
                        # notify fulfill information for fully filled orders
                        queued_order_notification = self.order_to_notification(
                            order=queued_order, partial=False, fill_size=fill_size, fill_price=opposite_price)
                        self.notify_fulfill(queued_order_notification)
                    else:
                        # the latest order is not big enough to eat the current queued order
                        queued_order["order_size"] -= latest_order["order_size"]
                        fill_size = latest_order["order_size"]
                        
                        # mark the latest_order completely fulfilled.
                        latest_order["order_size"] = 0
                        
                        # the latest order will always be the last one.
                        try:
                            if latest_order["order_direction"] == "buy":
                                self.data["limit_orderbook"]["buy"][self.data["latest_order"]["order_price"]][-1]["order_size"] = 0
                            else:
                                self.data["limit_orderbook"]["sell"][self.data["latest_order"]["order_price"]][-1]["order_size"] = 0
                        except:
                            print(" latest order not found!")
                            exit()                           
                        
                        # notify partial fulfill about latest order
                        latest_order_notification = self.order_to_notification(
                            order=latest_order, partial=False, fill_size=fill_size, fill_price=opposite_price)
                        self.notify_fulfill(latest_order_notification)
                        
                        # notify partial fulfill of queued order
                        queued_order_notification = self.order_to_notification(
                            order=queued_order, partial=True, fill_size=fill_size, fill_price=opposite_price)
                        self.notify_fulfill(queued_order_notification)
                        
                            # edge case that cancellation happens
                    # broadcast the fullfill information
                    fill_timestamp = int(time.time()*6)
                    feed = {
                        "exchange_id": id(self) + id(fill_timestamp),
                        "fill_price": opposite_price,
                        "fill_size": fill_size,
                        "fill_timestamp": fill_timestamp,
                        "level_2": {}
                    }
                    self.broadcast_price(feed=feed)
                
                # broadcast the order depth information after an order is processed
                feed["level_2"] = self.data["limit_orderbook"]
                self.broadcast_price(feed=feed)
                self.data["latest_feed"] = feed
                self.clean_limit_orderbook()
                
    def print_orderbook(self):
        print("-------------------Orderbook-------------------")
        for price in sorted(self.data["limit_orderbook"]["sell"].keys(), reverse= True):
            depth = sum(map(lambda order: order["order_size"], self.data["limit_orderbook"]["sell"][price]))
            print("sell side price: {}, depth: {}".format(price, depth))
        print("-----------------------------------------------")
        for price in sorted(self.data["limit_orderbook"]["buy"].keys(),reverse= True):
            depth = sum(map(lambda order: order["order_size"], self.data["limit_orderbook"]["buy"][price]))
            print("buy side price: {}, depth: {}".format(price, depth))
        print("\n\n")


    def clean_limit_orderbook(self):
        """
        Remove useless keys in the limit orderbook
        """
        for orderbook in [self.data["limit_orderbook"]["buy"], self.data["limit_orderbook"]["sell"]]:
            empty_prices = []
            for price in orderbook.keys():
                new_orders = list(filter(lambda order: order["order_size"] > 0, orderbook[price]))
                if len(new_orders) == 0:
                    empty_prices.append(price)
                else:
                    orderbook[price] = new_orders
            for price in empty_prices:
                del orderbook[price]

    def order_to_notification(self, order: dict, partial: bool, fill_size: int, fill_price: float):
        notification = {
            "order_id": order["order_id"],
            "fill_price": fill_price,
            "fill_size": fill_size,
            "partial_fill": partial,
            "received_timestamp": order["submit_timestamp"],
            "fill_timestamp": int(time.time()*6),
        }
        return notification

    def broadcast_price(self, feed: dict):
        print(feed)
        self.broadcast_producer.send(self.price_topic, feed)
        self.broadcast_producer.flush()

    async def broadcast_price_async(self):
        while True:
            if self.data["latest_feed"] != None:
                self.broadcast_producer.send(
                    self.price_topic, self.data["latest_feed"])
                self.broadcast_producer.flush()
            await asyncio.sleep(1)  # heartbeat broadcast

    def notify_fulfill(self, notification: dict):
        print(notification)
        self.notification_producer.send(topic = self.notification_topic, value = notification)


if __name__ == "__main__":
    Engine().run()
