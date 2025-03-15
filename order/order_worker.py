import asyncio
import time

from confluent_kafka import Producer, Consumer
import threading
import redis
import json
from msgspec import msgpack, Struct
from collections import defaultdict
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

from errors import TimeoutException


class OrderDBError(Exception):
    """Custom exception for db errors."""

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    status: str
    user_id: str
    total_cost: int

class OrderWorker():
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db
        self.logger.info("here!!!!")
        self.pending_orders = {}
        all_topics = ['ResponseStock', 'ResponsePayment', 'RollbackStock', 'RollbackPayment', 'UpdateStock', 'UpdatePayment']
        # for topic in all_topics:
        #     self.create_topic(topic)

        self.producer = self.create_kafka_producer()

        self.start_stock_response_consumer_thread()
        self.start_payment_response_consumer_thread()


    def start_stock_response_consumer_thread(self):
        consumer = self.create_kafka_consumer("ResponseStock")
        thread = threading.Thread(target=self.consume_messages, args=(consumer, self.process_response_stock,))
        thread.daemon = True
        thread.start()

    def create_topic(self, topic_name):
        admin_client = AdminClient({"bootstrap.servers": "kafka:9092"})
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        futures = admin_client.create_topics([new_topic])

        self.logger.info("Creating topics")

        for t, future in futures.items():
            try:
                future.result()
                self.logger.info(f"Topic {t} created successfully.")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    self.logger.info(f"Topic {t} already exists. Skipping creation.")
                else:
                    raise

    def start_payment_response_consumer_thread(self):
        consumer = self.create_kafka_consumer("ResponsePayment")
        thread = threading.Thread(target=self.consume_messages, args=(consumer, self.process_response_payment,))
        thread.daemon = True
        thread.start()

    def create_message_and_send(self, topic: str, order_id: str, order_entry: OrderValue):
        items_quantities = self.get_items_in_order(order_entry)
        msg = dict()
        match topic:
            case 'UpdateStock':
                msg["orderId"] = order_id
                msg["items"] = items_quantities
            case 'UpdatePayment':
                msg["orderId"] = order_id
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
            case 'RollbackPayment':
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
            case 'RollbackStock':
                msg["items"] = items_quantities
        self.send(topic, json.dumps(msg))

    # Check every 0.001s if the order with order_id is completed
    async def await_order(self, order_id: str, timeout=5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if order_id not in self.pending_orders:
                return
            await asyncio.sleep(0.001)
        raise TimeoutException(f"timeout for order {order_id} reached")

    def fail_order(self, order_id):
        order_entry: OrderValue = self.get_order_from_db(order_id)
        order_entry.status = "failed"
        self.db.set(order_id, msgpack.encode(order_entry))

    def process_response_stock(self, status):
        order_id = status["orderId"]

        if status["status"] is True:
            self.logger.info("Stock substraction successful")
            order_entry: OrderValue = self.get_order_from_db(order_id)
            if order_entry.status != "pending":
                self.create_message_and_send('RollbackStock', order_id, order_entry)
                return
            self.create_message_and_send('UpdatePayment', order_id, order_entry)
        else:
            self.logger.info("Stock substraction failed")
            self.fail_order(order_id)
            self.pending_orders.pop(order_id, None)

    def process_response_payment(self, status):
        order_id = status["orderId"]
        order_entry: OrderValue = self.get_order_from_db(order_id)
        if status["status"] is True:
            if order_entry.status != "pending":
                self.create_message_and_send('RollbackStock', order_id, order_entry)
                self.create_message_and_send('RollbackPayment', order_id, order_entry)
                return

            order_entry.paid = True
            order_entry.status = "success"
            self.db.set(order_id, msgpack.encode(order_entry))
            self.logger.info(f"order {order_id} checkout successful")
        else:
            self.logger.info("Payment failed, attempting stock rollback...")
            self.fail_order(order_id)
            self.create_message_and_send('RollbackStock', order_id, order_entry)
        self.pending_orders.pop(order_id, None)

    def get_items_in_order(self, order_entry: OrderValue):
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        return items_quantities

    async def checkout(self, order_id: str):
        self.logger.debug(f"Checking out {order_id}")
        order_entry: OrderValue = self.get_order_from_db(order_id)
        self.create_message_and_send('UpdateStock', order_id, order_entry)
        self.pending_orders[order_id] = True

        await self.await_order(order_id)
        order: OrderValue = self.get_order_from_db(order_id)
        return order

    def get_order_from_db(self, order_id: str) -> OrderValue | None:
        try:
            # get serialized data
            entry: bytes = self.db.get(order_id)
        except redis.exceptions.RedisError:
            raise OrderDBError("can't reach Redis")
        # deserialize data if it exists else return null
        entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
        if entry is None:
            # if order does not exist in the database
            raise OrderDBError("can't reach Redis")
        return entry

    def create_kafka_producer(self):
        conf = {'bootstrap.servers': "kafka:9092"}
        producer = Producer(**conf)
        return producer

    def send(self, topic, data):
        self.producer.produce(topic, data)
        self.producer.flush()

    def create_kafka_consumer(self, topic):
        conf = {
            'bootstrap.servers': "kafka:9092",
            'group.id': "stocks",
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(**conf)
        consumer.subscribe([topic])
        return consumer

    def consume_messages(self, consumer, callback):
        while True:
            message = consumer.poll(0.1)
            if message is None:
                continue
            if message.error():
                self.logger.info(f"Consumer error: {message.error()}")
                continue

            self.logger.info(f"Received message: {message.value().decode('utf-8')}")
            try:
                msg = json.loads(message.value().decode('utf-8'))
                callback(msg)
            except json.JSONDecodeError:
                self.logger.debug(f"Malformed JSON: {msg.value().decode('utf-8')}")


