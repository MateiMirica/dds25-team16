from confluent_kafka import Producer, Consumer
import threading
import redis
import json
from msgspec import msgpack, Struct
from collections import defaultdict
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError


class OrderDBError(Exception):
    """Custom exception for db errors."""

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

class OrderWorker():
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db
        self.logger.info("here!!!!")

        all_topics = ['ResponseStock', 'ResponsePayment', 'RollbackStock', 'RollbackPayment', 'UpdateStock', 'UpdatePayment']
        for topic in all_topics:
            self.create_topic(topic)

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
        print("Creating topics...")

        for t, future in futures.items():
            try:
                future.result()
                print(f"Topic {t} created successfully.")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"Topic {t} already exists. Skipping creation.")
                else:
                    raise

    def start_payment_response_consumer_thread(self):
        consumer = self.create_kafka_consumer("ResponsePayment")
        thread = threading.Thread(target=self.consume_messages, args=(consumer, self.process_response_payment,))
        thread.daemon = True
        thread.start()

    def process_response_stock(self, status):
        if status["status"] is True:
            self.logger.info("Stock substraction successful")
            order_id = status["orderId"]
            # can be avoided if payment details are included in stock update
            order_entry: OrderValue = self.get_order_from_db(order_id)
            payment = dict()
            payment["orderId"] = order_id
            payment["userId"] = order_entry.user_id
            payment["amount"] = order_entry.total_cost
            self.send('UpdatePayment', json.dumps(payment))
        else:
            self.logger.info("Stock substraction failed")

    def process_response_payment(self, status):
        order_id = status["orderId"]
        order_entry: OrderValue = self.get_order_from_db(order_id)
        if status["status"] is True:
            order_entry.paid = True
            self.db.set(order_id, msgpack.encode(order_entry))
            self.logger.info(f"order {order_id} checkout successful")
        else:
            self.logger.info("Payment failed, attempting stock rollback...")
            items_quantities = self.get_items_in_order(order_entry)
            self.rollback_stock(items_quantities)

    def get_items_in_order(self, order_entry: OrderValue):
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        return items_quantities

    def checkout(self, order_id):
        self.logger.debug(f"Checking out {order_id}")
        order_entry: OrderValue = self.get_order_from_db(order_id)
        # get the quantity per item
        items_quantities = self.get_items_in_order(order_entry)
        items = dict()
        items["orderId"] = order_id
        items["items"] = items_quantities
        self.send('UpdateStock', json.dumps(items))

    def get_order_from_db(self, order_id: str) -> OrderValue | None:
        try:
            # get serialized data
            entry: bytes = self.db.get(order_id)
        except redis.exceptions.RedisError:
            return OrderDBError("can't reach Redis")
        # deserialize data if it exists else return null
        entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
        if entry is None:
            # if order does not exist in the database
            raise OrderDBError("can't reach Redis")
        return entry

    def rollback_stock(self, removed_items):
        items = dict()
        items["items"] = removed_items
        self.send('RollbackStock', json.dumps(items))

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


