from confluent_kafka import Producer, Consumer
import threading
import redis
import json
from msgspec import msgpack, Struct

class PaymentDBError(Exception):
    """Custom exception for db errors."""
    
class UserValue(Struct):
    credit: int

class PaymentWorker():
    def __init__(self, logger, db):
        self.producer = self.create_kafka_producer()
        self.logger = logger
        self.db = db
        self.start_transaction_consumer_thread()
        self.start_rollback_consumer_thread()

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
            'group.id': "payments",
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

    def start_transaction_consumer_thread(self):
        consumer = self.create_kafka_consumer("UpdatePayment")
        thread = threading.Thread(target=self.consume_messages, args=(consumer,self.performTransaction,))
        thread.daemon = True
        thread.start()
    
    def start_rollback_consumer_thread(self):
        consumer = self.create_kafka_consumer("RollbackPayment")
        thread = threading.Thread(target=self.consume_messages, args=(consumer,self.performRollback,))
        thread.daemon = True
        thread.start()




    def get_user_from_db(self, user_id: str) -> UserValue | None:
        try:
            # get serialized data
            entry: bytes = self.db.get(user_id)
        except redis.exceptions.RedisError:
            raise PaymentDBError("can't reach Redis")
        # deserialize data if it exists else return null
        entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
        return entry

    def paymentSuccess(self, orderId):
        data = {'orderId': orderId, 'status': True} 
        self.send("ResponsePayment", json.dumps(data))
    
    def paymentFailed(self, orderId):
        data = {'orderId': orderId, 'status': False}
        self.send("ResponsePayment", json.dumps(data))

    def performTransaction(self, msg):
        """This method should be refactored. It currently does too much"""
        orderId, userId, amount = msg["orderId"], msg["userId"], msg["amount"]
        self.logger.debug(f"Removing {amount} credit from user: {userId}")
        try:
            user_entry: UserValue = self.get_user_from_db(userId)
            if user_entry == None: #no user with this key
                self.paymentFailed(orderId)
        except:
            self.paymentFailed(orderId)
            return 
        
        # update credit, serialize and update database
        user_entry.credit -= int(amount)
        if user_entry.credit < 0:
            self.logger.info(f"Not enough balance for user {userId}")
            self.paymentFailed(orderId)
            return
        try:
            self.db.set(userId, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            self.paymentFailed(orderId)
            return

        self.logger.info(f"Payment successful for order {orderId}")
        self.paymentSuccess(orderId)
    
    def performRollback(self, msg):
        userId, amount = msg["userId"], msg["amount"]
        self.logger.debug(f"Adding {amount} credit to user: {userId}")
        try:
            user_entry: UserValue = self.get_user_from_db(userId)
        except:
            self.send('RollbackPayment', json.dumps(msg)) # retry
            return
        if user_entry == None: #no user with this key
                raise Exception(f"No user with id {userId}")
        
        user_entry.credit += int(amount)
        try:
            self.db.set(userId, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            self.send('RollbackPayment', json.dumps(msg)) #retry
            return

        self.logger.info(f"Payment rollback successful for user {userId}")


