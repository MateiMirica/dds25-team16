from confluent_kafka import Producer, Consumer
import threading
import redis
import json
from msgspec import msgpack, Struct

class StockDBError(Exception):
    """Custom exception for db errors."""

class StockTransactionError(Exception):
    """Custom exception for stock transaction errors."""
    
class StockValue(Struct):
    stock: int
    price: int

class StockWorker():
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

    def start_transaction_consumer_thread(self):
        consumer = self.create_kafka_consumer("UpdateStock")
        thread = threading.Thread(target=self.consume_messages, args=(consumer,self.performTransaction,))
        thread.daemon = True
        thread.start()
    
    def start_rollback_consumer_thread(self):
        consumer = self.create_kafka_consumer("RollbackStock")
        thread = threading.Thread(target=self.consume_messages, args=(consumer,self.performRollback,))
        thread.daemon = True
        thread.start()


    def get_item_from_db(self, item_id: str) -> StockValue | None:
        # get serialized data
        try:
            entry: bytes = self.db.get(item_id)
        except redis.exceptions.RedisError:
            raise StockDBError()
        # deserialize data if it exists else return null
        entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
        return entry

    def stockSuccess(self, orderId):
        data = {'orderId': orderId, 'status': True} 
        self.send("ResponseStock", json.dumps(data))
    
    def stockFailed(self, orderId):
        data = {'orderId': orderId, 'status': True} 
        self.send("ResponseStock", json.dumps(data))

    def performTransaction(self, msg):
        orderId, items = msg["orderId"], msg["items"]
        try:
            t = self.multiItemSubInMemory(items)
            self.db.mset(t)
        except:
            self.stockFailed(orderId)
            return
        self.logger.debug(f"Stock substraction succesfull for order {orderId}")
        self.stockSuccess(orderId)        
    
    def performRollback(self, msg):
        items = msg["items"]
        try:
            t = self.multiItemAddInMemory(items)
            self.db.mset(t)
        except:
            self.send("RollbackStock", json.dumps(msg))
            return
        self.logger.debug("Rollback successful")

    def multiItemSubInMemory(self, items):
        """Might throw when accessing DB"""
        in_mem_transaction = dict()
        for item_id, amount in items.items():
            item_entry: StockValue = self.get_item_from_db(item_id)
            if item_entry == None or item_entry.stock < int(amount):
                raise StockTransactionError()
            in_mem_transaction.put(item_id, item_entry.stock - int(amount))
        
        return in_mem_transaction
    
    def multiItemAddInMemory(self, items):
        """Might throw when accessing DB"""
        in_mem_transaction = dict()
        for item_id, amount in items.items():
            item_entry: StockValue = self.get_item_from_db(item_id)
            if item_entry == None:
                raise StockTransactionError()
            in_mem_transaction.put(item_id, item_entry.stock + int(amount))
        
        return in_mem_transaction
