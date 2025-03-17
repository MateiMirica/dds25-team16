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
    def __init__(self, logger, db, router):
        self.logger = logger
        self.db = db
        self.router = router
        self.setup_subscriber()

    def setup_subscriber(self):
        @self.router.subscriber("UpdateStock")
        async def consume_update(msg: str):
            msg = json.loads(msg)
            return self.performTransaction(msg)

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
        return json.dumps(data)
    
    def stockFailed(self, orderId):
        data = {'orderId': orderId, 'status': False}
        return json.dumps(data)

    def performTransaction(self, msg):
        orderId, items = msg["orderId"], msg["items"]
        try:
            t = self.multiItemSubInMemory(items)
            self.db.mset(t)
        except Exception as e:
            self.logger.info(str(e))
            return self.stockFailed(orderId)
        self.logger.info(f"Stock substraction succesfull for order {orderId}")
        return self.stockSuccess(orderId)

    def multiItemSubInMemory(self, items):
        """Might throw when accessing DB"""
        in_mem_transaction = dict()
        for item_id, amount in items.items():
            item_entry: StockValue = self.get_item_from_db(item_id)
            if item_entry == None or item_entry.stock < int(amount):
                raise StockTransactionError()
            in_mem_transaction[item_id] = msgpack.encode(StockValue(item_entry.stock - int(amount), item_entry.price))
        
        return in_mem_transaction
    
    def multiItemAddInMemory(self, items):
        """Might throw when accessing DB"""
        in_mem_transaction = dict()
        for item_id, amount in items.items():
            item_entry: StockValue = self.get_item_from_db(item_id)
            if item_entry == None:
                raise StockTransactionError()
            in_mem_transaction[item_id] = msgpack.encode(StockValue(item_entry.stock + int(amount), item_entry.price))
        
        return in_mem_transaction
