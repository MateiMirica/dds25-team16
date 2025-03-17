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

        self.transaction_lua_script = self.db.register_script(
            """
            local n = #KEYS
            for i = 1, n do
                local key = KEYS[i]
                local amount = tonumber(ARGV[i])
                local data = redis.call("GET", key)
                if not data then
                    return "ITEM_NOT_FOUND"
                end
                local item = cmsgpack.unpack(data)
                if item.stock < amount then
                    return "INSUFFICIENT_STOCK"
                end
            end
            for i = 1, n do
                local key = KEYS[i]
                local amount = tonumber(ARGV[i])
                local data = redis.call("GET", key)
                local item = cmsgpack.unpack(data)
                item.stock = item.stock - amount
                redis.call("SET", key, cmsgpack.pack(item))
            end
            return "SUCCESS"
            """
        )

    def setup_subscriber(self):
        @self.router.subscriber("UpdateStock")
        async def consume_update(msg: str):
            msg = json.loads(msg)
            return self.performTransaction(msg)

    def stockSuccess(self, orderId):
        data = {'orderId': orderId, 'status': True}
        return json.dumps(data)

    def stockFailed(self, orderId):
        data = {'orderId': orderId, 'status': False}
        return json.dumps(data)

    def performTransaction(self, msg):
        orderId, items = msg["orderId"], msg["items"]
        keys = []
        args = []
        for item_id, amount in items.items():
            keys.append(item_id)
            args.append(str(amount))

        self.logger.debug(f"Attempting to subtract stock for order {orderId} on items: {items}")
        try:
            result = self.transaction_lua_script(keys=keys, args=args)
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis Error: {str(e)}")
            return self.stockFailed(orderId)

        if result == b"ITEM_NOT_FOUND":
            self.logger.error("One or more items were not found during stock update.")
            return self.stockFailed(orderId)
        elif result == b"INSUFFICIENT_STOCK":
            self.logger.info("Insufficient stock available for one or more items.")
            return self.stockFailed(orderId)
        elif result == b"SUCCESS":
            self.logger.info(f"Stock subtraction successful for order {orderId}")
            return self.stockSuccess(orderId)

        return self.stockFailed(orderId)
