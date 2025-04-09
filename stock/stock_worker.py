import time

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
        self.update_subscriber = self.router.subscriber("UpdateStock", group_id="stock_workers")
        self.rollback_subscriber = self.router.subscriber("RollbackStock", group_id="stock_workers")
        self.update_subscriber(self.consume_update)
        self.rollback_subscriber(self.consume_rollback)

        self.transaction_lua_script = self.db.register_script(
            """
            local n = #KEYS
            for i = 1, n do
                local key = KEYS[i]
                local amount = tonumber(ARGV[i])
                local data = redis.call("GET", key)
                if not data then
                    redis.call("SET", "order:" .. tostring(ARGV[n+1]), cmsgpack.pack("REJECTED"))
                    return "ITEM_NOT_FOUND"
                end
                local item = cmsgpack.unpack(data)
                if item.stock < amount then
                    redis.call("SET", "order:" .. tostring(ARGV[n+1]), cmsgpack.pack("REJECTED"))
                    return "INSUFFICIENT_STOCK"
                end
            end
            local order_data = redis.call("GET", "order:" .. ARGV[n+1])
            if order_data ~= nil and order_data == cmsgpack.pack("PAID") then
                return "SUCCESS"
            end
            if order_data ~= nil and order_data == cmsgpack.pack("REJECTED") then
                return "INSUFFICIENT_STOCK"
            end
            for i = 1, n do
                local key = KEYS[i]
                local amount = tonumber(ARGV[i])
                local data = redis.call("GET", key)
                local item = cmsgpack.unpack(data)
                item.stock = item.stock - amount
                redis.call("SET", key, cmsgpack.pack(item))
            end
            redis.call("SET", "order:" .. tostring(ARGV[n+1]), cmsgpack.pack("PAID"))
            return "SUCCESS"
            """
        )

        self.rollback_lua_script = self.db.register_script(
            """
            local n = #KEYS
            for i = 1, n do
                local key = KEYS[i]
                local data = redis.call("GET", key)
                if not data then
                    redis.call("SET", "order:" .. ARGV[n+1], cmsgpack.pack("REJECTED"))
                    return "ITEM_NOT_FOUND"
                end
            end
            local order_data = redis.call("GET", "order:" .. ARGV[n+1])
            if order_data ~= nil and order_data == cmsgpack.pack("ROLLEDBACK") then
                return "SUCCESS"
            end
            for i = 1, n do
                local key = KEYS[i]
                local amount = tonumber(ARGV[i])
                local data = redis.call("GET", key)
                local item = cmsgpack.unpack(data)
                item.stock = item.stock + amount
                redis.call("SET", key, cmsgpack.pack(item))
            end
            redis.call("SET", "order:" .. ARGV[n+1], cmsgpack.pack("ROLLEDBACK"))
            return "SUCCESS"
            """
        )

    def consume_rollback(self, msg: str):
        msg = json.loads(msg)
        self.rollbackTransaction(msg)

    def consume_update(self, msg: str):
        msg = json.loads(msg)
        return self.performTransaction(msg)

    def stockSuccess(self, msg):
        msg["status"] = True
        return json.dumps(msg)

    def stockFailed(self, msg):
        msg["status"] = False
        return json.dumps(msg)
    
    def rollbackTransaction(self, msg):
        orderId, items = msg["orderId"], msg["items"]
        keys = []
        args = []
        for item_id, amount in items.items():
            keys.append(item_id)
            args.append(str(amount))
        args.append(orderId)
        self.logger.debug(f"Attempting to rollback stock for order {orderId} on items: {items}")
        # try:
        #     result = self.rollback_lua_script(keys=keys, args=args)
        # except redis.exceptions.RedisError as e:
        #
        #     self.logger.error(f"Redis Error: {str(e)}")
        #     return
        result = None
        exception = False
        for i in range(10):
            time.sleep(5)
            try:
                result = self.rollback_lua_script(keys=keys, args=args)
                exception = False
                break
            except redis.exceptions.RedisError as e:
                self.logger.error(f"Redis Error: {str(e)}")
                exception = True
        if exception:
            return
        if result == b"ITEM_NOT_FOUND":
            self.logger.error("One or more items were not found during stock update.")
            return 
        else:
            self.logger.info(f"Stock subtraction successful for order {orderId}")
            return 

    def performTransaction(self, msg):
        orderId, items = msg["orderId"], msg["items"]
        keys = []
        args = []
        for item_id, amount in items.items():
            keys.append(item_id)
            args.append(str(amount))
        args.append(orderId)
        self.logger.debug(f"Attempting to subtract stock for order {orderId} on items: {items}")
        try:
            result = self.transaction_lua_script(keys=keys, args=args)
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis Error: {str(e)}")
            return self.stockFailed(msg)

        if result == b"ITEM_NOT_FOUND":
            self.logger.error("One or more items were not found during stock update.")
            return self.stockFailed(msg)
        elif result == b"INSUFFICIENT_STOCK":
            self.logger.info("Insufficient stock available for one or more items.")
            return self.stockFailed(msg)
        elif result == b"SUCCESS":
            self.logger.info(f"Stock subtraction successful for order {orderId}")
            return self.stockSuccess(msg)

        return self.stockFailed(msg)
