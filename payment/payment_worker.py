import redis
import json
from msgspec import msgpack, Struct
from faststream.kafka import KafkaMessage

class PaymentDBError(Exception):
    """Custom exception for db errors."""
    
class UserValue(Struct):
    credit: int

class PaymentWorker():
    def __init__(self, logger, db, router):
        self.logger = logger
        self.db = db
        self.router = router
        self.update_subscriber = self.router.subscriber("UpdatePayment", group_id="payment_workers")
        self.rollback_subscriber = self.router.subscriber("RollbackPayment", group_id="payment_workers")
        self.update_subscriber(self.consume_update)
        self.rollback_subscriber(self.consume_rollback)
        self.transactions_failed = set()


        self.transaction_lua_script = self.db.register_script("""
        local userId = KEYS[1]
        local amount = tonumber(ARGV[1])

        local user_data = redis.call("GET", userId)
        if not user_data then
            return "USER_NOT_FOUND"
        end

        local user = cmsgpack.unpack(user_data)
        if user.credit < amount then
            return "INSUFFICIENT_FUNDS"
        end

        user.credit = user.credit - amount
        redis.call("SET", userId, cmsgpack.pack(user))
        return "SUCCESS"
        """)

        self.rollback_lua_script = self.db.register_script("""
        local userId = KEYS[1]
        local amount = tonumber(ARGV[1])

        local user_data = redis.call("GET", userId)
        if not user_data then
            return "USER_NOT_FOUND"
        end

        local user = cmsgpack.unpack(user_data)

        user.credit = user.credit + amount

        redis.call("SET", userId, cmsgpack.pack(user))
        return "SUCCESS"
        """)

    def consume_update(self, msg: str):
        msg = json.loads(msg)
        return self.performTransaction(msg)

    def consume_rollback(self, msg: str):
        msg = json.loads(msg)
        self.performRollback(msg)

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
        return json.dumps(data)
    
    def paymentFailed(self, orderId):
        data = {'orderId': orderId, 'status': False}
        return json.dumps(data)

    def performTransaction(self, msg):
        orderId, userId, amount = msg["orderId"], msg["userId"], msg["amount"]
        self.logger.debug(f"Removing {amount} credit from user: {userId}")

        try:
            result = self.transaction_lua_script(keys=[userId], args=[amount])
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis Error: {str(e)}")
            return self.paymentFailed(orderId)

        # Handle different cases
        if result == b"USER_NOT_FOUND":
            self.transactions_failed.add(orderId)
            return self.paymentFailed(orderId)
        elif result == b"INSUFFICIENT_FUNDS":
            self.transactions_failed.add(orderId)
            self.logger.info(f"Not enough balance for user {userId}")
            return self.paymentFailed(orderId)
        elif result == b"SUCCESS":
            self.logger.info(f"Payment successful for order {orderId}")
            return self.paymentSuccess(orderId)

    def performRollback(self, msg):
        if(msg["orderId"] in self.transactions_failed):
            self.logger.error(f"Transaction for user {msg['userId']} already failed.")
            self.transactions_failed.remove(msg["orderId"])
            return

        userId, amount = msg["userId"], msg["amount"]
        self.logger.debug(f"Adding {amount} credit to user: {userId}")

        try:
            result = self.rollback_lua_script(keys=[userId], args=[amount])
        except redis.exceptions.RedisError as e:
            self.logger.error(f"Redis Error: {str(e)}")
            return

        if result == "USER_NOT_FOUND":
            self.logger.error(f"Rollback failed: No user with id {userId}")
            return

        self.logger.info(f"Payment rollback successful for user {userId}")
