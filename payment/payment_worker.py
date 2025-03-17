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
        self.setup_subscriber()

    def setup_subscriber(self):
        @self.router.subscriber("UpdatePayment")
        async def consume_update(msg: str):
            msg = json.loads(msg)
            return self.performTransaction(msg)

        @self.router.subscriber("RollbackPayment")
        async def consume_rollback(msg: str):
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
        """This method should be refactored. It currently does too much"""
        orderId, userId, amount = msg["orderId"], msg["userId"], msg["amount"]
        self.logger.debug(f"Removing {amount} credit from user: {userId}")
        try:
            user_entry: UserValue = self.get_user_from_db(userId)
            if user_entry == None: #no user with this key
                return self.paymentFailed(orderId)
        except:
            return self.paymentFailed(orderId)

        # update credit, serialize and update database
        user_entry.credit -= int(amount)
        if user_entry.credit < 0:
            self.logger.info(f"Not enough balance for user {userId}")
            return self.paymentFailed(orderId)
        try:
            self.db.set(userId, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            return self.paymentFailed(orderId)

        self.logger.info(f"Payment successful for order {orderId}")
        return self.paymentSuccess(orderId)
    
    def performRollback(self, msg):
        userId, amount = msg["userId"], msg["amount"]
        self.logger.debug(f"Adding {amount} credit to user: {userId}")
        try:
            user_entry: UserValue = self.get_user_from_db(userId)
        except:
            # self.send('RollbackPayment', json.dumps(msg)) # retry
            return
        # if user_entry == None: #no user with this key
        #         raise Exception(f"No user with id {userId}")
        
        user_entry.credit += int(amount)
        try:
            self.db.set(userId, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            # self.send('RollbackPayment', json.dumps(msg)) #retry
            return

        self.logger.info(f"Payment rollback successful for user {userId}")


