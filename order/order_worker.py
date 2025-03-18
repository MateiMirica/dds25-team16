import redis
import json
from msgspec import msgpack, Struct
from collections import defaultdict
from rpc_worker import RPCWorker
import uuid


class OrderDBError(Exception):
    """Custom exception for db errors."""

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    status: str
    user_id: str
    total_cost: int

class OrderWorker():
    def __init__(self, logger, db, router):
        self.logger = logger
        self.db = db
        self.router = router
        unique_group_id = f"worker_{uuid.uuid4()}"
        self.payment_worker = RPCWorker(router, "ReplyResponsePayment", unique_group_id)
        self.stock_worker = RPCWorker(router, "ReplyResponseStock", unique_group_id)
        self.payment_publisher = router.publisher("RollbackPayment")
        self.stock_publisher = router.publisher("RollbackStock")

    async def create_message_and_send(self, topic: str, order_id: str, order_entry: OrderValue):
        msg = dict()
        match topic:
            case 'UpdateStock':
                items_quantities = self.get_items_in_order(order_entry)
                msg["orderId"] = order_id
                msg["items"] = items_quantities
                response = await self.stock_worker.request(json.dumps(msg), "UpdateStock", correlation_id=order_id)
                return json.loads(response)
            case 'UpdatePayment':
                msg["orderId"] = order_id
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
                response = await self.payment_worker.request(json.dumps(msg), "UpdatePayment", correlation_id=order_id)
                return json.loads(response)
            case 'RollbackPayment':
                msg["orderId"] = order_id
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
                await self.payment_publisher.publish(json.dumps(msg))
                return None
            case 'RollbackStock':
                items_quantities = self.get_items_in_order(order_entry)
                msg["orderId"] = order_id
                msg["items"] = items_quantities
                await self.stock_publisher.publish(json.dumps(msg))
                return None

    def get_items_in_order(self, order_entry: OrderValue):
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        return items_quantities

    async def checkout(self, order_id: str):
        self.logger.debug(f"Checking out {order_id}")
        order_entry: OrderValue = self.get_order_from_db(order_id)
        if order_entry.paid is True:
            return order_entry

        status_payment = await self.create_message_and_send('UpdatePayment', order_id, order_entry)
        if status_payment["status"] is True:
            status_stock = await self.create_message_and_send('UpdateStock', order_id, order_entry)
            if status_stock["status"] is True:
                order_entry.paid = True
                self.db.set(order_id, msgpack.encode(order_entry))
            else:
                if status_stock.get("timeout"):
                    await self.create_message_and_send('RollbackStock', order_id, order_entry)
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
            

        return order_entry

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

    async def send(self, topic, data):
        self.router.publish(topic, data)



