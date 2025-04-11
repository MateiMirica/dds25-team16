import redis
import json
import requests
from msgspec import msgpack, Struct
from collections import defaultdict
from RecoveryLogger import RecoveryLogger
from rpc_worker import RPCWorker
import os

from order_status import Status


class OrderDBError(Exception):
    """Custom exception for db errors."""

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    status: str
    user_id: str
    total_cost: int

COMPLETED_ORDER = "COMPLETED"
STARTED_ORDER = "STARTED"

class OrderWorker():
    def __init__(self, logger, db, router):
        self.logger = logger
        self.db = db
        self.router = router
        self.unique_group_id = f"order_worker_{os.environ["HOSTNAME"]}"
        self.recovery_logger = RecoveryLogger(f"/order/logs/order_logs_{os.environ["HOSTNAME"]}.txt")
        self.payment_worker = RPCWorker(router, "ReplyResponsePayment", self.unique_group_id, self.recovery_logger, db)
        self.stock_worker = RPCWorker(router, "ReplyResponseStock", self.unique_group_id, self.recovery_logger, db)
        self.emergency_orders = self.recovery_logger.get_unfinished_orders()

    async def repair_state(self):
        self.logger.info("REPAIRING STATE")

        for order_id in self.emergency_orders:
            payment_data = requests.get(f"{os.environ['GATEWAY_URL']}/payment/checkid/{order_id}").text
            stock_data = requests.get(f"{os.environ['GATEWAY_URL']}/stock/checkid/{order_id}").text
            
            payment_status = Status.convertResponseFromDB(json.loads(payment_data))
            stock_status = Status.convertResponseFromDB(json.loads(stock_data))

            order_entry: OrderValue = self.get_order_from_db(order_id)

            if order_entry.status == "completed":
                continue

            order_entry.status = "completed"
            if payment_status == Status.PAID and stock_status == Status.PAID:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
                order_entry.paid = True
            elif payment_status == Status.PAID and stock_status == Status.REJECTED:
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                self.logger.info("ROLLBACK PAYMENT")
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.ROLLEDBACK and stock_status == Status.REJECTED:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.PAID and stock_status == Status.MISSING:
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                self.logger.info("ROLLBACK PAYMENT")
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.ROLLEDBACK and stock_status == Status.MISSING:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.ROLLEDBACK and stock_status == Status.ROLLEDBACK:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.ROLLEDBACK and stock_status == Status.PAID:
                await self.create_message_and_send('RollbackStock', order_id, order_entry)
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.MISSING and stock_status == Status.MISSING:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            elif payment_status == Status.REJECTED and stock_status == Status.MISSING:
                self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
            else:
                self.logger.info("UNKNOWN STATE")
                self.logger.info(payment_status + " " + stock_status + " " + order_id)
            self.db.set(order_id, msgpack.encode(order_entry))

        self.logger.info("FINISHED REPAIRING STATE")

    async def create_message_and_send(self, topic: str, order_id: str, order_entry: OrderValue):
        msg = dict()
        match topic:
            case 'UpdateStock':
                items_quantities = self.get_items_in_order(order_entry)
                msg["orderId"] = order_id
                msg["items"] = items_quantities
                msg["serviceId"] = self.unique_group_id
                response = await self.stock_worker.request(json.dumps(msg), "UpdateStock", correlation_id=order_id)
                return json.loads(response)
            case 'UpdatePayment':
                msg["orderId"] = order_id
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
                msg["serviceId"] = self.unique_group_id
                response = await self.payment_worker.request(json.dumps(msg), "UpdatePayment", correlation_id=order_id)
                return json.loads(response)
            case 'RollbackPayment':
                msg["orderId"] = order_id
                msg["userId"] = order_entry.user_id
                msg["amount"] = order_entry.total_cost
                await self.payment_worker.request_no_response(json.dumps(msg), "RollbackPayment")
                return None
            case 'RollbackStock':
                items_quantities = self.get_items_in_order(order_entry)
                msg["orderId"] = order_id
                msg["items"] = items_quantities
                await self.stock_worker.request_no_response(json.dumps(msg), "RollbackStock")


    def get_items_in_order(self, order_entry: OrderValue):
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        return items_quantities

    async def checkout(self, order_id: str):
        self.logger.debug(f"Checking out {order_id}")
        order_entry: OrderValue = self.get_order_from_db(order_id)
        if order_entry.status == "completed":
            return order_entry
        self.recovery_logger.write_to_log(order_id, STARTED_ORDER)
        status_payment = await self.create_message_and_send('UpdatePayment', order_id, order_entry)
        if status_payment["status"] is True:
            status_stock = await self.create_message_and_send('UpdateStock', order_id, order_entry)
            if status_stock["status"] is True:
                order_entry.paid = True
            else:
                self.logger.info(f"Rolling back payment for order {order_id}")
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                if status_stock.get("timeout", False):
                    self.logger.info(f"Rolling back stock for order {order_id}")
                    await self.create_message_and_send('RollbackStock', order_id, order_entry)

        elif status_payment.get("timeout", False):
                self.logger.info(f"Rolling back payment for order {order_id}")
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)

        order_entry_copy = self.get_order_from_db(order_id)
        if order_entry_copy.status == 'completed':
            order_entry.paid = False
            await self.create_message_and_send('RollbackPayment', order_id, order_entry)
            await self.create_message_and_send('RollbackStock', order_id, order_entry)
        else:
            self.recovery_logger.write_to_log(order_id, COMPLETED_ORDER)
        order_entry.status = "completed"
        self.db.set(order_id, msgpack.encode(order_entry))
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



