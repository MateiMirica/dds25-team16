import redis
import json
import requests
from msgspec import msgpack, Struct
from collections import defaultdict
from RecoveryLogger import RecoveryLogger
from rpc_worker import RPCWorker
import os


class OrderDBError(Exception):
    """Custom exception for db errors."""

class EmergencyStateError(Exception):
    """Custom exception for emergency state errors."""

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
        self.unique_group_id = f"order_worker_{os.environ["HOSTNAME"]}"
        self.recovery_logger = RecoveryLogger(f"/order/logs/order_logs_{os.environ["HOSTNAME"]}.txt")
        self.payment_worker = RPCWorker(router, "ReplyResponsePayment", self.unique_group_id, self.recovery_logger)
        self.stock_worker = RPCWorker(router, "ReplyResponseStock", self.unique_group_id, self.recovery_logger
                                      )
        self.in_emergency_state = True
        self.emergency_orders = self.recovery_logger.get_unfinished_orders()
        # self.router.after_startup(self.after_startup_callback)
    #
    # async def after_startup_callback(self, app):
    #     await self.repair_state()
    #     return {}

    async def repair_state(self):
        self.logger.info("RESTARTING")

        self.logger.info("EMERGENCY ORDERS")
        self.logger.info(self.emergency_orders)
        self.logger.info("REPAIRING STATE")

        # self.logger.info('right after this')
        # await self.create_message_and_send('aaa', None, None)

        for order_id in self.emergency_orders:
            
            payment_data = requests.get(f"{os.environ['GATEWAY_URL']}/payment/checkid/{order_id}").text
            stock_data = requests.get(f"{os.environ['GATEWAY_URL']}/stock/checkid/{order_id}").text
            
            payment_status = json.loads(payment_data)
            stock_status = json.loads(stock_data)

            order_entry: OrderValue = self.get_order_from_db(order_id)

            if payment_status == "PAID" and stock_status == "PAID":
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
                self.logger.info("COMPLETED")
                order_entry.paid = True
                self.db.set(order_id, msgpack.encode(order_entry))
            elif stock_status == "REJECTED" and payment_status == "PAID":
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                self.logger.info("ROLLBACK PAYMENT")
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            elif stock_status == "REJECTED" and payment_status == "ROLLEDBACK":
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            elif payment_status == "PAID" and stock_status == "MISSING":
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                self.logger.info("ROLLBACK PAYMENT")
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            elif payment_status == "ROLLEDBACK" and stock_status == "MISSING":
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            elif payment_status == "ROLLEDBACK" and stock_status == "ROLLEDBACK":
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            elif payment_status == "ROLLEDBACK" and stock_status == "PAID":
                await self.create_message_and_send('RollbackStock', order_id, order_entry)
                self.recovery_logger.write_to_log(order_id, "COMPLETED")
            else:
                self.logger.info("UNKNOWN STATE")
                self.logger.info(payment_status + " " + stock_status + " " + order_id)
            ## cine face asta?
            ## daca am dat rollback la payment, dar am dat timeout la stock
            ## ce se intampla?
        self.in_emergency_state = False

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
            # case _:
            #     self.logger.info("this should come")
            #     msg["orderId"] = "aaa"
            #     await self.payment_worker.request_no_response(json.dumps(msg), "RollbackPayment")

    def get_items_in_order(self, order_entry: OrderValue):
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        return items_quantities

    async def checkout(self, order_id: str):
        # while self.in_emergency_state:
        #     pass
        # if self.in_emergency_state:
        #     raise EmergencyStateError() # do not accept requests while repairing state
        self.logger.debug(f"Checking out {order_id}")
        order_entry: OrderValue = self.get_order_from_db(order_id)
        if order_entry.paid is True:
            return order_entry
        self.recovery_logger.write_to_log(order_id, "STARTED")
        status_payment = await self.create_message_and_send('UpdatePayment', order_id, order_entry)
        if status_payment["status"] is True:
            status_stock = await self.create_message_and_send('UpdateStock', order_id, order_entry)
            if status_stock["status"] is True:
                order_entry.paid = True
                self.db.set(order_id, msgpack.encode(order_entry))
                # self.recovery_logger.write_to_log(order_id, "COMPLETED")
            else:
                await self.create_message_and_send('RollbackPayment', order_id, order_entry)
                # OK should this be handled in the recovery?
                # self.recovery_logger.write_to_log(order_id, "COMPLETED") # WRONG - IF WE ROLLBACK BECAUSE STOCK TIMES OUT ITS NOT YET COMPLETE
        self.recovery_logger.write_to_log(order_id, "COMPLETED")


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



