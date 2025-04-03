import logging
from asyncio import Future, wait_for
import json
from faststream.types import SendableMessage
from faststream.kafka.fastapi import KafkaRouter
from RecoveryLogger import RecoveryLogger

COMPLETED_ORDER = "COMPLETED"

class RPCWorker:
    def __init__(self, router: KafkaRouter, reply_topic: str, unique_group_id: str, recovery_logger: RecoveryLogger) -> None:
        self.responses: dict[str, Future[bytes]] = {}
        self.router = router
        self.unique_group_id = unique_group_id
        self.reply_topic = reply_topic + f"{unique_group_id}"
        self.subscriber = router.subscriber(self.reply_topic, group_id=unique_group_id)
        self.subscriber(self._handle_responses)
        # self.recovery_logger = RecoveryLogger(f"/order/logs/order_logs_{os.environ["HOSTNAME"]}.txt")
        self.recovery_logger = recovery_logger

    async def _handle_responses(self, msg) -> None:
        message = json.loads(msg)
        if message["serviceId"] != self.unique_group_id:
            return

        if future := self.responses.pop(message["orderId"], None):
            future.set_result(msg)
        elif message["status"] is True:
            if self.reply_topic == "ReplyResponsePayment":
                await self.request_no_response(json.dumps(message), "RollbackPayment")
                self.recovery_logger.write_to_log(message["orderId"], COMPLETED_ORDER)
            elif self.reply_topic == "ReplyResponseStock":
                await self.request_no_response(json.dumps(message), "RollbackStock")
                self.recovery_logger.write_to_log(message["orderId"], COMPLETED_ORDER)


    async def request(
        self,
        data: SendableMessage,
        topic: str,
        correlation_id: str,
        timeout: float = 10.0,
    ) -> bytes:
        future = self.responses[correlation_id] = Future[bytes]()

        await self.router.broker.publish(
            data, topic,
            reply_to=self.reply_topic,
            correlation_id=correlation_id,
        )

        try:
            response: bytes = await wait_for(future, timeout=timeout)
        except Exception:
            logging.getLogger().info("Timedout")
            self.responses.pop(correlation_id, None)
            msg = dict()
            msg["status"] = False
            msg["timeout"] = True
            return json.dumps(msg).encode("utf-8")
        else:
            return response

    async def request_no_response(self, data: SendableMessage, topic: str):
        await self.router.broker.publish(data, topic)
