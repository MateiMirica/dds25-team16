import logging
from uuid import uuid4
from asyncio import Future, wait_for
import json
from faststream.types import SendableMessage
from faststream.kafka.fastapi import KafkaRouter

class RPCWorker:
    def __init__(self, router: KafkaRouter, reply_topic: str, unique_group_id: str) -> None:
        self.responses: dict[str, Future[bytes]] = {}
        self.router = router
        self.reply_topic = reply_topic
        self.unique_group_id = unique_group_id

        self.subscriber = router.subscriber(reply_topic, group_id=unique_group_id)
        self.subscriber(self._handle_responses)

    async def _handle_responses(self, msg) -> None:
        message = json.loads(msg)
        if message["serviceId"] != self.unique_group_id:
            return

        if future := self.responses.pop(message["orderId"], None):
            future.set_result(msg)
        elif message["status"] is True:
            if self.reply_topic == "ReplyResponsePayment":
                await self.request_no_response(json.dumps(message), "RollbackPayment")
            elif self.reply_topic == "ReplyResponseStock":
                await self.request_no_response(json.dumps(message), "RollbackStock")


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
            return json.dumps(msg).encode("utf-8")
        else:
            return response
        
    async def request_no_response(self, data: SendableMessage, topic: str):        
        await self.router.broker.publish(data, topic)
