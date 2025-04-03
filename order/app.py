import asyncio
import logging
import os
import atexit
import random
import uuid
from contextlib import asynccontextmanager

import redis
import requests
import uvicorn

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from faststream.kafka.fastapi import KafkaRouter
from redis.sentinel import Sentinel

from order_worker import OrderWorker, OrderValue

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

router = KafkaRouter("kafka:9092", logger=None)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sentinel_hosts = [
    (host.split(":")[0], int(host.split(":")[1]))
    for host in os.environ["REDIS_SENTINEL_HOSTS"].split(",")
]

sentinel = Sentinel(
    sentinel_hosts,
    socket_timeout=0.5,
    password=os.environ["REDIS_PASSWORD"]
)

db = sentinel.master_for(
    os.environ["REDIS_MASTER_NAME"],
    socket_timeout=0.5,
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ.get("REDIS_DB", 0))
)

orderWorker = OrderWorker(logger, db, router)
async def after_startup_callback(app):
    await router.broker.connect()
    await orderWorker.repair_state()
    return {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(orderWorker.recovery_logger.file_path)
    router.after_startup(after_startup_callback)
    yield

app = FastAPI(title="order-service", lifespan=lifespan)
app.include_router(router)

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

batch_create_order_lua_script = db.register_script(""""
local n = tonumber(ARGV[1])
local n_items = tonumber(ARGV[2])
local n_users = tonumber(ARGV[3])
local item_price = tonumber(ARGV[4])

math.randomseed(os.time())

for i = 0, n - 1 do
    local user_id = tostring(math.random(0, n_users - 1))
    local item1_id = tostring(math.random(0, n_items - 1))
    local item2_id = tostring(math.random(0, n_items - 1))
    
    local entry = {
        paid = false,
        status = "pending",
        items = { {item1_id, 1}, {item2_id, 1} },
        user_id = user_id,
        total_cost = 2 * item_price
    }
    
    redis.call("SET", tostring(i), cmsgpack.pack(entry))
end

return "SUCCESS"
""")

@app.post('/create/{user_id}')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, status="pending", items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {'order_id': key}


@app.post('/batch_init/{n}/{n_items}/{n_users}/{item_price}')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    try:
        batch_create_order_lua_script(keys=[], args=[n, n_items, n_users, item_price])
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    return {"msg": "Batch init for orders successful"}


@app.get('/find/{order_id}')
def find_order(order_id: str):
    order_entry: OrderValue = orderWorker.get_order_from_db(order_id)
    return {
        "order_id": order_id,
        "status": order_entry.status,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }



def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        raise HTTPException(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        raise HTTPException(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/{order_id}/{item_id}/{quantity}')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = orderWorker.get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        raise HTTPException(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    except Exception:
        raise HTTPException(409, REQ_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status_code=200)

@app.post('/checkout/{order_id}')
async def checkout(order_id: str):
    order: OrderValue = await orderWorker.checkout(order_id)
    # logger.warning(f"Order: {order}")
    if order.paid:
        logger.info(f"CHECKOUT PAID {order_id}")
        return Response("Checkout successful", status_code=200)
    else:
        logger.info(f"CHECKOUT NOT PAID {order_id}")
        return Response("Checkout unsuccessful", status_code=400)


if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    logger.info("Starting Order Service with uvicorn")