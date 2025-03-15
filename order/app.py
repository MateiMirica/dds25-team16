import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import json
from unittest import case

import redis
import requests
import uvicorn

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from confluent_kafka import Producer, Consumer
from errors import TimeoutException

import threading
from order_worker import OrderWorker, OrderValue
import asyncio, time

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = FastAPI(title="order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
orderWorker = OrderWorker(logger, db)

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

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           status="pending",
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
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
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status_code=200)

# def rollback_payment(user_id, total_cost):
#     msg = {
#         "userId": user_id,
#         "amount": total_cost
#     }
#     send_to_kafka('RollbackPayment', json.dumps(msg))

# def rollback_stock(removed_items):
#     items = dict()
#     items["items"] = removed_items
#     send_to_kafka('RollbackStock', json.dumps(items))

@app.post('/checkout/{order_id}')
async def checkout(order_id: str):
    try:
        order: OrderValue = await orderWorker.checkout(order_id)
        # logger.warning(f"Order: {order}")
        if order.paid:
            return Response("Checkout successful", status_code=200)
        else:
            logger.info("CHECKOUT NOT PAID")
            return Response("Checkout unsuccessful", status_code=400)
    except TimeoutException:
        logger.info("TIMEOUT")
        return Response("Checkout TIMEOUT", status_code=400)


if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    logger.info("Starting Order Service with uvicorn")
