import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import json

import redis
import requests
import uvicorn

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from confluent_kafka import Producer, Consumer
from errors import TimeoutException

import threading
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

def create_kafka_producer():
    conf = {'bootstrap.servers': "kafka:9092"}
    producer = Producer(**conf)
    return producer

def create_kafka_consumer(topic):
    conf = {
        'bootstrap.servers': "kafka:9092",
        'group.id': "orders",
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)
    consumer.subscribe([topic])
    return consumer

producer = create_kafka_producer()

class OrderValue(Struct):
    paid: bool
    status: str 
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        raise HTTPException(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        raise HTTPException(400, f"Order: {order_id} not found!")
    return entry


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
    order_entry: OrderValue = get_order_from_db(order_id)
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
    order_entry: OrderValue = get_order_from_db(order_id)
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

def rollback_payment(user_id, total_cost):
    msg = {
        "userId": user_id,
        "amount": total_cost
    }
    send_to_kafka('RollbackPayment', json.dumps(msg))

def rollback_stock(removed_items):
    items = dict()
    items["items"] = removed_items
    send_to_kafka('RollbackStock', json.dumps(items))

def get_items_in_order(order_entry: OrderValue):
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    return items_quantities

#store order_ids which i handle currently
pending_orders = {}

@app.post('/checkout/{order_id}')
async def checkout(order_id: str):
    logging.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities = get_items_in_order(order_entry)
    items = dict()
    items["orderId"] = order_id
    items["items"] = items_quantities
    send_to_kafka('UpdateStock', json.dumps(items))
    pending_orders[order_id] = True

    try:
        await await_order(order_id)
        order: OrderValue = get_order_from_db(order_id)
        if order.paid:
            return Response("Checkout successful", status_code=200)
        else:
            return Response("Checkout unsuccessful", status_code=400)
    except TimeoutException:
        return Response("Checkout unsuccessful", status_code=400)


# Check every 0.001s if the order with order_id is completed
async def await_order(order_id: str, timeout=10):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if order_id not in pending_orders:
            return
        asyncio.sleep(0.001)
    raise Exception(f"timeout for order {order_id} reached")


def fail_order(order_id):
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.status = "failed"
    db.set(order_id, msgpack.encode(order_entry))

def process_response_stock(response: str):
    status = json.loads(response)
    order_id = status["orderId"]

    if status["status"] is True:
        logging.info("Stock substraction successful")
        order_entry: OrderValue = get_order_from_db(order_id)
        if order_entry.status != "pending":
            rollback_stock(get_items_in_order(order_entry))
            return
        
        payment = dict()
        payment["orderId"] = order_id
        payment["userId"] = order_entry.user_id
        payment["amount"] = order_entry.total_cost
        send_to_kafka('UpdatePayment', json.dumps(payment))
    else:
        logging.info("Stock substraction failed")
        fail_order(order_id)


def process_response_payment(response: str):
    status = json.loads(response)
    order_id = status["orderId"]
    pending_orders.pop(order_id, None)
    order_entry: OrderValue = get_order_from_db(order_id)
    if status["status"] is True:
        if order_entry.status != "pending":
            # rollback both
            rollback_stock(get_items_in_order(order_entry))
            rollback_payment(order_entry.user_id, order_entry.total_cost)
            return
        
        order_entry.paid = True
        order_entry.status = "success"
        db.set(order_id, msgpack.encode(order_entry))
        logging.info(f"order {order_id} checkout successful")
    else:
        logging.info("Payment failed, attempting stock rollback...")
        fail_order(order_id)
        items_quantities = get_items_in_order(order_entry)
        rollback_stock(items_quantities)

def send_to_kafka(topic, data):
    producer.produce(topic, data)
    producer.flush()

@app.route('/kafka_demo', methods=['POST'])
def demo_kafka():
    """ Demo kafka """
    message = {
        'message': 'Hello',
        'from': 'order',
    }
    send_to_kafka('demo_topic', json.dumps(message))
    return {'status': 'Message sent'}, 200

def consume_messages(consumer, action):
    while True:
        message = consumer.poll(0.1)
        if message is None:
            continue
        if message.error():
            logging.info(f"Consumer error: {message.error()}")
            continue
        action(message.value().decode('utf-8'))

def start_consumer_thread(topic, action):
    consumer = create_kafka_consumer(topic)
    thread = threading.Thread(target=consume_messages, args=(consumer,action,))
    thread.daemon = True
    thread.start()

consumer_topics = ['ResponseStock', 'ResponsePayment']
consumer_actions = [process_response_stock, process_response_payment]

if __name__ == '__main__':
    for i in range(len(consumer_topics)):
        start_consumer_thread(consumer_topics[i], consumer_actions[i])
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
else:
    for i in range(len(consumer_topics)):
        start_consumer_thread(consumer_topics[i], consumer_actions[i])
    logger.info("Starting Order Service with uvicorn")
