import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import json

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from confluent_kafka import Producer, Consumer

import threading

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


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
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
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
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items):
    items = dict()
    items["items"] = removed_items
    send_to_kafka('RollbackStock', json.dumps(items))

def get_items_in_order(order_entry: OrderValue):
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    return items_quantities

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities = get_items_in_order(order_entry)
    items = dict()
    items["orderId"] = order_id
    items["items"] = items_quantities
    send_to_kafka('UpdateStock', json.dumps(items))
    return Response("Checkout accepted", status=202)

def process_response_stock(response: str):
    status = json.loads(response)
    if status["status"] is True:
        app.logger.info("Stock substraction successful")
        order_id = status["orderId"]
        # can be avoided if payment details are included in stock update
        order_entry: OrderValue = get_order_from_db(order_id)
        payment = dict()
        payment["orderId"] = order_id
        payment["userId"] = order_entry.user_id
        payment["amount"] = order_entry.total_cost
        send_to_kafka('UpdatePayment', json.dumps(payment))
    else:
        app.logger.info("Stock substraction failed")

def process_response_payment(response: str):
    status = json.loads(response)
    order_id = status["oderId"]
    order_entry: OrderValue = get_order_from_db(order_id)
    if status["status"] is True:
        order_entry.paid = True
        db.set(order_id, msgpack.encode(order_entry))
        app.logger.info(f"order {order_id} checkout successful")
    else:
        app.logger.info("Payment failed, attempting stock rollback...")
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
    return jsonify({'status': 'Message sent'}), 200

def consume_messages(consumer, action):
    while True:
        message = consumer.poll(0.1)
        if message is None:
            continue
        if message.error():
            app.logger.info(f"Consumer error: {message.error()}")
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
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    for i in range(len(consumer_topics)):
        start_consumer_thread(consumer_topics[i], consumer_actions[i])
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
