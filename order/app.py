import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import threading
import time
import json

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import pika

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


class Publisher(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.is_running = True
        self.name = "Publisher"
        self.queue = "main"

        parameters = pika.ConnectionParameters("rabbitmq", )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable = True)

    def run(self):
        while self.is_running:
            self.connection.process_data_events(time_limit=1)

    def _publish(self, message, correlation_id="", reply_to=""):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue,
            body=message.encode(),
            properties=pika.BasicProperties(
                correlation_id=correlation_id,
                reply_to=reply_to))

    def publish(self, message, correlation_id="", reply_to=""):
        self.connection.add_callback_threadsafe(lambda: self._publish(message, correlation_id, reply_to))

    def stop(self):
        print("Stopping...")
        self.is_running = False
        # Wait until all the data events have been processed
        self.connection.process_data_events(time_limit=1)
        if self.connection.is_open:
            self.connection.close()
        print("Stopped")


def create_connection():
    retries = 5
    while retries > 0:
        try:
            publisher = Publisher()
            publisher.start()
            return publisher
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection failed: {e}, retrying...")
            time.sleep(5)
            retries -= 1
    raise Exception("Failed to connect to RabbitMQ after several attempts")


# Initialize connection
publisher = create_connection()

class RequestStatus(Struct):
    status: str

def consume_status_queue():
    def status_callback(ch, method, properties, body):
        status_data = json.loads(body)
        key = status_data['correlation_id']
        status = status_data['status']

        value = msgpack.encode(RequestStatus(status=status))
        try:
            db.set(key, value)
        except redis.exceptions.RedisError:
            print(DB_ERROR_STR)
            ch.basic_nack(delivery_tag=method.delivery_tag)
        app.logger.debug(f'Request {key} is updated to {status}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    status_connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    status_channel = status_connection.channel()

    status_channel.queue_declare(queue='status', durable=True)
    status_channel.basic_consume(queue='status', on_message_callback=status_callback, auto_ack=False)

    print('Waiting for status messages')
    status_channel.start_consuming()

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


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

def get_status_from_db(status_id: str) -> RequestStatus | None:
    try:
        entry: bytes = db.get(status_id)
        app.logger.debug(f"get status from db: entry is {entry}")
    except redis.exceptions.RedisError:
        app.logger.debug(f"get status from db: entry is and error")
        return abort(400, DB_ERROR_STR)
    app.logger.debug(f"get status from db: entry is {entry}")
    entry: RequestStatus | None = msgpack.decode(entry, type=RequestStatus) if entry else None
    app.logger.debug(f"DECODED get status from db: entry is {entry}")
    if entry is None:
        abort(400, f"Status: {status_id} not found!")
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
                           total_cost=2 * item_price)
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
def add_item_request(order_id: str, item_id: str, quantity: int):
    correlation_id = str(uuid.uuid4())
    try:
        message = json.dumps({
            "function": "handle_add_item",
            "args": [order_id, item_id, quantity]
        })
        publisher.publish(message, correlation_id, "status")
        value = msgpack.encode(RequestStatus(status='Pending'))
        db.set(correlation_id, value)
        return jsonify({"success": "Item addition request sent", "correlation_id": correlation_id}), 200
    except Exception as e:
        print(e)
        return jsonify({"error": "Failed to add item", "details": str(e)}), 500


@app.post('/addItemProcess/<order_id>/<item_id>/<quantity>/<price>')
def add_item_process(order_id: str, item_id: str, quantity: int, price: int):
    try:
        quantity = int(quantity)
        price = int(price)

        order_entry: OrderValue = get_order_from_db(order_id)
        if not order_entry:
            return jsonify({"error": f"Order {order_id} not found"}), 404

        order_entry.items.append((item_id, quantity))
        order_entry.total_cost += quantity * price

        try:
            db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        return Response(f"Item: {item_id} added to: {order_id}, price updated to: {order_entry.total_cost}", status=200)

    except Exception as e:
        return jsonify({"error": "Failed to add item", "details": str(e)}), 500


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout_request(order_id: str):
    correlation_id = str(uuid.uuid4())
    try:
        # # Get Order
        # order_entry: OrderValue = get_order_from_db(order_id)

        # Create Message
        message = json.dumps({
            "function": "handle_checkout",
            "args": (order_id, )
        })

        # Publish Message
        publisher.publish(message)

        # Store request status
        value = msgpack.encode(RequestStatus(status='Pending'))
        db.set(correlation_id, value)

        return jsonify({"success": "Checkout request sent", "correlation_id": correlation_id}), 202
    except Exception as e:
        return jsonify({"error": "Failed to initiate checkout", "details": str(e)}), 500


@app.post('/checkoutProcess/<order_id>')
def checkout_process(order_id: str):
    app.logger.debug(f"Saving order {order_id}")

    # Get Order
    order_entry: OrderValue = get_order_from_db(order_id)

    # Update Order
    order_entry.paid = True

    # Save Order
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(500, DB_ERROR_STR)

    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)


@app.get('/status/<correlation_id>')
def get_status(correlation_id: str):
    app.logger.debug(f"GET request for {correlation_id}")
    status_entry: RequestStatus = get_status_from_db(correlation_id)
    app.logger.debug(f"GET request for {correlation_id} is {status_entry} ")
    return jsonify(
        {
            "correlation_id": correlation_id,
            "status": status_entry.status
        }
    ), 200


if __name__ == '__main__':
    # Start the status queue consumer in a separate Thread
    status_thread = threading.Thread(target=consume_status_queue())
    status_thread.daemon = True
    status_thread.start()

    app.run(host="0.0.0.0", port=8000, debug=True, threaded=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
